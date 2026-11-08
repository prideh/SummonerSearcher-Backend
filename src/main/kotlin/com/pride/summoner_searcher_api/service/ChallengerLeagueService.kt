package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.CachedLeaderboardDto
import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.PreparedStatement
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/**
 * Manages Challenger leaderboard retrieval and cache warming with:
 * - Bounded in-memory last-known-good fallback strictly capped to the 3 supported regions (`euw1`, `na1`, `kr`),
 *   retaining ONLY complete snapshots that fit within both entry count (`<= 300`) and hard serialized byte caps
 *   (`<= 128 KiB` per region, `<= 384 KiB` total). Oversized snapshots are rejected as a whole (never serving partial rank lists).
 * - On-demand singleflight self-healing with failure backoff when Redis keys are absent/evicted or malformed.
 * - Single bounded Riot League-V4 call (`fetchChallengerLeagueBounded`) with zero 300-account fanout.
 * - Local identity enrichment from `indexed_players` via a bounded JDBC `PreparedStatement` with `setQueryTimeout`,
 *   `setNetworkTimeout`, and active `Statement.cancel()` / `Connection.abort()` on deadline, or omitted when a bounded
 *   JDBC `DataSource` is not available.
 */
@Service
class ChallengerLeagueService @Autowired constructor(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService,
    @Autowired(required = false)
    private val dataSource: DataSource? = null,
    private val timeProvider: TimeProvider = SystemTimeProvider(),
    private val memoryStatsProvider: MemoryStatsProvider = SystemMemoryStatsProvider(),
    @Value("\${cohort.collector.container-memory-limit-bytes:536870912}")
    private val configuredContainerLimitBytes: Long = 536_870_912L,
    @Value("\${cohort.collector.max-memory-utilization-ratio:0.78}")
    private val maxMemoryUtilizationRatio: Double = 0.78,
    @Value("\${cohort.collector.min-free-memory-bytes:67108864}")
    private val minFreeMemoryBytes: Long = 67_108_864L,
    @Value("\${leaderboard.challenger.refresh-deadline-ms:5000}")
    private val refreshDeadlineMs: Long = DEFAULT_REFRESH_DEADLINE_MS,
    @Value("\${leaderboard.challenger.failure-backoff-ms:30000}")
    private val failureBackoffMs: Long = DEFAULT_FAILURE_BACKOFF_MS,
    @Value("\${leaderboard.challenger.max-response-bytes:524288}")
    private val maxResponseBytes: Int = RiotApiService.MAX_CHALLENGER_LEAGUE_RESPONSE_BYTES,
    @Value("\${leaderboard.challenger.db-lookup-timeout-ms:1500}")
    private val dbLookupTimeoutMs: Long = DEFAULT_DB_LOOKUP_TIMEOUT_MS,
    private val maxEntriesPerRegion: Int = MAX_LEADERBOARD_ENTRIES,
    private val maxLastKnownGoodRegionBytes: Int = MAX_LAST_KNOWN_GOOD_REGION_BYTES,
    private val maxLastKnownGoodTotalBytes: Int = MAX_LAST_KNOWN_GOOD_TOTAL_BYTES
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val objectMapper = ObjectMapper().findAndRegisterModules()
    private val singleFlightMutex = Mutex()
    private val lastKnownGoodLock = Any()
    private val inFlightRefreshes = ConcurrentHashMap<String, CompletableDeferred<LeagueListDTO?>>()
    private val failureBackoffUntilMs = ConcurrentHashMap<String, Long>()
    private val lastRefreshFailureReasonByRegion = ConcurrentHashMap<String, String>()
    private val lastKnownGoodByRegion = ConcurrentHashMap<String, BoundedLeaderboardSnapshot>()
    private val lastIdentityCoverageByRegion = ConcurrentHashMap<String, IdentityCoverageStats>()
    private val refreshScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val activeDbLookupWorkers = AtomicInteger(0)
    private val dbLookupPermits = Semaphore(MAX_CONCURRENT_DB_LOOKUPS)
    private val dbLookupThreadCounter = AtomicInteger(0)
    private val dbLookupExecutor = java.util.concurrent.ThreadPoolExecutor(
        MAX_CONCURRENT_DB_LOOKUPS,
        MAX_CONCURRENT_DB_LOOKUPS,
        30L,
        java.util.concurrent.TimeUnit.SECONDS,
        java.util.concurrent.SynchronousQueue(),
        java.util.concurrent.ThreadFactory { runnable ->
            Thread(runnable, "challenger-db-lookup-${dbLookupThreadCounter.incrementAndGet()}").apply {
                isDaemon = true
            }
        },
        java.util.concurrent.ThreadPoolExecutor.AbortPolicy()
    ).apply {
        allowCoreThreadTimeOut(true)
    }
    private val dbAbortExecutor = java.util.concurrent.Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "challenger-db-abort").apply {
            isDaemon = true
        }
    }

    internal data class BoundedLeaderboardSnapshot(
        val region: String,
        val queue: String,
        val cachedDto: CachedLeaderboardDto,
        val serializedBytes: Int
    )

    internal data class IdentityCoverageStats(
        val totalEntries: Int,
        val resolvedIdentityCount: Int,
        val missingIdentityCount: Int,
        val dbResolvedCount: Int,
        val snapshotResolvedCount: Int
    )

    companion object {
        val SUPPORTED_REGIONS: Set<String> = setOf("euw1", "na1", "kr")
        const val DEFAULT_QUEUE = "RANKED_SOLO_5x5"
        const val MAX_LEADERBOARD_ENTRIES = 300
        const val MAX_LAST_KNOWN_GOOD_REGION_BYTES = 131_072 // 128 KiB per region
        const val MAX_LAST_KNOWN_GOOD_TOTAL_BYTES = 393_216 // 384 KiB across 3 regions
        const val DEFAULT_REFRESH_DEADLINE_MS = 5_000L
        const val DEFAULT_DB_LOOKUP_TIMEOUT_MS = 1_500L
        const val DEFAULT_FAILURE_BACKOFF_MS = 30_000L
        const val CACHE_STALE_HOURS = 24L
        const val MAX_CONCURRENT_DB_LOOKUPS = 3
    }

    private fun normalizeRegion(region: String): String = region.trim().lowercase()

    private fun getCacheKey(region: String, queue: String) = "leaderboard:${normalizeRegion(region)}::$queue"

    internal fun isSupportedRegion(region: String): Boolean = normalizeRegion(region) in SUPPORTED_REGIONS

    internal fun isValidLeaderboard(leaderboard: LeagueListDTO?): Boolean {
        if (leaderboard == null || leaderboard.entries.isEmpty()) return false
        return leaderboard.entries.any { !it.puuid.isNullOrBlank() }
    }

    internal fun getLastKnownGoodRegionCount(): Int = synchronized(lastKnownGoodLock) {
        lastKnownGoodByRegion.size
    }

    internal fun getLastKnownGoodRegionBytes(region: String): Int = synchronized(lastKnownGoodLock) {
        lastKnownGoodByRegion[normalizeRegion(region)]?.serializedBytes ?: 0
    }

    internal fun getLastKnownGoodTotalBytes(): Int = synchronized(lastKnownGoodLock) {
        lastKnownGoodByRegion.values.sumOf { it.serializedBytes }
    }

    internal fun getInFlightRefreshCount(): Int = inFlightRefreshes.size

    internal fun getActiveDbLookupWorkers(): Int = activeDbLookupWorkers.get()

    internal fun getLastIdentityCoverage(region: String): IdentityCoverageStats? =
        lastIdentityCoverageByRegion[normalizeRegion(region)]

    internal fun getRemainingBackoffMs(region: String, queue: String = DEFAULT_QUEUE): Long {
        val cacheKey = getCacheKey(region, queue)
        val until = failureBackoffUntilMs[cacheKey] ?: return 0L
        return (until - timeProvider.now()).coerceAtLeast(0L)
    }

    internal fun getLastRefreshFailureReason(region: String): String? =
        lastRefreshFailureReasonByRegion[normalizeRegion(region)]

    internal fun hasMemoryHeadroomForLeaderboardRefresh(): Boolean {
        return try {
            val raw = memoryStatsProvider.sample()
            if (raw.jvmHeapMaxBytes <= 0L || raw.jvmHeapUsedBytes < 0L) {
                return false
            }
            val safeContainerCap = configuredContainerLimitBytes.coerceAtLeast(1L)
            val safeHeapMax = raw.jvmHeapMaxBytes
            val safeHeapUsed = raw.jvmHeapUsedBytes
            val hasBoundedContainerCgroupLimit = raw.cgroupMemoryMaxBytes != null &&
                raw.cgroupMemoryMaxBytes in 1L..2_147_483_648L
            if (hasBoundedContainerCgroupLimit) {
                val cgroupCurrent = raw.cgroupMemoryCurrentBytes
                if (cgroupCurrent == null || cgroupCurrent <= 0L) {
                    logger.warn(
                        "Bounded container cgroup limit={} detected with missing/invalid cgroupMemoryCurrentBytes={}; failing safe (false).",
                        raw.cgroupMemoryMaxBytes,
                        cgroupCurrent
                    )
                    return false
                }
            }
            val effectiveLimit = if (hasBoundedContainerCgroupLimit) {
                minOf(safeContainerCap, raw.cgroupMemoryMaxBytes!!)
            } else {
                safeHeapMax
            }
            val effectiveUsed = if (hasBoundedContainerCgroupLimit) {
                maxOf(raw.cgroupMemoryCurrentBytes!!, safeHeapUsed)
            } else {
                safeHeapUsed
            }
            if (effectiveLimit <= 0L) {
                return false
            }
            val freeBytes = (effectiveLimit - effectiveUsed).coerceAtLeast(0L)
            val ratio = effectiveUsed.toDouble() / effectiveLimit.toDouble()
            ratio.isFinite() && ratio < maxMemoryUtilizationRatio && freeBytes >= minFreeMemoryBytes
        } catch (e: Exception) {
            logger.warn("Memory headroom sample failed during leaderboard refresh guard; failing safe (false): {}", e.message)
            false
        }
    }

    /**
     * Retrieves the Challenger leaderboard for [region] and [queue]:
     * 1. Returns fresh valid Redis cached data immediately while updating the bounded 3-region last-known-good store.
     * 2. On missing/evicted, malformed/empty, or stale Redis data, executes a singleflight bounded refresh
     *    (1 League-V4 call, 0 account fanout, local DB + snapshot identity reuse).
     * 3. Falls back to the complete bounded last-known-good snapshot if upstream refresh is in backoff or blocked by
     *    deadline, rate-limit, network error, or memory headroom guard.
     * 4. Returns `null` when no valid complete leaderboard exists anywhere so the controller responds with explicit HTTP 503.
     */
    fun getChallengerLeagueFromCache(region: String, queue: String): LeagueListDTO? = runBlocking {
        getOrRefreshChallengerLeague(region, queue)
    }

    suspend fun getOrRefreshChallengerLeague(region: String, queue: String = DEFAULT_QUEUE): LeagueListDTO? {
        val normalizedRegion = normalizeRegion(region)
        if (normalizedRegion !in SUPPORTED_REGIONS) {
            logger.warn("Unsupported challenger leaderboard region requested: {}", region)
            return null
        }

        val cacheKey = getCacheKey(normalizedRegion, queue)
        logger.info("Frontend requested challenger leaderboard for key: {}", cacheKey)

        val cachedData = runCatching {
            redisCacheService.get(cacheKey, CachedLeaderboardDto::class.java)
        }.getOrNull()

        val cachedLeaderboard = cachedData?.leaderboard
        if (isValidLeaderboard(cachedLeaderboard)) {
            val previousSnapshot = synchronized(lastKnownGoodLock) {
                lastKnownGoodByRegion[normalizedRegion]?.cachedDto?.leaderboard
            }
            val enrichedCached = enrichAndBoundLeaderboard(
                normalizedRegion = normalizedRegion,
                rawLeague = cachedLeaderboard!!,
                previousSnapshot = previousSnapshot,
                onlyMissingDbIdentities = true,
                deadlineAtMs = timeProvider.now() + dbLookupTimeoutMs.coerceAtLeast(100L)
            )
            val refreshedAt = cachedData?.lastRefreshed ?: Instant.ofEpochMilli(timeProvider.now())
            storeLastKnownGoodBounded(
                normalizedRegion = normalizedRegion,
                queue = queue,
                lastRefreshed = refreshedAt,
                leaderboard = enrichedCached
            )
            val isFresh = cachedData?.lastRefreshed != null &&
                Duration.between(cachedData.lastRefreshed, Instant.ofEpochMilli(timeProvider.now())).toHours() < CACHE_STALE_HOURS
            if (isFresh) {
                return enrichedCached
            }
            logger.info("[ChallengerLeaderboard] Stale cache hit for key: {}. Attempting guarded singleflight refresh.", cacheKey)
            return refreshChallengerLeagueSingleFlight(
                normalizedRegion = normalizedRegion,
                queue = queue,
                previousLeaderboard = enrichedCached
            ) ?: enrichedCached
        }

        if (cachedData != null) {
            logger.warn("[ChallengerLeaderboard] Malformed or empty Redis entry for key: {}. Self-healing via singleflight.", cacheKey)
        } else {
            logger.info("[ChallengerLeaderboard] Absent Redis entry for key: {}. Self-healing via singleflight.", cacheKey)
        }

        val fallbackBeforeRefresh = synchronized(lastKnownGoodLock) {
            lastKnownGoodByRegion[normalizedRegion]?.cachedDto?.leaderboard
        }
        val refreshed = refreshChallengerLeagueSingleFlight(
            normalizedRegion = normalizedRegion,
            queue = queue,
            previousLeaderboard = fallbackBeforeRefresh
        )
        if (refreshed != null) {
            return refreshed
        }

        return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, fallbackBeforeRefresh)
    }

    /**
     * Warms the Challenger leaderboard cache for [region] and [queue] using the same singleflight,
     * single-league-call, and local DB identity enrichment path.
     */
    suspend fun warmChallengerLeagueCache(region: String, queue: String) {
        val normalizedRegion = normalizeRegion(region)
        if (normalizedRegion !in SUPPORTED_REGIONS) {
            logger.warn("[Cache Warmer] Skipping unsupported region: {}", region)
            return
        }
        val cacheKey = getCacheKey(normalizedRegion, queue)
        val cachedData = runCatching {
            redisCacheService.get(cacheKey, CachedLeaderboardDto::class.java)
        }.getOrNull()

        val lastRefreshed = cachedData?.lastRefreshed
        if (isValidLeaderboard(cachedData?.leaderboard)) {
            val previousSnapshot = synchronized(lastKnownGoodLock) {
                lastKnownGoodByRegion[normalizedRegion]?.cachedDto?.leaderboard
            }
            val enrichedCached = enrichAndBoundLeaderboard(
                normalizedRegion = normalizedRegion,
                rawLeague = cachedData!!.leaderboard!!,
                previousSnapshot = previousSnapshot,
                onlyMissingDbIdentities = true,
                deadlineAtMs = timeProvider.now() + dbLookupTimeoutMs.coerceAtLeast(100L)
            )
            storeLastKnownGoodBounded(
                normalizedRegion = normalizedRegion,
                queue = queue,
                lastRefreshed = lastRefreshed ?: Instant.ofEpochMilli(timeProvider.now()),
                leaderboard = enrichedCached
            )
            if (lastRefreshed != null &&
                Duration.between(lastRefreshed, Instant.ofEpochMilli(timeProvider.now())).toHours() < CACHE_STALE_HOURS
            ) {
                logger.info("[Cache Warmer] HIT for key: {}. Data is fresh. No refresh needed.", cacheKey)
                return
            }
        }

        val logMessage = if (cachedData == null || !isValidLeaderboard(cachedData.leaderboard)) "MISS" else "STALE"
        logger.info("[Cache Warmer] {} for key: {}. Fetching from API...", logMessage, cacheKey)

        val previousLeaderboard = synchronized(lastKnownGoodLock) {
            lastKnownGoodByRegion[normalizedRegion]?.cachedDto?.leaderboard
        } ?: cachedData?.leaderboard
        refreshChallengerLeagueSingleFlight(
            normalizedRegion = normalizedRegion,
            queue = queue,
            previousLeaderboard = previousLeaderboard
        )
    }

    private suspend fun refreshChallengerLeagueSingleFlight(
        normalizedRegion: String,
        queue: String,
        previousLeaderboard: LeagueListDTO?
    ): LeagueListDTO? {
        val cacheKey = getCacheKey(normalizedRegion, queue)
        val now = timeProvider.now()
        val backoffUntil = failureBackoffUntilMs[cacheKey]
        if (backoffUntil != null && now < backoffUntil && !inFlightRefreshes.containsKey(cacheKey)) {
            logger.info(
                "[ChallengerLeaderboard] Skipping upstream refresh for {} during backoff ({}ms remaining)",
                cacheKey, backoffUntil - now
            )
            return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
        }

        var isLeader = false
        val deferred = singleFlightMutex.withLock {
            val existing = inFlightRefreshes[cacheKey]?.takeIf { !it.isCompleted }
            if (existing != null) {
                existing
            } else {
                val nowLocked = timeProvider.now()
                val lockedBackoffUntil = failureBackoffUntilMs[cacheKey]
                if (lockedBackoffUntil != null && nowLocked < lockedBackoffUntil) {
                    return@withLock null
                }
                val created = CompletableDeferred<LeagueListDTO?>()
                inFlightRefreshes[cacheKey] = created
                isLeader = true
                created
            }
        } ?: return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)

        val safeRefreshDeadlineMs = refreshDeadlineMs.coerceAtLeast(100L)
        if (isLeader) {
            refreshScope.launch {
                try {
                    val result = withTimeoutOrNull(safeRefreshDeadlineMs + 350L) {
                        executeBoundedLeaderRefresh(
                            normalizedRegion = normalizedRegion,
                            queue = queue,
                            cacheKey = cacheKey,
                            previousLeaderboard = previousLeaderboard
                        )
                    } ?: run {
                        if (lastRefreshFailureReasonByRegion[normalizedRegion] == null) {
                            recordFailureBackoff(cacheKey, normalizedRegion, "DEADLINE_EXCEEDED")
                        }
                        reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
                    }
                    inFlightRefreshes.remove(cacheKey, deferred)
                    deferred.complete(result)
                } catch (t: Throwable) {
                    recordFailureBackoff(cacheKey, normalizedRegion, "EXCEPTION")
                    val fallback = reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
                    inFlightRefreshes.remove(cacheKey, deferred)
                    deferred.complete(fallback)
                } finally {
                    inFlightRefreshes.remove(cacheKey, deferred)
                }
            }
        }

        val waitBudgetMs = safeRefreshDeadlineMs + 150L
        val awaited = withTimeoutOrNull(waitBudgetMs) {
            deferred.await()
        }
        if (awaited != null) {
            return awaited
        }
        // Do NOT remove `deferred` from `inFlightRefreshes` on waiter timeout while the leader job is still active;
        // keeping `deferred` registered prevents overlapping duplicate refreshes until the leader job completes and cleans up.
        return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
    }

    private fun reseedAndReturnLastKnownGood(
        cacheKey: String,
        normalizedRegion: String,
        previousLeaderboard: LeagueListDTO?
    ): LeagueListDTO? {
        val snapshot = synchronized(lastKnownGoodLock) {
            lastKnownGoodByRegion[normalizedRegion]
        }
        if (snapshot != null && isValidLeaderboard(snapshot.cachedDto.leaderboard)) {
            runCatching {
                redisCacheService.set(cacheKey, snapshot.cachedDto, Duration.ZERO)
            }
            return snapshot.cachedDto.leaderboard
        }
        return previousLeaderboard?.takeIf { isValidLeaderboard(it) }
    }

    private suspend fun executeBoundedLeaderRefresh(
        normalizedRegion: String,
        queue: String,
        cacheKey: String,
        previousLeaderboard: LeagueListDTO?
    ): LeagueListDTO? {
        if (!hasMemoryHeadroomForLeaderboardRefresh()) {
            recordFailureBackoff(cacheKey, normalizedRegion, "MEMORY_HEADROOM_STOP")
            logger.warn("[ChallengerLeaderboard] Blocked refresh for {} due to memory headroom guard.", cacheKey)
            return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
        }

        val deadlineAtMs = timeProvider.now() + refreshDeadlineMs.coerceAtLeast(100L)
        val httpAttempts = AtomicInteger(0)
        val failureReasonHolder = AtomicReference<String?>(null)

        val freshLeague = try {
            riotApiService.fetchChallengerLeagueBounded(
                region = normalizedRegion,
                queue = queue,
                priority = ApiPriority.LOW,
                maxRetries = 0,
                deadlineAtMs = deadlineAtMs,
                maxResponseBytes = maxResponseBytes,
                maxEntries = maxEntriesPerRegion.coerceIn(1, MAX_LEADERBOARD_ENTRIES),
                headroomGuard = ::hasMemoryHeadroomForLeaderboardRefresh,
                httpAttemptCounter = httpAttempts,
                failureReasonHolder = failureReasonHolder
            )
        } catch (e: Exception) {
            failureReasonHolder.compareAndSet(null, "UPSTREAM_EXCEPTION")
            logger.warn("[ChallengerLeaderboard] Upstream fetch failed for {}: {}", cacheKey, e.message)
            null
        }

        if (!isValidLeaderboard(freshLeague)) {
            val reason = failureReasonHolder.get() ?: if (freshLeague == null) "EMPTY_OR_NULL_RESPONSE" else "EMPTY_ENTRIES"
            recordFailureBackoff(cacheKey, normalizedRegion, reason)
            logger.warn(
                "[ChallengerLeaderboard] Refresh for {} did not return valid entries (reason={}, httpAttempts={}).",
                cacheKey, reason, httpAttempts.get()
            )
            return reseedAndReturnLastKnownGood(cacheKey, normalizedRegion, previousLeaderboard)
        }

        val fallbackSnapshot = previousLeaderboard ?: synchronized(lastKnownGoodLock) {
            lastKnownGoodByRegion[normalizedRegion]?.cachedDto?.leaderboard
        }
        val enriched = enrichAndBoundLeaderboard(
            normalizedRegion = normalizedRegion,
            rawLeague = freshLeague!!,
            previousSnapshot = fallbackSnapshot,
            onlyMissingDbIdentities = false,
            deadlineAtMs = deadlineAtMs
        )

        val refreshedAt = Instant.ofEpochMilli(timeProvider.now())
        storeLastKnownGoodBounded(
            normalizedRegion = normalizedRegion,
            queue = queue,
            lastRefreshed = refreshedAt,
            leaderboard = enriched
        )
        val fullDtoToCache = CachedLeaderboardDto(
            lastRefreshed = refreshedAt,
            leaderboard = enriched
        )

        failureBackoffUntilMs.remove(cacheKey)
        lastRefreshFailureReasonByRegion.remove(normalizedRegion)

        runCatching {
            redisCacheService.set(cacheKey, fullDtoToCache, Duration.ZERO)
            logger.info("[Cache Warmer] Populating cache with fresh data for key: {} (entries={})", cacheKey, enriched.entries.size)
        }.onFailure { e ->
            logger.warn("[ChallengerLeaderboard] Failed to write refreshed leaderboard to Redis for {}: {}", cacheKey, e.message)
        }

        return enriched
    }

    private fun recordFailureBackoff(cacheKey: String, normalizedRegion: String, reason: String) {
        val backoffMs = failureBackoffMs.coerceAtLeast(0L)
        if (backoffMs > 0L) {
            failureBackoffUntilMs[cacheKey] = timeProvider.now() + backoffMs
        }
        lastRefreshFailureReasonByRegion[normalizedRegion] = reason
    }

    /**
     * Enriches [rawLeague] entries with `(gameName, tagLine)` using ONLY:
     * 1. Local DB `indexed_players` rows via a single batched JDBC `PreparedStatement` (`<= 300` PUUIDs) bounded by
     *    `setQueryTimeout`, `setNetworkTimeout`, and active `Statement.cancel()` / `Connection.abort()` (or omitted if
     *    no bounded JDBC [DataSource] is available), and
     * 2. Previously cached / last-known-good leaderboard entries for the region.
     * Makes ZERO Riot Account-V1 API calls and records [IdentityCoverageStats] for observability.
     */
    private fun enrichAndBoundLeaderboard(
        normalizedRegion: String,
        rawLeague: LeagueListDTO,
        previousSnapshot: LeagueListDTO?,
        onlyMissingDbIdentities: Boolean = false,
        deadlineAtMs: Long? = null
    ): LeagueListDTO {
        val safeMaxEntries = maxEntriesPerRegion.coerceIn(1, MAX_LEADERBOARD_ENTRIES)
        val sortedBoundedEntries = rawLeague.entries
            .asSequence()
            .filter { !it.puuid.isNullOrBlank() }
            .sortedByDescending { it.leaguePoints ?: Int.MIN_VALUE }
            .take(safeMaxEntries)
            .toList()

        val previousIdentityByPuuid = HashMap<String, Pair<String, String>>()
        previousSnapshot?.entries?.forEach { prev ->
            val p = prev.puuid?.trim()?.takeIf { it.isNotBlank() } ?: return@forEach
            val gn = prev.gameName?.trim()?.takeIf { it.isNotBlank() }
            val tl = prev.tagLine?.trim()?.takeIf { it.isNotBlank() }
            if (gn != null && tl != null) {
                previousIdentityByPuuid[p] = gn to tl
            }
        }

        val candidatePuuidsForDb = if (onlyMissingDbIdentities) {
            sortedBoundedEntries
                .filter { it.gameName.isNullOrBlank() || it.tagLine.isNullOrBlank() }
                .mapNotNull { it.puuid?.trim()?.takeIf { id -> id.isNotBlank() } }
                .distinct()
        } else {
            sortedBoundedEntries
                .mapNotNull { it.puuid?.trim()?.takeIf { id -> id.isNotBlank() } }
                .distinct()
        }
        val ds = dataSource
        val dbIdentityByPuuid: Map<String, Pair<String, String>> = if (candidatePuuidsForDb.isNotEmpty() && ds != null) {
            fetchDbIdentitiesBounded(normalizedRegion, ds, candidatePuuidsForDb, deadlineAtMs)
        } else {
            emptyMap()
        }

        var resolvedCount = 0
        var dbResolvedCount = 0
        var snapshotResolvedCount = 0

        val enrichedEntries = sortedBoundedEntries.map { entry ->
            val puuid = entry.puuid?.trim()
            val dbIdentity = puuid?.let { dbIdentityByPuuid[it] }
            val prevIdentity = puuid?.let { previousIdentityByPuuid[it] }
            val existingGameName = entry.gameName?.trim()?.takeIf { it.isNotBlank() }
            val existingTagLine = entry.tagLine?.trim()?.takeIf { it.isNotBlank() }

            val resolvedGameName = dbIdentity?.first ?: existingGameName ?: prevIdentity?.first
            val resolvedTagLine = dbIdentity?.second ?: existingTagLine ?: prevIdentity?.second

            if (!resolvedGameName.isNullOrBlank() && !resolvedTagLine.isNullOrBlank()) {
                resolvedCount++
                if (dbIdentity != null) {
                    dbResolvedCount++
                } else if (existingGameName == null && prevIdentity != null) {
                    snapshotResolvedCount++
                }
            }

            LeagueEntryDto(
                puuid = puuid,
                leaguePoints = entry.leaguePoints,
                rank = entry.rank,
                wins = entry.wins,
                losses = entry.losses,
                veteran = entry.veteran,
                inactive = entry.inactive,
                freshBlood = entry.freshBlood,
                hotStreak = entry.hotStreak,
                leagueId = entry.leagueId,
                queueType = entry.queueType,
                tier = entry.tier,
                summonerName = entry.summonerName,
                gameName = resolvedGameName,
                tagLine = resolvedTagLine
            )
        }

        val missingCount = (enrichedEntries.size - resolvedCount).coerceAtLeast(0)
        val coverage = IdentityCoverageStats(
            totalEntries = enrichedEntries.size,
            resolvedIdentityCount = resolvedCount,
            missingIdentityCount = missingCount,
            dbResolvedCount = dbResolvedCount,
            snapshotResolvedCount = snapshotResolvedCount
        )
        lastIdentityCoverageByRegion[normalizedRegion] = coverage
        if (missingCount > 0) {
            logger.warn(
                "[ChallengerLeaderboard] Partial Riot ID coverage for {}: resolved={}/{} (missing={}, dbResolved={}, snapshotResolved={}). Rows without gameName/tagLine are unclickable in Dashboard until indexed.",
                normalizedRegion, resolvedCount, enrichedEntries.size, missingCount, dbResolvedCount, snapshotResolvedCount
            )
        } else {
            logger.info(
                "[ChallengerLeaderboard] Full Riot ID coverage for {}: resolved={}/{} (dbResolved={}, snapshotResolved={}).",
                normalizedRegion, resolvedCount, enrichedEntries.size, dbResolvedCount, snapshotResolvedCount
            )
        }

        return LeagueListDTO(
            tier = rawLeague.tier,
            leagueId = rawLeague.leagueId,
            queue = rawLeague.queue ?: DEFAULT_QUEUE,
            name = rawLeague.name,
            entries = enrichedEntries
        )
    }

    private fun fetchDbIdentitiesBounded(
        normalizedRegion: String,
        ds: DataSource,
        candidatePuuids: List<String>,
        deadlineAtMs: Long?
    ): Map<String, Pair<String, String>> {
        val safeDbTimeoutMs = dbLookupTimeoutMs.coerceAtLeast(50L)
        val remainingDeadlineMs = if (deadlineAtMs != null) {
            deadlineAtMs - timeProvider.now()
        } else {
            safeDbTimeoutMs
        }
        val effectiveTimeoutMs = minOf(safeDbTimeoutMs, remainingDeadlineMs)
        if (effectiveTimeoutMs <= 0L) {
            logger.warn("[ChallengerLeaderboard] Skipping DB identity lookup for {} because deadline already expired.", normalizedRegion)
            return emptyMap()
        }
        if (!dbLookupPermits.tryAcquire()) {
            logger.warn("[ChallengerLeaderboard] Skipping DB identity lookup for {} because all {} DB lookup permits are in use.", normalizedRegion, MAX_CONCURRENT_DB_LOOKUPS)
            return emptyMap()
        }

        val taskClaimedOrCancelled = java.util.concurrent.atomic.AtomicBoolean(false)
        val cancelled = java.util.concurrent.atomic.AtomicBoolean(false)
        val activeConnectionRef = AtomicReference<Connection?>(null)
        val activeStatementRef = AtomicReference<PreparedStatement?>(null)
        val workerThreadRef = AtomicReference<Thread?>(null)
        val workerDoneLatch = java.util.concurrent.CountDownLatch(1)

        val future = try {
            dbLookupExecutor.submit(java.util.concurrent.Callable<Map<String, Pair<String, String>>> {
                if (!taskClaimedOrCancelled.compareAndSet(false, true)) {
                    dbLookupPermits.release()
                    workerDoneLatch.countDown()
                    return@Callable emptyMap()
                }
                activeDbLookupWorkers.incrementAndGet()
                workerThreadRef.set(Thread.currentThread())
                try {
                    if (cancelled.get() || Thread.currentThread().isInterrupted) {
                        return@Callable emptyMap()
                    }
                    val queryTimeoutSeconds = ((effectiveTimeoutMs + 999L) / 1000L).toInt().coerceAtLeast(1)
                    val placeholders = candidatePuuids.joinToString(",") { "?" }
                    val sql = "SELECT puuid, game_name, tag_line FROM indexed_players WHERE puuid IN ($placeholders)"
                    val resolved = HashMap<String, Pair<String, String>>()

                    ds.connection.use { conn ->
                        activeConnectionRef.set(conn)
                        if (cancelled.get() || Thread.currentThread().isInterrupted) {
                            return@Callable emptyMap()
                        }
                        runCatching { conn.isReadOnly = true }
                        runCatching { conn.setNetworkTimeout(dbAbortExecutor, effectiveTimeoutMs.toInt().coerceAtLeast(1)) }
                        conn.prepareStatement(sql).use { stmt ->
                            activeStatementRef.set(stmt)
                            stmt.maxRows = candidatePuuids.size.coerceAtMost(MAX_LEADERBOARD_ENTRIES)
                            stmt.queryTimeout = queryTimeoutSeconds
                            for (i in candidatePuuids.indices) {
                                stmt.setString(i + 1, candidatePuuids[i])
                            }
                            if (cancelled.get() || Thread.currentThread().isInterrupted) {
                                return@Callable emptyMap()
                            }
                            stmt.executeQuery().use { rs ->
                                while (rs.next()) {
                                    if (cancelled.get() || Thread.currentThread().isInterrupted) {
                                        break
                                    }
                                    val p = rs.getString(1)?.trim().orEmpty()
                                    val gn = rs.getString(2)?.trim().orEmpty()
                                    val tl = rs.getString(3)?.trim().orEmpty()
                                    if (p.isNotBlank() && gn.isNotBlank() && tl.isNotBlank()) {
                                        resolved[p] = gn to tl
                                    }
                                }
                            }
                        }
                    }
                    resolved
                } finally {
                    activeStatementRef.set(null)
                    activeConnectionRef.set(null)
                    workerThreadRef.set(null)
                    activeDbLookupWorkers.decrementAndGet()
                    dbLookupPermits.release()
                    Thread.interrupted()
                    workerDoneLatch.countDown()
                }
            })
        } catch (e: java.util.concurrent.RejectedExecutionException) {
            dbLookupPermits.release()
            logger.warn("[ChallengerLeaderboard] DB lookup executor saturated for {}; continuing without DB enrichment.", normalizedRegion)
            return emptyMap()
        }

        return try {
            val rows = future.get(effectiveTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
            if (!taskClaimedOrCancelled.compareAndSet(false, true)) {
                workerDoneLatch.await(250L, java.util.concurrent.TimeUnit.MILLISECONDS)
            }
            rows
        } catch (e: java.util.concurrent.TimeoutException) {
            cancelled.set(true)
            val cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            if (cancelledBeforeStart) {
                dbLookupPermits.release()
            }
            runCatching { activeStatementRef.get()?.cancel() }
            runCatching { activeConnectionRef.get()?.abort(dbAbortExecutor) }
            runCatching { activeConnectionRef.get()?.close() }
            workerThreadRef.get()?.interrupt()
            future.cancel(true)
            if (!cancelledBeforeStart) {
                workerDoneLatch.await(250L, java.util.concurrent.TimeUnit.MILLISECONDS)
            }
            logger.warn(
                "[ChallengerLeaderboard] Local DB identity lookup timed out after {}ms for {} (JDBC Statement.cancel invoked); continuing with snapshot identities.",
                effectiveTimeoutMs, normalizedRegion
            )
            emptyMap()
        } catch (e: Exception) {
            cancelled.set(true)
            val cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            if (cancelledBeforeStart) {
                dbLookupPermits.release()
            }
            runCatching { activeStatementRef.get()?.cancel() }
            runCatching { activeConnectionRef.get()?.abort(dbAbortExecutor) }
            runCatching { activeConnectionRef.get()?.close() }
            workerThreadRef.get()?.interrupt()
            future.cancel(true)
            if (!cancelledBeforeStart) {
                workerDoneLatch.await(250L, java.util.concurrent.TimeUnit.MILLISECONDS)
            }
            logger.warn("[ChallengerLeaderboard] Local DB identity lookup failed for {}: {}", normalizedRegion, e.message)
            emptyMap()
        }
    }

    /**
     * Stores [leaderboard] in [lastKnownGoodByRegion] while atomically enforcing:
     * 1. Region allowlist (`euw1`, `na1`, `kr` — max 3 regions),
     * 2. Entry count cap (`<= maxEntriesPerRegion`, max 300),
     * 3. Per-region serialized UTF-8 byte cap (`<= maxLastKnownGoodRegionBytes`), and
     * 4. Total 3-region serialized UTF-8 byte cap (`<= maxLastKnownGoodTotalBytes`).
     * Retains ONLY a complete snapshot that fits within the byte caps; rejects an oversized whole
     * without storing partial rank lists (so an absent/rejected snapshot yields HTTP 503 on upstream failure).
     */
    private fun storeLastKnownGoodBounded(
        normalizedRegion: String,
        queue: String,
        lastRefreshed: Instant,
        leaderboard: LeagueListDTO
    ): BoundedLeaderboardSnapshot? {
        if (normalizedRegion !in SUPPORTED_REGIONS) return null
        synchronized(lastKnownGoodLock) {
            val safeRegionByteCap = maxLastKnownGoodRegionBytes.coerceIn(1_024, MAX_LAST_KNOWN_GOOD_REGION_BYTES)
            val safeTotalByteCap = maxLastKnownGoodTotalBytes.coerceIn(safeRegionByteCap, MAX_LAST_KNOWN_GOOD_TOTAL_BYTES)
            val otherRegionsBytes = lastKnownGoodByRegion.entries
                .filter { it.key != normalizedRegion }
                .sumOf { it.value.serializedBytes }
            val remainingTotalBudget = (safeTotalByteCap - otherRegionsBytes).coerceAtLeast(0)
            val effectiveByteBudget = minOf(safeRegionByteCap, remainingTotalBudget)
            if (effectiveByteBudget <= 0) {
                lastKnownGoodByRegion.remove(normalizedRegion)
                return null
            }

            val fullBoundedEntries = leaderboard.entries.take(maxEntriesPerRegion.coerceIn(1, MAX_LEADERBOARD_ENTRIES))
            if (fullBoundedEntries.isEmpty()) {
                lastKnownGoodByRegion.remove(normalizedRegion)
                return null
            }

            val candidateDto = CachedLeaderboardDto(
                lastRefreshed = lastRefreshed,
                leaderboard = leaderboard.copy(entries = fullBoundedEntries)
            )
            val serializedBytes = objectMapper.writeValueAsString(candidateDto)
                .toByteArray(StandardCharsets.UTF_8)
                .size
            if (serializedBytes <= effectiveByteBudget) {
                val snapshot = BoundedLeaderboardSnapshot(
                    region = normalizedRegion,
                    queue = queue,
                    cachedDto = candidateDto,
                    serializedBytes = serializedBytes
                )
                lastKnownGoodByRegion[normalizedRegion] = snapshot
                return snapshot
            }

            logger.warn(
                "[ChallengerLeaderboard] Rejecting oversized last-known-good snapshot for {} (serializedBytes={} > effectiveByteBudget={}, entries={}); no partial rank fallback stored.",
                normalizedRegion, serializedBytes, effectiveByteBudget, fullBoundedEntries.size
            )
            lastKnownGoodByRegion.remove(normalizedRegion)
            return null
        }
    }
}
