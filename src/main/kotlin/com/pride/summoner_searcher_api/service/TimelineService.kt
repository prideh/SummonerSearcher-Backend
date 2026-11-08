package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.CoachTimelinePreparationResultDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.MissingMatchTimelineDto
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Service responsible for fetching, caching, and bounded on-demand preparation of match timelines.
 * Uses Redis with a 24-hour TTL for each raw timeline to minimize Riot API calls.
 */
@Service
class TimelineService @Autowired constructor(
    private val riotApiService: RiotApiService,
    private val redisCacheService: RedisCacheService,
    private val timelineAnalysisService: TimelineAnalysisService = TimelineAnalysisService(),
    @Autowired(required = false)
    private val rateLimiter: RiotApiRateLimiter? = null,
    private val timeProvider: TimeProvider = SystemTimeProvider(),
    private val memoryStatsProvider: MemoryStatsProvider = SystemMemoryStatsProvider(),
    @Value("\${cohort.collector.container-memory-limit-bytes:536870912}")
    private val configuredContainerLimitBytes: Long = 536_870_912L,
    @Value("\${cohort.collector.max-memory-utilization-ratio:0.78}")
    private val maxMemoryUtilizationRatio: Double = 0.78,
    @Value("\${cohort.collector.min-free-memory-bytes:67108864}")
    private val minFreeMemoryBytes: Long = 67_108_864L,
    @Value("\${coach.timeline.admission-max-used-bytes:367001600}")
    private val coachAdmissionMaxUsedBytes: Long = COACH_PREP_ADMISSION_MAX_BYTES,
    @Value("\${coach.timeline.stop-max-used-bytes:377487360}")
    private val coachStopMaxUsedBytes: Long = COACH_PREP_STOP_MAX_BYTES,
    @Value("\${coach.timeline.prep-deadline-ms:12000}")
    private val prepDeadlineMs: Long = DEFAULT_PREP_DEADLINE_MS,
    @Value("\${coach.timeline.max-concurrent-preparations:2}")
    private val maxConcurrentPreparations: Int = DEFAULT_MAX_CONCURRENT_PREPARATIONS,
    @Value("\${coach.timeline.prepared-cache-max-entries:32}")
    private val preparedCacheMaxEntries: Int = DEFAULT_PREPARED_CACHE_MAX_ENTRIES,
    @Value("\${coach.timeline.prepared-cache-ttl-ms:900000}")
    private val preparedCacheTtlMs: Long = DEFAULT_PREPARED_CACHE_TTL_MS
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val MAX_COACH_TIMELINE_MATCHES = 20
        const val COACH_TIMELINE_ANALYZER_VERSION = "v2-compact-fold"
        const val COACH_PREP_ADMISSION_MAX_BYTES = 350L * 1024L * 1024L // 367,001,600 bytes (350 MiB)
        const val COACH_PREP_STOP_MAX_BYTES = 360L * 1024L * 1024L      // 377,487,360 bytes (360 MiB)
        const val DEFAULT_PREP_DEADLINE_MS = 12_000L
        const val DEFAULT_MAX_CONCURRENT_PREPARATIONS = 2
        const val DEFAULT_PREPARED_CACHE_MAX_ENTRIES = 32
        const val DEFAULT_PREPARED_CACHE_TTL_MS = 15L * 60L * 1000L // 15 minutes
    }

    private data class CachedPreparationEntry(
        val playerScopeKey: String,
        val expiresAtMs: Long,
        val result: CoachTimelinePreparationResultDto
    )

    private val singleflightMutex = Mutex()
    private val inFlightPreparations = ConcurrentHashMap<String, CompletableDeferred<CoachTimelinePreparationResultDto>>()
    private val activeGlobalPreparations = AtomicInteger(0)

    private val preparedCacheMutex = Mutex()
    private val preparedCache = object : LinkedHashMap<String, CachedPreparationEntry>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, CachedPreparationEntry>?): Boolean {
            return size > preparedCacheMaxEntries.coerceAtLeast(1)
        }
    }

    private fun getCacheKey(matchId: String, region: String) =
        "timeline:${region.lowercase()}:$matchId"

    fun buildPreparationCacheKey(
        region: String,
        puuid: String,
        orderedMatchIds: List<String>,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION
    ): String =
        "coach_prep:${region.lowercase()}:${puuid}:${orderedMatchIds.joinToString(",")}:$analyzerVersion"

    fun hasCoachAdmissionHeadroom(): Boolean = evaluateMemoryHeadroom(coachAdmissionMaxUsedBytes)

    fun buildAdmissionBlockedResult(
        region: String,
        puuid: String,
        requestedMatchIds: List<String>,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION
    ): CoachTimelinePreparationResultDto {
        val normalizedRegion = region.lowercase()
        val orderedMatchIds = requestedMatchIds
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()
            .take(MAX_COACH_TIMELINE_MATCHES)
        return CoachTimelinePreparationResultDto(
            status = "BLOCKED",
            region = normalizedRegion,
            puuid = puuid,
            analyzerVersion = analyzerVersion,
            requestedMatchIds = orderedMatchIds,
            coveredMatchIds = emptyList(),
            missingMatches = orderedMatchIds.map {
                MissingMatchTimelineDto(it, "MEMORY_HEADROOM_ADMISSION_BLOCKED")
            },
            stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
            cacheHit = false,
            peakRetainedRawTimelines = 0,
            analysis = null
        )
    }

    internal fun evaluateMemoryHeadroom(maxUsedBytesThreshold: Long): Boolean {
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
            if (effectiveUsed >= maxUsedBytesThreshold) {
                return false
            }
            val freeBytes = (effectiveLimit - effectiveUsed).coerceAtLeast(0L)
            val ratio = effectiveUsed.toDouble() / effectiveLimit.toDouble()
            ratio.isFinite() && ratio < maxMemoryUtilizationRatio && freeBytes >= minFreeMemoryBytes
        } catch (e: Exception) {
            logger.warn("Memory headroom sample failed during coach timeline preparation guard; failing safe (false): {}", e.message)
            false
        }
    }

    private suspend fun getCachedPreparation(cacheKey: String): CoachTimelinePreparationResultDto? {
        val now = timeProvider.now()
        return preparedCacheMutex.withLock {
            val entry = preparedCache[cacheKey] ?: return@withLock null
            if (now >= entry.expiresAtMs) {
                preparedCache.remove(cacheKey)
                return@withLock null
            }
            entry.result.copy(cacheHit = true)
        }
    }

    private suspend fun putCachedPreparation(
        cacheKey: String,
        playerScopeKey: String,
        result: CoachTimelinePreparationResultDto
    ) {
        val now = timeProvider.now()
        preparedCacheMutex.withLock {
            // Invalidate any stale entries for the same (region, puuid) with older match ID lists or analyzer versions
            val staleKeys = preparedCache.entries
                .filter { (k, v) -> k != cacheKey && (v.playerScopeKey == playerScopeKey || now >= v.expiresAtMs) }
                .map { it.key }
            staleKeys.forEach { preparedCache.remove(it) }

            preparedCache[cacheKey] = CachedPreparationEntry(
                playerScopeKey = playerScopeKey,
                expiresAtMs = now + preparedCacheTtlMs.coerceAtLeast(1_000L),
                result = result.copy(cacheHit = false)
            )
        }
    }

    /**
     * Bounded on-demand preparation of at most [MAX_COACH_TIMELINE_MATCHES] match timelines for AI Coach:
     *  - Coalesces concurrent requests for the same `(region, puuid, orderedMatchIds, analyzerVersion)` via singleflight.
     *  - Caps global cross-player concurrent preparations at [maxConcurrentPreparations].
     *  - Serves repeat/reopen requests from bounded in-memory LRU cache with zero Riot network calls.
     *  - Enforces 350 MiB admission headroom before starting and 360 MiB stop headroom before each match decode/fetch.
     *  - Fetches at most ONE timeline at a time sequentially (Redis 24h cache first, then zero-retry Riot fetch),
     *    compacting each timeline immediately and releasing the raw [MatchTimelineDto] before the next step.
     */
    suspend fun prepareCoachTimelines(
        region: String,
        puuid: String,
        requestedMatchIds: List<String>,
        matchDtos: List<MatchDto>,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION
    ): CoachTimelinePreparationResultDto {
        val normalizedRegion = region.lowercase()
        val orderedMatchIds = requestedMatchIds
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()
            .take(MAX_COACH_TIMELINE_MATCHES)

        if (orderedMatchIds.isEmpty()) {
            return CoachTimelinePreparationResultDto(
                status = "BLOCKED",
                region = normalizedRegion,
                puuid = puuid,
                analyzerVersion = analyzerVersion,
                requestedMatchIds = emptyList(),
                coveredMatchIds = emptyList(),
                missingMatches = emptyList(),
                stopReason = "NO_MATCHES_AVAILABLE",
                cacheHit = false,
                peakRetainedRawTimelines = 0,
                analysis = null
            )
        }

        val playerScopeKey = "$normalizedRegion:$puuid"
        val cacheKey = buildPreparationCacheKey(normalizedRegion, puuid, orderedMatchIds, analyzerVersion)

        getCachedPreparation(cacheKey)?.let { cachedResult ->
            return cachedResult
        }

        var isOwner = false
        val deferred = singleflightMutex.withLock {
            getCachedPreparation(cacheKey)?.let { cachedAfterLock ->
                return cachedAfterLock
            }
            val existing = inFlightPreparations[cacheKey]
            if (existing != null) {
                existing
            } else {
                isOwner = true
                CompletableDeferred<CoachTimelinePreparationResultDto>().also {
                    inFlightPreparations[cacheKey] = it
                }
            }
        }

        if (!isOwner) {
            return deferred.await()
        }

        try {
            val result = executeBoundedCoachPreparation(
                normalizedRegion = normalizedRegion,
                puuid = puuid,
                orderedMatchIds = orderedMatchIds,
                matchDtos = matchDtos,
                analyzerVersion = analyzerVersion,
                cacheKey = cacheKey,
                playerScopeKey = playerScopeKey
            )
            deferred.complete(result)
            return result
        } catch (t: Throwable) {
            deferred.completeExceptionally(t)
            throw t
        } finally {
            singleflightMutex.withLock {
                inFlightPreparations.remove(cacheKey, deferred)
            }
        }
    }

    private suspend fun executeBoundedCoachPreparation(
        normalizedRegion: String,
        puuid: String,
        orderedMatchIds: List<String>,
        matchDtos: List<MatchDto>,
        analyzerVersion: String,
        cacheKey: String,
        playerScopeKey: String
    ): CoachTimelinePreparationResultDto {
        val safeMaxConcurrent = maxConcurrentPreparations.coerceAtLeast(1)
        val acquiredSlot = tryAcquireGlobalSlot(safeMaxConcurrent)
        if (!acquiredSlot) {
            return CoachTimelinePreparationResultDto(
                status = "BLOCKED",
                region = normalizedRegion,
                puuid = puuid,
                analyzerVersion = analyzerVersion,
                requestedMatchIds = orderedMatchIds,
                coveredMatchIds = emptyList(),
                missingMatches = orderedMatchIds.map { MissingMatchTimelineDto(it, "CONCURRENCY_LIMIT_REACHED") },
                stopReason = "CONCURRENCY_LIMIT_REACHED",
                cacheHit = false,
                peakRetainedRawTimelines = 0,
                analysis = null
            )
        }

        try {
            val startTimeMs = timeProvider.now()
            val deadlineMs = prepDeadlineMs.coerceAtLeast(100L)

            // 1. Strict 350 MiB admission headroom guard before starting preparation
            if (!evaluateMemoryHeadroom(coachAdmissionMaxUsedBytes)) {
                logger.warn(
                    "Coach timeline preparation blocked at admission (350 MiB guard) for puuid={} region={}",
                    puuid,
                    normalizedRegion
                )
                return CoachTimelinePreparationResultDto(
                    status = "BLOCKED",
                    region = normalizedRegion,
                    puuid = puuid,
                    analyzerVersion = analyzerVersion,
                    requestedMatchIds = orderedMatchIds,
                    coveredMatchIds = emptyList(),
                    missingMatches = orderedMatchIds.map {
                        MissingMatchTimelineDto(it, "MEMORY_HEADROOM_ADMISSION_BLOCKED")
                    },
                    stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                    cacheHit = false,
                    peakRetainedRawTimelines = 0,
                    analysis = null
                )
            }

            val matchDtosById = matchDtos.mapNotNull { dto ->
                val mid = dto.metadata?.matchId ?: return@mapNotNull null
                mid to dto
            }.toMap()
            val orderedMatchDtos = orderedMatchIds.mapNotNull { matchDtosById[it] }

            val compactedTriples = mutableListOf<Triple<String, MatchTimelineDto, MatchDto>>()
            val coveredMatchIds = mutableListOf<String>()
            val missingMatches = mutableListOf<MissingMatchTimelineDto>()
            var stopReason: String? = null
            val liveRawTimelines = AtomicInteger(0)
            val peakRetainedRawTimelines = AtomicInteger(0)

            for ((index, matchId) in orderedMatchIds.withIndex()) {
                val matchDto = matchDtosById[matchId]
                if (matchDto == null) {
                    missingMatches.add(MissingMatchTimelineDto(matchId, "MISSING_MATCH_DTO"))
                    continue
                }

                // Check deadline before each match decode/fetch
                val elapsedMs = timeProvider.now() - startTimeMs
                if (elapsedMs >= deadlineMs) {
                    stopReason = "DEADLINE_EXCEEDED"
                    for (remainingId in orderedMatchIds.subList(index, orderedMatchIds.size)) {
                        missingMatches.add(MissingMatchTimelineDto(remainingId, "DEADLINE_EXCEEDED"))
                    }
                    break
                }

                // Check 360 MiB stop headroom before each Redis decode / Riot fetch
                if (!evaluateMemoryHeadroom(coachStopMaxUsedBytes)) {
                    stopReason = "MEMORY_HEADROOM_STOP"
                    for (remainingId in orderedMatchIds.subList(index, orderedMatchIds.size)) {
                        missingMatches.add(MissingMatchTimelineDto(remainingId, "MEMORY_HEADROOM_STOP"))
                    }
                    break
                }

                val rawCacheKey = getCacheKey(matchId, normalizedRegion)
                val cachedOutcome = readAndCompactCachedTimeline(
                    rawCacheKey = rawCacheKey,
                    matchDto = matchDto,
                    puuid = puuid,
                    liveRawTimelines = liveRawTimelines,
                    peakRetainedRawTimelines = peakRetainedRawTimelines
                )
                val hadCachedRaw = cachedOutcome.hadCachedRaw
                val legacyNeedsBackfill = cachedOutcome.legacyNeedsBackfill
                var compactedTimeline: MatchTimelineDto? = cachedOutcome.compactedTimeline

                var fetchStopReason: String? = null
                var itemMissingReason: String? = null

                if (!hadCachedRaw || legacyNeedsBackfill) {
                    val activeCooldownMs = rateLimiter?.getActiveRetryAfterWaitMs(normalizedRegion) ?: 0L
                    if (activeCooldownMs > 0L) {
                        if (compactedTimeline == null) {
                            fetchStopReason = "RATE_LIMIT_COOLDOWN"
                        }
                    } else {
                        val remainingBudgetMs = deadlineMs - (timeProvider.now() - startTimeMs)
                        if (remainingBudgetMs <= 0L) {
                            fetchStopReason = "DEADLINE_EXCEEDED"
                        } else if (!evaluateMemoryHeadroom(coachStopMaxUsedBytes)) {
                            fetchStopReason = "MEMORY_HEADROOM_STOP"
                        } else {
                            val failureReason = AtomicReference<String?>(null)
                            val fetched: MatchTimelineDto? = riotApiService.getMatchTimelineBounded(
                                matchId = matchId,
                                region = normalizedRegion,
                                priority = ApiPriority.HIGH,
                                maxRetries = 0,
                                failureReasonHolder = failureReason,
                                maxTotalDurationMs = remainingBudgetMs,
                                headroomGuard = { evaluateMemoryHeadroom(coachStopMaxUsedBytes) }
                            )
                            if (fetched != null) {
                                val postFetchElapsedMs = timeProvider.now() - startTimeMs
                                if (postFetchElapsedMs >= deadlineMs) {
                                    fetchStopReason = "DEADLINE_EXCEEDED"
                                } else {
                                    val compactedReplacement = cacheAndCompactFetchedTimeline(
                                        rawCacheKey = rawCacheKey,
                                        fetched = fetched,
                                        matchDto = matchDto,
                                        puuid = puuid,
                                        liveRawTimelines = liveRawTimelines,
                                        peakRetainedRawTimelines = peakRetainedRawTimelines
                                    )
                                    if (compactedReplacement != null) {
                                        compactedTimeline = compactedReplacement
                                    } else if (compactedTimeline == null) {
                                        itemMissingReason = "TIMELINE_UNAVAILABLE"
                                    }
                                }
                            } else {
                                when (failureReason.get()) {
                                    "DEADLINE_EXCEEDED" -> fetchStopReason = "DEADLINE_EXCEEDED"
                                    "MEMORY_HEADROOM_STOP" -> fetchStopReason = "MEMORY_HEADROOM_STOP"
                                    "HTTP_429_TOO_MANY_REQUESTS" -> fetchStopReason = "RATE_LIMIT_429"
                                    "RATE_LIMIT_COOLDOWN" -> fetchStopReason = "RATE_LIMIT_COOLDOWN"
                                    "NETWORK_TIMEOUT" -> fetchStopReason = "NETWORK_TIMEOUT"
                                    "HTTP_404_NOT_FOUND" -> {
                                        if (compactedTimeline == null) {
                                            itemMissingReason = "TIMELINE_NOT_FOUND"
                                        }
                                    }
                                    else -> {
                                        if (compactedTimeline == null) {
                                            itemMissingReason = "TIMELINE_UNAVAILABLE"
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if (compactedTimeline == null) {
                    itemMissingReason = "TIMELINE_UNAVAILABLE"
                }

                if (fetchStopReason != null) {
                    compactedTimeline = null
                    stopReason = fetchStopReason
                    val currentReason = fetchStopReason
                    val subsequentReason = if (fetchStopReason == "RATE_LIMIT_429") "RATE_LIMIT_COOLDOWN" else fetchStopReason
                    missingMatches.add(MissingMatchTimelineDto(matchId, currentReason))
                    for (remainingId in orderedMatchIds.subList(index + 1, orderedMatchIds.size)) {
                        missingMatches.add(MissingMatchTimelineDto(remainingId, subsequentReason))
                    }
                    break
                }

                if (compactedTimeline == null) {
                    missingMatches.add(MissingMatchTimelineDto(matchId, itemMissingReason ?: "TIMELINE_UNAVAILABLE"))
                    continue
                }

                compactedTriples.add(Triple(matchId, compactedTimeline, matchDto))
                coveredMatchIds.add(matchId)
            }

            if (coveredMatchIds.isEmpty() && stopReason == null) {
                stopReason = missingMatches.firstOrNull()?.reason ?: "TIMELINE_UNAVAILABLE"
            }

            val status = when {
                coveredMatchIds.isEmpty() -> "BLOCKED"
                coveredMatchIds.size == orderedMatchIds.size && missingMatches.isEmpty() -> "COMPLETE"
                else -> "PARTIAL"
            }

            val analysis = if (compactedTriples.isNotEmpty()) {
                timelineAnalysisService.analyze(compactedTriples, puuid, orderedMatchDtos)
            } else {
                null
            }

            val finalResult = CoachTimelinePreparationResultDto(
                status = status,
                region = normalizedRegion,
                puuid = puuid,
                analyzerVersion = analyzerVersion,
                requestedMatchIds = orderedMatchIds,
                coveredMatchIds = coveredMatchIds,
                missingMatches = missingMatches,
                stopReason = stopReason,
                cacheHit = false,
                peakRetainedRawTimelines = peakRetainedRawTimelines.get(),
                analysis = analysis
            )

            // Cache COMPLETE results and non-transient PARTIAL results (e.g., permanent 404 on a match)
            if (status == "COMPLETE" || (status == "PARTIAL" && stopReason == null)) {
                putCachedPreparation(cacheKey, playerScopeKey, finalResult)
            }

            return finalResult
        } finally {
            activeGlobalPreparations.decrementAndGet()
        }
    }

    private data class CachedTimelineCompactionOutcome(
        val hadCachedRaw: Boolean,
        val legacyNeedsBackfill: Boolean,
        val compactedTimeline: MatchTimelineDto?
    )

    private fun readAndCompactCachedTimeline(
        rawCacheKey: String,
        matchDto: MatchDto,
        puuid: String,
        liveRawTimelines: AtomicInteger,
        peakRetainedRawTimelines: AtomicInteger
    ): CachedTimelineCompactionOutcome {
        val rawTimeline = redisCacheService.get(rawCacheKey, MatchTimelineDto::class.java)
            ?: return CachedTimelineCompactionOutcome(
                hadCachedRaw = false,
                legacyNeedsBackfill = false,
                compactedTimeline = null
            )
        val cur = liveRawTimelines.incrementAndGet()
        peakRetainedRawTimelines.accumulateAndGet(cur, ::maxOf)
        return try {
            val legacy = needsTimelineBackfill(rawTimeline)
            val compacted = timelineAnalysisService.compactTimelineForPlayer(rawTimeline, matchDto, puuid)
            CachedTimelineCompactionOutcome(
                hadCachedRaw = true,
                legacyNeedsBackfill = legacy,
                compactedTimeline = compacted
            )
        } finally {
            liveRawTimelines.decrementAndGet()
        }
    }

    private fun cacheAndCompactFetchedTimeline(
        rawCacheKey: String,
        fetched: MatchTimelineDto,
        matchDto: MatchDto,
        puuid: String,
        liveRawTimelines: AtomicInteger,
        peakRetainedRawTimelines: AtomicInteger
    ): MatchTimelineDto? {
        val cur = liveRawTimelines.incrementAndGet()
        peakRetainedRawTimelines.accumulateAndGet(cur, ::maxOf)
        return try {
            val compacted = timelineAnalysisService.compactTimelineForPlayer(fetched, matchDto, puuid)
            if (compacted != null) {
                redisCacheService.set(rawCacheKey, fetched, Duration.ofHours(24))
            }
            compacted
        } finally {
            liveRawTimelines.decrementAndGet()
        }
    }

    private fun tryAcquireGlobalSlot(maxConcurrent: Int): Boolean {
        while (true) {
            val current = activeGlobalPreparations.get()
            if (current >= maxConcurrent) {
                return false
            }
            if (activeGlobalPreparations.compareAndSet(current, current + 1)) {
                return true
            }
        }
    }

    /**
     * Fetches a single match timeline, using Redis cache with 24h TTL.
     * Lazily backfills cached timelines that predate CHAMPION_KILL bounty/shutdownBounty fields.
     */
    suspend fun getTimeline(matchId: String, region: String): MatchTimelineDto? {
        val cacheKey = getCacheKey(matchId, region)

        val cached = redisCacheService.get(cacheKey, MatchTimelineDto::class.java)
        if (cached != null && !needsTimelineBackfill(cached)) {
            logger.info("Timeline Cache HIT: {}", cacheKey)
            return cached
        }

        if (cached != null) {
            logger.info("Timeline Cache LEGACY (missing bounty fields): {} — lazily backfilling from Riot API", cacheKey)
        } else {
            logger.info("Timeline Cache MISS: {} — fetching from Riot API", cacheKey)
        }
        val timeline = riotApiService.getMatchTimeline(matchId, region)

        if (timeline != null) {
            redisCacheService.set(cacheKey, timeline, Duration.ofHours(24))
            return timeline
        }

        return cached
    }

    private fun needsTimelineBackfill(timeline: MatchTimelineDto): Boolean {
        val frames = timeline.info?.frames?.takeIf { it.isNotEmpty() } ?: return false
        val killEvents = frames.flatMap { it.events ?: emptyList() }.filter { it.type == "CHAMPION_KILL" }
        return killEvents.isNotEmpty() && killEvents.all { it.bounty == null && it.shutdownBounty == null }
    }

    /**
     * Fetches timelines for the given match IDs (up to [limit]) concurrently.
     * Each timeline is individually cached.
     */
    suspend fun getTimelines(matchIds: List<String>, region: String, limit: Int = 20): List<MatchTimelineDto> =
        coroutineScope {
            matchIds.take(limit)
                .map { matchId ->
                    async { getTimeline(matchId, region) }
                }
                .awaitAll()
                .filterNotNull()
        }
}
