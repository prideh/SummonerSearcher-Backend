package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * A service for managing the caching of player profiles.
 * This service acts as an intelligent layer between the frontend request and the Riot API,
 * using Redis to store and retrieve player data to minimize API calls.
 */
@Service
class PlayerCacheService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService,
    private val indexedPlayerRepository: com.pride.summoner_searcher_api.repository.IndexedPlayerRepository,
    private val timeProvider: TimeProvider = SystemTimeProvider(),
    @org.springframework.beans.factory.annotation.Value("\${player.cache.lock-poll-interval-ms:250}")
    private val lockPollIntervalMs: Long = LOCK_POLL_INTERVAL_MS,
    @org.springframework.beans.factory.annotation.Value("\${player.cache.lock-max-poll-attempts:240}")
    private val lockMaxPollAttempts: Int = LOCK_MAX_POLL_ATTEMPTS,
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private val playerSeasonStatsRepository: com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository? = null,
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private val transactionTemplate: org.springframework.transaction.support.TransactionTemplate? = null,
    private val objectMapper: com.fasterxml.jackson.databind.ObjectMapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules(),
    private val memoryStatsProvider: MemoryStatsProvider = SystemMemoryStatsProvider(),
    @org.springframework.beans.factory.annotation.Value("\${cohort.collector.container-memory-limit-bytes:536870912}")
    private val configuredContainerLimitBytes: Long = 536_870_912L,
    @org.springframework.beans.factory.annotation.Value("\${cohort.collector.max-memory-utilization-ratio:0.78}")
    private val maxMemoryUtilizationRatio: Double = 0.78,
    @org.springframework.beans.factory.annotation.Value("\${cohort.collector.min-free-memory-bytes:67108864}")
    private val minFreeMemoryBytes: Long = 67_108_864L
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val timelineAnalysisService = TimelineAnalysisService()
    private val inFlightMutex = Mutex()
    private val inFlightRefreshes = ConcurrentHashMap<String, CompletableDeferred<SummonerProfileDto?>>()
    private val lastBackgroundRefreshAtMs = ConcurrentHashMap<String, Long>()
    private val backgroundRefreshScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val indexedPlayersUpsertedCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val indexedPlayersStaleSkippedCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val seasonStatsDbSavesCounter = java.util.concurrent.atomic.AtomicLong(0L)

    companion object {
        private const val RECENT_MATCHES_LIMIT = 20
        private const val CHUNK_SIZE = 50
        private const val MAX_MATCHES_FOR_PARTICIPANT_INDEXING = 20
        private const val MAX_INDEXED_PLAYERS_PER_BATCH = 200
        private const val MAX_OPTIMISTIC_LOCK_RETRIES = 2
        private val PROFILE_CACHE_TTL = Duration.ofDays(14)
        private val MATCH_CACHE_TTL = Duration.ofDays(60)
        val PROFILE_FRESHNESS_WINDOW: Duration = Duration.ofMinutes(5)
        val EXPLICIT_REFRESH_MIN_COOLDOWN: Duration = Duration.ofSeconds(60)
        val PROFILE_LOCK_TTL: Duration = Duration.ofSeconds(60)
        const val LOCK_POLL_INTERVAL_MS = 250L
        const val LOCK_MAX_POLL_ATTEMPTS = 240 // Full 60s wait matching PROFILE_LOCK_TTL (240 * 250ms = 60s)
    }

    fun getDiscoveryAndSeasonWriteTelemetry(): Map<String, Long> = mapOf(
        "indexedPlayersUpserted" to indexedPlayersUpsertedCounter.get(),
        "indexedPlayersStaleSkipped" to indexedPlayersStaleSkippedCounter.get(),
        "seasonStatsDbSaves" to seasonStatsDbSavesCounter.get()
    )

    internal fun hasMemoryHeadroomForSeasonBackfill(): Boolean {
        return try {
            val raw = memoryStatsProvider.sample()
            val safeContainerCap = configuredContainerLimitBytes.coerceAtLeast(1L)
            val safeHeapMax = raw.jvmHeapMaxBytes.coerceAtLeast(1L)
            val safeHeapUsed = raw.jvmHeapUsedBytes.coerceAtLeast(0L)
            val isBoundedContainerCgroup = raw.cgroupMemoryMaxBytes != null &&
                raw.cgroupMemoryMaxBytes in 1L..2_147_483_648L &&
                raw.cgroupMemoryCurrentBytes != null &&
                raw.cgroupMemoryCurrentBytes > 0L
            val effectiveLimit = if (isBoundedContainerCgroup) {
                minOf(safeContainerCap, raw.cgroupMemoryMaxBytes!!)
            } else {
                safeHeapMax
            }
            val effectiveUsed = if (isBoundedContainerCgroup) {
                maxOf(raw.cgroupMemoryCurrentBytes!!, safeHeapUsed)
            } else {
                safeHeapUsed
            }
            val freeBytes = (effectiveLimit - effectiveUsed).coerceAtLeast(0L)
            val ratio = effectiveUsed.toDouble() / effectiveLimit.toDouble()
            ratio < maxMemoryUtilizationRatio && freeBytes >= minFreeMemoryBytes
        } catch (_: Exception) {
            true
        }
    }

    private data class LoadedDurableSeasonState(
        val entityVersion: Long,
        val state: DurablePlayerSeasonStatsDto
    )

    internal fun isValidDurableSeasonState(
        dto: DurablePlayerSeasonStatsDto?,
        expectedRegion: String,
        expectedPuuid: String,
        expectedSeasonYear: Int
    ): Boolean {
        if (dto == null) return false
        if (dto.schemaVersion != DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION) return false
        if (!dto.region.equals(expectedRegion, ignoreCase = true)) return false
        if (dto.puuid != expectedPuuid) return false
        if (dto.seasonYear != expectedSeasonYear || dto.queueId != 420) return false
        if (dto.processedMatchIds.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) return false
        if (dto.processedMatchIds.distinct().size != dto.processedMatchIds.size) return false
        if (dto.overallCounters.totalGames < 0 || dto.overallCounters.totalGames > dto.processedMatchIds.size) return false
        if (dto.overallCounters.totalWins < 0 || dto.overallCounters.totalWins > dto.overallCounters.totalGames) return false
        if (dto.overallCounters.blueSideGames < 0 || dto.overallCounters.redSideGames < 0) return false
        if (dto.overallCounters.blueSideGames + dto.overallCounters.redSideGames > dto.overallCounters.totalGames) return false
        val champGamesSum = dto.championCounters.sumOf { it.games }
        if (champGamesSum != dto.overallCounters.totalGames) return false
        if (dto.pendingMatchIds.isNotEmpty() && dto.historyComplete) return false
        if (dto.historyCapped && dto.historyComplete) return false
        return true
    }

    private fun loadValidDurableSeasonState(
        region: String,
        puuid: String,
        seasonYear: Int
    ): LoadedDurableSeasonState? {
        val repo = playerSeasonStatsRepository ?: return null
        val normalizedRegion = region.lowercase().trim()
        return try {
            val entity = repo.findByRegionAndPuuidAndSeasonYearAndQueueId(normalizedRegion, puuid, seasonYear, 420)
                ?: return null
            if (entity.schemaVersion != DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION) {
                logger.info("Ignoring durable season stats for {} ({}) due to schemaVersion={} != {}", puuid, normalizedRegion, entity.schemaVersion, DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION)
                return null
            }
            val dto = objectMapper.readValue(entity.stateJson, DurablePlayerSeasonStatsDto::class.java)
            if (!isValidDurableSeasonState(dto, normalizedRegion, puuid, seasonYear)) {
                logger.warn("Discarding invalid durable season stats state for {} ({}) year={}", puuid, normalizedRegion, seasonYear)
                return null
            }
            LoadedDurableSeasonState(entityVersion = entity.version, state = dto)
        } catch (e: Exception) {
            logger.warn("Failed to load durable season stats for {} ({}): {}", puuid, normalizedRegion, e.message)
            null
        }
    }

    /**
     * Atomically persists [state] to `player_season_stats` with `@Version` optimistic locking and
     * bounded version-conflict retry (`0..MAX_OPTIMISTIC_LOCK_RETRIES`, up to 3 total DB attempts).
     *
     * Executes ZERO HTTP or Redis calls inside the database transaction. If a concurrent writer
     * incremented the row's `@Version` after [expectedPreviousVersion] was read, the retry reloads
     * the committed DB row inside the transaction, folds only the [newlyFoldedMatches] from this
     * refresh that are not yet in `existing.processedMatchIds`, reconciles `pendingMatchIds` /
     * `excludedMatchIds` / `historyCapped` / `historyComplete`, and commits via `saveAndFlush`.
     */
    private fun persistDurableSeasonState(
        state: DurablePlayerSeasonStatsDto,
        expectedPreviousVersion: Long?,
        newlyFoldedMatches: List<Pair<String, MatchDto>> = emptyList(),
        effectiveSeasonalWindowIds: List<String> = emptyList(),
        stoppedForMemoryHeadroom: Boolean = false
    ): Boolean {
        val repo = playerSeasonStatsRepository ?: return false
        val tx = transactionTemplate
        if (tx == null) {
            logger.warn("Refusing to persist durable season stats for {} ({}) without wired TransactionTemplate", state.puuid, state.region)
            return false
        }
        val normalizedRegion = state.region.lowercase().trim()

        for (attempt in 0..MAX_OPTIMISTIC_LOCK_RETRIES) {
            try {
                val saved = tx.execute {
                    val existing = repo.findByRegionAndPuuidAndSeasonYearAndQueueId(
                        region = normalizedRegion,
                        puuid = state.puuid,
                        seasonYear = state.seasonYear,
                        queueId = state.queueId
                    )
                    if (existing != null) {
                        val stateToPersist = if (attempt == 0 && expectedPreviousVersion != null && existing.version == expectedPreviousVersion) {
                            state
                        } else {
                            if (attempt == 0 && newlyFoldedMatches.isEmpty()) {
                                throw IllegalStateException(
                                    "Optimistic version mismatch on player_season_stats for ${state.puuid} ($normalizedRegion): expected=$expectedPreviousVersion, actual=${existing.version}"
                                )
                            }
                            mergeRefreshDeltaOntoExistingDbState(
                                existingEntity = existing,
                                candidateState = state,
                                newlyFoldedMatches = newlyFoldedMatches,
                                effectiveSeasonalWindowIds = effectiveSeasonalWindowIds,
                                stoppedForMemoryHeadroom = stoppedForMemoryHeadroom
                            ) ?: throw IllegalStateException(
                                "Cannot merge refresh delta onto invalid or incompatible concurrent DB state for ${state.puuid} ($normalizedRegion)"
                            )
                        }
                        val nowInstant = Instant.ofEpochMilli(stateToPersist.updatedAtEpochMilli.takeIf { it > 0L } ?: timeProvider.now())
                        existing.schemaVersion = stateToPersist.schemaVersion
                        existing.foldedGamesCount = stateToPersist.processedMatchIds.size
                        existing.pendingGamesCount = stateToPersist.pendingMatchIds.size
                        existing.historyCapped = stateToPersist.historyCapped
                        existing.historyComplete = stateToPersist.historyComplete
                        existing.stateJson = objectMapper.writeValueAsString(stateToPersist)
                        existing.updatedAt = nowInstant
                        repo.saveAndFlush(existing)
                    } else {
                        if (attempt == 0 && expectedPreviousVersion != null) {
                            throw IllegalStateException(
                                "Expected existing player_season_stats version=$expectedPreviousVersion for ${state.puuid} ($normalizedRegion), but row was absent"
                            )
                        }
                        val nowInstant = Instant.ofEpochMilli(state.updatedAtEpochMilli.takeIf { it > 0L } ?: timeProvider.now())
                        val created = com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                            region = normalizedRegion,
                            puuid = state.puuid,
                            seasonYear = state.seasonYear,
                            queueId = state.queueId,
                            schemaVersion = state.schemaVersion,
                            foldedGamesCount = state.processedMatchIds.size,
                            pendingGamesCount = state.pendingMatchIds.size,
                            historyCapped = state.historyCapped,
                            historyComplete = state.historyComplete,
                            stateJson = objectMapper.writeValueAsString(state),
                            updatedAt = nowInstant
                        )
                        repo.saveAndFlush(created)
                    }
                    true
                } == true
                if (saved) {
                    seasonStatsDbSavesCounter.incrementAndGet()
                    return true
                }
            } catch (e: Exception) {
                if (attempt < MAX_OPTIMISTIC_LOCK_RETRIES && newlyFoldedMatches.isNotEmpty()) {
                    logger.info(
                        "Retrying durable season stats persistence for {} ({}) after version conflict on attempt {}/{}: {}",
                        state.puuid, normalizedRegion, attempt + 1, MAX_OPTIMISTIC_LOCK_RETRIES + 1, e.message
                    )
                    continue
                }
                logger.warn("Failed to persist durable season stats for {} ({}) year={}: {}", state.puuid, normalizedRegion, state.seasonYear, e.message)
                return false
            }
        }
        return false
    }

    private fun mergeRefreshDeltaOntoExistingDbState(
        existingEntity: com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity,
        candidateState: DurablePlayerSeasonStatsDto,
        newlyFoldedMatches: List<Pair<String, MatchDto>>,
        effectiveSeasonalWindowIds: List<String>,
        stoppedForMemoryHeadroom: Boolean
    ): DurablePlayerSeasonStatsDto? {
        val normalizedRegion = candidateState.region.lowercase().trim()
        if (existingEntity.schemaVersion != DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION) return null
        val existingDto = runCatching {
            objectMapper.readValue(existingEntity.stateJson, DurablePlayerSeasonStatsDto::class.java)
        }.getOrNull() ?: return null
        if (!isValidDurableSeasonState(existingDto, normalizedRegion, candidateState.puuid, candidateState.seasonYear)) {
            return null
        }

        val mergedAcc = MatchStatsAccumulator(candidateState.puuid)
        mergedAcc.restoreFromAdditiveSnapshots(existingDto.overallCounters, existingDto.championCounters)
        val mergedFoldedSet = LinkedHashSet<String>(existingDto.processedMatchIds)
        for ((canonicalId, matchDto) in newlyFoldedMatches) {
            if (mergedFoldedSet.add(canonicalId)) {
                mergedAcc.add(matchDto)
            }
        }

        val mergedExcludedSet = LinkedHashSet<String>(existingDto.excludedMatchIds)
        for (exId in candidateState.excludedMatchIds) {
            if (exId !in mergedFoldedSet && mergedExcludedSet.size < DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS) {
                mergedExcludedSet.add(exId)
            }
        }

        val windowList = if (effectiveSeasonalWindowIds.isNotEmpty()) {
            effectiveSeasonalWindowIds
        } else {
            (candidateState.processedMatchIds + existingDto.processedMatchIds).distinct()
        }
        val windowSet = windowList.toHashSet()
        val orderedProcessed = ArrayList<String>(mergedFoldedSet.size)
        for (id in windowList) {
            if (id in mergedFoldedSet && orderedProcessed.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                orderedProcessed.add(id)
            }
        }
        for (id in mergedFoldedSet) {
            if (id !in windowSet && orderedProcessed.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                orderedProcessed.add(id)
            }
        }

        val candidatePendingSet = (existingDto.pendingMatchIds + candidateState.pendingMatchIds).toHashSet()
        val orderedPending = windowList.filter { id ->
            (id in candidatePendingSet || id !in mergedFoldedSet) && id !in mergedFoldedSet && id !in mergedExcludedSet
        }.take(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)

        val mergedHitBoundary = existingDto.hitPreSeasonBoundary || candidateState.hitPreSeasonBoundary
        val mergedBoundaryId = candidateState.firstPreSeasonMatchId ?: existingDto.firstPreSeasonMatchId
        val isCapped = existingDto.historyCapped ||
            candidateState.historyCapped ||
            mergedFoldedSet.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS ||
            (!mergedHitBoundary && windowList.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
        val isComplete = !isCapped &&
            !stoppedForMemoryHeadroom &&
            orderedPending.isEmpty() &&
            windowList.all { it in mergedFoldedSet || it in mergedExcludedSet }

        val (overallSnap, champSnaps) = mergedAcc.toAdditiveSnapshots()
        return DurablePlayerSeasonStatsDto(
            schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
            region = normalizedRegion,
            puuid = candidateState.puuid,
            seasonYear = candidateState.seasonYear,
            queueId = candidateState.queueId,
            processedMatchIds = orderedProcessed,
            pendingMatchIds = orderedPending,
            excludedMatchIds = mergedExcludedSet.take(DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS),
            hitPreSeasonBoundary = mergedHitBoundary,
            firstPreSeasonMatchId = mergedBoundaryId,
            historyCapped = isCapped,
            historyComplete = isComplete,
            overallCounters = overallSnap,
            championCounters = champSnaps,
            updatedAtEpochMilli = timeProvider.now()
        )
    }

    /**
     * Generates a unique key for storing a player's profile in Redis.
     */
    private fun getProfileCacheKey(puuid: String, region: String) = "player:profile:${region.lowercase()}:$puuid"
    
    private fun getMatchCacheKey(matchId: String, region: String) = "match:${region.lowercase()}:$matchId"

    private fun getLockKey(puuid: String, region: String) = "lock:profile:${region.lowercase()}:$puuid"

    private fun getRefreshCooldownKey(puuid: String, region: String) =
        "cooldown:profile:refresh:${region.lowercase()}:$puuid"

    private fun getFlightKey(puuid: String, region: String) = "${region.lowercase()}:$puuid"

    /**
     * True when a match is completely absent from Redis and must be fetched for full-season stats accuracy.
     */
    private fun isMissingForSeasonStats(cached: MatchDto?): Boolean {
        return cached?.info == null
    }

    /**
     * True when a match is missing or lacks enriched fields (`gameVersion`, `teams`) needed for
     * recent-20 match display, patch badges, and `improvementStats` objective control.
     */
    private fun needsMatchBackfill(cached: MatchDto?): Boolean {
        val info = cached?.info ?: return true
        return info.gameVersion == null || info.teams == null
    }

    /**
     * A cached profile is ready for immediate cache-hit response
     * when its full-season `overallStats`, `improvementStats`, and enriched `recentMatches` are present.
     */
    internal fun isProfileReadyForImmediateServe(cached: SummonerProfileDto?): Boolean {
        if (cached == null) return false
        if (cached.overallStats == null || cached.improvementStats == null) return false
        val recent = cached.recentMatches
        if (cached.allMatchIds.isNotEmpty() && recent.isNullOrEmpty()) return false
        return recent?.none { needsMatchBackfill(it) } ?: true
    }

    internal fun isProfileWithinFreshnessWindow(
        cached: SummonerProfileDto?,
        now: Instant,
        window: Duration = PROFILE_FRESHNESS_WINDOW
    ): Boolean {
        val refreshedAt = cached?.lastRefreshed ?: return false
        return !refreshedAt.isBefore(now.minus(window))
    }

    /**
     * Retrieves a player's profile with bounded freshness and explicit-refresh semantics:
     * - If a ready cached profile exists and [explicitRefresh] is `false`:
     *   - Within [PROFILE_FRESHNESS_WINDOW] (5m): returns `cachedProfile` immediately with **zero** Riot API calls
     *     and **zero** background refresh tasks.
     *   - Older than [PROFILE_FRESHNESS_WINDOW]: returns `cachedProfile` immediately and triggers at most one
     *     cooldown-guarded background refresh.
     * - If [explicitRefresh] is `true`:
     *   - If refreshed within [EXPLICIT_REFRESH_MIN_COOLDOWN] (60s), returns `cachedProfile` immediately.
     *   - Otherwise performs a single-flight synchronous refresh.
     * - If cache miss or legacy un-enriched cache entry: performs a single-flight synchronous refresh.
     */
    suspend fun getPlayerProfile(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String,
        explicitRefresh: Boolean = false
    ): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
        val now = Instant.ofEpochMilli(timeProvider.now())

        if (isProfileReadyForImmediateServe(cachedProfile)) {
            if (explicitRefresh) {
                if (isProfileWithinFreshnessWindow(cachedProfile, now, EXPLICIT_REFRESH_MIN_COOLDOWN)) {
                    logger.info("Explicit refresh requested within 60s cooldown for $summonerName#$tagLine ($region); serving fresh cached profile.")
                    return cachedProfile
                }
                logger.info("Explicit refresh requested for $summonerName#$tagLine ($region); refreshing synchronously.")
                return refreshPlayerProfile(puuid, region, summonerName, tagLine)
            }

            if (isProfileWithinFreshnessWindow(cachedProfile, now, PROFILE_FRESHNESS_WINDOW)) {
                logger.info("Fresh cache hit for $summonerName#$tagLine ($region); serving cached profile with zero Riot calls.")
                return cachedProfile
            }

            logger.info("Stale cache hit for $summonerName#$tagLine ($region); serving cached profile and checking background refresh cooldown.")
            triggerBackgroundRefreshIfCooldownAllows(puuid, region, summonerName, tagLine)
            return cachedProfile
        }

        return refreshPlayerProfile(puuid, region, summonerName, tagLine)
    }

    /**
     * Triggers an asynchronous background refresh only if:
     * 1. No refresh is currently in flight for `(region, puuid)`, AND
     * 2. Neither the in-process cooldown nor the Redis cooldown key (`cooldown:profile:refresh:{region}:{puuid}`)
     *    was triggered within [PROFILE_FRESHNESS_WINDOW].
     */
    fun triggerBackgroundRefreshIfCooldownAllows(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): Boolean {
        val flightKey = getFlightKey(puuid, region)
        val existing = inFlightRefreshes[flightKey]
        if (existing != null && existing.isActive) {
            return false
        }

        val nowMs = timeProvider.now()
        val windowMs = PROFILE_FRESHNESS_WINDOW.toMillis()
        val prevMs = lastBackgroundRefreshAtMs[flightKey]
        if (prevMs != null && (nowMs - prevMs) < windowMs) {
            return false
        }

        val cooldownKey = getRefreshCooldownKey(puuid, region)
        val acquiredCooldown = redisCacheService.setIfAbsent(cooldownKey, "COOLDOWN", PROFILE_FRESHNESS_WINDOW)
        if (!acquiredCooldown) {
            return false
        }

        val cutoffMs = nowMs - windowMs
        lastBackgroundRefreshAtMs.entries.removeIf { it.value < cutoffMs }
        while (lastBackgroundRefreshAtMs.size >= 256) {
            val oldestKey = lastBackgroundRefreshAtMs.entries.minByOrNull { it.value }?.key ?: break
            lastBackgroundRefreshAtMs.remove(oldestKey)
        }
        lastBackgroundRefreshAtMs[flightKey] = nowMs
        backgroundRefreshScope.launch {
            try {
                refreshPlayerProfile(puuid, region, summonerName, tagLine)
            } catch (e: Exception) {
                logger.warn("Background profile refresh failed for $summonerName#$tagLine ($region): ${e.message}")
            }
        }
        return true
    }

    /**
     * Single-flight profile refresh with duplicate-lock coordination.
     * Concurrent callers in the same JVM share the in-flight [Deferred] instead of timing out after 2.5s
     * and returning `null`.
     */
    suspend fun refreshPlayerProfile(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): SummonerProfileDto? {
        val flightKey = getFlightKey(puuid, region)
        val (deferred, isLeader) = inFlightMutex.withLock {
            val active = inFlightRefreshes[flightKey]
            if (active != null && active.isActive) {
                active to false
            } else {
                val created = CompletableDeferred<SummonerProfileDto?>()
                inFlightRefreshes[flightKey] = created
                created to true
            }
        }

        if (!isLeader) {
            logger.info("Joining in-flight profile refresh for $summonerName#$tagLine ($region)")
            return deferred.await()
        }

        try {
            val result = executeLockedRefresh(puuid, region, summonerName, tagLine)
            deferred.complete(result)
            return result
        } catch (t: Throwable) {
            deferred.completeExceptionally(t)
            throw t
        } finally {
            inFlightMutex.withLock {
                if (inFlightRefreshes[flightKey] === deferred) {
                    inFlightRefreshes.remove(flightKey)
                }
            }
        }
    }

    private suspend fun executeLockedRefresh(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): SummonerProfileDto? = coroutineScope {
        val cacheKey = getProfileCacheKey(puuid, region)
        val lockKey = getLockKey(puuid, region)
        val waitStart = Instant.ofEpochMilli(timeProvider.now())

        var ownsLock = redisCacheService.setIfAbsent(lockKey, "LOCKED", PROFILE_LOCK_TTL)

        if (!ownsLock) {
            logger.info("Profile refresh lock held externally for $summonerName#$tagLine ($region). Waiting up to ${lockMaxPollAttempts * lockPollIntervalMs}ms.")
            val immediateCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (isProfileReadyForImmediateServe(immediateCached)) {
                return@coroutineScope immediateCached
            }

            repeat(lockMaxPollAttempts) {
                val cached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
                if (cached != null && (
                        cached.lastRefreshed?.isAfter(waitStart.minusSeconds(10)) == true ||
                            isProfileReadyForImmediateServe(cached)
                        )
                ) {
                    logger.info("Cache populated during lock wait for $summonerName#$tagLine ($region)")
                    return@coroutineScope cached
                }
                delay(lockPollIntervalMs)
            }

            val afterWaitCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (afterWaitCached != null) {
                return@coroutineScope afterWaitCached
            }

            // Never bypass a still-held cross-instance lock! Only proceed if the previous lock expired
            // AND this caller successfully acquires the lock.
            ownsLock = redisCacheService.setIfAbsent(lockKey, "LOCKED", PROFILE_LOCK_TTL)
            if (!ownsLock) {
                logger.warn("Cross-instance lock still held after wait for $summonerName#$tagLine ($region); refusing to bypass lock or duplicate cold refresh.")
                return@coroutineScope redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            }

            val postAcquireCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (postAcquireCached != null) {
                redisCacheService.delete(lockKey)
                ownsLock = false
                return@coroutineScope postAcquireCached
            }
            logger.warn("Previous lock expired with empty cache for $summonerName#$tagLine ($region); acquired lock and proceeding with fallback refresh.")
        }

        try {
            val refreshStartMs = timeProvider.now()
            val normalizedRegion = region.lowercase().trim()
            val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

            if (cachedProfile != null) {
                logger.info("Refreshing cached profile for $summonerName#$tagLine ($region)")
            } else {
                logger.info("Cache miss for $summonerName#$tagLine ($region) - Fetching from API")
            }

            // Calculate current season year & Start of Year (Jan 1st 00:00 UTC) from timeProvider for deterministic rollover
            val currentYear = Instant.ofEpochMilli(refreshStartMs).atZone(java.time.ZoneOffset.UTC).year
            val startOfYear = java.time.Year.of(currentYear).atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()
            val startOfYearMs = startOfYear * 1000L

            // 1. Verify summoner exists on this regional shard and fetch fresh level & icon
            val metaStartMs = timeProvider.now()
            val freshSummoner = riotApiService.getSummonerByPuuid(puuid, region)
            if (freshSummoner == null) {
                logger.info("No summoner found on shard $region for $summonerName#$tagLine (puuid=$puuid). Returning null.")
                if (cachedProfile != null) {
                    redisCacheService.delete(cacheKey)
                }
                return@coroutineScope null
            }
            val refreshedLevel = freshSummoner.summonerLevel ?: cachedProfile?.summonerLevel
            val refreshedIconId = freshSummoner.profileIconId ?: cachedProfile?.profileIconId

            // Fetch rank and season match ID list concurrently
            val rankDeferred = async { riotApiService.fetchLeagueRank(puuid, region) }
            val matchIdsDeferred = async { riotApiService.fetchMatchIdsSince(puuid, region, startOfYear) }
            val freshRank = rankDeferred.await()
            val rawAllMatchIds = matchIdsDeferred.await()
            val distinctAllMatchIds = rawAllMatchIds
                .asSequence()
                .map { it.trim() }
                .filter { it.isNotBlank() }
                .distinct()
                .take(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
                .toList()
            val metadataAndIdsMs = timeProvider.now() - metaStartMs

            val loadedDurable = loadValidDurableSeasonState(normalizedRegion, puuid, currentYear)
            val durableState = loadedDurable?.state

            val candidateWindowIds = if (durableState != null && durableState.hitPreSeasonBoundary && durableState.firstPreSeasonMatchId != null) {
                val boundaryIdx = distinctAllMatchIds.indexOf(durableState.firstPreSeasonMatchId)
                if (boundaryIdx >= 0) distinctAllMatchIds.subList(0, boundaryIdx) else distinctAllMatchIds
            } else {
                distinctAllMatchIds
            }

            // Check whether durable state (if repo is wired) is already complete with zero unfolded or pending match IDs
            val durableCompleteAndUpToDate = if (playerSeasonStatsRepository != null) {
                if (durableState == null || !durableState.historyComplete || durableState.pendingMatchIds.isNotEmpty()) {
                    false
                } else {
                    val foldedLookup = durableState.processedMatchIds.toHashSet()
                    val excludedLookup = durableState.excludedMatchIds.toHashSet()
                    candidateWindowIds.all { it in foldedLookup || it in excludedLookup }
                }
            } else {
                true
            }

            // Fast-path: if we already have a cached profile, no new or out-of-order/pending matches exist,
            // durable state (when wired) is complete, and all served recentMatches have gameVersion + teams populated,
            // skip re-streaming and re-accumulating all matches from Redis while updating rank, level, and icon.
            val recentMatchesComplete = cachedProfile?.recentMatches?.none { needsMatchBackfill(it) } ?: true
            if (cachedProfile != null && recentMatchesComplete && durableCompleteAndUpToDate && (
                (distinctAllMatchIds.isEmpty() && cachedProfile.allMatchIds.isEmpty()) ||
                (cachedProfile.allMatchIds.isNotEmpty() &&
                    distinctAllMatchIds.firstOrNull() == cachedProfile.allMatchIds.first() &&
                    distinctAllMatchIds.size == cachedProfile.allMatchIds.size &&
                    cachedProfile.overallStats != null)
            )) {
                val fastImprovementStats = cachedProfile.improvementStats
                    ?: timelineAnalysisService.computeImprovementStats(emptyList(), puuid, cachedProfile.recentMatches ?: emptyList())
                val fastUpdatedProfile = cachedProfile.copy(
                    gameName = summonerName,
                    tagLine = tagLine,
                    summonerLevel = refreshedLevel,
                    profileIconId = refreshedIconId,
                    soloQueueRank = freshRank,
                    improvementStats = fastImprovementStats,
                    lastRefreshed = Instant.ofEpochMilli(timeProvider.now())
                )
                redisCacheService.set(cacheKey, fastUpdatedProfile, PROFILE_CACHE_TTL)
                val totalFastMs = timeProvider.now() - refreshStartMs
                logger.info(
                    "Fast-path refresh for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, totalMatches={} (no new matches since {}).",
                    summonerName, tagLine, region, totalFastMs, metadataAndIdsMs,
                    cachedProfile.totalMatches, cachedProfile.allMatchIds.firstOrNull()
                )
                return@coroutineScope fastUpdatedProfile
            }

            val matchProcessingStartMs = timeProvider.now()
            val accumulator = MatchStatsAccumulator(puuid)
            val foldedSet = LinkedHashSet<String>()
            val excludedSet = LinkedHashSet<String>()
            val newPendingSet = LinkedHashSet<String>()
            var hitPreSeasonBoundary = false
            var firstPreSeasonMatchId: String? = null
            var stoppedForMemoryHeadroom = false

            if (durableState != null) {
                accumulator.restoreFromAdditiveSnapshots(durableState.overallCounters, durableState.championCounters)
                foldedSet.addAll(durableState.processedMatchIds)
                excludedSet.addAll(durableState.excludedMatchIds)
                hitPreSeasonBoundary = durableState.hitPreSeasonBoundary
                firstPreSeasonMatchId = durableState.firstPreSeasonMatchId
            }

            // Determine which match IDs actually require Redis decode or Riot fetch:
            // - When durableState != null: ONLY unfolded candidate match IDs (not in foldedSet and not in excludedSet).
            //   Already-folded matches (e.g. 900 old games) are NEVER re-decoded from Redis or re-fetched from Riot.
            // - When durableState == null: all distinctAllMatchIds in chunks of 50.
            val idsToProcess = if (durableState != null) {
                candidateWindowIds.filter { it !in foldedSet && it !in excludedSet }
            } else {
                distinctAllMatchIds
            }

            val projectedTop20Set = candidateWindowIds
                .asSequence()
                .filter { it !in excludedSet }
                .take(RECENT_MATCHES_LIMIT)
                .toHashSet()

            val chunks = idsToProcess.chunked(CHUNK_SIZE)
            var processedCount = 0
            var projectedRecentCount = 0
            var cacheHitsCount = 0
            var missingSeasonFetchCount = 0
            var recent20BackfillCount = 0
            var skippedOlderBackfillCount = 0
            val alreadyFetchedIds = mutableSetOf<String>()
            val resolvedMatchesById = LinkedHashMap<String, MatchDto>()
            val newlyFoldedMatchesInRefresh = ArrayList<Pair<String, MatchDto>>()
            val matchesForParticipantIndexing = mutableListOf<MatchDto>()

            for ((chunkIndex, chunk) in chunks.withIndex()) {
                if (chunkIndex > 0 && !hasMemoryHeadroomForSeasonBackfill()) {
                    stoppedForMemoryHeadroom = true
                    for (remIdx in chunkIndex until chunks.size) {
                        for (remId in chunks[remIdx]) {
                            if (remId !in foldedSet && remId !in excludedSet) {
                                newPendingSet.add(remId)
                            }
                        }
                    }
                    logger.warn(
                        "Paused seasonal backfill for {}#{} ({}) at chunk {}/{} due to memory headroom guard; {} match IDs recorded as pending.",
                        summonerName, tagLine, region, chunkIndex + 1, chunks.size, newPendingSet.size
                    )
                    break
                }

                val redisKeys = chunk.map { getMatchCacheKey(it, region) }
                val cachedMatchesMap = redisCacheService.multiGet(redisKeys, MatchDto::class.java)

                val missingIds = mutableListOf<String>()
                for (id in chunk) {
                    val cached = cachedMatchesMap[getMatchCacheKey(id, region)]
                    val isRecentSlot = if (durableState != null) {
                        id in projectedTop20Set
                    } else {
                        projectedRecentCount < RECENT_MATCHES_LIMIT
                    }
                    if (isMissingForSeasonStats(cached)) {
                        missingIds.add(id)
                        missingSeasonFetchCount++
                        if (durableState == null && projectedRecentCount < RECENT_MATCHES_LIMIT) {
                            projectedRecentCount++
                        }
                    } else {
                        cacheHitsCount++
                        val creation = cached?.info?.gameCreation ?: 0L
                        if (creation >= startOfYearMs) {
                            if (isRecentSlot) {
                                if (durableState == null) projectedRecentCount++
                                if (needsMatchBackfill(cached)) {
                                    missingIds.add(id)
                                    recent20BackfillCount++
                                }
                            } else if (needsMatchBackfill(cached)) {
                                skippedOlderBackfillCount++
                            }
                        }
                    }
                }

                if (missingIds.isNotEmpty()) {
                    alreadyFetchedIds.addAll(missingIds)
                    logger.info(
                        "Chunk {}/{}: Fetching {} matches from API (missing={}, recent20Backfill={}, skippedOlderBackfill={})",
                        processedCount / CHUNK_SIZE + 1, chunks.size, missingIds.size,
                        missingSeasonFetchCount, recent20BackfillCount, skippedOlderBackfillCount
                    )
                }

                val fetchedMatches = missingIds.map { matchId ->
                    async {
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()

                val validFetchedMatches = fetchedMatches.filter { match ->
                    val creation = match.info?.gameCreation ?: 0
                    match.metadata?.matchId != null && creation >= startOfYearMs
                }

                validFetchedMatches.forEach { match ->
                    val matchId = match.metadata!!.matchId!!
                    redisCacheService.set(getMatchCacheKey(matchId, region), match, MATCH_CACHE_TTL)
                }

                val fetchedById = fetchedMatches.associateBy { it.metadata?.matchId }
                var chunkHitPreSeason = false

                for (id in chunk) {
                    val match = fetchedById[id] ?: cachedMatchesMap[getMatchCacheKey(id, region)]
                    val info = match?.info
                    if (info == null) {
                        newPendingSet.add(id)
                        continue
                    }
                    val creation = info.gameCreation ?: 0L
                    if (creation < startOfYearMs) {
                        hitPreSeasonBoundary = true
                        if (firstPreSeasonMatchId == null) {
                            firstPreSeasonMatchId = id
                        }
                        if (excludedSet.size < DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS) {
                            excludedSet.add(id)
                        }
                        chunkHitPreSeason = true
                        break
                    }
                    val canonicalId = match.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() } ?: id
                    if ((info.gameDuration ?: 0L) <= 0L) {
                        if (excludedSet.size < DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS) {
                            excludedSet.add(canonicalId)
                        }
                        continue
                    }
                    if (foldedSet.add(canonicalId)) {
                        accumulator.add(match)
                        newlyFoldedMatchesInRefresh.add(canonicalId to match)
                        newPendingSet.remove(canonicalId)
                        if (matchesForParticipantIndexing.size < MAX_MATCHES_FOR_PARTICIPANT_INDEXING) {
                            matchesForParticipantIndexing.add(match)
                        }
                    }
                    if (canonicalId in projectedTop20Set || resolvedMatchesById.size < RECENT_MATCHES_LIMIT) {
                        resolvedMatchesById[canonicalId] = match
                    }
                }

                processedCount += chunk.size

                if (chunkHitPreSeason) {
                    logger.info("Hit match time boundary (start of year). Stopping fetch early.")
                    break
                }
            }

            if (matchesForParticipantIndexing.isNotEmpty()) {
                indexPlayersFromMatches(matchesForParticipantIndexing, region)
            }

            // Build ordered validMatchIds preserving Riot's newest-first ordering across newly + previously folded IDs
            val effectiveBoundaryIdx = if (hitPreSeasonBoundary && firstPreSeasonMatchId != null) {
                distinctAllMatchIds.indexOf(firstPreSeasonMatchId)
            } else {
                -1
            }
            val effectiveSeasonalWindowIds = if (effectiveBoundaryIdx >= 0) {
                distinctAllMatchIds.subList(0, effectiveBoundaryIdx)
            } else {
                distinctAllMatchIds
            }
            val windowIdSet = effectiveSeasonalWindowIds.toHashSet()
            val validMatchIds = ArrayList<String>(foldedSet.size)
            for (id in effectiveSeasonalWindowIds) {
                if (id in foldedSet) {
                    validMatchIds.add(id)
                }
            }
            for (id in foldedSet) {
                if (id !in windowIdSet && validMatchIds.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                    validMatchIds.add(id)
                }
            }

            // Assemble recentMatches (up to 20 newest valid seasonal matches) from newly resolved matches + cachedProfile.recentMatches
            // without decoding already-folded historical matches from Redis.
            val priorRecentById = cachedProfile?.recentMatches.orEmpty()
                .mapNotNull { m -> m.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() }?.let { it to m } }
                .toMap()
            val top20ValidIds = validMatchIds.take(RECENT_MATCHES_LIMIT)
            val missingRecentCardIds = top20ValidIds.filter { id ->
                !resolvedMatchesById.containsKey(id) && !priorRecentById.containsKey(id)
            }
            val hydratedRecentCardById = if (missingRecentCardIds.isNotEmpty()) {
                alreadyFetchedIds.addAll(missingRecentCardIds)
                getMatchesByIds(missingRecentCardIds, region)
                    .mapNotNull { m -> m.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() }?.let { it to m } }
                    .toMap()
            } else {
                emptyMap()
            }
            val recentMatches = mutableListOf<MatchDto>()
            for (id in top20ValidIds) {
                val m = resolvedMatchesById[id] ?: priorRecentById[id] ?: hydratedRecentCardById[id]
                if (m != null) {
                    recentMatches.add(m)
                }
            }

            // Safety top-up: if any match in the final recent 20 still needs backfill AND was not already fetched
            // (e.g., an earlier match 404'd and shifted cached match #21 into slot #20), enrich only those entries.
            val unenrichedRecentIds = recentMatches
                .filter { needsMatchBackfill(it) }
                .mapNotNull { it.metadata?.matchId }
                .filter { it !in alreadyFetchedIds }
            if (unenrichedRecentIds.isNotEmpty()) {
                val topUpFetched = unenrichedRecentIds.map { matchId ->
                    async {
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()
                val topUpById = topUpFetched.associateBy { it.metadata?.matchId }
                topUpFetched.forEach { match ->
                    val matchId = match.metadata?.matchId
                    if (matchId != null) {
                        redisCacheService.set(getMatchCacheKey(matchId, region), match, MATCH_CACHE_TTL)
                    }
                }
                for (i in recentMatches.indices) {
                    val mid = recentMatches[i].metadata?.matchId
                    val replacement = if (mid != null) topUpById[mid] else null
                    if (replacement != null) {
                        recentMatches[i] = replacement
                        recent20BackfillCount++
                    }
                }
            }

            // Carry over any prior pending IDs that remain in the current seasonal window and were not resolved
            if (durableState != null) {
                for (priorPending in durableState.pendingMatchIds) {
                    if (priorPending in windowIdSet && priorPending !in foldedSet && priorPending !in excludedSet) {
                        newPendingSet.add(priorPending)
                    }
                }
            }
            for (id in effectiveSeasonalWindowIds) {
                if (id !in foldedSet && id !in excludedSet) {
                    newPendingSet.add(id)
                }
            }

            val orderedPendingIds = effectiveSeasonalWindowIds.filter { it in newPendingSet }
            val isHistoryCapped = (durableState?.historyCapped == true) ||
                validMatchIds.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS ||
                (!hitPreSeasonBoundary && effectiveSeasonalWindowIds.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
            val isHistoryComplete = !isHistoryCapped &&
                orderedPendingIds.isEmpty() &&
                !stoppedForMemoryHeadroom &&
                effectiveSeasonalWindowIds.all { it in foldedSet || it in excludedSet }

            if (playerSeasonStatsRepository != null) {
                val (overallSnap, champSnaps) = accumulator.toAdditiveSnapshots()
                val newDurableDto = DurablePlayerSeasonStatsDto(
                    schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
                    region = normalizedRegion,
                    puuid = puuid,
                    seasonYear = currentYear,
                    queueId = 420,
                    processedMatchIds = validMatchIds.take(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS),
                    pendingMatchIds = orderedPendingIds.take(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS),
                    excludedMatchIds = excludedSet.take(DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS),
                    hitPreSeasonBoundary = hitPreSeasonBoundary,
                    firstPreSeasonMatchId = firstPreSeasonMatchId,
                    historyCapped = isHistoryCapped,
                    historyComplete = isHistoryComplete,
                    overallCounters = overallSnap,
                    championCounters = champSnaps,
                    updatedAtEpochMilli = timeProvider.now()
                )
                persistDurableSeasonState(
                    state = newDurableDto,
                    expectedPreviousVersion = loadedDurable?.entityVersion,
                    newlyFoldedMatches = newlyFoldedMatchesInRefresh,
                    effectiveSeasonalWindowIds = effectiveSeasonalWindowIds,
                    stoppedForMemoryHeadroom = stoppedForMemoryHeadroom
                )
            }
            
            val (championStats, overallStats) = accumulator.getResult()
            val matchOnlyImprovementStats = timelineAnalysisService.computeImprovementStats(
                timelines = emptyList(),
                puuid = puuid,
                allMatches = recentMatches
            )
            
            val updatedProfile = SummonerProfileDto(
                puuid = puuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = refreshedLevel,
                profileIconId = refreshedIconId,
                soloQueueRank = freshRank,
                recentMatches = recentMatches,
                allMatchIds = validMatchIds,
                championStats = championStats,
                overallStats = overallStats,
                improvementStats = matchOnlyImprovementStats,
                totalMatches = validMatchIds.size,
                lastRefreshed = Instant.ofEpochMilli(timeProvider.now())
            )
            
            redisCacheService.set(cacheKey, updatedProfile, PROFILE_CACHE_TTL)
            val matchProcessingMs = timeProvider.now() - matchProcessingStartMs
            val totalRefreshMs = timeProvider.now() - refreshStartMs
            logger.info(
                "Refreshed profile for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, matchProcessingMs={}, validMatches={}/{}, pendingGaps={}, historyComplete={}, cacheHits={}, missingFetches={}, recent20Backfills={}, skippedOlderBackfills={}",
                summonerName, tagLine, region, totalRefreshMs, metadataAndIdsMs, matchProcessingMs,
                validMatchIds.size, distinctAllMatchIds.size, orderedPendingIds.size, isHistoryComplete,
                cacheHitsCount, missingSeasonFetchCount, recent20BackfillCount, skippedOlderBackfillCount
            )
            
            return@coroutineScope updatedProfile

        } finally {
            if (ownsLock) {
                redisCacheService.delete(lockKey)
            }
        }
    }

    /**
     * Retrieves a cached profile directly. Used for pagination.
     */
    fun getCachedProfile(puuid: String, region: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        return redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
    }

    suspend fun getMatchesByIds(matchIds: List<String>, region: String): List<com.pride.summoner_searcher_api.dto.MatchDto> = kotlinx.coroutines.coroutineScope {
        if (matchIds.isEmpty()) return@coroutineScope emptyList()
        
        val redisKeys = matchIds.map { getMatchCacheKey(it, region) }
        val cachedMatchesMap = redisCacheService.multiGet(redisKeys, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
        
        val missingIds = matchIds.filter { id ->
            val cached = cachedMatchesMap[getMatchCacheKey(id, region)]
            needsMatchBackfill(cached)
        }
        
        val fetchedMatches = missingIds.map { matchId ->
            async {
                try {
                    riotApiService.getMatchById(matchId, region)
                } catch (e: Exception) {
                    null
                }
            }
        }.awaitAll().filterNotNull()
        
        val fetchedById = fetchedMatches.associateBy { it.metadata?.matchId }
        fetchedMatches.forEach { match ->
             val mid = match.metadata?.matchId
             if (mid != null) {
                 redisCacheService.set(getMatchCacheKey(mid, region), match, Duration.ofDays(60))
             }
        }
        
        // Index players found in these new matches for autocomplete search
        if (fetchedMatches.isNotEmpty()) {
            indexPlayersFromMatches(fetchedMatches, region)
        }
        
        matchIds.mapNotNull { id ->
            fetchedById[id] ?: cachedMatchesMap[getMatchCacheKey(id, region)]
        }
    }

    /**
     * Upserts searchable participant identities from already-fetched matches while strictly preserving
     * each player's newest observed Riot ID (`gameName#tagLine`), `profileIconId`, `summonerLevel`, and `lastSeenAt`.
     *
     * - Uses each match's `info.gameCreation` timestamp as `lastSeenAt` (falling back to `timeProvider.now()` only if `<= 0`).
     * - Deduplicates within the batch by keeping the participant entry from the newest match (`max(lastSeenAt)`).
     * - Executes `findAllById` + monotonic `lastSeenAt` comparison + `saveAll` atomically inside a single DB transaction
     *   (when [transactionTemplate] is wired) so historic/backfilled matches can never overwrite a newer Riot ID or level.
     */
    internal fun indexPlayersFromMatches(matches: List<com.pride.summoner_searcher_api.dto.MatchDto>, region: String) {
        val normalizedRegion = region.lowercase().trim()
        val latestByPuuid = LinkedHashMap<String, com.pride.summoner_searcher_api.model.IndexedPlayer>()

        for (match in matches.take(MAX_MATCHES_FOR_PARTICIPANT_INDEXING)) {
            val info = match.info ?: continue
            val matchPlayedAtMs = (info.gameCreation ?: 0L).takeIf { it > 0L } ?: timeProvider.now()
            val matchPlayedAt = Instant.ofEpochMilli(matchPlayedAtMs)

            for (p in info.participants) {
                val cleanPuuid = p.puuid?.trim()?.takeIf { it.isNotBlank() } ?: continue
                val cleanGameName = p.riotIdGameName?.trim()?.takeIf { it.isNotBlank() } ?: continue
                val cleanTagLine = p.riotIdTagline?.trim()?.takeIf { it.isNotBlank() } ?: continue

                val existingInBatch = latestByPuuid[cleanPuuid]
                if (existingInBatch == null || !matchPlayedAt.isBefore(existingInBatch.lastSeenAt)) {
                    latestByPuuid[cleanPuuid] = com.pride.summoner_searcher_api.model.IndexedPlayer(
                        puuid = cleanPuuid,
                        gameName = cleanGameName,
                        tagLine = cleanTagLine,
                        region = normalizedRegion,
                        profileIconId = p.profileIcon ?: 0,
                        summonerLevel = p.summonerLevel?.toLong() ?: 0L,
                        lastSeenAt = matchPlayedAt
                    )
                }
            }
        }

        if (latestByPuuid.isEmpty()) return

        val boundedCandidates = latestByPuuid.values
            .sortedByDescending { it.lastSeenAt }
            .take(MAX_INDEXED_PLAYERS_PER_BATCH)

        val upsertBlock = {
            val candidateIds = boundedCandidates.map { it.puuid }
            val existingInDbByPuuid = runCatching {
                indexedPlayerRepository.findAllById(candidateIds).associateBy { it.puuid }
            }.getOrDefault(emptyMap())

            val rowsToSave = ArrayList<com.pride.summoner_searcher_api.model.IndexedPlayer>(boundedCandidates.size)
            for (candidate in boundedCandidates) {
                val existingDb = existingInDbByPuuid[candidate.puuid]
                if (existingDb != null && candidate.lastSeenAt.isBefore(existingDb.lastSeenAt)) {
                    indexedPlayersStaleSkippedCounter.incrementAndGet()
                    continue
                }
                rowsToSave.add(candidate)
            }

            if (rowsToSave.isNotEmpty()) {
                rowsToSave.chunked(100).forEach { batch ->
                    indexedPlayerRepository.saveAll(batch)
                }
                indexedPlayersUpsertedCounter.addAndGet(rowsToSave.size.toLong())
            }
        }

        try {
            val tx = transactionTemplate
            if (tx != null) {
                tx.executeWithoutResult { upsertBlock() }
            } else {
                upsertBlock()
            }
        } catch (e: Exception) {
            logger.error("Error saving indexed players: ${e.message}")
        }
    }
}
