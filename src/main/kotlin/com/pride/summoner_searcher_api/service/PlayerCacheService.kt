package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.repository.indexParticipantsFromMatchesBounded
import com.pride.summoner_searcher_api.repository.upsertPlayerIfNewer
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
    private val minFreeMemoryBytes: Long = 67_108_864L,
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private val timelineService: TimelineService? = null
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val timelineAnalysisService = TimelineAnalysisService()
    private val inFlightMutex = Mutex()
    private val inFlightRefreshes = ConcurrentHashMap<String, InFlightRefreshEntry>()
    private val lastBackgroundRefreshAtMs = ConcurrentHashMap<String, Long>()
    private val riotIdToPuuidIndex = ConcurrentHashMap<String, RiotIdMappingEntry>()
    private val verifiedCompleteRedisOnlySeasons = ConcurrentHashMap<String, VerifiedRedisSeasonLedger>()
    private val backgroundRefreshScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val indexedPlayersUpsertedCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val indexedPlayersStaleSkippedCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val seasonStatsDbSavesCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val seasonStatsDbFailuresCounter = java.util.concurrent.atomic.AtomicLong(0L)
    private val lastRefreshPeakRetainedMatchDtos = java.util.concurrent.atomic.AtomicInteger(0)
    private val lastRefreshPeakCrossChunkRetainedMatchDtos = java.util.concurrent.atomic.AtomicInteger(0)

    private data class InFlightRefreshEntry(
        val deferred: CompletableDeferred<SummonerProfileDto?>,
        val priorityRef: java.util.concurrent.atomic.AtomicReference<ApiPriority>,
        val promotionSignal: CompletableDeferred<Unit>,
        val activeWaiters: java.util.concurrent.atomic.AtomicInteger,
        val workerJobRef: java.util.concurrent.atomic.AtomicReference<kotlinx.coroutines.Job?>
    )

    private data class RiotIdMappingEntry(
        val puuid: String,
        val updatedAtMs: Long
    )

    private data class VerifiedRedisSeasonLedger(
        val seasonYear: Int,
        val orderedMatchIds: List<String>,
        val excludedMatchIds: Set<String>,
        val historyCapped: Boolean,
        val updatedAtMs: Long
    )

    data class ImmediateProfileLookupResult(
        val served: Boolean = false,
        val profile: SummonerProfileDto? = null
    )

    companion object {
        private const val RECENT_MATCHES_LIMIT = 20
        private const val CHUNK_SIZE = 50
        private const val MAX_CONCURRENT_MATCH_FETCHES_PER_REFRESH = 20
        private const val MAX_RIOT_ID_INDEX_ENTRIES = 512
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
        "seasonStatsDbSaves" to seasonStatsDbSavesCounter.get(),
        "seasonStatsDbFailures" to seasonStatsDbFailuresCounter.get(),
        "lastRefreshPeakRetainedMatchDtos" to lastRefreshPeakRetainedMatchDtos.get().toLong(),
        "lastRefreshPeakLiveMatchDtos" to lastRefreshPeakRetainedMatchDtos.get().toLong(),
        "lastRefreshPeakCrossChunkRetainedMatchDtos" to lastRefreshPeakCrossChunkRetainedMatchDtos.get().toLong()
    )

    private fun countDistinctLiveMatchDtos(vararg collections: Iterable<MatchDto?>?): Int {
        val identitySet = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<MatchDto, Boolean>())
        for (collection in collections) {
            if (collection == null) continue
            for (dto in collection) {
                if (dto != null) {
                    identitySet.add(dto)
                }
            }
        }
        return identitySet.size
    }

    internal fun hasMemoryHeadroomForSeasonBackfill(): Boolean {
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
            logger.warn("Memory headroom sample failed during seasonal backfill guard; failing safe (false): {}", e.message)
            false
        }
    }

    private data class LoadedDurableSeasonState(
        val entityVersion: Long,
        val state: DurablePlayerSeasonStatsDto?
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
        // Counters must equal the saved processed-ID ledger size (never outgrow or diverge from processedMatchIds)
        if (dto.overallCounters.totalGames < 0 || dto.overallCounters.totalGames != dto.processedMatchIds.size) return false
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
        if (transactionTemplate == null) {
            seasonStatsDbFailuresCounter.incrementAndGet()
            throw IllegalStateException("PlayerSeasonStatsRepository is wired without a usable TransactionTemplate for $puuid ($region)")
        }
        val normalizedRegion = region.lowercase().trim()
        val entity = repo.findByRegionAndPuuidAndSeasonYearAndQueueId(normalizedRegion, puuid, seasonYear, 420)
            ?: return null
        if (entity.schemaVersion != DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION) {
            logger.info(
                "Rebuilding durable season stats for {} ({}) due to stale schemaVersion={} != {}",
                puuid, normalizedRegion, entity.schemaVersion, DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION
            )
            return LoadedDurableSeasonState(entityVersion = entity.version, state = null)
        }
        val dto = try {
            objectMapper.readValue(entity.stateJson, DurablePlayerSeasonStatsDto::class.java)
        } catch (e: Exception) {
            logger.warn("Rebuilding durable season stats for {} ({}) due to unreadable stateJson: {}", puuid, normalizedRegion, e.message)
            return LoadedDurableSeasonState(entityVersion = entity.version, state = null)
        }
        if (!isValidDurableSeasonState(dto, normalizedRegion, puuid, seasonYear)) {
            logger.warn("Rebuilding invalid durable season stats state for {} ({}) year={}", puuid, normalizedRegion, seasonYear)
            return LoadedDurableSeasonState(entityVersion = entity.version, state = null)
        }
        return LoadedDurableSeasonState(entityVersion = entity.version, state = dto)
    }

    /**
     * Atomically persists [state] to `player_season_stats` with `@Version` optimistic locking and
     * bounded version-conflict retry (`0..MAX_OPTIMISTIC_LOCK_RETRIES`, up to 3 total DB attempts).
     *
     * Executes ZERO HTTP or Redis calls inside the database transaction. Returns the exact committed
     * [DurablePlayerSeasonStatsDto] (including any concurrent merge) so the caller updates Redis
     * `player:profile` with the authoritative committed totals. Throws [IllegalStateException] if
     * persistence fails so durability failure is never hidden behind a stale/uncommitted Redis write.
     */
    private fun persistDurableSeasonState(
        state: DurablePlayerSeasonStatsDto,
        expectedPreviousVersion: Long?,
        isFullRebuild: Boolean = false,
        newlyFoldedDeltas: List<SingleMatchAdditiveDelta> = emptyList(),
        evictedDeltas: List<SingleMatchAdditiveDelta> = emptyList(),
        effectiveSeasonalWindowIds: List<String> = emptyList(),
        rawFetchHitSafetyLimit: Boolean = false,
        rawFetchTruncatedByLedgerCap: Boolean = false,
        paginationExhaustedCleanly: Boolean = true,
        stoppedForMemoryHeadroom: Boolean = false
    ): DurablePlayerSeasonStatsDto {
        val repo = playerSeasonStatsRepository
            ?: throw IllegalStateException("Cannot call persistDurableSeasonState without PlayerSeasonStatsRepository")
        val tx = transactionTemplate
        if (tx == null) {
            seasonStatsDbFailuresCounter.incrementAndGet()
            throw IllegalStateException("Refusing to persist durable season stats for ${state.puuid} (${state.region}) without wired TransactionTemplate")
        }
        val normalizedRegion = state.region.lowercase().trim()
        var lastError: Throwable? = null

        for (attempt in 0..MAX_OPTIMISTIC_LOCK_RETRIES) {
            try {
                val committedState = tx.execute {
                    val existing = repo.findByRegionAndPuuidAndSeasonYearAndQueueId(
                        region = normalizedRegion,
                        puuid = state.puuid,
                        seasonYear = state.seasonYear,
                        queueId = state.queueId
                    )
                    if (existing != null) {
                        val existingDto = if (existing.schemaVersion == DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION) {
                            runCatching {
                                objectMapper.readValue(existing.stateJson, DurablePlayerSeasonStatsDto::class.java)
                            }.getOrNull()?.takeIf {
                                isValidDurableSeasonState(it, normalizedRegion, state.puuid, state.seasonYear)
                            }
                        } else {
                            null
                        }

                        val stateToPersist = when {
                            attempt == 0 && expectedPreviousVersion != null && existing.version == expectedPreviousVersion -> {
                                state
                            }
                            existingDto == null && isFullRebuild -> {
                                // Existing DB row has a stale schemaVersion or unreadable stateJson and this refresh computed a full rebuild
                                state
                            }
                            else -> {
                                if (attempt == 0 && newlyFoldedDeltas.isEmpty() && evictedDeltas.isEmpty()) {
                                    throw IllegalStateException(
                                        "Optimistic version mismatch on player_season_stats for ${state.puuid} ($normalizedRegion): expected=$expectedPreviousVersion, actual=${existing.version}"
                                    )
                                }
                                mergeRefreshDeltaOntoExistingDbState(
                                    existingDto = existingDto ?: throw IllegalStateException(
                                        "Cannot merge incremental delta onto incompatible/stale concurrent DB state for ${state.puuid} ($normalizedRegion)"
                                    ),
                                    candidateState = state,
                                    newlyFoldedDeltas = newlyFoldedDeltas,
                                    evictedDeltas = evictedDeltas,
                                    effectiveSeasonalWindowIds = effectiveSeasonalWindowIds,
                                    rawFetchHitSafetyLimit = rawFetchHitSafetyLimit,
                                    rawFetchTruncatedByLedgerCap = rawFetchTruncatedByLedgerCap,
                                    paginationExhaustedCleanly = paginationExhaustedCleanly,
                                    stoppedForMemoryHeadroom = stoppedForMemoryHeadroom
                                )
                            }
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
                        stateToPersist
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
                        state
                    }
                }
                if (committedState != null) {
                    seasonStatsDbSavesCounter.incrementAndGet()
                    return committedState
                }
            } catch (e: Exception) {
                lastError = e
                val canRetryConflict = attempt < MAX_OPTIMISTIC_LOCK_RETRIES &&
                    (newlyFoldedDeltas.isNotEmpty() || evictedDeltas.isNotEmpty() || isFullRebuild)
                if (canRetryConflict) {
                    logger.info(
                        "Retrying durable season stats persistence for {} ({}) after version conflict on attempt {}/{}: {}",
                        state.puuid, normalizedRegion, attempt + 1, MAX_OPTIMISTIC_LOCK_RETRIES + 1, e.message
                    )
                    continue
                }
                break
            }
        }
        seasonStatsDbFailuresCounter.incrementAndGet()
        throw IllegalStateException(
            "Failed to persist durable season stats for ${state.puuid} ($normalizedRegion) year=${state.seasonYear}",
            lastError
        )
    }

    private fun mergeRefreshDeltaOntoExistingDbState(
        existingDto: DurablePlayerSeasonStatsDto,
        candidateState: DurablePlayerSeasonStatsDto,
        newlyFoldedDeltas: List<SingleMatchAdditiveDelta>,
        evictedDeltas: List<SingleMatchAdditiveDelta>,
        effectiveSeasonalWindowIds: List<String>,
        rawFetchHitSafetyLimit: Boolean,
        rawFetchTruncatedByLedgerCap: Boolean,
        paginationExhaustedCleanly: Boolean,
        stoppedForMemoryHeadroom: Boolean
    ): DurablePlayerSeasonStatsDto {
        val normalizedRegion = candidateState.region.lowercase().trim()

        val mergedAcc = MatchStatsAccumulator(candidateState.puuid)
        mergedAcc.restoreFromAdditiveSnapshots(existingDto.overallCounters, existingDto.championCounters)
        val mergedFoldedSet = LinkedHashSet<String>(existingDto.processedMatchIds)
        val addedDeltasDuringMerge = ArrayList<SingleMatchAdditiveDelta>()
        for (delta in newlyFoldedDeltas) {
            if (delta.matchId !in mergedFoldedSet) {
                mergedFoldedSet.add(delta.matchId)
                mergedAcc.addDelta(delta)
                addedDeltasDuringMerge.add(delta)
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
        val allOrderedMerged = ArrayList<String>(mergedFoldedSet.size)
        for (id in windowList) {
            if (id in mergedFoldedSet) {
                allOrderedMerged.add(id)
            }
        }
        for (id in mergedFoldedSet) {
            if (id !in windowSet) {
                allOrderedMerged.add(id)
            }
        }

        val evictedDuringMergeSet = HashSet<String>()
        val rolledBackToPendingSet = LinkedHashSet<String>()
        var hitLedgerCapDuringMerge = false
        if (allOrderedMerged.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
            hitLedgerCapDuringMerge = true
            val knownDeltasById = LinkedHashMap<String, SingleMatchAdditiveDelta>()
            for (d in evictedDeltas) knownDeltasById[d.matchId] = d
            for (d in newlyFoldedDeltas) knownDeltasById[d.matchId] = d

            val overflowTailIds = allOrderedMerged.drop(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
            for (tailId in overflowTailIds) {
                if (mergedFoldedSet.size <= DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) break
                val knownDelta = knownDeltasById[tailId]
                if (knownDelta != null && tailId in mergedFoldedSet) {
                    mergedAcc.subtractDelta(knownDelta)
                    mergedFoldedSet.remove(tailId)
                    evictedDuringMergeSet.add(tailId)
                }
            }
            while (mergedFoldedSet.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS &&
                addedDeltasDuringMerge.isNotEmpty()
            ) {
                val rolledBack = addedDeltasDuringMerge.removeAt(addedDeltasDuringMerge.lastIndex)
                if (mergedFoldedSet.remove(rolledBack.matchId)) {
                    mergedAcc.subtractDelta(rolledBack)
                    rolledBackToPendingSet.add(rolledBack.matchId)
                }
            }
        }

        val orderedProcessed = ArrayList<String>(
            mergedFoldedSet.size.coerceAtMost(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
        )
        for (id in allOrderedMerged) {
            if (id in mergedFoldedSet && orderedProcessed.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                orderedProcessed.add(id)
            }
        }

        // Preserve all unresolved pending IDs from both existingDto and candidateState (even if absent from current windowList)
        val candidatePendingUnion = LinkedHashSet<String>()
        candidatePendingUnion.addAll(existingDto.pendingMatchIds)
        candidatePendingUnion.addAll(candidateState.pendingMatchIds)
        candidatePendingUnion.addAll(rolledBackToPendingSet)
        for (id in windowList) {
            if (id !in mergedFoldedSet && id !in mergedExcludedSet && id !in evictedDuringMergeSet) {
                candidatePendingUnion.add(id)
            }
        }
        val orderedPending = ArrayList<String>()
        for (id in windowList) {
            if (id in candidatePendingUnion &&
                id !in mergedFoldedSet &&
                id !in mergedExcludedSet &&
                id !in evictedDuringMergeSet &&
                orderedPending.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS
            ) {
                orderedPending.add(id)
            }
        }
        for (id in candidatePendingUnion) {
            if (id !in windowSet &&
                id !in mergedFoldedSet &&
                id !in mergedExcludedSet &&
                id !in evictedDuringMergeSet &&
                orderedPending.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS
            ) {
                orderedPending.add(id)
            }
        }

        val mergedHitBoundary = existingDto.hitPreSeasonBoundary || candidateState.hitPreSeasonBoundary
        val mergedBoundaryId = candidateState.firstPreSeasonMatchId ?: existingDto.firstPreSeasonMatchId
        val mergedPaginationCoversSeasonHistory = paginationExhaustedCleanly ||
            mergedHitBoundary ||
            (existingDto.historyComplete &&
                windowList.any { it in existingDto.processedMatchIds || it in existingDto.excludedMatchIds })
        val isCapped = existingDto.historyCapped ||
            candidateState.historyCapped ||
            hitLedgerCapDuringMerge ||
            rawFetchTruncatedByLedgerCap ||
            (!mergedHitBoundary && rawFetchHitSafetyLimit)
        val isComplete = !isCapped &&
            mergedPaginationCoversSeasonHistory &&
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

    private fun normalizeRiotIdKey(region: String, summonerName: String, tagLine: String): String {
        return "${region.lowercase().trim()}|${summonerName.trim().lowercase()}|${tagLine.trim().lowercase()}"
    }

    private fun recordRiotIdMapping(region: String, summonerName: String?, tagLine: String?, puuid: String?) {
        val cleanName = summonerName?.trim()?.takeIf { it.isNotBlank() } ?: return
        val cleanTag = tagLine?.trim()?.takeIf { it.isNotBlank() } ?: return
        val cleanPuuid = puuid?.trim()?.takeIf { it.isNotBlank() } ?: return
        val normalizedRegionPrefix = "${region.lowercase().trim()}|"
        val key = normalizeRiotIdKey(region, cleanName, cleanTag)
        val nowMs = timeProvider.now()
        // Evict any prior Riot-ID alias on the same regional shard pointing to this PUUID (handles renames cleanly).
        riotIdToPuuidIndex.entries.removeIf { (existingKey, existingEntry) ->
            existingKey != key && existingKey.startsWith(normalizedRegionPrefix) && existingEntry.puuid == cleanPuuid
        }
        if (!riotIdToPuuidIndex.containsKey(key)) {
            while (riotIdToPuuidIndex.size >= MAX_RIOT_ID_INDEX_ENTRIES) {
                val oldestKey = riotIdToPuuidIndex.entries.minByOrNull { it.value.updatedAtMs }?.key ?: break
                riotIdToPuuidIndex.remove(oldestKey)
            }
        }
        riotIdToPuuidIndex[key] = RiotIdMappingEntry(puuid = cleanPuuid, updatedAtMs = nowMs)
    }

    internal fun reconcileVerifiedRiotIdOwner(region: String, summonerName: String, tagLine: String, verifiedPuuid: String) {
        val key = normalizeRiotIdKey(region, summonerName, tagLine)
        riotIdToPuuidIndex.computeIfPresent(key) { _, entry ->
            if (entry.puuid != verifiedPuuid) null else entry
        }
    }

    internal fun evictRiotIdMapping(region: String, summonerName: String, tagLine: String, expectedPuuid: String? = null) {
        val key = normalizeRiotIdKey(region, summonerName, tagLine)
        if (expectedPuuid == null) {
            riotIdToPuuidIndex.remove(key)
        } else {
            riotIdToPuuidIndex.computeIfPresent(key) { _, entry ->
                if (entry.puuid == expectedPuuid) null else entry
            }
        }
    }

    private fun cacheMatchesBatch(entries: Map<String, MatchDto>, expiration: Duration) {
        if (entries.isEmpty()) return
        if (redisCacheService.canPipelineBatchWrites()) {
            redisCacheService.setAll(entries, expiration)
        } else {
            for ((key, match) in entries) {
                redisCacheService.set(key, match, expiration)
            }
        }
    }

    /**
     * Resolves an immediately-servable cached profile by normalized `(region, summonerName, tagLine)` without
     * blocking on `account-v1-by-riot-id`:
     * - Fresh (`<5m`, `explicitRefresh = false`): served immediately with **0** Riot API calls.
     * - Stale (`>=5m`, `explicitRefresh = false`): served immediately with **0** foreground Riot API calls while
     *   triggering a cooldown-guarded `LOW`-priority background refresh that also reconciles Riot ID ownership.
     * - Explicit refresh within `60s` cooldown (`explicitRefresh = true`): served immediately with **0** Riot API calls.
     * - Cold miss, un-enriched legacy cache entry, or explicit refresh past `60s`: returns `served = false` so the
     *   caller performs live `getAccountByRiotId` verification before synchronous refresh.
     */
    fun resolveImmediateCachedProfileByRiotId(
        region: String,
        summonerName: String,
        tagLine: String,
        explicitRefresh: Boolean = false
    ): ImmediateProfileLookupResult {
        val trimmedName = summonerName.trim()
        val trimmedTag = tagLine.trim()
        if (trimmedName.isEmpty() || trimmedTag.isEmpty()) {
            return ImmediateProfileLookupResult(served = false)
        }
        val key = normalizeRiotIdKey(region, trimmedName, trimmedTag)
        val entry = riotIdToPuuidIndex[key] ?: return ImmediateProfileLookupResult(served = false)
        val cacheKey = getProfileCacheKey(entry.puuid, region)
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
        if (cachedProfile == null ||
            cachedProfile.puuid != entry.puuid ||
            !cachedProfile.gameName.orEmpty().trim().equals(trimmedName, ignoreCase = true) ||
            !cachedProfile.tagLine.orEmpty().trim().equals(trimmedTag, ignoreCase = true) ||
            !isProfileReadyForImmediateServe(cachedProfile)
        ) {
            evictRiotIdMapping(region, trimmedName, trimmedTag, entry.puuid)
            return ImmediateProfileLookupResult(served = false)
        }

        val now = Instant.ofEpochMilli(timeProvider.now())
        if (explicitRefresh) {
            if (isProfileWithinFreshnessWindow(cachedProfile, now, EXPLICIT_REFRESH_MIN_COOLDOWN)) {
                recordRiotIdMapping(region, cachedProfile.gameName ?: trimmedName, cachedProfile.tagLine ?: trimmedTag, entry.puuid)
                logger.info("Explicit refresh requested within 60s cooldown for $summonerName#$tagLine ($region); serving fresh cached profile with zero Riot calls.")
                return ImmediateProfileLookupResult(served = true, profile = cachedProfile)
            }
            return ImmediateProfileLookupResult(served = false)
        }

        recordRiotIdMapping(region, cachedProfile.gameName ?: trimmedName, cachedProfile.tagLine ?: trimmedTag, entry.puuid)
        if (isProfileWithinFreshnessWindow(cachedProfile, now, PROFILE_FRESHNESS_WINDOW)) {
            logger.info("Fresh Riot-ID cache hit for $summonerName#$tagLine ($region); serving cached profile with zero Riot calls.")
            return ImmediateProfileLookupResult(served = true, profile = cachedProfile)
        }

        logger.info("Stale Riot-ID cache hit for $summonerName#$tagLine ($region); serving cached profile with zero foreground Riot calls and scheduling LOW-priority refresh.")
        triggerBackgroundRefreshIfCooldownAllows(
            puuid = entry.puuid,
            region = region,
            summonerName = cachedProfile.gameName ?: summonerName,
            tagLine = cachedProfile.tagLine ?: tagLine,
            verifyRiotIdOwnership = true
        )
        return ImmediateProfileLookupResult(served = true, profile = cachedProfile)
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
            recordRiotIdMapping(region, summonerName, tagLine, puuid)
            recordRiotIdMapping(region, cachedProfile?.gameName, cachedProfile?.tagLine, puuid)
            if (explicitRefresh) {
                if (isProfileWithinFreshnessWindow(cachedProfile, now, EXPLICIT_REFRESH_MIN_COOLDOWN)) {
                    logger.info("Explicit refresh requested within 60s cooldown for $summonerName#$tagLine ($region); serving fresh cached profile.")
                    return cachedProfile
                }
                logger.info("Explicit refresh requested for $summonerName#$tagLine ($region); refreshing synchronously.")
                return refreshPlayerProfileInternal(
                    puuid = puuid,
                    region = region,
                    summonerName = summonerName,
                    tagLine = tagLine,
                    requestedPriority = ApiPriority.HIGH,
                    preloadedCacheSnapshot = true to cachedProfile
                )
            }

            if (isProfileWithinFreshnessWindow(cachedProfile, now, PROFILE_FRESHNESS_WINDOW)) {
                logger.info("Fresh cache hit for $summonerName#$tagLine ($region); serving cached profile with zero Riot calls.")
                return cachedProfile
            }

            logger.info("Stale cache hit for $summonerName#$tagLine ($region); serving cached profile and checking background refresh cooldown.")
            triggerBackgroundRefreshIfCooldownAllows(puuid, region, summonerName, tagLine)
            return cachedProfile
        }

        return refreshPlayerProfileInternal(
            puuid = puuid,
            region = region,
            summonerName = summonerName,
            tagLine = tagLine,
            requestedPriority = ApiPriority.HIGH,
            preloadedCacheSnapshot = true to cachedProfile
        )
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
        tagLine: String,
        verifyRiotIdOwnership: Boolean = false
    ): Boolean {
        val flightKey = getFlightKey(puuid, region)
        val existing = inFlightRefreshes[flightKey]
        if (existing != null && existing.deferred.isActive) {
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
                if (verifyRiotIdOwnership) {
                    val accountCheck = kotlinx.coroutines.withContext(
                        RiotApiService.RiotCallPriorityElement(priorityProvider = { ApiPriority.LOW })
                    ) {
                        riotApiService.getAccountByRiotId(summonerName, tagLine, region)
                    }
                    val verifiedPuuid = accountCheck?.puuid?.trim()?.takeIf { it.isNotBlank() }
                    if (verifiedPuuid == null || verifiedPuuid != puuid) {
                        evictRiotIdMapping(region, summonerName, tagLine, puuid)
                        logger.info(
                            "Background Riot-ID ownership changed or missing for {}#{} ({}) (expectedPuuid={}, actualPuuid={}); evicted Riot-ID mapping.",
                            summonerName, tagLine, region, puuid, verifiedPuuid
                        )
                        return@launch
                    }
                }
                refreshPlayerProfileInternal(
                    puuid = puuid,
                    region = region,
                    summonerName = summonerName,
                    tagLine = tagLine,
                    requestedPriority = ApiPriority.LOW
                )
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
        return refreshPlayerProfileInternal(
            puuid = puuid,
            region = region,
            summonerName = summonerName,
            tagLine = tagLine,
            requestedPriority = ApiPriority.HIGH
        )
    }

    private suspend fun refreshPlayerProfileInternal(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String,
        requestedPriority: ApiPriority = ApiPriority.HIGH,
        preloadedCacheSnapshot: Pair<Boolean, SummonerProfileDto?>? = null
    ): SummonerProfileDto? {
        val flightKey = getFlightKey(puuid, region)
        val (entry, isLeader) = inFlightMutex.withLock {
            val active = inFlightRefreshes[flightKey]
            if (active != null && active.deferred.isActive) {
                active.activeWaiters.incrementAndGet()
                if (requestedPriority == ApiPriority.HIGH) {
                    active.priorityRef.set(ApiPriority.HIGH)
                    active.promotionSignal.complete(Unit)
                }
                active to false
            } else {
                val created = InFlightRefreshEntry(
                    deferred = CompletableDeferred(),
                    priorityRef = java.util.concurrent.atomic.AtomicReference(requestedPriority),
                    promotionSignal = CompletableDeferred<Unit>().apply {
                        if (requestedPriority == ApiPriority.HIGH) complete(Unit)
                    },
                    activeWaiters = java.util.concurrent.atomic.AtomicInteger(1),
                    workerJobRef = java.util.concurrent.atomic.AtomicReference(null)
                )
                val workerJob = backgroundRefreshScope.launch(
                    RiotApiService.RiotCallPriorityElement(
                        priorityProvider = { created.priorityRef.get() },
                        promotionSignal = created.promotionSignal
                    )
                ) {
                    try {
                        val result = executeLockedRefresh(puuid, region, summonerName, tagLine, preloadedCacheSnapshot)
                        created.deferred.complete(result)
                    } catch (t: Throwable) {
                        created.deferred.completeExceptionally(t)
                    } finally {
                        inFlightMutex.withLock {
                            if (inFlightRefreshes[flightKey] === created) {
                                inFlightRefreshes.remove(flightKey)
                            }
                        }
                    }
                }
                created.workerJobRef.set(workerJob)
                inFlightRefreshes[flightKey] = created
                created to true
            }
        }

        if (!isLeader) {
            logger.info("Joining in-flight profile refresh for $summonerName#$tagLine ($region) (priority={})", requestedPriority)
        }

        return try {
            entry.deferred.await()
        } finally {
            val remaining = entry.activeWaiters.decrementAndGet()
            if (remaining <= 0 && !entry.deferred.isCompleted) {
                entry.workerJobRef.get()?.cancel()
            }
        }
    }

    private suspend fun executeLockedRefresh(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String,
        preloadedCacheSnapshot: Pair<Boolean, SummonerProfileDto?>? = null
    ): SummonerProfileDto? = coroutineScope {
        val cacheKey = getProfileCacheKey(puuid, region)
        val lockKey = getLockKey(puuid, region)
        val waitStart = Instant.ofEpochMilli(timeProvider.now())

        var ownsLock = redisCacheService.setIfAbsent(lockKey, "LOCKED", PROFILE_LOCK_TTL)
        var waitedForExternalLock = false

        if (!ownsLock) {
            waitedForExternalLock = true
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
            val cachedProfile = if (!waitedForExternalLock && preloadedCacheSnapshot?.first == true) {
                preloadedCacheSnapshot.second
            } else {
                redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            }

            if (cachedProfile != null) {
                logger.info("Refreshing cached profile for $summonerName#$tagLine ($region)")
            } else {
                logger.info("Cache miss for $summonerName#$tagLine ($region) - Fetching from API")
            }

            // Calculate current season year & Start of Year (Jan 1st 00:00 UTC) from timeProvider for deterministic rollover
            val currentYear = Instant.ofEpochMilli(refreshStartMs).atZone(java.time.ZoneOffset.UTC).year
            val startOfYear = java.time.Year.of(currentYear).atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()
            val startOfYearMs = startOfYear * 1000L

            val loadedDurable = loadValidDurableSeasonState(normalizedRegion, puuid, currentYear)
            val durableState = loadedDurable?.state
            val recentMatchesComplete = cachedProfile?.recentMatches?.none { needsMatchBackfill(it) } ?: true
            val redisSeasonKey = "$normalizedRegion|$puuid|$currentYear"

            // Build incremental match-ID pagination anchor ONLY when prior seasonal history is proven complete
            // (or cleanly at the 2,100-match cap) with zero unresolved pending gaps — never from a partial ledger.
            val matchIdsAnchor = when {
                playerSeasonStatsRepository != null -> {
                    if (durableState != null &&
                        durableState.pendingMatchIds.isEmpty() &&
                        durableState.processedMatchIds.isNotEmpty() &&
                        (durableState.historyComplete ||
                            (durableState.historyCapped &&
                                durableState.processedMatchIds.size >= DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS))
                    ) {
                        RiotApiService.MatchIdsIncrementalAnchor(
                            knownOrderedSeasonalIds = durableState.processedMatchIds,
                            knownExcludedIds = durableState.excludedMatchIds.toHashSet(),
                            priorHistoryCapped = durableState.historyCapped
                        )
                    } else {
                        null
                    }
                }
                else -> {
                    val verifiedLedger = verifiedCompleteRedisOnlySeasons[redisSeasonKey]
                    if (verifiedLedger != null &&
                        verifiedLedger.seasonYear == currentYear &&
                        cachedProfile != null &&
                        cachedProfile.allMatchIds.isNotEmpty() &&
                        cachedProfile.allMatchIds == verifiedLedger.orderedMatchIds &&
                        cachedProfile.overallStats != null &&
                        cachedProfile.totalMatches == cachedProfile.allMatchIds.size &&
                        recentMatchesComplete
                    ) {
                        RiotApiService.MatchIdsIncrementalAnchor(
                            knownOrderedSeasonalIds = verifiedLedger.orderedMatchIds,
                            knownExcludedIds = verifiedLedger.excludedMatchIds,
                            priorHistoryCapped = verifiedLedger.historyCapped
                        )
                    } else {
                        null
                    }
                }
            }

            suspend fun fetchSeasonalMatchIdsOutcome(): RiotApiService.MatchIdsPaginationOutcome {
                return if (matchIdsAnchor != null) {
                    kotlinx.coroutines.withContext(RiotApiService.MatchIdsIncrementalAnchorElement(matchIdsAnchor)) {
                        riotApiService.fetchMatchIdsWithOutcomeSince(puuid, region, startOfYear)
                    }
                } else {
                    riotApiService.fetchMatchIdsWithOutcomeSince(puuid, region, startOfYear)
                }
            }

            // 1. Verify summoner exists on this regional shard and fetch fresh level, icon, rank, and match IDs.
            // For summoners already verified on this regional shard (cachedProfile or durableState present),
            // overlap Summoner-V4, League-V4, and Match-V5 IDs concurrently to save a full sequential RTT.
            val metaStartMs = timeProvider.now()
            val (freshSummoner, freshRank, paginationOutcome) = if (cachedProfile != null || durableState != null) {
                val summonerDeferred = async { riotApiService.getSummonerByPuuid(puuid, region) }
                val rankDeferred = async { riotApiService.fetchLeagueRank(puuid, region) }
                val matchIdsOutcomeDeferred = async { fetchSeasonalMatchIdsOutcome() }
                val summoner = summonerDeferred.await()
                if (summoner == null) {
                    rankDeferred.cancel()
                    matchIdsOutcomeDeferred.cancel()
                    logger.info("No summoner found on shard $region for $summonerName#$tagLine (puuid=$puuid). Returning null.")
                    if (cachedProfile != null) {
                        redisCacheService.delete(cacheKey)
                    }
                    evictRiotIdMapping(region, summonerName, tagLine, puuid)
                    return@coroutineScope null
                }
                Triple(summoner, rankDeferred.await(), matchIdsOutcomeDeferred.await())
            } else {
                val summoner = riotApiService.getSummonerByPuuid(puuid, region)
                if (summoner == null) {
                    logger.info("No summoner found on shard $region for $summonerName#$tagLine (puuid=$puuid). Returning null.")
                    evictRiotIdMapping(region, summonerName, tagLine, puuid)
                    return@coroutineScope null
                }
                val rankDeferred = async { riotApiService.fetchLeagueRank(puuid, region) }
                val matchIdsOutcomeDeferred = async { fetchSeasonalMatchIdsOutcome() }
                Triple(summoner, rankDeferred.await(), matchIdsOutcomeDeferred.await())
            }
            val refreshedLevel = freshSummoner.summonerLevel ?: cachedProfile?.summonerLevel
            val refreshedIconId = freshSummoner.profileIconId ?: cachedProfile?.profileIconId
            val rawAllMatchIds = paginationOutcome.matchIds
            val rawNonBlankIds = rawAllMatchIds
                .asSequence()
                .map { it.trim() }
                .filter { it.isNotBlank() }
                .toList()
            val rawDistinctMatchIds = rawNonBlankIds.distinct()
            val paginationStoppedOnFailure = paginationOutcome.stoppedOnPageFailure
            // Capture pagination safety-cap and truncation evidence BEFORE truncating to MAX_TRACKED_SEASON_MATCH_IDS (2,100).
            val rawFetchHitSafetyLimit = paginationOutcome.hitSafetyCap ||
                rawAllMatchIds.size > DurablePlayerSeasonStatsDto.RIOT_MATCH_IDS_SAFETY_THRESHOLD ||
                rawDistinctMatchIds.size > DurablePlayerSeasonStatsDto.RIOT_MATCH_IDS_SAFETY_THRESHOLD
            val rawFetchTruncatedByLedgerCap = rawDistinctMatchIds.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS
            val paginationExhaustedCleanly = paginationOutcome.exhaustedCleanly &&
                !paginationStoppedOnFailure &&
                !rawFetchHitSafetyLimit &&
                !rawFetchTruncatedByLedgerCap
            val distinctAllMatchIds = rawDistinctMatchIds.take(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
            val metadataAndIdsMs = timeProvider.now() - metaStartMs

            // Never conflate a page-0 match-ID pagination failure (`matchIds == null` at start=0) with an empty season:
            if (paginationStoppedOnFailure && paginationOutcome.pagesFetched == 0 && distinctAllMatchIds.isEmpty()) {
                logger.warn(
                    "Match ID pagination failed on page 0 for {}#{} ({}); refusing to treat failed page as empty season.",
                    summonerName, tagLine, region
                )
                if (cachedProfile != null) {
                    return@coroutineScope cachedProfile
                }
                if (durableState == null) {
                    return@coroutineScope null
                }
            }

            val candidateWindowIds = if (durableState != null && durableState.hitPreSeasonBoundary && durableState.firstPreSeasonMatchId != null) {
                val boundaryIdx = distinctAllMatchIds.indexOf(durableState.firstPreSeasonMatchId)
                if (boundaryIdx >= 0) distinctAllMatchIds.subList(0, boundaryIdx) else distinctAllMatchIds
            } else {
                distinctAllMatchIds
            }

            // Check whether durable state (if repo is wired) is already complete with zero unfolded or pending match IDs
            val excludedLookupForFastPath = durableState?.excludedMatchIds?.toHashSet() ?: emptySet()
            val durableCompleteAndUpToDate = if (playerSeasonStatsRepository != null) {
                if (durableState == null || !durableState.historyComplete || durableState.pendingMatchIds.isNotEmpty()) {
                    false
                } else {
                    val foldedLookup = durableState.processedMatchIds.toHashSet()
                    candidateWindowIds.all { it in foldedLookup || it in excludedLookupForFastPath } &&
                        cachedProfile?.totalMatches == durableState.overallCounters.totalGames &&
                        cachedProfile.allMatchIds == durableState.processedMatchIds
                }
            } else {
                true
            }

            // Fast-path: if we already have a cached profile, no new or out-of-order/pending matches exist,
            // durable state (when wired) is complete, and all served recentMatches have gameVersion + teams populated,
            // skip re-streaming and re-accumulating all matches from Redis while updating rank, level, and icon.
            val unexcludedCandidateIds = if (excludedLookupForFastPath.isNotEmpty()) {
                candidateWindowIds.filter { it !in excludedLookupForFastPath }
            } else {
                candidateWindowIds
            }
            if (cachedProfile != null && recentMatchesComplete && durableCompleteAndUpToDate && (
                (unexcludedCandidateIds.isEmpty() && cachedProfile.allMatchIds.isEmpty()) ||
                (cachedProfile.allMatchIds.isNotEmpty() &&
                    unexcludedCandidateIds.firstOrNull() == cachedProfile.allMatchIds.first() &&
                    unexcludedCandidateIds.size == cachedProfile.allMatchIds.size &&
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
                recordRiotIdMapping(region, summonerName, tagLine, puuid)
                val totalFastMs = timeProvider.now() - refreshStartMs
                logger.info(
                    "Fast-path refresh for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, totalMatches={} (no new matches since {}).",
                    summonerName, tagLine, region, totalFastMs, metadataAndIdsMs,
                    cachedProfile.totalMatches, cachedProfile.allMatchIds.firstOrNull()
                )
                return@coroutineScope fastUpdatedProfile
            }

            val matchProcessingStartMs = timeProvider.now()
            val matchFetchSemaphore = kotlinx.coroutines.sync.Semaphore(MAX_CONCURRENT_MATCH_FETCHES_PER_REFRESH)
            val accumulator = MatchStatsAccumulator(puuid)
            val foldedSet = LinkedHashSet<String>()
            val excludedSet = LinkedHashSet<String>()
            val newPendingSet = LinkedHashSet<String>()
            val cappedOutMatchIds = HashSet<String>()
            var hitPreSeasonBoundary = false
            var firstPreSeasonMatchId: String? = null
            var stoppedForMemoryHeadroom = false
            var hitLedgerCapDuringRefresh = false

            if (durableState != null) {
                accumulator.restoreFromAdditiveSnapshots(durableState.overallCounters, durableState.championCounters)
                foldedSet.addAll(durableState.processedMatchIds)
                excludedSet.addAll(durableState.excludedMatchIds)
                hitPreSeasonBoundary = durableState.hitPreSeasonBoundary
                firstPreSeasonMatchId = durableState.firstPreSeasonMatchId
            }

            // Determine which match IDs actually require Redis decode or Riot fetch:
            // - When durableState != null: ONLY unfolded candidate match IDs (not in foldedSet and not in excludedSet)
            //   PLUS any prior unresolved pendingMatchIds (even if absent from the current candidateWindowIds).
            // - When durableState == null: all distinctAllMatchIds in chunks of 50.
            val idsToProcess = if (durableState != null) {
                val combined = LinkedHashSet<String>()
                for (id in candidateWindowIds) {
                    if (id !in foldedSet && id !in excludedSet) {
                        combined.add(id)
                    }
                }
                for (priorPending in durableState.pendingMatchIds) {
                    if (priorPending !in foldedSet && priorPending !in excludedSet) {
                        combined.add(priorPending)
                    }
                }
                combined.toList()
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
            var peakLiveMatchDtos = countDistinctLiveMatchDtos(cachedProfile?.recentMatches)
            var peakCrossChunkRetainedMatchDtos = countDistinctLiveMatchDtos(cachedProfile?.recentMatches)
            val alreadyFetchedIds = mutableSetOf<String>()
            val resolvedMatchesById = LinkedHashMap<String, MatchDto>()
            val newlyFoldedDeltasInRefresh = ArrayList<SingleMatchAdditiveDelta>()
            val matchesForParticipantIndexing = ArrayList<MatchDto>(MAX_MATCHES_FOR_PARTICIPANT_INDEXING)

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
                        matchFetchSemaphore.acquire()
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        } finally {
                            matchFetchSemaphore.release()
                        }
                    }
                }.awaitAll().filterNotNull()

                val seasonalMatchesToCache = LinkedHashMap<String, MatchDto>(fetchedMatches.size)
                for (match in fetchedMatches) {
                    val creation = match.info?.gameCreation ?: 0
                    val matchId = match.metadata?.matchId
                    if (matchId != null && creation >= startOfYearMs) {
                        seasonalMatchesToCache[getMatchCacheKey(matchId, region)] = match
                    }
                }
                cacheMatchesBatch(seasonalMatchesToCache, MATCH_CACHE_TTL)

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
                        newPendingSet.remove(id)
                        chunkHitPreSeason = true
                        break
                    }
                    val canonicalId = match.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() } ?: id
                    if ((info.gameDuration ?: 0L) <= 0L) {
                        if (excludedSet.size < DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS) {
                            excludedSet.add(canonicalId)
                        }
                        newPendingSet.remove(canonicalId)
                        newPendingSet.remove(id)
                        continue
                    }
                    if (canonicalId !in foldedSet) {
                        if (newlyFoldedDeltasInRefresh.size >= DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                            hitLedgerCapDuringRefresh = true
                            cappedOutMatchIds.add(canonicalId)
                        } else {
                            val delta = accumulator.addMatchAndExtractDelta(canonicalId, match)
                            if (delta != null || (playerSeasonStatsRepository == null && info.participants.isEmpty())) {
                                foldedSet.add(canonicalId)
                                if (delta != null) {
                                    newlyFoldedDeltasInRefresh.add(delta)
                                }
                                newPendingSet.remove(canonicalId)
                                newPendingSet.remove(id)
                                if (matchesForParticipantIndexing.size < MAX_MATCHES_FOR_PARTICIPANT_INDEXING) {
                                    matchesForParticipantIndexing.add(match)
                                }
                            } else {
                                if (excludedSet.size < DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS) {
                                    excludedSet.add(canonicalId)
                                }
                                newPendingSet.remove(canonicalId)
                                newPendingSet.remove(id)
                            }
                        }
                    }
                    if ((canonicalId in projectedTop20Set || resolvedMatchesById.size < RECENT_MATCHES_LIMIT) &&
                        (resolvedMatchesById.containsKey(canonicalId) || resolvedMatchesById.size < RECENT_MATCHES_LIMIT)
                    ) {
                        resolvedMatchesById[canonicalId] = match
                    }
                }

                val currentLiveDtos = countDistinctLiveMatchDtos(
                    cachedMatchesMap.values,
                    fetchedMatches,
                    fetchedById.values,
                    resolvedMatchesById.values,
                    matchesForParticipantIndexing,
                    cachedProfile?.recentMatches
                )
                if (currentLiveDtos > peakLiveMatchDtos) {
                    peakLiveMatchDtos = currentLiveDtos
                }
                val currentCrossChunkDtos = countDistinctLiveMatchDtos(
                    resolvedMatchesById.values,
                    matchesForParticipantIndexing,
                    cachedProfile?.recentMatches
                )
                if (currentCrossChunkDtos > peakCrossChunkRetainedMatchDtos) {
                    peakCrossChunkRetainedMatchDtos = currentCrossChunkDtos
                }

                processedCount += chunk.size

                if (chunkHitPreSeason) {
                    logger.info("Hit match time boundary (start of year). Stopping fetch early.")
                    break
                }
            }

            if (matchesForParticipantIndexing.isNotEmpty()) {
                indexPlayersFromMatches(matchesForParticipantIndexing, region)
                matchesForParticipantIndexing.clear()
            }

            // Build ordered candidate folded list preserving Riot's newest-first ordering across newly + previously folded IDs
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
            val allOrderedFoldedIds = ArrayList<String>(foldedSet.size)
            for (id in effectiveSeasonalWindowIds) {
                if (id in foldedSet) {
                    allOrderedFoldedIds.add(id)
                }
            }
            for (id in foldedSet) {
                if (id !in windowIdSet) {
                    allOrderedFoldedIds.add(id)
                }
            }

            // If folding new matches onto a capped player's 2,100-ID ledger pushes total folded IDs past 2,100,
            // slide the 2,100-match moving window forward by evicting the oldest tail match IDs and subtracting
            // their exact SingleMatchAdditiveDelta so newest cards and totals update without counter drift or archive expansion.
            val evictedDeltasInRefresh = ArrayList<SingleMatchAdditiveDelta>()
            if (allOrderedFoldedIds.size > DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                hitLedgerCapDuringRefresh = true
                val overflowTailIds = allOrderedFoldedIds.drop(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
                val newlyFoldedById = newlyFoldedDeltasInRefresh.associateBy { it.matchId }
                val resolvedEvictionDeltas = LinkedHashMap<String, SingleMatchAdditiveDelta>()
                val missingEvictionIds = ArrayList<String>()

                for (evictId in overflowTailIds) {
                    val inMemoryDelta = newlyFoldedById[evictId]
                        ?: resolvedMatchesById[evictId]?.let { accumulator.extractDelta(evictId, it) }
                    if (inMemoryDelta != null) {
                        resolvedEvictionDeltas[evictId] = inMemoryDelta
                    } else {
                        missingEvictionIds.add(evictId)
                    }
                }

                if (missingEvictionIds.isNotEmpty()) {
                    for (evictChunk in missingEvictionIds.chunked(CHUNK_SIZE)) {
                        val evictKeys = evictChunk.map { getMatchCacheKey(it, region) }
                        val evictCachedMap = redisCacheService.multiGet(evictKeys, MatchDto::class.java)
                        val toFetchFromRiot = ArrayList<String>()
                        for (evictId in evictChunk) {
                            val cachedMatch = evictCachedMap[getMatchCacheKey(evictId, region)]
                            val delta = cachedMatch?.let { accumulator.extractDelta(evictId, it) }
                            if (delta != null) {
                                resolvedEvictionDeltas[evictId] = delta
                            } else {
                                toFetchFromRiot.add(evictId)
                            }
                        }
                        val evictFetched = if (toFetchFromRiot.isNotEmpty()) {
                            toFetchFromRiot.map { matchId ->
                                async {
                                    try {
                                        riotApiService.getMatchById(matchId, region)
                                    } catch (e: Exception) {
                                        null
                                    }
                                }
                            }.awaitAll().filterNotNull()
                        } else {
                            emptyList()
                        }
                        val evictLiveDtos = countDistinctLiveMatchDtos(
                            evictCachedMap.values,
                            evictFetched,
                            resolvedMatchesById.values,
                            matchesForParticipantIndexing,
                            cachedProfile?.recentMatches
                        )
                        if (evictLiveDtos > peakLiveMatchDtos) {
                            peakLiveMatchDtos = evictLiveDtos
                        }
                        for (fetchedMatch in evictFetched) {
                            val mid = fetchedMatch.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() } ?: continue
                            redisCacheService.set(getMatchCacheKey(mid, region), fetchedMatch, MATCH_CACHE_TTL)
                            val delta = accumulator.extractDelta(mid, fetchedMatch)
                            if (delta != null) {
                                resolvedEvictionDeltas[mid] = delta
                            }
                        }
                    }
                }

                for (evictId in overflowTailIds) {
                    val evictDelta = resolvedEvictionDeltas[evictId]
                    if (evictDelta != null) {
                        accumulator.subtractDelta(evictDelta)
                        foldedSet.remove(evictId)
                        newlyFoldedDeltasInRefresh.removeIf { it.matchId == evictId }
                        evictedDeltasInRefresh.add(evictDelta)
                        cappedOutMatchIds.add(evictId)
                    } else if (newlyFoldedDeltasInRefresh.isNotEmpty()) {
                        val rolledBack = newlyFoldedDeltasInRefresh.removeAt(newlyFoldedDeltasInRefresh.lastIndex)
                        accumulator.subtractDelta(rolledBack)
                        foldedSet.remove(rolledBack.matchId)
                        newPendingSet.add(rolledBack.matchId)
                    }
                }
            }

            lastRefreshPeakRetainedMatchDtos.set(peakLiveMatchDtos)
            lastRefreshPeakCrossChunkRetainedMatchDtos.set(peakCrossChunkRetainedMatchDtos)

            val validMatchIds = ArrayList<String>(
                foldedSet.size.coerceAtMost(DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS)
            )
            for (id in allOrderedFoldedIds) {
                if (id in foldedSet && validMatchIds.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                    validMatchIds.add(id)
                }
            }

            // Carry over ALL unresolved prior pending IDs (including those absent from the current seasonal window)
            if (durableState != null) {
                for (priorPending in durableState.pendingMatchIds) {
                    if (priorPending !in foldedSet && priorPending !in excludedSet && priorPending !in cappedOutMatchIds) {
                        newPendingSet.add(priorPending)
                    }
                }
            }
            for (id in effectiveSeasonalWindowIds) {
                if (id !in foldedSet && id !in excludedSet && id !in cappedOutMatchIds) {
                    newPendingSet.add(id)
                }
            }

            val orderedPendingIds = ArrayList<String>(newPendingSet.size)
            for (id in effectiveSeasonalWindowIds) {
                if (id in newPendingSet && orderedPendingIds.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                    orderedPendingIds.add(id)
                }
            }
            for (id in newPendingSet) {
                if (id !in windowIdSet && orderedPendingIds.size < DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS) {
                    orderedPendingIds.add(id)
                }
            }

            val paginationCoversSeasonHistory = paginationExhaustedCleanly ||
                hitPreSeasonBoundary ||
                (durableState?.historyComplete == true &&
                    effectiveSeasonalWindowIds.any { it in durableState.processedMatchIds || it in durableState.excludedMatchIds })
            val isHistoryCapped = (durableState?.historyCapped == true) ||
                hitLedgerCapDuringRefresh ||
                rawFetchTruncatedByLedgerCap ||
                (!hitPreSeasonBoundary && rawFetchHitSafetyLimit)
            val isHistoryComplete = !isHistoryCapped &&
                paginationCoversSeasonHistory &&
                orderedPendingIds.isEmpty() &&
                !stoppedForMemoryHeadroom &&
                effectiveSeasonalWindowIds.all { it in foldedSet || it in excludedSet }

            // Persist durable state BEFORE writing Redis profile so durability failure is never hidden
            // and Redis profile always uses the committed merged DB state.
            val committedDurableState = if (playerSeasonStatsRepository != null) {
                val (overallSnap, champSnaps) = accumulator.toAdditiveSnapshots()
                val newDurableDto = DurablePlayerSeasonStatsDto(
                    schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
                    region = normalizedRegion,
                    puuid = puuid,
                    seasonYear = currentYear,
                    queueId = 420,
                    processedMatchIds = validMatchIds,
                    pendingMatchIds = orderedPendingIds,
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
                    isFullRebuild = durableState == null,
                    newlyFoldedDeltas = newlyFoldedDeltasInRefresh,
                    evictedDeltas = evictedDeltasInRefresh,
                    effectiveSeasonalWindowIds = effectiveSeasonalWindowIds,
                    rawFetchHitSafetyLimit = rawFetchHitSafetyLimit,
                    rawFetchTruncatedByLedgerCap = rawFetchTruncatedByLedgerCap,
                    paginationExhaustedCleanly = paginationExhaustedCleanly,
                    stoppedForMemoryHeadroom = stoppedForMemoryHeadroom
                )
            } else {
                null
            }

            val finalValidMatchIds = committedDurableState?.processedMatchIds ?: validMatchIds
            val finalAccumulator = if (committedDurableState != null) {
                MatchStatsAccumulator(puuid).also {
                    it.restoreFromAdditiveSnapshots(
                        committedDurableState.overallCounters,
                        committedDurableState.championCounters
                    )
                }
            } else {
                accumulator
            }

            // Assemble recentMatches (up to 20 newest valid seasonal matches) from newly resolved matches + cachedProfile.recentMatches
            // without decoding already-folded historical matches from Redis.
            val priorRecentById = cachedProfile?.recentMatches.orEmpty()
                .mapNotNull { m -> m.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() }?.let { it to m } }
                .toMap()
            val top20ValidIds = finalValidMatchIds.take(RECENT_MATCHES_LIMIT)
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
                val topUpToCache = LinkedHashMap<String, MatchDto>(topUpFetched.size)
                for (match in topUpFetched) {
                    val matchId = match.metadata?.matchId
                    if (matchId != null) {
                        topUpToCache[getMatchCacheKey(matchId, region)] = match
                    }
                }
                cacheMatchesBatch(topUpToCache, MATCH_CACHE_TTL)
                for (i in recentMatches.indices) {
                    val mid = recentMatches[i].metadata?.matchId
                    val replacement = if (mid != null) topUpById[mid] else null
                    if (replacement != null) {
                        recentMatches[i] = replacement
                        recent20BackfillCount++
                    }
                }
            }

            val (championStats, overallStats) = finalAccumulator.getResult()
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
                allMatchIds = finalValidMatchIds,
                championStats = championStats,
                overallStats = overallStats,
                improvementStats = matchOnlyImprovementStats,
                totalMatches = finalValidMatchIds.size,
                lastRefreshed = Instant.ofEpochMilli(timeProvider.now())
            )
            
            redisCacheService.set(cacheKey, updatedProfile, PROFILE_CACHE_TTL)
            recordRiotIdMapping(region, summonerName, tagLine, puuid)
            if (playerSeasonStatsRepository == null) {
                val canRecordVerifiedRedisLedger = orderedPendingIds.isEmpty() &&
                    !stoppedForMemoryHeadroom &&
                    finalValidMatchIds.isNotEmpty() &&
                    (isHistoryComplete ||
                        (isHistoryCapped &&
                            finalValidMatchIds.size >= DurablePlayerSeasonStatsDto.MAX_TRACKED_SEASON_MATCH_IDS &&
                            !paginationStoppedOnFailure))
                if (canRecordVerifiedRedisLedger) {
                    if (!verifiedCompleteRedisOnlySeasons.containsKey(redisSeasonKey)) {
                        while (verifiedCompleteRedisOnlySeasons.size >= MAX_RIOT_ID_INDEX_ENTRIES) {
                            val oldestKey = verifiedCompleteRedisOnlySeasons.entries.minByOrNull { it.value.updatedAtMs }?.key ?: break
                            verifiedCompleteRedisOnlySeasons.remove(oldestKey)
                        }
                    }
                    verifiedCompleteRedisOnlySeasons[redisSeasonKey] = VerifiedRedisSeasonLedger(
                        seasonYear = currentYear,
                        orderedMatchIds = finalValidMatchIds,
                        excludedMatchIds = excludedSet.take(DurablePlayerSeasonStatsDto.MAX_EXCLUDED_MATCH_IDS).toHashSet(),
                        historyCapped = isHistoryCapped,
                        updatedAtMs = timeProvider.now()
                    )
                } else {
                    verifiedCompleteRedisOnlySeasons.remove(redisSeasonKey)
                }
            }
            timelineService?.invalidatePlayerPreparedCache(normalizedRegion, puuid)
            val matchProcessingMs = timeProvider.now() - matchProcessingStartMs
            val totalRefreshMs = timeProvider.now() - refreshStartMs
            logger.info(
                "Refreshed profile for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, matchProcessingMs={}, validMatches={}/{}, pendingGaps={}, historyComplete={}, cacheHits={}, missingFetches={}, recent20Backfills={}, skippedOlderBackfills={}",
                summonerName, tagLine, region, totalRefreshMs, metadataAndIdsMs, matchProcessingMs,
                finalValidMatchIds.size, distinctAllMatchIds.size,
                (committedDurableState?.pendingMatchIds ?: orderedPendingIds).size,
                committedDurableState?.historyComplete ?: isHistoryComplete,
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
        val fetchSemaphore = kotlinx.coroutines.sync.Semaphore(MAX_CONCURRENT_MATCH_FETCHES_PER_REFRESH)
        val fetchedMatches = missingIds.map { matchId ->
            async {
                fetchSemaphore.acquire()
                try {
                    riotApiService.getMatchById(matchId, region)
                } catch (e: Exception) {
                    null
                } finally {
                    fetchSemaphore.release()
                }
            }
        }.awaitAll().filterNotNull()
        
        val fetchedById = fetchedMatches.associateBy { it.metadata?.matchId }
        val fetchedToCache = LinkedHashMap<String, MatchDto>(fetchedMatches.size)
        for (match in fetchedMatches) {
            val mid = match.metadata?.matchId
            if (mid != null) {
                fetchedToCache[getMatchCacheKey(mid, region)] = match
            }
        }
        cacheMatchesBatch(fetchedToCache, MATCH_CACHE_TTL)
        
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
     * - Executes conditional SQL upsert (`INSERT ... ON CONFLICT (puuid) DO UPDATE ... WHERE indexed_players.last_seen_at <= EXCLUDED.last_seen_at`)
     *   inside [transactionTemplate] with zero `findAllById` read-before-write races and zero `emptyMap()` fallback overwrites.
     */
    internal fun indexPlayersFromMatches(matches: List<com.pride.summoner_searcher_api.dto.MatchDto>, region: String) {
        if (matches.isEmpty()) return
        val outcome = indexedPlayerRepository.indexParticipantsFromMatchesBounded(
            matches = matches,
            region = region,
            fallbackNowEpochMilli = timeProvider.now(),
            transactionTemplate = transactionTemplate,
            maxMatches = MAX_MATCHES_FOR_PARTICIPANT_INDEXING,
            maxPlayersPerBatch = MAX_INDEXED_PLAYERS_PER_BATCH
        )
        if (outcome.upsertedCount > 0L) {
            indexedPlayersUpsertedCounter.addAndGet(outcome.upsertedCount)
        }
        if (outcome.staleSkippedCount > 0L) {
            indexedPlayersStaleSkippedCounter.addAndGet(outcome.staleSkippedCount)
        }
    }
}

