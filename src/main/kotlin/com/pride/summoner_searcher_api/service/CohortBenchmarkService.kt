package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import com.pride.summoner_searcher_api.model.CohortCollectorCheckpointEntity
import com.pride.summoner_searcher_api.repository.CohortBenchmarkCellRepository
import com.pride.summoner_searcher_api.repository.CohortCollectorCheckpointRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.roundToLong

data class RawMemorySample(
    val jvmHeapUsedBytes: Long,
    val jvmHeapCommittedBytes: Long,
    val jvmHeapMaxBytes: Long,
    val jvmNonHeapUsedBytes: Long = 0L,
    val jvmMetaspaceUsedBytes: Long = 0L,
    val jvmMetaspaceCommittedBytes: Long = 0L,
    val jvmCompressedClassSpaceUsedBytes: Long = 0L,
    val jvmCompressedClassSpaceCommittedBytes: Long = 0L,
    val jvmCodeCacheUsedBytes: Long = 0L,
    val jvmCodeCacheCommittedBytes: Long = 0L,
    val directBufferUsedBytes: Long = 0L,
    val directBufferCapacityBytes: Long = 0L,
    val mappedBufferUsedBytes: Long = 0L,
    val jvmThreadCount: Int = 0,
    val cgroupMemoryCurrentBytes: Long? = null,
    val cgroupMemoryMaxBytes: Long? = null
)

interface MemoryStatsProvider {
    fun sample(): RawMemorySample
}

@org.springframework.stereotype.Component
class SystemMemoryStatsProvider : MemoryStatsProvider {
    override fun sample(): RawMemorySample {
        val rt = Runtime.getRuntime()
        val committed = rt.totalMemory().coerceAtLeast(0L)
        val free = rt.freeMemory().coerceAtLeast(0L)
        val used = (committed - free).coerceAtLeast(0L)
        val max = rt.maxMemory().takeIf { it > 0L && it < Long.MAX_VALUE } ?: committed.coerceAtLeast(1L)
        val nonHeap = try {
            java.lang.management.ManagementFactory.getMemoryMXBean().nonHeapMemoryUsage.used.coerceAtLeast(0L)
        } catch (_: Exception) {
            0L
        }

        var metaspaceUsed = 0L
        var metaspaceCommitted = 0L
        var compressedClassUsed = 0L
        var compressedClassCommitted = 0L
        var codeCacheUsed = 0L
        var codeCacheCommitted = 0L
        try {
            for (pool in java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
                val poolName = pool.name ?: continue
                val usage = pool.usage ?: continue
                when {
                    poolName.contains("Metaspace", ignoreCase = true) -> {
                        metaspaceUsed = usage.used.coerceAtLeast(0L)
                        metaspaceCommitted = usage.committed.coerceAtLeast(0L)
                    }
                    poolName.contains("Compressed Class", ignoreCase = true) -> {
                        compressedClassUsed = usage.used.coerceAtLeast(0L)
                        compressedClassCommitted = usage.committed.coerceAtLeast(0L)
                    }
                    poolName.contains("CodeCache", ignoreCase = true) || poolName.contains("CodeHeap", ignoreCase = true) -> {
                        codeCacheUsed += usage.used.coerceAtLeast(0L)
                        codeCacheCommitted += usage.committed.coerceAtLeast(0L)
                    }
                }
            }
        } catch (_: Exception) {
        }

        var directUsed = 0L
        var directCap = 0L
        var mappedUsed = 0L
        try {
            for (bp in java.lang.management.ManagementFactory.getPlatformMXBeans(java.lang.management.BufferPoolMXBean::class.java)) {
                val bpName = bp.name ?: continue
                when {
                    bpName.equals("direct", ignoreCase = true) -> {
                        directUsed = bp.memoryUsed.coerceAtLeast(0L)
                        directCap = bp.totalCapacity.coerceAtLeast(0L)
                    }
                    bpName.equals("mapped", ignoreCase = true) -> {
                        mappedUsed += bp.memoryUsed.coerceAtLeast(0L)
                    }
                }
            }
        } catch (_: Exception) {
        }

        val threadCount = try {
            java.lang.management.ManagementFactory.getThreadMXBean().threadCount.coerceAtLeast(0)
        } catch (_: Exception) {
            0
        }

        val cgroupCurrent = readLongFromFile("/sys/fs/cgroup/memory.current")
            ?: readLongFromFile("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        val cgroupMax = readLongFromFile("/sys/fs/cgroup/memory.max")
            ?: readLongFromFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")?.takeIf { it in 1L..68_719_476_736L }

        return RawMemorySample(
            jvmHeapUsedBytes = used,
            jvmHeapCommittedBytes = committed,
            jvmHeapMaxBytes = max,
            jvmNonHeapUsedBytes = nonHeap,
            jvmMetaspaceUsedBytes = metaspaceUsed,
            jvmMetaspaceCommittedBytes = metaspaceCommitted,
            jvmCompressedClassSpaceUsedBytes = compressedClassUsed,
            jvmCompressedClassSpaceCommittedBytes = compressedClassCommitted,
            jvmCodeCacheUsedBytes = codeCacheUsed,
            jvmCodeCacheCommittedBytes = codeCacheCommitted,
            directBufferUsedBytes = directUsed,
            directBufferCapacityBytes = directCap,
            mappedBufferUsedBytes = mappedUsed,
            jvmThreadCount = threadCount,
            cgroupMemoryCurrentBytes = cgroupCurrent,
            cgroupMemoryMaxBytes = cgroupMax
        )
    }

    private fun readLongFromFile(path: String): Long? {
        return try {
            val file = java.io.File(path)
            if (!file.exists() || !file.canRead()) return null
            val raw = file.readText().trim()
            if (raw.isEmpty() || raw.equals("max", ignoreCase = true)) return null
            raw.toLongOrNull()?.takeIf { it > 0L }
        } catch (_: Exception) {
            null
        }
    }
}

@Service
class CohortBenchmarkService(
    private val redisCacheService: RedisCacheService,
    private val cohortBenchmarkCellRepository: CohortBenchmarkCellRepository,
    private val riotApiRateLimiter: RiotApiRateLimiter,
    private val riotApiService: RiotApiService,
    private val objectMapper: ObjectMapper,
    @Value("\${cohort.network-collection.enabled:false}")
    private val networkCollectionEnabled: Boolean = false,
    private val timeProvider: TimeProvider = SystemTimeProvider(),
    @Value("\${cohort.collector.container-memory-limit-bytes:536870912}")
    private val configuredContainerLimitBytes: Long = DEFAULT_CONTAINER_MEMORY_LIMIT_BYTES,
    @Value("\${cohort.collector.max-memory-utilization-ratio:0.78}")
    private val maxMemoryUtilizationRatio: Double = DEFAULT_MAX_MEMORY_UTILIZATION_RATIO,
    @Value("\${cohort.collector.min-free-memory-bytes:67108864}")
    private val minFreeMemoryBytes: Long = DEFAULT_MIN_FREE_MEMORY_BYTES,
    private val memoryStatsProvider: MemoryStatsProvider = SystemMemoryStatsProvider(),
    @Autowired(required = false)
    private val cohortCollectorCheckpointRepository: CohortCollectorCheckpointRepository? = null
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val lastGcHintAtMs = java.util.concurrent.atomic.AtomicLong(0L)

    companion object {
        const val SOLO_QUEUE_ID = 420
        const val MIN_GAME_DURATION_SECONDS = 600L
        const val MAX_SCAN_KEYS_PER_NAMESPACE = 500
        const val PROFILE_SCAN_CHUNK_SIZE = 10
        const val SNAPSHOT_SCAN_CHUNK_SIZE = 25
        const val MATCH_SCAN_CHUNK_SIZE = 10
        const val MAX_IN_MEMORY_PARTICIPANT_SAMPLES = 6_000
        const val MAX_CHECKPOINT_PROCESSED_PUUIDS = 5_000
        const val MAX_CHECKPOINT_PROCESSED_MATCH_IDS = 10_000
        const val MAX_CHECKPOINT_PENDING_PUUIDS = 500
        const val DEFAULT_CONTAINER_MEMORY_LIMIT_BYTES = 536_870_912L // 512 MB Render container cap
        const val DEFAULT_MAX_MEMORY_UTILIZATION_RATIO = 0.78
        const val DEFAULT_MIN_FREE_MEMORY_BYTES = 67_108_864L // 64 MB minimum free headroom
        const val UNCOMMITTED_HEAP_GC_TRIGGER_BYTES = 33_554_432L // 32 MiB unused committed heap
        val ELIGIBLE_ROLES = setOf("TOP", "MIDDLE", "BOTTOM")
        val ALLOWED_REGIONS = setOf(
            "euw1", "na1", "kr", "eun1", "br1", "jp1", "la1", "la2",
            "oc1", "tr1", "ru", "ph2", "sg2", "th2", "tw2", "vn2", "me1"
        )
        private val PATCH_REGEX = Regex("^\\d{1,2}\\.\\d{1,2}$")
        val TIER_ORDER = listOf(
            "IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM",
            "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER"
        )
        val MAX_RANK_SNAPSHOT_AGE: Duration = Duration.ofDays(7)
        val MAX_MATCH_AGE: Duration = Duration.ofDays(14)
        val MAX_MATCH_TO_SNAPSHOT_SKEW: Duration = Duration.ofDays(7)
        private val BENCHMARK_CACHE_TTL = Duration.ofHours(24)
        private val REPORT_CACHE_TTL = Duration.ofMinutes(15)
        private val RECOMPUTE_GUARD_TTL = Duration.ofMinutes(5)
        private val MATCH_CACHE_TTL = Duration.ofDays(7)
        private val COLLECTOR_LOCK_TTL = Duration.ofMinutes(10)
        val REQUIRED_COLLECTOR_ENDPOINTS = setOf(
            "league-v4-entries-by-puuid",
            "match-v5-ids-by-puuid",
            "match-v5-match-by-id"
        )
    }

    /**
     * Evaluates live JVM heap + Linux container cgroup memory headroom against the 512MB container cap.
     * Triggers a rate-limited GC hint (max once per 10s) when near the cap or when unused committed heap
     * exceeds [UNCOMMITTED_HEAP_GC_TRIGGER_BYTES] (32 MiB) so SerialGC with `-Xms32m -XX:MaxHeapFreeRatio=25 -XX:-ShrinkHeapInSteps`
     * returns unused committed heap pages to the OS before utilization reaches 78%.
     */
    fun getMemoryHeadroomSnapshot(allowGcHintOnPressure: Boolean = true): MemoryHeadroomSnapshotDto {
        val nowMs = timeProvider.now()
        val raw = memoryStatsProvider.sample()
        val snap = buildMemoryHeadroomSnapshot(raw, Instant.ofEpochMilli(nowMs))
        val shouldHintGc = !snap.hasHeadroomForLowPriority ||
            snap.effectiveUtilizationRatio >= 0.70 ||
            snap.jvmHeapUnusedCommittedBytes >= UNCOMMITTED_HEAP_GC_TRIGGER_BYTES
        if (shouldHintGc && allowGcHintOnPressure) {
            val prevGc = lastGcHintAtMs.get()
            if (nowMs - prevGc >= 10_000L && lastGcHintAtMs.compareAndSet(prevGc, nowMs)) {
                System.gc()
                val afterGc = memoryStatsProvider.sample()
                return buildMemoryHeadroomSnapshot(afterGc, Instant.ofEpochMilli(timeProvider.now()))
            }
        }
        return snap
    }

    private fun buildMemoryHeadroomSnapshot(raw: RawMemorySample, measuredAt: Instant): MemoryHeadroomSnapshotDto {
        val safeContainerCap = configuredContainerLimitBytes.coerceAtLeast(1L)
        val safeHeapMax = raw.jvmHeapMaxBytes.coerceAtLeast(1L)
        val safeHeapUsed = raw.jvmHeapUsedBytes.coerceAtLeast(0L)
        val safeHeapCommitted = raw.jvmHeapCommittedBytes.coerceAtLeast(safeHeapUsed)
        val unusedCommittedHeap = (safeHeapCommitted - safeHeapUsed).coerceAtLeast(0L)
        val safeNonHeapUsed = raw.jvmNonHeapUsedBytes.coerceAtLeast(0L)
        val safeDirectUsed = raw.directBufferUsedBytes.coerceAtLeast(0L)

        // Use cgroup container utilization only when running inside a bounded container cgroup (<= 2GB max)
        val isBoundedContainerCgroup = raw.cgroupMemoryMaxBytes != null &&
            raw.cgroupMemoryMaxBytes in 1L..2_147_483_648L &&
            raw.cgroupMemoryCurrentBytes != null &&
            raw.cgroupMemoryCurrentBytes > 0L

        val effectiveLimitBytes = if (isBoundedContainerCgroup) {
            minOf(safeContainerCap, raw.cgroupMemoryMaxBytes!!)
        } else {
            minOf(safeContainerCap, safeHeapMax)
        }

        val effectiveUsedBytes = if (isBoundedContainerCgroup) {
            maxOf(raw.cgroupMemoryCurrentBytes!!, safeHeapUsed)
        } else {
            safeHeapUsed
        }

        val estimatedNativeResidual = if (raw.cgroupMemoryCurrentBytes != null && raw.cgroupMemoryCurrentBytes > 0L) {
            (raw.cgroupMemoryCurrentBytes - safeHeapCommitted - safeNonHeapUsed - safeDirectUsed).coerceAtLeast(0L)
        } else {
            0L
        }

        val heapRatio = round4(safeHeapUsed.toDouble() / safeHeapMax.toDouble())
        val containerRatio = if (isBoundedContainerCgroup) {
            round4(raw.cgroupMemoryCurrentBytes!!.toDouble() / effectiveLimitBytes.coerceAtLeast(1L).toDouble())
        } else {
            round4(safeHeapUsed.toDouble() / effectiveLimitBytes.coerceAtLeast(1L).toDouble())
        }
        val effectiveRatio = maxOf(heapRatio, containerRatio)

        val freeHeapBytes = (safeHeapMax - safeHeapUsed).coerceAtLeast(0L)
        val freeContainerBytes = (effectiveLimitBytes - effectiveUsedBytes).coerceAtLeast(0L)
        val estimatedFree = minOf(freeHeapBytes, freeContainerBytes)

        val clampedMaxRatio = maxMemoryUtilizationRatio.coerceIn(0.10, 0.98)
        val clampedMinFree = minFreeMemoryBytes.coerceAtLeast(0L)
        val hasHeadroom = effectiveRatio < clampedMaxRatio && estimatedFree >= clampedMinFree

        return MemoryHeadroomSnapshotDto(
            measuredAt = measuredAt,
            memoryMeasurementSource = if (isBoundedContainerCgroup) "CGROUP_CONTAINER_AND_JVM" else "JVM_HEAP",
            configuredContainerLimitBytes = safeContainerCap,
            cgroupMemoryCurrentBytes = raw.cgroupMemoryCurrentBytes,
            cgroupMemoryMaxBytes = raw.cgroupMemoryMaxBytes,
            jvmHeapUsedBytes = safeHeapUsed,
            jvmHeapCommittedBytes = safeHeapCommitted,
            jvmHeapUnusedCommittedBytes = unusedCommittedHeap,
            jvmHeapMaxBytes = safeHeapMax,
            jvmNonHeapUsedBytes = safeNonHeapUsed,
            jvmMetaspaceUsedBytes = raw.jvmMetaspaceUsedBytes.coerceAtLeast(0L),
            jvmMetaspaceCommittedBytes = raw.jvmMetaspaceCommittedBytes.coerceAtLeast(0L),
            jvmCompressedClassSpaceUsedBytes = raw.jvmCompressedClassSpaceUsedBytes.coerceAtLeast(0L),
            jvmCompressedClassSpaceCommittedBytes = raw.jvmCompressedClassSpaceCommittedBytes.coerceAtLeast(0L),
            jvmCodeCacheUsedBytes = raw.jvmCodeCacheUsedBytes.coerceAtLeast(0L),
            jvmCodeCacheCommittedBytes = raw.jvmCodeCacheCommittedBytes.coerceAtLeast(0L),
            directBufferUsedBytes = safeDirectUsed,
            directBufferCapacityBytes = raw.directBufferCapacityBytes.coerceAtLeast(0L),
            mappedBufferUsedBytes = raw.mappedBufferUsedBytes.coerceAtLeast(0L),
            jvmThreadCount = raw.jvmThreadCount.coerceAtLeast(0),
            estimatedNativeResidualBytes = estimatedNativeResidual,
            effectiveMemoryUsedBytes = effectiveUsedBytes,
            effectiveMemoryLimitBytes = effectiveLimitBytes,
            jvmHeapUtilizationRatio = heapRatio,
            containerUtilizationRatio = containerRatio,
            effectiveUtilizationRatio = effectiveRatio,
            maxAllowedUtilizationRatio = clampedMaxRatio,
            minRequiredFreeBytes = clampedMinFree,
            estimatedFreeHeadroomBytes = estimatedFree,
            hasHeadroomForLowPriority = hasHeadroom,
            nearCapStopTriggered = !hasHeadroom,
            scanKeyCapPerNamespace = MAX_SCAN_KEYS_PER_NAMESPACE,
            profileScanChunkSize = PROFILE_SCAN_CHUNK_SIZE,
            snapshotScanChunkSize = SNAPSHOT_SCAN_CHUNK_SIZE,
            matchScanChunkSize = MATCH_SCAN_CHUNK_SIZE,
            maxInMemoryParticipantSamples = MAX_IN_MEMORY_PARTICIPANT_SAMPLES
        )
    }

    private fun round4(value: Double): Double = (value * 10_000.0).roundToLong() / 10_000.0

    private data class RankSnapshot(
        val puuid: String,
        val tier: String,
        val snapshotAt: Instant
    )

    private data class ParticipantSample(
        val matchId: String,
        val puuid: String,
        val tier: String,
        val role: String,
        val gameCreation: Long,
        val rankSnapshotAt: Instant,
        val laneCsAt10: Double?,
        val laneOnlyCsPerMin: Double?,
        val totalCsPerMinWithCamps: Double?,
        val timelineTotalCsAt10WithCamps: Double?,
        val goldPerMin: Double?,
        val damagePerMin: Double?,
        val visionScorePerMin: Double?,
        val killParticipation: Double?,
        val deathsPerGame: Double?,
        val timeSpentDeadShare: Double?
    )

    /**
     * Public-safe, cached and recompute-guarded report endpoint entrypoint.
     * - Validates [region] and optional [targetPatch] before touching Redis.
     * - Serves cached report (`cohort:report:{region}:{patch}`) with zero Redis scans and zero DB writes.
     * - Guards cache-miss recomputation with a cooldown lock (`lock:cohort:report:{region}:{patch}`) so
     *   public `GET` requests can never trigger unbounded or repeated scans/persistence.
     */
    fun getPublicCohortReport(
        region: String,
        targetPatch: String? = null
    ): CohortExtractionReportDto {
        val normalizedRegion = region.lowercase().trim()
        val now = Instant.ofEpochMilli(timeProvider.now())
        if (normalizedRegion !in ALLOWED_REGIONS) {
            return buildEmptyGuardedReport(
                region = normalizedRegion,
                now = now,
                reportSource = "INVALID_REGION",
                note = "Unsupported region '$normalizedRegion'; no Redis scan executed."
            )
        }

        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
        if (cleanPatch != null && !PATCH_REGEX.matches(cleanPatch)) {
            return buildEmptyGuardedReport(
                region = normalizedRegion,
                now = now,
                reportSource = "INVALID_PATCH_FORMAT",
                note = "Invalid patch parameter '$cleanPatch' (expected major.minor); no Redis scan executed."
            )
        }

        val rKey = reportCacheKey(normalizedRegion, cleanPatch)
        val cachedReport = redisCacheService.get(rKey, CohortExtractionReportDto::class.java)
        if (cachedReport != null) {
            val freshVerification = riotApiRateLimiter.getVerificationSnapshot(
                region = normalizedRegion,
                networkCollectionEnabled = networkCollectionEnabled
            )
            return cachedReport.copy(
                reportSource = "CACHED_REPORT",
                rateLimitVerification = freshVerification,
                memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)
            )
        }

        val guardKey = recomputeGuardLockKey(normalizedRegion, cleanPatch)
        val acquiredGuard = redisCacheService.setIfAbsent(guardKey, "LOCKED", RECOMPUTE_GUARD_TTL)
        if (!acquiredGuard) {
            val persistedCells = getPersistedCohortCells(normalizedRegion, cleanPatch)
            val fallbackPatch = cleanPatch ?: persistedCells.firstOrNull()?.patch
            return CohortExtractionReportDto(
                reportSource = "GUARDED_COOLDOWN_FALLBACK",
                inventory = CohortCacheInventoryDto(
                    region = normalizedRegion,
                    scannedAt = now,
                    scanKeyCapPerNamespace = MAX_SCAN_KEYS_PER_NAMESPACE,
                    maxRankSnapshotAgeHours = MAX_RANK_SNAPSHOT_AGE.toHours(),
                    maxMatchAgeHours = MAX_MATCH_AGE.toHours(),
                    maxMatchToSnapshotSkewHours = MAX_MATCH_TO_SNAPSHOT_SKEW.toHours(),
                    cachedProfileCount = 0,
                    profilesWithSoloQueueRankSnapshot = 0,
                    cachedChallengerLeaderboardPlayers = 0,
                    totalDistinctRankedPlayersInCache = 0,
                    ranksByTier = emptyMap(),
                    cachedMatchCount = 0,
                    cachedTimelineCount = 0,
                    matchesByPatch = emptyMap(),
                    latestCachedPatch = fallbackPatch,
                    selectedTargetPatch = fallbackPatch,
                    patchSelectionSource = if (cleanPatch != null) "EXPLICIT_TARGET_PATCH" else "PERSISTED_CACHE_FALLBACK",
                    soloQueue420MatchesOnTargetPatch = 0,
                    excludedNonSoloQueueMatches = 0,
                    excludedMissingGameVersionMatches = 0,
                    excludedOtherPatchMatches = 0,
                    excludedShortOrRemakeMatches = 0,
                    topMidBotParticipantCandidates = 0,
                    excludedOtherRoleParticipants = 0,
                    participantsWithRankSnapshot = 0,
                    participantsWithoutRankSnapshot = 0,
                    deduplicatedParticipantEntries = 0,
                    excludedOverPlayerCapSamples = 0,
                    eligibleCohortSamples = persistedCells.sumOf { it.eligibleGames },
                    distinctEligibleCohortPlayers = persistedCells.sumOf { it.distinctPlayers },
                    cellsTotal = persistedCells.size,
                    cellsPassingQualityGate = persistedCells.count { it.passesQualityGate }
                ),
                rateLimitVerification = riotApiRateLimiter.getVerificationSnapshot(
                    region = normalizedRegion,
                    networkCollectionEnabled = networkCollectionEnabled
                ),
                memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                cells = persistedCells,
                methodologyNotes = listOf(
                    "Recompute cooldown active for region '$normalizedRegion'; serving persisted de-identified cells with zero Redis scans and zero DB writes."
                )
            )
        }

        val freshReport = extractCachedCohortsAndReport(
            region = normalizedRegion,
            targetPatch = cleanPatch,
            maxGamesPerPlayer = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
        )
        try {
            redisCacheService.set(rKey, freshReport, REPORT_CACHE_TTL)
            if (cleanPatch == null && freshReport.inventory.selectedTargetPatch != null) {
                redisCacheService.set(
                    reportCacheKey(normalizedRegion, freshReport.inventory.selectedTargetPatch),
                    freshReport,
                    REPORT_CACHE_TTL
                )
            }
        } catch (e: Exception) {
            logger.warn("Failed to cache cohort report for {}: {}", normalizedRegion, e.message)
        }
        return freshReport
    }

    /**
     * Bounded, streaming, non-accumulating cache-only cohort extraction, inventory, and aggregation for [region].
     * - Makes ZERO Riot API calls.
     * - Streams profiles in chunks of [PROFILE_SCAN_CHUNK_SIZE] (20), rank snapshots in chunks of [SNAPSHOT_SCAN_CHUNK_SIZE] (50),
     *   and matches in chunks of [MATCH_SCAN_CHUNK_SIZE] (25) WITHOUT `.flatMap` accumulation across chunks, so full DTOs
     *   are immediately eligible for GC after each small chunk.
     * - Never bulk-loads `MatchTimelineDto` maps into heap; timelines are only fetched one-at-a-time on demand if an eligible
     *   participant lacks `challenges.laneMinionsFirst10Minutes`.
     */
    fun extractCachedCohortsAndReport(
        region: String = "euw1",
        targetPatch: String? = null,
        maxGamesPerPlayer: Int = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
    ): CohortExtractionReportDto {
        val normalizedRegion = region.lowercase().trim()
        val effectiveCap = maxGamesPerPlayer.coerceAtLeast(1)
        val scannedAt = Instant.ofEpochMilli(timeProvider.now())
        val minSnapshotInstant = scannedAt.minus(MAX_RANK_SNAPSHOT_AGE)
        val maxFutureSkewInstant = scannedAt.plus(Duration.ofHours(2))
        val minMatchInstant = scannedAt.minus(MAX_MATCH_AGE)

        // 1. Bounded streaming scan of cached player profiles (chunks of 20, zero cross-chunk accumulation)
        val rankSnapshotsByPuuid = mutableMapOf<String, RankSnapshot>()
        val profileKeys = redisCacheService.scanKeys(
            pattern = "player:profile:$normalizedRegion:*",
            maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE
        )
        var profilesWithSoloQueueRank = 0
        var excludedStaleRankSnapshots = 0

        for (chunk in profileKeys.chunked(PROFILE_SCAN_CHUNK_SIZE)) {
            val chunkProfiles = redisCacheService.multiGet(chunk, SummonerProfileDto::class.java)
            for ((_, profile) in chunkProfiles) {
                if (profile == null) continue
                val puuid = profile.puuid?.takeIf { it.isNotBlank() } ?: continue
                val rank = profile.soloQueueRank ?: continue
                val tier = rank.tier?.uppercase()?.trim()?.takeIf { it.isNotBlank() } ?: continue
                if (rank.queueType != null && rank.queueType != "RANKED_SOLO_5x5") continue
                val snapshotAt = profile.lastRefreshed ?: continue
                if (snapshotAt.isBefore(minSnapshotInstant) || snapshotAt.isAfter(maxFutureSkewInstant)) {
                    excludedStaleRankSnapshots++
                    continue
                }
                profilesWithSoloQueueRank++

                val existing = rankSnapshotsByPuuid[puuid]
                if (existing == null || snapshotAt.isAfter(existing.snapshotAt)) {
                    rankSnapshotsByPuuid[puuid] = RankSnapshot(
                        puuid = puuid,
                        tier = tier,
                        snapshotAt = snapshotAt
                    )
                }
            }
        }

        // Also stream compact cohort rank snapshot keys (chunks of 50, zero cross-chunk accumulation)
        val snapshotKeys = redisCacheService.scanKeys(
            pattern = "cohort:rank:snapshot:$normalizedRegion:*",
            maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE
        )
        for (chunk in snapshotKeys.chunked(SNAPSHOT_SCAN_CHUNK_SIZE)) {
            val chunkSnapshots = redisCacheService.multiGet(chunk, CohortRankSnapshotDto::class.java)
            for ((_, snap) in chunkSnapshots) {
                if (snap == null) continue
                val puuid = snap.puuid.takeIf { it.isNotBlank() } ?: continue
                if (snap.queueType != "RANKED_SOLO_5x5") continue
                val tier = snap.tier.uppercase().trim().takeIf { it.isNotBlank() } ?: continue
                val snapshotAt = snap.snapshotAt
                if (snapshotAt.isBefore(minSnapshotInstant) || snapshotAt.isAfter(maxFutureSkewInstant)) {
                    excludedStaleRankSnapshots++
                    continue
                }
                val existing = rankSnapshotsByPuuid[puuid]
                if (existing == null || snapshotAt.isAfter(existing.snapshotAt)) {
                    rankSnapshotsByPuuid[puuid] = RankSnapshot(
                        puuid = puuid,
                        tier = tier,
                        snapshotAt = snapshotAt
                    )
                }
            }
        }

        // Also scan Challenger leaderboard cache keys for the region (bounded)
        val leaderboardKeys = (
            redisCacheService.scanKeys(
                pattern = "leaderboard:$normalizedRegion:*",
                maxKeys = 50
            ) +
                listOf(
                    "leaderboard:$normalizedRegion::RANKED_SOLO_5x5",
                    "leaderboard:$normalizedRegion:RANKED_SOLO_5x5"
                )
            ).distinct()

        var challengerLeaderboardPlayers = 0
        for (lbKey in leaderboardKeys) {
            val cachedLb = redisCacheService.get(lbKey, CachedLeaderboardDto::class.java) ?: continue
            val snapshotAt = cachedLb.lastRefreshed ?: continue
            val queue = cachedLb.leaderboard?.queue
            if (queue != null && queue != "RANKED_SOLO_5x5") continue
            if (snapshotAt.isBefore(minSnapshotInstant) || snapshotAt.isAfter(maxFutureSkewInstant)) {
                excludedStaleRankSnapshots += cachedLb.leaderboard?.entries?.size ?: 0
                continue
            }
            val lbTier = cachedLb.leaderboard?.tier?.uppercase()?.trim()?.takeIf { it.isNotBlank() } ?: "CHALLENGER"
            for (entry in cachedLb.leaderboard?.entries.orEmpty()) {
                val puuid = entry.puuid?.takeIf { it.isNotBlank() } ?: continue
                challengerLeaderboardPlayers++
                val existing = rankSnapshotsByPuuid[puuid]
                if (existing == null || snapshotAt.isAfter(existing.snapshotAt)) {
                    rankSnapshotsByPuuid[puuid] = RankSnapshot(
                        puuid = puuid,
                        tier = lbTier,
                        snapshotAt = snapshotAt
                    )
                }
            }
        }

        val ranksByTier = rankSnapshotsByPuuid.values
            .groupingBy { it.tier }
            .eachCount()
            .toSortedMap(compareBy { tierIndex(it) })

        // 2. Bounded single-pass streaming scan of cached matches in chunks of 25 (zero full-list accumulation)
        val matchKeys = redisCacheService.scanKeys(
            pattern = "match:$normalizedRegion:*",
            maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE
        )
        val timelineKeys = redisCacheService.scanKeys(
            pattern = "timeline:$normalizedRegion:*",
            maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE
        )
        val timelineKeySet = timelineKeys.toHashSet()

        val cleanTargetPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
        val matchesByPatchCounts = mutableMapOf<String, Int>()
        val soloQueueMatchesByPatch = mutableMapOf<String, Int>()
        var cachedMatchCount = 0
        var highestSoloPatchSeen: String? = null

        var soloQueue420MatchesOnTargetPatch = 0
        var excludedNonSoloQueueMatches = 0
        var excludedMissingGameVersionMatches = 0
        var excludedOtherPatchMatches = 0
        var excludedShortOrRemakeMatches = 0
        var excludedStaleOrUnboundedMatches = 0
        var topMidBotParticipantCandidates = 0
        var excludedOtherRoleParticipants = 0
        var participantsWithRankSnapshot = 0
        var participantsWithoutRankSnapshot = 0
        var excludedParticipantsOutsideSnapshotWindow = 0
        var deduplicatedParticipantEntries = 0

        val seenMatchPuuid = mutableSetOf<Pair<String, String>>()
        val candidateSamples = mutableListOf<ParticipantSample>()

        for (chunk in matchKeys.chunked(MATCH_SCAN_CHUNK_SIZE)) {
            val chunkMatches = redisCacheService.multiGet(chunk, MatchDto::class.java).values.filterNotNull()
            cachedMatchCount += chunkMatches.size

            for (match in chunkMatches) {
                val info = match.info ?: continue
                val patch = extractMajorMinorPatch(info.gameVersion)
                if (patch != null) {
                    matchesByPatchCounts[patch] = (matchesByPatchCounts[patch] ?: 0) + 1
                    if (info.queueId == SOLO_QUEUE_ID) {
                        soloQueueMatchesByPatch[patch] = (soloQueueMatchesByPatch[patch] ?: 0) + 1
                    }
                }

                if (info.queueId != SOLO_QUEUE_ID) {
                    excludedNonSoloQueueMatches++
                    continue
                }
                if (patch == null) {
                    excludedMissingGameVersionMatches++
                    continue
                }

                val activePatch: String = if (cleanTargetPatch != null) {
                    cleanTargetPatch
                } else {
                    val prevBest = highestSoloPatchSeen
                    if (prevBest == null) {
                        highestSoloPatchSeen = patch
                        patch
                    } else {
                        val cmp = patchComparator.compare(patch, prevBest)
                        if (cmp > 0) {
                            // Found a newer Solo Queue patch in stream: fold prior patch's 420 matches into excludedOtherPatchMatches
                            // and discard prior patch's in-memory candidate samples so only the latest patch is retained.
                            val priorPatchMatchCount = soloQueueMatchesByPatch[prevBest] ?: 0
                            excludedOtherPatchMatches += priorPatchMatchCount
                            soloQueue420MatchesOnTargetPatch = 0
                            excludedShortOrRemakeMatches = 0
                            excludedStaleOrUnboundedMatches = 0
                            topMidBotParticipantCandidates = 0
                            excludedOtherRoleParticipants = 0
                            participantsWithRankSnapshot = 0
                            participantsWithoutRankSnapshot = 0
                            excludedParticipantsOutsideSnapshotWindow = 0
                            deduplicatedParticipantEntries = 0
                            seenMatchPuuid.clear()
                            candidateSamples.clear()
                            highestSoloPatchSeen = patch
                            patch
                        } else {
                            prevBest
                        }
                    }
                }

                if (patch != activePatch) {
                    excludedOtherPatchMatches++
                    continue
                }

                val durationSeconds = info.gameDuration ?: 0L
                val isRemakeOrEarlySurrender = durationSeconds < MIN_GAME_DURATION_SECONDS ||
                    info.participants.any { it.gameEndedInEarlySurrender == true || it.teamEarlySurrendered == true }
                if (isRemakeOrEarlySurrender) {
                    excludedShortOrRemakeMatches++
                    continue
                }

                val gameCreation = info.gameCreation ?: 0L
                if (gameCreation <= 0L) {
                    excludedStaleOrUnboundedMatches++
                    continue
                }
                val matchPlayedAt = Instant.ofEpochMilli(gameCreation)
                if (matchPlayedAt.isBefore(minMatchInstant) || matchPlayedAt.isAfter(maxFutureSkewInstant)) {
                    excludedStaleOrUnboundedMatches++
                    continue
                }

                soloQueue420MatchesOnTargetPatch++
                val matchId = match.metadata?.matchId?.takeIf { it.isNotBlank() }
                    ?: info.gameId?.let { "${normalizedRegion.uppercase()}_$it" }
                    ?: continue
                val durationMinutes = durationSeconds.toDouble() / 60.0
                var lazyTimelineLoaded = false
                var lazyTimeline: MatchTimelineDto? = null
                var lazyFrameAt10m: TimelineFrameDto? = null

                val teamKillsByTeamId = info.participants
                    .groupBy { it.teamId ?: 0 }
                    .mapValues { (_, plist) -> plist.sumOf { it.kills ?: 0 } }

                for (participant in info.participants) {
                    val role = resolveNormalizedRole(participant)
                    if (role !in ELIGIBLE_ROLES) {
                        excludedOtherRoleParticipants++
                        continue
                    }
                    topMidBotParticipantCandidates++

                    val puuid = participant.puuid?.takeIf { it.isNotBlank() }
                    if (puuid == null) {
                        participantsWithoutRankSnapshot++
                        continue
                    }

                    // Never invent or infer historical rank when a participant has no fresh timestamped rank snapshot
                    val rankSnapshot = rankSnapshotsByPuuid[puuid]
                    if (rankSnapshot == null) {
                        participantsWithoutRankSnapshot++
                        continue
                    }

                    // Bound match timestamp proximity to rank snapshot so current rank is never projected onto old games
                    val matchToSnapshotDistance = Duration.between(matchPlayedAt, rankSnapshot.snapshotAt).abs()
                    if (matchToSnapshotDistance > MAX_MATCH_TO_SNAPSHOT_SKEW) {
                        excludedParticipantsOutsideSnapshotWindow++
                        continue
                    }
                    participantsWithRankSnapshot++

                    val dedupeKey = matchId to puuid
                    if (!seenMatchPuuid.add(dedupeKey)) {
                        deduplicatedParticipantEntries++
                        continue
                    }

                    val needsTimelineFallback = (
                        participant.challenges?.laneMinionsFirst10Minutes == null ||
                            participant.challenges.jungleCsBefore10Minutes == null
                        ) && "timeline:$normalizedRegion:$matchId" in timelineKeySet

                    if (needsTimelineFallback && !lazyTimelineLoaded) {
                        lazyTimelineLoaded = true
                        val tKey = "timeline:$normalizedRegion:$matchId"
                        lazyTimeline = redisCacheService.get(tKey, MatchTimelineDto::class.java)
                            ?: redisCacheService.multiGet(listOf(tKey), MatchTimelineDto::class.java)[tKey]
                        lazyFrameAt10m = findFrameAt10Minutes(lazyTimeline)
                    }

                    val participantFrame10m = if (lazyFrameAt10m != null) {
                        findParticipantFrame(lazyFrameAt10m, participant, lazyTimeline)
                    } else {
                        null
                    }

                    // Strictly lane-only RAW CS at 10m (primary metric, never percentages, never includes jungle camps)
                    val laneCs10 = participant.challenges?.laneMinionsFirst10Minutes?.toDouble()
                        ?: participantFrame10m?.minionsKilled?.toDouble()

                    // Strictly lane-only CS/min at end of game (totalMinionsKilled is lane minions only in Riot Match-v5)
                    val laneOnlyCsPerMin = participant.totalMinionsKilled?.let {
                        if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                    }

                    // Separate pro-compatible total CS metrics: missing jungle counter MUST stay null (never default to 0)
                    val totalCsPerMinWithCamps = if (
                        participant.totalMinionsKilled != null &&
                        participant.neutralMinionsKilled != null &&
                        durationMinutes > 0.0
                    ) {
                        (participant.totalMinionsKilled + participant.neutralMinionsKilled).toDouble() / durationMinutes
                    } else {
                        null
                    }

                    val timelineTotalCs10WithCamps = if (
                        participantFrame10m?.minionsKilled != null &&
                        participantFrame10m.jungleMinionsKilled != null
                    ) {
                        (participantFrame10m.minionsKilled + participantFrame10m.jungleMinionsKilled).toDouble()
                    } else if (
                        participant.challenges?.laneMinionsFirst10Minutes != null &&
                        participant.challenges.jungleCsBefore10Minutes != null
                    ) {
                        participant.challenges.laneMinionsFirst10Minutes.toDouble() +
                            participant.challenges.jungleCsBefore10Minutes.toDouble()
                    } else {
                        null
                    }

                    val goldPerMin = participant.challenges?.goldPerMinute?.toDouble()
                        ?: participant.goldEarned?.let { if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null }

                    val damagePerMin = participant.challenges?.damagePerMinute?.toDouble()
                        ?: participant.totalDamageDealtToChampions?.let {
                            if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                        }

                    val visionScorePerMin = participant.challenges?.visionScorePerMinute?.toDouble()
                        ?: participant.visionScore?.let {
                            if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                        }

                    val kp = participant.challenges?.killParticipation?.toDouble() ?: run {
                        val teamKills = teamKillsByTeamId[participant.teamId ?: 0] ?: 0
                        if (teamKills > 0 && participant.kills != null && participant.assists != null) {
                            ((participant.kills + participant.assists).toDouble() / teamKills.toDouble()).coerceIn(0.0, 1.0)
                        } else {
                            null
                        }
                    }

                    val deathsPerGame = participant.deaths?.toDouble()
                    val timeSpentDeadShare = participant.totalTimeSpentDead?.let { deadSec ->
                        if (durationSeconds > 0L) (deadSec.toDouble() / durationSeconds.toDouble()).coerceIn(0.0, 1.0) else null
                    }

                    if (candidateSamples.size < MAX_IN_MEMORY_PARTICIPANT_SAMPLES) {
                        candidateSamples.add(
                            ParticipantSample(
                                matchId = matchId,
                                puuid = puuid,
                                tier = rankSnapshot.tier,
                                role = role,
                                gameCreation = gameCreation,
                                rankSnapshotAt = rankSnapshot.snapshotAt,
                                laneCsAt10 = laneCs10,
                                laneOnlyCsPerMin = laneOnlyCsPerMin,
                                totalCsPerMinWithCamps = totalCsPerMinWithCamps,
                                timelineTotalCsAt10WithCamps = timelineTotalCs10WithCamps,
                                goldPerMin = goldPerMin,
                                damagePerMin = damagePerMin,
                                visionScorePerMin = visionScorePerMin,
                                killParticipation = kp,
                                deathsPerGame = deathsPerGame,
                                timeSpentDeadShare = timeSpentDeadShare
                            )
                        )
                    }
                }
            }
        }

        val latestCachedPatch = soloQueueMatchesByPatch.keys.maxWithOrNull(patchComparator)
            ?: matchesByPatchCounts.keys.maxWithOrNull(patchComparator)
        val resolvedPatch = cleanTargetPatch ?: latestCachedPatch
        val patchSelectionSource = when {
            cleanTargetPatch != null -> "EXPLICIT_TARGET_PATCH"
            latestCachedPatch != null -> "LATEST_CACHED_PATCH_IN_REDIS"
            else -> "NO_CACHED_PATCH_FOUND"
        }

        // 3. Group by (tier, role), enforce per-player game cap (newest games first), and compute P25/Median/P75/IQR
        val groupedByCell = candidateSamples.groupBy { it.tier to it.role }
        var totalExcludedOverPlayerCap = 0
        var totalEligibleSamples = 0
        val distinctEligiblePlayers = mutableSetOf<String>()
        val computedCells = mutableListOf<CohortBenchmarkCellDto>()
        val effectivePatchLabel = resolvedPatch ?: "UNKNOWN"

        val sortedCellKeys = groupedByCell.keys.sortedWith(
            compareBy<Pair<String, String>> { tierIndex(it.first) }.thenBy { roleIndex(it.second) }
        )

        for (cellKey in sortedCellKeys) {
            val (tier, role) = cellKey
            val cellCandidates = groupedByCell[cellKey].orEmpty()
            val byPlayer = cellCandidates.groupBy { it.puuid }

            val retainedSamples = mutableListOf<ParticipantSample>()
            var cellExcludedOverCap = 0

            for ((_, playerSamples) in byPlayer) {
                val sortedNewestFirst = playerSamples.sortedWith(
                    compareByDescending<ParticipantSample> { it.gameCreation }.thenByDescending { it.matchId }
                )
                val kept = sortedNewestFirst.take(effectiveCap)
                val dropped = (sortedNewestFirst.size - kept.size).coerceAtLeast(0)
                cellExcludedOverCap += dropped
                retainedSamples.addAll(kept)
            }

            totalExcludedOverPlayerCap += cellExcludedOverCap
            totalEligibleSamples += retainedSamples.size
            retainedSamples.forEach { distinctEligiblePlayers.add(it.puuid) }

            val distinctPlayersCount = byPlayer.keys.size
            val eligibleGamesCount = retainedSamples.size
            val passesGate = distinctPlayersCount >= CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE &&
                eligibleGamesCount >= CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE

            val gateStatus = if (passesGate) {
                "READY_FOR_COLOR_BANDS"
            } else {
                "INSUFFICIENT_SAMPLE (players=$distinctPlayersCount/${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE}, games=$eligibleGamesCount/${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE})"
            }

            val earliestSnapshot = retainedSamples.minOfOrNull { it.rankSnapshotAt }
            val latestSnapshot = retainedSamples.maxOfOrNull { it.rankSnapshotAt }
            val roleLaneCeiling = theoreticalLaneMinionSupplyAt10(resolvedPatch, role)

            val metricsBundle = CohortMetricsBundleDto(
                laneCsAt10 = buildDistribution(
                    metricKey = "laneCsAt10",
                    unit = "lane_cs",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = roleLaneCeiling,
                    samples = retainedSamples,
                    extractor = { it.laneCsAt10 }
                ),
                laneOnlyCsPerMin = buildDistribution(
                    metricKey = "laneOnlyCsPerMin",
                    unit = "lane_cs_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.laneOnlyCsPerMin }
                ),
                totalCsPerMinWithCamps = buildDistribution(
                    metricKey = "totalCsPerMinWithCamps",
                    unit = "total_cs_per_min",
                    includesJungleCamps = true,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.totalCsPerMinWithCamps }
                ),
                timelineTotalCsAt10WithCamps = buildDistribution(
                    metricKey = "timelineTotalCsAt10WithCamps",
                    unit = "total_cs",
                    includesJungleCamps = true,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.timelineTotalCsAt10WithCamps }
                ),
                goldPerMin = buildDistribution(
                    metricKey = "goldPerMin",
                    unit = "gold_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.goldPerMin }
                ),
                damagePerMin = buildDistribution(
                    metricKey = "damagePerMin",
                    unit = "damage_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.damagePerMin }
                ),
                visionScorePerMin = buildDistribution(
                    metricKey = "visionScorePerMin",
                    unit = "vision_score_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.visionScorePerMin }
                ),
                killParticipation = buildDistribution(
                    metricKey = "killParticipation",
                    unit = "ratio_0_to_1",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.killParticipation }
                ),
                deathsPerGame = buildDistribution(
                    metricKey = "deathsPerGame",
                    unit = "deaths",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.deathsPerGame }
                ),
                timeSpentDeadShare = buildDistribution(
                    metricKey = "timeSpentDeadShare",
                    unit = "ratio_0_to_1",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.timeSpentDeadShare }
                )
            )

            computedCells.add(
                CohortBenchmarkCellDto(
                    region = normalizedRegion,
                    patch = effectivePatchLabel,
                    patchSelectionSource = patchSelectionSource,
                    queueId = SOLO_QUEUE_ID,
                    tier = tier,
                    role = role,
                    distinctPlayers = distinctPlayersCount,
                    eligibleGames = eligibleGamesCount,
                    maxGamesPerPlayerCap = effectiveCap,
                    excludedOverPlayerCapGames = cellExcludedOverCap,
                    passesQualityGate = passesGate,
                    colorBandsEnabled = passesGate,
                    qualityGateStatus = gateStatus,
                    earliestRankSnapshotAt = earliestSnapshot,
                    latestRankSnapshotAt = latestSnapshot,
                    computedAt = scannedAt,
                    metrics = metricsBundle
                )
            )
        }

        // 4. Persist compact de-identified cells to PostgreSQL/JPA and Redis only when using canonical game cap
        if (resolvedPatch != null &&
            computedCells.isNotEmpty() &&
            effectiveCap == CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
        ) {
            persistDeidentifiedCells(normalizedRegion, resolvedPatch, computedCells)
        }

        val inventory = CohortCacheInventoryDto(
            region = normalizedRegion,
            scannedAt = scannedAt,
            scanKeyCapPerNamespace = MAX_SCAN_KEYS_PER_NAMESPACE,
            maxRankSnapshotAgeHours = MAX_RANK_SNAPSHOT_AGE.toHours(),
            maxMatchAgeHours = MAX_MATCH_AGE.toHours(),
            maxMatchToSnapshotSkewHours = MAX_MATCH_TO_SNAPSHOT_SKEW.toHours(),
            cachedProfileCount = profileKeys.size,
            profilesWithSoloQueueRankSnapshot = profilesWithSoloQueueRank,
            excludedStaleRankSnapshots = excludedStaleRankSnapshots,
            cachedChallengerLeaderboardPlayers = challengerLeaderboardPlayers,
            totalDistinctRankedPlayersInCache = rankSnapshotsByPuuid.size,
            ranksByTier = ranksByTier,
            cachedMatchCount = cachedMatchCount,
            cachedTimelineCount = timelineKeys.size,
            matchesByPatch = matchesByPatchCounts.toSortedMap(patchComparator.reversed()),
            latestCachedPatch = latestCachedPatch,
            selectedTargetPatch = resolvedPatch,
            patchSelectionSource = patchSelectionSource,
            soloQueue420MatchesOnTargetPatch = soloQueue420MatchesOnTargetPatch,
            excludedNonSoloQueueMatches = excludedNonSoloQueueMatches,
            excludedMissingGameVersionMatches = excludedMissingGameVersionMatches,
            excludedOtherPatchMatches = excludedOtherPatchMatches,
            excludedShortOrRemakeMatches = excludedShortOrRemakeMatches,
            excludedStaleOrUnboundedMatches = excludedStaleOrUnboundedMatches,
            topMidBotParticipantCandidates = topMidBotParticipantCandidates,
            excludedOtherRoleParticipants = excludedOtherRoleParticipants,
            participantsWithRankSnapshot = participantsWithRankSnapshot,
            participantsWithoutRankSnapshot = participantsWithoutRankSnapshot,
            excludedParticipantsOutsideSnapshotWindow = excludedParticipantsOutsideSnapshotWindow,
            deduplicatedParticipantEntries = deduplicatedParticipantEntries,
            excludedOverPlayerCapSamples = totalExcludedOverPlayerCap,
            eligibleCohortSamples = totalEligibleSamples,
            distinctEligibleCohortPlayers = distinctEligiblePlayers.size,
            cellsTotal = computedCells.size,
            cellsPassingQualityGate = computedCells.count { it.passesQualityGate }
        )

        val rateLimitVerification = riotApiRateLimiter.getVerificationSnapshot(
            region = normalizedRegion,
            networkCollectionEnabled = networkCollectionEnabled
        )
        val memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)

        val midCeilingForPatch = theoreticalLaneMinionSupplyAt10(resolvedPatch, "MIDDLE")
        val methodologyNotes = listOf(
            "Bounded streaming cache-only extraction scans at most $MAX_SCAN_KEYS_PER_NAMESPACE Redis keys per namespace in small chunks (profiles=$PROFILE_SCAN_CHUNK_SIZE, snapshots=$SNAPSHOT_SCAN_CHUNK_SIZE, matches=$MATCH_SCAN_CHUNK_SIZE) for region '$normalizedRegion' with zero Riot API requests and zero full-list DTO accumulation.",
            "Only Ranked Solo (queueId=420) matches on selected patch '${resolvedPatch ?: "NONE"}' (source=$patchSelectionSource, latestCachedPatch=${latestCachedPatch ?: "NONE"}) with duration >= 600s and roles TOP, MIDDLE, BOTTOM are included; when inferred from cache, this reflects the latest cached patch in Redis rather than an unverified live Riot patch.",
            "Rank assignment strictly requires a fresh timestamped current-rank snapshot (age <= ${MAX_RANK_SNAPSHOT_AGE.toHours()}h) and recent match timestamp within ${MAX_MATCH_TO_SNAPSHOT_SKEW.toHours()}h of the snapshot (match age <= ${MAX_MATCH_AGE.toHours()}h); historical rank is never invented or projected onto distant games.",
            "Samples are deduplicated by (matchId, puuid) and capped at maxGamesPerPlayer=$effectiveCap newest games per player per (tier, role) cell.",
            "RAW lane CS@10 (laneCsAt10, unit='lane_cs') is the primary CS@10 metric — never percentages and never hardcoded guesses (e.g. 95/100/105). Bands (P25/median/P75) are derived strictly from sufficient actual (tier, role) data.",
            "Per-role lane-only CS@10 theoretical supply ceiling (TOP=114, BOTTOM=114, MIDDLE=$midCeilingForPatch for patch ${resolvedPatch ?: "default"}) is kept strictly separate from pro-compatible total CS reference metrics (timelineTotalCsAt10WithCamps, totalCsPerMinWithCamps where includesJungleCamps=true and missing jungle counters stay null).",
            "Quality gate requires >= ${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE} distinct players AND >= ${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE} games per cell before passesQualityGate / colorBandsEnabled becomes true."
        )

        return CohortExtractionReportDto(
            reportSource = "FRESH_BOUNDED_EXTRACTION",
            inventory = inventory,
            rateLimitVerification = rateLimitVerification,
            memoryHeadroom = memoryHeadroom,
            cells = computedCells,
            methodologyNotes = methodologyNotes
        )
    }

    /**
     * Retrieves persisted de-identified cohort benchmark cells for [region] and optional [patch].
     */
    fun getPersistedCohortCells(region: String, patch: String? = null): List<CohortBenchmarkCellDto> {
        val normalizedRegion = region.lowercase().trim()
        if (!patch.isNullOrBlank()) {
            val cacheKey = benchmarkCacheKey(normalizedRegion, patch.trim())
            val cached = runCatching {
                redisCacheService.get(cacheKey, Array<CohortBenchmarkCellDto>::class.java)
            }.getOrNull()
            if (cached != null && cached.isNotEmpty()) {
                return cached.toList()
            }
            val entities = cohortBenchmarkCellRepository.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(
                region = normalizedRegion,
                patch = patch.trim(),
                queueId = SOLO_QUEUE_ID
            )
            return entities.mapNotNull { entityToDto(it) }
        }

        val allForRegion = cohortBenchmarkCellRepository.findByRegionOrderByPatchDescTierAscRoleAsc(normalizedRegion)
        if (allForRegion.isEmpty()) return emptyList()
        val latestPatch = allForRegion.map { it.patch }.maxWithOrNull(patchComparator) ?: allForRegion.first().patch
        return allForRegion.filter { it.patch == latestPatch }.mapNotNull { entityToDto(it) }
    }

    /**
     * Guarded, low-priority network cohort collection step.
     *
     * Refuses to execute unless:
     * 1. `cohort.network-collection.enabled=true` is explicitly configured, AND
     * 2. Memory headroom is below `maxMemoryUtilizationRatio` (with `>= minFreeMemoryBytes` free), AND
     * 3. Actual Riot `X-App-Rate-Limit` and `X-Method-Rate-Limit` headers have been verified on [region]
     *    with sufficient headroom, no active `429 Retry-After` / `service` window, and no active `HIGH`-priority user requests.
     */
    suspend fun runGuardedLowPriorityCollectionStep(
        region: String = "euw1",
        candidatePuuids: List<String>,
        maxNewMatchesPerPlayer: Int = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
    ): Map<String, Any> {
        val normalizedRegion = region.lowercase().trim()
        val effectiveCap = maxNewMatchesPerPlayer.coerceIn(1, CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER)
        val verification = riotApiRateLimiter.getVerificationSnapshot(
            region = normalizedRegion,
            networkCollectionEnabled = networkCollectionEnabled
        )

        if (!networkCollectionEnabled) {
            return mapOf(
                "executed" to false,
                "reason" to "NETWORK_COLLECTION_DISABLED_BY_CONFIG",
                "region" to normalizedRegion,
                "fetchedMatches" to 0
            )
        }

        val memSnap = getMemoryHeadroomSnapshot()
        if (!memSnap.hasHeadroomForLowPriority) {
            return mapOf(
                "executed" to false,
                "reason" to "PAUSED_MEMORY_HEADROOM",
                "region" to normalizedRegion,
                "effectiveUtilizationRatio" to memSnap.effectiveUtilizationRatio,
                "estimatedFreeHeadroomBytes" to memSnap.estimatedFreeHeadroomBytes,
                "fetchedMatches" to 0
            )
        }

        if (!verification.readyForLowPriorityNetworkCollection) {
            return mapOf(
                "executed" to false,
                "reason" to "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY",
                "region" to normalizedRegion,
                "appLimitsVerifiedFromHeaders" to verification.appLimitsVerifiedFromHeaders,
                "appHasHeadroomForLowPriority" to verification.appHasHeadroomForLowPriority,
                "verifiedMethodEndpoints" to verification.verifiedMethodEndpoints,
                "serviceRateLimitActive" to verification.serviceRateLimitActive,
                "foregroundYieldActive" to verification.foregroundYieldActive,
                "activeUserRequests" to verification.activeUserRequests,
                "retryAfterUntil" to (verification.retryAfterUntil?.toString() ?: "NONE"),
                "fetchedMatches" to 0
            )
        }

        var fetchedMatches = 0
        for (puuid in candidatePuuids.distinct()) {
            if (!getMemoryHeadroomSnapshot().hasHeadroomForLowPriority) {
                logger.info("Low-priority cohort worker pausing on {}: memory headroom near cap", normalizedRegion)
                break
            }
            riotApiRateLimiter.awaitLowPriorityClearance(normalizedRegion)
            val nowVerify = riotApiRateLimiter.getVerificationSnapshot(normalizedRegion, networkCollectionEnabled)
            if (!nowVerify.readyForLowPriorityNetworkCollection) {
                logger.info("Low-priority cohort worker pausing on {}: verification/headroom not ready", normalizedRegion)
                break
            }

            val matchIds = riotApiService.getMatchIdsByPuuidLowPriority(
                puuid = puuid,
                region = normalizedRegion,
                queueId = SOLO_QUEUE_ID,
                count = effectiveCap,
                start = 0
            ).orEmpty()

            for (matchId in matchIds.take(effectiveCap)) {
                val matchKey = "match:$normalizedRegion:$matchId"
                if (redisCacheService.get(matchKey, MatchDto::class.java) != null) {
                    continue
                }
                if (!getMemoryHeadroomSnapshot().hasHeadroomForLowPriority) {
                    logger.info("Low-priority cohort worker pausing before match fetch on {}: memory headroom near cap", normalizedRegion)
                    break
                }
                riotApiRateLimiter.awaitLowPriorityClearance(normalizedRegion)
                val matchVerify = riotApiRateLimiter.getVerificationSnapshot(normalizedRegion, networkCollectionEnabled)
                if (!matchVerify.readyForLowPriorityNetworkCollection) {
                    logger.info("Low-priority cohort worker pausing before match fetch on {}", normalizedRegion)
                    break
                }
                val match = riotApiService.getMatchByIdLowPriority(matchId, normalizedRegion) ?: continue
                redisCacheService.set(matchKey, match, MATCH_CACHE_TTL)
                fetchedMatches++
            }
        }

        return mapOf(
            "executed" to true,
            "reason" to "COMPLETED_LOW_PRIORITY_STEP",
            "region" to normalizedRegion,
            "fetchedMatches" to fetchedMatches
        )
    }

    /**
     * Resumable, checkpointed LOW-priority collector batch for EUW (or specified [region]),
     * targeting current-patch Ranked Solo (`420`), roles `TOP`/`MIDDLE`/`BOTTOM`, all 10 rank tiers,
     * capped at `10` games/player per `(tier, role)` cell, with quality gate `50` players + `200` games per cell.
     *
     * Strictly enforces:
     * - 512MB container memory headroom safety gate (`PAUSED_MEMORY_HEADROOM`) before bulk scans, before each player,
     *   and before each match fetch.
     * - Bounded streaming cache-only inventory & checkpoint initialization first.
     * - Network calls start ONLY when `cohort.network-collection.enabled=true` AND actual App + Method
     *   rate-limit headers and headroom are verified, with no active `429 Retry-After` / `service` backoff
     *   and no active/recent foreground user request (`foregroundYieldActive == false`).
     * - Persists resumable checkpoint state in Redis (`cohort:collector:checkpoint:{region}:{patch}`) after each player
     *   and immediately on any yield/backoff/headroom/memory pause.
     */
    suspend fun runResumableLowPriorityCollectorBatch(
        region: String = "euw1",
        targetPatch: String? = null,
        seedPuuids: List<String> = emptyList(),
        maxPlayersPerBatch: Int = 25,
        maxGamesPerPlayer: Int = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER,
        maxSessionNetworkCalls: Int? = null,
        sessionBaselineNetworkCalls: Int = 0,
        sessionProductiveBatchesBeforeBatch: Int = 0,
        maxSessionProductiveBatches: Int = 0,
        shouldStop: () -> Boolean = { false }
    ): CohortCollectorCheckpointDto {
        val normalizedRegion = region.lowercase().trim()
        val effectiveCap = maxGamesPerPlayer.coerceIn(1, CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER)
        val effectiveBatchLimit = maxPlayersPerBatch.coerceIn(1, 200)
        val nowInstant = Instant.ofEpochMilli(timeProvider.now())
        val cleanRequestedPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }

        val existingCheckpoint = getCollectorCheckpoint(normalizedRegion, cleanRequestedPatch)
            ?: if (cleanRequestedPatch != null) getCollectorCheckpoint(normalizedRegion, null) else null

        // Pre-scan memory safety check: if memory is near the 512MB cap, yield BEFORE scanning Redis or making network calls!
        val preScanMem = getMemoryHeadroomSnapshot()
        if (!preScanMem.hasHeadroomForLowPriority) {
            val pausedCheckpoint = (existingCheckpoint ?: CohortCollectorCheckpointDto(
                region = normalizedRegion,
                targetPatch = cleanRequestedPatch,
                patchSelectionSource = if (cleanRequestedPatch != null) "EXPLICIT_TARGET_PATCH" else "DEFERRED_MEMORY_HEADROOM",
                maxGamesPerPlayerCap = effectiveCap,
                cellProgress = buildAllTargetCellProgress(emptyList()),
                startedAt = nowInstant,
                updatedAt = nowInstant
            )).let { base ->
                base.copy(
                    pausedForMemoryHeadroomCount = base.pausedForMemoryHeadroomCount + 1,
                    sessionProductiveBatchesCompleted = sessionProductiveBatchesBeforeBatch,
                    maxSessionProductiveBatches = maxSessionProductiveBatches,
                    sessionNetworkCallsMade = (base.totalNetworkCallsMade - sessionBaselineNetworkCalls).coerceAtLeast(0),
                    maxSessionNetworkCalls = maxSessionNetworkCalls ?: 0,
                    lastStopReason = "PAUSED_MEMORY_HEADROOM",
                    updatedAt = Instant.ofEpochMilli(timeProvider.now())
                )
            }
            saveCollectorCheckpoint(pausedCheckpoint)
            return pausedCheckpoint
        }

        // 1. Initialize baseline cell progress from compact persisted cells (zero 500-key profile/match scans during batch runs)
        val persistedCells = if (existingCheckpoint == null || existingCheckpoint.cellProgress.isEmpty()) {
            getPersistedCohortCells(normalizedRegion, cleanRequestedPatch)
        } else {
            emptyList()
        }
        var effectivePatch = cleanRequestedPatch
            ?: existingCheckpoint?.targetPatch
            ?: persistedCells.firstOrNull()?.patch
        val patchSource = existingCheckpoint?.patchSelectionSource
            ?: persistedCells.firstOrNull()?.patchSelectionSource
            ?: if (cleanRequestedPatch != null) "EXPLICIT_TARGET_PATCH" else "NO_CACHED_PATCH_FOUND"

        val processedPuuids = existingCheckpoint?.processedPuuids?.take(MAX_CHECKPOINT_PROCESSED_PUUIDS)?.toMutableSet() ?: mutableSetOf()
        val initialProcessedPuuidCount = processedPuuids.size
        val processedMatchIds = existingCheckpoint?.processedMatchIds?.take(MAX_CHECKPOINT_PROCESSED_MATCH_IDS)?.toMutableSet() ?: mutableSetOf()
        val playerCellGameCounts = existingCheckpoint?.playerCellGameCounts?.toMutableMap() ?: mutableMapOf()
        val pendingQueue = LinkedHashSet<String>()
        seedPuuids.map { it.trim() }.filter { it.isNotBlank() && it !in processedPuuids }.forEach {
            if (pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) pendingQueue.add(it)
        }
        existingCheckpoint?.pendingCandidatePuuids?.filter { it !in processedPuuids }?.forEach {
            if (pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) pendingQueue.add(it)
        }

        // Seed additional candidates from cache only when pendingQueue is completely empty
        if (pendingQueue.isEmpty() && getMemoryHeadroomSnapshot().hasHeadroomForLowPriority) {
            val discoveredFromCache = discoverCandidatePuuidsFromCache(normalizedRegion, effectivePatch)
            for (candidate in discoveredFromCache) {
                if (candidate !in processedPuuids && pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) {
                    pendingQueue.add(candidate)
                }
            }
        }

        var cellProgress = if (existingCheckpoint != null && existingCheckpoint.cellProgress.isNotEmpty()) {
            mergeCellProgressWithPlayerCounts(existingCheckpoint.cellProgress, playerCellGameCounts)
        } else {
            mergeCellProgressWithPlayerCounts(buildAllTargetCellProgress(persistedCells), playerCellGameCounts)
        }
        var cellsPassing = cellProgress.values.count { it.passesQualityGate }
        val startedAt = existingCheckpoint?.startedAt ?: nowInstant
        var totalNetworkCalls = existingCheckpoint?.totalNetworkCallsMade ?: 0
        var fetchedRankSnapshots = existingCheckpoint?.fetchedRankSnapshots ?: 0
        var fetchedMatchIdLists = existingCheckpoint?.fetchedMatchIdLists ?: 0
        var fetchedMatches = existingCheckpoint?.fetchedMatches ?: 0
        var yieldedToForeground = existingCheckpoint?.yieldedToForegroundCount ?: 0
        var pausedForRetryAfter = existingCheckpoint?.pausedForRetryAfterCount ?: 0
        var pausedForHeadroom = existingCheckpoint?.pausedForHeadroomCount ?: 0
        var pausedForMemoryHeadroom = existingCheckpoint?.pausedForMemoryHeadroomCount ?: 0

        fun currentSessionNetworkCalls(): Int = (totalNetworkCalls - sessionBaselineNetworkCalls).coerceAtLeast(0)

        fun currentSessionProductiveBatches(): Int {
            return if (processedPuuids.size > initialProcessedPuuidCount) {
                sessionProductiveBatchesBeforeBatch + 1
            } else {
                sessionProductiveBatchesBeforeBatch
            }
        }

        fun buildCurrentCheckpoint(stopReason: String): CohortCollectorCheckpointDto {
            return CohortCollectorCheckpointDto(
                region = normalizedRegion,
                targetPatch = effectivePatch,
                patchSelectionSource = patchSource,
                queueId = SOLO_QUEUE_ID,
                roles = listOf("TOP", "MIDDLE", "BOTTOM"),
                maxGamesPerPlayerCap = effectiveCap,
                targetPlayersPerCell = CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE,
                targetGamesPerCell = CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE,
                processedPuuids = processedPuuids.take(MAX_CHECKPOINT_PROCESSED_PUUIDS).toSet(),
                pendingCandidatePuuids = pendingQueue.filter { it !in processedPuuids }.take(MAX_CHECKPOINT_PENDING_PUUIDS),
                processedMatchIds = processedMatchIds.take(MAX_CHECKPOINT_PROCESSED_MATCH_IDS).toSet(),
                playerCellGameCounts = playerCellGameCounts.toMap(),
                cellProgress = cellProgress,
                cellsPassingQualityGate = cellsPassing,
                targetCellsCount = TIER_ORDER.size * ELIGIBLE_ROLES.size,
                totalNetworkCallsMade = totalNetworkCalls,
                fetchedRankSnapshots = fetchedRankSnapshots,
                fetchedMatchIdLists = fetchedMatchIdLists,
                fetchedMatches = fetchedMatches,
                yieldedToForegroundCount = yieldedToForeground,
                pausedForRetryAfterCount = pausedForRetryAfter,
                pausedForHeadroomCount = pausedForHeadroom,
                pausedForMemoryHeadroomCount = pausedForMemoryHeadroom,
                sessionProductiveBatchesCompleted = currentSessionProductiveBatches(),
                maxSessionProductiveBatches = maxSessionProductiveBatches,
                sessionNetworkCallsMade = currentSessionNetworkCalls(),
                maxSessionNetworkCalls = maxSessionNetworkCalls ?: 0,
                lastStopReason = stopReason,
                startedAt = startedAt,
                updatedAt = Instant.ofEpochMilli(timeProvider.now())
            )
        }

        if (shouldStop()) {
            val stoppedCheckpoint = buildCurrentCheckpoint("STOPPED_BY_OPERATOR")
            saveCollectorCheckpoint(stoppedCheckpoint)
            return stoppedCheckpoint
        }

        // 2. If all 30 target (tier, role) cells already pass 50 players + 200 games, stop with 0 network calls
        if (cellsPassing >= TIER_ORDER.size * ELIGIBLE_ROLES.size) {
            val doneCheckpoint = buildCurrentCheckpoint("ALL_TARGET_CELLS_SATISFIED")
            saveCollectorCheckpoint(doneCheckpoint)
            return doneCheckpoint
        }

        // 3. Enforce network-collection config gate
        if (!networkCollectionEnabled) {
            val disabledCheckpoint = buildCurrentCheckpoint("NETWORK_COLLECTION_DISABLED_BY_CONFIG")
            saveCollectorCheckpoint(disabledCheckpoint)
            return disabledCheckpoint
        }

        // 4. Enforce Session Network Call Cap + Memory Headroom + Actual App + Method + Service rate-limit header & headroom verification
        fun evaluateLivePauseReason(requiredEndpoint: String? = null): String? {
            if (shouldStop()) {
                return "STOPPED_BY_OPERATOR"
            }
            if (maxSessionNetworkCalls != null && currentSessionNetworkCalls() >= maxSessionNetworkCalls) {
                return "SESSION_NETWORK_CALL_CAP_REACHED"
            }
            val memSnap = getMemoryHeadroomSnapshot()
            if (!memSnap.hasHeadroomForLowPriority) {
                pausedForMemoryHeadroom++
                return "PAUSED_MEMORY_HEADROOM"
            }
            val reqSet = if (requiredEndpoint != null) setOf(requiredEndpoint) else emptySet()
            val snap = riotApiRateLimiter.getVerificationSnapshot(
                region = normalizedRegion,
                networkCollectionEnabled = networkCollectionEnabled,
                requiredEndpoints = reqSet
            )
            if (snap.foregroundYieldActive || snap.activeUserRequests > 0) {
                yieldedToForeground++
                return "PAUSED_FOREGROUND_USER_REQUEST"
            }
            if (snap.serviceRateLimitActive || snap.retryAfterUntil != null) {
                pausedForRetryAfter++
                return "PAUSED_RETRY_AFTER_BACKOFF"
            }
            if (!snap.appLimitsVerifiedFromHeaders || snap.verifiedMethodEndpoints.isEmpty()) {
                return "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY"
            }
            if (!snap.appHasHeadroomForLowPriority || snap.methodLimitsByEndpoint.values.any { !it.hasHeadroomForLowPriority }) {
                pausedForHeadroom++
                return "PAUSED_RATE_LIMIT_HEADROOM"
            }
            if (requiredEndpoint != null && requiredEndpoint in snap.missingRequiredMethodEndpoints) {
                return "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY"
            }
            return null
        }

        val initialPauseReason = evaluateLivePauseReason()
        if (initialPauseReason != null) {
            val pausedCheckpoint = buildCurrentCheckpoint(initialPauseReason)
            saveCollectorCheckpoint(pausedCheckpoint)
            return pausedCheckpoint
        }

        if (pendingQueue.isEmpty()) {
            val emptyCheckpoint = buildCurrentCheckpoint("NO_CANDIDATES_REMAINING")
            saveCollectorCheckpoint(emptyCheckpoint)
            return emptyCheckpoint
        }

        // 5. Process up to maxPlayersPerBatch candidates at LOW priority, checkpointing after each player
        var playersProcessedThisBatch = 0
        var stopReason = "BATCH_BUDGET_REACHED_RESUMABLE"
        val minSnapshotInstant = Instant.ofEpochMilli(timeProvider.now()).minus(MAX_RANK_SNAPSHOT_AGE)

        val candidatesThisBatch = pendingQueue.filter { it !in processedPuuids }.take(effectiveBatchLimit)
        outerLoop@ for (puuid in candidatesThisBatch) {
            val prePlayerPause = evaluateLivePauseReason()
            if (prePlayerPause != null) {
                stopReason = prePlayerPause
                break@outerLoop
            }

            // Step A: Resolve or fetch fresh Solo Queue rank snapshot
            var playerTier = getFreshCachedTierForPuuid(normalizedRegion, puuid, minSnapshotInstant)
            if (playerTier == null) {
                val rankPause = evaluateLivePauseReason("league-v4-entries-by-puuid")
                if (rankPause != null) {
                    stopReason = rankPause
                    break@outerLoop
                }
                val rankAttempts = java.util.concurrent.atomic.AtomicInteger(0)
                val rankFailure = java.util.concurrent.atomic.AtomicReference<String?>(null)
                val leagueEntry = riotApiService.fetchLeagueRankLowPriority(
                    puuid = puuid,
                    region = normalizedRegion,
                    maxRetries = 0,
                    httpAttemptCounter = rankAttempts,
                    failureReasonHolder = rankFailure
                )
                totalNetworkCalls += rankAttempts.get().coerceAtLeast(1)
                fetchedRankSnapshots++
                val postRankSnap = getLiveRateLimitVerification(normalizedRegion)
                if (rankFailure.get() == "HTTP_429_TOO_MANY_REQUESTS" || postRankSnap.serviceRateLimitActive || postRankSnap.retryAfterUntil != null) {
                    pausedForRetryAfter++
                    stopReason = "PAUSED_RETRY_AFTER_BACKOFF"
                    break@outerLoop
                }
                if (rankFailure.get() == "NETWORK_TIMEOUT") {
                    stopReason = "PAUSED_NETWORK_TIMEOUT"
                    break@outerLoop
                }
                val fetchedTier = leagueEntry?.tier?.uppercase()?.trim()?.takeIf { it in TIER_ORDER }
                if (fetchedTier != null && leagueEntry.queueType == "RANKED_SOLO_5x5") {
                    cacheRankSnapshot(
                        region = normalizedRegion,
                        puuid = puuid,
                        tier = fetchedTier,
                        rank = leagueEntry.rank,
                        leaguePoints = leagueEntry.leaguePoints,
                        snapshotAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                    playerTier = fetchedTier
                } else {
                    // Unranked in Solo Queue; mark processed and checkpoint
                    if (processedPuuids.size < MAX_CHECKPOINT_PROCESSED_PUUIDS) processedPuuids.add(puuid)
                    pendingQueue.remove(puuid)
                    playersProcessedThisBatch++
                    saveCollectorCheckpoint(buildCurrentCheckpoint("IN_PROGRESS"))
                    continue@outerLoop
                }
            }

            // Step B: Skip player if all 3 roles in their tier already pass quality gate (50 players + 200 games)
            // or if the player already reached the 10-game cap in every under-filled role of their tier
            val underfilledRolesForTier = ELIGIBLE_ROLES.filter { role ->
                val cell = cellProgress["$playerTier:$role"]
                val passes = cell?.passesQualityGate == true
                val playerGames = playerCellGameCounts["$playerTier:$role:$puuid"] ?: 0
                !passes && playerGames < effectiveCap
            }
            if (underfilledRolesForTier.isEmpty()) {
                if (processedPuuids.size < MAX_CHECKPOINT_PROCESSED_PUUIDS) processedPuuids.add(puuid)
                pendingQueue.remove(puuid)
                playersProcessedThisBatch++
                saveCollectorCheckpoint(buildCurrentCheckpoint("IN_PROGRESS"))
                continue@outerLoop
            }

            // Step C: Fetch up to effectiveCap (10) recent Solo Queue match IDs at LOW priority
            val matchListPause = evaluateLivePauseReason("match-v5-ids-by-puuid")
            if (matchListPause != null) {
                stopReason = matchListPause
                break@outerLoop
            }
            val idsAttempts = java.util.concurrent.atomic.AtomicInteger(0)
            val idsFailure = java.util.concurrent.atomic.AtomicReference<String?>(null)
            val matchIds = riotApiService.getMatchIdsByPuuidLowPriority(
                puuid = puuid,
                region = normalizedRegion,
                queueId = SOLO_QUEUE_ID,
                count = effectiveCap,
                start = 0,
                maxRetries = 0,
                httpAttemptCounter = idsAttempts,
                failureReasonHolder = idsFailure
            ).orEmpty()
            totalNetworkCalls += idsAttempts.get().coerceAtLeast(1)
            fetchedMatchIdLists++
            val postIdsSnap = getLiveRateLimitVerification(normalizedRegion)
            if (idsFailure.get() == "HTTP_429_TOO_MANY_REQUESTS" || postIdsSnap.serviceRateLimitActive || postIdsSnap.retryAfterUntil != null) {
                pausedForRetryAfter++
                stopReason = "PAUSED_RETRY_AFTER_BACKOFF"
                break@outerLoop
            }
            if (idsFailure.get() == "NETWORK_TIMEOUT") {
                stopReason = "PAUSED_NETWORK_TIMEOUT"
                break@outerLoop
            }

            // Step D: Inspect/fetch matches on current patch, enforcing 10-game per-player cap
            val ownMatchIdSet = matchIds.take(effectiveCap).toSet()
            val candidateMatchIds = if (ownMatchIdSet.size < effectiveCap && processedMatchIds.isNotEmpty()) {
                (ownMatchIdSet + processedMatchIds.filter { it !in ownMatchIdSet }.takeLast(effectiveCap)).take(effectiveCap)
            } else {
                ownMatchIdSet.toList()
            }
            matchLoop@ for (matchId in candidateMatchIds) {
                val isOwnListedMatch = matchId in ownMatchIdSet
                val allRolesCappedForPlayer = ELIGIBLE_ROLES.all { role ->
                    (playerCellGameCounts["$playerTier:$role:$puuid"] ?: 0) >= effectiveCap
                }
                if (allRolesCappedForPlayer) {
                    break@matchLoop
                }

                val matchKey = "match:$normalizedRegion:$matchId"
                var match = redisCacheService.get(matchKey, MatchDto::class.java)
                if (match == null) {
                    if (!isOwnListedMatch) {
                        continue@matchLoop
                    }
                    val matchFetchPause = evaluateLivePauseReason("match-v5-match-by-id")
                    if (matchFetchPause != null) {
                        stopReason = matchFetchPause
                        break@outerLoop
                    }
                    val matchAttempts = java.util.concurrent.atomic.AtomicInteger(0)
                    val matchFailure = java.util.concurrent.atomic.AtomicReference<String?>(null)
                    match = riotApiService.getMatchByIdLowPriority(
                        matchId = matchId,
                        region = normalizedRegion,
                        maxRetries = 0,
                        httpAttemptCounter = matchAttempts,
                        failureReasonHolder = matchFailure
                    )
                    totalNetworkCalls += matchAttempts.get().coerceAtLeast(1)
                    val postMatchSnap = getLiveRateLimitVerification(normalizedRegion)
                    if (matchFailure.get() == "HTTP_429_TOO_MANY_REQUESTS" || postMatchSnap.serviceRateLimitActive || postMatchSnap.retryAfterUntil != null) {
                        pausedForRetryAfter++
                        stopReason = "PAUSED_RETRY_AFTER_BACKOFF"
                        break@outerLoop
                    }
                    if (matchFailure.get() == "NETWORK_TIMEOUT") {
                        stopReason = "PAUSED_NETWORK_TIMEOUT"
                        break@outerLoop
                    }
                    if (match != null) {
                        redisCacheService.set(matchKey, match, MATCH_CACHE_TTL)
                        fetchedMatches++
                    }
                }
                if (isOwnListedMatch && processedMatchIds.size < MAX_CHECKPOINT_PROCESSED_MATCH_IDS) {
                    processedMatchIds.add(matchId)
                }
                val info = match?.info ?: continue@matchLoop
                if (!isOwnListedMatch && info.participants.none { it.puuid == puuid }) {
                    continue@matchLoop
                }
                val matchPatch = extractMajorMinorPatch(info.gameVersion)
                if (effectivePatch == null && matchPatch != null) {
                    effectivePatch = matchPatch
                }
                // Match IDs are ordered newest-first; stop inspecting older matches once we cross into an older patch
                if (effectivePatch != null && matchPatch != null && patchComparator.compare(matchPatch, effectivePatch!!) < 0) {
                    if (isOwnListedMatch) break@matchLoop else continue@matchLoop
                }
                if (info.queueId != SOLO_QUEUE_ID || (info.gameDuration ?: 0L) < MIN_GAME_DURATION_SECONDS) {
                    continue@matchLoop
                }
                if (effectivePatch != null && matchPatch != effectivePatch) {
                    continue@matchLoop
                }

                for (participant in info.participants) {
                    val role = resolveNormalizedRole(participant)
                    if (role !in ELIGIBLE_ROLES) continue
                    val pPuuid = participant.puuid?.takeIf { it.isNotBlank() } ?: continue
                    if (pPuuid !in processedPuuids && pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) {
                        pendingQueue.add(pPuuid)
                    }
                    if (pPuuid == puuid) {
                        val countKey = "$playerTier:$role:$puuid"
                        val currentCount = playerCellGameCounts[countKey] ?: 0
                        if (currentCount < effectiveCap) {
                            playerCellGameCounts[countKey] = currentCount + 1
                        }
                    }
                }
            }

            if (processedPuuids.size < MAX_CHECKPOINT_PROCESSED_PUUIDS) {
                processedPuuids.add(puuid)
            }
            pendingQueue.remove(puuid)
            playersProcessedThisBatch++
            cellProgress = mergeCellProgressWithPlayerCounts(cellProgress, playerCellGameCounts)
            cellsPassing = cellProgress.values.count { it.passesQualityGate }
            saveCollectorCheckpoint(buildCurrentCheckpoint("IN_PROGRESS"))
        }

        // 6. Update cell progress incrementally in O(1) memory from playerCellGameCounts (zero full-cache profile/match scans)
        if (playersProcessedThisBatch > 0 || fetchedMatches > (existingCheckpoint?.fetchedMatches ?: 0)) {
            cellProgress = mergeCellProgressWithPlayerCounts(cellProgress, playerCellGameCounts)
            cellsPassing = cellProgress.values.count { it.passesQualityGate }
        }

        if (cellsPassing >= TIER_ORDER.size * ELIGIBLE_ROLES.size) {
            stopReason = "ALL_TARGET_CELLS_SATISFIED"
        } else if (maxSessionNetworkCalls != null && currentSessionNetworkCalls() >= maxSessionNetworkCalls &&
            (stopReason == "BATCH_BUDGET_REACHED_RESUMABLE" || stopReason == "SESSION_NETWORK_CALL_CAP_REACHED")
        ) {
            stopReason = "SESSION_NETWORK_CALL_CAP_REACHED"
        } else if (stopReason == "BATCH_BUDGET_REACHED_RESUMABLE" && pendingQueue.none { it !in processedPuuids }) {
            if (getMemoryHeadroomSnapshot(allowGcHintOnPressure = false).hasHeadroomForLowPriority) {
                for (candidate in discoverCandidatePuuidsFromCache(normalizedRegion, effectivePatch)) {
                    if (candidate !in processedPuuids && pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) {
                        pendingQueue.add(candidate)
                    }
                }
            }
            if (pendingQueue.none { it !in processedPuuids }) {
                stopReason = "NO_CANDIDATES_REMAINING"
            } else if (maxSessionProductiveBatches > 0 && currentSessionProductiveBatches() >= maxSessionProductiveBatches) {
                stopReason = "SESSION_BATCH_CAP_REACHED"
            }
        } else if (stopReason == "BATCH_BUDGET_REACHED_RESUMABLE" && maxSessionProductiveBatches > 0 && currentSessionProductiveBatches() >= maxSessionProductiveBatches) {
            stopReason = "SESSION_BATCH_CAP_REACHED"
        }

        val finalCheckpoint = buildCurrentCheckpoint(stopReason)
        saveCollectorCheckpoint(finalCheckpoint)
        return finalCheckpoint
    }

    fun isValidRegion(region: String): Boolean = region.lowercase().trim() in ALLOWED_REGIONS

    fun isValidPatchOrNull(patch: String?): Boolean {
        val clean = patch?.trim()?.takeIf { it.isNotBlank() } ?: return true
        return PATCH_REGEX.matches(clean)
    }

    fun getLiveRateLimitVerification(region: String): RateLimitVerificationSnapshotDto {
        val normalizedRegion = region.lowercase().trim()
        return riotApiRateLimiter.getVerificationSnapshot(
            region = normalizedRegion,
            networkCollectionEnabled = networkCollectionEnabled,
            requiredEndpoints = REQUIRED_COLLECTOR_ENDPOINTS
        )
    }

    fun acquireCollectorProcessLock(region: String): Boolean {
        val normalizedRegion = region.lowercase().trim()
        return redisCacheService.setIfAbsent("lock:cohort:collector:active:$normalizedRegion", "ACTIVE", COLLECTOR_LOCK_TTL)
    }

    fun refreshCollectorProcessLock(region: String) {
        val normalizedRegion = region.lowercase().trim()
        redisCacheService.set("lock:cohort:collector:active:$normalizedRegion", "ACTIVE", COLLECTOR_LOCK_TTL)
    }

    fun releaseCollectorProcessLock(region: String) {
        val normalizedRegion = region.lowercase().trim()
        redisCacheService.delete("lock:cohort:collector:active:$normalizedRegion")
    }

    fun acquireCollectorProbeLock(region: String, ttl: Duration = Duration.ofSeconds(30)): Boolean {
        val normalizedRegion = region.lowercase().trim()
        return redisCacheService.setIfAbsent("lock:cohort:collector:probe:$normalizedRegion", "PROBING", ttl)
    }

    fun releaseCollectorProbeLock(region: String) {
        val normalizedRegion = region.lowercase().trim()
        redisCacheService.delete("lock:cohort:collector:probe:$normalizedRegion")
    }

    /**
     * Guarded, bounded diagnostic probe from a cached regional seed (`euw1` by default).
     * - Purpose: Observe real Riot `X-App-Rate-Limit`, `X-App-Rate-Limit-Count`, `X-Method-Rate-Limit`, and
     *   `X-Method-Rate-Limit-Count` headers in the SAME backend JVM process BEFORE enabling bulk network collection
     *   (`cohort.network-collection.enabled` can remain `false`).
     * - Strictly bounded to AT MOST 1 actual HTTP attempt (`maxRetries = 0`, zero hidden retries) per missing required endpoint
     *   (`league-v4-entries-by-puuid`, `match-v5-ids-by-puuid`, `match-v5-match-by-id` -> max 3 HTTP attempts total;
     *   0 HTTP attempts for any endpoint already verified in [RiotApiRateLimiter]).
     * - Immediately stops on the first `429 TooManyRequests` honoring `Retry-After` (with zero hidden retries),
     *   or aborts before any call if memory headroom is near cap (`PAUSED_MEMORY_HEADROOM`), if foreground user traffic
     *   is active (`foregroundYieldActive` / `activeUserRequests > 0`), if a `429 Retry-After` / `service` backoff is active,
     *   or if verified buckets lack LOW-priority headroom.
     * - Reports actual HTTP attempts (`totalHttpAttemptsMade` and `httpAttemptsByEndpoint`) separately from wrapper calls.
     */
    suspend fun runBoundedDiagnosticRateLimitProbe(
        region: String = "euw1",
        targetPatch: String? = null,
        explicitSeedPuuid: String? = null,
        explicitSeedMatchId: String? = null
    ): CohortRateLimitProbeResultDto {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
        val nowInstant = Instant.ofEpochMilli(timeProvider.now())
        val zeroAttemptsMap = REQUIRED_COLLECTOR_ENDPOINTS.associateWith { 0 }

        if (!isValidRegion(normalizedRegion) || !isValidPatchOrNull(cleanPatch)) {
            return CohortRateLimitProbeResultDto(
                region = normalizedRegion,
                probeStatus = "INVALID_PARAMETERS",
                maxCallsPerEndpoint = 1,
                maxHttpAttemptsPerEndpoint = 1,
                maxRetriesConfigured = 0,
                totalProbeCallsMade = 0,
                totalHttpAttemptsMade = 0,
                httpAttemptsByEndpoint = zeroAttemptsMap,
                stoppedOnRateLimit429 = false,
                probedAt = nowInstant,
                rateLimitVerification = getLiveRateLimitVerification(normalizedRegion),
                memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                notes = listOf("Invalid region or patch format; zero Redis or Riot HTTP attempts executed.")
            )
        }

        // Pre-probe memory safety gate: refuse to scan or probe if memory is near the 512MB container cap
        val initialMemSnap = getMemoryHeadroomSnapshot()
        if (!initialMemSnap.hasHeadroomForLowPriority) {
            val existingCp = getCollectorCheckpoint(normalizedRegion, cleanPatch)
                ?: if (cleanPatch != null) getCollectorCheckpoint(normalizedRegion, null) else null
            val memPausedCp = existingCp?.copy(
                pausedForMemoryHeadroomCount = existingCp.pausedForMemoryHeadroomCount + 1,
                lastStopReason = "PAUSED_MEMORY_HEADROOM",
                updatedAt = Instant.ofEpochMilli(timeProvider.now())
            )
            if (memPausedCp != null) {
                saveCollectorCheckpoint(memPausedCp)
            }
            return CohortRateLimitProbeResultDto(
                region = normalizedRegion,
                probeStatus = "PAUSED_MEMORY_HEADROOM",
                maxCallsPerEndpoint = 1,
                maxHttpAttemptsPerEndpoint = 1,
                maxRetriesConfigured = 0,
                totalProbeCallsMade = 0,
                totalHttpAttemptsMade = 0,
                httpAttemptsByEndpoint = zeroAttemptsMap,
                stoppedOnRateLimit429 = false,
                endpointOutcomes = REQUIRED_COLLECTOR_ENDPOINTS.associateWith { "PAUSED_MEMORY_HEADROOM" },
                probedAt = Instant.ofEpochMilli(timeProvider.now()),
                rateLimitVerification = getLiveRateLimitVerification(normalizedRegion),
                memoryHeadroom = initialMemSnap,
                checkpoint = memPausedCp,
                notes = listOf(
                    "Memory headroom safety gate triggered (effectiveUtilizationRatio=${initialMemSnap.effectiveUtilizationRatio} >= ${initialMemSnap.maxAllowedUtilizationRatio} or free=${initialMemSnap.estimatedFreeHeadroomBytes} < ${initialMemSnap.minRequiredFreeBytes}); 0 HTTP attempts made."
                )
            )
        }

        // 1. Initialize baseline checkpoint from compact persisted cells (zero full-cache profile/match scans during probe)
        var checkpoint = getCollectorCheckpoint(normalizedRegion, cleanPatch)
            ?: if (cleanPatch != null) getCollectorCheckpoint(normalizedRegion, null) else null
        if (checkpoint == null) {
            val persistedCells = getPersistedCohortCells(normalizedRegion, cleanPatch)
            val effectivePatch = cleanPatch ?: persistedCells.firstOrNull()?.patch
            val patchSource = persistedCells.firstOrNull()?.patchSelectionSource
                ?: if (cleanPatch != null) "EXPLICIT_TARGET_PATCH" else "PERSISTED_CELLS_OR_DEFERRED"
            val candidates = discoverCandidatePuuidsFromCache(normalizedRegion, effectivePatch).take(MAX_CHECKPOINT_PENDING_PUUIDS)
            val cellProgress = buildAllTargetCellProgress(persistedCells)
            checkpoint = CohortCollectorCheckpointDto(
                region = normalizedRegion,
                targetPatch = effectivePatch,
                patchSelectionSource = patchSource,
                queueId = SOLO_QUEUE_ID,
                roles = listOf("TOP", "MIDDLE", "BOTTOM"),
                maxGamesPerPlayerCap = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER,
                targetPlayersPerCell = CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE,
                targetGamesPerCell = CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE,
                processedPuuids = emptySet(),
                pendingCandidatePuuids = candidates,
                processedMatchIds = emptySet(),
                playerCellGameCounts = emptyMap(),
                cellProgress = cellProgress,
                cellsPassingQualityGate = cellProgress.values.count { it.passesQualityGate },
                targetCellsCount = TIER_ORDER.size * ELIGIBLE_ROLES.size,
                totalNetworkCallsMade = 0,
                lastStopReason = "CACHE_BASELINE_INITIALIZED",
                startedAt = nowInstant,
                updatedAt = nowInstant
            )
            saveCollectorCheckpoint(checkpoint)
        }

        // 2. If all required App + Method headers are already verified in this JVM, make 0 calls
        val preSnap = getLiveRateLimitVerification(normalizedRegion)
        if (preSnap.appLimitsVerifiedFromHeaders && preSnap.missingRequiredMethodEndpoints.isEmpty()) {
            return CohortRateLimitProbeResultDto(
                region = normalizedRegion,
                probeStatus = "ALREADY_VERIFIED_ZERO_CALLS",
                seedSource = "NONE_NEEDED",
                seedPuuidUsed = false,
                seedMatchIdUsed = false,
                maxCallsPerEndpoint = 1,
                maxHttpAttemptsPerEndpoint = 1,
                maxRetriesConfigured = 0,
                totalProbeCallsMade = 0,
                totalHttpAttemptsMade = 0,
                httpAttemptsByEndpoint = zeroAttemptsMap,
                stoppedOnRateLimit429 = false,
                endpointOutcomes = REQUIRED_COLLECTOR_ENDPOINTS.associateWith { "SKIPPED_ALREADY_VERIFIED" },
                probedAt = Instant.ofEpochMilli(timeProvider.now()),
                rateLimitVerification = preSnap,
                memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                checkpoint = checkpoint,
                notes = listOf(
                    "All required App and Method rate-limit headers are already verified in-process; 0 Riot HTTP attempts made."
                )
            )
        }

        // 3. Resolve a single seed PUUID (and optional cached match ID) from EUW Redis cache
        val (seedPuuid, seedSource) = resolveDiagnosticSeedPuuid(normalizedRegion, explicitSeedPuuid, checkpoint)
        val cachedMatchId = explicitSeedMatchId?.trim()?.takeIf { it.isNotBlank() }
            ?: resolveCachedMatchId(normalizedRegion)

        if (seedPuuid == null) {
            return CohortRateLimitProbeResultDto(
                region = normalizedRegion,
                probeStatus = "NO_CACHED_SEED_PUUID_AVAILABLE",
                seedSource = "NONE",
                seedPuuidUsed = false,
                seedMatchIdUsed = false,
                maxCallsPerEndpoint = 1,
                maxHttpAttemptsPerEndpoint = 1,
                maxRetriesConfigured = 0,
                totalProbeCallsMade = 0,
                totalHttpAttemptsMade = 0,
                httpAttemptsByEndpoint = zeroAttemptsMap,
                stoppedOnRateLimit429 = false,
                endpointOutcomes = REQUIRED_COLLECTOR_ENDPOINTS.associateWith { "SKIPPED_NO_SEED_PUUID" },
                probedAt = Instant.ofEpochMilli(timeProvider.now()),
                rateLimitVerification = preSnap,
                memoryHeadroom = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                checkpoint = checkpoint,
                notes = listOf(
                    "No cached PUUID found in Challenger leaderboard, profiles, rank snapshots, or matches for '$normalizedRegion'; 0 Riot HTTP attempts made."
                )
            )
        }

        fun evaluateProbePauseReason(): String? {
            if (!getMemoryHeadroomSnapshot().hasHeadroomForLowPriority) {
                return "PAUSED_MEMORY_HEADROOM"
            }
            val snap = getLiveRateLimitVerification(normalizedRegion)
            if (snap.foregroundYieldActive || snap.activeUserRequests > 0) {
                return "PAUSED_FOREGROUND_USER_REQUEST"
            }
            if (snap.serviceRateLimitActive || snap.retryAfterUntil != null) {
                return "PAUSED_RETRY_AFTER_BACKOFF"
            }
            if (snap.appLimitsVerifiedFromHeaders && !snap.appHasHeadroomForLowPriority) {
                return "PAUSED_RATE_LIMIT_HEADROOM"
            }
            if (snap.methodLimitsByEndpoint.values.any { !it.hasHeadroomForLowPriority }) {
                return "PAUSED_RATE_LIMIT_HEADROOM"
            }
            return null
        }

        val outcomes = LinkedHashMap<String, String>()
        val httpAttemptsByEndpoint = LinkedHashMap<String, Int>().apply {
            REQUIRED_COLLECTOR_ENDPOINTS.forEach { put(it, 0) }
        }
        var callsMade = 0
        var totalHttpAttempts = 0
        var stoppedOn429 = false
        var usedMatchId = false
        var probedMatchId: String? = null
        var abortedReason: String? = null

        // Step A: Probe league-v4-entries-by-puuid (max 1 HTTP attempt, maxRetries=0)
        val snapBeforeLeague = getLiveRateLimitVerification(normalizedRegion)
        if (snapBeforeLeague.appLimitsVerifiedFromHeaders &&
            "league-v4-entries-by-puuid" !in snapBeforeLeague.missingRequiredMethodEndpoints
        ) {
            outcomes["league-v4-entries-by-puuid"] = "SKIPPED_ALREADY_VERIFIED"
        } else {
            val pause = evaluateProbePauseReason()
            if (pause != null) {
                outcomes["league-v4-entries-by-puuid"] = pause
                abortedReason = pause
            } else {
                val attemptCounter = java.util.concurrent.atomic.AtomicInteger(0)
                val failureHolder = java.util.concurrent.atomic.AtomicReference<String?>(null)
                val entry = riotApiService.fetchLeagueRankLowPriority(
                    puuid = seedPuuid,
                    region = normalizedRegion,
                    maxRetries = 0,
                    httpAttemptCounter = attemptCounter,
                    failureReasonHolder = failureHolder
                )
                val attemptsUsed = attemptCounter.get().coerceAtLeast(1)
                callsMade++
                totalHttpAttempts += attemptsUsed
                httpAttemptsByEndpoint["league-v4-entries-by-puuid"] = attemptsUsed

                val fetchedTier = entry?.tier?.uppercase()?.trim()?.takeIf { it in TIER_ORDER }
                if (fetchedTier != null && entry.queueType == "RANKED_SOLO_5x5") {
                    cacheRankSnapshot(
                        region = normalizedRegion,
                        puuid = seedPuuid,
                        tier = fetchedTier,
                        rank = entry.rank,
                        leaguePoints = entry.leaguePoints,
                        snapshotAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                }
                val snapAfterLeague = getLiveRateLimitVerification(normalizedRegion)
                when {
                    failureHolder.get() == "HTTP_429_TOO_MANY_REQUESTS" ||
                        snapAfterLeague.serviceRateLimitActive ||
                        snapAfterLeague.retryAfterUntil != null -> {
                        outcomes["league-v4-entries-by-puuid"] = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                        stoppedOn429 = true
                        abortedReason = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                    }
                    failureHolder.get() == "NETWORK_TIMEOUT" -> {
                        outcomes["league-v4-entries-by-puuid"] = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                        abortedReason = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                    }
                    "league-v4-entries-by-puuid" in snapAfterLeague.verifiedMethodEndpoints -> {
                        outcomes["league-v4-entries-by-puuid"] = "PROBED_1_CALL_VERIFIED"
                    }
                    else -> {
                        outcomes["league-v4-entries-by-puuid"] = "PROBED_1_CALL_HEADERS_MISSING"
                    }
                }
            }
        }

        // Step B: Probe match-v5-ids-by-puuid (max 1 HTTP attempt, count=1, maxRetries=0)
        if (abortedReason == null) {
            val snapBeforeIds = getLiveRateLimitVerification(normalizedRegion)
            if (snapBeforeIds.appLimitsVerifiedFromHeaders &&
                "match-v5-ids-by-puuid" !in snapBeforeIds.missingRequiredMethodEndpoints &&
                cachedMatchId != null
            ) {
                outcomes["match-v5-ids-by-puuid"] = "SKIPPED_ALREADY_VERIFIED"
            } else if (snapBeforeIds.appLimitsVerifiedFromHeaders &&
                "match-v5-ids-by-puuid" !in snapBeforeIds.missingRequiredMethodEndpoints &&
                "match-v5-match-by-id" !in snapBeforeIds.missingRequiredMethodEndpoints
            ) {
                outcomes["match-v5-ids-by-puuid"] = "SKIPPED_ALREADY_VERIFIED"
            } else {
                val pause = evaluateProbePauseReason()
                if (pause != null) {
                    outcomes["match-v5-ids-by-puuid"] = pause
                    abortedReason = pause
                } else {
                    val attemptCounter = java.util.concurrent.atomic.AtomicInteger(0)
                    val failureHolder = java.util.concurrent.atomic.AtomicReference<String?>(null)
                    val ids = riotApiService.getMatchIdsByPuuidLowPriority(
                        puuid = seedPuuid,
                        region = normalizedRegion,
                        queueId = SOLO_QUEUE_ID,
                        count = 1,
                        start = 0,
                        maxRetries = 0,
                        httpAttemptCounter = attemptCounter,
                        failureReasonHolder = failureHolder
                    ).orEmpty()
                    val attemptsUsed = attemptCounter.get().coerceAtLeast(1)
                    callsMade++
                    totalHttpAttempts += attemptsUsed
                    httpAttemptsByEndpoint["match-v5-ids-by-puuid"] = attemptsUsed
                    probedMatchId = ids.firstOrNull()?.trim()?.takeIf { it.isNotBlank() }

                    val snapAfterIds = getLiveRateLimitVerification(normalizedRegion)
                    when {
                        failureHolder.get() == "HTTP_429_TOO_MANY_REQUESTS" ||
                            snapAfterIds.serviceRateLimitActive ||
                            snapAfterIds.retryAfterUntil != null -> {
                            outcomes["match-v5-ids-by-puuid"] = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                            stoppedOn429 = true
                            abortedReason = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                        }
                        failureHolder.get() == "NETWORK_TIMEOUT" -> {
                            outcomes["match-v5-ids-by-puuid"] = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                            abortedReason = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                        }
                        "match-v5-ids-by-puuid" in snapAfterIds.verifiedMethodEndpoints -> {
                            outcomes["match-v5-ids-by-puuid"] = "PROBED_1_CALL_VERIFIED"
                        }
                        else -> {
                            outcomes["match-v5-ids-by-puuid"] = "PROBED_1_CALL_HEADERS_MISSING"
                        }
                    }
                }
            }
        } else {
            outcomes["match-v5-ids-by-puuid"] = "SKIPPED_DUE_TO_$abortedReason"
        }

        // Step C: Probe match-v5-match-by-id (max 1 HTTP attempt, maxRetries=0)
        if (abortedReason == null) {
            val snapBeforeMatch = getLiveRateLimitVerification(normalizedRegion)
            if (snapBeforeMatch.appLimitsVerifiedFromHeaders &&
                "match-v5-match-by-id" !in snapBeforeMatch.missingRequiredMethodEndpoints
            ) {
                outcomes["match-v5-match-by-id"] = "SKIPPED_ALREADY_VERIFIED"
            } else {
                val matchIdToProbe = probedMatchId ?: cachedMatchId
                if (matchIdToProbe == null) {
                    outcomes["match-v5-match-by-id"] = "SKIPPED_NO_SEED_MATCH_ID"
                } else {
                    val pause = evaluateProbePauseReason()
                    if (pause != null) {
                        outcomes["match-v5-match-by-id"] = pause
                        abortedReason = pause
                    } else {
                        val attemptCounter = java.util.concurrent.atomic.AtomicInteger(0)
                        val failureHolder = java.util.concurrent.atomic.AtomicReference<String?>(null)
                        val match = riotApiService.getMatchByIdLowPriority(
                            matchId = matchIdToProbe,
                            region = normalizedRegion,
                            maxRetries = 0,
                            httpAttemptCounter = attemptCounter,
                            failureReasonHolder = failureHolder
                        )
                        val attemptsUsed = attemptCounter.get().coerceAtLeast(1)
                        callsMade++
                        totalHttpAttempts += attemptsUsed
                        httpAttemptsByEndpoint["match-v5-match-by-id"] = attemptsUsed
                        usedMatchId = true
                        if (match != null) {
                            redisCacheService.set("match:$normalizedRegion:$matchIdToProbe", match, MATCH_CACHE_TTL)
                        }
                        val snapAfterMatch = getLiveRateLimitVerification(normalizedRegion)
                        when {
                            failureHolder.get() == "HTTP_429_TOO_MANY_REQUESTS" ||
                                snapAfterMatch.serviceRateLimitActive ||
                                snapAfterMatch.retryAfterUntil != null -> {
                                outcomes["match-v5-match-by-id"] = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                                stoppedOn429 = true
                                abortedReason = "STOPPED_ON_FIRST_429_RETRY_AFTER"
                            }
                            failureHolder.get() == "NETWORK_TIMEOUT" -> {
                                outcomes["match-v5-match-by-id"] = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                                abortedReason = "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                            }
                            "match-v5-match-by-id" in snapAfterMatch.verifiedMethodEndpoints -> {
                                outcomes["match-v5-match-by-id"] = "PROBED_1_CALL_VERIFIED"
                            }
                            else -> {
                                outcomes["match-v5-match-by-id"] = "PROBED_1_CALL_HEADERS_MISSING"
                            }
                        }
                    }
                }
            }
        } else {
            outcomes["match-v5-match-by-id"] = "SKIPPED_DUE_TO_$abortedReason"
        }

        val finalSnap = getLiveRateLimitVerification(normalizedRegion)
        val finalMemSnap = getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)
        val finalStatus = when {
            abortedReason != null -> abortedReason
            finalSnap.appLimitsVerifiedFromHeaders && finalSnap.missingRequiredMethodEndpoints.isEmpty() ->
                "ALL_REQUIRED_HEADERS_VERIFIED"
            else -> "PARTIAL_HEADERS_OBSERVED"
        }

        val updatedCheckpoint = checkpoint.copy(
            totalNetworkCallsMade = checkpoint.totalNetworkCallsMade + totalHttpAttempts,
            pausedForRetryAfterCount = checkpoint.pausedForRetryAfterCount + if (stoppedOn429) 1 else 0,
            pausedForMemoryHeadroomCount = checkpoint.pausedForMemoryHeadroomCount + if (abortedReason == "PAUSED_MEMORY_HEADROOM") 1 else 0,
            lastStopReason = when {
                stoppedOn429 -> "PAUSED_RETRY_AFTER_BACKOFF"
                abortedReason == "PAUSED_MEMORY_HEADROOM" -> "PAUSED_MEMORY_HEADROOM"
                else -> checkpoint.lastStopReason
            },
            updatedAt = Instant.ofEpochMilli(timeProvider.now())
        )
        saveCollectorCheckpoint(updatedCheckpoint)

        return CohortRateLimitProbeResultDto(
            region = normalizedRegion,
            probeStatus = finalStatus,
            seedSource = seedSource,
            seedPuuidUsed = true,
            seedMatchIdUsed = usedMatchId,
            maxCallsPerEndpoint = 1,
            maxHttpAttemptsPerEndpoint = 1,
            maxRetriesConfigured = 0,
            totalProbeCallsMade = callsMade,
            totalHttpAttemptsMade = totalHttpAttempts,
            httpAttemptsByEndpoint = httpAttemptsByEndpoint,
            stoppedOnRateLimit429 = stoppedOn429,
            endpointOutcomes = outcomes,
            probedAt = Instant.ofEpochMilli(timeProvider.now()),
            rateLimitVerification = finalSnap,
            memoryHeadroom = finalMemSnap,
            checkpoint = updatedCheckpoint,
            notes = listOf(
                "Bounded diagnostic probe executed in-process with maxRetries=0 (max 1 actual HTTP attempt per required endpoint, wrapperCalls=$callsMade, totalHttpAttempts=$totalHttpAttempts, stoppedOn429=$stoppedOn429).",
                "Bulk network collection remains ${if (networkCollectionEnabled) "ENABLED" else "DISABLED (cohort.network-collection.enabled=false)"}."
            )
        )
    }

    private fun resolveDiagnosticSeedPuuid(
        region: String,
        explicitSeedPuuid: String?,
        checkpoint: CohortCollectorCheckpointDto?
    ): Pair<String?, String> {
        val explicit = explicitSeedPuuid?.trim()?.takeIf { it.isNotBlank() }
        if (explicit != null) return explicit to "EXPLICIT_OPERATOR_SEED"

        val lbKeys = listOf(
            "leaderboard:$region::RANKED_SOLO_5x5",
            "leaderboard:$region:RANKED_SOLO_5x5"
        )
        for (lbKey in lbKeys) {
            val lb = redisCacheService.get(lbKey, CachedLeaderboardDto::class.java)
            val puuid = lb?.leaderboard?.entries?.firstOrNull { !it.puuid.isNullOrBlank() }?.puuid?.trim()
            if (!puuid.isNullOrBlank()) return puuid to "CACHED_CHALLENGER_LEADERBOARD"
        }

        val fromCheckpoint = checkpoint?.pendingCandidatePuuids?.firstOrNull { it.isNotBlank() }
            ?: checkpoint?.processedPuuids?.firstOrNull { it.isNotBlank() }
        if (!fromCheckpoint.isNullOrBlank()) return fromCheckpoint.trim() to "CACHED_COLLECTOR_CHECKPOINT"

        val profileKey = redisCacheService.scanKeys("player:profile:$region:*", maxKeys = 20).firstOrNull()
        if (profileKey != null) {
            val puuid = profileKey.substringAfterLast(":").trim()
            if (puuid.isNotBlank()) return puuid to "CACHED_SUMMONER_PROFILE"
        }

        val snapKey = redisCacheService.scanKeys("cohort:rank:snapshot:$region:*", maxKeys = 20).firstOrNull()
        if (snapKey != null) {
            val puuid = snapKey.substringAfterLast(":").trim()
            if (puuid.isNotBlank()) return puuid to "CACHED_RANK_SNAPSHOT"
        }

        val matchKeys = redisCacheService.scanKeys("match:$region:*", maxKeys = 10)
        if (matchKeys.isNotEmpty()) {
            val matches = redisCacheService.multiGet(matchKeys, MatchDto::class.java).values.filterNotNull()
            for (m in matches) {
                val p = m.info?.participants?.firstOrNull { !it.puuid.isNullOrBlank() }?.puuid?.trim()
                if (!p.isNullOrBlank()) return p to "CACHED_MATCH_PARTICIPANT"
            }
        }
        return null to "NONE"
    }

    private fun resolveCachedMatchId(region: String): String? {
        val matchKey = redisCacheService.scanKeys("match:$region:*", maxKeys = 10).firstOrNull() ?: return null
        return matchKey.substringAfterLast(":").trim().takeIf { it.isNotBlank() }
    }

    fun getCollectorCheckpoint(region: String, targetPatch: String? = null): CohortCollectorCheckpointDto? {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
        val key = collectorCheckpointKey(normalizedRegion, cleanPatch)
        val cached = redisCacheService.get(key, CohortCollectorCheckpointDto::class.java)
        if (cached != null) return cached

        val repo = cohortCollectorCheckpointRepository ?: return null
        return try {
            val entity = if (cleanPatch != null) {
                repo.findByRegionAndTargetPatchAndQueueId(normalizedRegion, cleanPatch, SOLO_QUEUE_ID)
            } else {
                repo.findByRegionAndTargetPatchAndQueueId(normalizedRegion, "latest", SOLO_QUEUE_ID)
                    ?: repo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(normalizedRegion, SOLO_QUEUE_ID)
            } ?: return null

            val restored = objectMapper.readValue(entity.checkpointJson, CohortCollectorCheckpointDto::class.java)
            redisCacheService.set(key, restored, MAX_RANK_SNAPSHOT_AGE)
            if (restored.targetPatch != null) {
                redisCacheService.set(
                    collectorCheckpointKey(normalizedRegion, restored.targetPatch),
                    restored,
                    MAX_RANK_SNAPSHOT_AGE
                )
            }
            restored
        } catch (e: Exception) {
            logger.warn("Failed to restore collector checkpoint from Postgres for {} {}: {}", normalizedRegion, cleanPatch ?: "latest", e.message)
            null
        }
    }

    fun saveCollectorCheckpoint(checkpoint: CohortCollectorCheckpointDto) {
        val normalizedRegion = checkpoint.region.lowercase().trim()
        val cleanPatch = checkpoint.targetPatch?.trim()?.takeIf { it.isNotBlank() }
        val primaryKey = collectorCheckpointKey(normalizedRegion, cleanPatch)
        redisCacheService.set(primaryKey, checkpoint, MAX_RANK_SNAPSHOT_AGE)
        if (cleanPatch != null) {
            redisCacheService.set(collectorCheckpointKey(normalizedRegion, null), checkpoint, MAX_RANK_SNAPSHOT_AGE)
        }
        persistCollectorCheckpointToPostgres(normalizedRegion, cleanPatch, checkpoint)
    }

    private fun persistCollectorCheckpointToPostgres(
        normalizedRegion: String,
        cleanPatch: String?,
        checkpoint: CohortCollectorCheckpointDto
    ) {
        val repo = cohortCollectorCheckpointRepository ?: return
        try {
            val patchKey = cleanPatch ?: "latest"
            val json = objectMapper.writeValueAsString(checkpoint)
            val upsertKeys = if (cleanPatch != null) listOf(cleanPatch, "latest") else listOf("latest")
            for (pKey in upsertKeys) {
                val existing = repo.findByRegionAndTargetPatchAndQueueId(normalizedRegion, pKey, checkpoint.queueId)
                val entity = existing ?: CohortCollectorCheckpointEntity(
                    region = normalizedRegion,
                    targetPatch = pKey,
                    queueId = checkpoint.queueId
                )
                entity.lastStopReason = checkpoint.lastStopReason
                entity.totalNetworkCallsMade = checkpoint.totalNetworkCallsMade
                entity.processedPuuidsCount = checkpoint.processedPuuids.size
                entity.pendingCandidatesCount = checkpoint.pendingCandidatePuuids.size
                entity.cellsPassingQualityGate = checkpoint.cellsPassingQualityGate
                entity.checkpointJson = json
                entity.startedAt = checkpoint.startedAt
                entity.updatedAt = checkpoint.updatedAt
                repo.save(entity)
            }
        } catch (e: Exception) {
            logger.warn("Failed to persist collector checkpoint to Postgres for {} {}: {}", normalizedRegion, cleanPatch ?: "latest", e.message)
        }
    }

    fun cacheRankSnapshot(
        region: String,
        puuid: String,
        tier: String,
        rank: String? = null,
        leaguePoints: Int? = null,
        snapshotAt: Instant = Instant.ofEpochMilli(timeProvider.now())
    ) {
        val normalizedRegion = region.lowercase().trim()
        val cleanPuuid = puuid.trim()
        val cleanTier = tier.uppercase().trim()
        if (cleanPuuid.isBlank() || cleanTier !in TIER_ORDER) return
        val dto = CohortRankSnapshotDto(
            puuid = cleanPuuid,
            region = normalizedRegion,
            tier = cleanTier,
            rank = rank,
            leaguePoints = leaguePoints,
            queueType = "RANKED_SOLO_5x5",
            snapshotAt = snapshotAt
        )
        redisCacheService.set("cohort:rank:snapshot:$normalizedRegion:$cleanPuuid", dto, MAX_RANK_SNAPSHOT_AGE)
    }

    private fun getFreshCachedTierForPuuid(
        region: String,
        puuid: String,
        minSnapshotInstant: Instant
    ): String? {
        val snap = redisCacheService.get("cohort:rank:snapshot:$region:$puuid", CohortRankSnapshotDto::class.java)
        if (snap != null && snap.queueType == "RANKED_SOLO_5x5" && !snap.snapshotAt.isBefore(minSnapshotInstant)) {
            val t = snap.tier.uppercase().trim()
            if (t in TIER_ORDER) return t
        }
        val profile = redisCacheService.get("player:profile:$region:$puuid", SummonerProfileDto::class.java)
        val refreshed = profile?.lastRefreshed
        val soloRank = profile?.soloQueueRank
        if (refreshed != null && !refreshed.isBefore(minSnapshotInstant) &&
            (soloRank?.queueType == null || soloRank.queueType == "RANKED_SOLO_5x5")
        ) {
            val t = soloRank?.tier?.uppercase()?.trim()
            if (t != null && t in TIER_ORDER) return t
        }
        val lbKeys = listOf("leaderboard:$region::RANKED_SOLO_5x5", "leaderboard:$region:RANKED_SOLO_5x5")
        for (lbKey in lbKeys) {
            val cachedLb = redisCacheService.get(lbKey, CachedLeaderboardDto::class.java) ?: continue
            val lbRefreshed = cachedLb.lastRefreshed ?: continue
            if (!lbRefreshed.isBefore(minSnapshotInstant)) {
                val hasPlayer = cachedLb.leaderboard?.entries?.any { it.puuid == puuid } == true
                if (hasPlayer) {
                    val lbTier = cachedLb.leaderboard?.tier?.uppercase()?.trim()?.takeIf { it in TIER_ORDER } ?: "CHALLENGER"
                    return lbTier
                }
            }
        }
        return null
    }

    private fun discoverCandidatePuuidsFromCache(region: String, targetPatch: String?): List<String> {
        val discovered = LinkedHashSet<String>()
        val lbKeys = listOf("leaderboard:$region::RANKED_SOLO_5x5", "leaderboard:$region:RANKED_SOLO_5x5")
        for (lbKey in lbKeys) {
            val cachedLb = redisCacheService.get(lbKey, CachedLeaderboardDto::class.java) ?: continue
            for (entry in cachedLb.leaderboard?.entries.orEmpty()) {
                val pid = entry.puuid?.trim()?.takeIf { it.isNotBlank() } ?: continue
                if (discovered.size < MAX_CHECKPOINT_PENDING_PUUIDS) discovered.add(pid)
            }
        }
        val profileKeys = redisCacheService.scanKeys("player:profile:$region:*", maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE)
        for (key in profileKeys) {
            val puuid = key.substringAfterLast(":").trim()
            if (puuid.isNotBlank() && discovered.size < MAX_CHECKPOINT_PENDING_PUUIDS) discovered.add(puuid)
        }
        val snapKeys = redisCacheService.scanKeys("cohort:rank:snapshot:$region:*", maxKeys = MAX_SCAN_KEYS_PER_NAMESPACE)
        for (key in snapKeys) {
            val puuid = key.substringAfterLast(":").trim()
            if (puuid.isNotBlank() && discovered.size < MAX_CHECKPOINT_PENDING_PUUIDS) discovered.add(puuid)
        }
        if (discovered.isEmpty()) {
            val matchKeys = redisCacheService.scanKeys("match:$region:*", maxKeys = 20)
            chunkLoop@ for (chunk in matchKeys.chunked(MATCH_SCAN_CHUNK_SIZE)) {
                if (discovered.size >= MAX_CHECKPOINT_PENDING_PUUIDS || !getMemoryHeadroomSnapshot().hasHeadroomForLowPriority) {
                    break@chunkLoop
                }
                val chunkMatches = redisCacheService.multiGet(chunk, MatchDto::class.java).values.filterNotNull()
                for (m in chunkMatches) {
                    val info = m.info ?: continue
                    if (info.queueId != SOLO_QUEUE_ID) continue
                    val p = extractMajorMinorPatch(info.gameVersion)
                    if (targetPatch != null && p != targetPatch) continue
                    for (part in info.participants) {
                        val role = resolveNormalizedRole(part)
                        val pid = part.puuid?.takeIf { it.isNotBlank() }
                        if (role in ELIGIBLE_ROLES && pid != null && discovered.size < MAX_CHECKPOINT_PENDING_PUUIDS) {
                            discovered.add(pid)
                        }
                    }
                }
            }
        }
        return discovered.toList()
    }

    private fun buildAllTargetCellProgress(
        computedCells: List<CohortBenchmarkCellDto>
    ): Map<String, CohortCellProgressDto> {
        val byKey = computedCells.associateBy { "${it.tier.uppercase()}:${it.role.uppercase()}" }
        val result = LinkedHashMap<String, CohortCellProgressDto>()
        for (tier in TIER_ORDER) {
            for (role in listOf("TOP", "MIDDLE", "BOTTOM")) {
                val key = "$tier:$role"
                val cell = byKey[key]
                val players = cell?.distinctPlayers ?: 0
                val games = cell?.eligibleGames ?: 0
                val passes = cell?.passesQualityGate == true ||
                    (players >= CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE &&
                        games >= CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE)
                result[key] = CohortCellProgressDto(
                    tier = tier,
                    role = role,
                    distinctPlayers = players,
                    eligibleGames = games,
                    targetPlayers = CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE,
                    targetGames = CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE,
                    passesQualityGate = passes,
                    deficitPlayers = (CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE - players).coerceAtLeast(0),
                    deficitGames = (CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE - games).coerceAtLeast(0)
                )
            }
        }
        return result
    }

    private fun mergeCellProgressWithPlayerCounts(
        base: Map<String, CohortCellProgressDto>,
        playerCountsByCell: Map<String, Int>
    ): Map<String, CohortCellProgressDto> {
        if (playerCountsByCell.isEmpty()) return base
        val playersPerCell = HashMap<String, Int>()
        val gamesPerCell = HashMap<String, Int>()
        for ((entryKey, games) in playerCountsByCell) {
            if (games <= 0) continue
            val firstColon = entryKey.indexOf(':')
            if (firstColon <= 0) continue
            val secondColon = entryKey.indexOf(':', firstColon + 1)
            val cellKey = if (secondColon > firstColon) entryKey.substring(0, secondColon) else entryKey
            playersPerCell[cellKey] = (playersPerCell[cellKey] ?: 0) + 1
            gamesPerCell[cellKey] = (gamesPerCell[cellKey] ?: 0) + games
        }
        val merged = LinkedHashMap<String, CohortCellProgressDto>(base.size)
        for ((key, progress) in base) {
            val mapPlayers = playersPerCell[key] ?: 0
            val mapGames = gamesPerCell[key] ?: 0
            if (mapPlayers <= 0 && mapGames <= 0) {
                merged[key] = progress
            } else {
                val newPlayers = maxOf(progress.distinctPlayers, mapPlayers)
                val newGames = maxOf(progress.eligibleGames, mapGames)
                val passes = progress.passesQualityGate ||
                    (newPlayers >= progress.targetPlayers && newGames >= progress.targetGames)
                merged[key] = progress.copy(
                    distinctPlayers = newPlayers,
                    eligibleGames = newGames,
                    passesQualityGate = passes,
                    deficitPlayers = (progress.targetPlayers - newPlayers).coerceAtLeast(0),
                    deficitGames = (progress.targetGames - newGames).coerceAtLeast(0)
                )
            }
        }
        return merged
    }

    private fun collectorCheckpointKey(region: String, patch: String?): String =
        "cohort:collector:checkpoint:${region.lowercase()}:${patch ?: "latest"}"


    private fun persistDeidentifiedCells(
        region: String,
        patch: String,
        cells: List<CohortBenchmarkCellDto>
    ) {
        try {
            val cacheKey = benchmarkCacheKey(region, patch)
            redisCacheService.set(cacheKey, cells, BENCHMARK_CACHE_TTL)
        } catch (e: Exception) {
            logger.warn("Failed to cache cohort benchmark cells in Redis for {} {}: {}", region, patch, e.message)
        }

        try {
            for (cell in cells) {
                val metricsJson = objectMapper.writeValueAsString(cell.metrics)
                val existing = cohortBenchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                    region = cell.region,
                    patch = cell.patch,
                    queueId = cell.queueId,
                    tier = cell.tier,
                    role = cell.role
                )
                if (existing != null) {
                    existing.distinctPlayers = cell.distinctPlayers
                    existing.eligibleGames = cell.eligibleGames
                    existing.maxGamesPerPlayerCap = cell.maxGamesPerPlayerCap
                    existing.excludedOverPlayerCapGames = cell.excludedOverPlayerCapGames
                    existing.passesQualityGate = cell.passesQualityGate
                    existing.colorBandsEnabled = cell.colorBandsEnabled
                    existing.earliestRankSnapshotAt = cell.earliestRankSnapshotAt
                    existing.latestRankSnapshotAt = cell.latestRankSnapshotAt
                    existing.computedAt = cell.computedAt
                    existing.metricsJson = metricsJson
                    cohortBenchmarkCellRepository.save(existing)
                } else {
                    cohortBenchmarkCellRepository.save(
                        CohortBenchmarkCellEntity(
                            region = cell.region,
                            patch = cell.patch,
                            queueId = cell.queueId,
                            tier = cell.tier,
                            role = cell.role,
                            distinctPlayers = cell.distinctPlayers,
                            eligibleGames = cell.eligibleGames,
                            maxGamesPerPlayerCap = cell.maxGamesPerPlayerCap,
                            excludedOverPlayerCapGames = cell.excludedOverPlayerCapGames,
                            passesQualityGate = cell.passesQualityGate,
                            colorBandsEnabled = cell.colorBandsEnabled,
                            earliestRankSnapshotAt = cell.earliestRankSnapshotAt,
                            latestRankSnapshotAt = cell.latestRankSnapshotAt,
                            computedAt = cell.computedAt,
                            metricsJson = metricsJson
                        )
                    )
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to persist cohort benchmark cells in DB for {} {}: {}", region, patch, e.message)
        }
    }

    private fun entityToDto(entity: CohortBenchmarkCellEntity): CohortBenchmarkCellDto? {
        return try {
            val metrics = objectMapper.readValue(entity.metricsJson, CohortMetricsBundleDto::class.java)
            val gateStatus = if (entity.passesQualityGate) {
                "READY_FOR_COLOR_BANDS"
            } else {
                "INSUFFICIENT_SAMPLE (players=${entity.distinctPlayers}/${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE}, games=${entity.eligibleGames}/${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE})"
            }
            CohortBenchmarkCellDto(
                region = entity.region,
                patch = entity.patch,
                queueId = entity.queueId,
                tier = entity.tier,
                role = entity.role,
                distinctPlayers = entity.distinctPlayers,
                eligibleGames = entity.eligibleGames,
                maxGamesPerPlayerCap = entity.maxGamesPerPlayerCap,
                excludedOverPlayerCapGames = entity.excludedOverPlayerCapGames,
                passesQualityGate = entity.passesQualityGate,
                colorBandsEnabled = entity.colorBandsEnabled,
                qualityGateStatus = gateStatus,
                earliestRankSnapshotAt = entity.earliestRankSnapshotAt,
                latestRankSnapshotAt = entity.latestRankSnapshotAt,
                computedAt = entity.computedAt,
                metrics = metrics
            )
        } catch (e: Exception) {
            logger.warn("Failed to deserialize CohortBenchmarkCellEntity {}: {}", entity.id, e.message)
            null
        }
    }

    private fun buildDistribution(
        metricKey: String,
        unit: String,
        includesJungleCamps: Boolean,
        theoreticalSupplyCeiling: Int?,
        samples: List<ParticipantSample>,
        extractor: (ParticipantSample) -> Double?
    ): CohortPercentileDistributionDto {
        val pairs = samples.mapNotNull { sample ->
            val v = extractor(sample)
            if (v != null && v.isFinite()) sample.puuid to v else null
        }
        if (pairs.isEmpty()) {
            return CohortPercentileDistributionDto(
                metricKey = metricKey,
                unit = unit,
                includesJungleCamps = includesJungleCamps,
                theoreticalSupplyCeiling = theoreticalSupplyCeiling,
                sampleGames = 0,
                distinctPlayers = 0,
                p25 = null,
                median = null,
                p75 = null,
                iqr = null
            )
        }

        val sortedValues = pairs.map { it.second }.sorted()
        val distinctPlayers = pairs.map { it.first }.toSet().size
        val rawP25 = quantile(sortedValues, 0.25)
        val rawMedian = quantile(sortedValues, 0.50)
        val rawP75 = quantile(sortedValues, 0.75)

        return CohortPercentileDistributionDto(
            metricKey = metricKey,
            unit = unit,
            includesJungleCamps = includesJungleCamps,
            theoreticalSupplyCeiling = theoreticalSupplyCeiling,
            sampleGames = sortedValues.size,
            distinctPlayers = distinctPlayers,
            p25 = round2(rawP25),
            median = round2(rawMedian),
            p75 = round2(rawP75),
            iqr = round2(rawP75 - rawP25)
        )
    }

    internal fun quantile(sortedValues: List<Double>, p: Double): Double {
        require(sortedValues.isNotEmpty()) { "sortedValues must not be empty" }
        if (sortedValues.size == 1) return sortedValues.first()
        val clampedP = p.coerceIn(0.0, 1.0)
        val pos = clampedP * (sortedValues.size - 1)
        val lower = floor(pos).toInt()
        val upper = ceil(pos).toInt()
        val weight = pos - lower
        return sortedValues[lower] * (1.0 - weight) + sortedValues[upper] * weight
    }

    private fun round2(value: Double): Double = (value * 100.0).roundToLong() / 100.0

    internal fun extractMajorMinorPatch(gameVersion: String?): String? {
        if (gameVersion.isNullOrBlank()) return null
        val parts = gameVersion.trim().split(".")
        if (parts.size < 2) return null
        val major = parts[0].toIntOrNull() ?: return null
        val minor = parts[1].toIntOrNull() ?: return null
        return "$major.$minor"
    }

    /**
     * Per-role theoretical lane-minion supply in the first 10:00 of Summoner's Rift:
     * - TOP and BOTTOM sidelanes have a longer lane travel path: 114 lane minions arrive by 10:00.
     * - MIDDLE lane (shorter path): 120 lane minions on Season 2026 / Patch 16.1+ (or 26.1+), 114 pre-16.1.
     */
    internal fun theoreticalLaneMinionSupplyAt10(patch: String?, role: String = "MIDDLE"): Int {
        val normalizedRole = role.uppercase().trim()
        if (normalizedRole == "TOP" || normalizedRole == "BOTTOM") {
            return 114
        }
        val major = patch?.substringBefore(".")?.toIntOrNull() ?: 16
        return if (major >= 16) 120 else 114
    }

    private fun resolveNormalizedRole(participant: ParticipantDto): String {
        val raw = participant.teamPosition?.takeIf { it.isNotBlank() }
            ?: participant.individualPosition?.takeIf { it.isNotBlank() }
            ?: ""
        return raw.uppercase().trim()
    }

    private fun findFrameAt10Minutes(timeline: MatchTimelineDto?): TimelineFrameDto? {
        val frames = timeline?.info?.frames ?: return null
        return frames.filter { frame ->
            val ts = frame.timestamp ?: return@filter false
            ts in 570_000L..630_000L
        }.minByOrNull { frame ->
            kotlin.math.abs((frame.timestamp ?: 0L) - 600_000L)
        }
    }

    private fun findParticipantFrame(
        frame: TimelineFrameDto?,
        participant: ParticipantDto,
        timeline: MatchTimelineDto?
    ): ParticipantFrameDto? {
        if (frame == null) return null
        val participantFrames = frame.participantFrames ?: return null
        val pid = participant.participantId
            ?: timeline?.info?.participants?.find { it.puuid == participant.puuid }?.participantId
            ?: return null
        return participantFrames[pid.toString()]
            ?: participantFrames.values.find { it.participantId == pid }
    }

    private fun benchmarkCacheKey(region: String, patch: String): String =
        "cohort:benchmarks:${region.lowercase()}:$patch"

    private fun reportCacheKey(region: String, patch: String?): String =
        "cohort:report:${region.lowercase()}:${patch ?: "latest"}"

    private fun recomputeGuardLockKey(region: String, patch: String?): String =
        "lock:cohort:report:${region.lowercase()}:${patch ?: "latest"}"

    private fun buildEmptyGuardedReport(
        region: String,
        now: Instant,
        reportSource: String,
        note: String
    ): CohortExtractionReportDto {
        return CohortExtractionReportDto(
            reportSource = reportSource,
            inventory = CohortCacheInventoryDto(
                region = region,
                scannedAt = now,
                scanKeyCapPerNamespace = MAX_SCAN_KEYS_PER_NAMESPACE,
                maxRankSnapshotAgeHours = MAX_RANK_SNAPSHOT_AGE.toHours(),
                maxMatchAgeHours = MAX_MATCH_AGE.toHours(),
                maxMatchToSnapshotSkewHours = MAX_MATCH_TO_SNAPSHOT_SKEW.toHours(),
                cachedProfileCount = 0,
                profilesWithSoloQueueRankSnapshot = 0,
                cachedChallengerLeaderboardPlayers = 0,
                totalDistinctRankedPlayersInCache = 0,
                ranksByTier = emptyMap(),
                cachedMatchCount = 0,
                cachedTimelineCount = 0,
                matchesByPatch = emptyMap(),
                latestCachedPatch = null,
                selectedTargetPatch = null,
                patchSelectionSource = reportSource,
                soloQueue420MatchesOnTargetPatch = 0,
                excludedNonSoloQueueMatches = 0,
                excludedMissingGameVersionMatches = 0,
                excludedOtherPatchMatches = 0,
                excludedShortOrRemakeMatches = 0,
                topMidBotParticipantCandidates = 0,
                excludedOtherRoleParticipants = 0,
                participantsWithRankSnapshot = 0,
                participantsWithoutRankSnapshot = 0,
                deduplicatedParticipantEntries = 0,
                excludedOverPlayerCapSamples = 0,
                eligibleCohortSamples = 0,
                distinctEligibleCohortPlayers = 0,
                cellsTotal = 0,
                cellsPassingQualityGate = 0
            ),
            rateLimitVerification = riotApiRateLimiter.getVerificationSnapshot(
                region = region,
                networkCollectionEnabled = networkCollectionEnabled
            ),
            cells = emptyList(),
            methodologyNotes = listOf(note)
        )
    }

    private fun tierIndex(tier: String): Int {
        val idx = TIER_ORDER.indexOf(tier.uppercase())
        return if (idx >= 0) idx else TIER_ORDER.size
    }

    private fun roleIndex(role: String): Int = when (role.uppercase()) {
        "TOP" -> 0
        "MIDDLE" -> 1
        "BOTTOM" -> 2
        else -> 99
    }

    private val patchComparator = Comparator<String> { a, b ->
        val aParts = a.split(".").map { it.toIntOrNull() ?: 0 }
        val bParts = b.split(".").map { it.toIntOrNull() ?: 0 }
        val majorCmp = (aParts.getOrElse(0) { 0 }).compareTo(bParts.getOrElse(0) { 0 })
        if (majorCmp != 0) majorCmp else (aParts.getOrElse(1) { 0 }).compareTo(bParts.getOrElse(1) { 0 })
    }
}
