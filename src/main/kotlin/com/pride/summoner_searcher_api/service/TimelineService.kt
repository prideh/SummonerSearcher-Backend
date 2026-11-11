package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.CoachTimelinePreparationResultDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.MissingMatchTimelineDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.dto.TimelineAnalysisDto
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.time.Duration
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * Service responsible for fetching, caching, and bounded on-demand preparation of match timelines.
 *
 * Guarantees:
 * - Pre-decode admission (`350 MiB`), warm cache lookup, singleflight coalescing, and global concurrency
 *   slot enforcement (`maxConcurrentPreparations = 2`) execute BEFORE `SummonerProfileDto` decode.
 * - Dual JVM heap + container cgroup headroom checks cover both `MaxRAMPercentage=40%` (`~204.8 MiB` heap
 *   in a `512 MiB` container) and non-cgroup JVMs.
 * - Sequential 1-match-at-a-time processing (`peakRetainedRawTimelines <= 1`), single-player compaction,
 *   and GZIP-compressed byte-capped `preparedCache` (`<= 1 MiB` total).
 * - Transient failures (`UPSTREAM_ERROR`, `HTTP_EXECUTOR_SATURATED`, `RATE_LIMIT_429`, `RATE_LIMIT_COOLDOWN`,
 *   `NETWORK_TIMEOUT`, `DEADLINE_EXCEEDED`, `MEMORY_HEADROOM_STOP`) stop hammering remaining matches and are
 *   never cached in `preparedCache`.
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
    @Value("\${coach.timeline.prepared-cache-max-entries:16}")
    private val preparedCacheMaxEntries: Int = DEFAULT_PREPARED_CACHE_MAX_ENTRIES,
    @Value("\${coach.timeline.prepared-cache-max-total-bytes:1048576}")
    private val preparedCacheMaxTotalBytes: Int = DEFAULT_PREPARED_CACHE_MAX_TOTAL_BYTES,
    @Value("\${coach.timeline.prepared-cache-ttl-ms:900000}")
    private val preparedCacheTtlMs: Long = DEFAULT_PREPARED_CACHE_TTL_MS,
    @Value("\${coach.timeline.lingering-worker-poll-ms:5000}")
    private val lingeringWorkerPollMs: Long = DEFAULT_LINGERING_WORKER_POLL_MS,
    @Value("\${coach.timeline.pre-admission-gc-enabled:false}")
    private val preAdmissionGcEnabled: Boolean = false,
    @Value("\${coach.timeline.pre-admission-gc-min-unused-committed-bytes:25165824}")
    private val preAdmissionGcMinUnusedCommittedBytes: Long = DEFAULT_PRE_ADMISSION_GC_MIN_UNUSED_COMMITTED_BYTES,
    @Value("\${coach.timeline.pre-admission-gc-cooldown-ms:30000}")
    private val preAdmissionGcCooldownMs: Long = DEFAULT_PRE_ADMISSION_GC_COOLDOWN_MS
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val COACH_PREP_ADMISSION_MAX_BYTES: Long = 350L * 1024L * 1024L // 350 MiB
        const val COACH_PREP_STOP_MAX_BYTES: Long = 360L * 1024L * 1024L      // 360 MiB
        const val MAX_COACH_TIMELINE_MATCHES: Int = 20
        const val COACH_TIMELINE_ANALYZER_VERSION: String = "coach-tl-v1"
        const val DEFAULT_PREP_DEADLINE_MS: Long = 12_000L
        const val DEFAULT_MAX_CONCURRENT_PREPARATIONS: Int = 2
        const val DEFAULT_PREPARED_CACHE_MAX_ENTRIES: Int = 16
        const val DEFAULT_PREPARED_CACHE_MAX_TOTAL_BYTES: Int = 1_048_576      // 1 MiB total compressed cap
        const val MAX_PREPARED_CACHE_SINGLE_COMPRESSED_BYTES: Int = 262_144    // 256 KiB per entry
        const val MAX_PREPARED_CACHE_SINGLE_INFLATED_BYTES: Int = 1_572_864    // 1.5 MiB inflated JSON cap
        const val DEFAULT_PREPARED_CACHE_TTL_MS: Long = 15L * 60L * 1000L      // 15 minutes
        const val DEFAULT_LINGERING_WORKER_POLL_MS: Long = 5_000L
        const val MAX_PROFILE_CACHE_INFLATE_BYTES: Int = 6 * 1024 * 1024       // 6 MiB profile inflate cap
        const val MAX_RAW_TIMELINE_CACHE_INFLATE_BYTES: Int = 4 * 1024 * 1024  // 4 MiB raw timeline inflate cap
        const val DEFAULT_PRE_ADMISSION_GC_MIN_UNUSED_COMMITTED_BYTES: Long = 24L * 1024L * 1024L // 24 MiB
        const val DEFAULT_PRE_ADMISSION_GC_COOLDOWN_MS: Long = 30_000L

        private const val ADMISSION_MAX_HEAP_RATIO: Double = 0.85
        private const val STOP_MAX_HEAP_RATIO: Double = 0.88
        private const val ADMISSION_MIN_HEAP_FREE_BYTES: Long = 32L * 1024L * 1024L
        private const val STOP_MIN_HEAP_FREE_BYTES: Long = 24L * 1024L * 1024L

        internal fun formatPayloadFreeMemorySample(sample: RawMemorySample?): String {
            if (sample == null) return "unavailable"
            val validHeap = sample.jvmHeapMaxBytes > 0L &&
                sample.jvmHeapUsedBytes >= 0L &&
                sample.jvmHeapCommittedBytes > 0L &&
                sample.jvmHeapCommittedBytes >= sample.jvmHeapUsedBytes &&
                sample.jvmHeapCommittedBytes <= sample.jvmHeapMaxBytes
            val unusedComm = if (validHeap) sample.jvmHeapCommittedBytes - sample.jvmHeapUsedBytes else -1L
            return "cgroupCur=${sample.cgroupMemoryCurrentBytes ?: -1}," +
                "cgroupMax=${sample.cgroupMemoryMaxBytes ?: -1}," +
                "cgroupPeak=${sample.cgroupMemoryPeakBytes ?: -1}," +
                "anon=${sample.cgroupAnonBytes ?: -1}," +
                "file=${sample.cgroupFileCacheBytes ?: -1}," +
                "inactiveFile=${sample.cgroupInactiveFileBytes ?: -1}," +
                "heapUsed=${sample.jvmHeapUsedBytes}," +
                "heapComm=${sample.jvmHeapCommittedBytes}," +
                "heapUnusedComm=$unusedComm," +
                "heapMax=${sample.jvmHeapMaxBytes}," +
                "nonHeapUsed=${sample.jvmNonHeapUsedBytes}," +
                "metaUsed=${sample.jvmMetaspaceUsedBytes}," +
                "metaComm=${sample.jvmMetaspaceCommittedBytes}," +
                "classUsed=${sample.jvmCompressedClassSpaceUsedBytes}," +
                "classComm=${sample.jvmCompressedClassSpaceCommittedBytes}," +
                "codeUsed=${sample.jvmCodeCacheUsedBytes}," +
                "codeComm=${sample.jvmCodeCacheCommittedBytes}," +
                "directUsed=${sample.directBufferUsedBytes}," +
                "directCap=${sample.directBufferCapacityBytes}," +
                "mappedUsed=${sample.mappedBufferUsedBytes}," +
                "threads=${sample.jvmThreadCount}," +
                "oldGc=${sample.oldGenGcCount}"
        }
    }

    internal data class PreAdmissionCheckResult(
        val admitted: Boolean,
        val initialSample: RawMemorySample?,
        val postGcSample: RawMemorySample? = null,
        val gcAttempted: Boolean = false,
        val gcStatus: String,
        val gcDurationMs: Long = 0L
    )

    sealed class PlayerPreparationOutcome {
        data object ProfileNotFound : PlayerPreparationOutcome()
        data class Ready(
            val result: CoachTimelinePreparationResultDto,
            val nonTransientEmptyAnalysis: TimelineAnalysisDto? = null
        ) : PlayerPreparationOutcome()
    }

    private data class CachedPreparationEntry(
        val playerScopeKey: String,
        val expiresAtMs: Long,
        val compressedBytes: ByteArray
    )

    private class InFlightEntry<T>(
        val deferred: CompletableDeferred<T> = CompletableDeferred(),
        @Volatile var workerJob: Job? = null,
        var activeWaiters: Int = 1
    )

    private val cacheObjectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val preparedCacheLock = Any()
    private var preparedCacheTotalBytes: Int = 0
    private val preparedCache = object : LinkedHashMap<String, CachedPreparationEntry>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, CachedPreparationEntry>?): Boolean {
            return false // Managed explicitly in putCachedPreparation to keep preparedCacheTotalBytes exact
        }
    }

    private val singleflightMutex = Mutex()
    private val inFlightDirectPreparations = ConcurrentHashMap<String, InFlightEntry<CoachTimelinePreparationResultDto>>()
    private val inFlightPlayerPreparations = ConcurrentHashMap<String, InFlightEntry<PlayerPreparationOutcome>>()
    private val singleflightScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val activeGlobalPreparations = AtomicInteger(0)
    private val activeCacheReadWorkers = AtomicInteger(0)
    private val preAdmissionGcLock = Any()
    private var lastPreAdmissionGcStartedAtMs: Long = Long.MIN_VALUE
    private var preAdmissionGcInProgress: Boolean = false

    @Volatile
    internal var warmCacheDecodeHook: (() -> Unit)? = null

    @Volatile
    internal var preAdmissionGcRunner: () -> Unit = { System.gc() }

    @Volatile
    internal var stageSummaryObserver: ((String) -> Unit)? = null

    private val cacheReadExecutor = ThreadPoolExecutor(
        2,
        4,
        30L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(8),
        ThreadFactory { runnable ->
            Thread(runnable, "timeline-bounded-cache-read").apply { isDaemon = true }
        },
        ThreadPoolExecutor.AbortPolicy()
    ).apply {
        allowCoreThreadTimeOut(true)
    }

    internal fun getActiveGlobalPreparations(): Int = activeGlobalPreparations.get()

    internal fun getActiveCacheReadWorkers(): Int = activeCacheReadWorkers.get()

    internal fun getPreparedCacheCompressedBytes(): Int = synchronized(preparedCacheLock) {
        preparedCacheTotalBytes
    }

    internal fun getPreparedCacheEntryCount(): Int = synchronized(preparedCacheLock) {
        preparedCache.size
    }

    /**
     * Evicts all cached timeline preparation snapshots for `(region, puuid)` when a player's profile refreshes.
     */
    fun invalidatePlayerPreparedCache(region: String, puuid: String) {
        val playerScopeKey = "${region.trim().lowercase()}:${puuid.trim()}"
        synchronized(preparedCacheLock) {
            val keysToRemove = preparedCache.entries
                .filter { it.value.playerScopeKey == playerScopeKey }
                .map { it.key }
            for (k in keysToRemove) {
                val removed = preparedCache.remove(k)
                if (removed != null) {
                    preparedCacheTotalBytes = (preparedCacheTotalBytes - removed.compressedBytes.size).coerceAtLeast(0)
                }
            }
        }
    }

    private fun getCacheKey(matchId: String, region: String) =
        "timeline:${region.trim().lowercase()}:${matchId.trim()}"

    private fun buildDirectPreparationCacheKey(
        normalizedRegion: String,
        puuid: String,
        orderedMatchIds: List<String>,
        analyzerVersion: String
    ): String = "direct|$normalizedRegion|$puuid|$analyzerVersion|${orderedMatchIds.joinToString(",")}"

    private fun buildPlayerPreparationCacheKey(
        normalizedRegion: String,
        puuid: String,
        explicitMatchIds: List<String>,
        analyzerVersion: String
    ): String = "player|$normalizedRegion|$puuid|$analyzerVersion|${explicitMatchIds.joinToString(",")}"

    fun hasCoachAdmissionHeadroom(): Boolean = evaluateMemoryHeadroom(coachAdmissionMaxUsedBytes)

    fun hasCoachContinueHeadroom(): Boolean = evaluateMemoryHeadroom(coachStopMaxUsedBytes)

    fun buildAdmissionBlockedResult(
        region: String,
        puuid: String,
        requestedMatchIds: List<String>,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION
    ): CoachTimelinePreparationResultDto {
        val normalizedRegion = region.trim().lowercase()
        val orderedIds = sanitizeMatchIds(requestedMatchIds)
        return buildBlockedResult(
            normalizedRegion = normalizedRegion,
            puuid = puuid.trim(),
            orderedMatchIds = orderedIds,
            stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
            analyzerVersion = analyzerVersion
        )
    }

    private fun buildBlockedResult(
        normalizedRegion: String,
        puuid: String,
        orderedMatchIds: List<String>,
        stopReason: String,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION,
        peakRetainedRawTimelines: Int = 0
    ): CoachTimelinePreparationResultDto {
        return CoachTimelinePreparationResultDto(
            status = "BLOCKED",
            region = normalizedRegion,
            puuid = puuid,
            analyzerVersion = analyzerVersion,
            requestedMatchIds = orderedMatchIds,
            coveredMatchIds = emptyList(),
            missingMatches = orderedMatchIds.map { MissingMatchTimelineDto(it, stopReason) },
            stopReason = stopReason,
            cacheHit = false,
            peakRetainedRawTimelines = peakRetainedRawTimelines,
            analysis = null
        )
    }

    private fun sanitizeMatchIds(matchIds: List<String?>?): List<String> {
        if (matchIds.isNullOrEmpty()) return emptyList()
        return matchIds.asSequence()
            .mapNotNull { raw ->
                val trimmed = raw?.trim() ?: return@mapNotNull null
                if (trimmed.isNotEmpty() && trimmed.length <= 64) trimmed else null
            }
            .distinct()
            .take(MAX_COACH_TIMELINE_MATCHES)
            .toList()
    }

    /**
     * Evaluates BOTH JVM heap headroom and bounded container cgroup headroom independently:
     * - Protects `MaxRAMPercentage=40.0` (~204.8 MiB heap inside a 512 MiB cgroup) from heap OOM when
     *   container RSS is still < 350 MiB.
     * - Protects 512 MiB containers from native + heap RSS exceeding 350 MiB (admission) / 360 MiB (stop),
     *   78% utilization, or 64 MiB min free.
     * - Works cleanly on non-cgroup JVMs (`cgroupMemoryMaxBytes == null` or `> 2 GiB`) using heap-appropriate
     *   free/ratio bounds (`85%` / `min(32 MiB, 15% maxHeap)` admission; `88%` / `min(24 MiB, 12% maxHeap)` stop)
     *   alongside `jvmHeapUsedBytes < maxUsedBytesThreshold`.
     */
    internal fun evaluateMemoryHeadroom(maxUsedBytesThreshold: Long): Boolean {
        return try {
            val raw = memoryStatsProvider.sample()
            evaluateSampleHeadroom(raw, maxUsedBytesThreshold)
        } catch (e: Exception) {
            logger.warn("Memory headroom sample failed during coach timeline preparation guard; failing safe (false): {}", e.message)
            false
        }
    }

    internal fun evaluateSampleHeadroom(raw: RawMemorySample, maxUsedBytesThreshold: Long): Boolean {
        val safeHeapMax = raw.jvmHeapMaxBytes
        val safeHeapUsed = raw.jvmHeapUsedBytes
        if (safeHeapMax <= 0L || safeHeapUsed < 0L || safeHeapUsed >= safeHeapMax) {
            return false
        }
        if (safeHeapUsed >= maxUsedBytesThreshold) {
            return false
        }

        val isAdmissionCheck = maxUsedBytesThreshold <= coachAdmissionMaxUsedBytes
        val maxHeapRatio = if (isAdmissionCheck) ADMISSION_MAX_HEAP_RATIO else STOP_MAX_HEAP_RATIO
        val minHeapFree = if (isAdmissionCheck) {
            minOf(ADMISSION_MIN_HEAP_FREE_BYTES, (safeHeapMax * 0.15).toLong().coerceAtLeast(1L))
        } else {
            minOf(STOP_MIN_HEAP_FREE_BYTES, (safeHeapMax * 0.12).toLong().coerceAtLeast(1L))
        }
        val heapFreeBytes = safeHeapMax - safeHeapUsed
        val heapRatio = safeHeapUsed.toDouble() / safeHeapMax.toDouble()
        if (!heapRatio.isFinite() || heapRatio >= maxHeapRatio || heapFreeBytes < minHeapFree) {
            return false
        }

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
            val safeContainerCap = configuredContainerLimitBytes.coerceAtLeast(1L)
            val effectiveLimit = minOf(safeContainerCap, raw.cgroupMemoryMaxBytes)
            val effectiveUsed = maxOf(cgroupCurrent, safeHeapUsed)
            if (effectiveLimit <= 0L || effectiveUsed >= maxUsedBytesThreshold) {
                return false
            }
            val freeBytes = (effectiveLimit - effectiveUsed).coerceAtLeast(0L)
            val ratio = effectiveUsed.toDouble() / effectiveLimit.toDouble()
            return ratio.isFinite() && ratio < maxMemoryUtilizationRatio && freeBytes >= minFreeMemoryBytes
        }

        return true
    }

    internal fun isValidSampleForGcEligibility(raw: RawMemorySample): Boolean {
        if (raw.jvmHeapMaxBytes <= 0L) return false
        if (raw.jvmHeapUsedBytes < 0L || raw.jvmHeapUsedBytes > raw.jvmHeapMaxBytes) return false
        if (raw.jvmHeapCommittedBytes <= 0L ||
            raw.jvmHeapCommittedBytes < raw.jvmHeapUsedBytes ||
            raw.jvmHeapCommittedBytes > raw.jvmHeapMaxBytes
        ) {
            return false
        }
        val hasBoundedContainerCgroupLimit = raw.cgroupMemoryMaxBytes != null &&
            raw.cgroupMemoryMaxBytes in 1L..2_147_483_648L
        if (hasBoundedContainerCgroupLimit) {
            if (configuredContainerLimitBytes <= 0L) return false
            val cgroupCurrent = raw.cgroupMemoryCurrentBytes
            if (cgroupCurrent == null || cgroupCurrent <= 0L) return false
        }
        return true
    }

    /**
     * Pre-admission-only check preserving raw 350/360/78%/64 MiB/512 MiB criteria:
     * - Never runs GC when initial admission already succeeds (`NOT_NEEDED`).
     * - Never runs GC when `coach.timeline.pre-admission-gc-enabled` is false (`DISABLED_BY_CONFIG`).
     * - Fails closed (`INVALID_INITIAL_SAMPLE`, zero GC) before any GC check if heap or bounded-cgroup
     *   sample fields are negative, zero, inverted (`committed < used`), or missing.
     * - Enforces hard safety floors: `minUnusedBytes >= 24 MiB` and `cooldownMs >= 30_000 ms`
     *   (negative, zero, or lower config overrides cannot weaken either floor).
     * - Concurrency-safe: at most ONE thread enters GC, enforcing `cooldownMs`, followed by ONE
     *   validated post-GC sample recheck with zero retry loops.
     */
    internal fun evaluatePreAdmissionHeadroom(): PreAdmissionCheckResult {
        val initialSample = try {
            memoryStatsProvider.sample()
        } catch (e: Exception) {
            logger.warn("Memory headroom sample failed during coach timeline pre-admission guard; failing safe (false): {}", e.message)
            return PreAdmissionCheckResult(
                admitted = false,
                initialSample = null,
                gcStatus = "INITIAL_SAMPLE_ERROR"
            )
        }

        if (!isValidSampleForGcEligibility(initialSample)) {
            return PreAdmissionCheckResult(
                admitted = false,
                initialSample = initialSample,
                gcStatus = "INVALID_INITIAL_SAMPLE"
            )
        }

        if (evaluateSampleHeadroom(initialSample, coachAdmissionMaxUsedBytes)) {
            return PreAdmissionCheckResult(
                admitted = true,
                initialSample = initialSample,
                gcStatus = "NOT_NEEDED"
            )
        }

        if (!preAdmissionGcEnabled) {
            return PreAdmissionCheckResult(
                admitted = false,
                initialSample = initialSample,
                gcStatus = "DISABLED_BY_CONFIG"
            )
        }

        val unusedCommitted = initialSample.jvmHeapCommittedBytes - initialSample.jvmHeapUsedBytes
        val minUnusedBytes = preAdmissionGcMinUnusedCommittedBytes
            .coerceAtLeast(DEFAULT_PRE_ADMISSION_GC_MIN_UNUSED_COMMITTED_BYTES)
        if (unusedCommitted < minUnusedBytes) {
            return PreAdmissionCheckResult(
                admitted = false,
                initialSample = initialSample,
                gcStatus = "SKIPPED_LOW_UNUSED_COMMITTED"
            )
        }

        val cooldownMs = preAdmissionGcCooldownMs
            .coerceAtLeast(DEFAULT_PRE_ADMISSION_GC_COOLDOWN_MS)
        val nowMs = timeProvider.now()
        val skipReason = synchronized(preAdmissionGcLock) {
            when {
                preAdmissionGcInProgress -> "SKIPPED_CONCURRENT_GC"
                lastPreAdmissionGcStartedAtMs != Long.MIN_VALUE &&
                    (nowMs - lastPreAdmissionGcStartedAtMs) < cooldownMs -> "SKIPPED_COOLDOWN"
                else -> {
                    preAdmissionGcInProgress = true
                    lastPreAdmissionGcStartedAtMs = nowMs
                    null
                }
            }
        }
        if (skipReason != null) {
            return PreAdmissionCheckResult(
                admitted = false,
                initialSample = initialSample,
                gcStatus = skipReason
            )
        }

        var gcDurationMs = 0L
        try {
            val gcStartMs = timeProvider.now()
            try {
                preAdmissionGcRunner.invoke()
            } catch (e: Exception) {
                gcDurationMs = (timeProvider.now() - gcStartMs).coerceAtLeast(0L)
                logger.warn("Pre-admission GC hint threw exception; failing safe (false): {}", e.message)
                return PreAdmissionCheckResult(
                    admitted = false,
                    initialSample = initialSample,
                    gcAttempted = true,
                    gcStatus = "GC_RUNNER_ERROR",
                    gcDurationMs = gcDurationMs
                )
            }
            gcDurationMs = (timeProvider.now() - gcStartMs).coerceAtLeast(0L)

            val postGcSample = try {
                memoryStatsProvider.sample()
            } catch (e: Exception) {
                logger.warn("Post-GC memory headroom sample failed during pre-admission guard; failing safe (false): {}", e.message)
                return PreAdmissionCheckResult(
                    admitted = false,
                    initialSample = initialSample,
                    gcAttempted = true,
                    gcStatus = "POST_GC_SAMPLE_ERROR",
                    gcDurationMs = gcDurationMs
                )
            }

            if (!isValidSampleForGcEligibility(postGcSample)) {
                return PreAdmissionCheckResult(
                    admitted = false,
                    initialSample = initialSample,
                    postGcSample = postGcSample,
                    gcAttempted = true,
                    gcStatus = "INVALID_POST_GC_SAMPLE",
                    gcDurationMs = gcDurationMs
                )
            }

            val admittedAfterGc = evaluateSampleHeadroom(postGcSample, coachAdmissionMaxUsedBytes)
            return PreAdmissionCheckResult(
                admitted = admittedAfterGc,
                initialSample = initialSample,
                postGcSample = postGcSample,
                gcAttempted = true,
                gcStatus = if (admittedAfterGc) "RECLAIMED_ADMITTED" else "STILL_BLOCKED_AFTER_GC",
                gcDurationMs = gcDurationMs
            )
        } finally {
            synchronized(preAdmissionGcLock) {
                preAdmissionGcInProgress = false
            }
        }
    }

    private fun safeSampleForTelemetry(): RawMemorySample? {
        return try {
            memoryStatsProvider.sample()
        } catch (_: Exception) {
            null
        }
    }

    private class ByteCountingOutputStream(
        private val delegate: OutputStream,
        private val maxBytes: Int
    ) : OutputStream() {
        var count: Int = 0
            private set

        override fun write(b: Int) {
            count++
            if (count > maxBytes) {
                throw RedisCacheService.BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
            delegate.write(b)
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            if (len <= 0) return
            count += len
            if (count > maxBytes) {
                throw RedisCacheService.BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
            delegate.write(b, off, len)
        }

        override fun flush() = delegate.flush()
        override fun close() = delegate.close()
    }

    private fun compressResultForCache(result: CoachTimelinePreparationResultDto): ByteArray? {
        if (preparedCacheMaxEntries <= 0 || preparedCacheMaxTotalBytes <= 0) return null
        val maxEntryCompressed = minOf(MAX_PREPARED_CACHE_SINGLE_COMPRESSED_BYTES, preparedCacheMaxTotalBytes)
        return try {
            val rawOut = ByteArrayOutputStream(2_048)
            val boundedCompressedOut = ByteCountingOutputStream(rawOut, maxEntryCompressed)
            GZIPOutputStream(boundedCompressedOut, 2_048).use { gzipOut ->
                val boundedJsonOut = ByteCountingOutputStream(gzipOut, MAX_PREPARED_CACHE_SINGLE_INFLATED_BYTES)
                cacheObjectMapper.writeValue(boundedJsonOut, result.copy(cacheHit = false))
            }
            rawOut.toByteArray()
        } catch (_: Exception) {
            null
        }
    }

    private sealed class CachedPreparationLookup {
        data object Miss : CachedPreparationLookup()
        data class Hit(val result: CoachTimelinePreparationResultDto) : CachedPreparationLookup()
        data class Aborted(val stopReason: String) : CachedPreparationLookup()
    }

    private fun evictCachedPreparationKey(cacheKey: String) {
        synchronized(preparedCacheLock) {
            val removed = preparedCache.remove(cacheKey)
            if (removed != null) {
                preparedCacheTotalBytes = (preparedCacheTotalBytes - removed.compressedBytes.size).coerceAtLeast(0)
            }
        }
    }

    private suspend fun readCachedPreparationBounded(
        cacheKey: String,
        deadlineAtMs: Long,
        lingeringWorkers: MutableList<RiotApiService.LingeringWorkerHandle>
    ): CachedPreparationLookup {
        val now = timeProvider.now()
        if (now >= deadlineAtMs) {
            return CachedPreparationLookup.Aborted("DEADLINE_EXCEEDED")
        }
        val compressedBytes = synchronized(preparedCacheLock) {
            val entry = preparedCache[cacheKey] ?: return@synchronized null
            if (now >= entry.expiresAtMs) {
                preparedCache.remove(cacheKey)
                preparedCacheTotalBytes = (preparedCacheTotalBytes - entry.compressedBytes.size).coerceAtLeast(0)
                return@synchronized null
            }
            entry.compressedBytes
        } ?: return CachedPreparationLookup.Miss

        if (!hasCoachAdmissionHeadroom()) {
            return CachedPreparationLookup.Aborted("MEMORY_HEADROOM_ADMISSION_BLOCKED")
        }

        val decodeFailure = AtomicReference<String?>(null)
        val decoded = runBoundedCacheReadWithDeadline(
            maxInflatedBytes = MAX_PREPARED_CACHE_SINGLE_INFLATED_BYTES,
            deadlineAtMs = deadlineAtMs,
            headroomGuard = ::hasCoachContinueHeadroom,
            failureReasonHolder = decodeFailure,
            lingeringWorkers = lingeringWorkers
        ) {
            warmCacheDecodeHook?.invoke()
            ByteArrayInputStream(compressedBytes).use { rawIn ->
                GZIPInputStream(rawIn, 2_048).use { gzipIn ->
                    RedisCacheService.BoundedInflatingInputStream(
                        delegate = gzipIn,
                        maxInflatedBytes = MAX_PREPARED_CACHE_SINGLE_INFLATED_BYTES,
                        deadlineAtMs = deadlineAtMs,
                        timeProvider = timeProvider,
                        headroomGuard = ::hasCoachContinueHeadroom,
                        failureReasonHolder = decodeFailure
                    ).use { boundedIn ->
                        cacheObjectMapper.readValue(
                            boundedIn,
                            CoachTimelinePreparationResultDto::class.java
                        ).copy(cacheHit = true)
                    }
                }
            }
        }

        val abortReason = decodeFailure.get()
        if (abortReason != null) {
            return when (abortReason) {
                "DEADLINE_EXCEEDED" -> CachedPreparationLookup.Aborted("DEADLINE_EXCEEDED")
                "MEMORY_HEADROOM_STOP" -> CachedPreparationLookup.Aborted("MEMORY_HEADROOM_STOP")
                "HTTP_EXECUTOR_SATURATED" -> CachedPreparationLookup.Aborted("HTTP_EXECUTOR_SATURATED")
                else -> {
                    evictCachedPreparationKey(cacheKey)
                    CachedPreparationLookup.Miss
                }
            }
        }

        return if (decoded != null) {
            CachedPreparationLookup.Hit(decoded)
        } else {
            evictCachedPreparationKey(cacheKey)
            CachedPreparationLookup.Miss
        }
    }

    private fun putCachedPreparation(
        keys: List<String>,
        playerScopeKey: String,
        result: CoachTimelinePreparationResultDto
    ) {
        val safeMaxEntries = preparedCacheMaxEntries.coerceAtLeast(0)
        val safeMaxBytes = preparedCacheMaxTotalBytes.coerceAtLeast(0)
        if (safeMaxEntries == 0 || safeMaxBytes == 0 || keys.isEmpty()) return
        val compressed = compressResultForCache(result) ?: return

        val now = timeProvider.now()
        val expiresAtMs = now + preparedCacheTtlMs.coerceAtLeast(1_000L)
        val distinctKeys = keys.distinct()

        synchronized(preparedCacheLock) {
            // Remove expired entries and any prior entries for the same (region, puuid)
            val staleKeys = preparedCache.entries
                .filter { (k, v) -> k !in distinctKeys && (v.playerScopeKey == playerScopeKey || now >= v.expiresAtMs) }
                .map { it.key }
            for (staleKey in staleKeys) {
                val removed = preparedCache.remove(staleKey)
                if (removed != null) {
                    preparedCacheTotalBytes = (preparedCacheTotalBytes - removed.compressedBytes.size).coerceAtLeast(0)
                }
            }

            if (compressed.size * distinctKeys.size > safeMaxBytes) {
                return
            }

            for (key in distinctKeys) {
                val prev = preparedCache.put(
                    key,
                    CachedPreparationEntry(
                        playerScopeKey = playerScopeKey,
                        expiresAtMs = expiresAtMs,
                        compressedBytes = compressed
                    )
                )
                if (prev != null) {
                    preparedCacheTotalBytes = (preparedCacheTotalBytes - prev.compressedBytes.size).coerceAtLeast(0)
                }
                preparedCacheTotalBytes += compressed.size
            }

            while (preparedCache.size > safeMaxEntries || preparedCacheTotalBytes > safeMaxBytes) {
                val eldestKey = preparedCache.entries.firstOrNull()?.key ?: break
                val evicted = preparedCache.remove(eldestKey)
                if (evicted != null) {
                    preparedCacheTotalBytes = (preparedCacheTotalBytes - evicted.compressedBytes.size).coerceAtLeast(0)
                }
            }
        }
    }

    private suspend fun <T> runBoundedCacheReadWithDeadline(
        maxInflatedBytes: Int,
        deadlineAtMs: Long?,
        headroomGuard: (() -> Boolean)?,
        failureReasonHolder: AtomicReference<String?>,
        lingeringWorkers: MutableList<RiotApiService.LingeringWorkerHandle>? = null,
        block: () -> T?
    ): T? {
        if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
            failureReasonHolder.set("DEADLINE_EXCEEDED")
            return null
        }
        if (headroomGuard != null && !headroomGuard.invoke()) {
            failureReasonHolder.set("MEMORY_HEADROOM_STOP")
            return null
        }

        val activeHook = kotlin.coroutines.coroutineContext[RiotApiService.LingeringWorkerHookElement]?.hook
        val taskClaimedOrCancelled = AtomicBoolean(false)
        val workerThreadRef = AtomicReference<Thread?>(null)
        val workerDoneLatch = CountDownLatch(1)

        val future = try {
            cacheReadExecutor.submit(Callable<T?> {
                val currentThread = Thread.currentThread()
                workerThreadRef.set(currentThread)
                if (!taskClaimedOrCancelled.compareAndSet(false, true)) {
                    workerThreadRef.set(null)
                    workerDoneLatch.countDown()
                    return@Callable null
                }
                activeCacheReadWorkers.incrementAndGet()
                try {
                    if (currentThread.isInterrupted ||
                        (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)
                    ) {
                        failureReasonHolder.set("DEADLINE_EXCEEDED")
                        null
                    } else {
                        RedisCacheService.withBoundedInflateGuards(
                            RedisCacheService.BoundedInflateContext(
                                maxInflatedBytes = maxInflatedBytes,
                                deadlineAtMs = deadlineAtMs,
                                timeProvider = timeProvider,
                                headroomGuard = headroomGuard,
                                failureReasonHolder = failureReasonHolder
                            )
                        ) {
                            block()
                        }
                    }
                } finally {
                    activeCacheReadWorkers.decrementAndGet()
                    Thread.interrupted()
                    workerDoneLatch.countDown()
                    workerThreadRef.set(null)
                }
            })
        } catch (_: RejectedExecutionException) {
            failureReasonHolder.set("HTTP_EXECUTOR_SATURATED")
            return null
        }

        var cancelledBeforeStart = false
        val value = try {
            kotlinx.coroutines.runInterruptible(Dispatchers.IO) {
                if (deadlineAtMs != null) {
                    val remainingMs = (deadlineAtMs - timeProvider.now()).coerceAtLeast(1L)
                    future.get(remainingMs, TimeUnit.MILLISECONDS)
                } else {
                    future.get()
                }
            }
        } catch (_: TimeoutException) {
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            future.cancel(true)
            if (failureReasonHolder.get() == null) {
                failureReasonHolder.set("DEADLINE_EXCEEDED")
            }
            null
        } catch (e: CancellationException) {
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            future.cancel(true)
            throw e
        } catch (_: InterruptedException) {
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            future.cancel(true)
            Thread.currentThread().interrupt()
            if (failureReasonHolder.get() == null) {
                failureReasonHolder.set("DEADLINE_EXCEEDED")
            }
            null
        } catch (e: ExecutionException) {
            val cause = e.cause
            val aborted = generateSequence<Throwable>(cause) { it.cause }
                .filterIsInstance<RedisCacheService.BoundedCacheReadAbortedException>()
                .firstOrNull()
            if (aborted != null) {
                failureReasonHolder.set(aborted.reason)
            } else if (failureReasonHolder.get() == null) {
                failureReasonHolder.set("CACHE_READ_ERROR")
            }
            null
        } finally {
            if (!cancelledBeforeStart && workerDoneLatch.count > 0L) {
                val joined = withContext(kotlinx.coroutines.NonCancellable + Dispatchers.IO) {
                    workerDoneLatch.await(25L, TimeUnit.MILLISECONDS)
                }
                if (!joined && workerDoneLatch.count > 0L) {
                    val handle = RiotApiService.LingeringWorkerHandle(workerDoneLatch, workerThreadRef)
                    if (lingeringWorkers != null) {
                        lingeringWorkers.add(handle)
                    } else {
                        activeHook?.invoke(handle)
                    }
                }
            }
        }

        if (failureReasonHolder.get() != null) {
            return null
        }
        if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
            failureReasonHolder.set("DEADLINE_EXCEEDED")
            return null
        }
        if (headroomGuard != null && !headroomGuard.invoke()) {
            failureReasonHolder.set("MEMORY_HEADROOM_STOP")
            return null
        }
        return value
    }

    private suspend fun <T> coalesceSingleflightWithCallerIsolation(
        key: String,
        map: ConcurrentHashMap<String, InFlightEntry<T>>,
        callerDeadlineMs: Long,
        onCallerDeadlineExceeded: () -> T,
        ownerBlock: suspend () -> T
    ): T {
        val entry = singleflightMutex.withLock {
            val existing = map[key]
            if (existing != null && !existing.deferred.isCompleted) {
                existing.activeWaiters += 1
                existing
            } else {
                val created = InFlightEntry<T>()
                map[key] = created
                created.workerJob = singleflightScope.launch {
                    try {
                        val outcome = ownerBlock()
                        created.deferred.complete(outcome)
                    } catch (t: Throwable) {
                        if (t is CancellationException) {
                            // Only happens when all waiters detached and cancelled workerJob
                            created.deferred.cancel(t)
                        } else {
                            created.deferred.completeExceptionally(t)
                        }
                    } finally {
                        singleflightMutex.withLock {
                            map.remove(key, created)
                        }
                    }
                }
                created
            }
        }

        suspend fun detachWaiterIfUnfinished() {
            singleflightMutex.withLock {
                if (entry.activeWaiters > 0) {
                    entry.activeWaiters -= 1
                }
                if (entry.activeWaiters == 0 && !entry.deferred.isCompleted) {
                    map.remove(key, entry)
                    entry.workerJob?.cancel()
                }
            }
        }

        val now = timeProvider.now()
        val remainingMs = callerDeadlineMs - now
        if (remainingMs <= 0L && !entry.deferred.isCompleted) {
            detachWaiterIfUnfinished()
            return onCallerDeadlineExceeded()
        }

        return try {
            val awaited = withTimeoutOrNull((remainingMs + 75L).coerceAtLeast(1L)) {
                entry.deferred.await()
            }
            if (awaited == null) {
                detachWaiterIfUnfinished()
                onCallerDeadlineExceeded()
            } else {
                awaited
            }
        } catch (ce: CancellationException) {
            withContext(kotlinx.coroutines.NonCancellable) {
                detachWaiterIfUnfinished()
            }
            throw ce
        }
    }

    /**
     * Controller-facing pre-decode entrypoint for `POST /prepare-coach/{region}/{puuid}` and
     * legacy `GET /analyze/{region}/{puuid}`:
     *  1. Enforces 350 MiB dual heap + cgroup admission headroom BEFORE `SummonerProfileDto` decode.
     *  2. Coalesces concurrent same-player requests (both warm and cold) via isolated singleflight BEFORE decode.
     *  3. Acquires bounded global concurrency slot (`maxConcurrentPreparations = 2`) BEFORE any warm/cold decode,
     *     retaining the slot until every spawned HTTP or cache/inflate worker thread actually exits.
     *  4. Checks warm compressed `preparedCache` with bounded inflate inside the slot BEFORE `SummonerProfileDto` decode.
     *  5. Decodes `SummonerProfileDto` with bounded inflate + deadline, slices ONLY `<= 20` match IDs and
     *     matching `recentMatches` DTOs (never calling `getMatchesByIds`), drops the full profile reference
     *     immediately so GC can reclaim 883-ID history, and runs sequential single-player compaction.
     */
    suspend fun prepareCoachTimelinesForPlayer(
        region: String,
        puuid: String,
        requestedMatchIds: List<String?>? = null,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION,
        includeEmptyFallbackAnalysisForLegacyRoute: Boolean = false,
        profileLoader: () -> SummonerProfileDto?
    ): PlayerPreparationOutcome {
        val normalizedRegion = region.trim().lowercase()
        val trimmedPuuid = puuid.trim()
        val sanitizedRequestedIds = sanitizeMatchIds(requestedMatchIds)
        val startMs = timeProvider.now()
        val callerDeadlineMs = startMs + prepDeadlineMs.coerceAtLeast(100L)

        // 1. Pre-decode 350 MiB admission guard (only stage eligible for optional pre-admission GC hint)
        val preAdmission = evaluatePreAdmissionHeadroom()
        if (!preAdmission.admitted) {
            val blocked = buildBlockedResult(
                normalizedRegion = normalizedRegion,
                puuid = trimmedPuuid,
                orderedMatchIds = sanitizedRequestedIds,
                stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                analyzerVersion = analyzerVersion
            )
            logCoachPrepStageSummary(
                normalizedRegion = normalizedRegion,
                stage = "pre_admission",
                result = blocked,
                totalMs = (timeProvider.now() - startMs).coerceAtLeast(0L),
                preAdmission = preAdmission,
                captureEndSample = false
            )
            return PlayerPreparationOutcome.Ready(blocked)
        }

        val playerCacheKey = buildPlayerPreparationCacheKey(
            normalizedRegion = normalizedRegion,
            puuid = trimmedPuuid,
            explicitMatchIds = sanitizedRequestedIds,
            analyzerVersion = analyzerVersion
        )

        // 2. Isolated singleflight coalescing (coalesces both warm and cold same-player requests; zero I/O under mutex)
        return coalesceSingleflightWithCallerIsolation(
            key = playerCacheKey,
            map = inFlightPlayerPreparations,
            callerDeadlineMs = callerDeadlineMs,
            onCallerDeadlineExceeded = {
                val blocked = buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = trimmedPuuid,
                    orderedMatchIds = sanitizedRequestedIds,
                    stopReason = "DEADLINE_EXCEEDED",
                    analyzerVersion = analyzerVersion
                )
                logCoachPrepStageSummary(
                    normalizedRegion = normalizedRegion,
                    stage = "singleflight_caller_deadline",
                    result = blocked,
                    totalMs = (timeProvider.now() - startMs).coerceAtLeast(0L),
                    preAdmission = preAdmission
                )
                PlayerPreparationOutcome.Ready(blocked)
            }
        ) {
            executeSlotGuardedPlayerPreparation(
                normalizedRegion = normalizedRegion,
                puuid = trimmedPuuid,
                sanitizedRequestedIds = sanitizedRequestedIds,
                analyzerVersion = analyzerVersion,
                playerCacheKey = playerCacheKey,
                includeEmptyFallbackAnalysisForLegacyRoute = includeEmptyFallbackAnalysisForLegacyRoute,
                preAdmission = preAdmission,
                profileLoader = profileLoader
            )
        }
    }

    private fun logCoachPrepStageSummary(
        normalizedRegion: String,
        stage: String,
        result: CoachTimelinePreparationResultDto,
        totalMs: Long,
        warmLookupMs: Long = 0L,
        profileDecodeMs: Long = 0L,
        riotLoopMs: Long = 0L,
        httpAttempts: Int = 0,
        preAdmission: PreAdmissionCheckResult? = null,
        captureEndSample: Boolean = true
    ) {
        val endSample = if (captureEndSample) {
            safeSampleForTelemetry()
        } else {
            preAdmission?.postGcSample ?: preAdmission?.initialSample
        }
        val memPreFormatted = formatPayloadFreeMemorySample(preAdmission?.initialSample)
        val memPostGcFormatted = preAdmission?.postGcSample?.let(::formatPayloadFreeMemorySample) ?: "none"
        val memEndFormatted = formatPayloadFreeMemorySample(endSample)
        val gcAttempted = preAdmission?.gcAttempted ?: false
        val gcStatus = preAdmission?.gcStatus ?: "NOT_APPLICABLE"
        val gcMs = preAdmission?.gcDurationMs ?: 0L

        if (result.status == "COMPLETE") {
            val message = "coachTimelinePrep summary region=$normalizedRegion status=${result.status} stage=$stage " +
                "covered=${result.coveredMatchIds.size}/${result.requestedMatchIds.size} cacheHit=${result.cacheHit} " +
                "totalMs=$totalMs warmLookupMs=$warmLookupMs profileDecodeMs=$profileDecodeMs riotLoopMs=$riotLoopMs httpAttempts=$httpAttempts " +
                "gcEnabled=$preAdmissionGcEnabled gcAttempted=$gcAttempted gcStatus=$gcStatus gcMs=$gcMs " +
                "memPre={$memPreFormatted} memPostGc={$memPostGcFormatted} memEnd={$memEndFormatted}"
            logger.info(message)
            stageSummaryObserver?.invoke(message)
        } else {
            val stopReason = result.stopReason ?: "NONE"
            val message = "coachTimelinePrep summary region=$normalizedRegion status=${result.status} stopReason=$stopReason stage=$stage " +
                "covered=${result.coveredMatchIds.size}/${result.requestedMatchIds.size} cacheHit=${result.cacheHit} " +
                "totalMs=$totalMs warmLookupMs=$warmLookupMs profileDecodeMs=$profileDecodeMs riotLoopMs=$riotLoopMs httpAttempts=$httpAttempts " +
                "gcEnabled=$preAdmissionGcEnabled gcAttempted=$gcAttempted gcStatus=$gcStatus gcMs=$gcMs " +
                "memPre={$memPreFormatted} memPostGc={$memPostGcFormatted} memEnd={$memEndFormatted}"
            logger.warn(message)
            stageSummaryObserver?.invoke(message)
        }
    }

    private suspend fun executeSlotGuardedPlayerPreparation(
        normalizedRegion: String,
        puuid: String,
        sanitizedRequestedIds: List<String>,
        analyzerVersion: String,
        playerCacheKey: String,
        includeEmptyFallbackAnalysisForLegacyRoute: Boolean,
        preAdmission: PreAdmissionCheckResult,
        profileLoader: () -> SummonerProfileDto?
    ): PlayerPreparationOutcome {
        val ownerStartMs = timeProvider.now()
        val ownerDeadlineMs = ownerStartMs + prepDeadlineMs.coerceAtLeast(100L)

        // Re-check 350 MiB admission headroom before slot & before any decode (strictly GC-free post-pre-admission)
        if (!hasCoachAdmissionHeadroom()) {
            val blocked = buildBlockedResult(
                normalizedRegion = normalizedRegion,
                puuid = puuid,
                orderedMatchIds = sanitizedRequestedIds,
                stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                analyzerVersion = analyzerVersion
            )
            logCoachPrepStageSummary(
                normalizedRegion = normalizedRegion,
                stage = "slot_admission",
                result = blocked,
                totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                preAdmission = preAdmission
            )
            return PlayerPreparationOutcome.Ready(blocked)
        }

        // 3. Acquire global concurrency slot BEFORE warm-cache inflate or profile decode
        val safeMaxConcurrent = maxConcurrentPreparations.coerceAtLeast(1)
        if (!tryAcquireGlobalSlot(safeMaxConcurrent)) {
            val blocked = buildBlockedResult(
                normalizedRegion = normalizedRegion,
                puuid = puuid,
                orderedMatchIds = sanitizedRequestedIds,
                stopReason = "CONCURRENCY_LIMIT_REACHED",
                analyzerVersion = analyzerVersion
            )
            logCoachPrepStageSummary(
                normalizedRegion = normalizedRegion,
                stage = "concurrency_slot",
                result = blocked,
                totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                preAdmission = preAdmission
            )
            return PlayerPreparationOutcome.Ready(blocked)
        }

        val lingeringWorkers = Collections.synchronizedList(mutableListOf<RiotApiService.LingeringWorkerHandle>())
        var warmLookupMs = 0L
        var profileDecodeMs = 0L
        try {
            if (!hasCoachAdmissionHeadroom()) {
                val blocked = buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = puuid,
                    orderedMatchIds = sanitizedRequestedIds,
                    stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                    analyzerVersion = analyzerVersion
                )
                logCoachPrepStageSummary(
                    normalizedRegion = normalizedRegion,
                    stage = "post_slot_admission",
                    result = blocked,
                    totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                    preAdmission = preAdmission
                )
                return PlayerPreparationOutcome.Ready(blocked)
            }

            // 4. Bounded warm cache lookup inside the global slot BEFORE SummonerProfileDto decode
            val warmStartMs = timeProvider.now()
            val warmPlayerLookup = readCachedPreparationBounded(playerCacheKey, ownerDeadlineMs, lingeringWorkers)
            warmLookupMs += (timeProvider.now() - warmStartMs).coerceAtLeast(0L)
            when (warmPlayerLookup) {
                is CachedPreparationLookup.Hit -> {
                    logCoachPrepStageSummary(
                        normalizedRegion = normalizedRegion,
                        stage = "player_warm_cache",
                        result = warmPlayerLookup.result,
                        totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                        warmLookupMs = warmLookupMs,
                        preAdmission = preAdmission
                    )
                    return PlayerPreparationOutcome.Ready(warmPlayerLookup.result)
                }
                is CachedPreparationLookup.Aborted -> {
                    val blocked = buildBlockedResult(
                        normalizedRegion = normalizedRegion,
                        puuid = puuid,
                        orderedMatchIds = sanitizedRequestedIds,
                        stopReason = warmPlayerLookup.stopReason,
                        analyzerVersion = analyzerVersion
                    )
                    logCoachPrepStageSummary(
                        normalizedRegion = normalizedRegion,
                        stage = "player_warm_cache",
                        result = blocked,
                        totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                        warmLookupMs = warmLookupMs,
                        preAdmission = preAdmission
                    )
                    return PlayerPreparationOutcome.Ready(blocked)
                }
                CachedPreparationLookup.Miss -> Unit
            }

            // 5. Bounded profile decode + immediate slice to <= 20 recent match IDs/DTOs (releasing 883-ID profile immediately)
            val profileFailure = AtomicReference<String?>(null)
            val profileStartMs = timeProvider.now()
            val slicedProfile = extractBoundedProfileSlice(
                sanitizedRequestedIds = sanitizedRequestedIds,
                deadlineAtMs = ownerDeadlineMs,
                failureReasonHolder = profileFailure,
                lingeringWorkers = lingeringWorkers,
                profileLoader = profileLoader
            )
            profileDecodeMs = (timeProvider.now() - profileStartMs).coerceAtLeast(0L)
            val profileAbortReason = profileFailure.get()
            if (profileAbortReason != null) {
                val mappedReason = when (profileAbortReason) {
                    "DEADLINE_EXCEEDED" -> "DEADLINE_EXCEEDED"
                    "MEMORY_HEADROOM_STOP" -> "MEMORY_HEADROOM_STOP"
                    "HTTP_EXECUTOR_SATURATED" -> "HTTP_EXECUTOR_SATURATED"
                    else -> "TIMELINE_UNAVAILABLE"
                }
                val blocked = buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = puuid,
                    orderedMatchIds = sanitizedRequestedIds,
                    stopReason = mappedReason,
                    analyzerVersion = analyzerVersion
                )
                logCoachPrepStageSummary(
                    normalizedRegion = normalizedRegion,
                    stage = "profile_decode",
                    result = blocked,
                    totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                    warmLookupMs = warmLookupMs,
                    profileDecodeMs = profileDecodeMs,
                    preAdmission = preAdmission
                )
                return PlayerPreparationOutcome.Ready(blocked)
            }
            if (slicedProfile == null) {
                val endFormatted = formatPayloadFreeMemorySample(safeSampleForTelemetry())
                val preFormatted = formatPayloadFreeMemorySample(preAdmission.initialSample)
                val postGcFormatted = preAdmission.postGcSample?.let(::formatPayloadFreeMemorySample) ?: "none"
                val notFoundMessage = "coachTimelinePrep summary region=$normalizedRegion status=PROFILE_NOT_FOUND stage=profile_decode " +
                    "totalMs=${(timeProvider.now() - ownerStartMs).coerceAtLeast(0L)} warmLookupMs=$warmLookupMs profileDecodeMs=$profileDecodeMs " +
                    "gcEnabled=$preAdmissionGcEnabled gcAttempted=${preAdmission.gcAttempted} gcStatus=${preAdmission.gcStatus} gcMs=${preAdmission.gcDurationMs} " +
                    "memPre={$preFormatted} memPostGc={$postGcFormatted} memEnd={$endFormatted}"
                logger.info(notFoundMessage)
                stageSummaryObserver?.invoke(notFoundMessage)
                return PlayerPreparationOutcome.ProfileNotFound
            }

            val orderedMatchIds = slicedProfile.effectiveMatchIds
            val orderedMatchDtos = slicedProfile.orderedMatchDtos
            if (orderedMatchIds.isEmpty()) {
                val blocked = CoachTimelinePreparationResultDto(
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
                logCoachPrepStageSummary(
                    normalizedRegion = normalizedRegion,
                    stage = "no_matches",
                    result = blocked,
                    totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                    warmLookupMs = warmLookupMs,
                    profileDecodeMs = profileDecodeMs,
                    preAdmission = preAdmission
                )
                return PlayerPreparationOutcome.Ready(blocked)
            }

            val directCacheKey = buildDirectPreparationCacheKey(
                normalizedRegion = normalizedRegion,
                puuid = puuid,
                orderedMatchIds = orderedMatchIds,
                analyzerVersion = analyzerVersion
            )
            val directWarmStartMs = timeProvider.now()
            val warmDirectLookup = readCachedPreparationBounded(directCacheKey, ownerDeadlineMs, lingeringWorkers)
            warmLookupMs += (timeProvider.now() - directWarmStartMs).coerceAtLeast(0L)
            when (warmDirectLookup) {
                is CachedPreparationLookup.Hit -> {
                    logCoachPrepStageSummary(
                        normalizedRegion = normalizedRegion,
                        stage = "direct_warm_cache",
                        result = warmDirectLookup.result,
                        totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                        warmLookupMs = warmLookupMs,
                        profileDecodeMs = profileDecodeMs,
                        preAdmission = preAdmission
                    )
                    return PlayerPreparationOutcome.Ready(warmDirectLookup.result)
                }
                is CachedPreparationLookup.Aborted -> {
                    val blocked = buildBlockedResult(
                        normalizedRegion = normalizedRegion,
                        puuid = puuid,
                        orderedMatchIds = orderedMatchIds,
                        stopReason = warmDirectLookup.stopReason,
                        analyzerVersion = analyzerVersion
                    )
                    logCoachPrepStageSummary(
                        normalizedRegion = normalizedRegion,
                        stage = "direct_warm_cache",
                        result = blocked,
                        totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                        warmLookupMs = warmLookupMs,
                        profileDecodeMs = profileDecodeMs,
                        preAdmission = preAdmission
                    )
                    return PlayerPreparationOutcome.Ready(blocked)
                }
                CachedPreparationLookup.Miss -> Unit
            }

            val playerScopeKey = "$normalizedRegion:$puuid"
            val httpAttempts = AtomicInteger(0)
            val riotLoopStartMs = timeProvider.now()
            val result = RiotApiService.withLingeringWorkerHook({ handle -> lingeringWorkers.add(handle) }) {
                executeCoreTimelinePreparation(
                    normalizedRegion = normalizedRegion,
                    puuid = puuid,
                    orderedMatchIds = orderedMatchIds,
                    matchDtos = orderedMatchDtos,
                    analyzerVersion = analyzerVersion,
                    cacheKeysToPopulate = listOf(directCacheKey, playerCacheKey),
                    playerScopeKey = playerScopeKey,
                    deadlineAtMs = ownerDeadlineMs,
                    lingeringWorkers = lingeringWorkers,
                    httpAttemptCounter = httpAttempts
                )
            }
            val riotLoopMs = (timeProvider.now() - riotLoopStartMs).coerceAtLeast(0L)
            logCoachPrepStageSummary(
                normalizedRegion = normalizedRegion,
                stage = "timeline_loop",
                result = result,
                totalMs = (timeProvider.now() - ownerStartMs).coerceAtLeast(0L),
                warmLookupMs = warmLookupMs,
                profileDecodeMs = profileDecodeMs,
                riotLoopMs = riotLoopMs,
                httpAttempts = httpAttempts.get(),
                preAdmission = preAdmission
            )

            val nonTransientEmptyAnalysis = if (
                includeEmptyFallbackAnalysisForLegacyRoute &&
                result.analysis == null &&
                result.status == "BLOCKED" &&
                isNonTransientPerMatchMissingStopReason(result.stopReason)
            ) {
                timelineAnalysisService.analyze(emptyList(), puuid, orderedMatchDtos)
            } else {
                null
            }

            return PlayerPreparationOutcome.Ready(
                result = result,
                nonTransientEmptyAnalysis = nonTransientEmptyAnalysis
            )
        } finally {
            releaseGlobalSlotAfterWorkers(lingeringWorkers)
        }
    }

    private data class BoundedProfileSlice(
        val effectiveMatchIds: List<String>,
        val orderedMatchDtos: List<MatchDto>
    )

    private suspend fun extractBoundedProfileSlice(
        sanitizedRequestedIds: List<String>,
        deadlineAtMs: Long,
        failureReasonHolder: AtomicReference<String?>,
        lingeringWorkers: MutableList<RiotApiService.LingeringWorkerHandle>,
        profileLoader: () -> SummonerProfileDto?
    ): BoundedProfileSlice? {
        val loadedProfile = runBoundedCacheReadWithDeadline(
            maxInflatedBytes = MAX_PROFILE_CACHE_INFLATE_BYTES,
            deadlineAtMs = deadlineAtMs,
            headroomGuard = ::hasCoachContinueHeadroom,
            failureReasonHolder = failureReasonHolder,
            lingeringWorkers = lingeringWorkers,
            block = profileLoader
        ) ?: return null

        val recentMatches = loadedProfile.recentMatches.orEmpty()
        val profileMatchIds = (
            loadedProfile.allMatchIds
                ?.asSequence()
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }
                ?.distinct()
                ?.take(MAX_COACH_TIMELINE_MATCHES)
                ?.toList()
                ?.takeIf { it.isNotEmpty() }
                ?: recentMatches.asSequence()
                    .mapNotNull { it.metadata?.matchId?.trim()?.takeIf { id -> id.isNotBlank() } }
                    .distinct()
                    .take(MAX_COACH_TIMELINE_MATCHES)
                    .toList()
            )

        val allowedSet = profileMatchIds.toSet()
        val effectiveMatchIds = if (sanitizedRequestedIds.isNotEmpty()) {
            sanitizedRequestedIds
                .filter { it in allowedSet }
                .take(MAX_COACH_TIMELINE_MATCHES)
                .ifEmpty { profileMatchIds }
        } else {
            profileMatchIds
        }

        val effectiveSet = effectiveMatchIds.toSet()
        val recentById = HashMap<String, MatchDto>(effectiveMatchIds.size)
        for (matchDto in recentMatches) {
            val mid = matchDto.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() } ?: continue
            if (mid in effectiveSet) {
                recentById[mid] = matchDto
            }
        }
        val orderedDtos = effectiveMatchIds.mapNotNull { recentById[it] }
        return BoundedProfileSlice(
            effectiveMatchIds = effectiveMatchIds,
            orderedMatchDtos = orderedDtos
        )
    }

    /**
     * Bounded on-demand preparation of at most [MAX_COACH_TIMELINE_MATCHES] match timelines when
     * `requestedMatchIds` and `matchDtos` are already resolved.
     */
    suspend fun prepareCoachTimelines(
        region: String,
        puuid: String,
        requestedMatchIds: List<String>,
        matchDtos: List<MatchDto>,
        analyzerVersion: String = COACH_TIMELINE_ANALYZER_VERSION
    ): CoachTimelinePreparationResultDto {
        val normalizedRegion = region.trim().lowercase()
        val trimmedPuuid = puuid.trim()
        val orderedMatchIds = sanitizeMatchIds(requestedMatchIds)

        if (orderedMatchIds.isEmpty()) {
            return CoachTimelinePreparationResultDto(
                status = "BLOCKED",
                region = normalizedRegion,
                puuid = trimmedPuuid,
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

        val startMs = timeProvider.now()
        val callerDeadlineMs = startMs + prepDeadlineMs.coerceAtLeast(100L)

        val preAdmission = evaluatePreAdmissionHeadroom()
        if (!preAdmission.admitted) {
            val blocked = buildBlockedResult(
                normalizedRegion = normalizedRegion,
                puuid = trimmedPuuid,
                orderedMatchIds = orderedMatchIds,
                stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                analyzerVersion = analyzerVersion
            )
            logCoachPrepStageSummary(
                normalizedRegion = normalizedRegion,
                stage = "direct_pre_admission",
                result = blocked,
                totalMs = (timeProvider.now() - startMs).coerceAtLeast(0L),
                preAdmission = preAdmission,
                captureEndSample = false
            )
            return blocked
        }

        val playerScopeKey = "$normalizedRegion:$trimmedPuuid"
        val cacheKey = buildDirectPreparationCacheKey(normalizedRegion, trimmedPuuid, orderedMatchIds, analyzerVersion)

        return coalesceSingleflightWithCallerIsolation(
            key = cacheKey,
            map = inFlightDirectPreparations,
            callerDeadlineMs = callerDeadlineMs,
            onCallerDeadlineExceeded = {
                buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = trimmedPuuid,
                    orderedMatchIds = orderedMatchIds,
                    stopReason = "DEADLINE_EXCEEDED",
                    analyzerVersion = analyzerVersion
                )
            }
        ) {
            val ownerStartMs = timeProvider.now()
            val ownerDeadlineMs = ownerStartMs + prepDeadlineMs.coerceAtLeast(100L)

            if (!hasCoachAdmissionHeadroom()) {
                return@coalesceSingleflightWithCallerIsolation buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = trimmedPuuid,
                    orderedMatchIds = orderedMatchIds,
                    stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                    analyzerVersion = analyzerVersion
                )
            }

            val safeMaxConcurrent = maxConcurrentPreparations.coerceAtLeast(1)
            if (!tryAcquireGlobalSlot(safeMaxConcurrent)) {
                return@coalesceSingleflightWithCallerIsolation buildBlockedResult(
                    normalizedRegion = normalizedRegion,
                    puuid = trimmedPuuid,
                    orderedMatchIds = orderedMatchIds,
                    stopReason = "CONCURRENCY_LIMIT_REACHED",
                    analyzerVersion = analyzerVersion
                )
            }

            val lingeringWorkers = Collections.synchronizedList(mutableListOf<RiotApiService.LingeringWorkerHandle>())
            try {
                if (!hasCoachAdmissionHeadroom()) {
                    return@coalesceSingleflightWithCallerIsolation buildBlockedResult(
                        normalizedRegion = normalizedRegion,
                        puuid = trimmedPuuid,
                        orderedMatchIds = orderedMatchIds,
                        stopReason = "MEMORY_HEADROOM_ADMISSION_BLOCKED",
                        analyzerVersion = analyzerVersion
                    )
                }

                when (val warmLookup = readCachedPreparationBounded(cacheKey, ownerDeadlineMs, lingeringWorkers)) {
                    is CachedPreparationLookup.Hit -> return@coalesceSingleflightWithCallerIsolation warmLookup.result
                    is CachedPreparationLookup.Aborted -> return@coalesceSingleflightWithCallerIsolation buildBlockedResult(
                        normalizedRegion = normalizedRegion,
                        puuid = trimmedPuuid,
                        orderedMatchIds = orderedMatchIds,
                        stopReason = warmLookup.stopReason,
                        analyzerVersion = analyzerVersion
                    )
                    CachedPreparationLookup.Miss -> Unit
                }

                RiotApiService.withLingeringWorkerHook({ handle -> lingeringWorkers.add(handle) }) {
                    executeCoreTimelinePreparation(
                        normalizedRegion = normalizedRegion,
                        puuid = trimmedPuuid,
                        orderedMatchIds = orderedMatchIds,
                        matchDtos = matchDtos,
                        analyzerVersion = analyzerVersion,
                        cacheKeysToPopulate = listOf(cacheKey),
                        playerScopeKey = playerScopeKey,
                        deadlineAtMs = ownerDeadlineMs,
                        lingeringWorkers = lingeringWorkers
                    )
                }
            } finally {
                releaseGlobalSlotAfterWorkers(lingeringWorkers)
            }
        }
    }

    private suspend fun executeCoreTimelinePreparation(
        normalizedRegion: String,
        puuid: String,
        orderedMatchIds: List<String>,
        matchDtos: List<MatchDto>,
        analyzerVersion: String,
        cacheKeysToPopulate: List<String>,
        playerScopeKey: String,
        deadlineAtMs: Long,
        lingeringWorkers: MutableList<RiotApiService.LingeringWorkerHandle>,
        httpAttemptCounter: AtomicInteger? = null
    ): CoachTimelinePreparationResultDto {
        val matchDtosById = matchDtos.mapNotNull { dto ->
            val mid = dto.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
            mid to dto
        }.toMap()
        val orderedMatchDtos = orderedMatchIds.mapNotNull { matchDtosById[it] }

        val compactedTriples = mutableListOf<Triple<String, MatchTimelineDto, MatchDto>>()
        val coveredMatchIds = mutableListOf<String>()
        val missingMatches = mutableListOf<MissingMatchTimelineDto>()
        val liveRawTimelines = AtomicInteger(0)
        val peakRetainedRawTimelines = AtomicInteger(0)
        var stopReason: String? = null

        for ((index, matchId) in orderedMatchIds.withIndex()) {
            val nowMs = timeProvider.now()
            if (nowMs >= deadlineAtMs) {
                stopReason = "DEADLINE_EXCEEDED"
                for (remainingId in orderedMatchIds.subList(index, orderedMatchIds.size)) {
                    missingMatches.add(MissingMatchTimelineDto(remainingId, "DEADLINE_EXCEEDED"))
                }
                break
            }

            if (!hasCoachContinueHeadroom()) {
                stopReason = "MEMORY_HEADROOM_STOP"
                for (remainingId in orderedMatchIds.subList(index, orderedMatchIds.size)) {
                    missingMatches.add(MissingMatchTimelineDto(remainingId, "MEMORY_HEADROOM_STOP"))
                }
                break
            }

            val matchDto = matchDtosById[matchId]
            if (matchDto == null) {
                missingMatches.add(MissingMatchTimelineDto(matchId, "MISSING_MATCH_DTO"))
                continue
            }

            val rawCacheKey = getCacheKey(matchId, normalizedRegion)
            val cacheReadOutcome = readAndCompactCachedTimeline(
                rawCacheKey = rawCacheKey,
                matchDto = matchDto,
                puuid = puuid,
                deadlineAtMs = deadlineAtMs,
                liveRawTimelines = liveRawTimelines,
                peakRetainedRawTimelines = peakRetainedRawTimelines,
                lingeringWorkers = lingeringWorkers
            )
            if (cacheReadOutcome.abortStopReason != null) {
                stopReason = cacheReadOutcome.abortStopReason
                for (remainingId in orderedMatchIds.subList(index, orderedMatchIds.size)) {
                    missingMatches.add(MissingMatchTimelineDto(remainingId, cacheReadOutcome.abortStopReason))
                }
                break
            }

            var compactedTimeline: MatchTimelineDto? = cacheReadOutcome.compactedTimeline
            val needsNetworkFetch = !cacheReadOutcome.hadCachedRaw || cacheReadOutcome.legacyNeedsBackfill
            var itemMissingReason: String? = null
            var fetchStopReason: String? = null

            if (needsNetworkFetch) {
                val nowBeforeFetch = timeProvider.now()
                val remainingMs = deadlineAtMs - nowBeforeFetch
                val activeCooldownMs = rateLimiter?.getActiveRetryAfterWaitMs(normalizedRegion) ?: 0L

                when {
                    remainingMs <= 0L -> fetchStopReason = "DEADLINE_EXCEEDED"
                    !hasCoachContinueHeadroom() -> fetchStopReason = "MEMORY_HEADROOM_STOP"
                    activeCooldownMs > 0L && activeCooldownMs + 10L > remainingMs -> fetchStopReason = "RATE_LIMIT_COOLDOWN"
                    else -> {
                        val failureReason = AtomicReference<String?>(null)
                        val fetched = riotApiService.getMatchTimelineBounded(
                            matchId = matchId,
                            region = normalizedRegion,
                            priority = ApiPriority.LOW,
                            maxRetries = 0,
                            httpAttemptCounter = httpAttemptCounter,
                            failureReasonHolder = failureReason,
                            maxTotalDurationMs = remainingMs,
                            headroomGuard = ::hasCoachContinueHeadroom
                        )
                        if (fetched != null) {
                            if (timeProvider.now() >= deadlineAtMs) {
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
                                "HTTP_EXECUTOR_SATURATED" -> fetchStopReason = "HTTP_EXECUTOR_SATURATED"
                                "UPSTREAM_5XX", "HTTP_ERROR" -> fetchStopReason = "UPSTREAM_ERROR"
                                "HTTP_404_NOT_FOUND" -> {
                                    if (compactedTimeline == null) {
                                        itemMissingReason = "TIMELINE_NOT_FOUND"
                                    }
                                }
                                "RESPONSE_TOO_LARGE" -> {
                                    if (compactedTimeline == null) {
                                        itemMissingReason = "OVERSIZED_TIMELINE"
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
                itemMissingReason = cacheReadOutcome.invalidItemReason ?: "TIMELINE_UNAVAILABLE"
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

        // Strictly cache ONLY COMPLETE results so transient/partial states are always immediately retryable
        if (status == "COMPLETE") {
            putCachedPreparation(cacheKeysToPopulate, playerScopeKey, finalResult)
        }

        return finalResult
    }

    internal fun isNonTransientPerMatchMissingStopReason(reason: String?): Boolean {
        return reason == "TIMELINE_NOT_FOUND" || reason == "MISSING_MATCH_DTO"
    }

    private data class CachedTimelineCompactionOutcome(
        val hadCachedRaw: Boolean,
        val legacyNeedsBackfill: Boolean,
        val compactedTimeline: MatchTimelineDto?,
        val abortStopReason: String? = null,
        val invalidItemReason: String? = null
    )

    private suspend fun readAndCompactCachedTimeline(
        rawCacheKey: String,
        matchDto: MatchDto,
        puuid: String,
        deadlineAtMs: Long,
        liveRawTimelines: AtomicInteger,
        peakRetainedRawTimelines: AtomicInteger,
        lingeringWorkers: MutableList<RiotApiService.LingeringWorkerHandle>
    ): CachedTimelineCompactionOutcome {
        val cacheFailure = AtomicReference<String?>(null)
        val rawTimeline = runBoundedCacheReadWithDeadline(
            maxInflatedBytes = MAX_RAW_TIMELINE_CACHE_INFLATE_BYTES,
            deadlineAtMs = deadlineAtMs,
            headroomGuard = ::hasCoachContinueHeadroom,
            failureReasonHolder = cacheFailure,
            lingeringWorkers = lingeringWorkers
        ) {
            redisCacheService.get(rawCacheKey, MatchTimelineDto::class.java)
        }

        val abortReason = cacheFailure.get()
        if (abortReason != null) {
            val mappedStop = when (abortReason) {
                "DEADLINE_EXCEEDED" -> "DEADLINE_EXCEEDED"
                "MEMORY_HEADROOM_STOP" -> "MEMORY_HEADROOM_STOP"
                "HTTP_EXECUTOR_SATURATED" -> "HTTP_EXECUTOR_SATURATED"
                else -> null
            }
            if (mappedStop != null) {
                return CachedTimelineCompactionOutcome(
                    hadCachedRaw = false,
                    legacyNeedsBackfill = false,
                    compactedTimeline = null,
                    abortStopReason = mappedStop
                )
            }
            val perItemInvalidReason = if (abortReason == "RESPONSE_TOO_LARGE") {
                "OVERSIZED_TIMELINE"
            } else {
                "TIMELINE_UNAVAILABLE"
            }
            return CachedTimelineCompactionOutcome(
                hadCachedRaw = true,
                legacyNeedsBackfill = false,
                compactedTimeline = null,
                invalidItemReason = perItemInvalidReason
            )
        }

        if (rawTimeline == null) {
            return CachedTimelineCompactionOutcome(
                hadCachedRaw = false,
                legacyNeedsBackfill = false,
                compactedTimeline = null
            )
        }

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
            if (compacted != null && hasCoachContinueHeadroom()) {
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

    private fun releaseGlobalSlotAfterWorkers(lingeringWorkers: List<RiotApiService.LingeringWorkerHandle>) {
        val snapshot = synchronized(lingeringWorkers) { lingeringWorkers.toList() }
            .filter { it.isStillRunning() }
        if (snapshot.isEmpty()) {
            activeGlobalPreparations.decrementAndGet()
        } else {
            val safePollMs = lingeringWorkerPollMs.coerceIn(10L, 30_000L)
            singleflightScope.launch(kotlinx.coroutines.NonCancellable + Dispatchers.IO) {
                try {
                    for (handle in snapshot) {
                        while (handle.isStillRunning()) {
                            try {
                                handle.doneLatch.await(safePollMs, TimeUnit.MILLISECONDS)
                            } catch (_: InterruptedException) {
                                // Never release global concurrency slot while worker thread is actually alive
                            }
                        }
                    }
                } finally {
                    activeGlobalPreparations.decrementAndGet()
                }
            }
        }
    }

    sealed class SingleTimelineOutcome {
        data class Success(val timeline: MatchTimelineDto?) : SingleTimelineOutcome()
        data class Blocked(val reason: String) : SingleTimelineOutcome()
    }

    /**
     * Controller-facing single-timeline fetch for `GET /api/riot/timeline/{region}/{matchId}`:
     * enforces 350 MiB admission headroom, global 2-slot concurrency cap, 4 MiB Redis/HTTP
     * inflate bounds, 360 MiB mid-stream continue headroom, and deadline/429/upstream error classification
     * while preserving `Success(null)` when Riot legitimately returns 404 (not found).
     */
    suspend fun getTimelineForController(matchId: String, region: String): SingleTimelineOutcome {
        val normalizedRegion = region.trim().lowercase()
        val trimmedMatchId = matchId.trim()
        if (normalizedRegion.isEmpty() || trimmedMatchId.isEmpty()) {
            return SingleTimelineOutcome.Success(null)
        }
        val preAdmission = evaluatePreAdmissionHeadroom()
        if (!preAdmission.admitted) {
            val preFormatted = formatPayloadFreeMemorySample(preAdmission.initialSample)
            val postGcFormatted = preAdmission.postGcSample?.let(::formatPayloadFreeMemorySample) ?: "none"
            logger.warn(
                "singleTimelineFetch summary region=$normalizedRegion status=BLOCKED stopReason=MEMORY_HEADROOM_ADMISSION_BLOCKED " +
                    "stage=pre_admission gcEnabled=$preAdmissionGcEnabled gcAttempted=${preAdmission.gcAttempted} " +
                    "gcStatus=${preAdmission.gcStatus} gcMs=${preAdmission.gcDurationMs} memPre={$preFormatted} memPostGc={$postGcFormatted}"
            )
            return SingleTimelineOutcome.Blocked("MEMORY_HEADROOM_ADMISSION_BLOCKED")
        }
        val safeMaxConcurrent = maxConcurrentPreparations.coerceAtLeast(1)
        if (!tryAcquireGlobalSlot(safeMaxConcurrent)) {
            return SingleTimelineOutcome.Blocked("CONCURRENCY_LIMIT_REACHED")
        }

        val lingeringWorkers = Collections.synchronizedList(mutableListOf<RiotApiService.LingeringWorkerHandle>())
        try {
            if (!hasCoachAdmissionHeadroom()) {
                return SingleTimelineOutcome.Blocked("MEMORY_HEADROOM_ADMISSION_BLOCKED")
            }

            val deadlineAtMs = timeProvider.now() + prepDeadlineMs.coerceAtLeast(1_000L)
            val cacheKey = getCacheKey(trimmedMatchId, normalizedRegion)
            val cacheFailure = AtomicReference<String?>(null)

            val cached = runBoundedCacheReadWithDeadline(
                maxInflatedBytes = MAX_RAW_TIMELINE_CACHE_INFLATE_BYTES,
                deadlineAtMs = deadlineAtMs,
                headroomGuard = ::hasCoachContinueHeadroom,
                failureReasonHolder = cacheFailure,
                lingeringWorkers = lingeringWorkers
            ) {
                redisCacheService.get(cacheKey, MatchTimelineDto::class.java)
            }

            val cacheAbort = cacheFailure.get()
            if (cacheAbort != null) {
                val mapped = when (cacheAbort) {
                    "DEADLINE_EXCEEDED" -> "DEADLINE_EXCEEDED"
                    "MEMORY_HEADROOM_STOP" -> "MEMORY_HEADROOM_STOP"
                    "HTTP_EXECUTOR_SATURATED" -> "HTTP_EXECUTOR_SATURATED"
                    else -> "TIMELINE_UNAVAILABLE"
                }
                return SingleTimelineOutcome.Blocked(mapped)
            }
            if (cached != null && !needsTimelineBackfill(cached)) {
                logger.info("Timeline Cache HIT: {}", cacheKey)
                return SingleTimelineOutcome.Success(cached)
            }

            val remainingMs = deadlineAtMs - timeProvider.now()
            if (remainingMs <= 0L) {
                return if (cached != null) SingleTimelineOutcome.Success(cached) else SingleTimelineOutcome.Blocked("DEADLINE_EXCEEDED")
            }
            if (!hasCoachContinueHeadroom()) {
                return if (cached != null) SingleTimelineOutcome.Success(cached) else SingleTimelineOutcome.Blocked("MEMORY_HEADROOM_STOP")
            }

            val activeCooldownMs = rateLimiter?.getActiveRetryAfterWaitMs(normalizedRegion) ?: 0L
            if (activeCooldownMs > 0L && activeCooldownMs + 10L > remainingMs) {
                return if (cached != null) SingleTimelineOutcome.Success(cached) else SingleTimelineOutcome.Blocked("RATE_LIMIT_COOLDOWN")
            }

            val failureReason = AtomicReference<String?>(null)
            val fetched = RiotApiService.withLingeringWorkerHook({ handle -> lingeringWorkers.add(handle) }) {
                riotApiService.getMatchTimelineBounded(
                    matchId = trimmedMatchId,
                    region = normalizedRegion,
                    priority = ApiPriority.LOW,
                    maxRetries = 0,
                    failureReasonHolder = failureReason,
                    maxTotalDurationMs = remainingMs,
                    headroomGuard = ::hasCoachContinueHeadroom
                )
            }

            if (fetched != null) {
                if (!hasCoachContinueHeadroom()) {
                    return if (cached != null) SingleTimelineOutcome.Success(cached) else SingleTimelineOutcome.Blocked("MEMORY_HEADROOM_STOP")
                }
                redisCacheService.set(cacheKey, fetched, Duration.ofHours(24))
                return SingleTimelineOutcome.Success(fetched)
            }

            if (cached != null) {
                return SingleTimelineOutcome.Success(cached)
            }

            return when (failureReason.get()) {
                null, "HTTP_404_NOT_FOUND", "RESPONSE_TOO_LARGE", "PARSE_ERROR" -> SingleTimelineOutcome.Success(null)
                "HTTP_429_TOO_MANY_REQUESTS" -> SingleTimelineOutcome.Blocked("RATE_LIMIT_429")
                "RATE_LIMIT_COOLDOWN" -> SingleTimelineOutcome.Blocked("RATE_LIMIT_COOLDOWN")
                "DEADLINE_EXCEEDED" -> SingleTimelineOutcome.Blocked("DEADLINE_EXCEEDED")
                "MEMORY_HEADROOM_STOP" -> SingleTimelineOutcome.Blocked("MEMORY_HEADROOM_STOP")
                "NETWORK_TIMEOUT" -> SingleTimelineOutcome.Blocked("NETWORK_TIMEOUT")
                "HTTP_EXECUTOR_SATURATED" -> SingleTimelineOutcome.Blocked("HTTP_EXECUTOR_SATURATED")
                else -> SingleTimelineOutcome.Blocked("UPSTREAM_ERROR")
            }
        } finally {
            releaseGlobalSlotAfterWorkers(lingeringWorkers)
        }
    }

    /**
     * Fetches a single match timeline, using Redis cache with 24h TTL and enforcing
     * 350/360 MiB dual memory headroom + global concurrency slot + bounded Redis/HTTP inflate guards.
     * Lazily backfills cached timelines that predate CHAMPION_KILL bounty/shutdownBounty fields.
     */
    suspend fun getTimeline(matchId: String, region: String): MatchTimelineDto? {
        val normalizedRegion = region.trim().lowercase()
        val trimmedMatchId = matchId.trim()
        if (normalizedRegion.isEmpty() || trimmedMatchId.isEmpty()) return null
        val preAdmission = evaluatePreAdmissionHeadroom()
        if (!preAdmission.admitted) {
            val preFormatted = formatPayloadFreeMemorySample(preAdmission.initialSample)
            val postGcFormatted = preAdmission.postGcSample?.let(::formatPayloadFreeMemorySample) ?: "none"
            logger.warn(
                "singleTimelineFetch summary region=$normalizedRegion status=BLOCKED stopReason=MEMORY_HEADROOM_ADMISSION_BLOCKED " +
                    "stage=pre_admission gcEnabled=$preAdmissionGcEnabled gcAttempted=${preAdmission.gcAttempted} " +
                    "gcStatus=${preAdmission.gcStatus} gcMs=${preAdmission.gcDurationMs} memPre={$preFormatted} memPostGc={$postGcFormatted}"
            )
            return null
        }
        val safeMaxConcurrent = maxConcurrentPreparations.coerceAtLeast(1)
        if (!tryAcquireGlobalSlot(safeMaxConcurrent)) {
            logger.warn("Single timeline fetch blocked at concurrency limit for matchId={} region={}", trimmedMatchId, normalizedRegion)
            return null
        }

        val lingeringWorkers = Collections.synchronizedList(mutableListOf<RiotApiService.LingeringWorkerHandle>())
        try {
            if (!hasCoachAdmissionHeadroom()) {
                return null
            }

            val deadlineAtMs = timeProvider.now() + prepDeadlineMs.coerceAtLeast(1_000L)
            val cacheKey = getCacheKey(trimmedMatchId, normalizedRegion)
            val cacheFailure = AtomicReference<String?>(null)

            val cached = runBoundedCacheReadWithDeadline(
                maxInflatedBytes = MAX_RAW_TIMELINE_CACHE_INFLATE_BYTES,
                deadlineAtMs = deadlineAtMs,
                headroomGuard = ::hasCoachContinueHeadroom,
                failureReasonHolder = cacheFailure,
                lingeringWorkers = lingeringWorkers
            ) {
                redisCacheService.get(cacheKey, MatchTimelineDto::class.java)
            }

            if (cacheFailure.get() != null) {
                return null
            }
            if (cached != null && !needsTimelineBackfill(cached)) {
                logger.info("Timeline Cache HIT: {}", cacheKey)
                return cached
            }

            if (!hasCoachContinueHeadroom() || timeProvider.now() >= deadlineAtMs) {
                return cached
            }

            if (cached != null) {
                logger.info("Timeline Cache LEGACY (missing bounty fields): {} — lazily backfilling from Riot API", cacheKey)
            } else {
                logger.info("Timeline Cache MISS: {} — fetching from Riot API", cacheKey)
            }

            val timeline = RiotApiService.withLingeringWorkerHook({ handle -> lingeringWorkers.add(handle) }) {
                riotApiService.getMatchTimeline(trimmedMatchId, normalizedRegion)
            }
            if (timeline != null && hasCoachContinueHeadroom()) {
                redisCacheService.set(cacheKey, timeline, Duration.ofHours(24))
                return timeline
            }

            return cached
        } finally {
            releaseGlobalSlotAfterWorkers(lingeringWorkers)
        }
    }

    private fun needsTimelineBackfill(timeline: MatchTimelineDto): Boolean {
        val frames = timeline.info?.frames?.takeIf { it.isNotEmpty() } ?: return false
        val killEvents = frames.flatMap { it.events ?: emptyList() }.filter { it.type == "CHAMPION_KILL" }
        return killEvents.isNotEmpty() && killEvents.all { it.bounty == null && it.shutdownBounty == null }
    }
}
