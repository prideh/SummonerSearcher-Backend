package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.dto.CohortBenchmarkCellDto
import com.pride.summoner_searcher_api.dto.CohortCollectorCheckpointDto
import com.pride.summoner_searcher_api.dto.CohortCollectorStatusDto
import com.pride.summoner_searcher_api.dto.CohortRateLimitProbeResultDto
import com.pride.summoner_searcher_api.dto.RateLimitVerificationSnapshotDto
import com.pride.summoner_searcher_api.service.CohortBenchmarkService
import com.pride.summoner_searcher_api.service.SystemTimeProvider
import com.pride.summoner_searcher_api.service.TimeProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Same-process, guarded, resumable LOW-priority cohort collector coordinator.
 *
 * Runs inside the SAME backend JVM process that serves foreground HTTP requests and runs
 * [ChallengerCacheWarmer], so it shares the live in-memory [com.pride.summoner_searcher_api.service.RiotApiRateLimiter]:
 * - Observes real `X-App-Rate-Limit`, `X-Method-Rate-Limit`, and `X-Rate-Limit-Type` headers populated by
 *   same-process traffic (avoiding separate-JVM empty-limiter bootstrap deadlocks).
 * - Observes live process-local `activeUserRequests` and `foregroundYieldActive` counters in real time.
 * - Keeps network collection OFF by default (`cohort.network-collection.enabled=false`) until actual
 *   App + Method headers and headroom are verified.
 * - Never promises guaranteed morning coverage; zero paid AI or paid API calls.
 */
@Component
class OvernightCohortCollectorRunner(
    private val cohortBenchmarkService: CohortBenchmarkService,
    @Value("\${cohort.collector.operator-token:}")
    private val configuredOperatorToken: String = "",
    private val timeProvider: TimeProvider = SystemTimeProvider()
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val running = AtomicBoolean(false)
    private val probeRunning = AtomicBoolean(false)
    private val stopRequested = AtomicBoolean(false)
    private val overnightSessionArmed = AtomicBoolean(false)

    @Volatile
    private var activeRegion: String = "euw1"

    @Volatile
    private var activeTargetPatch: String? = null

    @Volatile
    private var activeBatchIndex: Int = 0

    @Volatile
    private var productiveBatchesCompleted: Int = 0

    @Volatile
    private var pauseCyclesCount: Int = 0

    @Volatile
    private var consecutivePauseCycles: Int = 0

    @Volatile
    private var maxBatchesConfigured: Int = DEFAULT_MAX_BATCHES_PER_CHUNK

    @Volatile
    private var maxPauseCyclesConfigured: Int = 240

    @Volatile
    private var batchSizeConfigured: Int = DEFAULT_BATCH_SIZE

    @Volatile
    private var pauseDelayMsConfigured: Long = 2_500L

    @Volatile
    private var sessionProductiveBatchesCompleted: Int = 0

    @Volatile
    private var maxSessionProductiveBatchesConfigured: Int = DEFAULT_MAX_SESSION_PRODUCTIVE_BATCHES

    @Volatile
    private var sessionBaselineNetworkCalls: Int = 0

    @Volatile
    private var sessionNetworkCallsMade: Int = 0

    @Volatile
    private var maxSessionNetworkCallsConfigured: Int = DEFAULT_MAX_SESSION_NETWORK_CALLS

    @Volatile
    private var lastRunStartedAt: Instant? = null

    @Volatile
    private var lastRunCompletedAt: Instant? = null

    companion object {
        const val DEFAULT_MAX_BATCHES_PER_CHUNK = 2
        const val DEFAULT_BATCH_SIZE = 5
        const val DEFAULT_MAX_SESSION_PRODUCTIVE_BATCHES = 4
        const val DEFAULT_MAX_SESSION_NETWORK_CALLS = 60
        const val STOP_REASON_SESSION_BATCH_CAP = "SESSION_BATCH_CAP_REACHED"
        const val STOP_REASON_SESSION_NETWORK_CALL_CAP = "SESSION_NETWORK_CALL_CAP_REACHED"
        const val STOP_REASON_SESSION_ERROR_ABORT = "SESSION_ERROR_ABORT"

        private val LOOPBACK_ADDRESSES = setOf(
            "127.0.0.1",
            "::1",
            "0:0:0:0:0:0:0:1",
            "localhost"
        )
        private val TERMINAL_STOP_REASONS = setOf(
            "ALL_TARGET_CELLS_SATISFIED",
            "NO_CANDIDATES_REMAINING",
            "NETWORK_COLLECTION_DISABLED_BY_CONFIG",
            "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY",
            "STOPPED_BY_OPERATOR",
            STOP_REASON_SESSION_BATCH_CAP,
            STOP_REASON_SESSION_NETWORK_CALL_CAP,
            STOP_REASON_SESSION_ERROR_ABORT
        )
    }

    override fun run(args: ApplicationArguments?) {
        if (args == null || !args.containsOption("collect-cohorts")) {
            return
        }

        val region = args.getOptionValues("collect-region")?.firstOrNull()?.lowercase()?.trim() ?: "euw1"
        val targetPatch = args.getOptionValues("collect-patch")?.firstOrNull()?.trim()?.takeIf { it.isNotBlank() }
        val maxBatches = args.getOptionValues("collect-max-batches")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 500) ?: DEFAULT_MAX_BATCHES_PER_CHUNK
        val batchSize = args.getOptionValues("collect-batch-size")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 200) ?: DEFAULT_BATCH_SIZE
        val pauseDelayMs = args.getOptionValues("collect-pause-ms")?.firstOrNull()?.toLongOrNull()?.coerceIn(0L, 60_000L) ?: 2_500L
        val maxPauseCycles = args.getOptionValues("collect-max-pause-cycles")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 10_000) ?: 240
        val maxTotalProductiveBatches = args.getOptionValues("collect-max-session-batches")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 500) ?: DEFAULT_MAX_SESSION_PRODUCTIVE_BATCHES
        val maxTotalNetworkCalls = args.getOptionValues("collect-max-session-network-calls")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 5_000) ?: DEFAULT_MAX_SESSION_NETWORK_CALLS

        // Start asynchronously in the SAME process so the web server and ChallengerCacheWarmer run normally
        startOrResumeGuarded(
            region = region,
            targetPatch = targetPatch,
            maxBatches = maxBatches,
            batchSize = batchSize,
            pauseDelayMs = pauseDelayMs,
            maxPauseCycles = maxPauseCycles,
            maxTotalProductiveBatches = maxTotalProductiveBatches,
            maxTotalNetworkCalls = maxTotalNetworkCalls,
            async = true
        )
    }

    /**
     * Authorizes same-process collector control/status requests:
     * - If `cohort.collector.operator-token` (`COHORT_OPERATOR_TOKEN`) is configured, requires a constant-time
     *   matching `X-Cohort-Operator-Token` header.
     * - Otherwise, permits ONLY direct unproxied loopback requests (`127.0.0.1` / `::1` with no
     *   `X-Forwarded-For`, `Forwarded`, or `X-Real-IP` headers).
     */
    fun isAuthorizedOperatorRequest(
        remoteAddr: String?,
        forwardedFor: String? = null,
        forwarded: String? = null,
        realIp: String? = null,
        providedToken: String? = null
    ): Boolean {
        val expectedToken = configuredOperatorToken.trim()
        if (expectedToken.isNotBlank()) {
            val candidate = providedToken?.trim() ?: return false
            return MessageDigest.isEqual(
                expectedToken.toByteArray(Charsets.UTF_8),
                candidate.toByteArray(Charsets.UTF_8)
            )
        }
        if (!forwardedFor.isNullOrBlank() || !forwarded.isNullOrBlank() || !realIp.isNullOrBlank()) {
            return false
        }
        val normalizedRemote = remoteAddr?.trim()?.lowercase() ?: return false
        return normalizedRemote in LOOPBACK_ADDRESSES
    }

    /**
     * Guarded same-process start/resume entrypoint.
     * Ensures at most one collector job runs at a time (in-process [AtomicBoolean] + Redis lock)
     * inside the same backend JVM sharing [com.pride.summoner_searcher_api.service.RiotApiRateLimiter].
     *
     * Initializes session-level hard caps ([maxTotalProductiveBatches] and [maxTotalNetworkCalls])
     * that periodic auto-resume ticks ([scheduledAutoResumeTick]) NEVER reset.
     */
    fun startOrResumeGuarded(
        region: String = "euw1",
        targetPatch: String? = null,
        maxBatches: Int = DEFAULT_MAX_BATCHES_PER_CHUNK,
        batchSize: Int = DEFAULT_BATCH_SIZE,
        pauseDelayMs: Long = 2_500L,
        maxPauseCycles: Int = 240,
        maxTotalProductiveBatches: Int = DEFAULT_MAX_SESSION_PRODUCTIVE_BATCHES,
        maxTotalNetworkCalls: Int = DEFAULT_MAX_SESSION_NETWORK_CALLS,
        async: Boolean = true
    ): CohortCollectorStatusDto {
        return startOrResumeInternal(
            region = region,
            targetPatch = targetPatch,
            maxBatches = maxBatches,
            batchSize = batchSize,
            pauseDelayMs = pauseDelayMs,
            maxPauseCycles = maxPauseCycles,
            maxTotalProductiveBatches = maxTotalProductiveBatches,
            maxTotalNetworkCalls = maxTotalNetworkCalls,
            async = async,
            isAutoResume = false
        )
    }

    private fun startOrResumeInternal(
        region: String,
        targetPatch: String?,
        maxBatches: Int,
        batchSize: Int,
        pauseDelayMs: Long,
        maxPauseCycles: Int,
        maxTotalProductiveBatches: Int,
        maxTotalNetworkCalls: Int,
        async: Boolean,
        isAutoResume: Boolean
    ): CohortCollectorStatusDto {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }

        if (!cohortBenchmarkService.isValidRegion(normalizedRegion)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Unsupported region '$normalizedRegion'.")
        }
        if (!cohortBenchmarkService.isValidPatchOrNull(cleanPatch)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Invalid patch format '$cleanPatch' (expected major.minor).")
        }

        val coercedMaxBatches = maxBatches.coerceIn(1, 500)
        val coercedBatchSize = batchSize.coerceIn(1, 200)
        val coercedPauseMs = pauseDelayMs.coerceIn(0L, 60_000L)
        val coercedMaxPauseCycles = maxPauseCycles.coerceIn(1, 10_000)
        val coercedMaxSessionBatches = maxTotalProductiveBatches.coerceIn(1, 500)
        val coercedMaxSessionNetworkCalls = maxTotalNetworkCalls.coerceIn(1, 5_000)

        if (!running.compareAndSet(false, true)) {
            return getStatus(
                region = normalizedRegion,
                targetPatch = cleanPatch,
                action = "ALREADY_RUNNING_IN_PROCESS",
                customMessage = "Collector is already running in this backend JVM process; returning live status."
            )
        }

        val acquiredLock = try {
            cohortBenchmarkService.acquireCollectorProcessLock(normalizedRegion)
        } catch (e: Exception) {
            running.set(false)
            throw e
        }
        if (!acquiredLock) {
            running.set(false)
            return getStatus(
                region = normalizedRegion,
                targetPatch = cleanPatch,
                action = "LOCKED_BY_ANOTHER_INSTANCE",
                customMessage = "Collector lock is held in Redis for region '$normalizedRegion'; refusing duplicate run."
            )
        }

        val existingCheckpoint = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch)
        stopRequested.set(false)
        overnightSessionArmed.set(true)
        activeRegion = normalizedRegion
        activeTargetPatch = cleanPatch ?: existingCheckpoint?.targetPatch ?: activeTargetPatch
        activeBatchIndex = 0
        productiveBatchesCompleted = 0
        pauseCyclesCount = 0
        consecutivePauseCycles = 0
        maxBatchesConfigured = coercedMaxBatches
        maxPauseCyclesConfigured = coercedMaxPauseCycles
        batchSizeConfigured = coercedBatchSize
        pauseDelayMsConfigured = coercedPauseMs
        if (!isAutoResume) {
            sessionProductiveBatchesCompleted = 0
            maxSessionProductiveBatchesConfigured = coercedMaxSessionBatches
            sessionBaselineNetworkCalls = existingCheckpoint?.totalNetworkCallsMade ?: 0
            sessionNetworkCallsMade = 0
            maxSessionNetworkCallsConfigured = coercedMaxSessionNetworkCalls
        } else if (existingCheckpoint != null) {
            sessionNetworkCallsMade = (existingCheckpoint.totalNetworkCallsMade - sessionBaselineNetworkCalls).coerceAtLeast(0)
        }
        lastRunStartedAt = Instant.ofEpochMilli(timeProvider.now())

        val remainingSessionBatches = (maxSessionProductiveBatchesConfigured - sessionProductiveBatchesCompleted).coerceAtLeast(0)
        val effectiveChunkMaxBatches = minOf(coercedMaxBatches, remainingSessionBatches)

        if (async) {
            scope.launch {
                try {
                    executeResumableRun(
                        region = normalizedRegion,
                        targetPatch = cleanPatch,
                        maxBatches = effectiveChunkMaxBatches,
                        batchSize = coercedBatchSize,
                        pauseDelayMs = coercedPauseMs,
                        maxPauseCycles = coercedMaxPauseCycles
                    )
                } catch (e: Exception) {
                    logger.error("[OvernightCollector] Error during same-process background collector run for {}", normalizedRegion, e)
                } finally {
                    lastRunCompletedAt = Instant.ofEpochMilli(timeProvider.now())
                    cohortBenchmarkService.releaseCollectorProcessLock(normalizedRegion)
                    running.set(false)
                }
            }
            return getStatus(
                region = normalizedRegion,
                targetPatch = cleanPatch,
                action = "STARTED_OR_RESUMED_IN_PROCESS",
                customMessage = "Started/resumed guarded LOW-priority collector in the same backend JVM process."
            )
        } else {
            val finalCheckpoint = try {
                runBlocking {
                    executeResumableRun(
                        region = normalizedRegion,
                        targetPatch = cleanPatch,
                        maxBatches = effectiveChunkMaxBatches,
                        batchSize = coercedBatchSize,
                        pauseDelayMs = coercedPauseMs,
                        maxPauseCycles = coercedMaxPauseCycles
                    )
                }
            } finally {
                lastRunCompletedAt = Instant.ofEpochMilli(timeProvider.now())
                cohortBenchmarkService.releaseCollectorProcessLock(normalizedRegion)
                running.set(false)
            }
            return getStatus(
                region = normalizedRegion,
                targetPatch = cleanPatch ?: finalCheckpoint?.targetPatch,
                action = "COMPLETED_SYNCHRONOUS_RUN",
                customMessage = "Completed synchronous guarded collector run in same process (stopReason=${finalCheckpoint?.lastStopReason ?: "NONE"}, productiveBatches=$productiveBatchesCompleted/$coercedMaxBatches, sessionBatches=$sessionProductiveBatchesCompleted/$maxSessionProductiveBatchesConfigured, sessionNetworkCalls=$sessionNetworkCallsMade/$maxSessionNetworkCallsConfigured, pauseCycles=$pauseCyclesCount)."
            )
        }
    }

    /**
     * Periodic same-process auto-resume tick for armed overnight sessions.
     * If an overnight run paused for a long rate-limit window (`PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE`)
     * or finished a batch chunk (`BATCH_BUDGET_REACHED_RESUMABLE`) while target cells and candidates remain
     * AND session-level hard caps ([maxSessionProductiveBatchesConfigured], [maxSessionNetworkCallsConfigured])
     * have not been reached, automatically resumes from the checkpoint once `readyForLowPriorityNetworkCollection` is true.
     */
    @org.springframework.scheduling.annotation.Scheduled(
        fixedDelayString = "\${cohort.collector.auto-resume-interval-ms:30000}",
        initialDelayString = "\${cohort.collector.auto-resume-initial-delay-ms:30000}"
    )
    fun scheduledAutoResumeTick() {
        checkAndAutoResumeOvernightSession(async = true)
    }

    fun checkAndAutoResumeOvernightSession(async: Boolean = true): CohortCollectorStatusDto? {
        if (!overnightSessionArmed.get() || running.get() || stopRequested.get()) {
            return null
        }
        val region = activeRegion
        val currentCheckpoint = cohortBenchmarkService.getCollectorCheckpoint(region, activeTargetPatch)
        if (currentCheckpoint != null) {
            sessionNetworkCallsMade = (currentCheckpoint.totalNetworkCallsMade - sessionBaselineNetworkCalls).coerceAtLeast(0)
            if (currentCheckpoint.lastStopReason in TERMINAL_STOP_REASONS) {
                overnightSessionArmed.set(false)
                logger.info(
                    "[OvernightCollector] Disarming auto-resume for region={} patch={}: checkpoint has terminal stopReason={}.",
                    region, activeTargetPatch ?: "auto-cached", currentCheckpoint.lastStopReason
                )
                return null
            }
        }
        if (sessionProductiveBatchesCompleted >= maxSessionProductiveBatchesConfigured ||
            sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured
        ) {
            overnightSessionArmed.set(false)
            val capReason = if (sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured) {
                STOP_REASON_SESSION_NETWORK_CALL_CAP
            } else {
                STOP_REASON_SESSION_BATCH_CAP
            }
            if (currentCheckpoint != null && currentCheckpoint.lastStopReason != capReason) {
                cohortBenchmarkService.saveCollectorCheckpoint(
                    currentCheckpoint.copy(
                        sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                        maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade = sessionNetworkCallsMade,
                        maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                        lastStopReason = capReason,
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                )
            }
            logger.info(
                "[OvernightCollector] Disarming auto-resume for region={} patch={}: session hard cap reached ({}, sessionBatches={}/{}, sessionNetworkCalls={}/{}).",
                region, activeTargetPatch ?: "auto-cached", capReason,
                sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                sessionNetworkCallsMade, maxSessionNetworkCallsConfigured
            )
            return null
        }
        val liveSnap = cohortBenchmarkService.getLiveRateLimitVerification(region)
        if (!liveSnap.readyForLowPriorityNetworkCollection) {
            return null
        }
        val memSnap = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = true)
        if (!memSnap.hasHeadroomForLowPriority) {
            return null
        }
        logger.info(
            "[OvernightCollector] Auto-resuming armed overnight session for region={} patch={} after cooldown/memory/batch window (sessionBatches={}/{}, sessionNetworkCalls={}/{}).",
            region, activeTargetPatch ?: "auto-cached",
            sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
            sessionNetworkCallsMade, maxSessionNetworkCallsConfigured
        )
        return startOrResumeInternal(
            region = region,
            targetPatch = activeTargetPatch,
            maxBatches = maxBatchesConfigured,
            batchSize = batchSizeConfigured,
            pauseDelayMs = pauseDelayMsConfigured,
            maxPauseCycles = maxPauseCyclesConfigured,
            maxTotalProductiveBatches = maxSessionProductiveBatchesConfigured,
            maxTotalNetworkCalls = maxSessionNetworkCallsConfigured,
            async = async,
            isAutoResume = true
        )
    }

    /**
     * Signals the in-process collector to stop cleanly at the next call/batch boundary and persists checkpoint state.
     */
    fun requestStop(
        region: String = "euw1",
        targetPatch: String? = null
    ): CohortCollectorStatusDto {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }

        if (!cohortBenchmarkService.isValidRegion(normalizedRegion)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Unsupported region '$normalizedRegion'.")
        }
        if (!cohortBenchmarkService.isValidPatchOrNull(cleanPatch)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Invalid patch format '$cleanPatch' (expected major.minor).")
        }

        stopRequested.set(true)
        overnightSessionArmed.set(false)
        val isCurrentlyRunning = running.get()
        if (!isCurrentlyRunning) {
            val existing = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch)
            if (existing != null && existing.lastStopReason != "STOPPED_BY_OPERATOR") {
                cohortBenchmarkService.saveCollectorCheckpoint(
                    existing.copy(
                        lastStopReason = "STOPPED_BY_OPERATOR",
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                )
            }
        }

        return getStatus(
            region = normalizedRegion,
            targetPatch = cleanPatch,
            action = if (isCurrentlyRunning) "STOP_REQUESTED_IN_PROCESS" else "STOPPED",
            customMessage = if (isCurrentlyRunning) {
                "Stop requested; collector will halt at the next safe call boundary, disarm auto-resume, and checkpoint."
            } else {
                "Collector is not currently running; auto-resume disarmed and checkpoint marked STOPPED_BY_OPERATOR."
            }
        )
    }

    /**
     * Bounded, read-only status inspection from the same backend JVM process.
     * Makes zero Riot API calls and zero Redis SCANs (reads only the single checkpoint key + in-memory limiter & memory snapshots).
     */
    fun getStatus(
        region: String = "euw1",
        targetPatch: String? = null,
        action: String = "STATUS",
        customMessage: String? = null
    ): CohortCollectorStatusDto {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }

        if (!cohortBenchmarkService.isValidRegion(normalizedRegion)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Unsupported region '$normalizedRegion'.")
        }
        if (!cohortBenchmarkService.isValidPatchOrNull(cleanPatch)) {
            return buildInvalidParamStatus(normalizedRegion, cleanPatch, "Invalid patch format '$cleanPatch' (expected major.minor).")
        }

        val verification = cohortBenchmarkService.getLiveRateLimitVerification(normalizedRegion)
        val memoryHeadroom = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)
        val checkpoint = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch)
            ?: if (cleanPatch == null && activeTargetPatch != null) {
                cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, activeTargetPatch)
            } else {
                null
            }
        val effectivePatch = cleanPatch ?: checkpoint?.targetPatch ?: activeTargetPatch
        val isRunning = running.get() && activeRegion == normalizedRegion
        val isArmed = overnightSessionArmed.get() && activeRegion == normalizedRegion
        val defaultMsg = when {
            isRunning -> "Collector is running in-process (productiveBatches=$productiveBatchesCompleted/$maxBatchesConfigured, sessionBatches=$sessionProductiveBatchesCompleted/$maxSessionProductiveBatchesConfigured, sessionNetworkCalls=$sessionNetworkCallsMade/$maxSessionNetworkCallsConfigured, pauseCycles=$pauseCyclesCount)."
            isArmed && checkpoint != null -> "Collector paused in-process awaiting cooldown auto-resume (lastStopReason=${checkpoint.lastStopReason}, sessionBatches=$sessionProductiveBatchesCompleted/$maxSessionProductiveBatchesConfigured, sessionNetworkCalls=$sessionNetworkCallsMade/$maxSessionNetworkCallsConfigured, cellsPassing=${checkpoint.cellsPassingQualityGate}/${checkpoint.targetCellsCount})."
            checkpoint != null -> "Collector idle in-process (lastStopReason=${checkpoint.lastStopReason}, sessionBatches=$sessionProductiveBatchesCompleted/$maxSessionProductiveBatchesConfigured, sessionNetworkCalls=$sessionNetworkCallsMade/$maxSessionNetworkCallsConfigured, cellsPassing=${checkpoint.cellsPassingQualityGate}/${checkpoint.targetCellsCount})."
            else -> "Collector idle in-process; no checkpoint recorded yet for region '$normalizedRegion'."
        }

        return CohortCollectorStatusDto(
            region = normalizedRegion,
            targetPatch = effectivePatch,
            runningInProcess = isRunning,
            overnightSessionArmed = isArmed,
            stopRequested = stopRequested.get(),
            executionMode = "SAME_PROCESS_GUARDED",
            action = action,
            message = customMessage ?: defaultMsg,
            activeBatchIndex = if (isRunning) activeBatchIndex else productiveBatchesCompleted,
            productiveBatchesCompleted = productiveBatchesCompleted,
            pauseCyclesCount = pauseCyclesCount,
            consecutivePauseCycles = consecutivePauseCycles,
            maxBatchesConfigured = maxBatchesConfigured,
            maxPauseCyclesConfigured = maxPauseCyclesConfigured,
            batchSizeConfigured = batchSizeConfigured,
            pauseDelayMsConfigured = pauseDelayMsConfigured,
            sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
            maxSessionProductiveBatchesConfigured = maxSessionProductiveBatchesConfigured,
            sessionNetworkCallsMade = sessionNetworkCallsMade,
            maxSessionNetworkCallsConfigured = maxSessionNetworkCallsConfigured,
            lastRunStartedAt = lastRunStartedAt,
            lastRunCompletedAt = lastRunCompletedAt,
            rateLimitVerification = verification,
            memoryHeadroom = memoryHeadroom,
            checkpoint = checkpoint
        )
    }

    /**
     * Guarded, bounded diagnostic probe entrypoint in the same backend JVM process.
     * - Observes real Riot `X-App-Rate-Limit`, `X-App-Rate-Limit-Count`, `X-Method-Rate-Limit`, and
     *   `X-Method-Rate-Limit-Count` headers BEFORE enabling bulk network collection.
     * - Bounded to at most 1 actual HTTP attempt (`maxRetries = 0`) per missing required endpoint (`<= 3` HTTP attempts total;
     *   `0` HTTP attempts when already verified), stopping immediately on the first `429` honoring `Retry-After`.
     */
    fun runDiagnosticProbeGuarded(
        region: String = "euw1",
        targetPatch: String? = null,
        seedPuuid: String? = null,
        seedMatchId: String? = null
    ): CohortRateLimitProbeResultDto {
        val normalizedRegion = region.lowercase().trim()
        val cleanPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
        val nowInstant = Instant.ofEpochMilli(timeProvider.now())
        val zeroAttemptsMap = CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS.associateWith { 0 }

        if (!cohortBenchmarkService.isValidRegion(normalizedRegion) || !cohortBenchmarkService.isValidPatchOrNull(cleanPatch)) {
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
                rateLimitVerification = buildInvalidParamStatus(normalizedRegion, cleanPatch, "Invalid region or patch.").rateLimitVerification,
                memoryHeadroom = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                notes = listOf("Invalid region or patch parameter; zero Redis or Riot HTTP attempts executed.")
            )
        }

        if (!probeRunning.compareAndSet(false, true)) {
            return CohortRateLimitProbeResultDto(
                region = normalizedRegion,
                probeStatus = "PROBE_ALREADY_RUNNING_IN_PROCESS",
                maxCallsPerEndpoint = 1,
                maxHttpAttemptsPerEndpoint = 1,
                maxRetriesConfigured = 0,
                totalProbeCallsMade = 0,
                totalHttpAttemptsMade = 0,
                httpAttemptsByEndpoint = zeroAttemptsMap,
                stoppedOnRateLimit429 = false,
                probedAt = nowInstant,
                rateLimitVerification = cohortBenchmarkService.getLiveRateLimitVerification(normalizedRegion),
                memoryHeadroom = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                checkpoint = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch),
                notes = listOf("Diagnostic probe is already running in this JVM process; 0 duplicate HTTP attempts made.")
            )
        }

        try {
            val acquiredProbeLock = cohortBenchmarkService.acquireCollectorProbeLock(normalizedRegion)
            if (!acquiredProbeLock) {
                return CohortRateLimitProbeResultDto(
                    region = normalizedRegion,
                    probeStatus = "PROBE_COOLDOWN_LOCK_ACTIVE",
                    maxCallsPerEndpoint = 1,
                    maxHttpAttemptsPerEndpoint = 1,
                    maxRetriesConfigured = 0,
                    totalProbeCallsMade = 0,
                    totalHttpAttemptsMade = 0,
                    httpAttemptsByEndpoint = zeroAttemptsMap,
                    stoppedOnRateLimit429 = false,
                    probedAt = nowInstant,
                    rateLimitVerification = cohortBenchmarkService.getLiveRateLimitVerification(normalizedRegion),
                    memoryHeadroom = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
                    checkpoint = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch),
                    notes = listOf("Diagnostic probe lock is active in Redis for '$normalizedRegion'; 0 duplicate HTTP attempts made.")
                )
            }
            return try {
                runBlocking {
                    cohortBenchmarkService.runBoundedDiagnosticRateLimitProbe(
                        region = normalizedRegion,
                        targetPatch = cleanPatch,
                        explicitSeedPuuid = seedPuuid,
                        explicitSeedMatchId = seedMatchId
                    )
                }
            } finally {
                cohortBenchmarkService.releaseCollectorProbeLock(normalizedRegion)
            }
        } finally {
            probeRunning.set(false)
        }
    }

    suspend fun executeResumableRun(
        region: String = "euw1",
        targetPatch: String? = null,
        maxBatches: Int = DEFAULT_MAX_BATCHES_PER_CHUNK,
        batchSize: Int = DEFAULT_BATCH_SIZE,
        pauseDelayMs: Long = 2_500L,
        maxPauseCycles: Int = 240
    ): CohortCollectorCheckpointDto? {
        logger.info(
            "[OvernightCollector] Starting same-process resumable LOW-priority cohort collector: region={}, patch={}, maxProductiveBatches={}, maxSessionBatches={}/{}, maxSessionNetworkCalls={}/{}, maxPauseCycles={}, batchSize={}, maxGamesPerPlayer={}",
            region, targetPatch ?: "auto-cached", maxBatches,
            sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
            sessionNetworkCallsMade, maxSessionNetworkCallsConfigured,
            maxPauseCycles, batchSize, CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
        )

        var lastCheckpoint: CohortCollectorCheckpointDto? = null
        while (productiveBatchesCompleted < maxBatches) {
            if (stopRequested.get()) {
                overnightSessionArmed.set(false)
                val existing = lastCheckpoint ?: cohortBenchmarkService.getCollectorCheckpoint(region, targetPatch)
                if (existing != null) {
                    val stopped = existing.copy(
                        sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                        maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade = sessionNetworkCallsMade,
                        maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                        lastStopReason = "STOPPED_BY_OPERATOR",
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                    cohortBenchmarkService.saveCollectorCheckpoint(stopped)
                    lastCheckpoint = stopped
                }
                logger.info("[OvernightCollector] Stopping before productive batch {} due to operator stop request.", productiveBatchesCompleted + 1)
                break
            }

            if (sessionProductiveBatchesCompleted >= maxSessionProductiveBatchesConfigured ||
                sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured
            ) {
                overnightSessionArmed.set(false)
                val capReason = if (sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured) {
                    STOP_REASON_SESSION_NETWORK_CALL_CAP
                } else {
                    STOP_REASON_SESSION_BATCH_CAP
                }
                val existing = lastCheckpoint ?: cohortBenchmarkService.getCollectorCheckpoint(region, targetPatch ?: activeTargetPatch)
                if (existing != null && existing.lastStopReason != capReason) {
                    val capped = existing.copy(
                        sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                        maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade = sessionNetworkCallsMade,
                        maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                        lastStopReason = capReason,
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                    cohortBenchmarkService.saveCollectorCheckpoint(capped)
                    lastCheckpoint = capped
                }
                logger.info(
                    "[OvernightCollector] Stopping before batch {}: session hard cap reached ({}, sessionBatches={}/{}, sessionNetworkCalls={}/{}).",
                    productiveBatchesCompleted + 1, capReason,
                    sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                    sessionNetworkCallsMade, maxSessionNetworkCallsConfigured
                )
                break
            }

            activeBatchIndex = productiveBatchesCompleted + 1
            cohortBenchmarkService.refreshCollectorProcessLock(region)

            val processedBeforeBatch = lastCheckpoint?.processedPuuids?.size
                ?: cohortBenchmarkService.getCollectorCheckpoint(region, targetPatch ?: activeTargetPatch)?.processedPuuids?.size
                ?: 0

            val checkpoint = try {
                cohortBenchmarkService.runResumableLowPriorityCollectorBatch(
                    region = region,
                    targetPatch = targetPatch ?: lastCheckpoint?.targetPatch,
                    maxPlayersPerBatch = batchSize,
                    maxGamesPerPlayer = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER,
                    maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                    sessionBaselineNetworkCalls = sessionBaselineNetworkCalls,
                    sessionProductiveBatchesBeforeBatch = sessionProductiveBatchesCompleted,
                    maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                    shouldStop = { stopRequested.get() }
                )
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                overnightSessionArmed.set(false)
                val existing = lastCheckpoint ?: cohortBenchmarkService.getCollectorCheckpoint(region, targetPatch ?: activeTargetPatch)
                if (existing != null) {
                    sessionNetworkCallsMade = (existing.totalNetworkCallsMade - sessionBaselineNetworkCalls).coerceAtLeast(0)
                    val aborted = existing.copy(
                        sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                        maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade = sessionNetworkCallsMade,
                        maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                        lastStopReason = STOP_REASON_SESSION_ERROR_ABORT,
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                    cohortBenchmarkService.saveCollectorCheckpoint(aborted)
                    lastCheckpoint = aborted
                }
                logger.error(
                    "[OvernightCollector] Aborting chunk and disarming auto-resume due to exception on batch {}: region={}, patch={}, error={}",
                    productiveBatchesCompleted + 1, region, targetPatch ?: activeTargetPatch ?: "auto-cached", e.message
                )
                break
            }
            lastCheckpoint = checkpoint
            sessionNetworkCallsMade = (checkpoint.totalNetworkCallsMade - sessionBaselineNetworkCalls).coerceAtLeast(0)
            if (activeTargetPatch == null && checkpoint.targetPatch != null) {
                activeTargetPatch = checkpoint.targetPatch
            }

            when (checkpoint.lastStopReason) {
                in TERMINAL_STOP_REASONS -> {
                    if (checkpoint.processedPuuids.size > processedBeforeBatch) {
                        productiveBatchesCompleted++
                        sessionProductiveBatchesCompleted++
                        consecutivePauseCycles = 0
                    }
                    overnightSessionArmed.set(false)
                    logger.info(
                        "[OvernightCollector] Stopping cleanly (terminal): productiveBatches={}/{}, sessionBatches={}/{}, sessionNetworkCalls={}/{}, pauseCycles={}, reason={}",
                        productiveBatchesCompleted, maxBatches,
                        sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade, maxSessionNetworkCallsConfigured,
                        pauseCyclesCount, checkpoint.lastStopReason
                    )
                    break
                }
                "PAUSED_FOREGROUND_USER_REQUEST",
                "PAUSED_RETRY_AFTER_BACKOFF",
                "PAUSED_RATE_LIMIT_HEADROOM",
                "PAUSED_NETWORK_TIMEOUT",
                "PAUSED_MEMORY_HEADROOM" -> {
                    // Pause cycles do NOT consume productive batch budget (productiveBatchesCompleted is unchanged)
                    pauseCyclesCount++
                    consecutivePauseCycles++
                    logger.info(
                        "[OvernightCollector] Paused without consuming productive batch budget (productiveBatches={}/{}, sessionBatches={}/{}, consecutivePause={}/{}, totalPauses={}, reason={})",
                        productiveBatchesCompleted, maxBatches,
                        sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                        consecutivePauseCycles, maxPauseCycles, pauseCyclesCount, checkpoint.lastStopReason
                    )
                    if (consecutivePauseCycles >= maxPauseCycles) {
                        val cooldownCheckpoint = checkpoint.copy(
                            sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                            maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                            sessionNetworkCallsMade = sessionNetworkCallsMade,
                            maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                            lastStopReason = "PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE",
                            updatedAt = Instant.ofEpochMilli(timeProvider.now())
                        )
                        cohortBenchmarkService.saveCollectorCheckpoint(cooldownCheckpoint)
                        lastCheckpoint = cooldownCheckpoint
                        logger.info(
                            "[OvernightCollector] Reached maxPauseCycles={} during long rate-limit/memory/foreground window; saved PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE (overnightSessionArmed={} for auto-resume).",
                            maxPauseCycles, overnightSessionArmed.get()
                        )
                        break
                    }
                    if (pauseDelayMs > 0L && !stopRequested.get()) {
                        val waitMs = computeEffectivePauseWaitMs(region, checkpoint.lastStopReason, pauseDelayMs)
                        delayWithStopCheckAndLockRefresh(region, waitMs)
                    }
                }
                else -> {
                    productiveBatchesCompleted++
                    sessionProductiveBatchesCompleted++
                    consecutivePauseCycles = 0
                    logger.info(
                        "[OvernightCollector] Completed productive batch {}/{} (sessionBatches={}/{}, sessionNetworkCalls={}/{}): region={}, patch={}, cellsPassing={}/{}, processedPuuids={}, pendingCandidates={}, networkCalls={}, stopReason={}",
                        productiveBatchesCompleted, maxBatches,
                        sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                        sessionNetworkCallsMade, maxSessionNetworkCallsConfigured,
                        checkpoint.region, checkpoint.targetPatch ?: "NONE",
                        checkpoint.cellsPassingQualityGate, checkpoint.targetCellsCount,
                        checkpoint.processedPuuids.size, checkpoint.pendingCandidatePuuids.size,
                        checkpoint.totalNetworkCallsMade, checkpoint.lastStopReason
                    )
                    if (sessionProductiveBatchesCompleted >= maxSessionProductiveBatchesConfigured ||
                        sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured
                    ) {
                        overnightSessionArmed.set(false)
                        val capReason = if (sessionNetworkCallsMade >= maxSessionNetworkCallsConfigured) {
                            STOP_REASON_SESSION_NETWORK_CALL_CAP
                        } else {
                            STOP_REASON_SESSION_BATCH_CAP
                        }
                        if (checkpoint.lastStopReason != capReason) {
                            val cappedCheckpoint = checkpoint.copy(
                                sessionProductiveBatchesCompleted = sessionProductiveBatchesCompleted,
                                maxSessionProductiveBatches = maxSessionProductiveBatchesConfigured,
                                sessionNetworkCallsMade = sessionNetworkCallsMade,
                                maxSessionNetworkCalls = maxSessionNetworkCallsConfigured,
                                lastStopReason = capReason,
                                updatedAt = Instant.ofEpochMilli(timeProvider.now())
                            )
                            cohortBenchmarkService.saveCollectorCheckpoint(cappedCheckpoint)
                            lastCheckpoint = cappedCheckpoint
                        }
                        logger.info(
                            "[OvernightCollector] Disarming and stopping after productive batch {}: session hard cap reached ({}, sessionBatches={}/{}, sessionNetworkCalls={}/{}).",
                            productiveBatchesCompleted, capReason,
                            sessionProductiveBatchesCompleted, maxSessionProductiveBatchesConfigured,
                            sessionNetworkCallsMade, maxSessionNetworkCallsConfigured
                        )
                        break
                    }
                }
            }
        }
        return lastCheckpoint
    }

    private fun computeEffectivePauseWaitMs(
        region: String,
        stopReason: String,
        pauseDelayMs: Long
    ): Long {
        if (pauseDelayMs <= 0L) return 0L
        if (stopReason == "PAUSED_RETRY_AFTER_BACKOFF") {
            val snap = cohortBenchmarkService.getLiveRateLimitVerification(region)
            val retryUntilMs = snap.retryAfterUntil?.toEpochMilli() ?: 0L
            val remainingMs = (retryUntilMs - timeProvider.now() + 50L).coerceAtLeast(0L)
            val maxSingleCycleWaitMs = (pauseDelayMs * 24L).coerceAtMost(60_000L)
            return maxOf(pauseDelayMs, remainingMs.coerceAtMost(maxSingleCycleWaitMs))
        }
        return pauseDelayMs
    }

    private suspend fun delayWithStopCheckAndLockRefresh(region: String, totalWaitMs: Long) {
        var remaining = totalWaitMs
        val sliceMs = 2_500L
        while (remaining > 0L && !stopRequested.get()) {
            val step = minOf(remaining, sliceMs)
            delay(step)
            remaining -= step
            if (remaining > 0L && !stopRequested.get()) {
                cohortBenchmarkService.refreshCollectorProcessLock(region)
            }
        }
    }

    private fun buildInvalidParamStatus(
        region: String,
        targetPatch: String?,
        note: String
    ): CohortCollectorStatusDto {
        return CohortCollectorStatusDto(
            region = region,
            targetPatch = targetPatch,
            runningInProcess = false,
            overnightSessionArmed = false,
            stopRequested = false,
            executionMode = "SAME_PROCESS_GUARDED",
            action = "INVALID_PARAMETERS",
            message = note,
            rateLimitVerification = RateLimitVerificationSnapshotDto(
                region = region,
                appLimitsVerifiedFromHeaders = false,
                appRateLimitHeader = null,
                appRateLimitCountHeader = null,
                activeAppBuckets = emptyList(),
                appHasHeadroomForLowPriority = false,
                methodLimitsByEndpoint = emptyMap(),
                verifiedMethodEndpoints = emptySet(),
                missingRequiredMethodEndpoints = CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS,
                lastRateLimitType = null,
                serviceRateLimitActive = false,
                foregroundYieldActive = false,
                lowPriorityCapRatio = 0.70,
                userRequestYieldWindowMs = 2_000L,
                activeUserRequests = 0,
                lastHighPriorityRequestAt = null,
                retryAfterUntil = null,
                networkCollectionEnabled = false,
                readyForLowPriorityNetworkCollection = false
            ),
            memoryHeadroom = cohortBenchmarkService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false),
            checkpoint = null
        )
    }
}

