package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.dto.CohortBenchmarkCellDto
import com.pride.summoner_searcher_api.dto.CohortCollectorCheckpointDto
import com.pride.summoner_searcher_api.dto.CohortCollectorStatusDto
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
    private val stopRequested = AtomicBoolean(false)

    @Volatile
    private var activeRegion: String = "euw1"

    @Volatile
    private var activeTargetPatch: String? = null

    @Volatile
    private var activeBatchIndex: Int = 0

    @Volatile
    private var maxBatchesConfigured: Int = 20

    @Volatile
    private var batchSizeConfigured: Int = 25

    @Volatile
    private var pauseDelayMsConfigured: Long = 2_500L

    @Volatile
    private var lastRunStartedAt: Instant? = null

    @Volatile
    private var lastRunCompletedAt: Instant? = null

    companion object {
        private val LOOPBACK_ADDRESSES = setOf(
            "127.0.0.1",
            "::1",
            "0:0:0:0:0:0:0:1",
            "localhost"
        )
    }

    override fun run(args: ApplicationArguments?) {
        if (args == null || !args.containsOption("collect-cohorts")) {
            return
        }

        val region = args.getOptionValues("collect-region")?.firstOrNull()?.lowercase()?.trim() ?: "euw1"
        val targetPatch = args.getOptionValues("collect-patch")?.firstOrNull()?.trim()?.takeIf { it.isNotBlank() }
        val maxBatches = args.getOptionValues("collect-max-batches")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 500) ?: 20
        val batchSize = args.getOptionValues("collect-batch-size")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 200) ?: 25
        val pauseDelayMs = args.getOptionValues("collect-pause-ms")?.firstOrNull()?.toLongOrNull()?.coerceIn(0L, 60_000L) ?: 2_500L

        // Start asynchronously in the SAME process so the web server and ChallengerCacheWarmer run normally
        startOrResumeGuarded(
            region = region,
            targetPatch = targetPatch,
            maxBatches = maxBatches,
            batchSize = batchSize,
            pauseDelayMs = pauseDelayMs,
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
     */
    fun startOrResumeGuarded(
        region: String = "euw1",
        targetPatch: String? = null,
        maxBatches: Int = 20,
        batchSize: Int = 25,
        pauseDelayMs: Long = 2_500L,
        async: Boolean = true
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

        stopRequested.set(false)
        activeRegion = normalizedRegion
        activeTargetPatch = cleanPatch
        activeBatchIndex = 0
        maxBatchesConfigured = coercedMaxBatches
        batchSizeConfigured = coercedBatchSize
        pauseDelayMsConfigured = coercedPauseMs
        lastRunStartedAt = Instant.ofEpochMilli(timeProvider.now())

        if (async) {
            scope.launch {
                try {
                    executeResumableRun(
                        region = normalizedRegion,
                        targetPatch = cleanPatch,
                        maxBatches = coercedMaxBatches,
                        batchSize = coercedBatchSize,
                        pauseDelayMs = coercedPauseMs
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
                        maxBatches = coercedMaxBatches,
                        batchSize = coercedBatchSize,
                        pauseDelayMs = coercedPauseMs
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
                customMessage = "Completed synchronous guarded collector run in same process (stopReason=${finalCheckpoint?.lastStopReason ?: "NONE"})."
            )
        }
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
                "Stop requested; collector will halt at the next safe call boundary and checkpoint."
            } else {
                "Collector is not currently running; checkpoint marked STOPPED_BY_OPERATOR."
            }
        )
    }

    /**
     * Bounded, read-only status inspection from the same backend JVM process.
     * Makes zero Riot API calls and zero Redis SCANs (reads only the single checkpoint key + in-memory limiter snapshot).
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
        val checkpoint = cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, cleanPatch)
            ?: if (cleanPatch == null && activeTargetPatch != null) {
                cohortBenchmarkService.getCollectorCheckpoint(normalizedRegion, activeTargetPatch)
            } else {
                null
            }
        val effectivePatch = cleanPatch ?: checkpoint?.targetPatch ?: activeTargetPatch
        val isRunning = running.get() && activeRegion == normalizedRegion
        val defaultMsg = when {
            isRunning -> "Collector is running in-process (batch $activeBatchIndex/$maxBatchesConfigured)."
            checkpoint != null -> "Collector idle in-process (lastStopReason=${checkpoint.lastStopReason}, cellsPassing=${checkpoint.cellsPassingQualityGate}/${checkpoint.targetCellsCount})."
            else -> "Collector idle in-process; no checkpoint recorded yet for region '$normalizedRegion'."
        }

        return CohortCollectorStatusDto(
            region = normalizedRegion,
            targetPatch = effectivePatch,
            runningInProcess = isRunning,
            stopRequested = stopRequested.get(),
            executionMode = "SAME_PROCESS_GUARDED",
            action = action,
            message = customMessage ?: defaultMsg,
            activeBatchIndex = if (isRunning) activeBatchIndex else 0,
            maxBatchesConfigured = maxBatchesConfigured,
            batchSizeConfigured = batchSizeConfigured,
            pauseDelayMsConfigured = pauseDelayMsConfigured,
            lastRunStartedAt = lastRunStartedAt,
            lastRunCompletedAt = lastRunCompletedAt,
            rateLimitVerification = verification,
            checkpoint = checkpoint
        )
    }

    suspend fun executeResumableRun(
        region: String = "euw1",
        targetPatch: String? = null,
        maxBatches: Int = 20,
        batchSize: Int = 25,
        pauseDelayMs: Long = 2_500L
    ): CohortCollectorCheckpointDto? {
        logger.info(
            "[OvernightCollector] Starting same-process resumable LOW-priority cohort collector: region={}, patch={}, maxBatches={}, batchSize={}, maxGamesPerPlayer={}",
            region, targetPatch ?: "auto-cached", maxBatches, batchSize, CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
        )

        var lastCheckpoint: CohortCollectorCheckpointDto? = null
        for (batchIndex in 1..maxBatches) {
            if (stopRequested.get()) {
                val existing = lastCheckpoint ?: cohortBenchmarkService.getCollectorCheckpoint(region, targetPatch)
                if (existing != null) {
                    val stopped = existing.copy(
                        lastStopReason = "STOPPED_BY_OPERATOR",
                        updatedAt = Instant.ofEpochMilli(timeProvider.now())
                    )
                    cohortBenchmarkService.saveCollectorCheckpoint(stopped)
                    lastCheckpoint = stopped
                }
                logger.info("[OvernightCollector] Stopping before batch {} due to operator stop request.", batchIndex)
                break
            }

            activeBatchIndex = batchIndex
            cohortBenchmarkService.refreshCollectorProcessLock(region)

            val checkpoint = cohortBenchmarkService.runResumableLowPriorityCollectorBatch(
                region = region,
                targetPatch = targetPatch ?: lastCheckpoint?.targetPatch,
                maxPlayersPerBatch = batchSize,
                maxGamesPerPlayer = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER,
                shouldStop = { stopRequested.get() }
            )
            lastCheckpoint = checkpoint
            if (activeTargetPatch == null && checkpoint.targetPatch != null) {
                activeTargetPatch = checkpoint.targetPatch
            }

            logger.info(
                "[OvernightCollector] Batch {}/{}: region={}, patch={}, cellsPassing={}/{}, processedPuuids={}, pendingCandidates={}, networkCalls={}, stopReason={}",
                batchIndex, maxBatches, checkpoint.region, checkpoint.targetPatch ?: "NONE",
                checkpoint.cellsPassingQualityGate, checkpoint.targetCellsCount,
                checkpoint.processedPuuids.size, checkpoint.pendingCandidatePuuids.size,
                checkpoint.totalNetworkCallsMade, checkpoint.lastStopReason
            )

            when (checkpoint.lastStopReason) {
                "ALL_TARGET_CELLS_SATISFIED",
                "NO_CANDIDATES_REMAINING",
                "NETWORK_COLLECTION_DISABLED_BY_CONFIG",
                "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY",
                "STOPPED_BY_OPERATOR" -> {
                    logger.info("[OvernightCollector] Stopping cleanly at batch {} with reason={}", batchIndex, checkpoint.lastStopReason)
                    break
                }
                "PAUSED_FOREGROUND_USER_REQUEST",
                "PAUSED_RETRY_AFTER_BACKOFF",
                "PAUSED_RATE_LIMIT_HEADROOM" -> {
                    if (pauseDelayMs > 0L && batchIndex < maxBatches && !stopRequested.get()) {
                        delay(pauseDelayMs)
                    }
                }
            }
        }
        return lastCheckpoint
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
            checkpoint = null
        )
    }
}

