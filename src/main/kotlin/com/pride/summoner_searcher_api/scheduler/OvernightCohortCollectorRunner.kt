package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.dto.CohortBenchmarkCellDto
import com.pride.summoner_searcher_api.dto.CohortCollectorCheckpointDto
import com.pride.summoner_searcher_api.service.CohortBenchmarkService
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component

/**
 * Resumable, checkpointed LOW-priority overnight cohort collector runner.
 *
 * Triggered ONLY when `--collect-cohorts` is passed on the command line.
 * - Defaults to `euw1`, current patch, Ranked Solo (`420`), roles `TOP`/`MIDDLE`/`BOTTOM`, all 10 rank tiers,
 *   capped at `10` games/player per `(tier, role)` cell, targeting `50` players + `200` games per cell.
 * - Executes cache-only extraction first and starts network calls ONLY when `cohort.network-collection.enabled=true`
 *   AND actual Riot `X-App-Rate-Limit` and `X-Method-Rate-Limit` headers and headroom are verified.
 * - Yields to foreground user requests and respects `429 Retry-After` / `service` rate limits.
 * - Never promises guaranteed morning coverage; stops cleanly with checkpointed state.
 */
@Component
class OvernightCohortCollectorRunner(
    private val cohortBenchmarkService: CohortBenchmarkService
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run(args: ApplicationArguments?) {
        if (args == null || !args.containsOption("collect-cohorts")) {
            return
        }

        val region = args.getOptionValues("collect-region")?.firstOrNull()?.lowercase()?.trim() ?: "euw1"
        val targetPatch = args.getOptionValues("collect-patch")?.firstOrNull()?.trim()?.takeIf { it.isNotBlank() }
        val maxBatches = args.getOptionValues("collect-max-batches")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 500) ?: 20
        val batchSize = args.getOptionValues("collect-batch-size")?.firstOrNull()?.toIntOrNull()?.coerceIn(1, 200) ?: 25
        val pauseDelayMs = args.getOptionValues("collect-pause-ms")?.firstOrNull()?.toLongOrNull()?.coerceIn(0L, 60_000L) ?: 2_500L

        runBlocking {
            executeResumableRun(
                region = region,
                targetPatch = targetPatch,
                maxBatches = maxBatches,
                batchSize = batchSize,
                pauseDelayMs = pauseDelayMs
            )
        }
    }

    suspend fun executeResumableRun(
        region: String = "euw1",
        targetPatch: String? = null,
        maxBatches: Int = 20,
        batchSize: Int = 25,
        pauseDelayMs: Long = 2_500L
    ): CohortCollectorCheckpointDto? {
        logger.info(
            "[OvernightCollector] Starting resumable LOW-priority cohort collector: region={}, patch={}, maxBatches={}, batchSize={}, maxGamesPerPlayer={}",
            region, targetPatch ?: "auto-cached", maxBatches, batchSize, CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
        )

        var lastCheckpoint: CohortCollectorCheckpointDto? = null
        for (batchIndex in 1..maxBatches) {
            val checkpoint = cohortBenchmarkService.runResumableLowPriorityCollectorBatch(
                region = region,
                targetPatch = targetPatch ?: lastCheckpoint?.targetPatch,
                maxPlayersPerBatch = batchSize,
                maxGamesPerPlayer = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
            )
            lastCheckpoint = checkpoint

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
                "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY" -> {
                    logger.info("[OvernightCollector] Stopping cleanly at batch {} with reason={}", batchIndex, checkpoint.lastStopReason)
                    break
                }
                "PAUSED_FOREGROUND_USER_REQUEST",
                "PAUSED_RETRY_AFTER_BACKOFF",
                "PAUSED_RATE_LIMIT_HEADROOM" -> {
                    if (pauseDelayMs > 0L && batchIndex < maxBatches) {
                        delay(pauseDelayMs)
                    }
                }
            }
        }
        return lastCheckpoint
    }
}
