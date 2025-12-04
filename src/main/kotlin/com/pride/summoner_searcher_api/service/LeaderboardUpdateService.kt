package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * A background task scheduler responsible for keeping the challenger leaderboard cache warm.
 * This ensures that when a user requests the leaderboard, the data is already available and can be served instantly.
 *
 * It implements [ApplicationRunner] to trigger a cache check immediately on application startup.
 */
@Component
class ChallengerCacheWarmer(
    private val challengerLeagueService: ChallengerLeagueService
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val queue = "RANKED_SOLO_5x5"

    /**
     * This method is executed once, immediately after the application has started.
     * It ensures the cache is populated on deployment without waiting for the first scheduled run.
     */
    override fun run(args: ApplicationArguments?) {
        if (args != null && args.containsOption("fetch-player")) {
            logger.info("Skipping Challenger Cache Warmer because 'fetch-player' argument is present.")
            return
        }
        logger.info("Initial cache check on application startup...")
        refreshCacheIfNeeded()
    }

    /**
     * This method is scheduled to run automatically at a fixed interval.
     * The CRON expression "0 0 2 * * ?" translates to "at 2:00 AM every day".
     * The zone is set to CET to ensure it runs at a consistent time, regardless of server location.
     */
    @Scheduled(cron = "0 0 2 * * ?", zone = "CET")
    fun refreshCacheIfNeeded() {
        logger.info("Scheduled task running: Checking if Challenger league cache needs refreshing for queue: {}", queue)
        val regions = listOf("euw1", "na1", "kr")

        // Process regions in parallel - each region has independent rate limits
        runBlocking {
            regions.map { region ->
                async {
                    try {
                        logger.info("Starting cache refresh for region: {}", region)
                        challengerLeagueService.warmChallengerLeagueCache(region, queue)
                        logger.info("Completed cache refresh for region: {}", region)
                    } catch (e: Exception) {
                        // Catching a broad exception to ensure that a failure in one region does not stop the process for others.
                        logger.error("Error during scheduled cache check for {} - {}: {}", region, queue, e.message, e)
                    }
                }
            }.awaitAll()
        }
        logger.info("Scheduled cache check complete.")
    }
}
