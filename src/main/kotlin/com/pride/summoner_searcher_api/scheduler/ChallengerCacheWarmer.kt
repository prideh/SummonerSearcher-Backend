package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class ChallengerCacheWarmer(
    private val challengerLeagueService: ChallengerLeagueService
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val queue = "RANKED_SOLO_5x5"

    override fun run(args: ApplicationArguments?) {
        logger.info("Initial cache check on application startup...")
        refreshCacheIfNeeded()
    }

    @Scheduled(cron = "0 0 2 * * ?", zone = "CET")
    fun refreshCacheIfNeeded() {
        logger.info("Scheduled task running: Checking if Challenger league cache needs refreshing for queue: {}", queue)
        val regions = listOf("euw1", "na1", "kr")

        for (region in regions) {
            try {
                challengerLeagueService.warmChallengerLeagueCache(region, queue)
            } catch (e: Exception) {
                logger.error("Error during scheduled cache check for {} - {}: {}", region, queue, e.message)
            }
        }
        logger.info("Scheduled cache check complete.")
    }
}
