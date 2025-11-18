package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class ChallengerCacheWarmer(
    private val challengerLeagueService: ChallengerLeagueService
) : ApplicationRunner {

    private val queue = "RANKED_SOLO_5x5"

    override fun run(args: ApplicationArguments?) {
        println("Initial cache check on application startup...")
        refreshCacheIfNeeded()
    }

    @Scheduled(cron = "0 0 2 * * ?", zone = "CET")
    fun refreshCacheIfNeeded() {
        println("Scheduled task running: Checking if Challenger league cache needs refreshing...")
        val regions = listOf("euw1", "na1", "kr")

        for (region in regions) {
            try {
                challengerLeagueService.warmChallengerLeagueCache(region, queue)
            } catch (e: Exception) {
                println("Error during scheduled cache check for $region - $queue: ${e.message}")
            }
        }
        println("Scheduled cache check complete.")
    }
}
