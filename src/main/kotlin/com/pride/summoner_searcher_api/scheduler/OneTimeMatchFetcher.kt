package com.pride.summoner_searcher_api.scheduler

import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.service.PlayerCacheService
import com.pride.summoner_searcher_api.service.RedisCacheService
import com.pride.summoner_searcher_api.service.RiotApiService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class OneTimeMatchFetcher(
    private val riotApiService: RiotApiService,
    private val redisCacheService: RedisCacheService,
    private val playerCacheService: PlayerCacheService
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run(args: ApplicationArguments?) {
        if (args == null || !args.containsOption("fetch-player")) {
            return
        }

        val playerArg = args.getOptionValues("fetch-player").firstOrNull()
        val regionArg = args.getOptionValues("fetch-region")?.firstOrNull() ?: "euw1"

        if (playerArg == null) {
            logger.error("Please provide a player name in format Name#Tag")
            return
        }

        val parts = playerArg.split("#")
        if (parts.size != 2) {
            logger.error("Invalid player format. Use Name#Tag")
            return
        }

        val name = parts[0]
        val tag = parts[1]

        logger.info("Starting one-time match fetch for $name#$tag in $regionArg")

        runBlocking {
            try {
                fetchAndCacheAllMatches(name, tag, regionArg)
            } catch (e: Exception) {
                logger.error("Error fetching matches", e)
            }
        }
    }

    private suspend fun fetchAndCacheAllMatches(name: String, tag: String, region: String) {
        // 1. Get Account & PUUID
        val account = riotApiService.getAccountByRiotId(name, tag, region)
        if (account == null) {
            logger.error("Account not found: $name#$tag")
            return
        }
        val puuid = account.puuid ?: return

        // 2. Ensure Profile is Cached (Fetch this FIRST to fail fast if API is down)
        logger.info("Fetching/Ensuring base profile exists for $name#$tag...")
        val baseProfile = playerCacheService.getPlayerProfile(puuid, region, name, tag)
        if (baseProfile == null) {
            logger.error("Could not fetch base profile. Aborting.")
            return
        }
        logger.info("Base profile secured. Starting match fetch...")

        // 3. Fetch all match IDs
        val allMatchIds = mutableSetOf<String>()
        var endTime: Long? = null
        val maxMatches = 1000 // Reduced from 5000 to prevent OOM on small instances
        
        // Limit to matches starting from Jan 1, 2025
        val startTime = java.time.LocalDate.of(2025, 1, 1)
            .atStartOfDay(java.time.ZoneId.of("UTC"))
            .toEpochSecond()

        logger.info("Fetching match IDs for PUUID: $puuid starting from 2025-01-01 ($startTime)")

        while (true) {
            // Fetch batch of 100
            val ids = riotApiService.getMatchIdsByPuuid(
                puuid = puuid,
                region = region,
                queueId = 420, // Ranked Solo/Duo
                startTime = startTime, 
                endTime = endTime,
                count = 100
            )

            if (ids.isNullOrEmpty()) {
                break
            }

            val newIds = ids.filter { !allMatchIds.contains(it) }
            if (newIds.isEmpty()) {
                break // No new matches found
            }

            allMatchIds.addAll(newIds)
            logger.info("Fetched ${allMatchIds.size} match IDs so far...")

            if (allMatchIds.size >= maxMatches) {
                logger.warn("Reached max match limit ($maxMatches). Stopping.")
                break
            }

            // Get details of the last match to update endTime
            val lastMatchId = ids[ids.size - 1]
            val lastMatch = riotApiService.getMatchById(lastMatchId, region)
            
            if (lastMatch?.info?.gameCreation != null) {
                endTime = (lastMatch.info.gameCreation / 1000)
            } else {
                logger.warn("Could not get timestamp for match $lastMatchId. Stopping pagination.")
                break
            }
        }

        logger.info("Total match IDs found: ${allMatchIds.size}")

        // 4. Fetch details for ALL matches
        val allMatches = mutableListOf<MatchDto>()
        val chunkedIds = allMatchIds.chunked(50)
        
        chunkedIds.forEachIndexed { index, chunk ->
            logger.info("Fetching details for chunk ${index + 1}/${chunkedIds.size}...")
            val matches = coroutineScope {
                chunk.map { matchId ->
                    async {
                        riotApiService.getMatchById(matchId, region)
                    }
                }.awaitAll().filterNotNull()
            }
            allMatches.addAll(matches)
        }
        
        logger.info("Successfully fetched ${allMatches.size} match details.")

        // 5. Update Cache with FULL history
        val updatedProfile = baseProfile.copy(
            recentMatches = allMatches.sortedByDescending { it.info?.gameCreation }
        )
        val cacheKey = "player:profile:$region:$puuid"
        redisCacheService.set(cacheKey, updatedProfile, Duration.ZERO)
        logger.info("Cache updated for $name#$tag with ${allMatches.size} matches.")
    }
}
