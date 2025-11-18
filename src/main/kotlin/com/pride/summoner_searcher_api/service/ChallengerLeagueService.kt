package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.LeagueListDTO
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class ChallengerLeagueService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private fun getCacheKey(region: String, queue: String) = "leaderboard:${region.lowercase()}::$queue"

    fun getChallengerLeagueFromCache(region: String, queue: String): LeagueListDTO? {
        val cacheKey = getCacheKey(region, queue)
        logger.info("Frontend requested challenger leaderboard for key: {}", cacheKey)
        return redisCacheService.get(cacheKey, LeagueListDTO::class.java)
    }

    fun warmChallengerLeagueCache(region: String, queue: String) {
        val cacheKey = getCacheKey(region, queue)

        val existingData = redisCacheService.get(cacheKey, LeagueListDTO::class.java)
        if (existingData != null) {
            logger.info("[Cache Warmer] HIT for key: {}. No refresh needed.", cacheKey)
            return
        }

        logger.info("[Cache Warmer] MISS for key: {}. Fetching from API...", cacheKey)
        val freshLeague = riotApiService.fetchChallengerLeague(region, queue)

        if (freshLeague != null) {
            logger.info("[Cache Warmer] Populating cache with fresh data for key: {}", cacheKey)
            redisCacheService.set(cacheKey, freshLeague, Duration.ofHours(25))
        }
    }
}
