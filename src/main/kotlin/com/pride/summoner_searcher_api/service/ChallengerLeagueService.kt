package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.LeagueListDTO
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * A service responsible for managing the challenger leaderboard data.
 * It handles fetching the data for the frontend and warming the cache in the background.
 */
@Service
class ChallengerLeagueService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Generates a unique cache key for a region's leaderboard.
     */
    private fun getCacheKey(region: String, queue: String) = "leaderboard:${region.lowercase()}::$queue"

    /**
     * Retrieves the challenger leaderboard directly from the Redis cache.
     * This method is called by the controller and does not trigger any Riot API calls.
     * @return The cached [LeagueListDTO], or null if it doesn't exist in the cache.
     */
    fun getChallengerLeagueFromCache(region: String, queue: String): LeagueListDTO? {
        val cacheKey = getCacheKey(region, queue)
        logger.info("Frontend requested challenger leaderboard for key: {}", cacheKey)
        return redisCacheService.get(cacheKey, LeagueListDTO::class.java)
    }

    /**
     * Fetches the challenger leaderboard from the Riot API and populates the cache.
     * This method is designed to be called by a background scheduler. It will only perform the
     * expensive API fetch if the data is not already present in the cache.
     */
    fun warmChallengerLeagueCache(region: String, queue: String) {
        val cacheKey = getCacheKey(region, queue)

        // Check if the data already exists to avoid unnecessary API calls.
        val existingData = redisCacheService.get(cacheKey, LeagueListDTO::class.java)
        if (existingData != null) {
            logger.info("[Cache Warmer] HIT for key: {}. No refresh needed.", cacheKey)
            return // Exit if cache is already warm.
        }

        // If data is not in the cache, perform the slow, rate-limited fetch.
        logger.info("[Cache Warmer] MISS for key: {}. Fetching from API...", cacheKey)
        val freshLeague = riotApiService.fetchChallengerLeague(region, queue)

        if (freshLeague != null) {
            logger.info("[Cache Warmer] Populating cache with fresh data for key: {}", cacheKey)
            // Set a 25-hour expiration. This ensures the data is always available, even if the
            // daily refresh job fails once, but will be gone for the subsequent day's check.
            redisCacheService.set(cacheKey, freshLeague, Duration.ofHours(25))
        }
    }
}
