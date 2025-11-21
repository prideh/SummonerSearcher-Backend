package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.CachedLeaderboardDto
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant

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
        // Fetch the wrapper object and return only the leaderboard data.
        return redisCacheService.get(cacheKey, CachedLeaderboardDto::class.java)?.leaderboard
    }

    /**
     * Fetches the challenger leaderboard from the Riot API and populates the cache.
     * This method uses a "time-to-refresh" strategy. It will only perform the expensive API
     * fetch if the cached data is missing or is older than 24 hours.
     */
    fun warmChallengerLeagueCache(region: String, queue: String) {
        val cacheKey = getCacheKey(region, queue)
        val cachedData = redisCacheService.get(cacheKey, CachedLeaderboardDto::class.java)

        // Check if the data exists and is less than 24 hours old.
        if (cachedData != null && Duration.between(cachedData.lastRefreshed, Instant.now()).toHours() < 24) {
            logger.info("[Cache Warmer] HIT for key: {}. Data is fresh. No refresh needed.", cacheKey)
            return // Exit if cache is fresh.
        }

        // If data is missing or stale, perform the slow, rate-limited fetch.
        val logMessage = if (cachedData == null) "MISS" else "STALE"
        logger.info("[Cache Warmer] {} for key: {}. Fetching from API...", logMessage, cacheKey)
        
        val freshLeague = riotApiService.fetchChallengerLeague(region, queue)

        if (freshLeague != null) {
            logger.info("[Cache Warmer] Populating cache with fresh data for key: {}", cacheKey)
            val newCachedData = CachedLeaderboardDto(
                lastRefreshed = Instant.now(),
                leaderboard = freshLeague
            )
            // Store the data indefinitely. Our service now manages the refresh logic.
            redisCacheService.set(cacheKey, newCachedData, Duration.ZERO)
        }
    }
}
