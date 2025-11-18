package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.LeagueListDTO
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class ChallengerLeagueService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService
) {

    private fun getCacheKey(region: String, queue: String) = "leaderboard:${region.lowercase()}::$queue"

    // This method is for the frontend - it ONLY reads from the Redis cache.
    fun getChallengerLeagueFromCache(region: String, queue: String): LeagueListDTO? {
        val cacheKey = getCacheKey(region, queue)
        println("[Redis Cache] Frontend requested key: $cacheKey")
        return redisCacheService.get(cacheKey, LeagueListDTO::class.java)
    }

    // This method is for the background scheduler. It only fetches if the cache is empty.
    fun warmChallengerLeagueCache(region: String, queue: String) {
        val cacheKey = getCacheKey(region, queue)

        // Check if the data already exists
        val existingData = redisCacheService.get(cacheKey, LeagueListDTO::class.java)
        if (existingData != null) {
            println("[Cache Warmer] HIT for key: $cacheKey. No refresh needed.")
            return // Exit if cache is already warm
        }

        println("[Cache Warmer] MISS for key: $cacheKey. Fetching from API...")
        val freshLeague = riotApiService.fetchChallengerLeague(region, queue)

        if (freshLeague != null) {
            println("[Cache Warmer] Populating cache with fresh data for key: $cacheKey")
            // Set a 25-hour expiration to ensure it's always available, even if the refresher fails once.
            redisCacheService.set(cacheKey, freshLeague, Duration.ofHours(25))
        }
    }
}
