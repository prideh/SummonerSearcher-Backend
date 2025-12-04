package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * A service for managing the caching of player profiles.
 * This service acts as an intelligent layer between the frontend request and the Riot API,
 * using Redis to store and retrieve player data to minimize API calls.
 */
@Service
class PlayerCacheService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService,
    private val statsCalculator: StatsCalculator
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Generates a unique key for storing a player's profile in Redis.
     * The key is composed of the player's PUUID and their region to ensure data is not mixed up
     * for players with the same account on different servers.
     */
    private fun getProfileCacheKey(puuid: String, region: String) = "player:profile:${region.lowercase()}:$puuid"

    /**
     * Retrieves a player's profile, utilizing a cache-aside strategy with an "always check" update mechanism.
     *
     * @param puuid The player's unique PUUID.
     * @param region The server region for the player's profile.
     * @param summonerName The player's current game name.
     * @param tagLine The player's current tag line.
     * @return A [SummonerProfileDto] containing the player's up-to-date profile.
     */
    suspend fun getPlayerProfile(puuid: String, region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

        // Calculate Start of Year (Jan 1st) in Epoch Seconds
        val startOfYear = java.time.Year.now().atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()

        // --- Cache Miss ---
        if (cachedProfile == null) {
            logger.info("[Cache MISS] for key: {}. Fetching full profile.", cacheKey)
            val freshProfile = riotApiService.fetchSummonerProfile(puuid, region) ?: return null
            
            // Fetch ALL matches since start of year
            val allMatches = riotApiService.fetchAllMatchesSince(puuid, region, startOfYear)
            
            // Calculate Stats
            val (championStats, overallStats) = statsCalculator.calculateStats(allMatches, puuid)
            
            val completeProfile = freshProfile.copy(
                gameName = summonerName,
                tagLine = tagLine,
                recentMatches = allMatches,
                championStats = championStats,
                overallStats = overallStats,
                totalMatches = allMatches.size,
                lastRefreshed = java.time.Instant.now()
            )
            redisCacheService.set(cacheKey, completeProfile, Duration.ZERO) // Store indefinitely
            return completeProfile
        }

        // --- Cache Hit: "Always Check" Update Strategy ---
        var needsUpdate = false
        var updatedProfile = cachedProfile

        // 1. Check for new matches played since the last cached game.
        val lastKnownGameTimestamp = cachedProfile.recentMatches?.firstOrNull()?.info?.gameCreation ?: 0
        val startTimeForApi = (lastKnownGameTimestamp / 1000) + 1 // +1 to avoid fetching the same game
        
        // If the cached profile is old (e.g. from before we added stats), we might need to re-fetch everything
        // But for now, let's assume we just append new matches.
        
        val potentialNewMatches = riotApiService.fetchNewMatches(puuid, region, startTimeForApi)
        val existingMatchIds = cachedProfile.recentMatches?.mapNotNull { it.info?.gameId }?.toSet() ?: emptySet()
        val trulyNewMatches = potentialNewMatches?.filterNot { existingMatchIds.contains(it.info?.gameId) }

        if (!trulyNewMatches.isNullOrEmpty()) {
            logger.info("[Cache STALE] Found {} new matches for key: {}", trulyNewMatches.size, cacheKey)
            val combinedMatches = trulyNewMatches + cachedProfile.recentMatches.orEmpty()
            
            // Re-calculate stats with new matches
            val (championStats, overallStats) = statsCalculator.calculateStats(combinedMatches, puuid)
            
            updatedProfile = updatedProfile.copy(
                recentMatches = combinedMatches,
                championStats = championStats,
                overallStats = overallStats,
                totalMatches = combinedMatches.size,
                lastRefreshed = java.time.Instant.now()
            )
            needsUpdate = true
        } else if (cachedProfile.championStats.isEmpty() && !cachedProfile.recentMatches.isNullOrEmpty()) {
             // If we have matches but no stats (migration case), calculate them
             val (championStats, overallStats) = statsCalculator.calculateStats(cachedProfile.recentMatches, puuid)
             updatedProfile = updatedProfile.copy(
                championStats = championStats,
                overallStats = overallStats
            )
            needsUpdate = true
        }

        // 2. Check for rank/LP changes
        val freshRank = riotApiService.fetchLeagueRank(puuid, region)
        if (freshRank != cachedProfile.soloQueueRank) {
            logger.info("[Cache STALE] Found rank update for key: {}", cacheKey)
            updatedProfile = updatedProfile.copy(soloQueueRank = freshRank)
            needsUpdate = true
        }

        // 3. Always update the name and tagline
        updatedProfile = updatedProfile.copy(gameName = summonerName, tagLine = tagLine)

        // 4. Write to the cache only if there was an actual change
        if (needsUpdate || updatedProfile.gameName != cachedProfile.gameName || updatedProfile.tagLine != cachedProfile.tagLine) {
            logger.info("[Cache WRITE] Saving updated profile for key: {}", cacheKey)
            redisCacheService.set(cacheKey, updatedProfile, Duration.ZERO)
        } else {
            logger.info("[Cache HIT] Profile for key: {} is up-to-date.", cacheKey)
        }

        return updatedProfile
    }

    fun getCachedProfile(puuid: String, region: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        return redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
    }
}
