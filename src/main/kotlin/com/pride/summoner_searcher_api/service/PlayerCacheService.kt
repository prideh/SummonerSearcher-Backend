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
    private val riotApiService: RiotApiService
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

        // --- Cache Miss ---
        // If the player is not in the cache, fetch their full profile and initial match history for the first time.
        if (cachedProfile == null) {
            logger.info("[Cache MISS] for key: {}. Fetching full profile.", cacheKey)
            val freshProfile = riotApiService.fetchSummonerProfile(puuid, region) ?: return null
            val initialMatches = riotApiService.fetchMatchHistory(puuid, region, 10)
            val completeProfile = freshProfile.copy(
                gameName = summonerName,
                tagLine = tagLine,
                recentMatches = initialMatches
            )
            redisCacheService.set(cacheKey, completeProfile, Duration.ZERO) // Store indefinitely
            return completeProfile
        }

        // --- Cache Hit: "Always Check" Update Strategy ---
        // If a profile is found in the cache, always check for fresh data to ensure information is up-to-date.
        var needsUpdate = false
        var updatedProfile = cachedProfile

        // 1. Check for new matches played since the last cached game.
        val lastKnownGameTimestamp = cachedProfile.recentMatches?.firstOrNull()?.info?.gameCreation ?: 0
        val startTimeForApi = lastKnownGameTimestamp / 1000
        val potentialNewMatches = riotApiService.fetchNewMatches(puuid, region, startTimeForApi)
        val existingMatchIds = cachedProfile.recentMatches?.mapNotNull { it.info?.gameId }?.toSet() ?: emptySet()
        val trulyNewMatches = potentialNewMatches?.filterNot { existingMatchIds.contains(it.info?.gameId) }

        if (!trulyNewMatches.isNullOrEmpty()) {
            logger.info("[Cache STALE] Found {} new matches for key: {}", trulyNewMatches.size, cacheKey)
            val combinedMatches = trulyNewMatches + cachedProfile.recentMatches.orEmpty()
            updatedProfile = updatedProfile.copy(recentMatches = combinedMatches)
            needsUpdate = true
        }

        // 2. Check for rank/LP changes (e.g., from dodging or decay).
        val freshRank = riotApiService.fetchLeagueRank(puuid, region)
        if (freshRank != cachedProfile.soloQueueRank) {
            logger.info("[Cache STALE] Found rank update for key: {}", cacheKey)
            updatedProfile = updatedProfile.copy(soloQueueRank = freshRank)
            needsUpdate = true
        }

        // 3. Always update the name and tagline in case they have changed.
        updatedProfile = updatedProfile.copy(gameName = summonerName, tagLine = tagLine)

        // 4. Write to the cache only if there was an actual change to the data.
        if (needsUpdate || updatedProfile.gameName != cachedProfile.gameName || updatedProfile.tagLine != cachedProfile.tagLine) {
            logger.info("[Cache WRITE] Saving updated profile for key: {}", cacheKey)
            redisCacheService.set(cacheKey, updatedProfile, Duration.ZERO)
        } else {
            logger.info("[Cache HIT] Profile for key: {} is up-to-date.", cacheKey)
        }

        return updatedProfile
    }
}
