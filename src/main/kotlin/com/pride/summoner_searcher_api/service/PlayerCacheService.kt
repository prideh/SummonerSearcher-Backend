package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class PlayerCacheService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private fun getProfileCacheKey(puuid: String) = "player:profile:$puuid"

    fun getPlayerProfile(puuid: String, region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid)

        // 1. Try to get the profile from the cache
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

        // 2. Handle Cache Miss (first time ever seeing this player)
        if (cachedProfile == null || cachedProfile.recentMatches.isNullOrEmpty()) {
            logger.info("[Cache MISS] for {}. Fetching full profile and initial 20 matches...", puuid)
            val freshProfile = riotApiService.fetchSummonerProfile(puuid, region)
            val initialMatches = riotApiService.fetchMatchHistory(puuid, region, 20)

            val completeProfile = freshProfile?.copy(
                gameName = summonerName,
                tagLine = tagLine,
                recentMatches = initialMatches
            )

            if (completeProfile != null) {
                redisCacheService.set(cacheKey, completeProfile, Duration.ZERO)
            }
            return completeProfile
        }

        // 3. Handle Cache Hit: Perform the "Smart Update"
        val lastKnownGameTimestamp = cachedProfile.recentMatches.first().info?.gameCreation ?: 0
        val startTimeForApi = lastKnownGameTimestamp / 1000

        val potentialNewMatches = riotApiService.fetchNewMatches(puuid, region, startTimeForApi)

        // Filter out any matches we already have in the cache to find the truly new ones.
        val existingMatchIds = cachedProfile.recentMatches.mapNotNull { it.info?.gameId }.toSet()
        val trulyNewMatches = potentialNewMatches?.filterNot { existingMatchIds.contains(it.info?.gameId) }

        // 3a. If there are no new matches, return the cached data.
        if (trulyNewMatches.isNullOrEmpty()) {
            logger.info("[Cache HIT] for {}. No new matches found.", puuid)
            return cachedProfile.copy(gameName = summonerName, tagLine = tagLine)
        }

        // 3b. If there are new matches, prepend them and update the cache.
        logger.info("[Cache UPDATE] for {}. Found {} new matches. Prepending to list.", puuid, trulyNewMatches.size)
        val combinedMatches = trulyNewMatches + cachedProfile.recentMatches

        val updatedProfile = cachedProfile.copy(
            gameName = summonerName,
            tagLine = tagLine,
            recentMatches = combinedMatches
        )

        redisCacheService.set(cacheKey, updatedProfile, Duration.ZERO)

        return updatedProfile
    }
}
