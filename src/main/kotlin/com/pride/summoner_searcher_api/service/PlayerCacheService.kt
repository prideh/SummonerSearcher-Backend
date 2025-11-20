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

    private fun getProfileCacheKey(puuid: String, region: String) = "player:profile:$region:$puuid"

    fun getPlayerProfile(puuid: String, region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

        // Handle Cache Miss (first time ever seeing this player on this region)
        if (cachedProfile == null) {
            logger.info("[Cache MISS] for key: {}. Fetching full profile.", cacheKey)
            val freshProfile = riotApiService.fetchSummonerProfile(puuid, region) ?: return null
            val initialMatches = riotApiService.fetchMatchHistory(puuid, region, 20)
            val completeProfile = freshProfile.copy(
                gameName = summonerName,
                tagLine = tagLine,
                recentMatches = initialMatches
            )
            redisCacheService.set(cacheKey, completeProfile, Duration.ZERO)
            return completeProfile
        }

        // Handle Cache Hit: Always check for fresh data
        var needsUpdate = false
        var updatedProfile = cachedProfile

        // 1. Check for new matches
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

        // 2. Check for rank/LP changes
        val freshRank = riotApiService.fetchLeagueRank(puuid, region)
        if (freshRank != cachedProfile.soloQueueRank) {
            logger.info("[Cache STALE] Found rank update for key: {}", cacheKey)
            updatedProfile = updatedProfile.copy(soloQueueRank = freshRank)
            needsUpdate = true
        }

        // 3. Update name and tagline (in case they changed)
        updatedProfile = updatedProfile.copy(gameName = summonerName, tagLine = tagLine)

        // 4. Save to cache if any part of the profile was updated
        if (needsUpdate || updatedProfile.gameName != cachedProfile.gameName || updatedProfile.tagLine != cachedProfile.tagLine) {
            logger.info("[Cache WRITE] Saving updated profile for key: {}", cacheKey)
            redisCacheService.set(cacheKey, updatedProfile, Duration.ZERO)
        } else {
            logger.info("[Cache HIT] Profile for key: {} is up-to-date.", cacheKey)
        }

        return updatedProfile
    }
}
