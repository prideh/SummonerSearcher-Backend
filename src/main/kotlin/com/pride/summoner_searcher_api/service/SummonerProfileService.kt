package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.springframework.stereotype.Service

/**
 * The main service for handling summoner profile requests.
 * This class acts as an orchestrator, coordinating between the Riot API service and the caching service.
 */
@Service
class SummonerProfileService(
    private val riotApiService: RiotApiService,
    private val playerCacheService: PlayerCacheService
) {

    /**
     * Retrieves a summoner's profile, ensuring they exist on the specified region before proceeding.
     * This method implements a crucial two-step verification to handle Riot's global account system correctly.
     *
     * @param region The server region to search for the summoner on.
     * @param summonerName The game name part of the Riot ID.
     * @param tagLine The tag line part of the Riot ID.
     * @return A [SummonerProfileDto] if the summoner is found on the specified region, otherwise null.
     */
    suspend fun getSummonerProfile(region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        // Step 1: Get the global account PUUID from the Riot ID. This call is not cached.
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region)
        val puuid = accountDto?.puuid?.takeIf { it.isNotBlank() } ?: return null

        // Step 2: Get full profile from cache (or fetch if missing)
        val fullProfile = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine) ?: return null
        
        // Step 3: Return profile with only the first page of matches (e.g., 20)
        // We keep the stats as they are calculated from the full history.
        return fullProfile.copy(
            recentMatches = fullProfile.recentMatches?.take(20)
        )
    }

    suspend fun getMatches(region: String, puuid: String, page: Int, pageSize: Int = 20): List<com.pride.summoner_searcher_api.dto.MatchDto> {
        // We need to retrieve the full profile to get the matches.
        // Since we don't have name/tag here, we might need a method in PlayerCacheService to get by PUUID only?
        // Or we can just rely on the cache key being PUUID based.
        // But PlayerCacheService.getPlayerProfile requires name/tag to update them.
        // Let's add a getProfileByPuuid method to PlayerCacheService or just pass empty strings if we know it's cached?
        // Better: Add getCachedProfile(puuid, region) to PlayerCacheService.
        
        // For now, let's assume we can't easily get it without name/tag unless we change PlayerCacheService.
        // But wait, the controller usually has access to name/tag from the request if it's a search.
        // But for "Load More", we might only send PUUID.
        
        // Let's add getCachedProfile to PlayerCacheService.
        val fullProfile = playerCacheService.getCachedProfile(puuid, region) ?: return emptyList()
        
        val start = (page - 1) * pageSize
        val end = start + pageSize
        
        val allMatches = fullProfile.recentMatches ?: emptyList()
        if (start >= allMatches.size) return emptyList()
        
        return allMatches.subList(start, minOf(end, allMatches.size))
    }
}
