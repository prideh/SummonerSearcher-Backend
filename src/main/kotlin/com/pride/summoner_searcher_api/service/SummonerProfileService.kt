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
    fun getSummonerProfile(region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        // Step 1: Get the global account PUUID from the Riot ID. This call is not cached.
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region)
        val puuid = accountDto?.puuid?.takeIf { it.isNotBlank() } ?: return null

        // Step 2: Crucially, verify that this global PUUID has a summoner profile on the requested region.
        // This prevents returning data from the wrong server if a Riot ID exists globally but not on the searched region.
        riotApiService.getSummonerByPuuid(puuid, region) ?: return null

        // Step 3: Only if both checks pass, proceed to the caching layer to get the full profile.
        return playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)
    }
}
