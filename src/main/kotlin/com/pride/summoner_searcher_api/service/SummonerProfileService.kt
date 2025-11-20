package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.springframework.stereotype.Service

@Service
class SummonerProfileService(
    private val riotApiService: RiotApiService,
    private val playerCacheService: PlayerCacheService
) {

    fun getSummonerProfile(region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        // Step 1: Get the global account PUUID.
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region)
        val puuid = accountDto?.puuid?.takeIf { it.isNotBlank() } ?: return null

        // Step 2: Verify that this PUUID has a summoner on the requested region.
        // If not, it's a valid global account but not on this server, so return "not found".
        riotApiService.getSummonerByPuuid(puuid, region) ?: return null

        // Only if both are successful, proceed to the caching layer.
        return playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)
    }
}
