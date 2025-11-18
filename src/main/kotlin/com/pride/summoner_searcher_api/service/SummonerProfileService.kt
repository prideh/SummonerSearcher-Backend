package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.springframework.stereotype.Service

@Service
class SummonerProfileService(
    private val riotApiService: RiotApiService,
    private val playerCacheService: PlayerCacheService
) {

    fun getSummonerProfile(region: String, summonerName: String, tagLine: String): SummonerProfileDto? {
        // This call is not cached, as a user can change their Riot ID.
        // It's the entry point to get the permanent PUUID.
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region) ?: return null
        val puuid = accountDto.puuid!!

        // All subsequent calls use the PUUID and are handled by the intelligent cache service.
        return playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)
    }
}
