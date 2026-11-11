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
    suspend fun getSummonerProfile(
        region: String,
        summonerName: String,
        tagLine: String,
        explicitRefresh: Boolean = false
    ): SummonerProfileDto? {
        // Fast-path: serve ready fresh/stale/cooldown-guarded cached profiles without a blocking Account-V1 RTT.
        val immediate = playerCacheService.resolveImmediateCachedProfileByRiotId(
            region = region,
            summonerName = summonerName,
            tagLine = tagLine,
            explicitRefresh = explicitRefresh
        )
        if (immediate.served) {
            return immediate.profile?.let { profile ->
                profile.copy(recentMatches = profile.recentMatches?.take(20))
            }
        }

        // Step 1: Get the global account PUUID from the Riot ID when cold, refreshing past cooldown, or past 30m owner-auth window.
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region)
        val puuid = accountDto?.puuid?.takeIf { it.isNotBlank() } ?: run {
            playerCacheService.evictRiotIdMapping(region, summonerName, tagLine)
            return null
        }
        playerCacheService.reconcileVerifiedRiotIdOwner(region, summonerName, tagLine, puuid)
        val effectiveName = accountDto.gameName?.takeIf { it.isNotBlank() } ?: summonerName
        val effectiveTag = accountDto.tagLine?.takeIf { it.isNotBlank() } ?: tagLine

        // Step 2: Get full profile from cache (or refresh if missing / explicitly requested)
        val fullProfile = playerCacheService.getPlayerProfile(
            puuid = puuid,
            region = region,
            summonerName = effectiveName,
            tagLine = effectiveTag,
            explicitRefresh = explicitRefresh
        ) ?: return null

        // Only Account-V1-authenticated lookups record/advance ownerVerifiedAtMs (never unauthenticated cache hits).
        playerCacheService.recordVerifiedRiotIdOwner(
            region = region,
            queriedName = summonerName,
            queriedTag = tagLine,
            canonicalName = effectiveName,
            canonicalTag = effectiveTag,
            verifiedPuuid = puuid
        )

        // Step 3: Return profile with only the first page of matches (e.g., 20)
        // We keep the stats as they are calculated from the full history.
        return fullProfile.copy(
            recentMatches = fullProfile.recentMatches?.take(20)
        )
    }

    suspend fun getMatches(region: String, puuid: String, page: Int, pageSize: Int = 20): List<com.pride.summoner_searcher_api.dto.MatchDto> {
        val fullProfile = playerCacheService.getCachedProfile(puuid, region) ?: return emptyList()
        
        val allMatchIds = fullProfile.allMatchIds
        if (allMatchIds.isEmpty()) return emptyList()

        val start = (page - 1) * pageSize
        val end = start + pageSize
        
        if (start >= allMatchIds.size) return emptyList()
        
        val pageMatchIds = allMatchIds.subList(start, minOf(end, allMatchIds.size))
        
        // Use the bulk fetch method in PlayerCacheService
        return playerCacheService.getMatchesByIds(pageMatchIds, region)
    }
}
