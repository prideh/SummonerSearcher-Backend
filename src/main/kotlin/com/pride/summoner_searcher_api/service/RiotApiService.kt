package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.util.mapToRegionRouting
import io.github.resilience4j.ratelimiter.annotation.RateLimiter
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient
import org.springframework.web.client.body

/**
 * A service dedicated to interacting with the Riot Games API.
 * This class abstracts away the details of making HTTP requests, handles errors gracefully,
 * and manages API rate limiting using Resilience4J.
 *
 * The `open` keyword is required on the class and its public methods so that Spring AOP
 * can create a proxy to apply the @RateLimiter annotations.
 */
@Service
open class RiotApiService(
    private val riotRestClient: RestClient,
    private val riotMatchService: RiotMatchService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Fetches a player's global account details using their PUUID. Used by the challenger cache warmer.
     * This call is rate-limited.
     * @return An [AccountDto] containing the Riot ID, or null if not found.
     */
    @RateLimiter(name = "riot")
    open fun getAccountByPuuid(puuid: String, region: String): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$accountBaseUrl/riot/account/v1/accounts/by-puuid/{puuid}"
        return try {
            riotRestClient.get()
                .uri(uri, puuid)
                .retrieve()
                .body<AccountDto>()
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("getAccountByPuuid returned 404 for puuid: {}", puuid)
            null
        }
    }

    /**
     * Fetches a player's global account details using their Riot ID (game name + tagline).
     * This is the primary entry point for finding a player. This call is rate-limited.
     * @return An [AccountDto] containing the global PUUID, or null if not found.
     */
    @RateLimiter(name = "riot")
    open fun getAccountByRiotId(summonerName: String, tagLine: String, region: String): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val accountUri = "$accountBaseUrl/riot/account/v1/accounts/by-riot-id/{summonerName}/{tagLine}"
        return try {
            riotRestClient.get()
                .uri(accountUri, summonerName, tagLine)
                .retrieve()
                .body<AccountDto>()
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("getAccountByRiotId returned 404 for {}#{}", summonerName, tagLine)
            null
        }
    }

    /**
     * Fetches a player's summoner details (level, icon, etc.) for a specific region using their PUUID.
     * This is used to verify a player exists on a specific server. This call is rate-limited.
     * @return A [SummonerDto] if found on the specified region, otherwise null.
     */
    @RateLimiter(name = "riot")
    open fun getSummonerByPuuid(puuid: String, region: String): SummonerDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val summonerUri = "$platformBaseUrl/lol/summoner/v4/summoners/by-puuid/{puuid}"
        return try {
            riotRestClient.get()
                .uri(summonerUri, puuid)
                .retrieve()
                .body<SummonerDto>()
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("getSummonerByPuuid returned 404 for puuid {} in region {}", puuid, region)
            null
        }
    }
    
    /**
     * Assembles the initial, uncached summoner profile by fetching summoner data and rank data.
     * The underlying calls to get summoner and rank data are rate-limited.
     * @return A [SummonerProfileDto] ready to be cached, or null if the summoner doesn't exist.
     */
    open fun fetchSummonerProfile(puuid: String, region: String): SummonerProfileDto? {
        val summonerDto = getSummonerByPuuid(puuid, region) ?: return null
        val soloQueueRank = fetchLeagueRank(puuid, region)

        return SummonerProfileDto(
            puuid = puuid,
            gameName = null, // To be populated by the calling service
            tagLine = null,  // To be populated by the calling service
            summonerLevel = summonerDto.summonerLevel,
            profileIconId = summonerDto.profileIconId,
            soloQueueRank = soloQueueRank,
            recentMatches = null // To be populated by the calling service
        )
    }

    /**
     * Fetches a player's ranked information (specifically for Ranked Solo/Duo).
     * This call is rate-limited.
     * @return A [LeagueEntryDto] for the solo queue, or null if the player is unranked.
     */
    @RateLimiter(name = "riot")
    open fun fetchLeagueRank(puuid: String, region: String): LeagueEntryDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val leagueUri = "$platformBaseUrl/lol/league/v4/entries/by-puuid/{puuid}"
        val leagueEntriesType = object : ParameterizedTypeReference<List<LeagueEntryDto>>() {}
        val allLeagueEntries = try {
            riotRestClient.get()
                .uri(leagueUri, puuid)
                .retrieve()
                .body(leagueEntriesType)
        } catch (e: HttpClientErrorException.NotFound) {
            // This is a normal case for unranked players.
            logger.info("fetchLeagueRank returned 404 for puuid {} in region {}. Player is likely unranked.", puuid, region)
            null
        }
        return allLeagueEntries?.find { it.queueType == "RANKED_SOLO_5x5" }
    }

    /**
     * Fetches the list of challenger players for a given region and enriches it with their Riot IDs.
     * This is a slow process where each sub-call is rate-limited.
     * @return A [LeagueListDTO] containing the enriched list of challenger players.
     */
    @RateLimiter(name = "riot-long") // Apply the long-term rate limit to the initial call
    open fun fetchChallengerLeague(region: String, queue: String): LeagueListDTO? {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/league/v4/challengerleagues/by-queue/{queue}"
        val leagueList = try {
            riotRestClient.get()
                .uri(uri, queue)
                .retrieve()
                .body<LeagueListDTO>()
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("fetchChallengerLeague returned 404 for region {} queue {}", region, queue)
            return null
        }

        val totalEntries = leagueList?.entries?.size ?: 0
        val enrichedEntries = leagueList?.entries?.mapIndexed { index, entry ->
            if ((index + 1) % 50 == 0) {
                logger.info("[Cache Warmer] $region: Processed ${index + 1} / $totalEntries players...")
            }
            // Each call to getAccountByPuuid is individually rate-limited by the 'riot' limiter.
            val account = entry.puuid?.let { getAccountByPuuid(it, region) }
            entry.copy(
                gameName = account?.gameName,
                tagLine = account?.tagLine
            )
        } ?: listOf()

        return leagueList?.copy(entries = enrichedEntries)
    }

    /**
     * Fetches a list of match IDs for a given player. This call is rate-limited.
     * @return A list of match ID strings, or null if not found.
     */
    @RateLimiter(name = "riot")
    open fun getMatchIdsByPuuid(puuid: String, region: String, queueId: Int, startTime: Long, count: Int): List<String>? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queueId}&startTime={startTime}&count={count}"
        val responseType = object : ParameterizedTypeReference<List<String>>() {}
        return try {
            riotRestClient.get()
                .uri(uri, puuid, queueId, startTime, count)
                .retrieve()
                .body(responseType)
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("getMatchIdsByPuuid returned 404")
            null
        }
    }
    
    /**
     * Fetches a specified number of full match details for a player.
     * The underlying calls to get match IDs and match details are rate-limited.
     * @return A list of [MatchDto] objects.
     */
    open fun fetchMatchHistory(puuid: String, region: String, count: Int): List<MatchDto>? {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, 0, count) ?: return emptyList()
        return matchIds.mapNotNull { riotMatchService.getMatchById(it, region) }
    }

    /**
     * Fetches new matches for a player that have occurred after a given timestamp.
     * The underlying calls to get match IDs and match details are rate-limited.
     * @return A list of new [MatchDto] objects.
     */
    open fun fetchNewMatches(puuid: String, region: String, startTime: Long): List<MatchDto>? {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, startTime, 100) ?: return emptyList()
        return matchIds.mapNotNull { riotMatchService.getMatchById(it, region) }
    }

    /**
     * A helper to find the English translation from a list of translated content.
     * Falls back to the first available translation if English is not found.
     */
    private fun getPreferredContent(translations: List<RiotContent>): String? {
        return translations.find { it.locale.equals("en_US", ignoreCase = true) }?.content
            ?: translations.firstOrNull()?.content
    }

    /**
     * Fetches the live server status for a given region. This call is rate-limited.
     * @return A simplified [PlatformStatusDto] for the frontend.
     */
    @RateLimiter(name = "riot")
    open fun getPlatformData(region: String): PlatformStatusDto? {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/status/v4/platform-data"
        return try {
            riotRestClient.get()
                .uri(uri)
                .retrieve()
                .body<RiotPlatformData>()?.let {
                    PlatformStatusDto(
                        name = it.name,
                        maintenances = it.maintenances.map { maintenance ->
                            StatusItemDto(
                                status = maintenance.maintenanceStatus,
                                severity = maintenance.incidentSeverity,
                                title = getPreferredContent(maintenance.titles) ?: "No Title",
                                description = maintenance.updates.firstOrNull()?.let { update -> getPreferredContent(update.translations) },
                                platforms = maintenance.platforms
                            )
                        },
                        incidents = it.incidents.map { incident ->
                            StatusItemDto(
                                status = if (incident.active) "Active" else "Resolved",
                                severity = incident.incidentSeverity,
                                title = getPreferredContent(incident.titles) ?: "No Title",
                                description = incident.updates.firstOrNull()?.let { update -> getPreferredContent(update.translations) },
                                platforms = incident.platforms
                            )
                        }
                    )
                }
        } catch (e: HttpClientErrorException.NotFound) {
            logger.warn("getPlatformData returned 404 for region {}", region)
            null
        }
    }
}
