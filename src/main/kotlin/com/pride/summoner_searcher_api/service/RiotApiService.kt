package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.exception.RiotApiStatusException
import com.pride.summoner_searcher_api.util.mapToRegionRouting
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.client.body

@Service
class RiotApiService(
    private val riotRestClient: RestClient,
    private val riotMatchService: RiotMatchService
) {

    private fun getAccountByPuuid(puuid: String, region: String): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$accountBaseUrl/riot/account/v1/accounts/by-puuid/{puuid}"
        return riotRestClient.get()
            .uri(uri, puuid)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $uri !!!")
                }
                if (response.statusCode.value() == 404) {
                    return@onStatus
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), uri)
            }
            .body<AccountDto>()
    }

    fun getAccountByRiotId(summonerName: String, tagLine: String, region: String): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val accountUri = "$accountBaseUrl/riot/account/v1/accounts/by-riot-id/{summonerName}/{tagLine}"
        return riotRestClient.get()
            .uri(accountUri, summonerName, tagLine)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $accountUri !!!")
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), accountUri)
            }
            .body<AccountDto>()
    }

    fun fetchChallengerLeague(region: String, queue: String): LeagueListDTO? {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/league/v4/challengerleagues/by-queue/{queue}"
        val leagueList = riotRestClient.get()
            .uri(uri, queue)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $uri !!!")
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), uri)
            }
            .body<LeagueListDTO>()

        val totalEntries = leagueList?.entries?.size ?: 0
        val enrichedEntries = leagueList?.entries?.mapIndexed { index, entry ->
            if ((index + 1) % 50 == 0) {
                println("[Cache Warmer] $region: Processed ${index + 1} / $totalEntries players...")
            }
            Thread.sleep(1300)
            val account = entry.puuid?.let { getAccountByPuuid(it, region) }
            entry.copy(
                gameName = account?.gameName,
                tagLine = account?.tagLine
            )
        } ?: listOf()

        return leagueList?.copy(entries = enrichedEntries)
    }

    fun getPlatformData(region: String): PlatformStatusDto? {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/status/v4/platform-data"
        val riotPlatformData = riotRestClient.get()
            .uri(uri)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $uri !!!")
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), uri)
            }
            .body<RiotPlatformData>()

        return riotPlatformData?.let {
            PlatformStatusDto(
                name = it.name,
                maintenances = it.maintenances.map {
                    StatusItemDto(
                        status = it.maintenanceStatus,
                        severity = it.incidentSeverity,
                        title = it.titles.firstOrNull()?.content ?: "No Title",
                        platforms = it.platforms
                    )
                },
                incidents = it.incidents.map {
                    StatusItemDto(
                        status = if (it.active) "Active" else "Resolved",
                        severity = it.incidentSeverity,
                        title = it.titles.firstOrNull()?.content ?: "No Title",
                        platforms = it.platforms
                    )
                }
            )
        }
    }

    fun getMatchIdsByPuuid(puuid: String, region: String, queueId: Int, startTime: Long, count: Int): List<String>? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queueId}&startTime={startTime}&count={count}"
        val responseType = object : ParameterizedTypeReference<List<String>>() {}
        return riotRestClient.get()
            .uri(uri, puuid, queueId, startTime, count)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $uri !!!")
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), uri)
            }
            .body(responseType)
    }

    fun fetchSummonerProfile(puuid: String, region: String): SummonerProfileDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"

        val summonerUri = "$platformBaseUrl/lol/summoner/v4/summoners/by-puuid/{puuid}"
        val summonerDto = riotRestClient.get()
            .uri(summonerUri, puuid)
            .retrieve()
            .body<SummonerDto>()

        val leagueUri = "$platformBaseUrl/lol/league/v4/entries/by-puuid/{puuid}"
        val leagueEntriesType = object : ParameterizedTypeReference<List<LeagueEntryDto>>() {}
        val allLeagueEntries = riotRestClient.get()
            .uri(leagueUri, puuid)
            .retrieve()
            .body(leagueEntriesType)

        val soloQueueRank = allLeagueEntries?.find { it.queueType == "RANKED_SOLO_5x5" }

        return SummonerProfileDto(
            puuid = puuid,
            gameName = null, // This will be populated by the caching service
            tagLine = null, // This will be populated by the caching service
            summonerLevel = summonerDto?.summonerLevel,
            profileIconId = summonerDto?.profileIconId,
            soloQueueRank = soloQueueRank,
            recentMatches = null // Match history is handled separately
        )
    }

    fun fetchMatchHistory(puuid: String, region: String, count: Int): List<MatchDto>? {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, 0, count) ?: listOf()

        return matchIds.mapNotNull { matchId ->
            riotMatchService.getMatchById(matchId, region)
        }
    }

    fun fetchNewMatches(puuid: String, region: String, startTime: Long): List<MatchDto>? {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, startTime, 100) ?: listOf()

        return matchIds.mapNotNull { matchId ->
            riotMatchService.getMatchById(matchId, region)
        }
    }
}
