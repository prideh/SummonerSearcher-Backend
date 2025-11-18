package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.exception.RiotApiStatusException
import com.pride.summoner_searcher_api.util.mapToRegionRouting
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.client.body

@Service
class RiotMatchService(
    private val riotRestClient: RestClient
) {

    fun getMatchById(matchId: String, region: String): MatchDto? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}"
        return riotRestClient.get()
            .uri(uri, matchId)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { _, response ->
                if (response.statusCode.value() == 429) {
                    println("!!! RIOT API RATE LIMIT EXCEEDED (429) on URI: $uri !!!")
                }
                // Don't throw an error for 404s on individual matches
                if (response.statusCode.value() == 404) {
                    return@onStatus
                }
                throw RiotApiStatusException(response.statusCode, response.body.readBytes().toString(Charsets.UTF_8), uri)
            }
            .body<MatchDto>()
    }
}
