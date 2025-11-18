package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.dto.LeagueListDTO
import com.pride.summoner_searcher_api.dto.PlatformStatusDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import com.pride.summoner_searcher_api.service.RiotApiService
import com.pride.summoner_searcher_api.service.SummonerProfileService
import com.pride.summoner_searcher_api.service.UserService
import org.springframework.http.ResponseEntity
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/riot")
class RiotApiController(
    private val riotApiService: RiotApiService,
    private val challengerLeagueService: ChallengerLeagueService,
    private val userService: UserService,
    private val summonerProfileService: SummonerProfileService
) {

    @GetMapping("/leaderboards/challenger/{region}")
    fun getChallengerLeague(@PathVariable region: String): ResponseEntity<LeagueListDTO> {
        val queue = "RANKED_SOLO_5x5"
        val challengerLeague = challengerLeagueService.getChallengerLeagueFromCache(region, queue)
        return ResponseEntity.ok(challengerLeague)
    }

    @GetMapping("/status/{region}")
    fun getPlatformData(@PathVariable region: String): ResponseEntity<PlatformStatusDto> {
        val platformData = riotApiService.getPlatformData(region)
        return ResponseEntity.ok(platformData)
    }

    @GetMapping("/summoner/{region}/{summonerName}/{tagLine}")
    fun getSummonerProfileByRiotId(
        @PathVariable region: String,
        @PathVariable summonerName: String,
        @PathVariable tagLine: String
    ): ResponseEntity<SummonerProfileDto> {
        // This now calls the correct service, which handles the PUUID lookup and caching
        val summonerProfile = summonerProfileService.getSummonerProfile(region, summonerName, tagLine)

        // If the summoner is found, save it to the user's recent searches
        if (summonerProfile != null) {
            val authentication = SecurityContextHolder.getContext().authentication
            val userEmail = authentication.name
            userService.findByEmail(userEmail)?.let {
                val searchQuery = "$summonerName#$tagLine"
                userService.addRecentSearch(it, searchQuery)
            }
        }

        return ResponseEntity.ok(summonerProfile)
    }
}
