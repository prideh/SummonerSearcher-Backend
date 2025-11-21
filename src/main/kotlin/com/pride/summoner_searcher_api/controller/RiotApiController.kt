package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import com.pride.summoner_searcher_api.dto.PlatformStatusDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import com.pride.summoner_searcher_api.service.RiotApiService
import com.pride.summoner_searcher_api.service.SummonerProfileService
import com.pride.summoner_searcher_api.service.UserService
import kotlinx.coroutines.runBlocking
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Controller for handling all public-facing requests related to the Riot Games API.
 * All endpoints in this controller require authentication.
 */
@RestController
@RequestMapping("/api/riot")
class RiotApiController(
    private val riotApiService: RiotApiService,
    private val challengerLeagueService: ChallengerLeagueService,
    private val userService: UserService,
    private val summonerProfileService: SummonerProfileService
) {

    /**
     * Fetches the challenger leaderboard for a given region.
     * This data is read directly from the cache and is populated by a background job.
     * @param region The server region for the leaderboard (e.g., "euw1", "na1").
     * @return A [LeagueListDTO] containing the leaderboard, or null if not found in cache.
     */
    @GetMapping("/leaderboards/challenger/{region}")
    fun getChallengerLeague(@PathVariable region: String): ResponseEntity<LeagueListDTO?> {
        val queue = "RANKED_SOLO_5x5"
        val challengerLeague = challengerLeagueService.getChallengerLeagueFromCache(region, queue)
        return ResponseEntity.ok(challengerLeague)
    }

    /**
     * Fetches the current server status for a given region from the Riot API.
     * @param region The server region for the status check (e.g., "euw1", "na1").
     * @return A [PlatformStatusDto] containing the simplified status, or null if not found.
     */
    @GetMapping("/status/{region}")
    fun getPlatformData(@PathVariable region: String): ResponseEntity<PlatformStatusDto?> = runBlocking {
        val platformData = riotApiService.getPlatformData(region)
        ResponseEntity.ok(platformData)
    }

    /**
     * The main endpoint for searching for a summoner by their Riot ID (game name + tagline).
     * It orchestrates the two-step verification and caching logic handled by the [SummonerProfileService].
     * If the search is successful, it also adds the query to the logged-in user's recent searches.
     * @param region The server region to search on.
     * @param summonerName The game name part of the Riot ID.
     * @param tagLine The tag line part of the Riot ID.
     * @param user The currently authenticated user, injected by our custom [@CurrentUser] annotation.
     * @return A [SummonerProfileDto] if the summoner is found, or null if they are not.
     */
    @GetMapping("/summoner/{region}/{summonerName}/{tagLine}")
    fun getSummonerProfileByRiotId(
        @PathVariable region: String,
        @PathVariable summonerName: String,
        @PathVariable tagLine: String,
        @CurrentUser user: User
    ): ResponseEntity<SummonerProfileDto?> = runBlocking {
        val summonerProfile = summonerProfileService.getSummonerProfile(region, summonerName, tagLine)

        // If the summoner was found, add the search to the user's recent search history.
        if (summonerProfile != null) {
            val searchQuery = "$summonerName#$tagLine"
            userService.addRecentSearch(user, searchQuery)
        }

        ResponseEntity.ok(summonerProfile)
    }
}
