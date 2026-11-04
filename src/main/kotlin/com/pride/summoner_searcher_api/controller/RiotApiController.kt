package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.dto.CurrentGameInfo
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
    private val summonerProfileService: SummonerProfileService,
    private val redisCacheService: com.pride.summoner_searcher_api.service.RedisCacheService,
    private val cohortBenchmarkService: com.pride.summoner_searcher_api.service.CohortBenchmarkService? = null,
    private val overnightCohortCollectorRunner: com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner? = null
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
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "false") refresh: Boolean = false,
        @CurrentUser user: User?
    ): ResponseEntity<SummonerProfileDto?> = runBlocking {
        val summonerProfile = summonerProfileService.getSummonerProfile(
            region = region,
            summonerName = summonerName,
            tagLine = tagLine,
            explicitRefresh = refresh
        )

        // If the summoner was found and we have a logged-in user, add the search to the user's recent search history.
        if (summonerProfile != null && user != null) {
            val searchQuery = "$summonerName#$tagLine"
            userService.addRecentSearch(user, searchQuery, region)
        }

        ResponseEntity.ok(summonerProfile)
    }

    /**
     * Fetches a specific page of matches for a summoner.
     * @param region The server region.
     * @param puuid The player's PUUID.
     * @param page The page number (1-indexed).
     * @return A list of [MatchDto] for that page.
     */
    @GetMapping("/matches/{region}/{puuid}")
    fun getMatches(
        @PathVariable region: String,
        @PathVariable puuid: String,
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "1") page: Int
    ): ResponseEntity<List<com.pride.summoner_searcher_api.dto.MatchDto>> = runBlocking {
        val matches = summonerProfileService.getMatches(region, puuid, page)
        ResponseEntity.ok(matches)
    }
    /**
     * Fetches the live game for a summoner.
     * Caches the result for 5 minutes.
     */
    @GetMapping("/live-game/{region}/{puuid}")
    fun getLiveGame(
        @PathVariable region: String,
        @PathVariable puuid: String
    ): ResponseEntity<CurrentGameInfo?> = runBlocking {
        val cacheKey = "live-game:$region:$puuid"
        val cachedGame = redisCacheService.get(cacheKey, CurrentGameInfo::class.java)

        if (cachedGame != null) {
            org.slf4j.LoggerFactory.getLogger(javaClass).info("Live Game Cache HIT for $puuid in $region")
            return@runBlocking ResponseEntity.ok(cachedGame)
        }

        org.slf4j.LoggerFactory.getLogger(javaClass).info("Live Game Cache MISS for $puuid in $region. Fetching from API...")
        val liveGame = riotApiService.getActiveGameByPuuid(puuid, region)

        if (liveGame != null) {
            redisCacheService.set(cacheKey, liveGame, java.time.Duration.ofMinutes(10))
        }

        ResponseEntity.ok(liveGame)
    }

    /**
     * Returns the cached or recompute-guarded cohort extraction + rate-limit verification report for [region]
     * (zero Riot API calls, bounded scans, no unguarded public recomputation).
     */
    @GetMapping("/cohorts/{region}/report")
    fun getCohortExtractionReport(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null
    ): ResponseEntity<com.pride.summoner_searcher_api.dto.CohortExtractionReportDto?> {
        val report = cohortBenchmarkService?.getPublicCohortReport(
            region = region,
            targetPatch = patch
        )
        return ResponseEntity.ok(report)
    }

    /**
     * Returns persisted de-identified cohort benchmark cells for [region] and optional [patch].
     */
    @GetMapping("/cohorts/{region}")
    fun getCohortBenchmarks(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null
    ): ResponseEntity<List<com.pride.summoner_searcher_api.dto.CohortBenchmarkCellDto>> {
        val cells = cohortBenchmarkService?.getPersistedCohortCells(region, patch) ?: emptyList()
        return ResponseEntity.ok(cells)
    }

    /**
     * Guarded same-process status endpoint for the LOW-priority cohort collector.
     * Reads live in-memory [com.pride.summoner_searcher_api.service.RiotApiRateLimiter] verification + single checkpoint key.
     */
    @GetMapping("/cohorts/{region}/collector/status")
    fun getCohortCollectorStatus(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null,
        @org.springframework.web.bind.annotation.RequestHeader(name = "X-Cohort-Operator-Token", required = false) operatorToken: String? = null,
        httpRequest: jakarta.servlet.http.HttpServletRequest
    ): ResponseEntity<com.pride.summoner_searcher_api.dto.CohortCollectorStatusDto?> {
        val runner = overnightCohortCollectorRunner ?: return ResponseEntity.notFound().build()
        if (!isAuthorizedCollectorOperator(runner, httpRequest, operatorToken)) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.FORBIDDEN).build()
        }
        return ResponseEntity.ok(runner.getStatus(region = region, targetPatch = patch))
    }

    /**
     * Guarded same-process bounded diagnostic probe for the LOW-priority cohort collector.
     * Uses a cached seed (`euw1` by default) to perform at most 1 LOW-priority call per missing required endpoint
     * (`league-v4-entries-by-puuid`, `match-v5-ids-by-puuid`, `match-v5-match-by-id`) so actual Riot
     * `X-App-Rate-Limit` and `X-Method-Rate-Limit` headers can be observed BEFORE enabling bulk network collection.
     */
    @org.springframework.web.bind.annotation.RequestMapping(
        value = ["/cohorts/{region}/collector/probe"],
        method = [org.springframework.web.bind.annotation.RequestMethod.POST, org.springframework.web.bind.annotation.RequestMethod.GET]
    )
    fun probeCohortCollectorRateLimits(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null,
        @org.springframework.web.bind.annotation.RequestParam(required = false) seedPuuid: String? = null,
        @org.springframework.web.bind.annotation.RequestParam(required = false) seedMatchId: String? = null,
        @org.springframework.web.bind.annotation.RequestHeader(name = "X-Cohort-Operator-Token", required = false) operatorToken: String? = null,
        httpRequest: jakarta.servlet.http.HttpServletRequest
    ): ResponseEntity<com.pride.summoner_searcher_api.dto.CohortRateLimitProbeResultDto?> {
        val runner = overnightCohortCollectorRunner ?: return ResponseEntity.notFound().build()
        if (!isAuthorizedCollectorOperator(runner, httpRequest, operatorToken)) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.FORBIDDEN).build()
        }
        val probeResult = runner.runDiagnosticProbeGuarded(
            region = region,
            targetPatch = patch,
            seedPuuid = seedPuuid,
            seedMatchId = seedMatchId
        )
        return ResponseEntity.ok(probeResult)
    }

    /**
     * Guarded same-process start/resume endpoint for the LOW-priority cohort collector.
     * Executes inside the live backend JVM sharing [com.pride.summoner_searcher_api.service.RiotApiRateLimiter]
     * so foreground user requests and verified rate-limit headers are shared in-process.
     */
    @org.springframework.web.bind.annotation.RequestMapping(
        value = ["/cohorts/{region}/collector/start"],
        method = [org.springframework.web.bind.annotation.RequestMethod.POST, org.springframework.web.bind.annotation.RequestMethod.GET]
    )
    fun startOrResumeCohortCollector(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null,
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "20") maxBatches: Int = 20,
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "25") batchSize: Int = 25,
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "2500") pauseDelayMs: Long = 2_500L,
        @org.springframework.web.bind.annotation.RequestParam(defaultValue = "true") async: Boolean = true,
        @org.springframework.web.bind.annotation.RequestHeader(name = "X-Cohort-Operator-Token", required = false) operatorToken: String? = null,
        httpRequest: jakarta.servlet.http.HttpServletRequest
    ): ResponseEntity<com.pride.summoner_searcher_api.dto.CohortCollectorStatusDto?> {
        val runner = overnightCohortCollectorRunner ?: return ResponseEntity.notFound().build()
        if (!isAuthorizedCollectorOperator(runner, httpRequest, operatorToken)) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.FORBIDDEN).build()
        }
        val status = runner.startOrResumeGuarded(
            region = region,
            targetPatch = patch,
            maxBatches = maxBatches,
            batchSize = batchSize,
            pauseDelayMs = pauseDelayMs,
            async = async
        )
        return ResponseEntity.ok(status)
    }

    /**
     * Guarded same-process stop endpoint for the LOW-priority cohort collector.
     */
    @org.springframework.web.bind.annotation.RequestMapping(
        value = ["/cohorts/{region}/collector/stop"],
        method = [org.springframework.web.bind.annotation.RequestMethod.POST, org.springframework.web.bind.annotation.RequestMethod.GET]
    )
    fun stopCohortCollector(
        @PathVariable region: String,
        @org.springframework.web.bind.annotation.RequestParam(required = false) patch: String? = null,
        @org.springframework.web.bind.annotation.RequestHeader(name = "X-Cohort-Operator-Token", required = false) operatorToken: String? = null,
        httpRequest: jakarta.servlet.http.HttpServletRequest
    ): ResponseEntity<com.pride.summoner_searcher_api.dto.CohortCollectorStatusDto?> {
        val runner = overnightCohortCollectorRunner ?: return ResponseEntity.notFound().build()
        if (!isAuthorizedCollectorOperator(runner, httpRequest, operatorToken)) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.FORBIDDEN).build()
        }
        return ResponseEntity.ok(runner.requestStop(region = region, targetPatch = patch))
    }

    private fun isAuthorizedCollectorOperator(
        runner: com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner,
        httpRequest: jakarta.servlet.http.HttpServletRequest,
        operatorToken: String?
    ): Boolean {
        return runner.isAuthorizedOperatorRequest(
            remoteAddr = httpRequest.remoteAddr,
            forwardedFor = httpRequest.getHeader("X-Forwarded-For"),
            forwarded = httpRequest.getHeader("Forwarded"),
            realIp = httpRequest.getHeader("X-Real-IP"),
            providedToken = operatorToken
        )
    }
}


