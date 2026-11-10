package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.dto.CoachTimelinePreparationRequestDto
import com.pride.summoner_searcher_api.dto.CoachTimelinePreparationResultDto
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.TimelineAnalysisDto
import com.pride.summoner_searcher_api.service.PlayerCacheService
import com.pride.summoner_searcher_api.service.TimelineAnalysisService
import com.pride.summoner_searcher_api.service.TimelineService
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Provides bounded endpoints for the Riot Timeline API:
 *  - GET  /api/riot/timeline/{region}/{matchId}             — single timeline (bounded, cached 24h)
 *  - GET  /api/riot/timeline/analyze/{region}/{puuid}       — bounded sequential timeline analysis (legacy frontend contract)
 *  - POST /api/riot/timeline/prepare-coach/{region}/{puuid} — bounded on-demand coach timeline preparation (POST-only)
 */
@RestController
@RequestMapping("/api/riot/timeline")
class TimelineController(
    private val timelineService: TimelineService,
    private val timelineAnalysisService: TimelineAnalysisService,
    private val playerCacheService: PlayerCacheService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        private val VALID_REGION_REGEX = Regex("^[A-Za-z0-9]{2,8}$")
        private val VALID_MATCH_ID_REGEX = Regex("^[A-Za-z0-9_.-]{1,64}$")
        const val MAX_PUUID_LENGTH = 128
        const val MAX_REQUEST_MATCH_IDS = 200
    }

    private fun isValidRegion(region: String): Boolean =
        VALID_REGION_REGEX.matches(region.trim())

    private fun isValidPuuid(puuid: String): Boolean {
        val trimmed = puuid.trim()
        if (trimmed.isEmpty() || trimmed.length > MAX_PUUID_LENGTH) return false
        return trimmed.none { it.isWhitespace() || it.isISOControl() || it == '/' || it == '\\' }
    }

    private fun isValidMatchId(matchId: String): Boolean =
        VALID_MATCH_ID_REGEX.matches(matchId.trim())

    /**
     * Fetches a single match timeline, enforcing 350/360 MiB headroom, global concurrency cap,
     * and bounded Redis/HTTP GZIP inflation while caching valid timelines in Redis for 24 hours.
     */
    @GetMapping("/{region}/{matchId}")
    fun getTimeline(
        @PathVariable region: String,
        @PathVariable matchId: String
    ): ResponseEntity<MatchTimelineDto?> = runBlocking {
        if (!isValidRegion(region) || !isValidMatchId(matchId)) {
            return@runBlocking ResponseEntity.badRequest().build()
        }
        val normalizedRegion = region.trim().lowercase()
        val trimmedMatchId = matchId.trim()
        when (val outcome = timelineService.getTimelineForController(trimmedMatchId, normalizedRegion)) {
            is TimelineService.SingleTimelineOutcome.Success -> ResponseEntity.ok(outcome.timeline)
            is TimelineService.SingleTimelineOutcome.Blocked -> {
                val status = if (outcome.reason == "RATE_LIMIT_429" || outcome.reason == "RATE_LIMIT_COOLDOWN") {
                    HttpStatus.TOO_MANY_REQUESTS
                } else {
                    HttpStatus.SERVICE_UNAVAILABLE
                }
                ResponseEntity.status(status).build()
            }
        }
    }

    /**
     * Bounded on-demand coach timeline preparation (POST-only; never exposed via GET).
     * Enforces 350 MiB admission, warm compressed cache, singleflight coalescing, and global
     * 2-slot concurrency cap BEFORE `SummonerProfileDto` decode.
     */
    @PostMapping("/prepare-coach/{region}/{puuid}")
    fun prepareCoachTimelinesPost(
        @PathVariable region: String,
        @PathVariable puuid: String,
        @RequestBody(required = false) request: CoachTimelinePreparationRequestDto? = null
    ): ResponseEntity<CoachTimelinePreparationResultDto> = runBlocking {
        if (!isValidRegion(region) || !isValidPuuid(puuid)) {
            return@runBlocking ResponseEntity.badRequest().build()
        }
        val rawRequestedIds = request?.matchIds
        if (rawRequestedIds != null && rawRequestedIds.size > MAX_REQUEST_MATCH_IDS) {
            return@runBlocking ResponseEntity.badRequest().build()
        }

        val normalizedRegion = region.trim().lowercase()
        val trimmedPuuid = puuid.trim()

        when (
            val outcome = timelineService.prepareCoachTimelinesForPlayer(
                region = normalizedRegion,
                puuid = trimmedPuuid,
                requestedMatchIds = rawRequestedIds
            ) {
                playerCacheService.getCachedProfile(trimmedPuuid, normalizedRegion)
            }
        ) {
            is TimelineService.PlayerPreparationOutcome.ProfileNotFound -> {
                logger.warn("Coach timeline preparation requested but no cached profile for puuid={} region={}", trimmedPuuid, normalizedRegion)
                ResponseEntity.notFound().build()
            }
            is TimelineService.PlayerPreparationOutcome.Ready -> {
                ResponseEntity.ok(outcome.result)
            }
        }
    }

    /**
     * Legacy frontend timeline analysis endpoint (`GET /api/riot/timeline/analyze/{region}/{puuid}`).
     * Routes through the same pre-decode admission, singleflight, global concurrency cap, and
     * sequential single-player compaction pipeline without 20-way timeline fanout or `getMatchesByIds`,
     * while returning non-2xx (`429` / `503`) on transient blocks so `SummonerInfo.tsx` does not lock
     * empty state into React memory.
     */
    @GetMapping("/analyze/{region}/{puuid}")
    fun analyzeTimelines(
        @PathVariable region: String,
        @PathVariable puuid: String
    ): ResponseEntity<TimelineAnalysisDto?> = runBlocking {
        if (!isValidRegion(region) || !isValidPuuid(puuid)) {
            return@runBlocking ResponseEntity.badRequest().build()
        }
        val normalizedRegion = region.trim().lowercase()
        val trimmedPuuid = puuid.trim()

        when (
            val outcome = timelineService.prepareCoachTimelinesForPlayer(
                region = normalizedRegion,
                puuid = trimmedPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = true
            ) {
                playerCacheService.getCachedProfile(trimmedPuuid, normalizedRegion)
            }
        ) {
            is TimelineService.PlayerPreparationOutcome.ProfileNotFound -> {
                logger.warn("Timeline analysis requested but no cached profile for puuid={} region={}", trimmedPuuid, normalizedRegion)
                ResponseEntity.notFound().build()
            }
            is TimelineService.PlayerPreparationOutcome.Ready -> {
                val prep = outcome.result
                when {
                    prep.analysis != null -> ResponseEntity.ok(prep.analysis)
                    prep.stopReason == "NO_MATCHES_AVAILABLE" -> ResponseEntity.ok(null)
                    outcome.nonTransientEmptyAnalysis != null -> ResponseEntity.ok(outcome.nonTransientEmptyAnalysis)
                    prep.stopReason == "RATE_LIMIT_429" || prep.stopReason == "RATE_LIMIT_COOLDOWN" ->
                        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build()
                    else ->
                        ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build()
                }
            }
        }
    }
}
