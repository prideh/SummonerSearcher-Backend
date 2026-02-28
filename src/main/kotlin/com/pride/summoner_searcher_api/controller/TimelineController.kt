package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.TimelineAnalysisDto
import com.pride.summoner_searcher_api.service.PlayerCacheService
import com.pride.summoner_searcher_api.service.TimelineAnalysisService
import com.pride.summoner_searcher_api.service.TimelineService
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * Provides two endpoints for the Riot Timeline API:
 *  - GET /api/riot/timeline/{region}/{matchId}          — single timeline (cached 24h)
 *  - GET /api/riot/timeline/analyze/{region}/{puuid}    — analyze last 10 timelines for a player
 */
@RestController
@RequestMapping("/api/riot/timeline")
class TimelineController(
    private val timelineService: TimelineService,
    private val timelineAnalysisService: TimelineAnalysisService,
    private val playerCacheService: PlayerCacheService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Fetches a single match timeline, caching it in Redis for 24 hours.
     */
    @GetMapping("/{region}/{matchId}")
    fun getTimeline(
        @PathVariable region: String,
        @PathVariable matchId: String
    ): ResponseEntity<MatchTimelineDto?> = runBlocking {
        val timeline = timelineService.getTimeline(matchId, region)
        ResponseEntity.ok(timeline)
    }

    /**
     * Fetches and analyzes the last 10 ranked match timelines for a player.
     * Uses the cached match ID list from the player's profile (already stored by PlayerCacheService).
     */
    @GetMapping("/analyze/{region}/{puuid}")
    fun analyzeTimelines(
        @PathVariable region: String,
        @PathVariable puuid: String
    ): ResponseEntity<TimelineAnalysisDto?> = runBlocking {
        // 1. Get the most recent 10 ranked match IDs from cache
        val profile = playerCacheService.getCachedProfile(puuid, region)
        if (profile == null) {
            logger.warn("Timeline analysis requested but no cached profile for puuid={} region={}", puuid, region)
            return@runBlocking ResponseEntity.notFound().build()
        }

        val matchIds = (profile.allMatchIds ?: profile.recentMatches?.mapNotNull { it.metadata?.matchId })
            ?.take(20) ?: emptyList()

        if (matchIds.isEmpty()) {
            logger.warn("No match IDs found in cached profile for puuid={}", puuid)
            return@runBlocking ResponseEntity.ok(null)
        }

        // 2. Fetch timelines concurrently (each cached 24h)
        val timelines = timelineService.getTimelines(matchIds, region, 20)

        // 3. Get the corresponding MatchDtos (already in Redis from profile fetch)
        val matchDtos = playerCacheService.getMatchesByIds(matchIds, region)

        // 4. Zip timelines + matchDtos by matchId
        val analysisInput = timelines.mapNotNull { timeline ->
            val matchId = timeline.metadata?.matchId ?: return@mapNotNull null
            val matchDto = matchDtos.find { it.metadata?.matchId == matchId } ?: return@mapNotNull null
            Triple(matchId, timeline, matchDto)
        }

        logger.info("Analyzing {} match timelines for puuid={}", analysisInput.size, puuid)

        // 5. Run analysis
        val result = timelineAnalysisService.analyze(analysisInput, puuid)
        ResponseEntity.ok(result)
    }
}
