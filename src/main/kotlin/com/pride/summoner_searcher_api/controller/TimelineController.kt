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
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * Provides endpoints for the Riot Timeline API:
 *  - GET  /api/riot/timeline/{region}/{matchId}               — single timeline (cached 24h)
 *  - GET  /api/riot/timeline/analyze/{region}/{puuid}         — analyze last 20 timelines for a player
 *  - POST /api/riot/timeline/prepare-coach/{region}/{puuid}   — bounded sequential on-demand coach timeline prep
 *  - GET  /api/riot/timeline/prepare-coach/{region}/{puuid}   — bounded sequential on-demand coach timeline prep
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
     * Bounded on-demand timeline preparation when AI Coach opens.
     * Strictly uses at most 20 current cached profile match IDs (never hydrating 883-match seasonal history),
     * reuses already-loaded profile.recentMatches where available, and delegates to [TimelineService.prepareCoachTimelines].
     */
    @PostMapping("/prepare-coach/{region}/{puuid}")
    fun prepareCoachTimelinesPost(
        @PathVariable region: String,
        @PathVariable puuid: String,
        @RequestBody(required = false) request: CoachTimelinePreparationRequestDto?
    ): ResponseEntity<CoachTimelinePreparationResultDto> =
        prepareCoachTimelinesInternal(region, puuid, request?.matchIds)

    @GetMapping("/prepare-coach/{region}/{puuid}")
    fun prepareCoachTimelinesGet(
        @PathVariable region: String,
        @PathVariable puuid: String,
        @RequestParam(required = false) matchIds: List<String>?
    ): ResponseEntity<CoachTimelinePreparationResultDto> =
        prepareCoachTimelinesInternal(region, puuid, matchIds)

    private fun prepareCoachTimelinesInternal(
        region: String,
        puuid: String,
        requestedMatchIds: List<String>?
    ): ResponseEntity<CoachTimelinePreparationResultDto> = runBlocking {
        val normalizedRegion = region.lowercase()
        val sanitizedRequestedIds = requestedMatchIds
            ?.map { it.trim() }
            ?.filter { it.isNotBlank() }
            ?.distinct()
            ?.take(TimelineService.MAX_COACH_TIMELINE_MATCHES)
            .orEmpty()

        // 1. Check 350 MiB admission headroom BEFORE decoding cached profile from Redis
        if (!timelineService.hasCoachAdmissionHeadroom()) {
            logger.warn(
                "Coach timeline preparation blocked at controller admission (350 MiB guard) before profile decode for puuid={} region={}",
                puuid,
                normalizedRegion
            )
            return@runBlocking ResponseEntity.ok(
                timelineService.buildAdmissionBlockedResult(
                    region = normalizedRegion,
                    puuid = puuid,
                    requestedMatchIds = sanitizedRequestedIds
                )
            )
        }

        val profile = playerCacheService.getCachedProfile(puuid, region)
        if (profile == null) {
            logger.warn("Coach timeline preparation requested but no cached profile for puuid={} region={}", puuid, region)
            return@runBlocking ResponseEntity.notFound().build()
        }

        val recentMatches = profile.recentMatches.orEmpty()
        val recentMatchesById = recentMatches
            .mapNotNull { m -> m.metadata?.matchId?.trim()?.takeIf { it.isNotBlank() }?.let { it to m } }
            .toMap()

        val profileMatchIds = (
            profile.allMatchIds
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }
                ?.distinct()
                ?.takeIf { it.isNotEmpty() }
                ?: recentMatches.mapNotNull { it.metadata?.matchId?.trim()?.takeIf { id -> id.isNotBlank() } }.distinct()
            ).take(TimelineService.MAX_COACH_TIMELINE_MATCHES)

        val allowedSet = profileMatchIds.toSet()
        val effectiveMatchIds = if (sanitizedRequestedIds.isNotEmpty()) {
            sanitizedRequestedIds
                .filter { it in allowedSet }
                .take(TimelineService.MAX_COACH_TIMELINE_MATCHES)
                .ifEmpty { profileMatchIds }
        } else {
            profileMatchIds
        }

        // 2. Use ONLY current cached MatchDtos already attached to profile.recentMatches.
        // Never invoke playerCacheService.getMatchesByIds (which can multiGet, launch async Riot fanout, or write DB indexes).
        val orderedMatchDtos = effectiveMatchIds.mapNotNull { id -> recentMatchesById[id] }

        val result = timelineService.prepareCoachTimelines(
            region = normalizedRegion,
            puuid = puuid,
            requestedMatchIds = effectiveMatchIds,
            matchDtos = orderedMatchDtos
        )
        ResponseEntity.ok(result)
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

        // 5. Run analysis (passing all matchDtos so missing timelines are recorded in exclusions while match-level stats are preserved)
        val result = timelineAnalysisService.analyze(analysisInput, puuid, matchDtos)
        ResponseEntity.ok(result)
    }
}
