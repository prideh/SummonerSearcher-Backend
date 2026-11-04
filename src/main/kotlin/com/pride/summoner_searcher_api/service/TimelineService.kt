package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * Service responsible for fetching and caching match timelines.
 * Uses Redis with a 24-hour TTL for each timeline to minimize Riot API calls.
 */
@Service
class TimelineService(
    private val riotApiService: RiotApiService,
    private val redisCacheService: RedisCacheService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private fun getCacheKey(matchId: String, region: String) =
        "timeline:${region.lowercase()}:$matchId"

    /**
     * Fetches a single match timeline, using Redis cache with 24h TTL.
     * Lazily backfills cached timelines that predate CHAMPION_KILL bounty/shutdownBounty fields.
     */
    suspend fun getTimeline(matchId: String, region: String): MatchTimelineDto? {
        val cacheKey = getCacheKey(matchId, region)

        val cached = redisCacheService.get(cacheKey, MatchTimelineDto::class.java)
        if (cached != null && !needsTimelineBackfill(cached)) {
            logger.info("Timeline Cache HIT: {}", cacheKey)
            return cached
        }

        if (cached != null) {
            logger.info("Timeline Cache LEGACY (missing bounty fields): {} — lazily backfilling from Riot API", cacheKey)
        } else {
            logger.info("Timeline Cache MISS: {} — fetching from Riot API", cacheKey)
        }
        val timeline = riotApiService.getMatchTimeline(matchId, region)

        if (timeline != null) {
            redisCacheService.set(cacheKey, timeline, Duration.ofHours(24))
            return timeline
        }

        return cached
    }

    private fun needsTimelineBackfill(timeline: MatchTimelineDto): Boolean {
        val frames = timeline.info?.frames ?: return true
        val killEvents = frames.flatMap { it.events ?: emptyList() }.filter { it.type == "CHAMPION_KILL" }
        return killEvents.isNotEmpty() && killEvents.all { it.bounty == null && it.shutdownBounty == null }
    }

    /**
     * Fetches timelines for the given match IDs (up to [limit]) concurrently.
     * Each timeline is individually cached.
     */
    suspend fun getTimelines(matchIds: List<String>, region: String, limit: Int = 20): List<MatchTimelineDto> =
        coroutineScope {
            matchIds.take(limit)
                .map { matchId ->
                    async { getTimeline(matchId, region) }
                }
                .awaitAll()
                .filterNotNull()
        }
}
