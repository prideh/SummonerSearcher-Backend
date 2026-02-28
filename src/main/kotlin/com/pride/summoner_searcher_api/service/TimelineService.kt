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
     */
    suspend fun getTimeline(matchId: String, region: String): MatchTimelineDto? {
        val cacheKey = getCacheKey(matchId, region)

        val cached = redisCacheService.get(cacheKey, MatchTimelineDto::class.java)
        if (cached != null) {
            logger.info("Timeline Cache HIT: {}", cacheKey)
            return cached
        }

        logger.info("Timeline Cache MISS: {} — fetching from Riot API", cacheKey)
        val timeline = riotApiService.getMatchTimeline(matchId, region)

        if (timeline != null) {
            redisCacheService.set(cacheKey, timeline, Duration.ofHours(24))
        }

        return timeline
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
