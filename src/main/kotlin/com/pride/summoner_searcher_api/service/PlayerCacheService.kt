package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import java.time.Duration

/**
 * A service for managing the caching of player profiles.
 * This service acts as an intelligent layer between the frontend request and the Riot API,
 * using Redis to store and retrieve player data to minimize API calls.
 */
@Service
class PlayerCacheService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Generates a unique key for storing a player's profile in Redis.
     */
    private fun getProfileCacheKey(puuid: String, region: String) = "player:profile:${region.lowercase()}:$puuid"
    
    private fun getMatchCacheKey(matchId: String, region: String) = "match:${region.lowercase()}:$matchId"

    /**
     * Retrieves a player's profile, utilizing a cache-aside strategy with an "always check" update mechanism.
     * To prevent OOM, it processes matches in chunks and only stores the latest 20 in the profile.
     */
    suspend fun getPlayerProfile(puuid: String, region: String, summonerName: String, tagLine: String): SummonerProfileDto? = kotlinx.coroutines.coroutineScope {
        val cacheKey = getProfileCacheKey(puuid, region)
        val lockKey = "lock:profile:${region.lowercase()}:$puuid"

        // Try to acquire lock
        val acquired = redisCacheService.setIfAbsent(lockKey, "LOCKED", Duration.ofSeconds(15))

        if (!acquired) {
            logger.info("Profile refresh already in progress for $summonerName. Waiting/Returning cached.")
            // Wait briefly for the other request to finish (handling the double-request-on-first-load case)
            repeat(5) {
                val cached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
                if (cached != null && (cached.lastRefreshed?.isAfter(java.time.Instant.now().minusSeconds(10)) == true)) {
                    logger.info("Cache hit (during lock wait) for $summonerName")
                    return@coroutineScope cached
                }
                kotlinx.coroutines.delay(500)
            }
            // If still nothing or stale, return whatever is in cache (graceful degradation)
            return@coroutineScope redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
        }

        try {
            val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

            if (cachedProfile != null) {
                logger.info("Cache hit for $summonerName")
            } else {
                logger.info("Cache miss for $summonerName - Fetching from API")
            }

            // Calculate Start of Year (Jan 1st) in Epoch Seconds
            val startOfYear = java.time.Year.now().atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()

            // 1. Fetch current rank (fast, lightweight)
            val freshRank = riotApiService.fetchLeagueRank(puuid, region)
            
            // 2. Fetch ALL match IDs since start of year (pagination handled in service)
            val allMatchIds = riotApiService.fetchMatchIdsSince(puuid, region, startOfYear)
            
            // 3. Process matches in chunks to calculate stats without holding everything in memory
            val accumulator = MatchStatsAccumulator(puuid)
            val recentMatches = mutableListOf<com.pride.summoner_searcher_api.dto.MatchDto>()
            val validMatchIds = mutableListOf<String>()
            val startOfYearMs = startOfYear * 1000
            
            // We need to re-process ALL matches to ensure stats are accurate.
            // Thanks to Redis, this is fast for existing matches.
            val chunkSize = 50
            val chunks = allMatchIds.chunked(chunkSize)
            
            var processedCount = 0
            
            for (chunk in chunks) {
                // Bulk fetch from Redis
                val redisKeys = chunk.map { getMatchCacheKey(it, region) }
                val cachedMatchesMap = redisCacheService.multiGet(redisKeys, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
                
                // Identify missing matches
                val missingIds = chunk.filter { cachedMatchesMap[getMatchCacheKey(it, region)] == null }
                
                if (missingIds.isNotEmpty()) {
                    logger.info("Chunk ${processedCount / chunkSize + 1}/${chunks.size}: Fetching ${missingIds.size} missing matches from API")
                }
                
                // Fetch missing from API concurrently
                val fetchedMatches = missingIds.map { matchId ->
                    async { 
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()
                
                // Cache newly fetched matches - ONLY if they are valid (this year)
                // This prevents polluting Redis with old matches that Riot returns due to the bug.
                fetchedMatches.forEach { match ->
                     val matchId = match.metadata?.matchId
                     val creation = match.info?.gameCreation ?: 0
                     if (matchId != null && creation >= startOfYearMs) {
                         redisCacheService.set(getMatchCacheKey(matchId, region), match, Duration.ofDays(60))
                     }
                }
                
                // Combine cached and fetched matches for this chunk
                val chunkMatches = chunk.mapNotNull { id ->
                    cachedMatchesMap[getMatchCacheKey(id, region)] ?: fetchedMatches.find { it.metadata?.matchId == id }
                }
                
                // FILTER: Client-side work around for Riot API bug ignoring startTime
                // We only process matches that are actually from this year.
                // Since match IDs are chronological (newest first), if we find an old match, we stop processing.
                val validChunkMatches = chunkMatches.filter { (it.info?.gameCreation ?: 0) >= startOfYearMs }
                
                validChunkMatches.forEach { match -> 
                    match.metadata?.matchId?.let { validMatchIds.add(it) }
                }

                // Feed to accumulator
                validChunkMatches.forEach { accumulator.add(it) }
                
                // Keep the first 20 matches for the initial profile view
                if (recentMatches.size < 20) {
                    // We take as many as we need to fill up to 20
                    val needed = 20 - recentMatches.size
                    recentMatches.addAll(validChunkMatches.take(needed))
                }
                
                processedCount += chunk.size
                
                // OPTIMIZATION: If we filtered out any matches in this chunk, or if the chunk was empty to begin with (but valid IDs existed),
                // it means we hit the time boundary. Stop processing further chunks.
                if (validChunkMatches.size < chunkMatches.size) {
                    logger.info("Hit match time boundary (start of year). Stopping fetch early.")
                    break
                }
            }
            
            val (championStats, overallStats) = accumulator.getResult()
            
            val updatedProfile = SummonerProfileDto(
                puuid = puuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = cachedProfile?.summonerLevel ?: riotApiService.getSummonerByPuuid(puuid, region)?.summonerLevel, // Fallback if needed
                profileIconId = cachedProfile?.profileIconId ?: riotApiService.getSummonerByPuuid(puuid, region)?.profileIconId,
                soloQueueRank = freshRank,
                recentMatches = recentMatches, // Only 20 matches
                allMatchIds = validMatchIds,   // Only valid matches from this year
                championStats = championStats,
                overallStats = overallStats,
                totalMatches = validMatchIds.size,
                lastRefreshed = java.time.Instant.now()
            )
            
            redisCacheService.set(cacheKey, updatedProfile, Duration.ofDays(30)) // Store for 30 days
            logger.info("Refreshed profile for $summonerName. Analyzed ${validMatchIds.size} valid matches (out of ${allMatchIds.size} fetched IDs).")
            
            return@coroutineScope updatedProfile

        } finally {
            redisCacheService.delete(lockKey)
        }
    }

    /**
     * Retrieves a cached profile directly. Used for pagination.
     */
    fun getCachedProfile(puuid: String, region: String): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        return redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
    }

    suspend fun getMatchesByIds(matchIds: List<String>, region: String): List<com.pride.summoner_searcher_api.dto.MatchDto> = kotlinx.coroutines.coroutineScope {
        if (matchIds.isEmpty()) return@coroutineScope emptyList()
        
        val redisKeys = matchIds.map { getMatchCacheKey(it, region) }
        val cachedMatchesMap = redisCacheService.multiGet(redisKeys, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
        
        val missingIds = matchIds.filter { cachedMatchesMap[getMatchCacheKey(it, region)] == null }
        
        val fetchedMatches = missingIds.map { matchId ->
            async { riotApiService.getMatchById(matchId, region) }
        }.awaitAll().filterNotNull()
        
        fetchedMatches.forEach { match ->
             val mid = match.metadata?.matchId
             if (mid != null) {
                 redisCacheService.set(getMatchCacheKey(mid, region), match, Duration.ofDays(60))
             }
        }
        
        matchIds.mapNotNull { id ->
            cachedMatchesMap[getMatchCacheKey(id, region)] ?: fetchedMatches.find { it.metadata?.matchId == id }
        }
    }
}
