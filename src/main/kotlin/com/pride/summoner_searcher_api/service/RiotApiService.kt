package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.util.mapToRegionRouting
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient

/**
 * A service dedicated to interacting with the Riot Games API.
 * This class abstracts away the details of making HTTP requests and handles errors gracefully
 * by catching 404 Not Found exceptions and returning null instead of crashing.
 * 
 * All API calls are rate-limited using the RiotApiRateLimiter to prevent hitting Riot's rate limits.
 */
@Service
class RiotApiService(
    private val riotRestClient: RestClient,
    private val rateLimiter: RiotApiRateLimiter
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Wrapper function that enforces rate limiting for all Riot API calls.
     * 
     * Now supports Dynamic Rate Limiting by parsing response headers.
     */
    private suspend fun <T> executeRateLimitedCall(
        region: String, 
        priority: ApiPriority = ApiPriority.HIGH, 
        apiCall: () -> ResponseEntity<T>?
    ): T? {
        var attempts = 0
        val maxRetries = 3
        
        while (true) {
            rateLimiter.acquirePermit(region, priority)
            try {
                val response = withContext(Dispatchers.IO) {
                    apiCall()
                }
                
                if (response != null) {
                    val headers = response.headers
                    rateLimiter.updateLimits(
                        region,
                        headers.getFirst("X-App-Rate-Limit"),
                        headers.getFirst("X-App-Rate-Limit-Count")
                    )
                    return response.body
                } else {
                    return null
                }
            } catch (e: HttpClientErrorException.TooManyRequests) {
                attempts++
                
                // Update limits from the 429 response as well
                rateLimiter.updateLimits(
                    region,
                    e.responseHeaders?.getFirst("X-App-Rate-Limit"),
                    e.responseHeaders?.getFirst("X-App-Rate-Limit-Count")
                )

                if (attempts > maxRetries) {
                    logger.error("Exceeded max retries ($maxRetries) for rate limit. Giving up.")
                    throw e
                }
                
                // Parse Retry-After header (in seconds)
                val retryAfterSeconds = e.responseHeaders?.getFirst("Retry-After")?.toLongOrNull() ?: 1L
                val waitMs = (retryAfterSeconds * 1000) + 100 // Add 100ms buffer
                
                logger.warn("Hit 429 Rate Limit! Waiting {}ms before retry {}/{}.", waitMs, attempts, maxRetries)
                kotlinx.coroutines.delay(waitMs)
            } catch (e: org.springframework.web.client.ResourceAccessException) {
                attempts++
                if (attempts > maxRetries) {
                    logger.error("Exceeded max retries ($maxRetries) for network timeout. Giving up.")
                    throw e
                }
                val waitMs = 1000L * attempts // Exponential backoff: 1s, 2s, 3s
                logger.warn("Network timeout (ReadTimeout). Waiting {}ms before retry {}/{}.", waitMs, attempts, maxRetries)
                kotlinx.coroutines.delay(waitMs)
            }
        }
    }

    private suspend fun getAccountByPuuid(puuid: String, region: String, priority: ApiPriority = ApiPriority.HIGH): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$accountBaseUrl/riot/account/v1/accounts/by-puuid/{puuid}"
        return executeRateLimitedCall(region, priority) {
            try {
                riotRestClient.get()
                    .uri(uri, puuid)
                    .retrieve()
                    .toEntity(AccountDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getAccountByPuuid returned 404 for puuid: {}", puuid)
                null
            }
        }
    }

    suspend fun getAccountByRiotId(summonerName: String, tagLine: String, region: String): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val accountUri = "$accountBaseUrl/riot/account/v1/accounts/by-riot-id/{summonerName}/{tagLine}"
        return executeRateLimitedCall(region, ApiPriority.HIGH) {
            try {
                riotRestClient.get()
                    .uri(accountUri, summonerName, tagLine)
                    .retrieve()
                    .toEntity(AccountDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getAccountByRiotId returned 404 for {}#{}", summonerName, tagLine)
                null
            }
        }
    }

    suspend fun getSummonerByPuuid(puuid: String, region: String): SummonerDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val summonerUri = "$platformBaseUrl/lol/summoner/v4/summoners/by-puuid/{puuid}"
        return executeRateLimitedCall(region, ApiPriority.HIGH) {
            try {
                riotRestClient.get()
                    .uri(summonerUri, puuid)
                    .retrieve()
                    .toEntity(SummonerDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getSummonerByPuuid returned 404 for puuid {} in region {}", puuid, region)
                null
            }
        }
    }
    
    suspend fun fetchSummonerProfile(puuid: String, region: String): SummonerProfileDto? {
        val summonerDto = getSummonerByPuuid(puuid, region) ?: return null
        val soloQueueRank = fetchLeagueRank(puuid, region)

        return SummonerProfileDto(
            puuid = puuid,
            gameName = null,
            tagLine = null,
            summonerLevel = summonerDto.summonerLevel,
            profileIconId = summonerDto.profileIconId,
            soloQueueRank = soloQueueRank,
            recentMatches = null
        )
    }

    suspend fun fetchLeagueRank(puuid: String, region: String): LeagueEntryDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val leagueUri = "$platformBaseUrl/lol/league/v4/entries/by-puuid/{puuid}"
        val leagueEntriesType = object : ParameterizedTypeReference<List<LeagueEntryDto>>() {}
        val allLeagueEntries = executeRateLimitedCall(region, ApiPriority.HIGH) {
            try {
                riotRestClient.get()
                    .uri(leagueUri, puuid)
                    .retrieve()
                    .toEntity(leagueEntriesType)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.info("fetchLeagueRank returned 404 for puuid {} in region {}. Player is likely unranked.", puuid, region)
                null
            }
        }
        return allLeagueEntries?.find { it.queueType == "RANKED_SOLO_5x5" }
    }


    suspend fun fetchChallengerLeague(region: String, queue: String): LeagueListDTO? = coroutineScope {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/league/v4/challengerleagues/by-queue/{queue}"
        
        // Fetching the list itself is a background task but infrequent, can be HIGH or LOW. 
        // Let's keep it LOW since it's part of the warmer.
        val leagueList = executeRateLimitedCall(region, ApiPriority.LOW) {
            try {
                riotRestClient.get()
                    .uri(uri, queue)
                    .retrieve()
                    .toEntity(LeagueListDTO::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("fetchChallengerLeague returned 404 for region {} queue {}", region, queue)
                null
            }
        }
        
        if (leagueList == null) {
            return@coroutineScope null
        }

        val totalEntries = leagueList.entries.size
        logger.info("[Cache Warmer] $region: Starting enrichment for $totalEntries players...")

        // Process entries in parallel with rate limiting automatically handled by the rate limiter
        val enrichedEntries = leagueList.entries.mapIndexed { index, entry ->
            async {
                // Use LOW priority for cache warming to avoid blocking user requests
                val account = entry.puuid?.let { getAccountByPuuid(it, region, ApiPriority.LOW) }
                
                if ((index + 1) % 50 == 0) {
                    logger.info("[Cache Warmer] $region: Processed ${index + 1} / $totalEntries players...")
                }
                
                entry.copy(
                    gameName = account?.gameName,
                    tagLine = account?.tagLine
                )
            }
        }.awaitAll()

        logger.info("[Cache Warmer] $region: Enrichment complete for $totalEntries players")
        leagueList.copy(entries = enrichedEntries)
    }

    suspend fun getMatchIdsByPuuid(puuid: String, region: String, queueId: Int, startTime: Long? = null, endTime: Long? = null, count: Int, start: Int = 0): List<String>? {
        logger.info("API Request: getMatchIdsByPuuid - region=$region, queue=$queueId, startTime=$startTime, count=$count, start=$start")
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        
        val uriBuilder = StringBuilder("$matchBaseUrl/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queueId}&count={count}&start={start}")
        val uriVariables = mutableMapOf<String, Any>(
            "puuid" to puuid,
            "queueId" to queueId,
            "count" to count,
            "start" to start
        )

        if (startTime != null) {
            uriBuilder.append("&startTime={startTime}")
            uriVariables["startTime"] = startTime
        }
        if (endTime != null) {
            uriBuilder.append("&endTime={endTime}")
            uriVariables["endTime"] = endTime
        }

        val responseType = object : ParameterizedTypeReference<List<String>>() {}
        return executeRateLimitedCall(region) {
            try {
                riotRestClient.get()
                    .uri(uriBuilder.toString(), uriVariables)
                    .retrieve()
                    .toEntity(responseType)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getMatchIdsByPuuid returned 404")
                null
            }
        }

    }
    
    suspend fun fetchAllMatchesSince(puuid: String, region: String, startTime: Long? = null): List<MatchDto> = coroutineScope {
        val allMatchIds = mutableListOf<String>()
        var start = 0
        val count = 100
        
        while (true) {
            logger.info("Fetching matches for $puuid: start=$start, count=$count")
            val matchIds = getMatchIdsByPuuid(puuid, region, 420, startTime, null, count, start)
            
            if (matchIds.isNullOrEmpty()) {
                logger.info("Fetched empty list or null. Stopping. (Total: ${allMatchIds.size})")
                break
            }
            
            logger.info("Fetched ${matchIds.size} matches. (Total before add: ${allMatchIds.size})")
            allMatchIds.addAll(matchIds)
            
            if (matchIds.size < count) {
                logger.info("Fetched fewer than requested ($count). Reached end of history. (Total: ${allMatchIds.size})")
                break 
            }
            
            start += count
            // Safety break to prevent infinite loops if something goes wrong
            if (allMatchIds.size > 2000) {
                logger.warn("Hit safety limit of 2000 matches. Stopping.")
                break 
            }
        }
        
        logger.info("Found ${allMatchIds.size} match IDs since $startTime for $puuid")
        
        // EARLY TERMINATION WITH BATCH SAMPLING
        // Since Riot's startTime parameter is broken, we need to manually check timestamps.
        // To avoid fetching 800+ match details when only 3 are from 2026, we:
        // 1. Sample first 10 matches from each batch of 100 IDs
        // 2. If sample contains old matches, stop fetching entirely
        // 3. Otherwise, fetch the rest of the batch
        
        val validMatches = mutableListOf<MatchDto>()
        val sampleSize = 10
        
        if (startTime != null) {
            val startTimeMs = startTime * 1000
            logger.info("OPTIMIZATION: Using batch sampling with early termination (startTime=${startTime})")
            
            var currentIndex = 0
            
            while (currentIndex < allMatchIds.size) {
                val batchEnd = minOf(currentIndex + 100, allMatchIds.size)
                val batchIds = allMatchIds.subList(currentIndex, batchEnd)
                
                // Sample first matches of this batch
                val sampleIds = batchIds.take(sampleSize)
                logger.info("Sampling ${sampleIds.size} matches from batch starting at index $currentIndex")
                
                val sampleMatches = sampleIds.map { matchId ->
                    async { getMatchById(matchId, region) }
                }.awaitAll().filterNotNull()
                
                // Check if any sampled matches are old
                val hasOldMatches = sampleMatches.any { match ->
                    val gameCreation = match.info?.gameCreation ?: 0
                    gameCreation < startTimeMs
                }
                
                if (hasOldMatches) {
                    logger.warn("EARLY TERMINATION: Detected old matches in sample. Stopping at index $currentIndex (saved ${allMatchIds.size - currentIndex} API calls)")
                    
                    // Add only the valid matches from sample
                    sampleMatches.forEach { match ->
                        val gameCreation = match.info?.gameCreation ?: 0
                        if (gameCreation >= startTimeMs) {
                            validMatches.add(match)
                        }
                    }
                    break
                }
                
                // All samples are recent, add them to valid matches
                validMatches.addAll(sampleMatches)
                
                // Fetch remaining matches in this batch (if any)
                val remainingIds = batchIds.drop(sampleSize)
                if (remainingIds.isNotEmpty()) {
                    logger.info("Sample passed. Fetching ${remainingIds.size} remaining matches in batch")
                    val remainingMatches = remainingIds.map { matchId ->
                        async { getMatchById(matchId, region) }
                    }.awaitAll().filterNotNull()
                    
                    // Double-check these are also recent (defense in depth)
                    remainingMatches.forEach { match ->
                        val gameCreation = match.info?.gameCreation ?: 0
                        if (gameCreation >= startTimeMs) {
                            validMatches.add(match)
                        }
                    }
                }
                
                currentIndex = batchEnd
            }
            
            logger.info("Returning ${validMatches.size} valid matches after early termination optimization")
        } else {
            // No startTime filter, fetch all matches normally
            logger.info("No startTime specified. Fetching all ${allMatchIds.size} matches")
            val allMatches = allMatchIds.map { matchId ->
                async { getMatchById(matchId, region) }
            }.awaitAll().filterNotNull()
            validMatches.addAll(allMatches)
        }
        
        validMatches.toList()
    }

    suspend fun fetchMatchHistory(puuid: String, region: String, count: Int): List<MatchDto>? = coroutineScope {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, 0, null, count, 0) ?: return@coroutineScope emptyList()
        
        matchIds.map { matchId ->
            async {
                getMatchById(matchId, region)
            }
        }.awaitAll().filterNotNull()
    }

    suspend fun fetchNewMatches(puuid: String, region: String, startTime: Long): List<MatchDto>? = coroutineScope {
        val matchIds = getMatchIdsByPuuid(puuid, region, 420, startTime, null, 100, 0) ?: return@coroutineScope emptyList()
        
        val allMatches = matchIds.map { matchId ->
            async {
                getMatchById(matchId, region)
            }
        }.awaitAll().filterNotNull()
        
        // CLIENT-SIDE DATE FILTER: Riot's startTime parameter is broken.
        // Filter manually to only return matches newer than startTime.
        val startTimeMs = startTime * 1000
        val filtered = allMatches.filter { match ->
            (match.info?.gameCreation ?: 0) >= startTimeMs
        }
        
        if (filtered.size < allMatches.size) {
            logger.warn("fetchNewMatches: Filtered out ${allMatches.size - filtered.size} old matches (Riot API bug)")
        }
        
        filtered
    }

    suspend fun getMatchById(matchId: String, region: String): MatchDto? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}"
        
        return executeRateLimitedCall(region) {
            try {
                riotRestClient.get()
                    .uri(uri, matchId)
                    .retrieve()
                    .toEntity(MatchDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                // Don't throw an error for 404s on individual matches
                null
            }
        }
    }

    private fun getPreferredContent(translations: List<RiotContent>): String? {
        return translations.find { it.locale.equals("en_US", ignoreCase = true) }?.content
            ?: translations.firstOrNull()?.content
    }

    suspend fun getPlatformData(region: String): PlatformStatusDto? {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/status/v4/platform-data"
        return executeRateLimitedCall(region) {
            try {
                riotRestClient.get()
                    .uri(uri)
                    .retrieve()
                    .toEntity(RiotPlatformData::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getPlatformData returned 404 for region {}", region)
                null
            }
        }?.let {
            PlatformStatusDto(
                name = it.name,
                maintenances = it.maintenances.map { maintenance ->
                    StatusItemDto(
                        status = maintenance.maintenanceStatus,
                        severity = maintenance.incidentSeverity,
                        title = getPreferredContent(maintenance.titles) ?: "No Title",
                        description = maintenance.updates.firstOrNull()?.let { update -> getPreferredContent(update.translations) },
                        platforms = maintenance.platforms
                    )
                },
                incidents = it.incidents.map { incident ->
                    StatusItemDto(
                        status = if (incident.active) "Active" else "Resolved",
                        severity = incident.incidentSeverity,
                        title = getPreferredContent(incident.titles) ?: "No Title",
                        description = incident.updates.firstOrNull()?.let { update -> getPreferredContent(update.translations) },
                        platforms = incident.platforms
                    )
                }
            )
        }
    }

    suspend fun getActiveGameByPuuid(puuid: String, region: String): CurrentGameInfo? = coroutineScope {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val uri = "$platformBaseUrl/lol/spectator/v5/active-games/by-summoner/{puuid}"
        
        val gameInfo = executeRateLimitedCall(region) {
            try {
                riotRestClient.get()
                    .uri(uri, puuid)
                    .retrieve()
                    .toEntity(CurrentGameInfo::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                // 404 means the summoner is not in an active game
                null
            }
        } ?: return@coroutineScope null

        // Filter for Ranked Solo/Duo (Queue ID 420)
        if (gameInfo.gameQueueConfigId != 420L) {
            logger.info("Player $puuid is in game but not Ranked Solo (Queue ${gameInfo.gameQueueConfigId}). Returning null.")
            return@coroutineScope null
        }

        // Enrich participants with Rank (Tier/Division/LP)
        val enrichedParticipants = gameInfo.participants.map { participant ->
            async {
                // Skip bots or players without PUUID
                if (participant.puuid != null) {
                    try {
                        // Use PUUID to fetch rank, as summonerId is deprecated
                        val rank = fetchLeagueRank(participant.puuid, region)
                        participant.copy(
                            tier = rank?.tier,
                            rank = rank?.rank,
                            leaguePoints = rank?.leaguePoints,
                            wins = rank?.wins,
                            losses = rank?.losses
                        )
                    } catch (e: Exception) {
                        logger.error("Failed to fetch rank for participant ${participant.riotId}", e)
                        participant
                    }
                } else {
                    participant
                }
            }
        }.awaitAll()

        return@coroutineScope gameInfo.copy(participants = enrichedParticipants)
    }
}
