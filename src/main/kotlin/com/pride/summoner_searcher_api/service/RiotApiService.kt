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
class RiotApiService @org.springframework.beans.factory.annotation.Autowired constructor(
    @org.springframework.beans.factory.annotation.Qualifier("riotRestClient")
    private val riotRestClient: RestClient,
    private val rateLimiter: RiotApiRateLimiter,
    @org.springframework.beans.factory.annotation.Qualifier("dataDragonRestClient")
    private val dataDragonRestClient: RestClient = RestClient.builder().build(),
    private val timeProvider: TimeProvider = SystemTimeProvider()
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val boundedHttpExecutor = java.util.concurrent.Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "riot-bounded-http").apply { isDaemon = true }
    }

    private suspend fun <T> executeBlockingCallWithDeadline(
        remainingBudgetMs: Long,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>?,
        apiCall: () -> ResponseEntity<T>?
    ): ResponseEntity<T>? {
        val future = java.util.concurrent.CompletableFuture<ResponseEntity<T>?>()
        val workerThreadRef = java.util.concurrent.atomic.AtomicReference<Thread?>(null)
        boundedHttpExecutor.execute {
            workerThreadRef.set(Thread.currentThread())
            try {
                if (!Thread.currentThread().isInterrupted) {
                    future.complete(apiCall())
                } else {
                    future.complete(null)
                }
            } catch (t: Throwable) {
                future.completeExceptionally(t)
            } finally {
                workerThreadRef.set(null)
                Thread.interrupted()
            }
        }
        return try {
            withContext(Dispatchers.IO) {
                future.get(remainingBudgetMs.coerceAtLeast(1L), java.util.concurrent.TimeUnit.MILLISECONDS)
            }
        } catch (e: java.util.concurrent.TimeoutException) {
            future.cancel(true)
            workerThreadRef.get()?.interrupt()
            failureReasonHolder?.set("DEADLINE_EXCEEDED")
            null
        } catch (e: java.util.concurrent.ExecutionException) {
            throw (e.cause ?: e)
        }
    }

    /**
     * Wrapper function that enforces rate limiting for all Riot API calls.
     * 
     * Now supports Dynamic Rate Limiting by parsing both App and Method response headers,
     * as well as optional deadline-budget bounding and fresh memory headroom checks after
     * rate-limiter waits and before HTTP execution.
     */
    private suspend fun <T> executeRateLimitedCall(
        region: String, 
        priority: ApiPriority = ApiPriority.HIGH, 
        endpointKey: String? = null,
        maxRetries: Int = 3,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null,
        suppressThrowOnZeroRetryFailure: Boolean = false,
        deadlineAtMs: Long? = null,
        headroomGuard: (() -> Boolean)? = null,
        apiCall: () -> ResponseEntity<T>?
    ): T? {
        var attempts = 0
        val effectiveMaxRetries = maxRetries.coerceAtLeast(0)
        if (priority == ApiPriority.HIGH) {
            rateLimiter.beginUserRequest(region)
        }
        try {
            while (true) {
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }
                val waitStartMs = timeProvider.now()
                if (deadlineAtMs != null) {
                    val remainingForPermitMs = deadlineAtMs - waitStartMs
                    if (remainingForPermitMs <= 0L) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    val acquired = rateLimiter.tryAcquirePermit(region, priority, endpointKey, remainingForPermitMs)
                    if (!acquired) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                } else {
                    if (priority == ApiPriority.LOW) {
                        rateLimiter.awaitLowPriorityClearance(region)
                    }
                    rateLimiter.acquirePermit(region, priority, endpointKey)
                }
                val afterWaitMs = timeProvider.now()
                val limiterWaitMs = afterWaitMs - waitStartMs
                if (limiterWaitMs > 25L) {
                    logger.info(
                        "Riot rate limiter wait: region={}, endpoint={}, priority={}, limiterWaitMs={}",
                        region, endpointKey ?: "unspecified", priority, limiterWaitMs
                    )
                }
                if (deadlineAtMs != null && afterWaitMs >= deadlineAtMs) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                    return null
                }
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }
                try {
                    httpAttemptCounter?.incrementAndGet()
                    val httpStartMs = timeProvider.now()
                    val response = if (deadlineAtMs != null) {
                        val remainingForHttpMs = deadlineAtMs - httpStartMs
                        if (remainingForHttpMs <= 0L) {
                            failureReasonHolder?.set("DEADLINE_EXCEEDED")
                            return null
                        }
                        executeBlockingCallWithDeadline(remainingForHttpMs, failureReasonHolder, apiCall)
                    } else {
                        withContext(Dispatchers.IO) {
                            apiCall()
                        }
                    }
                    val httpMs = timeProvider.now() - httpStartMs
                    
                    if (response != null) {
                        val headers = response.headers
                        val appLimit = headers.getFirst("X-App-Rate-Limit")
                        val appCount = headers.getFirst("X-App-Rate-Limit-Count")
                        val methodLimit = headers.getFirst("X-Method-Rate-Limit")
                        val methodCount = headers.getFirst("X-Method-Rate-Limit-Count")
                        rateLimiter.updateLimits(
                            region = region,
                            limitHeader = appLimit,
                            countHeader = appCount,
                            endpointKey = endpointKey,
                            methodLimitHeader = methodLimit,
                            methodCountHeader = methodCount
                        )
                        logger.debug(
                            "Riot API call complete: region={}, endpoint={}, priority={}, limiterWaitMs={}, httpMs={}, appLimit={} ({}), methodLimit={} ({})",
                            region, endpointKey ?: "unspecified", priority, limiterWaitMs, httpMs,
                            appLimit ?: "n/a", appCount ?: "n/a", methodLimit ?: "n/a", methodCount ?: "n/a"
                        )
                        return response.body
                    } else {
                        return null
                    }
                } catch (e: HttpClientErrorException.TooManyRequests) {
                    attempts++
                    val rateLimitType = e.responseHeaders?.getFirst("X-Rate-Limit-Type")
                    failureReasonHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                    
                    // Update limits from the 429 response as well
                    rateLimiter.updateLimits(
                        region = region,
                        limitHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit"),
                        countHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit-Count"),
                        endpointKey = endpointKey,
                        methodLimitHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit"),
                        methodCountHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit-Count")
                    )

                    // Parse Retry-After header (in seconds) and record regional Retry-After window (including service 429s)
                    val retryAfterSeconds = e.responseHeaders?.getFirst("Retry-After")?.toLongOrNull()
                    val baseWaitMs = when {
                        retryAfterSeconds != null -> (retryAfterSeconds * 1000) + 100
                        rateLimitType.equals("service", ignoreCase = true) -> RiotApiRateLimiter.SERVICE_429_MIN_BACKOFF_MS
                        else -> 1_100L
                    }
                    rateLimiter.recordRetryAfter(region, baseWaitMs, rateLimitType)

                    if (attempts > effectiveMaxRetries) {
                        if (suppressThrowOnZeroRetryFailure && effectiveMaxRetries == 0) {
                            logger.warn(
                                "Hit 429 Rate Limit (type={}, endpoint={}) with maxRetries=0. Recorded Retry-After={}ms and stopping immediately without retry.",
                                rateLimitType ?: "n/a", endpointKey ?: "unspecified", baseWaitMs
                            )
                            return null
                        }
                        logger.error("Exceeded max retries ($effectiveMaxRetries) for rate limit (type=${rateLimitType ?: "n/a"}). Giving up.")
                        throw e
                    }
                    
                    logger.warn("Hit 429 Rate Limit (type={})! Waiting {}ms before retry {}/{}.", rateLimitType ?: "n/a", baseWaitMs, attempts, effectiveMaxRetries)
                    kotlinx.coroutines.delay(baseWaitMs)
                } catch (e: org.springframework.web.client.ResourceAccessException) {
                    attempts++
                    failureReasonHolder?.set("NETWORK_TIMEOUT")
                    if (attempts > effectiveMaxRetries) {
                        if (suppressThrowOnZeroRetryFailure && effectiveMaxRetries == 0) {
                            logger.warn(
                                "Network timeout on endpoint={} with maxRetries=0. Stopping immediately without retry.",
                                endpointKey ?: "unspecified"
                            )
                            return null
                        }
                        logger.error("Exceeded max retries ($effectiveMaxRetries) for network timeout. Giving up.")
                        throw e
                    }
                    val waitMs = 1000L * attempts // Exponential backoff: 1s, 2s, 3s
                    logger.warn("Network timeout (ReadTimeout). Waiting {}ms before retry {}/{}.", waitMs, attempts, effectiveMaxRetries)
                    kotlinx.coroutines.delay(waitMs)
                }
            }
        } finally {
            if (priority == ApiPriority.HIGH) {
                rateLimiter.endUserRequest(region)
            }
        }
    }

    private suspend fun getAccountByPuuid(puuid: String, region: String, priority: ApiPriority = ApiPriority.HIGH): AccountDto? {
        val regionalRouting = mapToRegionRouting(region)
        val accountBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$accountBaseUrl/riot/account/v1/accounts/by-puuid/{puuid}"
        return executeRateLimitedCall(region, priority, "account-v1-by-puuid") {
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
        return executeRateLimitedCall(region, ApiPriority.HIGH, "account-v1-by-riot-id") {
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
        return executeRateLimitedCall(region, ApiPriority.HIGH, "summoner-v4-by-puuid") {
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
        val allLeagueEntries = executeRateLimitedCall(region, ApiPriority.HIGH, "league-v4-entries-by-puuid") {
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

    suspend fun fetchLeagueRankLowPriority(
        puuid: String,
        region: String,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ): LeagueEntryDto? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val leagueUri = "$platformBaseUrl/lol/league/v4/entries/by-puuid/{puuid}"
        val leagueEntriesType = object : ParameterizedTypeReference<List<LeagueEntryDto>>() {}
        val allLeagueEntries = executeRateLimitedCall(
            region = region,
            priority = ApiPriority.LOW,
            endpointKey = "league-v4-entries-by-puuid",
            maxRetries = maxRetries,
            httpAttemptCounter = httpAttemptCounter,
            failureReasonHolder = failureReasonHolder,
            suppressThrowOnZeroRetryFailure = true
        ) {
            try {
                riotRestClient.get()
                    .uri(leagueUri, puuid)
                    .retrieve()
                    .toEntity(leagueEntriesType)
            } catch (e: HttpClientErrorException.NotFound) {
                null
            }
        }
        return allLeagueEntries?.find { it.queueType == "RANKED_SOLO_5x5" }
    }

    suspend fun fetchLeagueEntriesByTierDivisionLowPriority(
        region: String,
        queue: String = "RANKED_SOLO_5x5",
        tier: String,
        division: String = "I",
        page: Int = 1,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ): List<LeagueEntryDto>? {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val uri = "$platformBaseUrl/lol/league/v4/entries/{queue}/{tier}/{division}?page={page}"
        val uriVariables = mapOf(
            "queue" to queue,
            "tier" to tier.uppercase().trim(),
            "division" to division.uppercase().trim(),
            "page" to page.coerceAtLeast(1)
        )
        val responseType = object : ParameterizedTypeReference<List<LeagueEntryDto>>() {}
        return executeRateLimitedCall(
            region = region,
            priority = ApiPriority.LOW,
            endpointKey = "league-v4-entries-by-tier",
            maxRetries = maxRetries,
            httpAttemptCounter = httpAttemptCounter,
            failureReasonHolder = failureReasonHolder,
            suppressThrowOnZeroRetryFailure = true
        ) {
            try {
                riotRestClient.get()
                    .uri(uri, uriVariables)
                    .retrieve()
                    .toEntity(responseType)
            } catch (e: HttpClientErrorException.NotFound) {
                null
            }
        }
    }


    suspend fun fetchChallengerLeague(region: String, queue: String): LeagueListDTO? = coroutineScope {
        val baseUrl = "https://${region}.api.riotgames.com"
        val uri = "$baseUrl/lol/league/v4/challengerleagues/by-queue/{queue}"
        
        // Fetching the list itself is a background task but infrequent, can be HIGH or LOW. 
        // Let's keep it LOW since it's part of the warmer.
        val leagueList = executeRateLimitedCall(region, ApiPriority.LOW, "league-v4-challenger") {
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
        return executeRateLimitedCall(region, ApiPriority.HIGH, "match-v5-ids-by-puuid") {
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

    suspend fun getMatchIdsByPuuidLowPriority(
        puuid: String,
        region: String,
        queueId: Int = 420,
        count: Int = 20,
        start: Int = 0,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ): List<String>? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queueId}&count={count}&start={start}"
        val uriVariables = mapOf(
            "puuid" to puuid,
            "queueId" to queueId,
            "count" to count,
            "start" to start
        )
        val responseType = object : ParameterizedTypeReference<List<String>>() {}
        return executeRateLimitedCall(
            region = region,
            priority = ApiPriority.LOW,
            endpointKey = "match-v5-ids-by-puuid",
            maxRetries = maxRetries,
            httpAttemptCounter = httpAttemptCounter,
            failureReasonHolder = failureReasonHolder,
            suppressThrowOnZeroRetryFailure = true
        ) {
            try {
                riotRestClient.get()
                    .uri(uri, uriVariables)
                    .retrieve()
                    .toEntity(responseType)
            } catch (e: HttpClientErrorException.NotFound) {
                null
            }
        }
    }
    
    data class MatchIdsPaginationOutcome(
        val matchIds: List<String>,
        val pagesFetched: Int = 0,
        val exhaustedCleanly: Boolean = false,
        val hitSafetyCap: Boolean = false,
        val stoppedOnPageFailure: Boolean = false
    )

    /**
     * Paginates Ranked Solo (`queueId = 420`) match IDs since [startTime] while returning explicit
     * pagination completion metadata ([MatchIdsPaginationOutcome]):
     * - `matchIds == null` (or page fetch exception) sets `stoppedOnPageFailure = true` and `exhaustedCleanly = false`
     *   (never conflating a failed page with clean empty exhaustion).
     * - `matchIds.isEmpty()` or `matchIds.size < count` sets `exhaustedCleanly = true`.
     * - `allMatchIds.size > 2000` sets `hitSafetyCap = true` and `exhaustedCleanly = false`.
     */
    suspend fun fetchMatchIdsWithOutcomeSince(
        puuid: String,
        region: String,
        startTime: Long? = null
    ): MatchIdsPaginationOutcome = coroutineScope {
        val allMatchIds = mutableListOf<String>()
        var start = 0
        val count = 100
        var pagesFetched = 0
        var exhaustedCleanly = false
        var hitSafetyCap = false
        var stoppedOnPageFailure = false

        while (true) {
            logger.info("Fetching match IDs for $puuid: start=$start, count=$count")
            // We ask for queue 420 (Ranked Solo)
            val matchIds = try {
                getMatchIdsByPuuid(puuid, region, 420, startTime, null, count, start)
            } catch (e: kotlin.coroutines.cancellation.CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.warn("Error fetching match IDs page for {} ({}) at start={}: {}", puuid, region, start, e.message)
                null
            }

            if (matchIds == null) {
                stoppedOnPageFailure = true
                exhaustedCleanly = false
                logger.warn("Match ID pagination stopped on null/failed page for {} ({}) at start={}", puuid, region, start)
                break
            }

            pagesFetched++
            if (matchIds.isEmpty()) {
                exhaustedCleanly = true
                break
            }

            allMatchIds.addAll(matchIds)

            if (matchIds.size < count) {
                exhaustedCleanly = true
                break
            }

            start += count
            // Safety break
            if (allMatchIds.size > 2000) {
                hitSafetyCap = true
                exhaustedCleanly = false
                logger.warn("Hit safety limit of 2000 match IDs. Stopping.")
                break
            }
        }

        val outcome = MatchIdsPaginationOutcome(
            matchIds = allMatchIds.toList(),
            pagesFetched = pagesFetched,
            exhaustedCleanly = exhaustedCleanly,
            hitSafetyCap = hitSafetyCap,
            stoppedOnPageFailure = stoppedOnPageFailure
        )
        logger.info(
            "Found total ${allMatchIds.size} match IDs since $startTime for $puuid (pagesFetched=$pagesFetched, exhaustedCleanly=$exhaustedCleanly, hitSafetyCap=$hitSafetyCap, stoppedOnPageFailure=$stoppedOnPageFailure)"
        )
        outcome
    }

    /**
     * Fetches all match IDs since a given start time.
     */
    suspend fun fetchMatchIdsSince(puuid: String, region: String, startTime: Long? = null): List<String> =
        fetchMatchIdsWithOutcomeSince(puuid, region, startTime).matchIds

    // DEPRECATED: Use fetchMatchIdsSince + batch processing instead to avoid OOM
    suspend fun fetchAllMatchesSince(puuid: String, region: String, startTime: Long? = null): List<MatchDto> = coroutineScope {
        // This is kept only for backwards compatibility if needed, but should be avoided.
        // It now just calls fetchMatchIdsSince and fetches them all (WARNING: HIGH MEMORY USAGE)
        val ids = fetchMatchIdsSince(puuid, region, startTime)
        ids.map { 
             this@coroutineScope.async { getMatchById(it, region) } 
        }.awaitAll().filterNotNull()
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
        
        return executeRateLimitedCall(region, ApiPriority.HIGH, "match-v5-match-by-id") {
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

    suspend fun getMatchByIdLowPriority(
        matchId: String,
        region: String,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ): MatchDto? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}"

        return executeRateLimitedCall(
            region = region,
            priority = ApiPriority.LOW,
            endpointKey = "match-v5-match-by-id",
            maxRetries = maxRetries,
            httpAttemptCounter = httpAttemptCounter,
            failureReasonHolder = failureReasonHolder,
            suppressThrowOnZeroRetryFailure = true
        ) {
            try {
                riotRestClient.get()
                    .uri(uri, matchId)
                    .retrieve()
                    .toEntity(MatchDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                null
            }
        }
    }

    suspend fun getMatchTimeline(matchId: String, region: String): com.pride.summoner_searcher_api.dto.MatchTimelineDto? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}/timeline"

        return executeRateLimitedCall(region, ApiPriority.HIGH, "match-v5-timeline-by-id") {
            try {
                riotRestClient.get()
                    .uri(uri, matchId)
                    .retrieve()
                    .toEntity(com.pride.summoner_searcher_api.dto.MatchTimelineDto::class.java)
            } catch (e: HttpClientErrorException.NotFound) {
                logger.warn("getMatchTimeline returned 404 for matchId: {}", matchId)
                null
            }
        }
    }

    suspend fun getMatchTimelineBounded(
        matchId: String,
        region: String,
        priority: ApiPriority = ApiPriority.HIGH,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null,
        maxTotalDurationMs: Long? = null,
        headroomGuard: (() -> Boolean)? = null
    ): com.pride.summoner_searcher_api.dto.MatchTimelineDto? {
        val regionalRouting = mapToRegionRouting(region)
        val matchBaseUrl = "https://${regionalRouting}.api.riotgames.com"
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}/timeline"
        val deadlineAtMs = maxTotalDurationMs?.let { timeProvider.now() + it.coerceAtLeast(0L) }

        return executeRateLimitedCall(
            region = region,
            priority = priority,
            endpointKey = "match-v5-timeline-by-id",
            maxRetries = maxRetries,
            httpAttemptCounter = httpAttemptCounter,
            failureReasonHolder = failureReasonHolder,
            suppressThrowOnZeroRetryFailure = true,
            deadlineAtMs = deadlineAtMs,
            headroomGuard = headroomGuard
        ) {
            try {
                riotRestClient.get()
                    .uri(uri, matchId)
                    .exchange { _, response ->
                        val status = response.statusCode
                        val headers = response.headers
                        when {
                            status.value() == 404 -> {
                                failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                                logger.warn("getMatchTimelineBounded returned 404 for matchId: {}", matchId)
                                ResponseEntity.status(status).headers(headers).body(null)
                            }
                            status.value() == 429 -> {
                                throw HttpClientErrorException.create(
                                    status,
                                    response.statusText,
                                    headers,
                                    ByteArray(0),
                                    null
                                )
                            }
                            status.isError -> {
                                failureReasonHolder?.set("HTTP_ERROR")
                                logger.warn("getMatchTimelineBounded HTTP {} for matchId: {}", status.value(), matchId)
                                ResponseEntity.status(status).headers(headers).body(null)
                            }
                            deadlineAtMs != null && timeProvider.now() >= deadlineAtMs -> {
                                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                logger.warn("getMatchTimelineBounded deadline exceeded before decode for matchId: {}", matchId)
                                ResponseEntity.status(status).headers(headers).body(null)
                            }
                            headroomGuard != null && !headroomGuard.invoke() -> {
                                failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                                logger.warn("getMatchTimelineBounded memory headroom stop before decode for matchId: {}", matchId)
                                ResponseEntity.status(status).headers(headers).body(null)
                            }
                            else -> {
                                val decoded = response.bodyTo(com.pride.summoner_searcher_api.dto.MatchTimelineDto::class.java)
                                ResponseEntity.status(status).headers(headers).body(decoded)
                            }
                        }
                    }
            } catch (e: HttpClientErrorException.NotFound) {
                failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                logger.warn("getMatchTimelineBounded returned 404 for matchId: {}", matchId)
                null
            } catch (e: HttpClientErrorException.TooManyRequests) {
                throw e
            } catch (e: org.springframework.web.client.ResourceAccessException) {
                throw e
            } catch (e: org.springframework.web.client.RestClientException) {
                if (e.cause is InterruptedException || e.cause is java.io.InterruptedIOException) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                } else {
                    failureReasonHolder?.set("HTTP_ERROR")
                }
                logger.warn("getMatchTimelineBounded failed for matchId {}: {}", matchId, e.message)
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
        return executeRateLimitedCall(region, ApiPriority.HIGH, "status-v4-platform-data") {
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

    @org.springframework.beans.factory.annotation.Value("\${cohort.patch.ddragon-versions-url:https://ddragon.leagueoflegends.com/api/versions.json}")
    private var dataDragonVersionsUrl: String = "https://ddragon.leagueoflegends.com/api/versions.json"

    /**
     * Bounded lookup of Riot Data Dragon `versions.json` (e.g. `["16.22.1", "16.21.1", ...]`)
     * used to ground the current League of Legends `major.minor` patch without counting against
     * regional Riot API rate limits.
     * Uses the separate unauthenticated [dataDragonRestClient] (never [riotRestClient]) so the
     * `X-Riot-Token` header is never sent to `ddragon.leagueoflegends.com`.
     * Returns an empty list on any network/parse error (safe fallback: never crashes and never
     * advances to an unverified patch).
     */
    fun fetchDataDragonVersions(maxEntries: Int = 5): List<String> {
        val safeMax = maxEntries.coerceIn(1, 20)
        return try {
            val body = dataDragonRestClient.get()
                .uri(dataDragonVersionsUrl)
                .headers { it.remove("X-Riot-Token") }
                .retrieve()
                .body(Array<String>::class.java)
            body?.asSequence()
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }
                ?.take(safeMax)
                ?.toList()
                .orEmpty()
        } catch (e: Exception) {
            logger.debug("Data Dragon versions.json lookup failed (safe fallback): {}", e.message)
            emptyList()
        }
    }

    suspend fun getActiveGameByPuuid(puuid: String, region: String): CurrentGameInfo? = coroutineScope {
        val platformBaseUrl = "https://${region}.api.riotgames.com"
        val uri = "$platformBaseUrl/lol/spectator/v5/active-games/by-summoner/{puuid}"
        
        val gameInfo = executeRateLimitedCall(region, ApiPriority.HIGH, "spectator-v5-active-game") {
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
