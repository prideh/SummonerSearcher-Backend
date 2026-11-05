package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * A service for managing the caching of player profiles.
 * This service acts as an intelligent layer between the frontend request and the Riot API,
 * using Redis to store and retrieve player data to minimize API calls.
 */
@Service
class PlayerCacheService(
    private val redisCacheService: RedisCacheService,
    private val riotApiService: RiotApiService,
    private val indexedPlayerRepository: com.pride.summoner_searcher_api.repository.IndexedPlayerRepository,
    private val timeProvider: TimeProvider = SystemTimeProvider(),
    private val lockPollIntervalMs: Long = LOCK_POLL_INTERVAL_MS,
    private val lockMaxPollAttempts: Int = LOCK_MAX_POLL_ATTEMPTS
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val timelineAnalysisService = TimelineAnalysisService()
    private val inFlightMutex = Mutex()
    private val inFlightRefreshes = ConcurrentHashMap<String, CompletableDeferred<SummonerProfileDto?>>()
    private val lastBackgroundRefreshAtMs = ConcurrentHashMap<String, Long>()
    private val backgroundRefreshScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    companion object {
        private const val RECENT_MATCHES_LIMIT = 20
        private const val CHUNK_SIZE = 50
        private val PROFILE_CACHE_TTL = Duration.ofDays(14)
        private val MATCH_CACHE_TTL = Duration.ofDays(60)
        val PROFILE_FRESHNESS_WINDOW: Duration = Duration.ofMinutes(5)
        val EXPLICIT_REFRESH_MIN_COOLDOWN: Duration = Duration.ofSeconds(60)
        val PROFILE_LOCK_TTL: Duration = Duration.ofSeconds(60)
        const val LOCK_POLL_INTERVAL_MS = 250L
        const val LOCK_MAX_POLL_ATTEMPTS = 240 // Full 60s wait matching PROFILE_LOCK_TTL (240 * 250ms = 60s)
    }

    /**
     * Generates a unique key for storing a player's profile in Redis.
     */
    private fun getProfileCacheKey(puuid: String, region: String) = "player:profile:${region.lowercase()}:$puuid"
    
    private fun getMatchCacheKey(matchId: String, region: String) = "match:${region.lowercase()}:$matchId"

    private fun getLockKey(puuid: String, region: String) = "lock:profile:${region.lowercase()}:$puuid"

    private fun getRefreshCooldownKey(puuid: String, region: String) =
        "cooldown:profile:refresh:${region.lowercase()}:$puuid"

    private fun getFlightKey(puuid: String, region: String) = "${region.lowercase()}:$puuid"

    /**
     * True when a match is completely absent from Redis and must be fetched for full-season stats accuracy.
     */
    private fun isMissingForSeasonStats(cached: MatchDto?): Boolean {
        return cached?.info == null
    }

    /**
     * True when a match is missing or lacks enriched fields (`gameVersion`, `teams`) needed for
     * recent-20 match display, patch badges, and `improvementStats` objective control.
     */
    private fun needsMatchBackfill(cached: MatchDto?): Boolean {
        val info = cached?.info ?: return true
        return info.gameVersion == null || info.teams == null
    }

    /**
     * A cached profile is ready for immediate cache-hit response
     * when its full-season `overallStats`, `improvementStats`, and enriched `recentMatches` are present.
     */
    internal fun isProfileReadyForImmediateServe(cached: SummonerProfileDto?): Boolean {
        if (cached == null) return false
        if (cached.overallStats == null || cached.improvementStats == null) return false
        val recent = cached.recentMatches
        if (cached.allMatchIds.isNotEmpty() && recent.isNullOrEmpty()) return false
        return recent?.none { needsMatchBackfill(it) } ?: true
    }

    internal fun isProfileWithinFreshnessWindow(
        cached: SummonerProfileDto?,
        now: Instant,
        window: Duration = PROFILE_FRESHNESS_WINDOW
    ): Boolean {
        val refreshedAt = cached?.lastRefreshed ?: return false
        return !refreshedAt.isBefore(now.minus(window))
    }

    /**
     * Retrieves a player's profile with bounded freshness and explicit-refresh semantics:
     * - If a ready cached profile exists and [explicitRefresh] is `false`:
     *   - Within [PROFILE_FRESHNESS_WINDOW] (5m): returns `cachedProfile` immediately with **zero** Riot API calls
     *     and **zero** background refresh tasks.
     *   - Older than [PROFILE_FRESHNESS_WINDOW]: returns `cachedProfile` immediately and triggers at most one
     *     cooldown-guarded background refresh.
     * - If [explicitRefresh] is `true`:
     *   - If refreshed within [EXPLICIT_REFRESH_MIN_COOLDOWN] (60s), returns `cachedProfile` immediately.
     *   - Otherwise performs a single-flight synchronous refresh.
     * - If cache miss or legacy un-enriched cache entry: performs a single-flight synchronous refresh.
     */
    suspend fun getPlayerProfile(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String,
        explicitRefresh: Boolean = false
    ): SummonerProfileDto? {
        val cacheKey = getProfileCacheKey(puuid, region)
        val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
        val now = Instant.ofEpochMilli(timeProvider.now())

        if (isProfileReadyForImmediateServe(cachedProfile)) {
            if (explicitRefresh) {
                if (isProfileWithinFreshnessWindow(cachedProfile, now, EXPLICIT_REFRESH_MIN_COOLDOWN)) {
                    logger.info("Explicit refresh requested within 60s cooldown for $summonerName#$tagLine ($region); serving fresh cached profile.")
                    return cachedProfile
                }
                logger.info("Explicit refresh requested for $summonerName#$tagLine ($region); refreshing synchronously.")
                return refreshPlayerProfile(puuid, region, summonerName, tagLine)
            }

            if (isProfileWithinFreshnessWindow(cachedProfile, now, PROFILE_FRESHNESS_WINDOW)) {
                logger.info("Fresh cache hit for $summonerName#$tagLine ($region); serving cached profile with zero Riot calls.")
                return cachedProfile
            }

            logger.info("Stale cache hit for $summonerName#$tagLine ($region); serving cached profile and checking background refresh cooldown.")
            triggerBackgroundRefreshIfCooldownAllows(puuid, region, summonerName, tagLine)
            return cachedProfile
        }

        return refreshPlayerProfile(puuid, region, summonerName, tagLine)
    }

    /**
     * Triggers an asynchronous background refresh only if:
     * 1. No refresh is currently in flight for `(region, puuid)`, AND
     * 2. Neither the in-process cooldown nor the Redis cooldown key (`cooldown:profile:refresh:{region}:{puuid}`)
     *    was triggered within [PROFILE_FRESHNESS_WINDOW].
     */
    fun triggerBackgroundRefreshIfCooldownAllows(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): Boolean {
        val flightKey = getFlightKey(puuid, region)
        val existing = inFlightRefreshes[flightKey]
        if (existing != null && existing.isActive) {
            return false
        }

        val nowMs = timeProvider.now()
        val windowMs = PROFILE_FRESHNESS_WINDOW.toMillis()
        val prevMs = lastBackgroundRefreshAtMs[flightKey]
        if (prevMs != null && (nowMs - prevMs) < windowMs) {
            return false
        }

        val cooldownKey = getRefreshCooldownKey(puuid, region)
        val acquiredCooldown = redisCacheService.setIfAbsent(cooldownKey, "COOLDOWN", PROFILE_FRESHNESS_WINDOW)
        if (!acquiredCooldown) {
            return false
        }

        val cutoffMs = nowMs - windowMs
        lastBackgroundRefreshAtMs.entries.removeIf { it.value < cutoffMs }
        while (lastBackgroundRefreshAtMs.size >= 256) {
            val oldestKey = lastBackgroundRefreshAtMs.entries.minByOrNull { it.value }?.key ?: break
            lastBackgroundRefreshAtMs.remove(oldestKey)
        }
        lastBackgroundRefreshAtMs[flightKey] = nowMs
        backgroundRefreshScope.launch {
            try {
                refreshPlayerProfile(puuid, region, summonerName, tagLine)
            } catch (e: Exception) {
                logger.warn("Background profile refresh failed for $summonerName#$tagLine ($region): ${e.message}")
            }
        }
        return true
    }

    /**
     * Single-flight profile refresh with duplicate-lock coordination.
     * Concurrent callers in the same JVM share the in-flight [Deferred] instead of timing out after 2.5s
     * and returning `null`.
     */
    suspend fun refreshPlayerProfile(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): SummonerProfileDto? {
        val flightKey = getFlightKey(puuid, region)
        val (deferred, isLeader) = inFlightMutex.withLock {
            val active = inFlightRefreshes[flightKey]
            if (active != null && active.isActive) {
                active to false
            } else {
                val created = CompletableDeferred<SummonerProfileDto?>()
                inFlightRefreshes[flightKey] = created
                created to true
            }
        }

        if (!isLeader) {
            logger.info("Joining in-flight profile refresh for $summonerName#$tagLine ($region)")
            return deferred.await()
        }

        try {
            val result = executeLockedRefresh(puuid, region, summonerName, tagLine)
            deferred.complete(result)
            return result
        } catch (t: Throwable) {
            deferred.completeExceptionally(t)
            throw t
        } finally {
            inFlightMutex.withLock {
                if (inFlightRefreshes[flightKey] === deferred) {
                    inFlightRefreshes.remove(flightKey)
                }
            }
        }
    }

    private suspend fun executeLockedRefresh(
        puuid: String,
        region: String,
        summonerName: String,
        tagLine: String
    ): SummonerProfileDto? = coroutineScope {
        val cacheKey = getProfileCacheKey(puuid, region)
        val lockKey = getLockKey(puuid, region)
        val waitStart = Instant.ofEpochMilli(timeProvider.now())

        var ownsLock = redisCacheService.setIfAbsent(lockKey, "LOCKED", PROFILE_LOCK_TTL)

        if (!ownsLock) {
            logger.info("Profile refresh lock held externally for $summonerName#$tagLine ($region). Waiting up to ${lockMaxPollAttempts * lockPollIntervalMs}ms.")
            val immediateCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (isProfileReadyForImmediateServe(immediateCached)) {
                return@coroutineScope immediateCached
            }

            repeat(lockMaxPollAttempts) {
                val cached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
                if (cached != null && (
                        cached.lastRefreshed?.isAfter(waitStart.minusSeconds(10)) == true ||
                            isProfileReadyForImmediateServe(cached)
                        )
                ) {
                    logger.info("Cache populated during lock wait for $summonerName#$tagLine ($region)")
                    return@coroutineScope cached
                }
                delay(lockPollIntervalMs)
            }

            val afterWaitCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (afterWaitCached != null) {
                return@coroutineScope afterWaitCached
            }

            // Never bypass a still-held cross-instance lock! Only proceed if the previous lock expired
            // AND this caller successfully acquires the lock.
            ownsLock = redisCacheService.setIfAbsent(lockKey, "LOCKED", PROFILE_LOCK_TTL)
            if (!ownsLock) {
                logger.warn("Cross-instance lock still held after wait for $summonerName#$tagLine ($region); refusing to bypass lock or duplicate cold refresh.")
                return@coroutineScope redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            }

            val postAcquireCached = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)
            if (postAcquireCached != null) {
                redisCacheService.delete(lockKey)
                ownsLock = false
                return@coroutineScope postAcquireCached
            }
            logger.warn("Previous lock expired with empty cache for $summonerName#$tagLine ($region); acquired lock and proceeding with fallback refresh.")
        }

        try {
            val refreshStartMs = timeProvider.now()
            val cachedProfile = redisCacheService.get(cacheKey, SummonerProfileDto::class.java)

            if (cachedProfile != null) {
                logger.info("Refreshing cached profile for $summonerName#$tagLine ($region)")
            } else {
                logger.info("Cache miss for $summonerName#$tagLine ($region) - Fetching from API")
            }

            // Calculate Start of Year (Jan 1st) in Epoch Seconds
            val startOfYear = java.time.Year.now().atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()

            // 1. Verify summoner exists on this regional shard and fetch fresh level & icon
            val metaStartMs = timeProvider.now()
            val freshSummoner = riotApiService.getSummonerByPuuid(puuid, region)
            if (freshSummoner == null) {
                logger.info("No summoner found on shard $region for $summonerName#$tagLine (puuid=$puuid). Returning null.")
                if (cachedProfile != null) {
                    redisCacheService.delete(cacheKey)
                }
                return@coroutineScope null
            }
            val refreshedLevel = freshSummoner.summonerLevel ?: cachedProfile?.summonerLevel
            val refreshedIconId = freshSummoner.profileIconId ?: cachedProfile?.profileIconId

            // Fetch rank and season match ID list concurrently
            val rankDeferred = async { riotApiService.fetchLeagueRank(puuid, region) }
            val matchIdsDeferred = async { riotApiService.fetchMatchIdsSince(puuid, region, startOfYear) }
            val freshRank = rankDeferred.await()
            val allMatchIds = matchIdsDeferred.await()
            val metadataAndIdsMs = timeProvider.now() - metaStartMs

            // Fast-path: if we already have a cached profile, no new matches have been played,
            // and all served recentMatches already have info.gameVersion and info.teams populated,
            // skip re-streaming and re-accumulating all matches from Redis while updating rank, level, and icon.
            val recentMatchesComplete = cachedProfile?.recentMatches?.none { needsMatchBackfill(it) } ?: true
            if (cachedProfile != null && recentMatchesComplete && (
                (allMatchIds.isEmpty() && cachedProfile.allMatchIds.isEmpty()) ||
                (cachedProfile.allMatchIds.isNotEmpty() && allMatchIds.firstOrNull() == cachedProfile.allMatchIds.first() && cachedProfile.overallStats != null)
            )) {
                val fastImprovementStats = cachedProfile.improvementStats
                    ?: timelineAnalysisService.computeImprovementStats(emptyList(), puuid, cachedProfile.recentMatches ?: emptyList())
                val fastUpdatedProfile = cachedProfile.copy(
                    gameName = summonerName,
                    tagLine = tagLine,
                    summonerLevel = refreshedLevel,
                    profileIconId = refreshedIconId,
                    soloQueueRank = freshRank,
                    improvementStats = fastImprovementStats,
                    lastRefreshed = Instant.ofEpochMilli(timeProvider.now())
                )
                redisCacheService.set(cacheKey, fastUpdatedProfile, PROFILE_CACHE_TTL)
                val totalFastMs = timeProvider.now() - refreshStartMs
                logger.info(
                    "Fast-path refresh for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, totalMatches={} (no new matches since {}).",
                    summonerName, tagLine, region, totalFastMs, metadataAndIdsMs,
                    cachedProfile.totalMatches, cachedProfile.allMatchIds.firstOrNull()
                )
                return@coroutineScope fastUpdatedProfile
            }
            
            // 3. Process matches in chunks to calculate full-season stats without holding everything in memory.
            // Recent-20-only enrichment:
            // - Matches in the recent 20 (first 20 valid seasonal matches) are fetched if missing OR if they lack gameVersion/teams.
            // - Matches beyond the recent 20 (index >= 20) are fetched ONLY if genuinely missing from Redis (cached?.info == null),
            //   avoiding re-fetching hundreds of older cached matches just for gameVersion/teams while preserving full-season stats accuracy.
            val matchProcessingStartMs = timeProvider.now()
            val accumulator = MatchStatsAccumulator(puuid)
            val recentMatches = mutableListOf<MatchDto>()
            val validMatchIds = mutableListOf<String>()
            val startOfYearMs = startOfYear * 1000
            
            val chunks = allMatchIds.chunked(CHUNK_SIZE)
            var processedCount = 0
            var projectedRecentCount = 0
            var cacheHitsCount = 0
            var missingSeasonFetchCount = 0
            var recent20BackfillCount = 0
            var skippedOlderBackfillCount = 0
            val alreadyFetchedIds = mutableSetOf<String>()
            
            for (chunk in chunks) {
                val redisKeys = chunk.map { getMatchCacheKey(it, region) }
                val cachedMatchesMap = redisCacheService.multiGet(redisKeys, MatchDto::class.java)
                
                val missingIds = mutableListOf<String>()
                for (id in chunk) {
                    val cached = cachedMatchesMap[getMatchCacheKey(id, region)]
                    if (isMissingForSeasonStats(cached)) {
                        missingIds.add(id)
                        missingSeasonFetchCount++
                        if (projectedRecentCount < RECENT_MATCHES_LIMIT) {
                            projectedRecentCount++
                        }
                    } else {
                        cacheHitsCount++
                        val creation = cached?.info?.gameCreation ?: 0L
                        if (creation >= startOfYearMs) {
                            if (projectedRecentCount < RECENT_MATCHES_LIMIT) {
                                projectedRecentCount++
                                if (needsMatchBackfill(cached)) {
                                    missingIds.add(id)
                                    recent20BackfillCount++
                                }
                            } else if (needsMatchBackfill(cached)) {
                                skippedOlderBackfillCount++
                            }
                        }
                    }
                }
                
                if (missingIds.isNotEmpty()) {
                    alreadyFetchedIds.addAll(missingIds)
                    logger.info(
                        "Chunk {}/{}: Fetching {} matches from API (missing={}, recent20Backfill={}, skippedOlderBackfill={})",
                        processedCount / CHUNK_SIZE + 1, chunks.size, missingIds.size,
                        missingSeasonFetchCount, recent20BackfillCount, skippedOlderBackfillCount
                    )
                }
                
                val fetchedMatches = missingIds.map { matchId ->
                    async { 
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()

                val validFetchedMatches = fetchedMatches.filter { match ->
                    val creation = match.info?.gameCreation ?: 0
                    match.metadata?.matchId != null && creation >= startOfYearMs
                }

                validFetchedMatches.forEach { match ->
                    val matchId = match.metadata!!.matchId!!
                    redisCacheService.set(getMatchCacheKey(matchId, region), match, MATCH_CACHE_TTL)
                }
                
                val fetchedById = fetchedMatches.associateBy { it.metadata?.matchId }
                val chunkMatches = chunk.mapNotNull { id ->
                    fetchedById[id] ?: cachedMatchesMap[getMatchCacheKey(id, region)]
                }
                
                val validChunkMatches = chunkMatches.filter { (it.info?.gameCreation ?: 0) >= startOfYearMs }
                
                if (validChunkMatches.isNotEmpty() && processedCount < 10) {
                    val remainingToIndex = (10 - processedCount).coerceAtMost(validChunkMatches.size)
                    indexPlayersFromMatches(validChunkMatches.take(remainingToIndex), region)
                }

                validChunkMatches.forEach { match -> 
                    match.metadata?.matchId?.let { validMatchIds.add(it) }
                }

                validChunkMatches.forEach { accumulator.add(it) }
                
                if (recentMatches.size < RECENT_MATCHES_LIMIT) {
                    val needed = RECENT_MATCHES_LIMIT - recentMatches.size
                    recentMatches.addAll(validChunkMatches.take(needed))
                }
                
                processedCount += chunk.size
                
                if (validChunkMatches.size < chunkMatches.size) {
                    logger.info("Hit match time boundary (start of year). Stopping fetch early.")
                    break
                }
            }

            // Safety top-up: if any match in the final recent 20 still needs backfill AND was not already fetched
            // (e.g., an earlier match 404'd and shifted cached match #21 into slot #20), enrich only those entries.
            val unenrichedRecentIds = recentMatches
                .filter { needsMatchBackfill(it) }
                .mapNotNull { it.metadata?.matchId }
                .filter { it !in alreadyFetchedIds }
            if (unenrichedRecentIds.isNotEmpty()) {
                val topUpFetched = unenrichedRecentIds.map { matchId ->
                    async {
                        try {
                            riotApiService.getMatchById(matchId, region)
                        } catch (e: Exception) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()
                val topUpById = topUpFetched.associateBy { it.metadata?.matchId }
                topUpFetched.forEach { match ->
                    val matchId = match.metadata?.matchId
                    if (matchId != null) {
                        redisCacheService.set(getMatchCacheKey(matchId, region), match, MATCH_CACHE_TTL)
                    }
                }
                for (i in recentMatches.indices) {
                    val mid = recentMatches[i].metadata?.matchId
                    val replacement = if (mid != null) topUpById[mid] else null
                    if (replacement != null) {
                        recentMatches[i] = replacement
                        recent20BackfillCount++
                    }
                }
            }
            
            val (championStats, overallStats) = accumulator.getResult()
            val matchOnlyImprovementStats = timelineAnalysisService.computeImprovementStats(
                timelines = emptyList(),
                puuid = puuid,
                allMatches = recentMatches
            )
            
            val updatedProfile = SummonerProfileDto(
                puuid = puuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = refreshedLevel,
                profileIconId = refreshedIconId,
                soloQueueRank = freshRank,
                recentMatches = recentMatches,
                allMatchIds = validMatchIds,
                championStats = championStats,
                overallStats = overallStats,
                improvementStats = matchOnlyImprovementStats,
                totalMatches = validMatchIds.size,
                lastRefreshed = Instant.ofEpochMilli(timeProvider.now())
            )
            
            redisCacheService.set(cacheKey, updatedProfile, PROFILE_CACHE_TTL)
            val matchProcessingMs = timeProvider.now() - matchProcessingStartMs
            val totalRefreshMs = timeProvider.now() - refreshStartMs
            logger.info(
                "Refreshed profile for {}#{} ({}): totalMs={}, metadataAndIdsMs={}, matchProcessingMs={}, validMatches={}/{}, cacheHits={}, missingFetches={}, recent20Backfills={}, skippedOlderBackfills={}",
                summonerName, tagLine, region, totalRefreshMs, metadataAndIdsMs, matchProcessingMs,
                validMatchIds.size, allMatchIds.size, cacheHitsCount, missingSeasonFetchCount,
                recent20BackfillCount, skippedOlderBackfillCount
            )
            
            return@coroutineScope updatedProfile

        } finally {
            if (ownsLock) {
                redisCacheService.delete(lockKey)
            }
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
        
        val missingIds = matchIds.filter { id ->
            val cached = cachedMatchesMap[getMatchCacheKey(id, region)]
            needsMatchBackfill(cached)
        }
        
        val fetchedMatches = missingIds.map { matchId ->
            async {
                try {
                    riotApiService.getMatchById(matchId, region)
                } catch (e: Exception) {
                    null
                }
            }
        }.awaitAll().filterNotNull()
        
        val fetchedById = fetchedMatches.associateBy { it.metadata?.matchId }
        fetchedMatches.forEach { match ->
             val mid = match.metadata?.matchId
             if (mid != null) {
                 redisCacheService.set(getMatchCacheKey(mid, region), match, Duration.ofDays(60))
             }
        }
        
        // Index players found in these new matches for autocomplete search
        if (fetchedMatches.isNotEmpty()) {
            indexPlayersFromMatches(fetchedMatches, region)
        }
        
        matchIds.mapNotNull { id ->
            fetchedById[id] ?: cachedMatchesMap[getMatchCacheKey(id, region)]
        }
    }

    private fun indexPlayersFromMatches(matches: List<com.pride.summoner_searcher_api.dto.MatchDto>, region: String) {
        val playersToIndex = matches.flatMap { it.info?.participants ?: emptyList() }
            // Filter out players with missing essential Riot ID info
            .filter { !it.puuid.isNullOrBlank() && !it.riotIdGameName.isNullOrBlank() && !it.riotIdTagline.isNullOrBlank() }
            .map { p ->
                com.pride.summoner_searcher_api.model.IndexedPlayer(
                    puuid = p.puuid!!,
                    gameName = p.riotIdGameName!!,
                    tagLine = p.riotIdTagline!!,
                    region = region,
                    profileIconId = p.profileIcon ?: 0,
                    summonerLevel = p.summonerLevel?.toLong() ?: 0L,
                    lastSeenAt = java.time.Instant.now()
                )
            }
            // If the same player appears in multiple matches in this batch, keep only the latest object.
            .distinctBy { it.puuid }
            // Limit to a maximum of 200 players per batch to prevent massive database write loads
            // when fullly refreshing a user's seasonal match history (>200 matches * 10 players)
            .take(200)

        if (playersToIndex.isNotEmpty()) {
            try {
                // To avoid blocking the main thread significantly if there are many matches, we launch a coroutine on the IO dispatcher.
                CoroutineScope(Dispatchers.IO).launch {
                    try {
                        // Chunk inserts to prevent exceeding parameter limits or causing long write locks
                        playersToIndex.chunked(100).forEach { batch ->
                            indexedPlayerRepository.saveAll(batch)
                        }
                    } catch (e: Exception) {
                        logger.error("Error saving indexed players: ${e.message}")
                    }
                }
            } catch (e: Exception) {
                logger.error("Error dispatching players to index: ${e.message}")
            }
        }
    }
}
