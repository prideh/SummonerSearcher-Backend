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

    internal var platformBaseUrlResolver: (String) -> String = { region ->
        "https://${region.trim().lowercase()}.api.riotgames.com"
    }

    internal var matchBaseUrlResolver: (String) -> String = { regionalRouting ->
        "https://${regionalRouting.trim().lowercase()}.api.riotgames.com"
    }

    internal var maxTimelineResponseBytes: Int = MAX_TIMELINE_RESPONSE_BYTES

    private val activeBoundedHttpWorkers = java.util.concurrent.atomic.AtomicInteger(0)

    private val boundedHttpExecutor = java.util.concurrent.ThreadPoolExecutor(
        2,
        4,
        30L,
        java.util.concurrent.TimeUnit.SECONDS,
        java.util.concurrent.LinkedBlockingQueue(8),
        java.util.concurrent.ThreadFactory { runnable ->
            Thread(runnable, "riot-bounded-league-http").apply { isDaemon = true }
        },
        java.util.concurrent.ThreadPoolExecutor.AbortPolicy()
    ).apply {
        allowCoreThreadTimeOut(true)
    }

    private val leagueObjectMapper = com.fasterxml.jackson.databind.ObjectMapper()
        .findAndRegisterModules()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    internal fun getActiveBoundedHttpWorkers(): Int = activeBoundedHttpWorkers.get()

    internal fun getActiveRetryAfterWaitMs(region: String): Long =
        rateLimiter.getActiveRetryAfterWaitMs(region.trim().lowercase())

    internal class HttpCancellationHandle {
        private val cancelled = java.util.concurrent.atomic.AtomicBoolean(false)
        private val closeables = java.util.concurrent.CopyOnWriteArrayList<AutoCloseable>()

        fun isCancelled(): Boolean = cancelled.get()

        fun registerCloseable(closeable: AutoCloseable) {
            closeables.add(closeable)
            if (cancelled.get()) {
                runCatching { closeable.close() }
            }
        }

        fun cancel() {
            cancelled.set(true)
            for (c in closeables) {
                runCatching { c.close() }
            }
        }
    }

    private class BoundedLeagueStreamAbortedException(val reason: String) : java.io.IOException(reason)

    private inner class BoundedLeagueInputStream(
        private val delegate: java.io.InputStream,
        private val response: org.springframework.http.client.ClientHttpResponse,
        private val cancellationHandle: HttpCancellationHandle?,
        private val deadlineAtMs: Long?,
        private val maxResponseBytes: Int,
        private val headroomGuard: (() -> Boolean)?,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>?,
        private val headroomCheckStrideBytes: Int = 1_024,
        private val maxReadChunkBytes: Int = 1_024
    ) : java.io.InputStream() {
        private var totalBytesRead = 0
        private var initialHeadroomChecked = false
        private var nextHeadroomCheckAtBytes = headroomCheckStrideBytes

        private fun checkFastGuardsOrThrow() {
            if (cancellationHandle?.isCancelled() == true ||
                Thread.currentThread().isInterrupted ||
                (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)
            ) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                abortResponseBody(response, delegate)
                throw BoundedLeagueStreamAbortedException("DEADLINE_EXCEEDED")
            }
            if (totalBytesRead > maxResponseBytes) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                abortResponseBody(response, delegate)
                throw BoundedLeagueStreamAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        private fun checkHeadroomIfDueOrThrow(force: Boolean = false) {
            if (headroomGuard != null && (force || !initialHeadroomChecked || totalBytesRead >= nextHeadroomCheckAtBytes)) {
                initialHeadroomChecked = true
                while (nextHeadroomCheckAtBytes <= totalBytesRead) {
                    nextHeadroomCheckAtBytes += headroomCheckStrideBytes
                }
                if (!headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    abortResponseBody(response, delegate)
                    throw BoundedLeagueStreamAbortedException("MEMORY_HEADROOM_STOP")
                }
            }
        }

        override fun read(): Int {
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val b = delegate.read()
            if (b != -1) {
                totalBytesRead++
            }
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            return b
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (len <= 0) return 0
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val boundedLen = len.coerceAtMost(maxReadChunkBytes)
            val n = delegate.read(b, off, boundedLen)
            if (n > 0) {
                totalBytesRead += n
            }
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            return n
        }

        override fun close() {
            abortResponseBody(response, delegate)
        }
    }

    private fun unwrapClientHttpResponse(
        response: org.springframework.http.client.ClientHttpResponse
    ): org.springframework.http.client.ClientHttpResponse {
        var current = response
        while (current.javaClass.name != "org.springframework.http.client.ReactorClientHttpResponse") {
            val delegateField = runCatching {
                current.javaClass.getDeclaredField("delegate").apply { isAccessible = true }
            }.getOrNull() ?: break
            val next = runCatching {
                delegateField.get(current) as? org.springframework.http.client.ClientHttpResponse
            }.getOrNull() ?: break
            if (next === current) break
            current = next
        }
        return current
    }

    private fun abortResponseBody(
        response: org.springframework.http.client.ClientHttpResponse,
        openedBodyStream: java.io.InputStream? = null
    ) {
        try {
            openedBodyStream?.close()
        } catch (_: Throwable) {
        }
        try {
            val underlying = unwrapClientHttpResponse(response)
            runCatching { underlying.body.close() }
            if (underlying.javaClass.name == "org.springframework.http.client.ReactorClientHttpResponse") {
                val connField = underlying.javaClass.getDeclaredField("connection").apply { isAccessible = true }
                val conn = connField.get(underlying) as? reactor.netty.Connection
                conn?.dispose()
            }
        } catch (_: Throwable) {
        }
        try {
            response.close()
        } catch (_: Throwable) {
        }
    }

    private suspend fun <T> runBoundedHttpCallWithDeadline(
        deadlineAtMs: Long?,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>?,
        maxShutdownJoinMs: Long = 250L,
        block: (HttpCancellationHandle) -> T?
    ): T? {
        if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
            failureReasonHolder?.set("DEADLINE_EXCEEDED")
            return null
        }
        val cancellationHandle = HttpCancellationHandle()
        val taskClaimedOrCancelled = java.util.concurrent.atomic.AtomicBoolean(false)
        val workerThreadRef = java.util.concurrent.atomic.AtomicReference<Thread?>(null)
        val workerDoneLatch = java.util.concurrent.CountDownLatch(1)
        val submittedFuture: java.util.concurrent.Future<T?> = try {
            boundedHttpExecutor.submit(java.util.concurrent.Callable<T?> {
                val currentThread = Thread.currentThread()
                workerThreadRef.set(currentThread)
                if (!taskClaimedOrCancelled.compareAndSet(false, true)) {
                    workerThreadRef.set(null)
                    workerDoneLatch.countDown()
                    return@Callable null
                }
                activeBoundedHttpWorkers.incrementAndGet()
                try {
                    if (cancellationHandle.isCancelled() || currentThread.isInterrupted) {
                        null
                    } else {
                        block(cancellationHandle)
                    }
                } finally {
                    activeBoundedHttpWorkers.decrementAndGet()
                    Thread.interrupted()
                    workerDoneLatch.countDown()
                    workerThreadRef.set(null)
                }
            })
        } catch (e: java.util.concurrent.RejectedExecutionException) {
            failureReasonHolder?.set("HTTP_EXECUTOR_SATURATED")
            return null
        }

        val safeJoinMs = maxShutdownJoinMs.coerceIn(10L, 250L)
        val activeHook = kotlin.coroutines.coroutineContext[LingeringWorkerHookElement]?.hook
        var cancelledBeforeStart = false
        return try {
            kotlinx.coroutines.runInterruptible(Dispatchers.IO) {
                try {
                    if (deadlineAtMs != null) {
                        val remainingBudgetMs = (deadlineAtMs - timeProvider.now()).coerceAtLeast(1L)
                        submittedFuture.get(remainingBudgetMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                    } else {
                        submittedFuture.get()
                    }
                } catch (ee: java.util.concurrent.ExecutionException) {
                    var unwrapped: Throwable = ee
                    while (unwrapped is java.util.concurrent.ExecutionException && unwrapped.cause != null) {
                        unwrapped = unwrapped.cause!!
                    }
                    throw unwrapped
                }
            }
        } catch (e: java.util.concurrent.TimeoutException) {
            cancellationHandle.cancel()
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            submittedFuture.cancel(true)
            if (failureReasonHolder?.get() == null) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
            }
            null
        } catch (e: kotlinx.coroutines.CancellationException) {
            cancellationHandle.cancel()
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            submittedFuture.cancel(true)
            throw e
        } catch (e: InterruptedException) {
            cancellationHandle.cancel()
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            workerThreadRef.get()?.interrupt()
            submittedFuture.cancel(true)
            Thread.currentThread().interrupt()
            if (failureReasonHolder?.get() == null) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
            }
            null
        } catch (e: Throwable) {
            cancelledBeforeStart = taskClaimedOrCancelled.compareAndSet(false, true)
            var unwrapped: Throwable = e
            while (unwrapped is java.util.concurrent.ExecutionException && unwrapped.cause != null) {
                unwrapped = unwrapped.cause!!
            }
            throw unwrapped
        } finally {
            if (!cancelledBeforeStart && workerDoneLatch.count > 0L) {
                val joined = withContext(kotlinx.coroutines.NonCancellable + Dispatchers.IO) {
                    workerDoneLatch.await(safeJoinMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                }
                if (!joined && workerDoneLatch.count > 0L) {
                    activeHook?.invoke(LingeringWorkerHandle(workerDoneLatch, workerThreadRef))
                }
            }
        }
    }

    internal class LingeringWorkerHandle(
        val doneLatch: java.util.concurrent.CountDownLatch,
        private val workerThreadRef: java.util.concurrent.atomic.AtomicReference<Thread?>
    ) {
        fun isStillRunning(): Boolean {
            if (doneLatch.count <= 0L) return false
            val workerThread = workerThreadRef.get() ?: return false
            return workerThread.isAlive && doneLatch.count > 0L
        }
    }

    internal class LingeringWorkerHookElement(
        val hook: (LingeringWorkerHandle) -> Unit
    ) : kotlin.coroutines.AbstractCoroutineContextElement(LingeringWorkerHookElement) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<LingeringWorkerHookElement>
    }

    internal class RiotCallPriorityElement(
        val priorityProvider: () -> ApiPriority,
        val promotionSignal: kotlinx.coroutines.CompletableDeferred<Unit>? = null
    ) : kotlin.coroutines.AbstractCoroutineContextElement(RiotCallPriorityElement) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<RiotCallPriorityElement>
    }

    internal data class MatchIdsIncrementalAnchor(
        val knownOrderedSeasonalIds: List<String>,
        val knownExcludedIds: Set<String> = emptySet(),
        val priorHistoryCapped: Boolean = false
    )

    internal class MatchIdsIncrementalAnchorElement(
        val anchor: MatchIdsIncrementalAnchor
    ) : kotlin.coroutines.AbstractCoroutineContextElement(MatchIdsIncrementalAnchorElement) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<MatchIdsIncrementalAnchorElement>
    }

    companion object {
        const val MAX_CHALLENGER_LEAGUE_RESPONSE_BYTES = 524_288 // 512 KiB strict bound
        const val MAX_CHALLENGER_LEAGUE_ENTRIES = 300
        const val MAX_TIMELINE_RESPONSE_BYTES = 4 * 1024 * 1024 // 4 MiB strict raw timeline bound
        const val DEFAULT_SINGLE_TIMELINE_DEADLINE_MS = 10_000L
        const val ANCHOR_MIN_CONTIGUOUS_KNOWN_TAIL = 20

        internal suspend fun <R> withLingeringWorkerHook(
            hook: (LingeringWorkerHandle) -> Unit,
            block: suspend () -> R
        ): R {
            return withContext(LingeringWorkerHookElement(hook)) {
                block()
            }
        }
    }

    /**
     * Wrapper function that enforces rate limiting for all Riot API calls.
     * 
     * Now supports Dynamic Rate Limiting by parsing both App and Method response headers.
     */
    private suspend fun <T> executeRateLimitedCall(
        region: String, 
        priority: ApiPriority = ApiPriority.HIGH, 
        endpointKey: String? = null,
        maxRetries: Int = 3,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null,
        suppressThrowOnZeroRetryFailure: Boolean = false,
        apiCall: () -> ResponseEntity<T>?
    ): T? {
        var attempts = 0
        val effectiveMaxRetries = maxRetries.coerceAtLeast(0)
        val priorityContext = kotlin.coroutines.coroutineContext[RiotCallPriorityElement]
        fun resolveEffectivePriority(): ApiPriority {
            if (priority == ApiPriority.LOW) return ApiPriority.LOW
            return priorityContext?.priorityProvider?.invoke() ?: priority
        }

        var registeredHighUserRequest = false
        if (resolveEffectivePriority() == ApiPriority.HIGH) {
            rateLimiter.beginUserRequest(region)
            registeredHighUserRequest = true
        }
        try {
            while (true) {
                var attemptPriority = resolveEffectivePriority()
                if (attemptPriority == ApiPriority.HIGH && !registeredHighUserRequest) {
                    rateLimiter.beginUserRequest(region)
                    registeredHighUserRequest = true
                }
                val waitStartMs = System.currentTimeMillis()
                if (attemptPriority == ApiPriority.LOW) {
                    val promoSignal = if (priority == ApiPriority.LOW) null else priorityContext?.promotionSignal
                    if (promoSignal != null) {
                        coroutineScope {
                            val clearanceJob = async { rateLimiter.awaitLowPriorityClearance(region) }
                            val promoWatcher = async {
                                promoSignal.await()
                                clearanceJob.cancel()
                            }
                            try {
                                clearanceJob.await()
                            } catch (_: kotlin.coroutines.cancellation.CancellationException) {
                                if (!promoSignal.isCompleted) throw kotlin.coroutines.cancellation.CancellationException()
                            } finally {
                                promoWatcher.cancel()
                            }
                        }
                        attemptPriority = resolveEffectivePriority()
                        if (attemptPriority == ApiPriority.HIGH && !registeredHighUserRequest) {
                            rateLimiter.beginUserRequest(region)
                            registeredHighUserRequest = true
                        }
                    } else {
                        rateLimiter.awaitLowPriorityClearance(region)
                    }
                }
                rateLimiter.acquirePermit(region, attemptPriority, endpointKey)
                val limiterWaitMs = System.currentTimeMillis() - waitStartMs
                if (limiterWaitMs > 25L) {
                    logger.info(
                        "Riot rate limiter wait: region={}, endpoint={}, priority={}, limiterWaitMs={}",
                        region, endpointKey ?: "unspecified", attemptPriority, limiterWaitMs
                    )
                }
                try {
                    httpAttemptCounter?.incrementAndGet()
                    val httpStartMs = System.currentTimeMillis()
                    val response = withContext(Dispatchers.IO) {
                        apiCall()
                    }
                    val httpMs = System.currentTimeMillis() - httpStartMs
                    
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
                            region, endpointKey ?: "unspecified", attemptPriority, limiterWaitMs, httpMs,
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
            if (registeredHighUserRequest) {
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


    /**
     * Fetches the Challenger league for [region] and [queue] in a SINGLE bounded Riot API call
     * (`/lol/league/v4/challengerleagues/by-queue/{queue}`) with:
     * - Zero `getAccountByPuuid` (`account-v1-by-puuid`) fanout,
     * - Strict stream byte cap ([maxResponseBytes], default 512 KiB) and entry cap ([maxEntries], default 300),
     * - Pre-flight, limiter-wait, mid-stream, and post-decode [deadlineAtMs] and [headroomGuard] enforcement,
     * - Immediate ReactorNetty connection disposal on deadline, headroom stop, or oversized payload.
     */
    suspend fun fetchChallengerLeagueBounded(
        region: String,
        queue: String = "RANKED_SOLO_5x5",
        priority: ApiPriority = ApiPriority.LOW,
        maxRetries: Int = 0,
        deadlineAtMs: Long? = null,
        maxResponseBytes: Int = MAX_CHALLENGER_LEAGUE_RESPONSE_BYTES,
        maxEntries: Int = MAX_CHALLENGER_LEAGUE_ENTRIES,
        headroomGuard: (() -> Boolean)? = null,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ): LeagueListDTO? {
        val normalizedRegion = region.trim().lowercase()
        val safeMaxBytes = maxResponseBytes.coerceIn(4_096, 2 * 1024 * 1024)
        val safeMaxEntries = maxEntries.coerceIn(1, MAX_CHALLENGER_LEAGUE_ENTRIES)
        val effectiveMaxRetries = maxRetries.coerceAtLeast(0)
        val endpointKey = "league-v4-challenger"

        if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
            failureReasonHolder?.set("DEADLINE_EXCEEDED")
            return null
        }
        if (headroomGuard != null && !headroomGuard.invoke()) {
            failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
            return null
        }

        val baseUrl = platformBaseUrlResolver(normalizedRegion)
        val uri = "$baseUrl/lol/league/v4/challengerleagues/by-queue/{queue}"

        if (priority == ApiPriority.HIGH) {
            rateLimiter.beginUserRequest(normalizedRegion)
        }
        try {
            var attempts = 0
            while (true) {
                if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                    return null
                }
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }

                val permitAcquired = if (deadlineAtMs != null) {
                    val remainingWaitMs = (deadlineAtMs - timeProvider.now()).coerceAtLeast(0L)
                    if (remainingWaitMs <= 0L) {
                        false
                    } else {
                        kotlinx.coroutines.withTimeoutOrNull(remainingWaitMs) {
                            if (priority == ApiPriority.LOW) {
                                rateLimiter.awaitLowPriorityClearance(normalizedRegion)
                            }
                            rateLimiter.acquirePermit(normalizedRegion, priority, endpointKey)
                            true
                        } ?: false
                    }
                } else {
                    if (priority == ApiPriority.LOW) {
                        rateLimiter.awaitLowPriorityClearance(normalizedRegion)
                    }
                    rateLimiter.acquirePermit(normalizedRegion, priority, endpointKey)
                    true
                }

                if (!permitAcquired || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                    return null
                }
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }

                try {
                    httpAttemptCounter?.incrementAndGet()
                    val response = runBoundedHttpCallWithDeadline(deadlineAtMs, failureReasonHolder) { cancellationHandle ->
                        try {
                            riotRestClient.get()
                                .uri(uri, queue)
                                .exchange { _, clientResponse ->
                                    cancellationHandle.registerCloseable(AutoCloseable {
                                        abortResponseBody(clientResponse)
                                    })
                                    val status = clientResponse.statusCode
                                    val headers = clientResponse.headers
                                    val declaredLength = headers.contentLength
                                    when {
                                        status.value() == 404 -> {
                                            failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                                            logger.warn("fetchChallengerLeagueBounded returned 404 for region {} queue {}", normalizedRegion, queue)
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        status.value() == 429 -> {
                                            abortResponseBody(clientResponse)
                                            throw HttpClientErrorException.create(
                                                status,
                                                "Too Many Requests",
                                                headers,
                                                ByteArray(0),
                                                null
                                            )
                                        }
                                        !status.is2xxSuccessful -> {
                                            failureReasonHolder?.set("HTTP_${status.value()}")
                                            logger.warn("fetchChallengerLeagueBounded HTTP {} for region {} queue {}", status.value(), normalizedRegion, queue)
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        declaredLength > safeMaxBytes -> {
                                            failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                                            logger.warn(
                                                "fetchChallengerLeagueBounded Content-Length {} exceeds maxResponseBytes {} for region {}",
                                                declaredLength, safeMaxBytes, normalizedRegion
                                            )
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        cancellationHandle.isCancelled() || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) -> {
                                            failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        headroomGuard != null && !headroomGuard.invoke() -> {
                                            failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        else -> {
                                            val rawStream = clientResponse.body
                                            val boundedStream = BoundedLeagueInputStream(
                                                delegate = rawStream,
                                                response = clientResponse,
                                                cancellationHandle = cancellationHandle,
                                                deadlineAtMs = deadlineAtMs,
                                                maxResponseBytes = safeMaxBytes,
                                                headroomGuard = headroomGuard,
                                                failureReasonHolder = failureReasonHolder
                                            )
                                            cancellationHandle.registerCloseable(boundedStream)
                                            val decoded = leagueObjectMapper.readValue(
                                                boundedStream,
                                                LeagueListDTO::class.java
                                            )
                                            if (cancellationHandle.isCancelled() || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)) {
                                                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                                abortResponseBody(clientResponse, rawStream)
                                                ResponseEntity.status(status).headers(headers).body(null)
                                            } else if (headroomGuard != null && !headroomGuard.invoke()) {
                                                failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                                                abortResponseBody(clientResponse, rawStream)
                                                ResponseEntity.status(status).headers(headers).body(null)
                                            } else {
                                                val boundedEntries = decoded.entries
                                                    .asSequence()
                                                    .filter { !it.puuid.isNullOrBlank() }
                                                    .sortedByDescending { it.leaguePoints ?: Int.MIN_VALUE }
                                                    .take(safeMaxEntries)
                                                    .map { entry ->
                                                        LeagueEntryDto(
                                                            puuid = entry.puuid?.trim(),
                                                            leaguePoints = entry.leaguePoints,
                                                            rank = entry.rank,
                                                            wins = entry.wins,
                                                            losses = entry.losses,
                                                            veteran = entry.veteran,
                                                            inactive = entry.inactive,
                                                            freshBlood = entry.freshBlood,
                                                            hotStreak = entry.hotStreak,
                                                            leagueId = entry.leagueId,
                                                            queueType = entry.queueType,
                                                            tier = entry.tier,
                                                            summonerName = entry.summonerName,
                                                            gameName = entry.gameName?.trim()?.takeIf { it.isNotBlank() },
                                                            tagLine = entry.tagLine?.trim()?.takeIf { it.isNotBlank() }
                                                        )
                                                    }
                                                    .toList()
                                                ResponseEntity.status(status).headers(headers).body(
                                                    decoded.copy(entries = boundedEntries)
                                                )
                                            }
                                        }
                                    }
                                }
                        } catch (e: BoundedLeagueStreamAbortedException) {
                            failureReasonHolder?.set(e.reason)
                            logger.warn("fetchChallengerLeagueBounded aborted stream decode ({}) for region {}", e.reason, normalizedRegion)
                            null
                        } catch (e: HttpClientErrorException.NotFound) {
                            failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                            null
                        } catch (e: HttpClientErrorException.TooManyRequests) {
                            throw e
                        } catch (e: Exception) {
                            val abortedByGuard = generateSequence<Throwable>(e) { it.cause }
                                .filterIsInstance<BoundedLeagueStreamAbortedException>()
                                .firstOrNull()
                            val hasInterrupt = generateSequence<Throwable>(e) { it.cause }
                                .any { it is InterruptedException || it is java.io.InterruptedIOException }
                            when {
                                abortedByGuard != null -> failureReasonHolder?.set(abortedByGuard.reason)
                                cancellationHandle.isCancelled() || hasInterrupt || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) -> {
                                    val currentReason = failureReasonHolder?.get()
                                    if (currentReason != "MEMORY_HEADROOM_STOP" && currentReason != "RESPONSE_TOO_LARGE") {
                                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                    }
                                }
                                else -> {
                                    if (failureReasonHolder?.get() == null) {
                                        failureReasonHolder?.set("HTTP_ERROR")
                                    }
                                }
                            }
                            logger.warn("fetchChallengerLeagueBounded failed for region {}: {}", normalizedRegion, e.message)
                            null
                        }
                    }

                    if (response != null) {
                        val headers = response.headers
                        rateLimiter.updateLimits(
                            region = normalizedRegion,
                            limitHeader = headers.getFirst("X-App-Rate-Limit"),
                            countHeader = headers.getFirst("X-App-Rate-Limit-Count"),
                            endpointKey = endpointKey,
                            methodLimitHeader = headers.getFirst("X-Method-Rate-Limit"),
                            methodCountHeader = headers.getFirst("X-Method-Rate-Limit-Count")
                        )
                        return response.body
                    }
                    return null
                } catch (e: HttpClientErrorException.TooManyRequests) {
                    attempts++
                    val rateLimitType = e.responseHeaders?.getFirst("X-Rate-Limit-Type")
                    failureReasonHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                    rateLimiter.updateLimits(
                        region = normalizedRegion,
                        limitHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit"),
                        countHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit-Count"),
                        endpointKey = endpointKey,
                        methodLimitHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit"),
                        methodCountHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit-Count")
                    )
                    val retryAfterSeconds = e.responseHeaders?.getFirst("Retry-After")?.toLongOrNull()
                    val baseWaitMs = when {
                        retryAfterSeconds != null -> (retryAfterSeconds * 1000) + 100
                        rateLimitType.equals("service", ignoreCase = true) -> RiotApiRateLimiter.SERVICE_429_MIN_BACKOFF_MS
                        else -> 1_100L
                    }
                    rateLimiter.recordRetryAfter(normalizedRegion, baseWaitMs, rateLimitType)
                    if (attempts > effectiveMaxRetries) {
                        return null
                    }
                    if (deadlineAtMs != null && timeProvider.now() + baseWaitMs >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    kotlinx.coroutines.delay(baseWaitMs)
                } catch (e: org.springframework.web.client.ResourceAccessException) {
                    val abortedByGuard = generateSequence<Throwable>(e) { it.cause }
                        .filterIsInstance<BoundedLeagueStreamAbortedException>()
                        .firstOrNull()
                    if (abortedByGuard != null) {
                        failureReasonHolder?.set(abortedByGuard.reason)
                        return null
                    }
                    val currentReason = failureReasonHolder?.get()
                    if (currentReason == "MEMORY_HEADROOM_STOP" || currentReason == "RESPONSE_TOO_LARGE" || currentReason == "DEADLINE_EXCEEDED") {
                        return null
                    }
                    if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    attempts++
                    failureReasonHolder?.set("NETWORK_TIMEOUT")
                    if (attempts > effectiveMaxRetries) {
                        return null
                    }
                    val waitMs = 1000L * attempts
                    if (deadlineAtMs != null && timeProvider.now() + waitMs >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    kotlinx.coroutines.delay(waitMs)
                }
            }
        } finally {
            if (priority == ApiPriority.HIGH) {
                rateLimiter.endUserRequest(normalizedRegion)
            }
        }
    }

    /**
     * Fetches the Challenger league in a single bounded Riot API call with zero per-entry
     * `account-v1-by-puuid` fanout. Identity enrichment is performed locally by [ChallengerLeagueService]
     * using cached/last-known-good snapshots and local DB `indexed_players` rows.
     */
    suspend fun fetchChallengerLeague(region: String, queue: String): LeagueListDTO? {
        return fetchChallengerLeagueBounded(
            region = region,
            queue = queue,
            deadlineAtMs = timeProvider.now() + 5_000L
        )
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
        val anchor = kotlin.coroutines.coroutineContext[MatchIdsIncrementalAnchorElement]?.anchor
            ?.takeIf { it.knownOrderedSeasonalIds.isNotEmpty() }
        val knownIndexById = if (anchor != null) {
            HashMap<String, Int>(anchor.knownOrderedSeasonalIds.size * 2).apply {
                anchor.knownOrderedSeasonalIds.forEachIndexed { idx, id ->
                    putIfAbsent(id.trim(), idx)
                }
            }
        } else {
            null
        }
        val knownExcludedSet = anchor?.knownExcludedIds

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

            if (anchor != null && knownIndexById != null && knownExcludedSet != null &&
                matchIds.size >= ANCHOR_MIN_CONTIGUOUS_KNOWN_TAIL
            ) {
                val tailStartIdx = matchIds.size - ANCHOR_MIN_CONTIGUOUS_KNOWN_TAIL
                var tailAllKnown = true
                val tailSeasonalIndices = ArrayList<Int>(ANCHOR_MIN_CONTIGUOUS_KNOWN_TAIL)
                for (i in tailStartIdx until matchIds.size) {
                    val trimmed = matchIds[i].trim()
                    val seasonalIdx = knownIndexById[trimmed]
                    if (seasonalIdx != null) {
                        tailSeasonalIndices.add(seasonalIdx)
                    } else if (trimmed !in knownExcludedSet) {
                        tailAllKnown = false
                        break
                    }
                }
                var tailStrictlyConsecutive = tailSeasonalIndices.isNotEmpty()
                if (tailAllKnown && tailStrictlyConsecutive) {
                    for (k in 1 until tailSeasonalIndices.size) {
                        if (tailSeasonalIndices[k] != tailSeasonalIndices[k - 1] + 1) {
                            tailStrictlyConsecutive = false
                            break
                        }
                    }
                }
                if (tailAllKnown && tailStrictlyConsecutive) {
                    val bridgeEndIdx = tailSeasonalIndices.last()
                    val fetchedLookup = HashSet<String>(allMatchIds.size * 2)
                    for (id in allMatchIds) {
                        fetchedLookup.add(id.trim())
                    }
                    // Verify zero upstream missing IDs across knownOrderedSeasonalIds[0..bridgeEndIdx] before splicing.
                    var prefixComplete = true
                    for (idx in 0..bridgeEndIdx) {
                        if (anchor.knownOrderedSeasonalIds[idx] !in fetchedLookup) {
                            prefixComplete = false
                            break
                        }
                    }
                    if (prefixComplete) {
                        for (idx in (bridgeEndIdx + 1) until anchor.knownOrderedSeasonalIds.size) {
                            val tailKnownId = anchor.knownOrderedSeasonalIds[idx]
                            if (tailKnownId !in fetchedLookup) {
                                allMatchIds.add(tailKnownId)
                            }
                        }
                        hitSafetyCap = anchor.priorHistoryCapped || allMatchIds.size > 2000
                        exhaustedCleanly = !hitSafetyCap
                        logger.info(
                            "Anchored match ID pagination bridged cleanly at index={} for {} ({}) after {} page(s); spliced total={} IDs.",
                            bridgeEndIdx, puuid, region, pagesFetched, allMatchIds.size
                        )
                        break
                    }
                }
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
        }?.copy(dtoSchemaVersion = MatchDto.CURRENT_DTO_SCHEMA_VERSION)
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
        }?.copy(dtoSchemaVersion = MatchDto.CURRENT_DTO_SCHEMA_VERSION)
    }

    suspend fun getMatchTimeline(matchId: String, region: String): com.pride.summoner_searcher_api.dto.MatchTimelineDto? {
        return getMatchTimelineBounded(
            matchId = matchId,
            region = region,
            priority = ApiPriority.LOW,
            maxRetries = 0,
            maxTotalDurationMs = DEFAULT_SINGLE_TIMELINE_DEADLINE_MS
        )
    }

    /**
     * Fetches a single match timeline with strict bounds on:
     * - Rate-limiter priority ([ApiPriority.LOW] by default so coach/timeline prep yields to foreground searches and caps at 80%),
     * - Permit & bucket-mutex wait budget ([maxTotalDurationMs]),
     * - Wire & GZIP-inflated response payload size ([maxTimelineResponseBytes], default 4 MiB),
     * - Mid-stream memory headroom ([headroomGuard], e.g. 360 MiB stop guard),
     * - 2xx and 429 rate-limit header updates (`deadlineAtMs` passed to [RiotApiRateLimiter.updateLimits]),
     * - Bounded HTTP executor saturation (`HTTP_EXECUTOR_SATURATED`) and upstream 5xx (`UPSTREAM_5XX`).
     */
    suspend fun getMatchTimelineBounded(
        matchId: String,
        region: String,
        priority: ApiPriority = ApiPriority.LOW,
        maxRetries: Int = 0,
        httpAttemptCounter: java.util.concurrent.atomic.AtomicInteger? = null,
        failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null,
        maxTotalDurationMs: Long? = null,
        headroomGuard: (() -> Boolean)? = null
    ): com.pride.summoner_searcher_api.dto.MatchTimelineDto? {
        val normalizedRegion = region.trim().lowercase()
        val trimmedMatchId = matchId.trim()
        if (normalizedRegion.isEmpty() || trimmedMatchId.isEmpty()) {
            failureReasonHolder?.set("INVALID_ARGUMENT")
            return null
        }

        val deadlineAtMs = maxTotalDurationMs?.let { timeProvider.now() + it.coerceAtLeast(0L) }
        if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
            failureReasonHolder?.set("DEADLINE_EXCEEDED")
            return null
        }
        if (headroomGuard != null && !headroomGuard.invoke()) {
            failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
            return null
        }

        val regionalRouting = mapToRegionRouting(normalizedRegion)
        val matchBaseUrl = matchBaseUrlResolver(regionalRouting)
        val uri = "$matchBaseUrl/lol/match/v5/matches/{matchId}/timeline"
        val endpointKey = "match-v5-timeline-by-id"
        val effectiveMaxRetries = maxRetries.coerceAtLeast(0)
        val safeMaxBytes = maxTimelineResponseBytes.coerceIn(4_096, MAX_TIMELINE_RESPONSE_BYTES * 2)

        if (priority == ApiPriority.HIGH) {
            rateLimiter.beginUserRequest(normalizedRegion)
        }
        try {
            var attempts = 0
            while (true) {
                val nowBeforePermit = timeProvider.now()
                if (deadlineAtMs != null && nowBeforePermit >= deadlineAtMs) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                    return null
                }
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }

                if (deadlineAtMs != null) {
                    val remainingWaitMs = (deadlineAtMs - nowBeforePermit).coerceAtLeast(0L)
                    val activeCooldownMs = rateLimiter.getActiveRetryAfterWaitMs(normalizedRegion)
                    if (activeCooldownMs > 0L && activeCooldownMs + 10L > remainingWaitMs) {
                        failureReasonHolder?.set("RATE_LIMIT_COOLDOWN")
                        return null
                    }
                    val acquired = rateLimiter.tryAcquirePermit(
                        region = normalizedRegion,
                        priority = priority,
                        endpointKey = endpointKey,
                        maxWaitMs = remainingWaitMs
                    )
                    if (!acquired) {
                        val postCooldownMs = rateLimiter.getActiveRetryAfterWaitMs(normalizedRegion)
                        val remainingAfter = (deadlineAtMs - timeProvider.now()).coerceAtLeast(0L)
                        if (postCooldownMs > 0L && postCooldownMs + 10L > remainingAfter) {
                            failureReasonHolder?.set("RATE_LIMIT_COOLDOWN")
                        } else {
                            failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        }
                        return null
                    }
                } else {
                    if (priority == ApiPriority.LOW) {
                        rateLimiter.awaitLowPriorityClearance(normalizedRegion)
                    }
                    rateLimiter.acquirePermit(normalizedRegion, priority, endpointKey)
                }

                if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                    return null
                }
                if (headroomGuard != null && !headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    return null
                }

                try {
                    httpAttemptCounter?.incrementAndGet()
                    val response = runBoundedHttpCallWithDeadline(
                        deadlineAtMs = deadlineAtMs,
                        failureReasonHolder = failureReasonHolder,
                        maxShutdownJoinMs = 50L
                    ) { cancellationHandle ->
                        try {
                            riotRestClient.get()
                                .uri(uri, trimmedMatchId)
                                .header(org.springframework.http.HttpHeaders.ACCEPT_ENCODING, "gzip")
                                .exchange { _, clientResponse ->
                                    cancellationHandle.registerCloseable(AutoCloseable {
                                        abortResponseBody(clientResponse)
                                    })
                                    val status = clientResponse.statusCode
                                    val headers = clientResponse.headers
                                    val declaredLength = headers.contentLength
                                    when {
                                        status.value() == 404 -> {
                                            failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                                            logger.warn("getMatchTimelineBounded returned 404 for matchId: {}", trimmedMatchId)
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        status.value() == 429 -> {
                                            abortResponseBody(clientResponse)
                                            throw HttpClientErrorException.create(
                                                status,
                                                "Too Many Requests",
                                                headers,
                                                ByteArray(0),
                                                null
                                            )
                                        }
                                        status.is5xxServerError -> {
                                            failureReasonHolder?.set("UPSTREAM_5XX")
                                            logger.warn("getMatchTimelineBounded upstream {} for matchId: {}", status.value(), trimmedMatchId)
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        !status.is2xxSuccessful -> {
                                            failureReasonHolder?.set("HTTP_ERROR")
                                            logger.warn("getMatchTimelineBounded HTTP {} for matchId: {}", status.value(), trimmedMatchId)
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        declaredLength > safeMaxBytes -> {
                                            failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                                            logger.warn(
                                                "getMatchTimelineBounded Content-Length {} exceeds maxTimelineResponseBytes {} for matchId {}",
                                                declaredLength, safeMaxBytes, trimmedMatchId
                                            )
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        cancellationHandle.isCancelled() || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) -> {
                                            failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        headroomGuard != null && !headroomGuard.invoke() -> {
                                            failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                                            abortResponseBody(clientResponse)
                                            ResponseEntity.status(status).headers(headers).body(null)
                                        }
                                        else -> {
                                            val rawStream = clientResponse.body
                                            val boundedWireStream = BoundedLeagueInputStream(
                                                delegate = rawStream,
                                                response = clientResponse,
                                                cancellationHandle = cancellationHandle,
                                                deadlineAtMs = deadlineAtMs,
                                                maxResponseBytes = safeMaxBytes,
                                                headroomGuard = headroomGuard,
                                                failureReasonHolder = failureReasonHolder
                                            )
                                            cancellationHandle.registerCloseable(boundedWireStream)
                                            val isGzipEncoded = headers.getFirst("Content-Encoding")
                                                ?.contains("gzip", ignoreCase = true) == true
                                            val decodeStream: java.io.InputStream = if (isGzipEncoded) {
                                                BoundedLeagueInputStream(
                                                    delegate = java.util.zip.GZIPInputStream(boundedWireStream, 2_048),
                                                    response = clientResponse,
                                                    cancellationHandle = cancellationHandle,
                                                    deadlineAtMs = deadlineAtMs,
                                                    maxResponseBytes = safeMaxBytes,
                                                    headroomGuard = headroomGuard,
                                                    failureReasonHolder = failureReasonHolder,
                                                    headroomCheckStrideBytes = 16_384,
                                                    maxReadChunkBytes = 4_096
                                                ).also { cancellationHandle.registerCloseable(it) }
                                            } else {
                                                boundedWireStream
                                            }
                                            val decoded = leagueObjectMapper.readValue(
                                                decodeStream,
                                                com.pride.summoner_searcher_api.dto.MatchTimelineDto::class.java
                                            )
                                            if (cancellationHandle.isCancelled() || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)) {
                                                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                                abortResponseBody(clientResponse, rawStream)
                                                ResponseEntity.status(status).headers(headers).body(null)
                                            } else if (headroomGuard != null && !headroomGuard.invoke()) {
                                                failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                                                abortResponseBody(clientResponse, rawStream)
                                                ResponseEntity.status(status).headers(headers).body(null)
                                            } else {
                                                ResponseEntity.status(status).headers(headers).body(decoded)
                                            }
                                        }
                                    }
                                }
                        } catch (e: BoundedLeagueStreamAbortedException) {
                            failureReasonHolder?.set(e.reason)
                            logger.warn("getMatchTimelineBounded aborted stream decode ({}) for matchId {}", e.reason, trimmedMatchId)
                            null
                        } catch (e: HttpClientErrorException.NotFound) {
                            failureReasonHolder?.set("HTTP_404_NOT_FOUND")
                            null
                        } catch (e: HttpClientErrorException.TooManyRequests) {
                            throw e
                        } catch (e: org.springframework.web.client.HttpServerErrorException) {
                            failureReasonHolder?.set("UPSTREAM_5XX")
                            logger.warn("getMatchTimelineBounded upstream 5xx for matchId {}: {}", trimmedMatchId, e.statusCode)
                            null
                        } catch (e: org.springframework.web.client.ResourceAccessException) {
                            val abortedByGuard = generateSequence<Throwable>(e) { it.cause }
                                .filterIsInstance<BoundedLeagueStreamAbortedException>()
                                .firstOrNull()
                            val hasInterrupt = generateSequence<Throwable>(e) { it.cause }
                                .any { it is InterruptedException || it is java.io.InterruptedIOException }
                            if (abortedByGuard != null) {
                                failureReasonHolder?.set(abortedByGuard.reason)
                                null
                            } else if (cancellationHandle.isCancelled() || hasInterrupt || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs)) {
                                val currentReason = failureReasonHolder?.get()
                                if (currentReason != "MEMORY_HEADROOM_STOP" && currentReason != "RESPONSE_TOO_LARGE") {
                                    failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                }
                                null
                            } else {
                                throw e
                            }
                        } catch (e: Exception) {
                            val abortedByGuard = generateSequence<Throwable>(e) { it.cause }
                                .filterIsInstance<BoundedLeagueStreamAbortedException>()
                                .firstOrNull()
                            val hasInterrupt = generateSequence<Throwable>(e) { it.cause }
                                .any { it is InterruptedException || it is java.io.InterruptedIOException }
                            when {
                                abortedByGuard != null -> failureReasonHolder?.set(abortedByGuard.reason)
                                cancellationHandle.isCancelled() || hasInterrupt || (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) -> {
                                    val currentReason = failureReasonHolder?.get()
                                    if (currentReason != "MEMORY_HEADROOM_STOP" && currentReason != "RESPONSE_TOO_LARGE") {
                                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                                    }
                                }
                                else -> {
                                    if (failureReasonHolder?.get() == null) {
                                        failureReasonHolder?.set("HTTP_ERROR")
                                    }
                                }
                            }
                            logger.warn("getMatchTimelineBounded failed for matchId {}: {}", trimmedMatchId, e.message)
                            null
                        }
                    }

                    if (response != null) {
                        val headers = response.headers
                        rateLimiter.updateLimits(
                            region = normalizedRegion,
                            limitHeader = headers.getFirst("X-App-Rate-Limit"),
                            countHeader = headers.getFirst("X-App-Rate-Limit-Count"),
                            endpointKey = endpointKey,
                            methodLimitHeader = headers.getFirst("X-Method-Rate-Limit"),
                            methodCountHeader = headers.getFirst("X-Method-Rate-Limit-Count"),
                            deadlineAtMs = deadlineAtMs
                        )
                        return response.body
                    }
                    return null
                } catch (e: HttpClientErrorException.TooManyRequests) {
                    attempts++
                    val rateLimitType = e.responseHeaders?.getFirst("X-Rate-Limit-Type")
                    failureReasonHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                    rateLimiter.updateLimits(
                        region = normalizedRegion,
                        limitHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit"),
                        countHeader = e.responseHeaders?.getFirst("X-App-Rate-Limit-Count"),
                        endpointKey = endpointKey,
                        methodLimitHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit"),
                        methodCountHeader = e.responseHeaders?.getFirst("X-Method-Rate-Limit-Count"),
                        deadlineAtMs = deadlineAtMs
                    )
                    val retryAfterSeconds = e.responseHeaders?.getFirst("Retry-After")?.toLongOrNull()
                    val baseWaitMs = when {
                        retryAfterSeconds != null -> (retryAfterSeconds * 1000) + 100
                        rateLimitType.equals("service", ignoreCase = true) -> RiotApiRateLimiter.SERVICE_429_MIN_BACKOFF_MS
                        else -> 1_100L
                    }
                    rateLimiter.recordRetryAfter(normalizedRegion, baseWaitMs, rateLimitType)
                    if (attempts > effectiveMaxRetries) {
                        return null
                    }
                    if (deadlineAtMs != null && timeProvider.now() + baseWaitMs >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    kotlinx.coroutines.delay(baseWaitMs)
                } catch (e: org.springframework.web.client.ResourceAccessException) {
                    val abortedByGuard = generateSequence<Throwable>(e) { it.cause }
                        .filterIsInstance<BoundedLeagueStreamAbortedException>()
                        .firstOrNull()
                    if (abortedByGuard != null) {
                        failureReasonHolder?.set(abortedByGuard.reason)
                        return null
                    }
                    val currentReason = failureReasonHolder?.get()
                    if (currentReason == "MEMORY_HEADROOM_STOP" || currentReason == "RESPONSE_TOO_LARGE" || currentReason == "DEADLINE_EXCEEDED") {
                        return null
                    }
                    if (deadlineAtMs != null && timeProvider.now() >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    attempts++
                    failureReasonHolder?.set("NETWORK_TIMEOUT")
                    if (attempts > effectiveMaxRetries) {
                        return null
                    }
                    val waitMs = 1000L * attempts
                    if (deadlineAtMs != null && timeProvider.now() + waitMs >= deadlineAtMs) {
                        failureReasonHolder?.set("DEADLINE_EXCEEDED")
                        return null
                    }
                    kotlinx.coroutines.delay(waitMs)
                }
            }
        } finally {
            if (priority == ApiPriority.HIGH) {
                rateLimiter.endUserRequest(normalizedRegion)
            }
        }
    }

    private fun getPreferredContent(translations: List<RiotContent>): String? {
        return translations.find { it.locale.equals("en_US", ignoreCase = true) }?.content
            ?: translations.firstOrNull()?.content
    }

    private fun formatPlainStatusLabel(raw: String?): String? {
        val normalized = raw?.trim()?.takeIf { it.isNotBlank() } ?: return null
        return when (normalized.lowercase()) {
            "scheduled" -> "Scheduled"
            "in_progress" -> "In Progress"
            "complete", "completed" -> "Completed"
            "active" -> "Active"
            "resolved" -> "Resolved"
            else -> normalized
                .split('_', '-')
                .filter { it.isNotBlank() }
                .joinToString(" ") { word ->
                    word.lowercase().replaceFirstChar { ch -> ch.titlecase() }
                }
                .takeIf { it.isNotBlank() }
        }
    }

    private fun formatPlainSeverityLabel(raw: String?): String? {
        val normalized = raw?.trim()?.takeIf { it.isNotBlank() } ?: return null
        return when (normalized.lowercase()) {
            "info" -> "Info"
            "warning" -> "Warning"
            "critical" -> "Critical"
            else -> formatPlainStatusLabel(normalized)
        }
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
                        status = formatPlainStatusLabel(maintenance.maintenanceStatus),
                        severity = formatPlainSeverityLabel(maintenance.incidentSeverity),
                        title = getPreferredContent(maintenance.titles) ?: "League Server Maintenance",
                        description = maintenance.updates.firstOrNull()?.let { update -> getPreferredContent(update.translations) },
                        platforms = maintenance.platforms
                    )
                },
                incidents = it.incidents.map { incident ->
                    StatusItemDto(
                        status = if (incident.active) "Active" else "Resolved",
                        severity = formatPlainSeverityLabel(incident.incidentSeverity),
                        title = getPreferredContent(incident.titles) ?: "League Server Notice",
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
