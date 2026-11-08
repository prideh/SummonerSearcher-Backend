package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.config.RestClientConfig
import com.pride.summoner_searcher_api.controller.RiotApiController
import com.pride.summoner_searcher_api.dto.CachedLeaderboardDto
import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import com.pride.summoner_searcher_api.model.IndexedPlayer
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import com.sun.net.httpserver.HttpServer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class ChallengerLeagueServiceTest {

    private val objectMapper = ObjectMapper().findAndRegisterModules()

    private class MutableClock(initialMs: Long = 1_760_000_000_000L) : TimeProvider {
        private val nowMs = AtomicLong(initialMs)
        override fun now(): Long = nowMs.get()
        fun advanceBy(deltaMs: Long) {
            nowMs.addAndGet(deltaMs)
        }
    }

    private class MutableMemoryProvider(
        @Volatile var cgroupUsedBytes: Long = 320L * 1024L * 1024L,
        @Volatile var cgroupMaxBytes: Long = 512L * 1024L * 1024L,
        @Volatile var heapUsedBytes: Long = 160L * 1024L * 1024L,
        @Volatile var heapMaxBytes: Long = 256L * 1024L * 1024L
    ) : MemoryStatsProvider {
        override fun sample(): RawMemorySample = RawMemorySample(
            jvmHeapUsedBytes = heapUsedBytes,
            jvmHeapCommittedBytes = heapMaxBytes,
            jvmHeapMaxBytes = heapMaxBytes,
            jvmNonHeapUsedBytes = 64L * 1024L * 1024L,
            cgroupMemoryCurrentBytes = cgroupUsedBytes,
            cgroupMemoryMaxBytes = cgroupMaxBytes
        )
    }

    private fun buildLeagueJson(entryCount: Int, regionPrefix: String = "euw1"): ByteArray {
        val entries = (1..entryCount).map { idx ->
            LeagueEntryDto(
                puuid = "$regionPrefix-puuid-$idx",
                leaguePoints = 2_000 - idx,
                rank = "I",
                wins = 120 + (idx % 30),
                losses = 90 + (idx % 20),
                veteran = idx % 2 == 0,
                inactive = false,
                freshBlood = idx % 5 == 0,
                hotStreak = idx % 3 == 0,
                leagueId = "challenger-$regionPrefix",
                queueType = "RANKED_SOLO_5x5",
                tier = "CHALLENGER",
                summonerName = "LegacySummoner$idx",
                gameName = null,
                tagLine = null
            )
        }
        val dto = LeagueListDTO(
            tier = "CHALLENGER",
            leagueId = "challenger-$regionPrefix",
            queue = "RANKED_SOLO_5x5",
            name = "Challenger League $regionPrefix",
            entries = entries
        )
        return objectMapper.writeValueAsString(dto).toByteArray(StandardCharsets.UTF_8)
    }

    @Test
    fun `absent or evicted EUW and KR Redis keys self-heal via single league call with local DB identity reuse and zero 300-account fanout`() = runBlocking {
        val leagueRequestsByRegion = ConcurrentHashMap<String, AtomicInteger>()
        val accountFanoutRequests = AtomicInteger(0)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool()
        server.createContext("/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5") { exchange ->
            val region = exchange.requestHeaders.getFirst("X-Test-Region") ?: "euw1"
            leagueRequestsByRegion.computeIfAbsent(region) { AtomicInteger(0) }.incrementAndGet()
            val payload = buildLeagueJson(entryCount = 300, regionPrefix = region)
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
            exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
            exchange.sendResponseHeaders(200, payload.size.toLong())
            exchange.responseBody.use { it.write(payload) }
        }
        server.createContext("/riot/account/v1/accounts/by-puuid") { exchange ->
            accountFanoutRequests.incrementAndGet()
            exchange.sendResponseHeaders(500, -1)
            exchange.close()
        }
        server.start()

        try {
            val port = server.address.port
            val clock = MutableClock()
            val memoryProvider = MutableMemoryProvider()
            val rateLimiter = RiotApiRateLimiter(clock)
            val restClient = RestClientConfig.buildRiotRestClient("test-key")
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, clock).apply {
                platformBaseUrlResolver = { _ -> "http://127.0.0.1:$port" }
            }

            val redisStore = ConcurrentHashMap<String, CachedLeaderboardDto>()
            // Simulate production state: NA1 present in Redis, EUW1 and KR absent
            redisStore["leaderboard:na1::RANKED_SOLO_5x5"] = CachedLeaderboardDto(
                lastRefreshed = Instant.ofEpochMilli(clock.now() - 60_000L),
                leaderboard = LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-na1",
                    queue = "RANKED_SOLO_5x5",
                    name = "NA Challenger",
                    entries = listOf(
                        LeagueEntryDto(
                            puuid = "na1-puuid-1",
                            leaguePoints = 1999,
                            rank = "I",
                            wins = 100,
                            losses = 50,
                            veteran = false,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = true,
                            leagueId = "challenger-na1",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "NASummoner1",
                            gameName = "NAOne",
                            tagLine = "NA1"
                        )
                    )
                )
            )

            val redisCacheService = mockk<RedisCacheService>()
            every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } answers {
                redisStore[firstArg()]
            }
            every { redisCacheService.set(any(), any<CachedLeaderboardDto>(), any()) } answers {
                redisStore[firstArg()] = secondArg()
            }

            val dbFindAllCount = AtomicInteger(0)
            val indexedPlayerRepository = mockk<IndexedPlayerRepository>()
            every { indexedPlayerRepository.findAllById(any()) } answers {
                dbFindAllCount.incrementAndGet()
                val ids = firstArg<Iterable<String>>().toList()
                ids.take(5).mapIndexed { index, puuid ->
                    IndexedPlayer(
                        puuid = puuid,
                        gameName = "LocalDbPlayer${index + 1}",
                        tagLine = "EUW",
                        region = "euw1",
                        profileIconId = 10,
                        summonerLevel = 500L,
                        lastSeenAt = Instant.ofEpochMilli(clock.now())
                    )
                }
            }

            val service = ChallengerLeagueService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                memoryStatsProvider = memoryProvider
            )
            val controller = RiotApiController(
                riotApiService = riotApiService,
                challengerLeagueService = service,
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = redisCacheService
            )

            // 1. NA1 hits Redis directly with 0 Riot calls
            val naResponse = controller.getChallengerLeague("na1")
            assertEquals(HttpStatus.OK, naResponse.statusCode)
            assertEquals(1, naResponse.body?.entries?.size)
            assertEquals("NAOne", naResponse.body?.entries?.first()?.gameName)
            assertEquals(0, leagueRequestsByRegion.values.sumOf { it.get() })

            // 2. EUW1 key is absent in Redis -> self-heals with ONE league call, 0 account fanout, and local DB identity reuse
            val euwResponse = controller.getChallengerLeague("euw1")
            assertEquals(HttpStatus.OK, euwResponse.statusCode)
            val euwBody = euwResponse.body
            assertNotNull(euwBody)
            assertEquals(300, euwBody!!.entries.size)
            assertEquals("LocalDbPlayer1", euwBody.entries[0].gameName)
            assertEquals("EUW", euwBody.entries[0].tagLine)
            assertEquals(1, leagueRequestsByRegion.values.sumOf { it.get() })
            assertEquals(0, accountFanoutRequests.get(), "Must never perform 300-account getAccountByPuuid fanout")
            assertEquals(1, dbFindAllCount.get(), "Should batch local DB lookup in a single findAllById call")
            assertNotNull(redisStore["leaderboard:euw1::RANKED_SOLO_5x5"])

            // 3. Subsequent EUW1 call is served from re-populated Redis cache with 0 extra Riot calls
            val euwSecond = controller.getChallengerLeague("euw1")
            assertEquals(HttpStatus.OK, euwSecond.statusCode)
            assertEquals(300, euwSecond.body?.entries?.size)
            assertEquals(1, leagueRequestsByRegion.values.sumOf { it.get() })
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `malformed or empty Redis payload is rejected and self-healed or falls back to bounded 3-region last-known-good`() = runBlocking {
        val clock = MutableClock()
        val memoryProvider = MutableMemoryProvider()
        val redisStore = ConcurrentHashMap<String, CachedLeaderboardDto?>()
        val redisCacheService = mockk<RedisCacheService>()
        every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } answers {
            redisStore[firstArg()]
        }
        every { redisCacheService.set(any(), any<CachedLeaderboardDto>(), any()) } answers {
            redisStore[firstArg()] = secondArg()
        }

        val riotApiService = mockk<RiotApiService>()
        val leagueCallCounter = AtomicInteger(0)
        val shouldFailUpstream = AtomicBoolean(false)

        io.mockk.coEvery {
            riotApiService.fetchChallengerLeagueBounded(
                region = any(),
                queue = any(),
                priority = any(),
                maxRetries = any(),
                deadlineAtMs = any(),
                maxResponseBytes = any(),
                maxEntries = any(),
                headroomGuard = any(),
                httpAttemptCounter = any(),
                failureReasonHolder = any()
            )
        } answers {
            val region = arg<String>(0)
            val attemptCounter = arg<AtomicInteger?>(8)
            val failureHolder = arg<AtomicReference<String?>?>(9)
            leagueCallCounter.incrementAndGet()
            attemptCounter?.incrementAndGet()
            if (shouldFailUpstream.get()) {
                failureHolder?.set("HTTP_503")
                null
            } else {
                LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-$region",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger $region",
                    entries = (1..350).map { idx ->
                        LeagueEntryDto(
                            puuid = "$region-puuid-$idx",
                            leaguePoints = 2500 - idx,
                            rank = "I",
                            wins = 100,
                            losses = 80,
                            veteran = true,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = false,
                            leagueId = "challenger-$region",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "Player$idx",
                            gameName = "Player$idx",
                            tagLine = region.uppercase()
                        )
                    }
                )
            }
        }

        val service = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            indexedPlayerRepository = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            maxEntriesPerRegion = 300,
            maxLastKnownGoodRegionBytes = 24_576, // 24 KiB strict per-region byte cap
            maxLastKnownGoodTotalBytes = 65_536   // 64 KiB strict 3-region total byte cap
        )

        // 1. Seed malformed empty-entries payload in Redis for euw1 -> service rejects empty payload and self-heals
        redisStore["leaderboard:euw1::RANKED_SOLO_5x5"] = CachedLeaderboardDto(
            lastRefreshed = Instant.ofEpochMilli(clock.now()),
            leaderboard = LeagueListDTO("CHALLENGER", "id", "RANKED_SOLO_5x5", "Empty", emptyList())
        )
        val euwRecovered = service.getChallengerLeagueFromCache("euw1", "RANKED_SOLO_5x5")
        assertNotNull(euwRecovered)
        assertTrue(euwRecovered!!.entries.isNotEmpty())
        assertTrue(euwRecovered.entries.size <= 300)
        assertEquals(1, leagueCallCounter.get())

        // 2. Populate na1 and kr as well; verify 3-region cap, entry cap, and strict byte bounds
        assertNotNull(service.getChallengerLeagueFromCache("na1", "RANKED_SOLO_5x5"))
        assertNotNull(service.getChallengerLeagueFromCache("kr", "RANKED_SOLO_5x5"))
        assertNull(service.getChallengerLeagueFromCache("br1", "RANKED_SOLO_5x5"), "Unsupported 4th region must not be stored")
        assertEquals(3, service.getLastKnownGoodRegionCount())
        assertTrue(service.getLastKnownGoodRegionBytes("euw1") in 1..24_576)
        assertTrue(service.getLastKnownGoodRegionBytes("na1") in 1..24_576)
        assertTrue(service.getLastKnownGoodRegionBytes("kr") in 1..24_576)
        assertTrue(service.getLastKnownGoodTotalBytes() <= 65_536)

        // 3. Evict euw1 from Redis AND make upstream fail -> serves bounded last-known-good snapshot and re-seeds Redis
        redisStore.remove("leaderboard:euw1::RANKED_SOLO_5x5")
        shouldFailUpstream.set(true)
        val euwFromLastGood = service.getChallengerLeagueFromCache("euw1", "RANKED_SOLO_5x5")
        assertNotNull(euwFromLastGood)
        assertTrue(euwFromLastGood!!.entries.isNotEmpty())
        assertEquals("euw1-puuid-1", euwFromLastGood.entries.first().puuid)
        assertNotNull(redisStore["leaderboard:euw1::RANKED_SOLO_5x5"], "Should re-seed Redis from last-known-good")
    }

    @Test
    fun `singleflight coalesces concurrent cache misses into one league call, cleans up on waiter cancellation, and enforces failure backoff`() = runBlocking {
        val clock = MutableClock()
        val memoryProvider = MutableMemoryProvider()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } returns null

        val riotApiService = mockk<RiotApiService>()
        val upstreamCalls = AtomicInteger(0)
        val shouldFail = AtomicBoolean(false)

        io.mockk.coEvery {
            riotApiService.fetchChallengerLeagueBounded(
                region = any(),
                queue = any(),
                priority = any(),
                maxRetries = any(),
                deadlineAtMs = any(),
                maxResponseBytes = any(),
                maxEntries = any(),
                headroomGuard = any(),
                httpAttemptCounter = any(),
                failureReasonHolder = any()
            )
        } coAnswers {
            val region = arg<String>(0)
            val attemptCounter = arg<AtomicInteger?>(8)
            val failureHolder = arg<AtomicReference<String?>?>(9)
            upstreamCalls.incrementAndGet()
            attemptCounter?.incrementAndGet()
            delay(120L)
            if (shouldFail.get()) {
                failureHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                null
            } else {
                LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-$region",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger $region",
                    entries = listOf(
                        LeagueEntryDto(
                            puuid = "$region-puuid-1",
                            leaguePoints = 1800,
                            rank = "I",
                            wins = 90,
                            losses = 60,
                            veteran = false,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = true,
                            leagueId = "challenger-$region",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "Top1",
                            gameName = "TopOne",
                            tagLine = "EUW"
                        )
                    )
                )
            }
        }

        val service = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            indexedPlayerRepository = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            failureBackoffMs = 30_000L
        )
        val controller = RiotApiController(
            riotApiService = riotApiService,
            challengerLeagueService = service,
            userService = mockk(relaxed = true),
            summonerProfileService = mockk(relaxed = true),
            redisCacheService = redisCacheService
        )

        // 1. 10 concurrent callers on cache miss + 1 cancelled caller -> coalesced into 1 upstream call, cleaned up to 0 in-flight
        val cancelledJob = launch(Dispatchers.Default) {
            service.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
        }
        delay(25L)
        val concurrentWaiters = (1..10).map {
            async(Dispatchers.Default) {
                service.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
            }
        }
        cancelledJob.cancel(CancellationException("Simulated client disconnect"))
        val results = concurrentWaiters.awaitAll()

        assertEquals(1, upstreamCalls.get(), "All 11 concurrent callers must coalesce into 1 Riot API call")
        assertTrue(results.all { it != null && it.entries.size == 1 && it.entries.first().gameName == "TopOne" })
        assertEquals(0, service.getInFlightRefreshCount(), "Singleflight map must be empty after completion")

        // 2. Cold region (kr) with upstream failure -> returns 503 Service Unavailable, records failure backoff, and suppresses retries
        shouldFail.set(true)
        val krFirst = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, krFirst.statusCode)
        assertNull(krFirst.body)
        assertEquals(2, upstreamCalls.get())
        assertTrue(service.getRemainingBackoffMs("kr") > 0L)
        assertEquals("HTTP_429_TOO_MANY_REQUESTS", service.getLastRefreshFailureReason("kr"))
        assertEquals(0, service.getInFlightRefreshCount())

        // Subsequent KR calls during 30s backoff must make 0 additional upstream calls and return 503 immediately
        val krDuringBackoff = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, krDuringBackoff.statusCode)
        assertEquals(2, upstreamCalls.get(), "Must not call Riot API while region is in failure backoff")

        // Advance clock past 30s backoff and allow upstream recovery -> self-heals to 200 OK
        clock.advanceBy(31_000L)
        shouldFail.set(false)
        val krRecovered = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.OK, krRecovered.statusCode)
        assertEquals(1, krRecovered.body?.entries?.size)
        assertEquals(3, upstreamCalls.get())
        assertEquals(0L, service.getRemainingBackoffMs("kr"))
    }

    @Test
    fun `production ReactorNetty HTTP client enforces bounded chunked body size, deadline abort, and memory headroom guards`() = runBlocking {
        val oversizedSecondHalfCompleted = AtomicInteger(0)
        val stalledDeadlineCompleted = AtomicInteger(0)
        val midStreamHeadroomCompleted = AtomicInteger(0)
        val midStreamTrigger = AtomicBoolean(false)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool()
        server.createContext("/lol/league/v4/challengerleagues/by-queue/OVERSIZED_CHUNKED") { exchange ->
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, 0L) // chunked transfer encoding
            try {
                exchange.responseBody.use { out ->
                    val prefix = """{"tier":"CHALLENGER","leagueId":"id","queue":"RANKED_SOLO_5x5","name":"Big","entries":[""".toByteArray()
                    out.write(prefix)
                    out.flush()
                    val chunk = """{"puuid":"p1","leaguePoints":1000,"rank":"I","wins":10,"losses":5,"summonerName":"${"X".repeat(900)}"},""".toByteArray()
                    for (i in 1..40) {
                        out.write(chunk)
                        out.flush()
                        if (i == 20) {
                            Thread.sleep(50L)
                        }
                    }
                    oversizedSecondHalfCompleted.incrementAndGet()
                }
            } catch (_: Throwable) {
            } finally {
                exchange.close()
            }
        }
        server.createContext("/lol/league/v4/challengerleagues/by-queue/SLOW_CHUNKED_DEADLINE") { exchange ->
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, 0L)
            try {
                exchange.responseBody.use { out ->
                    out.write("""{"tier":"CHALLENGER","entries":[""".toByteArray())
                    out.flush()
                    Thread.sleep(4_000L)
                    stalledDeadlineCompleted.incrementAndGet()
                }
            } catch (_: Throwable) {
            } finally {
                exchange.close()
            }
        }
        server.createContext("/lol/league/v4/challengerleagues/by-queue/MID_STREAM_HEADROOM") { exchange ->
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, 0L)
            try {
                exchange.responseBody.use { out ->
                    val firstChunk = """{"tier":"CHALLENGER","leagueId":"id","queue":"RANKED_SOLO_5x5","name":"Mem","entries":[{"puuid":"p1","leaguePoints":1000,"summonerName":"${"M".repeat(1500)}"}""".toByteArray()
                    out.write(firstChunk)
                    out.flush()
                    midStreamTrigger.set(true)
                    Thread.sleep(80L)
                    out.write("""]}""".toByteArray())
                    out.flush()
                    Thread.sleep(3_000L)
                    midStreamHeadroomCompleted.incrementAndGet()
                }
            } catch (_: Throwable) {
            } finally {
                exchange.close()
            }
        }
        server.start()

        try {
            val port = server.address.port
            val timeProvider = SystemTimeProvider()
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            val restClient = RestClientConfig.buildRiotRestClient("test-key")
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider).apply {
                platformBaseUrlResolver = { _ -> "http://127.0.0.1:$port" }
            }

            // 1. Oversized chunked stream (> 8 KiB cap) aborts mid-stream with RESPONSE_TOO_LARGE
            run {
                val attempts = AtomicInteger(0)
                val reason = AtomicReference<String?>(null)
                val result = riotApiService.fetchChallengerLeagueBounded(
                    region = "euw1",
                    queue = "OVERSIZED_CHUNKED",
                    deadlineAtMs = timeProvider.now() + 3_000L,
                    maxResponseBytes = 8_192,
                    httpAttemptCounter = attempts,
                    failureReasonHolder = reason
                )
                assertNull(result)
                assertEquals(1, attempts.get())
                assertEquals("RESPONSE_TOO_LARGE", reason.get())
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
            }

            // 2. Slow chunked stream exceeding 220ms deadline aborts promptly with DEADLINE_EXCEEDED
            run {
                val attempts = AtomicInteger(0)
                val reason = AtomicReference<String?>(null)
                val startWall = System.currentTimeMillis()
                val result = riotApiService.fetchChallengerLeagueBounded(
                    region = "euw1",
                    queue = "SLOW_CHUNKED_DEADLINE",
                    deadlineAtMs = timeProvider.now() + 220L,
                    maxResponseBytes = 65_536,
                    httpAttemptCounter = attempts,
                    failureReasonHolder = reason
                )
                val elapsedWall = System.currentTimeMillis() - startWall
                assertNull(result)
                assertEquals(1, attempts.get())
                assertEquals("DEADLINE_EXCEEDED", reason.get())
                assertTrue(elapsedWall < 1_500L, "Deadline abort should finish near 220ms without waiting 4s, took ${elapsedWall}ms")
                assertEquals(0, stalledDeadlineCompleted.get())
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
            }

            // 3. Pre-flight memory headroom breach -> 0 HTTP attempts made
            run {
                val attempts = AtomicInteger(0)
                val reason = AtomicReference<String?>(null)
                val result = riotApiService.fetchChallengerLeagueBounded(
                    region = "euw1",
                    queue = "RANKED_SOLO_5x5",
                    deadlineAtMs = timeProvider.now() + 2_000L,
                    headroomGuard = { false },
                    httpAttemptCounter = attempts,
                    failureReasonHolder = reason
                )
                assertNull(result)
                assertEquals(0, attempts.get(), "Pre-flight memory headroom stop must make 0 HTTP calls")
                assertEquals("MEMORY_HEADROOM_STOP", reason.get())
            }

            // 4. Mid-stream memory headroom breach -> aborts chunked body mid-read with MEMORY_HEADROOM_STOP
            run {
                midStreamTrigger.set(false)
                val attempts = AtomicInteger(0)
                val reason = AtomicReference<String?>(null)
                val startWall = System.currentTimeMillis()
                val result = riotApiService.fetchChallengerLeagueBounded(
                    region = "euw1",
                    queue = "MID_STREAM_HEADROOM",
                    deadlineAtMs = timeProvider.now() + 4_000L,
                    headroomGuard = { !midStreamTrigger.get() },
                    httpAttemptCounter = attempts,
                    failureReasonHolder = reason
                )
                val elapsedWall = System.currentTimeMillis() - startWall
                assertNull(result)
                assertEquals(1, attempts.get())
                assertEquals("MEMORY_HEADROOM_STOP", reason.get())
                assertTrue(elapsedWall < 1_500L, "Mid-stream headroom abort should not wait 3s, took ${elapsedWall}ms")
                assertEquals(0, midStreamHeadroomCompleted.get())
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
            }
        } finally {
            server.stop(0)
        }
    }
}
