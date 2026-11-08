package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.config.RestClientConfig
import com.pride.summoner_searcher_api.controller.RiotApiController
import com.pride.summoner_searcher_api.dto.CachedLeaderboardDto
import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import com.sun.net.httpserver.HttpServer
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
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
    fun `absent or evicted EUW and KR Redis keys self-heal via single league call with bounded JDBC local DB identity reuse and zero 300-account fanout`() = runBlocking {
        val leagueRequestsByRegion = ConcurrentHashMap<String, AtomicInteger>()
        val accountFanoutRequests = AtomicInteger(0)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool()
        server.createContext("/euw1/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5") { exchange ->
            leagueRequestsByRegion.computeIfAbsent("euw1") { AtomicInteger(0) }.incrementAndGet()
            val payload = buildLeagueJson(entryCount = 300, regionPrefix = "euw1")
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
            exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
            exchange.sendResponseHeaders(200, payload.size.toLong())
            exchange.responseBody.use { it.write(payload) }
        }
        server.createContext("/kr/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5") { exchange ->
            leagueRequestsByRegion.computeIfAbsent("kr") { AtomicInteger(0) }.incrementAndGet()
            val payload = buildLeagueJson(entryCount = 300, regionPrefix = "kr")
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
                platformBaseUrlResolver = { r -> "http://127.0.0.1:$port/$r" }
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

            // Real H2 JDBC DataSource with indexed_players table (300/300 EUW1 rows, 5/300 KR rows)
            val dbName = "challenger_identity_${UUID.randomUUID().toString().replace("-", "")}"
            val h2DataSource = JdbcDataSource().apply {
                setURL("jdbc:h2:mem:$dbName;MODE=PostgreSQL;DB_CLOSE_DELAY=-1")
                user = "sa"
                password = ""
            }
            h2DataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        CREATE TABLE indexed_players (
                            puuid VARCHAR(128) PRIMARY KEY,
                            game_name VARCHAR(128) NOT NULL,
                            tag_line VARCHAR(64) NOT NULL,
                            region VARCHAR(32) NOT NULL
                        )
                        """.trimIndent()
                    )
                }
                conn.prepareStatement("INSERT INTO indexed_players (puuid, game_name, tag_line, region) VALUES (?, ?, ?, ?)").use { ps ->
                    for (idx in 1..300) {
                        ps.setString(1, "euw1-puuid-$idx")
                        ps.setString(2, "LocalDbPlayer$idx")
                        ps.setString(3, "EUW")
                        ps.setString(4, "euw1")
                        ps.addBatch()
                    }
                    for (idx in 1..5) {
                        ps.setString(1, "kr-puuid-$idx")
                        ps.setString(2, "KrIndexedPlayer$idx")
                        ps.setString(3, "KR1")
                        ps.setString(4, "kr")
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
            }

            val service = ChallengerLeagueService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                dataSource = h2DataSource,
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

            // 2. EUW1 key is absent in Redis -> self-heals with ONE league call, 0 account fanout, and 300/300 real JDBC identity coverage
            val euwResponse = controller.getChallengerLeague("euw1")
            assertEquals(HttpStatus.OK, euwResponse.statusCode)
            val euwBody = euwResponse.body
            assertNotNull(euwBody)
            assertEquals(300, euwBody!!.entries.size)
            assertEquals("LocalDbPlayer1", euwBody.entries[0].gameName)
            assertEquals("EUW", euwBody.entries[0].tagLine)
            assertEquals(300, euwBody.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })
            val euwCoverage = service.getLastIdentityCoverage("euw1")
            assertNotNull(euwCoverage)
            assertEquals(300, euwCoverage!!.totalEntries)
            assertEquals(300, euwCoverage.resolvedIdentityCount)
            assertEquals(0, euwCoverage.missingIdentityCount)
            assertEquals(300, euwCoverage.dbResolvedCount)
            assertEquals(1, leagueRequestsByRegion.values.sumOf { it.get() })
            assertEquals(0, accountFanoutRequests.get(), "Must never perform 300-account getAccountByPuuid fanout")
            assertNotNull(redisStore["leaderboard:euw1::RANKED_SOLO_5x5"])

            // 3. Subsequent EUW1 call is served from re-populated Redis cache with 0 extra Riot calls
            val euwSecond = controller.getChallengerLeague("euw1")
            assertEquals(HttpStatus.OK, euwSecond.statusCode)
            assertEquals(300, euwSecond.body?.entries?.size)
            assertEquals(1, leagueRequestsByRegion.values.sumOf { it.get() })

            // 4. KR key is absent in Redis with only 5/300 local DB identities -> explicitly tracks 295 missing identities (unclickable on Dashboard until indexed) with 0 account fanout
            val krResponse = controller.getChallengerLeague("kr")
            assertEquals(HttpStatus.OK, krResponse.statusCode)
            val krBody = krResponse.body
            assertNotNull(krBody)
            assertEquals(300, krBody!!.entries.size)
            assertEquals(5, krBody.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })
            assertEquals(295, krBody.entries.count { it.gameName.isNullOrBlank() || it.tagLine.isNullOrBlank() })
            val krCoverage = service.getLastIdentityCoverage("kr")
            assertNotNull(krCoverage)
            assertEquals(300, krCoverage!!.totalEntries)
            assertEquals(5, krCoverage.resolvedIdentityCount)
            assertEquals(295, krCoverage.missingIdentityCount)
            assertEquals(5, krCoverage.dbResolvedCount)
            assertEquals(0, accountFanoutRequests.get(), "Must never perform 300-account getAccountByPuuid fanout even when DB coverage is partial")
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `malformed or empty Redis payload is rejected and self-healed retaining only complete 300-entry fallback snapshots and rejecting oversized whole with HTTP 503`() = runBlocking {
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

        // Part A: Default production byte caps (128 KiB per region, 384 KiB total) retain the COMPLETE 300-entry snapshot for all 3 regions
        val serviceWithDefaultCaps = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            dataSource = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            maxEntriesPerRegion = 300
        )

        redisStore["leaderboard:euw1::RANKED_SOLO_5x5"] = CachedLeaderboardDto(
            lastRefreshed = Instant.ofEpochMilli(clock.now()),
            leaderboard = LeagueListDTO("CHALLENGER", "id", "RANKED_SOLO_5x5", "Empty", emptyList())
        )
        val euwRecovered = serviceWithDefaultCaps.getChallengerLeagueFromCache("euw1", "RANKED_SOLO_5x5")
        assertNotNull(euwRecovered)
        assertEquals(300, euwRecovered!!.entries.size, "Fresh normal response must preserve complete 300-entry leaderboard")
        assertEquals(300, redisStore["leaderboard:euw1::RANKED_SOLO_5x5"]?.leaderboard?.entries?.size)
        assertEquals(1, leagueCallCounter.get())

        val naFresh = serviceWithDefaultCaps.getChallengerLeagueFromCache("na1", "RANKED_SOLO_5x5")
        val krFresh = serviceWithDefaultCaps.getChallengerLeagueFromCache("kr", "RANKED_SOLO_5x5")
        assertEquals(300, naFresh?.entries?.size)
        assertEquals(300, krFresh?.entries?.size)
        assertNull(serviceWithDefaultCaps.getChallengerLeagueFromCache("br1", "RANKED_SOLO_5x5"), "Unsupported 4th region must not be stored")
        assertEquals(3, serviceWithDefaultCaps.getLastKnownGoodRegionCount())
        assertTrue(serviceWithDefaultCaps.getLastKnownGoodRegionBytes("euw1") in 1..ChallengerLeagueService.MAX_LAST_KNOWN_GOOD_REGION_BYTES)
        assertTrue(serviceWithDefaultCaps.getLastKnownGoodRegionBytes("na1") in 1..ChallengerLeagueService.MAX_LAST_KNOWN_GOOD_REGION_BYTES)
        assertTrue(serviceWithDefaultCaps.getLastKnownGoodRegionBytes("kr") in 1..ChallengerLeagueService.MAX_LAST_KNOWN_GOOD_REGION_BYTES)
        assertTrue(serviceWithDefaultCaps.getLastKnownGoodTotalBytes() <= ChallengerLeagueService.MAX_LAST_KNOWN_GOOD_TOTAL_BYTES)

        // Evict euw1 from Redis AND make upstream fail -> serves COMPLETE 300-entry last-known-good snapshot and re-seeds Redis
        redisStore.remove("leaderboard:euw1::RANKED_SOLO_5x5")
        shouldFailUpstream.set(true)
        val euwFromLastGood = serviceWithDefaultCaps.getChallengerLeagueFromCache("euw1", "RANKED_SOLO_5x5")
        assertNotNull(euwFromLastGood)
        assertEquals(300, euwFromLastGood!!.entries.size, "Fallback snapshot must be complete (all 300 ranks, never truncated)")
        assertEquals("euw1-puuid-1", euwFromLastGood.entries.first().puuid)
        assertEquals(300, redisStore["leaderboard:euw1::RANKED_SOLO_5x5"]?.leaderboard?.entries?.size)

        // Part B: When a snapshot exceeds the byte cap (e.g. 24 KiB cap < ~85 KiB 300-entry payload), storeLastKnownGoodBounded
        // rejects the oversized whole (no partial rank fallback stored), while still returning the complete 300-entry fresh response.
        // If Redis is later evicted and upstream fails, absent snapshot yields null (HTTP 503).
        redisStore.clear()
        shouldFailUpstream.set(false)
        val serviceWithTightCap = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            dataSource = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            maxEntriesPerRegion = 300,
            maxLastKnownGoodRegionBytes = 24_576,
            maxLastKnownGoodTotalBytes = 65_536
        )
        val controllerWithTightCap = RiotApiController(
            riotApiService = riotApiService,
            challengerLeagueService = serviceWithTightCap,
            userService = mockk(relaxed = true),
            summonerProfileService = mockk(relaxed = true),
            redisCacheService = redisCacheService
        )

        val freshUnderTightCap = controllerWithTightCap.getChallengerLeague("euw1")
        assertEquals(HttpStatus.OK, freshUnderTightCap.statusCode)
        assertEquals(300, freshUnderTightCap.body?.entries?.size, "Fresh response must still return all 300 entries")
        assertEquals(0, serviceWithTightCap.getLastKnownGoodRegionBytes("euw1"), "Oversized whole snapshot must be rejected rather than storing partial ranks")

        redisStore.remove("leaderboard:euw1::RANKED_SOLO_5x5")
        shouldFailUpstream.set(true)
        val evictedUnderTightCap = controllerWithTightCap.getChallengerLeague("euw1")
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, evictedUnderTightCap.statusCode, "Absent fallback snapshot must return HTTP 503 rather than partial rank list")
        assertNull(evictedUnderTightCap.body)
    }

    @Test
    fun `storeLastKnownGoodBounded rejects oversized single entry and atomically enforces total byte cap across concurrent multi-region updates without partial snapshots`() = runBlocking {
        val clock = MutableClock()
        val memoryProvider = MutableMemoryProvider()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } returns null

        val riotApiService = mockk<RiotApiService>()
        val serveOversizedSingleEntry = AtomicBoolean(true)

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
            attemptCounter?.incrementAndGet()
            if (serveOversizedSingleEntry.get()) {
                LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-$region",
                    queue = "RANKED_SOLO_5x5",
                    name = "OversizedSingle",
                    entries = listOf(
                        LeagueEntryDto(
                            puuid = "$region-oversized-1",
                            leaguePoints = 2000,
                            rank = "I",
                            wins = 100,
                            losses = 50,
                            veteran = false,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = false,
                            leagueId = "challenger-$region",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "X".repeat(100_000),
                            gameName = "Player1",
                            tagLine = "EUW"
                        )
                    )
                )
            } else {
                LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-$region",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger $region",
                    entries = (1..300).map { idx ->
                        LeagueEntryDto(
                            puuid = "$region-puuid-$idx",
                            leaguePoints = 2500 - idx,
                            rank = "I",
                            wins = 100,
                            losses = 80,
                            veteran = false,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = false,
                            leagueId = "challenger-$region",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "S$idx",
                            gameName = "G$idx",
                            tagLine = region.uppercase()
                        )
                    }
                )
            }
        }

        // Each 300-entry region is ~84 KiB. With 90 KiB per-region cap and 180 KiB custom total cap,
        // at most 2 complete 300-entry regions (~168 KiB) can fit in lastKnownGood at once; the 3rd is rejected as a whole.
        val service = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            dataSource = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            maxEntriesPerRegion = 300,
            maxLastKnownGoodRegionBytes = 92_160,  // 90 KiB per-region cap
            maxLastKnownGoodTotalBytes = 184_320   // 180 KiB custom total cap (2 * ~84 KiB fits, 3 * ~84 KiB rejected)
        )

        // 1. Single entry (~100 KiB) exceeds 90 KiB per-region cap -> rejected from lastKnownGood (0 bytes stored), while fresh response returns the entry
        val singleEntryResponse = service.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
        assertNotNull(singleEntryResponse)
        assertEquals(1, singleEntryResponse!!.entries.size)
        assertEquals(0, service.getLastKnownGoodRegionBytes("euw1"), "Oversized single entry must never violate hard per-region byte cap")
        assertEquals(0, service.getLastKnownGoodTotalBytes(), "Oversized single entry must leave total last-known-good bytes at 0")

        // 2. Concurrent updates across euw1, na1, kr with custom tight total budget (180 KiB)
        serveOversizedSingleEntry.set(false)
        val regions = listOf("euw1", "na1", "kr")
        val concurrentUpdates = (1..18).map { idx ->
            async(Dispatchers.Default) {
                val region = regions[idx % regions.size]
                val response = service.getOrRefreshChallengerLeague(region, "RANKED_SOLO_5x5")
                assertEquals(300, response?.entries?.size, "Fresh response for $region must preserve all 300 entries")
                assertTrue(service.getLastKnownGoodRegionBytes(region) <= 92_160)
                assertTrue(
                    service.getLastKnownGoodTotalBytes() <= 184_320,
                    "Concurrent cross-region updates must never exceed maxLastKnownGoodTotalBytes (184320)"
                )
            }
        }
        concurrentUpdates.awaitAll()
        assertEquals(2, service.getLastKnownGoodRegionCount(), "Only 2 complete ~84 KiB regions fit within 180 KiB total cap; 3rd oversized whole is rejected")
        assertTrue(service.getLastKnownGoodTotalBytes() in 1..184_320)
    }

    @Test
    fun `stalled JDBC query on real H2 DataSource is cancelled via Statement cancel and queryTimeout within deadline without stranding worker or retaining singleflight`() = runBlocking {
        val clock = MutableClock()
        val memoryProvider = MutableMemoryProvider()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } returns null

        val riotApiService = mockk<RiotApiService>()
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
            attemptCounter?.incrementAndGet()
            LeagueListDTO(
                tier = "CHALLENGER",
                leagueId = "challenger-$region",
                queue = "RANKED_SOLO_5x5",
                name = "Challenger $region",
                entries = listOf(
                    LeagueEntryDto(
                        puuid = "$region-puuid-1",
                        leaguePoints = 2100,
                        rank = "I",
                        wins = 110,
                        losses = 70,
                        veteran = false,
                        inactive = false,
                        freshBlood = false,
                        hotStreak = false,
                        leagueId = "challenger-$region",
                        queueType = "RANKED_SOLO_5x5",
                        tier = "CHALLENGER",
                        summonerName = "StalledDbSummoner"
                    )
                )
            )
        }

        // Create a real H2 JDBC DataSource where querying `indexed_players` runs a massive CPU-bound SQL cross-join
        // (no Thread.sleep!) that only terminates promptly when JDBC Statement.cancel() / setQueryTimeout aborts H2's SQL engine.
        val dbName = "challenger_stalled_sql_${UUID.randomUUID().toString().replace("-", "")}"
        val stalledH2DataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:$dbName;MODE=PostgreSQL;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }
        stalledH2DataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE VIEW indexed_players AS
                    SELECT CAST(a.x + b.x AS VARCHAR) AS puuid,
                           'SlowGameName' AS game_name,
                           'SlowTag' AS tag_line
                    FROM SYSTEM_RANGE(1, 50000000) a
                    CROSS JOIN SYSTEM_RANGE(1, 50000000) b
                    """.trimIndent()
                )
            }
        }

        val service = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            dataSource = stalledH2DataSource,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            refreshDeadlineMs = 800L,
            dbLookupTimeoutMs = 200L
        )

        val startWall = System.currentTimeMillis()
        val result = service.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
        val elapsedWall = System.currentTimeMillis() - startWall

        assertNotNull(result, "Refresh should still return the fresh league entries when optional JDBC enrichment is cancelled on timeout")
        assertEquals(1, result!!.entries.size)
        assertTrue(elapsedWall < 1_500L, "Real JDBC Statement.cancel() must abort CPU-bound SQL query near 200ms, took ${elapsedWall}ms")
        assertEquals(0, service.getInFlightRefreshCount(), "Singleflight entry must be cleaned up promptly after DB timeout")
        assertEquals(0, service.getActiveDbLookupWorkers(), "JDBC worker thread must be freed by Statement.cancel() and not stranded")
    }

    @Test
    fun `singleflight coalesces concurrent cache misses, retains active singleflight across waiter timeout to prevent overlap, and enforces failure backoff`() = runBlocking {
        val clock = MutableClock()
        val memoryProvider = MutableMemoryProvider()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } returns null

        val riotApiService = mockk<RiotApiService>()
        val upstreamCalls = AtomicInteger(0)
        val shouldFail = AtomicBoolean(false)
        val upstreamDelayMs = AtomicLong(120L)

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
            delay(upstreamDelayMs.get())
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
            dataSource = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            refreshDeadlineMs = 200L,
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

        // 2. Waiter timeout while leader job is still active must NOT remove the in-flight deferred or allow overlapping upstream call
        upstreamDelayMs.set(350L) // > waiter waitBudgetMs (100 + 150 = 250ms), < leader outer timeout (100 + 350 = 450ms)
        val shortDeadlineService = ChallengerLeagueService(
            redisCacheService = redisCacheService,
            riotApiService = riotApiService,
            dataSource = null,
            timeProvider = clock,
            memoryStatsProvider = memoryProvider,
            refreshDeadlineMs = 100L, // waiter timeout = 250ms, leader outer timeout = 450ms
            failureBackoffMs = 30_000L
        )
        val callsBeforeOverlapCheck = upstreamCalls.get()
        val firstWaiter = async(Dispatchers.Default) {
            shortDeadlineService.getOrRefreshChallengerLeague("na1", "RANKED_SOLO_5x5")
        }
        // Wait for firstWaiter to hit waiter timeout (250ms) and return while the leader (350ms) is still active
        val firstWaiterResult = firstWaiter.await()
        assertNull(firstWaiterResult, "First waiter should time out before the 350ms leader finishes")
        assertEquals(
            1,
            shortDeadlineService.getInFlightRefreshCount(),
            "Timed-out waiter must NOT remove active leader from inFlightRefreshes"
        )
        val secondWaiterDuringActiveLeader = async(Dispatchers.Default) {
            shortDeadlineService.getOrRefreshChallengerLeague("na1", "RANKED_SOLO_5x5")
        }
        val secondWaiterResult = secondWaiterDuringActiveLeader.await()
        assertNotNull(secondWaiterResult, "Second waiter coalescing onto active leader should receive leader result")
        assertEquals(
            callsBeforeOverlapCheck + 1,
            upstreamCalls.get(),
            "Must not start a second overlapping refresh after waiter timeout while leader is still active"
        )
        assertEquals(0, shortDeadlineService.getInFlightRefreshCount(), "Leader cleans up singleflight entry once finished")
        upstreamDelayMs.set(120L)

        // 3. Cold region (kr) with upstream failure -> returns 503 Service Unavailable, records failure backoff, and suppresses retries
        val callsBeforeKr = upstreamCalls.get()
        shouldFail.set(true)
        val krFirst = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, krFirst.statusCode)
        assertNull(krFirst.body)
        assertEquals(callsBeforeKr + 1, upstreamCalls.get())
        assertTrue(service.getRemainingBackoffMs("kr") > 0L)
        assertEquals("HTTP_429_TOO_MANY_REQUESTS", service.getLastRefreshFailureReason("kr"))
        assertEquals(0, service.getInFlightRefreshCount())

        // Subsequent KR calls during 30s backoff must make 0 additional upstream calls and return 503 immediately
        val krDuringBackoff = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, krDuringBackoff.statusCode)
        assertEquals(callsBeforeKr + 1, upstreamCalls.get(), "Must not call Riot API while region is in failure backoff")

        // Advance clock past 30s backoff and allow upstream recovery -> self-heals to 200 OK
        clock.advanceBy(31_000L)
        shouldFail.set(false)
        val krRecovered = controller.getChallengerLeague("kr")
        assertEquals(HttpStatus.OK, krRecovered.statusCode)
        assertEquals(1, krRecovered.body?.entries?.size)
        assertEquals(callsBeforeKr + 2, upstreamCalls.get())
        assertEquals(0L, service.getRemainingBackoffMs("kr"))
    }

    @Test
    fun `production ReactorNetty HTTP client enforces bounded chunked body size, pre-header and body deadline aborts, and memory headroom guards`() = runBlocking {
        val oversizedSecondHalfCompleted = AtomicInteger(0)
        val stalledDeadlineCompleted = AtomicInteger(0)
        val preHeaderStallCompleted = AtomicInteger(0)
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
        server.createContext("/lol/league/v4/challengerleagues/by-queue/PRE_HEADER_STALL_DEADLINE") { exchange ->
            try {
                // Stall BEFORE sending status code or response headers
                Thread.sleep(4_000L)
                preHeaderStallCompleted.incrementAndGet()
                exchange.sendResponseHeaders(200, -1L)
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

            // 2. Slow chunked body stream exceeding 220ms deadline aborts promptly with DEADLINE_EXCEEDED
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

            // 3. Pre-header stall (connection accepted, no response headers sent before 220ms deadline) aborts promptly with DEADLINE_EXCEEDED
            run {
                val attempts = AtomicInteger(0)
                val reason = AtomicReference<String?>(null)
                val startWall = System.currentTimeMillis()
                val result = riotApiService.fetchChallengerLeagueBounded(
                    region = "euw1",
                    queue = "PRE_HEADER_STALL_DEADLINE",
                    deadlineAtMs = timeProvider.now() + 220L,
                    maxResponseBytes = 65_536,
                    httpAttemptCounter = attempts,
                    failureReasonHolder = reason
                )
                val elapsedWall = System.currentTimeMillis() - startWall
                assertNull(result)
                assertEquals(1, attempts.get())
                assertEquals("DEADLINE_EXCEEDED", reason.get())
                assertTrue(elapsedWall < 1_500L, "Pre-header stall abort should finish near 220ms without waiting 4s, took ${elapsedWall}ms")
                assertEquals(0, preHeaderStallCompleted.get())
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
            }

            // 4. Pre-flight memory headroom breach -> 0 HTTP attempts made
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

            // 5. Mid-stream memory headroom breach -> aborts chunked body mid-read with MEMORY_HEADROOM_STOP
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
