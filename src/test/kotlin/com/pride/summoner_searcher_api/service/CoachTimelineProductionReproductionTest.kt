package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.config.RestClientConfig
import com.pride.summoner_searcher_api.dto.*
import com.sun.net.httpserver.HttpServer
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream

class CoachTimelineProductionReproductionTest {

    private val objectMapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    private val targetPuuid = "fj7q0_L_QDjddr9QbLBu_i616eJPg3OH3qrevxj834XoQQJ15qV5pIPCz7aV2mfRuE-xejxcPmMuPA"
    private val oppPuuid = "opp-puuid-top-euw1"

    private fun buildRealisticMatchDto(matchId: String, gameIndex: Int, playerPuuid: String = targetPuuid): MatchDto {
        val participants = (1..10).map { pid ->
            val isPlayer = pid == 1
            val isOpp = pid == 6
            val teamId = if (pid <= 5) 100 else 200
            val pos = when ((pid - 1) % 5) {
                0 -> "TOP"
                1 -> "JUNGLE"
                2 -> "MIDDLE"
                3 -> "BOTTOM"
                else -> "UTILITY"
            }
            val raw = mapOf(
                "puuid" to when {
                    isPlayer -> playerPuuid
                    isOpp -> oppPuuid
                    else -> "puuid-match-$gameIndex-pid-$pid-0123456789abcdef0123456789abcdef"
                },
                "participantId" to pid,
                "teamId" to teamId,
                "teamPosition" to pos,
                "individualPosition" to pos,
                "championId" to (10 + pid),
                "championName" to if (isPlayer) "Jax" else if (isOpp) "Darius" else "Champion$pid",
                "riotIdGameName" to "PlayerName$pid",
                "riotIdTagline" to "EUW$pid",
                "summonerName" to "PlayerName$pid",
                "win" to (teamId == 100),
                "kills" to (4 + (pid + gameIndex) % 7),
                "deaths" to (2 + (pid * 3 + gameIndex) % 6),
                "assists" to (5 + (pid * 2 + gameIndex) % 9),
                "champLevel" to 16,
                "champExperience" to 15840 + pid * 137,
                "totalMinionsKilled" to (185 + pid * 4),
                "neutralMinionsKilled" to (16 + pid * 2),
                "visionScore" to (24 + pid),
                "wardsPlaced" to (11 + pid % 5),
                "wardsKilled" to (4 + pid % 3),
                "detectorWardsPlaced" to (2 + pid % 3),
                "goldEarned" to (12850 + pid * 315 + gameIndex * 110),
                "goldSpent" to (12100 + pid * 290),
                "totalDamageDealt" to (145000 + pid * 4321),
                "totalDamageDealtToChampions" to (24500 + pid * 1123 + gameIndex * 331),
                "physicalDamageDealtToChampions" to (16200 + pid * 731),
                "magicDamageDealtToChampions" to (6100 + pid * 289),
                "trueDamageDealtToChampions" to (2200 + pid * 103),
                "totalDamageTaken" to (28400 + pid * 947),
                "damageSelfMitigated" to (21300 + pid * 812),
                "damageDealtToObjectives" to (9800 + pid * 650),
                "damageDealtToTurrets" to (4200 + pid * 310),
                "totalTimeSpentDead" to (38 + pid * 3),
                "item0" to 3078,
                "item1" to 3053,
                "item2" to 3111,
                "item3" to 6333,
                "item4" to 3026,
                "item5" to 1055,
                "item6" to 3340,
                "summoner1Id" to 4,
                "summoner2Id" to 12,
                "turretKills" to 2,
                "turretTakedowns" to 4,
                "dragonKills" to 0,
                "baronKills" to 0,
                "challenges" to mapOf(
                    "kda" to 3.75,
                    "killParticipation" to 0.58,
                    "damagePerMinute" to 812.4 + pid * 13.7,
                    "goldPerMinute" to 428.9 + pid * 9.3,
                    "teamDamagePercentage" to 0.24,
                    "visionScorePerMinute" to 0.92,
                    "laneMinionsFirst10Minutes" to 79,
                    "maxCsAdvantageOnLaneOpponent" to 18.0,
                    "maxLevelLeadLaneOpponent" to 2,
                    "turretPlatesTaken" to 4,
                    "soloKills" to 2,
                    "dragonTakedowns" to 2,
                    "voidMonsterKill" to 3
                ),
                "perks" to mapOf(
                    "statPerks" to mapOf("defense" to 5001, "flex" to 5008, "offense" to 5005),
                    "styles" to listOf(
                        mapOf(
                            "description" to "primaryStyle",
                            "style" to 8000,
                            "selections" to listOf(
                                mapOf("perk" to 8010, "var1" to 642, "var2" to 0, "var3" to 0),
                                mapOf("perk" to 9111, "var1" to 512, "var2" to 240, "var3" to 0),
                                mapOf("perk" to 9104, "var1" to 14, "var2" to 30, "var3" to 0),
                                mapOf("perk" to 8299, "var1" to 731, "var2" to 0, "var3" to 0)
                            )
                        ),
                        mapOf(
                            "description" to "subStyle",
                            "style" to 8400,
                            "selections" to listOf(
                                mapOf("perk" to 8444, "var1" to 891, "var2" to 0, "var3" to 0),
                                mapOf("perk" to 8451, "var1" to 186, "var2" to 0, "var3" to 0)
                            )
                        )
                    )
                )
            )
            objectMapper.convertValue(raw, ParticipantDto::class.java)
        }
        return MatchDto(
            metadata = MatchMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
            info = MatchInfo(
                gameId = 7_698_000_000L + gameIndex,
                gameCreation = 1_762_000_000_000L + gameIndex * 3_600_000L,
                gameDuration = 1_860L,
                gameMode = "CLASSIC",
                gameVersion = "16.9.1",
                queueId = 420,
                participants = participants,
                teams = listOf(
                    TeamDto(
                        teamId = 100,
                        win = true,
                        objectives = TeamObjectivesDto(
                            tower = ObjectiveStatsDto(true, 9),
                            dragon = ObjectiveStatsDto(true, 3),
                            baron = ObjectiveStatsDto(true, 1),
                            riftHerald = ObjectiveStatsDto(true, 1),
                            horde = ObjectiveStatsDto(true, 4)
                        )
                    ),
                    TeamDto(
                        teamId = 200,
                        win = false,
                        objectives = TeamObjectivesDto(
                            tower = ObjectiveStatsDto(false, 3),
                            dragon = ObjectiveStatsDto(false, 1),
                            baron = ObjectiveStatsDto(false, 0),
                            riftHerald = ObjectiveStatsDto(false, 0),
                            horde = ObjectiveStatsDto(false, 2)
                        )
                    )
                )
            )
        )
    }

    private fun buildRealisticTimelineBytes(matchId: String, seed: Int, playerPuuid: String = targetPuuid): ByteArray {
        val participants = (1..10).map { pid ->
            TimelineParticipantDto(
                participantId = pid,
                puuid = when (pid) {
                    1 -> playerPuuid
                    6 -> oppPuuid
                    else -> "puuid-pid-$pid-$seed"
                }
            )
        }
        val frames = (0..32).map { minute ->
            val pFrames = (1..10).associate { pid ->
                pid.toString() to ParticipantFrameDto(
                    participantId = pid,
                    totalGold = 500 + minute * (395 + pid * 7 + seed),
                    level = (1 + minute / 2).coerceAtMost(18),
                    xp = minute * (455 + pid * 9),
                    currentGold = 320 + (minute * 73 + pid * 41) % 1400,
                    minionsKilled = minute * 7 + pid % 4,
                    jungleMinionsKilled = if (pid in setOf(2, 7)) minute * 5 else minute / 3,
                    position = TimelinePositionDto(x = 1800 + minute * 260 + pid * 37, y = 12200 - minute * 215 - pid * 19)
                )
            }
            val events = ArrayList<TimelineEventDto>(60)
            for (pid in 1..10) {
                events.add(
                    TimelineEventDto(
                        type = "ITEM_PURCHASED",
                        timestamp = minute * 60_000L + pid * 1_100L,
                        participantId = pid,
                        killerId = null, victimId = null, assistingParticipantIds = null,
                        itemId = 1001 + (minute * 13 + pid) % 2200,
                        afterId = null, beforeId = null, skillSlot = null, levelUpType = null,
                        wardType = null, creatorId = null, level = null, buildingType = null,
                        laneType = null, towerType = null, teamId = null, monsterType = null,
                        monsterSubType = null, position = null, bounty = null, shutdownBounty = null, goldGain = null
                    )
                )
                events.add(
                    TimelineEventDto(
                        type = "WARD_PLACED",
                        timestamp = minute * 60_000L + pid * 1_700L,
                        participantId = null,
                        killerId = null, victimId = null, assistingParticipantIds = null,
                        itemId = null, afterId = null, beforeId = null, skillSlot = null, levelUpType = null,
                        wardType = "YELLOW_TRINKET", creatorId = pid, level = null, buildingType = null,
                        laneType = null, towerType = null, teamId = null, monsterType = null,
                        monsterSubType = null, position = TimelinePositionDto(4100 + pid * 90, 9200 - minute * 80),
                        bounty = null, shutdownBounty = null, goldGain = null
                    )
                )
            }
            if (minute in listOf(3, 6, 9, 12, 15, 18, 22, 26)) {
                events.add(
                    TimelineEventDto(
                        type = "CHAMPION_KILL",
                        timestamp = minute * 60_000L + 24_000L,
                        participantId = null,
                        killerId = if (minute % 2 == 0) 1 else 6,
                        victimId = if (minute % 2 == 0) 6 else 1,
                        assistingParticipantIds = listOf(2, 3),
                        itemId = null, afterId = null, beforeId = null, skillSlot = null, levelUpType = null,
                        wardType = null, creatorId = null, level = null, buildingType = null,
                        laneType = null, towerType = null, teamId = null, monsterType = null,
                        monsterSubType = null, position = TimelinePositionDto(3400 + minute * 120, 10800 - minute * 110),
                        bounty = 300, shutdownBounty = 150, goldGain = null
                    )
                )
            }
            TimelineFrameDto(timestamp = minute * 60_000L, participantFrames = pFrames, events = events)
        }
        val dto = MatchTimelineDto(
            metadata = MatchTimelineMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
            info = MatchTimelineInfo(frameInterval = 60_000L, frames = frames, participants = participants)
        )
        return objectMapper.writeValueAsBytes(dto)
    }

    @org.junit.jupiter.api.DisplayName("production cost SystemMemoryStatsProvider + real GZIP_B64 + ReactorNetty GZIP HTTP 200 completes 3-match and 20-match profiles without per-byte sampling amplification")
    @Test
    fun completesRealisticGzipB64ProfilesWithProductionMemorySampler() = runBlocking {
        val redisMap = ConcurrentHashMap<String, String>()
        val valueOps = mockk<ValueOperations<String, String>>(relaxed = true)
        every { valueOps.set(any(), any(), any<Duration>()) } answers {
            redisMap[firstArg()] = secondArg()
        }
        every { valueOps.get(any()) } answers {
            redisMap[firstArg()]
        }
        val stringRedisTemplate = mockk<StringRedisTemplate>(relaxed = true)
        every { stringRedisTemplate.opsForValue() } returns valueOps
        every { stringRedisTemplate.delete(any<String>()) } answers {
            redisMap.remove(firstArg()) != null
        }

        val redisCacheService = RedisCacheService(stringRedisTemplate, objectMapper)
        val realSystemMemory = SystemMemoryStatsProvider()
        val sampleCalls = AtomicInteger(0)
        val sampleNanos = AtomicLong(0L)
        val countingSystemMemory = object : MemoryStatsProvider {
            override fun sample(): RawMemorySample {
                val t0 = System.nanoTime()
                val real = realSystemMemory.sample()
                sampleNanos.addAndGet(System.nanoTime() - t0)
                sampleCalls.incrementAndGet()
                return real.copy(
                    jvmHeapUsedBytes = 90L * 1024L * 1024L,
                    jvmHeapCommittedBytes = 205L * 1024L * 1024L,
                    jvmHeapMaxBytes = 205L * 1024L * 1024L,
                    cgroupMemoryCurrentBytes = 352_731_136L,
                    cgroupMemoryMaxBytes = 536_870_912L
                )
            }
        }

        val matchIds3 = listOf("EUW1_7698327758", "EUW1_7682617257", "EUW1_7682584353")
        val matches3 = matchIds3.mapIndexed { idx, id -> buildRealisticMatchDto(id, idx, targetPuuid) }
        val profile3 = SummonerProfileDto(
            puuid = targetPuuid,
            gameName = "pride",
            tagLine = "persi",
            summonerLevel = 412L,
            profileIconId = 4802,
            soloQueueRank = null,
            recentMatches = matches3,
            allMatchIds = matchIds3,
            totalMatches = 3,
            lastRefreshed = Instant.now()
        )
        val profileKey = "summoner:profile:v11:euw1:$targetPuuid"
        redisCacheService.set(profileKey, profile3, Duration.ofHours(1))
        val rawStored3 = redisMap[profileKey]!!
        assertTrue(rawStored3.startsWith("GZIP_B64:"))

        val puuid20 = "puuid-20-real-cost"
        val matchIds20 = (1..20).map { "EUW1_77000000$it" }
        val matches20 = matchIds20.mapIndexed { idx, id -> buildRealisticMatchDto(id, idx, puuid20) }
        val profile20 = profile3.copy(
            puuid = puuid20,
            recentMatches = matches20,
            allMatchIds = (1..400).map { if (it <= 20) matchIds20[it - 1] else "EUW1_OLD_$it" },
            totalMatches = 400
        )
        val profileKey20 = "summoner:profile:v11:euw1:$puuid20"
        redisCacheService.set(profileKey20, profile20, Duration.ofHours(1))
        val rawStored20 = redisMap[profileKey20]!!
        assertTrue(rawStored20.startsWith("GZIP_B64:"))
        assertTrue(rawStored20.length > 50_000)

        val gzipTimelinesByMatchId = HashMap<String, ByteArray>(24)
        for ((idx, id) in matchIds3.withIndex()) {
            val raw = buildRealisticTimelineBytes(id, idx + 1, targetPuuid)
            gzipTimelinesByMatchId[id] = ByteArrayOutputStream().use { bos ->
                GZIPOutputStream(bos).use { gz -> gz.write(raw) }
                bos.toByteArray()
            }
        }
        for ((idx, id) in matchIds20.withIndex()) {
            val raw = buildRealisticTimelineBytes(id, idx + 10, puuid20)
            gzipTimelinesByMatchId[id] = ByteArrayOutputStream().use { bos ->
                GZIPOutputStream(bos).use { gz -> gz.write(raw) }
                bos.toByteArray()
            }
        }

        val httpHits = AtomicInteger(0)
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool { r -> Thread(r, "repro-riot-http").apply { isDaemon = true } }
        server.createContext("/lol/match/v5/matches/") { exchange ->
            try {
                httpHits.incrementAndGet()
                val matchId = exchange.requestURI.path.substringAfter("/matches/").substringBefore("/timeline")
                val payload = gzipTimelinesByMatchId[matchId]
                if (payload == null) {
                    exchange.sendResponseHeaders(404, -1L)
                } else {
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.responseHeaders.add("X-App-Rate-Limit", "100:1,500:120")
                    exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
                    exchange.responseHeaders.add("X-Method-Rate-Limit", "2000:10")
                    exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "1:10")
                    exchange.responseHeaders.add("Content-Encoding", "gzip")
                    exchange.sendResponseHeaders(200, payload.size.toLong())
                    exchange.responseBody.use { it.write(payload) }
                }
            } finally {
                runCatching { exchange.close() }
            }
        }
        server.start()
        try {
            val port = server.address.port
            val rateLimiter = RiotApiRateLimiter(SystemTimeProvider())
            val riotRestClient = RestClientConfig.buildRiotRestClient("test-key")
            val riotApiService = RiotApiService(
                riotRestClient = riotRestClient,
                rateLimiter = rateLimiter
            ).apply {
                matchBaseUrlResolver = { "http://127.0.0.1:$port" }
            }
            val timelineService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingSystemMemory
            )

            // 1. 3-match profile (previously 22,986 sample() calls / 6,066ms on 6e817d7)
            val tStart3 = System.currentTimeMillis()
            val outcome3 = timelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = matchIds3,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { redisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            )
            val elapsed3Ms = System.currentTimeMillis() - tStart3
            val ready3 = outcome3 as TimelineService.PlayerPreparationOutcome.Ready
            val calls3 = sampleCalls.get()
            println(
                "REGRESSION_3MATCH rawRedisChars=${rawStored3.length} status=${ready3.result.status} " +
                    "covered=${ready3.result.coveredMatchIds.size}/${ready3.result.requestedMatchIds.size} " +
                    "elapsedMs=$elapsed3Ms sampleCalls=$calls3 sampleCpuMs=${sampleNanos.get() / 1_000_000}"
            )
            assertEquals("COMPLETE", ready3.result.status)
            assertEquals(matchIds3, ready3.result.coveredMatchIds)
            assertNotNull(ready3.result.analysis)
            assertTrue(calls3 < 2_500, "Expected < 2,500 sample() calls for 3-match profile + 3 GZIP timelines, got $calls3")
            assertTrue(elapsed3Ms < 6_000L, "Expected 3-match cold run < 6s (including JVM/Netty warmup), took ${elapsed3Ms}ms")

            // 2. 20-match 53,961-char compressed profile + 20 GZIP HTTP 200 timelines
            //    (previously failed BLOCKED / DEADLINE_EXCEEDED at 12,013ms with 46,863 sample() calls & 0 HTTP hits on 6e817d7!)
            sampleCalls.set(0)
            sampleNanos.set(0L)
            httpHits.set(0)
            val tStart20 = System.currentTimeMillis()
            val outcome20 = timelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = puuid20,
                requestedMatchIds = matchIds20,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { redisCacheService.get(profileKey20, SummonerProfileDto::class.java) }
            )
            val elapsed20Ms = System.currentTimeMillis() - tStart20
            val ready20 = outcome20 as TimelineService.PlayerPreparationOutcome.Ready
            val calls20 = sampleCalls.get()
            println(
                "REGRESSION_20MATCH rawRedisChars=${rawStored20.length} status=${ready20.result.status} " +
                    "covered=${ready20.result.coveredMatchIds.size}/${ready20.result.requestedMatchIds.size} " +
                    "elapsedMs=$elapsed20Ms sampleCalls=$calls20 sampleCpuMs=${sampleNanos.get() / 1_000_000} httpHits=${httpHits.get()}"
            )
            assertEquals("COMPLETE", ready20.result.status)
            assertEquals(20, ready20.result.coveredMatchIds.size)
            assertEquals(20, httpHits.get())
            assertNotNull(ready20.result.analysis)
            assertTrue(calls20 < 12_000, "Expected < 12,000 sample() calls across 54k-char profile + 20 full timelines, got $calls20")
            assertTrue(elapsed20Ms < 9_000L, "Expected 20-match cold run well under 12s deadline, took ${elapsed20Ms}ms")
        } finally {
            server.stop(0)
        }
    }

    @org.junit.jupiter.api.DisplayName("bounded chunk Base64 decoder enforces exact byte caps, deadline, malformed Base64 fail-closed, and 1 KiB midstream headroom stop")
    @Test
    fun enforcesBoundedChunkBase64GuardsAndFailClosedInvariants() {
        val store = ConcurrentHashMap<String, String>()
        val valueOps = mockk<ValueOperations<String, String>>(relaxed = true)
        every { valueOps.get(any()) } answers { store[firstArg()] }
        every { valueOps.set(any(), any(), any<Duration>()) } answers { store[firstArg()] = secondArg() }
        val redisTemplate = mockk<StringRedisTemplate>(relaxed = true)
        every { redisTemplate.opsForValue() } returns valueOps
        val redisCacheService = RedisCacheService(redisTemplate, objectMapper)

        val matches = (1..4).map { buildRealisticMatchDto("EUW1_GUARD_$it", it) }
        val profile = SummonerProfileDto(
            puuid = targetPuuid,
            gameName = "GuardCheck",
            tagLine = "EUW",
            summonerLevel = 150L,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = matches,
            allMatchIds = matches.mapNotNull { it.metadata?.matchId },
            totalMatches = 4,
            lastRefreshed = Instant.now()
        )
        redisCacheService.set("guard:valid", profile, Duration.ofMinutes(10))
        val validRaw = store["guard:valid"]!!
        assertTrue(validRaw.length > 8_192, "Expected > 8 KiB Base64 fixture for multi-1KiB stride verification")

        // 1. Midstream 1 KiB headroom check aborts promptly when headroom flips false after initial check
        val headroomCalls = AtomicInteger(0)
        val allowHeadroom = AtomicBoolean(true)
        val midstreamReason = AtomicReference<String?>(null)
        val midstreamDecoded = RedisCacheService.withBoundedInflateGuards(
            RedisCacheService.BoundedInflateContext(
                maxInflatedBytes = 6 * 1024 * 1024,
                headroomGuard = {
                    val call = headroomCalls.incrementAndGet()
                    if (call >= 3) {
                        allowHeadroom.set(false)
                    }
                    allowHeadroom.get()
                },
                failureReasonHolder = midstreamReason
            )
        ) {
            redisCacheService.get("guard:valid", SummonerProfileDto::class.java)
        }
        assertNull(midstreamDecoded)
        assertEquals("MEMORY_HEADROOM_STOP", midstreamReason.get())
        assertTrue(headroomCalls.get() in 3..6, "Expected midstream stop at 1 KiB boundary without per-byte polling, got ${headroomCalls.get()} calls")

        // 2. Deadline exceeded mid-decode aborts with DEADLINE_EXCEEDED
        val fakeNow = AtomicLong(1_000L)
        val deadlineReason = AtomicReference<String?>(null)
        val deadlineDecoded = RedisCacheService.withBoundedInflateGuards(
            RedisCacheService.BoundedInflateContext(
                maxInflatedBytes = 6 * 1024 * 1024,
                deadlineAtMs = 1_500L,
                timeProvider = object : TimeProvider {
                    override fun now(): Long = fakeNow.addAndGet(200L)
                },
                failureReasonHolder = deadlineReason
            )
        ) {
            redisCacheService.get("guard:valid", SummonerProfileDto::class.java)
        }
        assertNull(deadlineDecoded)
        assertEquals("DEADLINE_EXCEEDED", deadlineReason.get())

        // 3. Malformed Base64 variants (truncated quantum, non-ASCII, bad padding, trailing data after '=') fail closed
        val validB64Body = validRaw.removePrefix("GZIP_B64:")
        store["guard:truncated"] = "GZIP_B64:" + validB64Body.dropLast(1)
        store["guard:nonascii"] = "GZIP_B64:AAAA" + "\u00E9" + "AAA"
        store["guard:badpad"] = "GZIP_B64:A===AAAA"
        val paddedSample = Base64.getEncoder().encodeToString("hello".toByteArray()) // "aGVsbG8="
        store["guard:trailing"] = "GZIP_B64:" + paddedSample + "AAAA"

        for (badKey in listOf("guard:truncated", "guard:nonascii", "guard:badpad", "guard:trailing")) {
            assertNull(
                redisCacheService.get(badKey, SummonerProfileDto::class.java),
                "Expected fail-closed null for malformed payload $badKey"
            )
        }
    }
}
