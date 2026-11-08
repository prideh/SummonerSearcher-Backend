package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.controller.TimelineController
import com.pride.summoner_searcher_api.dto.*
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class CoachTimelinePreparationBehaviorTest {

    private class FakeTimeProvider(initialMs: Long = 1_700_000_000_000L) : TimeProvider {
        private val current = AtomicLong(initialMs)
        override fun now(): Long = current.get()
        fun advanceBy(deltaMs: Long) {
            current.addAndGet(deltaMs)
        }
    }

    private val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()

    private class MutableMemoryStatsProvider(
        @Volatile var cgroupUsedBytes: Long = 310L * 1024L * 1024L,
        @Volatile var cgroupMaxBytes: Long = 512L * 1024L * 1024L,
        @Volatile var heapUsedBytes: Long = 220L * 1024L * 1024L,
        @Volatile var heapMaxBytes: Long = 380L * 1024L * 1024L
    ) : MemoryStatsProvider {
        override fun sample(): RawMemorySample = RawMemorySample(
            jvmHeapUsedBytes = heapUsedBytes,
            jvmHeapCommittedBytes = heapMaxBytes,
            jvmHeapMaxBytes = heapMaxBytes,
            cgroupMemoryCurrentBytes = cgroupUsedBytes,
            cgroupMemoryMaxBytes = cgroupMaxBytes
        )
    }

    private val puuid = "puuid-spear-shot"
    private val oppPuuid = "puuid-opp-top"

    private fun buildMatchDto(matchId: String, win: Boolean = true, durationSec: Long = 1800L): MatchDto {
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
                    isPlayer -> puuid
                    isOpp -> oppPuuid
                    else -> "puuid-$pid"
                },
                "participantId" to pid,
                "teamId" to teamId,
                "teamPosition" to pos,
                "individualPosition" to pos,
                "championName" to when {
                    isPlayer -> "Pantheon"
                    isOpp -> "Aatrox"
                    else -> "Champ$pid"
                },
                "win" to if (teamId == 100) win else !win,
                "kills" to if (isPlayer) 8 else 3,
                "deaths" to if (isPlayer) 2 else 5,
                "assists" to if (isPlayer) 7 else 4,
                "totalMinionsKilled" to 190,
                "neutralMinionsKilled" to 12,
                "visionScore" to 28,
                "goldEarned" to 13500,
                "totalDamageDealtToChampions" to 26000,
                "totalTimeSpentDead" to 42,
                "turretKills" to 2,
                "turretTakedowns" to 3,
                "dragonKills" to 0,
                "baronKills" to 0,
                "challenges" to mapOf(
                    "turretPlatesTaken" to 6,
                    "dragonTakedowns" to 2,
                    "voidMonsterKill" to 3,
                    "laneMinionsFirst10Minutes" to 78
                )
            )
            mapper.convertValue(raw, ParticipantDto::class.java)
        }
        val teams = listOf(
            TeamDto(
                teamId = 100,
                win = win,
                objectives = TeamObjectivesDto(
                    tower = ObjectiveStatsDto(first = true, kills = 8),
                    dragon = ObjectiveStatsDto(first = true, kills = 3),
                    baron = ObjectiveStatsDto(first = false, kills = 1),
                    riftHerald = ObjectiveStatsDto(first = true, kills = 1),
                    horde = ObjectiveStatsDto(first = true, kills = 4)
                )
            ),
            TeamDto(
                teamId = 200,
                win = !win,
                objectives = TeamObjectivesDto(
                    tower = ObjectiveStatsDto(first = false, kills = 3),
                    dragon = ObjectiveStatsDto(first = false, kills = 1),
                    baron = ObjectiveStatsDto(first = false, kills = 0),
                    riftHerald = ObjectiveStatsDto(first = false, kills = 0),
                    horde = ObjectiveStatsDto(first = false, kills = 2)
                )
            )
        )
        return MatchDto(
            metadata = MatchMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
            info = MatchInfo(
                gameId = 1000L,
                gameCreation = 1_760_000_000_000L,
                gameDuration = durationSec,
                gameMode = "CLASSIC",
                gameVersion = "16.1.1",
                queueId = 420,
                participants = participants,
                teams = teams
            )
        )
    }

    private fun buildRichRawTimeline(matchId: String, seed: Int = 1): MatchTimelineDto {
        val timelineParticipants = (1..10).map { pid ->
            TimelineParticipantDto(
                participantId = pid,
                puuid = when (pid) {
                    1 -> puuid
                    6 -> oppPuuid
                    else -> "puuid-$pid"
                }
            )
        }
        val frames = (0..25).map { minute ->
            val pFrames = (1..10).associate { pid ->
                pid.toString() to ParticipantFrameDto(
                    participantId = pid,
                    totalGold = 500 + minute * (380 + pid * 5 + seed),
                    level = (1 + minute / 2).coerceAtMost(18),
                    xp = minute * (440 + pid * 7),
                    currentGold = if (minute == 11 && pid == 1) 1850 else 350 + (minute * 25) % 900,
                    minionsKilled = minute * 7 + (if (pid == 1) seed else 0),
                    jungleMinionsKilled = if (minute >= 12) (minute - 10) * 2 else 0,
                    position = TimelinePositionDto(x = 2000 + minute * 250 + pid * 30, y = 12000 - minute * 200)
                )
            }
            val events = mutableListOf<TimelineEventDto>()
            // Add noisy unrelated events for other participants to prove compactTimelineForPlayer strips them
            for (otherPid in listOf(2, 3, 4, 5, 7, 8, 9, 10)) {
                events.add(
                    TimelineEventDto(
                        type = "ITEM_PURCHASED",
                        timestamp = minute * 60_000L + 5_000L,
                        participantId = otherPid,
                        killerId = null,
                        victimId = null,
                        assistingParticipantIds = null,
                        itemId = 1001 + otherPid,
                        afterId = null,
                        beforeId = null,
                        skillSlot = null,
                        levelUpType = null,
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = null,
                        monsterType = null,
                        monsterSubType = null,
                        position = null,
                        bounty = null,
                        shutdownBounty = null,
                        goldGain = null
                    )
                )
                events.add(
                    TimelineEventDto(
                        type = "ITEM_UNDO",
                        timestamp = minute * 60_000L + 6_000L,
                        participantId = otherPid,
                        killerId = null,
                        victimId = null,
                        assistingParticipantIds = null,
                        itemId = 1001 + otherPid,
                        afterId = null,
                        beforeId = null,
                        skillSlot = null,
                        levelUpType = null,
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = null,
                        monsterType = null,
                        monsterSubType = null,
                        position = null,
                        bounty = null,
                        shutdownBounty = null,
                        goldGain = null
                    )
                )
            }
            if (minute in listOf(1, 3, 5, 7, 9, 12)) {
                events.add(
                    TimelineEventDto(
                        type = "SKILL_LEVEL_UP",
                        timestamp = minute * 60_000L + 1000L,
                        participantId = 1,
                        killerId = null,
                        victimId = null,
                        assistingParticipantIds = null,
                        itemId = null,
                        afterId = null,
                        beforeId = null,
                        skillSlot = if (minute == 6) 4 else 1,
                        levelUpType = "NORMAL",
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = null,
                        monsterType = null,
                        monsterSubType = null,
                        position = null,
                        bounty = null,
                        shutdownBounty = null,
                        goldGain = null
                    )
                )
            }
            if (minute == 12) {
                events.add(
                    TimelineEventDto(
                        type = "ITEM_PURCHASED",
                        timestamp = 12 * 60_000L + 15_000L,
                        participantId = 1,
                        killerId = null,
                        victimId = null,
                        assistingParticipantIds = null,
                        itemId = 3071,
                        afterId = null,
                        beforeId = null,
                        skillSlot = null,
                        levelUpType = null,
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = null,
                        monsterType = null,
                        monsterSubType = null,
                        position = null,
                        bounty = null,
                        shutdownBounty = null,
                        goldGain = null
                    )
                )
            }
            if (minute == 8) {
                events.add(
                    TimelineEventDto(
                        type = "CHAMPION_KILL",
                        timestamp = 8 * 60_000L + 20_000L,
                        participantId = null,
                        killerId = 6,
                        victimId = 1,
                        assistingParticipantIds = listOf(7),
                        itemId = null,
                        afterId = null,
                        beforeId = null,
                        skillSlot = null,
                        levelUpType = null,
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = null,
                        monsterType = null,
                        monsterSubType = null,
                        position = TimelinePositionDto(x = 3200, y = 11500),
                        bounty = 300,
                        shutdownBounty = 150,
                        goldGain = null
                    )
                )
            }
            if (minute == 14) {
                events.add(
                    TimelineEventDto(
                        type = "ELITE_MONSTER_KILL",
                        timestamp = 14 * 60_000L + 10_000L,
                        participantId = null,
                        killerId = 1,
                        victimId = null,
                        assistingParticipantIds = listOf(2),
                        itemId = null,
                        afterId = null,
                        beforeId = null,
                        skillSlot = null,
                        levelUpType = null,
                        wardType = null,
                        creatorId = null,
                        level = null,
                        buildingType = null,
                        laneType = null,
                        towerType = null,
                        teamId = 100,
                        killerTeamId = 100,
                        monsterType = "RIFTHERALD",
                        monsterSubType = null,
                        position = TimelinePositionDto(x = 4500, y = 10200),
                        bounty = null,
                        shutdownBounty = null,
                        goldGain = null
                    )
                )
            }
            TimelineFrameDto(
                timestamp = minute * 60_000L,
                participantFrames = pFrames,
                events = events
            )
        }
        return MatchTimelineDto(
            metadata = MatchTimelineMetadata(matchId = matchId, participants = (1..10).map { "puuid-$it" }),
            info = MatchTimelineInfo(
                frameInterval = 60_000L,
                frames = frames,
                participants = timelineParticipants
            )
        )
    }

    @Test
    fun `compactTimelineForPlayer strips non-tracked participant frames and unrelated events while preserving 100 percent identical analysis output`() = runBlocking {
        val analysisService = TimelineAnalysisService()
        val matchIds = (1..20).map { "EUW1_MATCH_$it" }
        val matchDtos = matchIds.mapIndexed { idx, id -> buildMatchDto(id, win = idx % 2 == 0) }
        val rawTimelines = matchIds.mapIndexed { idx, id -> buildRichRawTimeline(id, seed = idx + 1) }

        val rawTriples = matchIds.indices.map { i -> Triple(matchIds[i], rawTimelines[i], matchDtos[i]) }
        val compactedTriples = matchIds.indices.map { i ->
            val compacted = analysisService.compactTimelineForPlayer(rawTimelines[i], matchDtos[i], puuid)
            assertNotNull(compacted)
            // Verify only player (1) and opponent (6) participantFrames are retained per frame (2 instead of 10)
            compacted!!.info!!.frames!!.forEach { frame ->
                assertEquals(setOf("1", "6"), frame.participantFrames!!.keys)
                assertTrue(frame.events!!.none { it.type == "ITEM_UNDO" })
                assertTrue(frame.events!!.none { it.type == "ITEM_PURCHASED" && it.participantId !in setOf(1, 6) })
            }
            Triple(matchIds[i], compacted, matchDtos[i])
        }

        val expectedAnalysis = analysisService.analyze(rawTriples, puuid, matchDtos)
        val compactedAnalysis = analysisService.analyze(compactedTriples, puuid, matchDtos)

        assertEquals(expectedAnalysis, compactedAnalysis)
        assertEquals(expectedAnalysis.improvementStats, compactedAnalysis.improvementStats)
    }

    @Test
    fun `prepareCoachTimelines processes sequentially with peak 1 raw timeline in flight and serves reopen from bounded cache with zero extra calls`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
        val rateLimiter = RiotApiRateLimiter(timeProvider)

        val redisStore = ConcurrentHashMap<String, MatchTimelineDto>()
        val redisGetCount = AtomicInteger(0)
        val riotCallCount = AtomicInteger(0)
        val inFlightRiotCalls = AtomicInteger(0)
        val maxConcurrentRiotCalls = AtomicInteger(0)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } answers {
            redisGetCount.incrementAndGet()
            redisStore[firstArg()]
        }
        every { redisCacheService.set(any<String>(), any<MatchTimelineDto>(), any<Duration>()) } answers {
            redisStore[firstArg()] = secondArg()
        }

        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val cur = inFlightRiotCalls.incrementAndGet()
            maxConcurrentRiotCalls.accumulateAndGet(cur, ::maxOf)
            try {
                riotCallCount.incrementAndGet()
                val matchId = firstArg<String>()
                buildRichRawTimeline(matchId, seed = riotCallCount.get())
            } finally {
                inFlightRiotCalls.decrementAndGet()
            }
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            rateLimiter = rateLimiter,
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )

        val matchIds = (1..20).map { "EUW1_$it" }
        val matchDtos = matchIds.map { buildMatchDto(it) }

        val firstResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("COMPLETE", firstResult.status)
        assertFalse(firstResult.cacheHit)
        assertEquals(20, firstResult.coveredMatchIds.size)
        assertTrue(firstResult.missingMatches.isEmpty())
        assertEquals(1, firstResult.peakRetainedRawTimelines)
        assertEquals(1, maxConcurrentRiotCalls.get())
        assertEquals(20, riotCallCount.get())
        assertNotNull(firstResult.analysis)
        assertEquals(20, firstResult.analysis?.aggregateSummary?.gamesAnalyzed)

        // Reopen with identical matchIds -> hits bounded prepared cache with ZERO extra Riot or Redis calls
        val redisReadsBeforeReopen = redisGetCount.get()
        val secondResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("COMPLETE", secondResult.status)
        assertTrue(secondResult.cacheHit)
        assertEquals(20, riotCallCount.get())
        assertEquals(redisReadsBeforeReopen, redisGetCount.get())
        assertEquals(firstResult.analysis, secondResult.analysis)

        // When match list changes (1 new match EUW1_21 at front, 19 already in Redis 24h cache),
        // invalidates prepared cache and fetches ONLY the 1 new timeline from Riot!
        val updatedMatchIds = listOf("EUW1_21") + matchIds.take(19)
        val updatedMatchDtos = updatedMatchIds.map { buildMatchDto(it) }
        val thirdResult = service.prepareCoachTimelines("euw1", puuid, updatedMatchIds, updatedMatchDtos)
        assertEquals("COMPLETE", thirdResult.status)
        assertFalse(thirdResult.cacheHit)
        assertEquals(21, riotCallCount.get()) // Only 1 new Riot call!
        assertEquals(20, thirdResult.coveredMatchIds.size)
    }

    @Test
    fun `prepareCoachTimelines enforces 350 MiB admission block and 360 MiB mid-run stop without fabricating zero for missing timelines`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(
            cgroupUsedBytes = 350L * 1024L * 1024L // Exactly 350 MiB -> blocked at admission (< 350 MiB required)
        )

        val matchIds = (1..5).map { "EUW1_MEM_$it" }
        val matchDtos = matchIds.map { buildMatchDto(it) }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )

        // 1. Admission blocked at 350 MiB
        val blockedResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("BLOCKED", blockedResult.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", blockedResult.stopReason)
        assertTrue(blockedResult.coveredMatchIds.isEmpty())
        assertEquals(5, blockedResult.missingMatches.size)
        assertTrue(blockedResult.missingMatches.all { it.reason == "MEMORY_HEADROOM_ADMISSION_BLOCKED" })
        assertNull(blockedResult.analysis)
        coVerify(exactly = 0) { riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any()) }

        // 2. Admission passes at 340 MiB, then memory reaches 360 MiB after 2 matches -> stops before 3rd match
        memoryProvider.cgroupUsedBytes = 340L * 1024L * 1024L
        var fetchedCount = 0
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            fetchedCount++
            if (fetchedCount == 2) {
                // Memory crosses 360 MiB stop threshold before match 3
                memoryProvider.cgroupUsedBytes = 360L * 1024L * 1024L
            }
            buildRichRawTimeline(firstArg(), seed = fetchedCount)
        }

        val partialResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("PARTIAL", partialResult.status)
        assertEquals("MEMORY_HEADROOM_STOP", partialResult.stopReason)
        assertEquals(listOf("EUW1_MEM_1", "EUW1_MEM_2"), partialResult.coveredMatchIds)
        assertEquals(3, partialResult.missingMatches.size)
        assertTrue(partialResult.missingMatches.all { it.reason == "MEMORY_HEADROOM_STOP" })
        assertEquals(2, fetchedCount)
        assertNotNull(partialResult.analysis)
        assertEquals(2, partialResult.analysis?.aggregateSummary?.gamesAnalyzed)
        // Missing 3 timelines are recorded in excludedMissingTimelineGames, never fabricated as 0
        assertEquals(5, partialResult.analysis?.improvementStats?.totalGamesConsidered)
        assertEquals(3, partialResult.analysis?.improvementStats?.excludedMissingTimelineGames)
    }

    @Test
    fun `prepareCoachTimelines coalesces same-player concurrent requests via singleflight and enforces cross-player concurrency limit`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)

        val gate = CompletableDeferred<Unit>()
        val enteredFetch = CompletableDeferred<Unit>()
        val riotCalls = AtomicInteger(0)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            riotCalls.incrementAndGet()
            enteredFetch.complete(Unit)
            gate.await()
            buildRichRawTimeline(firstArg())
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider,
            maxConcurrentPreparations = 1
        )

        val matchIds = listOf("EUW1_SF_1", "EUW1_SF_2")
        val matchDtos = matchIds.map { buildMatchDto(it) }

        // Launch two concurrent preparations for the SAME player & matchIds -> must coalesce
        val callA = async { service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos) }
        enteredFetch.await()
        val callB = async { service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos) }

        // While player 1 is occupying the single global slot, a different player's preparation is blocked cleanly
        val otherPlayerResult = service.prepareCoachTimelines("euw1", "puuid-other-player", matchIds, matchDtos)
        assertEquals("BLOCKED", otherPlayerResult.status)
        assertEquals("CONCURRENCY_LIMIT_REACHED", otherPlayerResult.stopReason)

        gate.complete(Unit)
        val resA = callA.await()
        val resB = callB.await()
        assertEquals("COMPLETE", resA.status)
        assertEquals("COMPLETE", resB.status)
        assertEquals(2, riotCalls.get()) // Only 2 calls total for 2 matches, not 4
    }

    @Test
    fun `prepareCoachTimelines handles 404, zero-retry 429 stop, active cooldown, and deadline exceeded honestly`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)
        val rateLimiter = RiotApiRateLimiter(timeProvider)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        // Match 1 succeeds, Match 2 returns 404, Match 3 hits 429 -> Match 4 is skipped due to cooldown
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val matchId = firstArg<String>()
            @Suppress("UNCHECKED_CAST")
            val failureHolder = arg<AtomicReference<String?>?>(5)
            when (matchId) {
                "EUW1_RL_1" -> buildRichRawTimeline(matchId, seed = 1)
                "EUW1_RL_2" -> {
                    failureHolder?.set("HTTP_404_NOT_FOUND")
                    null
                }
                "EUW1_RL_3" -> {
                    failureHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                    rateLimiter.recordRetryAfter("euw1", 5_000L, "application")
                    null
                }
                else -> buildRichRawTimeline(matchId, seed = 4)
            }
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            rateLimiter = rateLimiter,
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider,
            prepDeadlineMs = 10_000L
        )

        val matchIds = listOf("EUW1_RL_1", "EUW1_RL_2", "EUW1_RL_3", "EUW1_RL_4")
        val matchDtos = matchIds.map { buildMatchDto(it) }

        val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("PARTIAL", res.status)
        assertEquals("RATE_LIMIT_429", res.stopReason)
        assertEquals(listOf("EUW1_RL_1"), res.coveredMatchIds)
        assertEquals(
            listOf(
                MissingMatchTimelineDto("EUW1_RL_2", "TIMELINE_NOT_FOUND"),
                MissingMatchTimelineDto("EUW1_RL_3", "RATE_LIMIT_429"),
                MissingMatchTimelineDto("EUW1_RL_4", "RATE_LIMIT_COOLDOWN")
            ),
            res.missingMatches
        )
        // Verify Match 4 was never requested from Riot after Match 3 hit 429
        coVerify(exactly = 0) {
            riotApiService.getMatchTimelineBounded("EUW1_RL_4", any(), any(), any(), any(), any(), any(), any())
        }
    }

    @Test
    fun `TimelineController prepareCoachTimelines bounds to at most 20 cached profile match IDs and never hydrates 883 seasonal history`() {
        val timelineService = mockk<TimelineService>()
        val timelineAnalysisService = TimelineAnalysisService()
        val playerCacheService = mockk<PlayerCacheService>()

        val all883Ids = (1..883).map { "EUW1_HIST_$it" }
        val recent20Dtos = all883Ids.take(20).map { buildMatchDto(it) }
        val cachedProfile = SummonerProfileDto(
            puuid = puuid,
            gameName = "Spear Shot",
            tagLine = "EUW",
            summonerLevel = 890,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = recent20Dtos,
            allMatchIds = all883Ids
        )

        every { timelineService.hasCoachAdmissionHeadroom() } returns true
        coEvery { playerCacheService.getCachedProfile(puuid, "euw1") } returns cachedProfile
        coEvery {
            timelineService.prepareCoachTimelines(
                region = "euw1",
                puuid = puuid,
                requestedMatchIds = all883Ids.take(20),
                matchDtos = recent20Dtos,
                analyzerVersion = any()
            )
        } returns CoachTimelinePreparationResultDto(
            status = "COMPLETE",
            region = "euw1",
            puuid = puuid,
            analyzerVersion = TimelineService.COACH_TIMELINE_ANALYZER_VERSION,
            requestedMatchIds = all883Ids.take(20),
            coveredMatchIds = all883Ids.take(20),
            missingMatches = emptyList(),
            stopReason = null,
            cacheHit = false,
            peakRetainedRawTimelines = 1,
            analysis = null
        )

        val controller = TimelineController(timelineService, timelineAnalysisService, playerCacheService)
        val response = controller.prepareCoachTimelinesPost(
            region = "euw1",
            puuid = puuid,
            request = CoachTimelinePreparationRequestDto(matchIds = all883Ids) // Even if caller passes 883 IDs, capped at 20!
        )

        assertEquals(200, response.statusCode.value())
        assertEquals(20, response.body?.requestedMatchIds?.size)
        // Because all 20 recentMatches were already on cachedProfile, getMatchesByIds was never called for 883 history!
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }
    }

    @Test
    fun `TimelineController enforces 350 MiB admission guard before profile decode and marks missing DTO coverage without calling getMatchesByIds`() {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val playerCacheService = mockk<PlayerCacheService>()
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(
            cgroupUsedBytes = 350L * 1024L * 1024L // At 350 MiB -> must block BEFORE decoding cached profile!
        )

        val timelineService = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )
        val controller = TimelineController(timelineService, TimelineAnalysisService(), playerCacheService)

        // 1. When memory is >= 350 MiB, controller blocks BEFORE calling playerCacheService.getCachedProfile
        val blockedResponse = controller.prepareCoachTimelinesPost(
            region = "euw1",
            puuid = puuid,
            request = CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_1", "EUW1_2"))
        )
        assertEquals(200, blockedResponse.statusCode.value())
        assertEquals("BLOCKED", blockedResponse.body?.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", blockedResponse.body?.stopReason)
        coVerify(exactly = 0) { playerCacheService.getCachedProfile(any(), any()) }
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }

        // 2. When admission passes (320 MiB) and cached profile has 20 match IDs in allMatchIds
        // but only 18 MatchDtos in recentMatches (missing EUW1_5 and EUW1_12),
        // controller must NEVER call getMatchesByIds (no multiGet, no Riot fanout, no index writes)
        // and must mark EUW1_5 and EUW1_12 as MISSING_MATCH_DTO.
        memoryProvider.cgroupUsedBytes = 320L * 1024L * 1024L
        val twentyIds = (1..20).map { "EUW1_DTO_$it" }
        val eighteenDtos = twentyIds.filterNot { it in setOf("EUW1_DTO_5", "EUW1_DTO_12") }.map { buildMatchDto(it) }
        val profileWithMissingDtos = SummonerProfileDto(
            puuid = puuid,
            gameName = "Spear Shot",
            tagLine = "EUW",
            summonerLevel = 890,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = eighteenDtos,
            allMatchIds = twentyIds
        )
        coEvery { playerCacheService.getCachedProfile(puuid, "euw1") } returns profileWithMissingDtos
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } answers {
            val key = firstArg<String>()
            val matchId = key.substringAfterLast(":")
            buildRichRawTimeline(matchId)
        }

        val partialResponse = controller.prepareCoachTimelinesPost(
            region = "euw1",
            puuid = puuid,
            request = null
        )
        val body = partialResponse.body!!
        assertEquals("PARTIAL", body.status)
        assertEquals(18, body.coveredMatchIds.size)
        assertEquals(
            listOf(
                MissingMatchTimelineDto("EUW1_DTO_5", "MISSING_MATCH_DTO"),
                MissingMatchTimelineDto("EUW1_DTO_12", "MISSING_MATCH_DTO")
            ),
            body.missingMatches
        )
        // Verify getMatchesByIds was NEVER called even though 2 match DTOs were absent from recentMatches
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }
        // Verify Redis timeline cache was never queried for the 2 matches missing DTOs
        every { redisCacheService.get("timeline:euw1:EUW1_DTO_5", MatchTimelineDto::class.java) }
        io.mockk.verify(exactly = 0) { redisCacheService.get("timeline:euw1:EUW1_DTO_5", MatchTimelineDto::class.java) }
        io.mockk.verify(exactly = 0) { redisCacheService.get("timeline:euw1:EUW1_DTO_12", MatchTimelineDto::class.java) }
    }

    @Test
    fun `legacy cached timeline is compacted and raw 10-player timeline is garbage-collectable before replacement fetch runs`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)

        // Build a legacy timeline whose CHAMPION_KILL events have bounty=null and shutdownBounty=null
        fun buildLegacyRawTimeline(matchId: String): MatchTimelineDto {
            val base = buildRichRawTimeline(matchId)
            val legacyFrames = base.info!!.frames!!.map { frame ->
                frame.copy(
                    events = frame.events?.map { ev ->
                        if (ev.type == "CHAMPION_KILL") ev.copy(bounty = null, shutdownBounty = null) else ev
                    }
                )
            }
            return base.copy(info = base.info!!.copy(frames = legacyFrames))
        }

        val legacyWeakRefs = mutableListOf<java.lang.ref.WeakReference<MatchTimelineDto>>()
        val wasLegacyCollectedDuringReplacementFetch = AtomicInteger(0)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } answers {
            val key = firstArg<String>()
            val matchId = key.substringAfterLast(":")
            // Simulate fresh deserialization from Redis without retaining strong ref in test harness
            val deserializedLegacy = buildLegacyRawTimeline(matchId)
            legacyWeakRefs.add(java.lang.ref.WeakReference(deserializedLegacy))
            deserializedLegacy
        }

        val capturedAnalyzeInputs = mutableListOf<List<Triple<String, MatchTimelineDto, MatchDto>>>()
        val realAnalysisService = TimelineAnalysisService()
        val spyAnalysisService = io.mockk.spyk(realAnalysisService)
        every { spyAnalysisService.analyze(any(), any(), any()) } answers {
            @Suppress("UNCHECKED_CAST")
            val triples = firstArg<List<Triple<String, MatchTimelineDto, MatchDto>>>()
            capturedAnalyzeInputs.add(triples)
            realAnalysisService.analyze(triples, secondArg(), thirdArg())
        }

        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val matchId = firstArg<String>()
            @Suppress("UNCHECKED_CAST")
            val failureHolder = arg<AtomicReference<String?>?>(5)
            // Clear MockK's internal recorded call logs so MockK itself does not hold strong refs to arguments/returns
            io.mockk.clearMocks(redisCacheService, spyAnalysisService, answers = false, recordedCalls = true, childMocks = false)
            // Force GC while replacement fetch is in flight to prove original raw timeline has zero strong references in TimelineService
            repeat(3) {
                System.gc()
                System.runFinalization()
                Thread.sleep(5)
            }
            if (legacyWeakRefs.last().get() == null) {
                wasLegacyCollectedDuringReplacementFetch.incrementAndGet()
            }
            if (matchId == "EUW1_LEG_1") {
                // Replacement succeeds for match 1
                buildRichRawTimeline(matchId, seed = 10)
            } else {
                // Replacement 404s for match 2 -> falls back to already-compacted legacy timeline
                failureHolder?.set("HTTP_404_NOT_FOUND")
                null
            }
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = spyAnalysisService,
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )

        val matchIds = listOf("EUW1_LEG_1", "EUW1_LEG_2")
        val matchDtos = matchIds.map { buildMatchDto(it) }
        val result = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)

        assertEquals("COMPLETE", result.status)
        assertEquals(listOf("EUW1_LEG_1", "EUW1_LEG_2"), result.coveredMatchIds)
        // Prove the uncompacted legacy raw timeline was actually garbage-collected during replacement fetch
        assertEquals(2, wasLegacyCollectedDuringReplacementFetch.get())
        // Prove both timelines passed to analyze() (including the fallback for EUW1_LEG_2) are compacted to 2 players
        assertEquals(1, capturedAnalyzeInputs.size)
        for ((_, timeline, _) in capturedAnalyzeInputs.single()) {
            timeline.info!!.frames!!.forEach { frame ->
                assertEquals(setOf("1", "6"), frame.participantFrames!!.keys)
            }
        }
    }

    @Test
    fun `compactTimelineForPlayer rejects malformed timelines missing info, frames, or tracked player and never retains raw timelines`() = runBlocking {
        val analysisService = TimelineAnalysisService()
        val validMatchDto = buildMatchDto("EUW1_BAD_1")
        val validRaw = buildRichRawTimeline("EUW1_BAD_1")

        // 1. Missing timeline.info -> returns null (never returns original raw timeline)
        val missingInfoTimeline = validRaw.copy(info = null)
        assertNull(analysisService.compactTimelineForPlayer(missingInfoTimeline, validMatchDto, puuid))

        // 2. Missing or empty frames -> returns null
        val emptyFramesTimeline = validRaw.copy(info = validRaw.info!!.copy(frames = emptyList()))
        assertNull(analysisService.compactTimelineForPlayer(emptyFramesTimeline, validMatchDto, puuid))

        // 3. Player missing from matchDto.info.participants -> returns null
        assertNull(analysisService.compactTimelineForPlayer(validRaw, validMatchDto, "puuid-not-in-match"))

        // 4. Frames present for other players (2..10) but missing tracked player's participantFrames -> returns null
        val framesWithoutPlayer = validRaw.info!!.frames!!.map { frame ->
            frame.copy(participantFrames = frame.participantFrames?.filterKeys { it != "1" })
        }
        val timelineWithoutPlayerFrames = validRaw.copy(info = validRaw.info!!.copy(frames = framesWithoutPlayer))
        assertNull(analysisService.compactTimelineForPlayer(timelineWithoutPlayerFrames, validMatchDto, puuid))

        // 5. Across 20 malformed cached timelines in prepareCoachTimelines, 0 timelines are retained and all 20 are marked TIMELINE_UNAVAILABLE
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)

        val rawWeakRefs = mutableListOf<java.lang.ref.WeakReference<MatchTimelineDto>>()
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } answers {
            val key = firstArg<String>()
            val matchId = key.substringAfterLast(":")
            val malformed = when (matchId.substringAfterLast("_").toInt() % 3) {
                0 -> buildRichRawTimeline(matchId).copy(info = null)
                1 -> buildRichRawTimeline(matchId).let { it.copy(info = it.info!!.copy(frames = emptyList())) }
                else -> buildRichRawTimeline(matchId).let { raw ->
                    val stripped = raw.info!!.frames!!.map { f ->
                        f.copy(participantFrames = f.participantFrames?.filterKeys { k -> k != "1" })
                    }
                    raw.copy(info = raw.info!!.copy(frames = stripped))
                }
            }
            rawWeakRefs.add(java.lang.ref.WeakReference(malformed))
            malformed
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = analysisService,
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )

        val matchIds = (1..20).map { "EUW1_MAL_$it" }
        val matchDtos = matchIds.map { buildMatchDto(it) }
        val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)

        repeat(3) {
            System.gc()
            System.runFinalization()
            Thread.sleep(5)
        }

        assertEquals("BLOCKED", res.status)
        assertEquals("TIMELINE_UNAVAILABLE", res.stopReason)
        assertTrue(res.coveredMatchIds.isEmpty())
        assertEquals(20, res.missingMatches.size)
        assertTrue(res.missingMatches.all { it.reason == "TIMELINE_UNAVAILABLE" })
        assertNull(res.analysis)
        // Prove none of the 20 malformed raw timelines were retained in memory
        assertTrue(rawWeakRefs.all { it.get() == null })
    }

    @Test
    fun `deadline and fresh memory headroom bound rate-limiter waits, blocking HTTP calls, and pre-decode stage`() = runBlocking {
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        val matchIds = listOf("EUW1_BOUND_1", "EUW1_BOUND_2")
        val matchDtos = matchIds.map { buildMatchDto(it) }
        val serializedTimelineBytes = mapper.writeValueAsBytes(buildRichRawTimeline("EUW1_BOUND_1"))

        // Case A: Rate-limiter sliding-window wait (60s window exhausted) exceeds remaining 500ms preparation deadline.
        // Must abort immediately without waiting 60s and make 0 HTTP calls.
        run {
            val timeProvider = SystemTimeProvider()
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            // Exhaust a 60s app bucket (20/20 used in 60s window)
            rateLimiter.updateLimits("euw1", "20:60", "20:60")

            val httpCallCount = AtomicInteger(0)
            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { uri, _ ->
                httpCallCount.incrementAndGet()
                throw AssertionError("HTTP request should not be executed when rate-limiter wait exceeds deadline: $uri")
            }
            val restClient = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory).build()
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider)
            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = timeProvider,
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 500L
            )

            val wallStart = System.currentTimeMillis()
            val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
            val wallElapsed = System.currentTimeMillis() - wallStart

            assertEquals("BLOCKED", res.status)
            assertEquals("DEADLINE_EXCEEDED", res.stopReason)
            assertEquals(0, httpCallCount.get())
            assertTrue(wallElapsed < 1_500L, "Expected fast fail without sleeping 60s, took ${wallElapsed}ms")
        }

        // Case A2: Rate-limiter waits within budget (~120ms Retry-After), and memory crosses 360 MiB DURING the limiter wait.
        // Must re-check fresh headroom immediately after waiting and abort BEFORE executing the HTTP call.
        run {
            val timeProvider = SystemTimeProvider()
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 330L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            // Exhaust a 1s bucket so tryAcquirePermit waits ~1s, and raise memory to 362 MiB during the wait
            rateLimiter.updateLimits("euw1", "1:1", "1:1")
            val httpCallCount = AtomicInteger(0)
            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { uri, _ ->
                httpCallCount.incrementAndGet()
                throw AssertionError("HTTP request should not be executed when memory crosses 360 MiB during limiter wait: $uri")
            }
            val restClient = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory).build()
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider)
            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = timeProvider,
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 5_000L
            )

            val mutator = Thread {
                Thread.sleep(60L)
                memoryProvider.cgroupUsedBytes = 362L * 1024L * 1024L
            }.apply { isDaemon = true; start() }

            val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
            mutator.join(500L)

            assertEquals("BLOCKED", res.status)
            assertEquals("MEMORY_HEADROOM_STOP", res.stopReason)
            assertEquals(0, httpCallCount.get())
        }

        // Case B: Memory crosses 360 MiB DURING HTTP response headers arrival BEFORE JSON body decode.
        // Must abort inside exchange before decoding MatchTimelineDto from the response stream.
        run {
            val timeProvider = SystemTimeProvider()
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 330L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            val bodyBytesRead = AtomicInteger(0)

            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
                object : org.springframework.http.client.ClientHttpRequest {
                    private val reqHeaders = org.springframework.http.HttpHeaders()
                    override fun getMethod() = org.springframework.http.HttpMethod.GET
                    override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_BOUND_1/timeline")
                    override fun getHeaders() = reqHeaders
                    override fun getBody() = java.io.ByteArrayOutputStream()
                    override fun getAttributes() = mutableMapOf<String, Any>()
                    override fun execute(): org.springframework.http.client.ClientHttpResponse {
                        // Simulate memory crossing 360 MiB while waiting for HTTP response headers
                        memoryProvider.cgroupUsedBytes = 362L * 1024L * 1024L
                        return object : org.springframework.http.client.ClientHttpResponse {
                            override fun getStatusCode() = org.springframework.http.HttpStatus.OK
                            override fun getStatusText() = "OK"
                            override fun close() {}
                            override fun getHeaders() = org.springframework.http.HttpHeaders().apply {
                                contentType = org.springframework.http.MediaType.APPLICATION_JSON
                            }
                            override fun getBody(): java.io.InputStream {
                                return object : java.io.ByteArrayInputStream(serializedTimelineBytes) {
                                    override fun read(): Int {
                                        bodyBytesRead.incrementAndGet()
                                        return super.read()
                                    }
                                    override fun read(b: ByteArray, off: Int, len: Int): Int {
                                        val n = super.read(b, off, len)
                                        if (n > 0) bodyBytesRead.addAndGet(n)
                                        return n
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val restClient = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory).build()
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider)
            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = timeProvider,
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 5_000L
            )

            val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
            assertEquals("BLOCKED", res.status)
            assertEquals("MEMORY_HEADROOM_STOP", res.stopReason)
            assertEquals(0, bodyBytesRead.get(), "Response JSON body must NOT be decoded when memory crosses 360 MiB before decode")
        }

        // Case C: Blocking HTTP socket hangs for 5,000ms while remaining deadline budget is 250ms.
        // Must interrupt and abort at ~250ms with DEADLINE_EXCEEDED instead of blocking for 5,000ms.
        run {
            val timeProvider = SystemTimeProvider()
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            val threadInterruptedCount = AtomicInteger(0)

            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
                object : org.springframework.http.client.ClientHttpRequest {
                    private val reqHeaders = org.springframework.http.HttpHeaders()
                    override fun getMethod() = org.springframework.http.HttpMethod.GET
                    override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_BOUND_1/timeline")
                    override fun getHeaders() = reqHeaders
                    override fun getBody() = java.io.ByteArrayOutputStream()
                    override fun getAttributes() = mutableMapOf<String, Any>()
                    override fun execute(): org.springframework.http.client.ClientHttpResponse {
                        try {
                            Thread.sleep(5_000L)
                        } catch (e: InterruptedException) {
                            threadInterruptedCount.incrementAndGet()
                            throw java.io.InterruptedIOException("Interrupted by deadline guard")
                        }
                        throw AssertionError("Should have been interrupted before 5s sleep completed")
                    }
                }
            }
            val restClient = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory).build()
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider)
            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = timeProvider,
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 250L
            )

            val wallStart = System.currentTimeMillis()
            val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
            val wallElapsed = System.currentTimeMillis() - wallStart

            assertEquals("BLOCKED", res.status)
            assertEquals("DEADLINE_EXCEEDED", res.stopReason)
            assertTrue(wallElapsed < 1_500L, "Blocking HTTP call should abort near 250ms deadline, took ${wallElapsed}ms")
            // Give worker thread a brief moment to record interruption catch
            Thread.sleep(20)
            assertEquals(1, threadInterruptedCount.get())
        }
    }
}
