package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.controller.TimelineController
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.DiscoveredPatternRepository
import com.pride.summoner_searcher_api.repository.PromptVersionRepository
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream

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
        @Volatile var cgroupUsedBytes: Long? = 310L * 1024L * 1024L,
        @Volatile var cgroupMaxBytes: Long? = 512L * 1024L * 1024L,
        @Volatile var heapUsedBytes: Long = 120L * 1024L * 1024L,
        @Volatile var heapMaxBytes: Long = 205L * 1024L * 1024L
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

    private fun buildMatchDto(
        matchId: String,
        win: Boolean = true,
        durationSec: Long = 1800L,
        targetPuuid: String = puuid
    ): MatchDto {
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
                    isPlayer -> targetPuuid
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

    private fun buildRichRawTimeline(
        matchId: String,
        seed: Int = 1,
        targetPuuid: String = puuid
    ): MatchTimelineDto {
        val timelineParticipants = (1..10).map { pid ->
            TimelineParticipantDto(
                participantId = pid,
                puuid = when (pid) {
                    1 -> targetPuuid
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
            if (minute in listOf(12, 16)) {
                events.add(
                    TimelineEventDto(
                        type = "ITEM_PURCHASED",
                        timestamp = minute * 60_000L + 15_000L,
                        participantId = if (minute == 12) 1 else 6,
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
    fun `prepareCoachTimelines processes sequentially with peak 1 raw timeline in flight and round-trips MatchEventEntry flags losslessly from compressed cache`() = runBlocking {
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

        // Verify isPlayer and isOpponent flags exist before and after GZIP cache round-trip
        val firstEvents = firstResult.analysis!!.eventTimelines.first().events
        assertTrue(firstEvents.any { it.isPlayer }, "Expected player events in analysis")
        assertTrue(firstEvents.any { it.isOpponent }, "Expected opponent events in analysis")

        val redisReadsBeforeReopen = redisGetCount.get()
        val secondResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("COMPLETE", secondResult.status)
        assertTrue(secondResult.cacheHit)
        assertEquals(20, riotCallCount.get())
        assertEquals(redisReadsBeforeReopen, redisGetCount.get())
        assertEquals(firstResult.analysis, secondResult.analysis)
        assertEquals(firstEvents, secondResult.analysis!!.eventTimelines.first().events)

        // When match list changes, fetches ONLY the 1 new timeline from Riot
        val updatedMatchIds = listOf("EUW1_21") + matchIds.take(19)
        val updatedMatchDtos = updatedMatchIds.map { buildMatchDto(it) }
        val thirdResult = service.prepareCoachTimelines("euw1", puuid, updatedMatchIds, updatedMatchDtos)
        assertEquals("COMPLETE", thirdResult.status)
        assertFalse(thirdResult.cacheHit)
        assertEquals(21, riotCallCount.get())
        assertEquals(20, thirdResult.coveredMatchIds.size)
    }

    @Test
    fun `dual heap and cgroup memory headroom guards enforce both MaxRAMPercentage 40 percent heap ceiling and non-cgroup JVM heap headroom`() {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()

        // 1. Dockerfile MaxRAMPercentage=40% (~204.8 MiB heap inside 512 MiB container):
        // cgroup RSS is 310 MiB (< 350 MiB), but JVM heap is 199 MiB / 200 MiB -> MUST block!
        val nearFullHeapIn512mCgroup = MutableMemoryStatsProvider(
            cgroupUsedBytes = 310L * 1024L * 1024L,
            cgroupMaxBytes = 512L * 1024L * 1024L,
            heapUsedBytes = 199L * 1024L * 1024L,
            heapMaxBytes = 200L * 1024L * 1024L
        )
        val service1 = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timeProvider = timeProvider,
            memoryStatsProvider = nearFullHeapIn512mCgroup
        )
        assertFalse(service1.hasCoachAdmissionHeadroom(), "199/200 MiB heap must block admission even when cgroup RSS is 310 MiB")
        assertFalse(service1.hasCoachContinueHeadroom(), "199/200 MiB heap must stop mid-run even when cgroup RSS is 310 MiB")

        // 2. Non-cgroup JVM (cgroupMemoryMaxBytes = null) with healthy 120 MiB / 256 MiB heap -> MUST pass!
        val healthyNonCgroupHeap = MutableMemoryStatsProvider(
            cgroupUsedBytes = null,
            cgroupMaxBytes = null,
            heapUsedBytes = 120L * 1024L * 1024L,
            heapMaxBytes = 256L * 1024L * 1024L
        )
        val service2 = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timeProvider = timeProvider,
            memoryStatsProvider = healthyNonCgroupHeap
        )
        assertTrue(service2.hasCoachAdmissionHeadroom(), "Healthy 120/256 MiB non-cgroup heap must pass admission")
        assertTrue(service2.hasCoachContinueHeadroom(), "Healthy 120/256 MiB non-cgroup heap must pass continue guard")

        // 3. Non-cgroup JVM with saturated 235 MiB / 256 MiB heap (> 88% ratio / < 24 MiB free) -> MUST block!
        healthyNonCgroupHeap.heapUsedBytes = 235L * 1024L * 1024L
        assertFalse(service2.hasCoachAdmissionHeadroom())
        assertFalse(service2.hasCoachContinueHeadroom())

        // 4. Bounded 512 MiB cgroup with missing/invalid cgroupMemoryCurrentBytes = null -> fails safe (false)
        val brokenCgroupSample = MutableMemoryStatsProvider(
            cgroupUsedBytes = null,
            cgroupMaxBytes = 512L * 1024L * 1024L,
            heapUsedBytes = 80L * 1024L * 1024L,
            heapMaxBytes = 205L * 1024L * 1024L
        )
        val service3 = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timeProvider = timeProvider,
            memoryStatsProvider = brokenCgroupSample
        )
        assertFalse(service3.hasCoachAdmissionHeadroom())
    }

    @Test
    fun `prepareCoachTimelines enforces 350 MiB admission block and 360 MiB mid-run stop without fabricating zero for missing timelines`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(
            cgroupUsedBytes = 350L * 1024L * 1024L
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

        val blockedResult = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("BLOCKED", blockedResult.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", blockedResult.stopReason)
        assertTrue(blockedResult.coveredMatchIds.isEmpty())
        assertEquals(5, blockedResult.missingMatches.size)
        assertTrue(blockedResult.missingMatches.all { it.reason == "MEMORY_HEADROOM_ADMISSION_BLOCKED" })
        assertNull(blockedResult.analysis)
        coVerify(exactly = 0) { riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any()) }

        memoryProvider.cgroupUsedBytes = 340L * 1024L * 1024L
        var fetchedCount = 0
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            fetchedCount++
            if (fetchedCount == 2) {
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
        assertEquals(5, partialResult.analysis?.improvementStats?.totalGamesConsidered)
        assertEquals(3, partialResult.analysis?.improvementStats?.excludedMissingTimelineGames)
    }

    @Test
    fun `TimelineController bounds admission warm-cache singleflight and 2-slot concurrency BEFORE decoding SummonerProfileDto`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val playerCacheService = mockk<PlayerCacheService>()
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(
            cgroupUsedBytes = 350L * 1024L * 1024L
        )

        val profileDecodeCount = AtomicInteger(0)
        val concurrentProfileDecodes = AtomicInteger(0)
        val maxConcurrentProfileDecodes = AtomicInteger(0)

        val all883Ids = (1..883).map { "EUW1_HIST_$it" }
        val eighteenDtos = all883Ids.take(20).filterNot { it in setOf("EUW1_HIST_5", "EUW1_HIST_12") }.map { buildMatchDto(it) }

        every { playerCacheService.getCachedProfile(any(), any()) } answers {
            val target = firstArg<String>()
            val cur = concurrentProfileDecodes.incrementAndGet()
            maxConcurrentProfileDecodes.accumulateAndGet(cur, ::maxOf)
            try {
                profileDecodeCount.incrementAndGet()
                val targetDtos = all883Ids.take(20)
                    .filterNot { target == puuid && it in setOf("EUW1_HIST_5", "EUW1_HIST_12") }
                    .map { buildMatchDto(it, targetPuuid = target) }
                SummonerProfileDto(
                    puuid = target,
                    gameName = "Player-$target",
                    tagLine = "EUW",
                    summonerLevel = 500,
                    profileIconId = 1,
                    soloQueueRank = null,
                    recentMatches = targetDtos,
                    allMatchIds = all883Ids
                )
            } finally {
                concurrentProfileDecodes.decrementAndGet()
            }
        }

        val timelineService = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider,
            maxConcurrentPreparations = 2
        )
        val controller = TimelineController(timelineService, TimelineAnalysisService(), playerCacheService)

        // 1. 12 concurrent distinct-player requests when memory is at 350 MiB -> ZERO profile decodes!
        val blockedBurst = (1..12).map { idx ->
            async(Dispatchers.IO) {
                controller.prepareCoachTimelinesPost("euw1", "puuid-burst-$idx", null)
            }
        }.awaitAll()
        assertTrue(blockedBurst.all { it.statusCode.value() == 200 && it.body?.stopReason == "MEMORY_HEADROOM_ADMISSION_BLOCKED" })
        assertEquals(0, profileDecodeCount.get(), "Admission block MUST reject before decoding SummonerProfileDto")

        // 2. 10 concurrent distinct-player requests when maxConcurrentPreparations = 2:
        // Hold the 2 active workers inside Riot fetch; the other 8 requests MUST be rejected BEFORE getCachedProfile!
        memoryProvider.cgroupUsedBytes = 315L * 1024L * 1024L
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        val twoSlotsOccupiedLatch = CountDownLatch(2)
        val releaseTwoSlotsLatch = CountDownLatch(1)

        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val matchId = firstArg<String>()
            twoSlotsOccupiedLatch.countDown()
            withContext(Dispatchers.IO) {
                releaseTwoSlotsLatch.await(5_000L, TimeUnit.MILLISECONDS)
            }
            buildRichRawTimeline(matchId)
        }

        val slotHolders = (1..2).map { idx ->
            async(Dispatchers.IO) {
                controller.prepareCoachTimelinesPost(
                    "euw1",
                    "puuid-holder-$idx",
                    CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_HIST_1"))
                )
            }
        }
        assertTrue(twoSlotsOccupiedLatch.await(2_000L, TimeUnit.MILLISECONDS))
        assertEquals(2, profileDecodeCount.get())

        val rejectedOverflow = (3..10).map { idx ->
            async(Dispatchers.IO) {
                controller.prepareCoachTimelinesPost(
                    "euw1",
                    "puuid-overflow-$idx",
                    CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_HIST_1"))
                )
            }
        }.awaitAll()
        assertTrue(rejectedOverflow.all { it.body?.status == "BLOCKED" && it.body?.stopReason == "CONCURRENCY_LIMIT_REACHED" })
        // Profile decode count MUST remain 2 (zero profile decodes for the 8 overflow callers!)
        assertEquals(2, profileDecodeCount.get(), "Overflow callers beyond 2-slot cap must NEVER decode SummonerProfileDto")
        assertTrue(maxConcurrentProfileDecodes.get() <= 2)

        releaseTwoSlotsLatch.countDown()
        slotHolders.awaitAll()

        // 3. 10 concurrent SAME-player requests coalesce BEFORE profile decode -> only 1 profile decode total,
        // and subsequent warm-cache request performs 0 profile decodes until invalidated!
        profileDecodeCount.set(0)
        val sfEntered = CountDownLatch(1)
        val sfRelease = CountDownLatch(1)
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            sfEntered.countDown()
            withContext(Dispatchers.IO) {
                sfRelease.await(5_000L, TimeUnit.MILLISECONDS)
            }
            buildRichRawTimeline(firstArg(), targetPuuid = "puuid-coalesced")
        }

        val tenSamePlayerJobs = (1..10).map {
            async(Dispatchers.IO) {
                controller.prepareCoachTimelinesPost(
                    "euw1",
                    "puuid-coalesced",
                    CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_HIST_1"))
                )
            }
        }
        assertTrue(sfEntered.await(2_000L, TimeUnit.MILLISECONDS))
        delay(40L)
        sfRelease.countDown()
        val tenSamePlayerResponses = tenSamePlayerJobs.awaitAll()
        assertTrue(tenSamePlayerResponses.all { it.body?.status == "COMPLETE" })
        assertEquals(1, profileDecodeCount.get(), "10 concurrent same-player requests must decode SummonerProfileDto only ONCE")

        // Warm preparedCache hit BEFORE profile decode -> 0 additional profile decodes!
        val warmResponse = controller.prepareCoachTimelinesPost(
            "euw1",
            "puuid-coalesced",
            CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_HIST_1"))
        )
        assertEquals("COMPLETE", warmResponse.body?.status)
        assertTrue(warmResponse.body!!.cacheHit)
        assertEquals(1, profileDecodeCount.get(), "Warm preparedCache hit must return BEFORE decoding SummonerProfileDto")

        // Invalidating player's preparedCache causes next call to re-decode cleanly
        timelineService.invalidatePlayerPreparedCache("euw1", "puuid-coalesced")
        val afterInvalidation = controller.prepareCoachTimelinesPost(
            "euw1",
            "puuid-coalesced",
            CoachTimelinePreparationRequestDto(matchIds = listOf("EUW1_HIST_1"))
        )
        assertEquals("COMPLETE", afterInvalidation.body?.status)
        assertFalse(afterInvalidation.body!!.cacheHit)
        assertEquals(2, profileDecodeCount.get())

        // 4. Profile with 883 allMatchIds and 18 recentMatches DTOs: caps at 20, marks 2 MISSING_MATCH_DTO, never calls getMatchesByIds
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } answers {
            val key = firstArg<String>()
            buildRichRawTimeline(key.substringAfterLast(":"))
        }
        val partial20 = controller.prepareCoachTimelinesPost("euw1", puuid, null)
        assertEquals("PARTIAL", partial20.body?.status)
        assertEquals(20, partial20.body?.requestedMatchIds?.size)
        assertEquals(18, partial20.body?.coveredMatchIds?.size)
        assertEquals(
            listOf(
                MissingMatchTimelineDto("EUW1_HIST_5", "MISSING_MATCH_DTO"),
                MissingMatchTimelineDto("EUW1_HIST_12", "MISSING_MATCH_DTO")
            ),
            partial20.body?.missingMatches
        )
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }
        assertEquals(18, eighteenDtos.size)
    }

    @Test
    fun `TimelineController exposes POST-only prepare-coach route and validates path and request body bounds`() {
        // 1. Reflection proof: prepare-coach is @PostMapping ONLY; zero @GetMapping on prepare-coach
        val methods = TimelineController::class.java.declaredMethods
        val postPrepareMethods = methods.filter { m ->
            m.getAnnotation(PostMapping::class.java)?.value?.any { it.contains("prepare-coach") } == true
        }
        val getPrepareMethods = methods.filter { m ->
            m.getAnnotation(GetMapping::class.java)?.value?.any { it.contains("prepare-coach") } == true
        }
        assertEquals(1, postPrepareMethods.size, "Expected exactly 1 @PostMapping for prepare-coach")
        assertTrue(getPrepareMethods.isEmpty(), "Side-effecting prepare-coach route MUST NOT have @GetMapping")

        // 2. Verify TimelineService has no legacy 20-way getTimelines fanout method
        assertTrue(
            TimelineService::class.java.declaredMethods.none { it.name == "getTimelines" },
            "Legacy 20-way TimelineService.getTimelines fanout must be removed"
        )

        // 3. Input validation bounds (oversized matchIds > 200, invalid region/puuid/matchId -> 400 Bad Request)
        val timelineService = mockk<TimelineService>()
        val playerCacheService = mockk<PlayerCacheService>()
        val controller = TimelineController(timelineService, TimelineAnalysisService(), playerCacheService)

        val oversizedIds = (1..201).map { "EUW1_$it" }
        assertEquals(
            400,
            controller.prepareCoachTimelinesPost("euw1", puuid, CoachTimelinePreparationRequestDto(oversizedIds)).statusCode.value()
        )
        assertEquals(400, controller.prepareCoachTimelinesPost("invalid_region_too_long", puuid, null).statusCode.value())
        assertEquals(400, controller.prepareCoachTimelinesPost("euw1", "bad/puuid", null).statusCode.value())
        assertEquals(400, controller.analyzeTimelines("euw1", " ").statusCode.value())
        assertEquals(400, controller.getTimeline("euw1", "BAD/MATCH/ID").statusCode.value())

        coVerify(exactly = 0) { playerCacheService.getCachedProfile(any(), any()) }
    }

    @Test
    fun `legacy GET analyzeTimelines and GET getTimeline routes enforce pre-decode admission and concurrency bounds while preserving frontend compatibility`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val playerCacheService = mockk<PlayerCacheService>()
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(
            cgroupUsedBytes = 480L * 1024L * 1024L // High memory pressure (480 MiB / 512 MiB)
        )

        val timelineService = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider,
            maxConcurrentPreparations = 2
        )
        val controller = TimelineController(timelineService, TimelineAnalysisService(), playerCacheService)

        // 1. At 480 MiB, legacy GET /analyze/{region}/{puuid} and GET /{region}/{matchId} reject with 503 BEFORE decoding profile or calling Riot
        val blockedAnalyze = controller.analyzeTimelines("euw1", puuid)
        assertEquals(503, blockedAnalyze.statusCode.value())
        val blockedSingle = controller.getTimeline("euw1", "EUW1_1")
        assertEquals(503, blockedSingle.statusCode.value())
        coVerify(exactly = 0) { playerCacheService.getCachedProfile(any(), any()) }
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }
        coVerify(exactly = 0) { riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any()) }

        // 2. At healthy 315 MiB, legacy GET /analyze returns 200 OK TimelineAnalysisDto sequentially with 0 getMatchesByIds calls
        memoryProvider.cgroupUsedBytes = 315L * 1024L * 1024L
        val ids = listOf("EUW1_A1", "EUW1_A2")
        val dtos = ids.map { buildMatchDto(it) }
        every { playerCacheService.getCachedProfile(puuid, "euw1") } returns SummonerProfileDto(
            puuid = puuid,
            gameName = "Spear Shot",
            tagLine = "EUW",
            summonerLevel = 800,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = dtos,
            allMatchIds = ids
        )
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            buildRichRawTimeline(firstArg())
        }

        val okAnalyze = controller.analyzeTimelines("euw1", puuid)
        assertEquals(200, okAnalyze.statusCode.value())
        assertEquals(2, okAnalyze.body?.aggregateSummary?.gamesAnalyzed)
        coVerify(exactly = 0) { playerCacheService.getMatchesByIds(any(), any()) }

        // 3. Empty match history returns 200 OK with null body (exact legacy frontend contract)
        every { playerCacheService.getCachedProfile("puuid-empty", "euw1") } returns SummonerProfileDto(
            puuid = "puuid-empty",
            gameName = "Empty",
            tagLine = "EUW",
            summonerLevel = 30,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = emptyList(),
            allMatchIds = emptyList()
        )
        val emptyAnalyze = controller.analyzeTimelines("euw1", "puuid-empty")
        assertEquals(200, emptyAnalyze.statusCode.value())
        assertNull(emptyAnalyze.body)

        // 4. Rate-limited legacy GET /analyze returns 429 (not 200 empty, preventing frontend React state poisoning)
        every { playerCacheService.getCachedProfile("puuid-rl", "euw1") } returns SummonerProfileDto(
            puuid = "puuid-rl",
            gameName = "RateLimited",
            tagLine = "EUW",
            summonerLevel = 100,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = listOf(buildMatchDto("EUW1_RL_LEG", targetPuuid = "puuid-rl")),
            allMatchIds = listOf("EUW1_RL_LEG")
        )
        coEvery {
            riotApiService.getMatchTimelineBounded(eq("EUW1_RL_LEG"), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            arg<AtomicReference<String?>?>(5)?.set("HTTP_429_TOO_MANY_REQUESTS")
            null
        }
        val rlAnalyze = controller.analyzeTimelines("euw1", "puuid-rl")
        assertEquals(429, rlAnalyze.statusCode.value())
    }

    @Test
    fun `transient upstream 5xx and HTTP executor saturation stop immediately without hammering remaining matches and never poison preparedCache`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        val matchIds = (1..20).map { "EUW1_5XX_$it" }
        val matchDtos = matchIds.map { buildMatchDto(it) }
        val riotAttempts = AtomicInteger(0)
        val shouldFailMatch2 = AtomicReference(true)

        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val matchId = firstArg<String>()
            val attempt = riotAttempts.incrementAndGet()
            val failureHolder = arg<AtomicReference<String?>?>(5)
            if (matchId == "EUW1_5XX_2" && shouldFailMatch2.get()) {
                failureHolder?.set("UPSTREAM_5XX")
                null
            } else {
                buildRichRawTimeline(matchId, seed = attempt)
            }
        }

        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = TimelineAnalysisService(),
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider
        )

        // 1. Match 1 succeeds, Match 2 returns HTTP 503 -> MUST stop immediately at 2 attempts (never hammering 3..20),
        // report stopReason = UPSTREAM_ERROR, and MUST NOT cache PARTIAL result in preparedCache!
        val firstRun = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("PARTIAL", firstRun.status)
        assertEquals("UPSTREAM_ERROR", firstRun.stopReason)
        assertEquals(listOf("EUW1_5XX_1"), firstRun.coveredMatchIds)
        assertEquals(2, riotAttempts.get(), "Transient 5xx on match 2 must abort remaining 18 uncached fetches immediately")
        assertEquals(0, service.getPreparedCacheEntryCount(), "PARTIAL transient failure must NEVER be cached in preparedCache")

        // 2. Subsequent retry after upstream recovers -> NOT poisoned by preparedCache, completes all 20, and caches COMPLETE!
        shouldFailMatch2.set(false)
        val secondRun = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("COMPLETE", secondRun.status)
        assertFalse(secondRun.cacheHit)
        assertEquals(20, secondRun.coveredMatchIds.size)
        assertEquals(1, service.getPreparedCacheEntryCount())

        val thirdRun = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
        assertEquals("COMPLETE", thirdRun.status)
        assertTrue(thirdRun.cacheHit)

        // 3. Real RiotApiService boundedHttpExecutor saturation (4 active + 8 queued = 12 capacity) reports HTTP_EXECUTOR_SATURATED (not DEADLINE_EXCEEDED)
        val holdExecutorLatch = CountDownLatch(1)
        val fourStartedLatch = CountDownLatch(4)
        val realRateLimiter = RiotApiRateLimiter(SystemTimeProvider())
        val saturatingFactory = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
            object : org.springframework.http.client.ClientHttpRequest {
                private val headers = org.springframework.http.HttpHeaders()
                override fun getMethod() = org.springframework.http.HttpMethod.GET
                override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_SAT/timeline")
                override fun getHeaders() = headers
                override fun getBody() = ByteArrayOutputStream()
                override fun getAttributes() = mutableMapOf<String, Any>()
                override fun execute(): org.springframework.http.client.ClientHttpResponse {
                    fourStartedLatch.countDown()
                    holdExecutorLatch.await(5_000L, TimeUnit.MILLISECONDS)
                    throw java.io.IOException("aborted")
                }
            }
        }
        val saturatingRestClient = org.springframework.web.client.RestClient.builder().requestFactory(saturatingFactory).build()
        val realRiotApi = RiotApiService(saturatingRestClient, realRateLimiter, saturatingRestClient, SystemTimeProvider())
        val satService = TimelineService(
            riotApiService = realRiotApi,
            redisCacheService = redisCacheService,
            rateLimiter = realRateLimiter,
            timeProvider = SystemTimeProvider(),
            memoryStatsProvider = memoryProvider,
            maxConcurrentPreparations = 16
        )

        val backgroundBlockers = (1..12).map { idx ->
            async(Dispatchers.IO) {
                realRiotApi.getMatchTimelineBounded(
                    matchId = "EUW1_BG_$idx",
                    region = "euw1",
                    maxTotalDurationMs = 5_000L
                )
            }
        }
        assertTrue(fourStartedLatch.await(2_000L, TimeUnit.MILLISECONDS))
        delay(80L) // Ensure all 8 queue slots are filled as well

        try {
            val saturatedRes = satService.prepareCoachTimelines(
                "euw1",
                "puuid-sat-victim",
                listOf("EUW1_SAT_1"),
                listOf(buildMatchDto("EUW1_SAT_1", targetPuuid = "puuid-sat-victim"))
            )
            assertEquals("BLOCKED", saturatedRes.status)
            assertEquals("HTTP_EXECUTOR_SATURATED", saturatedRes.stopReason)
        } finally {
            holdExecutorLatch.countDown()
            backgroundBlockers.awaitAll()
        }
    }

    @Test
    fun `bounded Redis GZIP inflate HTTP GZIP wire-and-inflate caps and 1 MiB compressed preparedCache cap block bombs and memory spikes`() = runBlocking {
        // 1. Real RedisCacheService GZIP bomb (> 4 MiB inflated JSON) and mid-inflate 360 MiB jump abort cleanly
        val redisTemplate = mockk<StringRedisTemplate>()
        val valueOps = mockk<ValueOperations<String, String>>(relaxed = true)
        every { redisTemplate.opsForValue() } returns valueOps
        val realRedisCacheService = RedisCacheService(redisTemplate, mapper)

        val oversizedJsonBytes = ByteArray(5 * 1024 * 1024) { ' '.code.toByte() }.also {
            val prefix = """{"metadata":{"matchId":"EUW1_BOMB","participants":[]},"info":{"frameInterval":60000,"frames":[],"participants":[""".toByteArray()
            System.arraycopy(prefix, 0, it, 0, prefix.size)
        }
        val gzipBombPayload = "GZIP:" + Base64.getEncoder().encodeToString(
            ByteArrayOutputStream().use { bos ->
                GZIPOutputStream(bos).use { gz -> gz.write(oversizedJsonBytes) }
                bos.toByteArray()
            }
        )
        assertTrue(gzipBombPayload.length < 64_000, "Compressed GZIP bomb should be tiny on wire (< 64 KB)")

        val validTimelineGzipPayload = "GZIP:" + Base64.getEncoder().encodeToString(
            ByteArrayOutputStream().use { bos ->
                GZIPOutputStream(bos).use { gz -> gz.write(mapper.writeValueAsBytes(buildRichRawTimeline("EUW1_MID_INF"))) }
                bos.toByteArray()
            }
        )

        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 325L * 1024L * 1024L)
        every { valueOps.get("timeline:euw1:EUW1_BOMB") } returns gzipBombPayload
        every { valueOps.get("timeline:euw1:EUW1_MID_INF") } answers {
            // Raise cgroup usage above 360 MiB right as Redis returns the compressed string before/during GZIP inflate
            memoryProvider.cgroupUsedBytes = 362L * 1024L * 1024L
            validTimelineGzipPayload
        }

        // 2. Real HTTP Content-Encoding: gzip bomb (> 4 MiB inflated from small compressed response) against RiotApiService
        val compressedHttpBomb = ByteArrayOutputStream().use { bos ->
            GZIPOutputStream(bos).use { gz -> gz.write(oversizedJsonBytes) }
            bos.toByteArray()
        }
        val gzipHttpFactory = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
            object : org.springframework.http.client.ClientHttpRequest {
                private val reqHeaders = org.springframework.http.HttpHeaders()
                override fun getMethod() = org.springframework.http.HttpMethod.GET
                override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_HTTP_GZ_BOMB/timeline")
                override fun getHeaders() = reqHeaders
                override fun getBody() = ByteArrayOutputStream()
                override fun getAttributes() = mutableMapOf<String, Any>()
                override fun execute(): org.springframework.http.client.ClientHttpResponse {
                    return object : org.springframework.http.client.ClientHttpResponse {
                        override fun getStatusCode() = org.springframework.http.HttpStatus.OK
                        override fun getStatusText() = "OK"
                        override fun close() {}
                        override fun getHeaders() = org.springframework.http.HttpHeaders().apply {
                            add(org.springframework.http.HttpHeaders.CONTENT_ENCODING, "gzip")
                            contentType = org.springframework.http.MediaType.APPLICATION_JSON
                        }
                        override fun getBody() = java.io.ByteArrayInputStream(compressedHttpBomb)
                    }
                }
            }
        }
        val gzipRestClient = org.springframework.web.client.RestClient.builder().requestFactory(gzipHttpFactory).build()
        val rateLimiter = RiotApiRateLimiter(SystemTimeProvider())
        val realRiotApi = RiotApiService(gzipRestClient, rateLimiter, gzipRestClient, SystemTimeProvider())

        val service = TimelineService(
            riotApiService = realRiotApi,
            redisCacheService = realRedisCacheService,
            rateLimiter = rateLimiter,
            timeProvider = SystemTimeProvider(),
            memoryStatsProvider = memoryProvider
        )

        // Redis > 4 MiB GZIP bomb -> rejected as OVERSIZED_TIMELINE (and HTTP fallback GZIP bomb also rejected!)
        every { valueOps.get("timeline:euw1:EUW1_HTTP_GZ_BOMB") } returns null
        val bombRes = service.prepareCoachTimelines(
            "euw1",
            puuid,
            listOf("EUW1_BOMB", "EUW1_HTTP_GZ_BOMB"),
            listOf(buildMatchDto("EUW1_BOMB"), buildMatchDto("EUW1_HTTP_GZ_BOMB"))
        )
        assertEquals("BLOCKED", bombRes.status)
        assertEquals("OVERSIZED_TIMELINE", bombRes.stopReason)
        assertTrue(bombRes.missingMatches.all { it.reason == "OVERSIZED_TIMELINE" })

        // Mid-inflate 360 MiB jump -> stops immediately with MEMORY_HEADROOM_STOP
        memoryProvider.cgroupUsedBytes = 325L * 1024L * 1024L
        val midInflateRes = service.prepareCoachTimelines(
            "euw1",
            puuid,
            listOf("EUW1_MID_INF"),
            listOf(buildMatchDto("EUW1_MID_INF"))
        )
        assertEquals("BLOCKED", midInflateRes.status)
        assertEquals("MEMORY_HEADROOM_STOP", midInflateRes.stopReason)

        // 3. Populate 32 distinct full 20-match COMPLETE entries into preparedCache -> total compressed bytes MUST stay <= 1 MiB and <= 16 entries!
        memoryProvider.cgroupUsedBytes = 310L * 1024L * 1024L
        val mockRiot = mockk<RiotApiService>()
        val mockRedis = mockk<RedisCacheService>(relaxed = true)
        every { mockRedis.get(any(), MatchTimelineDto::class.java) } returns null
        coEvery {
            mockRiot.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val mid = firstArg<String>()
            val p = mid.substringBefore("_M_")
            buildRichRawTimeline(mid, seed = mid.hashCode() and 0x7f, targetPuuid = p)
        }
        val cacheBoundedService = TimelineService(
            riotApiService = mockRiot,
            redisCacheService = mockRedis,
            timeProvider = FakeTimeProvider(),
            memoryStatsProvider = memoryProvider,
            preparedCacheMaxEntries = 16,
            preparedCacheMaxTotalBytes = 1_048_576
        )
        for (playerIdx in 1..32) {
            val pId = "puuid-cache-cap-$playerIdx"
            val mIds = (1..20).map { "${pId}_M_$it" }
            val mDtos = mIds.map { buildMatchDto(it, targetPuuid = pId) }
            val r = cacheBoundedService.prepareCoachTimelines("euw1", pId, mIds, mDtos)
            assertEquals("COMPLETE", r.status)
            assertTrue(cacheBoundedService.getPreparedCacheEntryCount() <= 16)
            assertTrue(
                cacheBoundedService.getPreparedCacheCompressedBytes() <= 1_048_576,
                "preparedCache compressed bytes (${cacheBoundedService.getPreparedCacheCompressedBytes()}) exceeded 1 MiB cap"
            )
        }
        assertTrue(cacheBoundedService.getPreparedCacheCompressedBytes() in 1..1_048_576)
    }

    @Test
    fun `singleflight isolates follower coroutines from owner cancellation enforces per-follower deadlines and bounds Redis stalls`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)

        // 1. Owner coroutine cancelled while follower is awaiting -> follower MUST NOT throw CancellationException and MUST receive COMPLETE!
        run {
            val enteredFetch = CountDownLatch(1)
            val allowFinish = CountDownLatch(1)
            every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
            coEvery {
                riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
            } coAnswers {
                enteredFetch.countDown()
                withContext(Dispatchers.IO) {
                    allowFinish.await(5_000L, TimeUnit.MILLISECONDS)
                }
                buildRichRawTimeline(firstArg())
            }

            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 5_000L
            )
            val ids = listOf("EUW1_ISO_1")
            val dtos = ids.map { buildMatchDto(it) }

            val ownerJob = launch(Dispatchers.IO) {
                service.prepareCoachTimelines("euw1", puuid, ids, dtos)
            }
            assertTrue(enteredFetch.await(2_000L, TimeUnit.MILLISECONDS))

            val followerDeferred = async(Dispatchers.IO) {
                service.prepareCoachTimelines("euw1", puuid, ids, dtos)
            }
            delay(40L) // Ensure follower registered on singleflight entry

            // Cancel original caller!
            ownerJob.cancel()
            ownerJob.join()

            // Release underlying fetch and prove follower still succeeds without CancellationException
            allowFinish.countDown()
            val followerResult = followerDeferred.await()
            assertEquals("COMPLETE", followerResult.status)
            assertEquals(listOf("EUW1_ISO_1"), followerResult.coveredMatchIds)
        }

        // 2. Slow Redis stall (700ms) with 150ms prepDeadlineMs -> both owner and follower abort near ~150ms with DEADLINE_EXCEEDED!
        run {
            val slowRedis = mockk<RedisCacheService>(relaxed = true)
            every { slowRedis.get(any(), MatchTimelineDto::class.java) } answers {
                try {
                    Thread.sleep(700L)
                } catch (_: InterruptedException) {
                    // Interrupted cleanly by runBoundedCacheReadWithDeadline
                }
                buildRichRawTimeline("EUW1_REDIS_SLOW")
            }

            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = slowRedis,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 150L
            )
            val ids = listOf("EUW1_REDIS_SLOW")
            val dtos = ids.map { buildMatchDto(it) }

            val wallStart = System.currentTimeMillis()
            val results = listOf(
                async(Dispatchers.IO) { service.prepareCoachTimelines("euw1", puuid, ids, dtos) },
                async(Dispatchers.IO) { service.prepareCoachTimelines("euw1", puuid, ids, dtos) }
            ).awaitAll()
            val wallElapsed = System.currentTimeMillis() - wallStart

            assertTrue(results.all { it.status == "BLOCKED" && it.stopReason == "DEADLINE_EXCEEDED" })
            assertTrue(wallElapsed < 550L, "650ms+ Redis stall must abort near 150ms deadline, took ${wallElapsed}ms")
        }
    }

    @Test
    fun `HTTP 429 rateLimiter updateLimits honors deadlineAtMs under mutex contention and ApiPriority LOW yields to foreground HIGH traffic`() = runBlocking {
        // 1. 429 response when rateLimiter bucket mutex is contended for 5s and deadline is 200ms -> returns near 200ms!
        val timeProvider = SystemTimeProvider()
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        rateLimiter.updateLimits("euw1", "20:1", "1:1")

        val requestFactory429 = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
            object : org.springframework.http.client.ClientHttpRequest {
                private val reqHeaders = org.springframework.http.HttpHeaders()
                override fun getMethod() = org.springframework.http.HttpMethod.GET
                override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_429_MTX/timeline")
                override fun getHeaders() = reqHeaders
                override fun getBody() = ByteArrayOutputStream()
                override fun getAttributes() = mutableMapOf<String, Any>()
                override fun execute(): org.springframework.http.client.ClientHttpResponse {
                    return object : org.springframework.http.client.ClientHttpResponse {
                        override fun getStatusCode() = org.springframework.http.HttpStatus.TOO_MANY_REQUESTS
                        override fun getStatusText() = "Too Many Requests"
                        override fun close() {}
                        override fun getHeaders() = org.springframework.http.HttpHeaders().apply {
                            add("Retry-After", "3")
                            add("X-App-Rate-Limit", "20:1")
                            add("X-App-Rate-Limit-Count", "20:1")
                        }
                        override fun getBody() = java.io.ByteArrayInputStream(ByteArray(0))
                    }
                }
            }
        }
        val restClient429 = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory429).build()
        val riotApi429 = RiotApiService(restClient429, rateLimiter, restClient429, timeProvider)

        // Acquire permit first, then lock a SECOND window bucket (120s) right before 429 updateLimits("20:1,100:120") or lock 1s bucket after permit
        rateLimiter.updateLimits("euw1", "20:1,100:120", "1:1,1:120")
        val enteredExecute = CountDownLatch(1)
        val mutexHeld = CountDownLatch(1)
        val releaseMutex = CountDownLatch(1)

        val synchronized429Factory = org.springframework.http.client.ClientHttpRequestFactory { uri, method ->
            object : org.springframework.http.client.ClientHttpRequest {
                private val reqHeaders = org.springframework.http.HttpHeaders()
                override fun getMethod() = method
                override fun getURI() = uri
                override fun getHeaders() = reqHeaders
                override fun getBody() = ByteArrayOutputStream()
                override fun getAttributes() = mutableMapOf<String, Any>()
                override fun execute(): org.springframework.http.client.ClientHttpResponse {
                    enteredExecute.countDown()
                    mutexHeld.await(2_000L, TimeUnit.MILLISECONDS)
                    return requestFactory429.createRequest(uri, method).execute()
                }
            }
        }
        val syncRestClient = org.springframework.web.client.RestClient.builder().requestFactory(synchronized429Factory).build()
        val syncRiotApi = RiotApiService(syncRestClient, rateLimiter, syncRestClient, timeProvider)

        val holderJob = launch(Dispatchers.Default) {
            withContext(Dispatchers.IO) {
                enteredExecute.await(2_000L, TimeUnit.MILLISECONDS)
            }
            rateLimiter.withRegionAppBucketLockForTest("euw1", 1) {
                mutexHeld.countDown()
                withContext(Dispatchers.IO) {
                    releaseMutex.await(5_000L, TimeUnit.MILLISECONDS)
                }
            }
        }

        try {
            val failureReason = AtomicReference<String?>(null)
            val wallStart = System.currentTimeMillis()
            val res = syncRiotApi.getMatchTimelineBounded(
                matchId = "EUW1_429_MTX",
                region = "euw1",
                priority = ApiPriority.LOW,
                maxRetries = 0,
                failureReasonHolder = failureReason,
                maxTotalDurationMs = 220L
            )
            val wallElapsed = System.currentTimeMillis() - wallStart
            assertNull(res)
            assertEquals("HTTP_429_TOO_MANY_REQUESTS", failureReason.get())
            assertTrue(wallElapsed < 1_200L, "429 updateLimits must respect deadlineAtMs instead of blocking 5s on bucket mutex, took ${wallElapsed}ms")
        } finally {
            releaseMutex.countDown()
            holderJob.join()
        }

        // 2. ApiPriority.LOW never sets lastHighPriorityAtMs, caps at 80% of bucket, and yields to active HIGH foreground request
        val fakeClock = FakeTimeProvider()
        val prioLimiter = RiotApiRateLimiter(fakeClock)
        prioLimiter.updateLimits("na1", "20:10", "15:10")

        // First LOW permit brings bucket to 16/20 (80% cap) without marking foreground HIGH activity
        assertTrue(prioLimiter.tryAcquirePermit("na1", ApiPriority.LOW, "match-timeline-v5", fakeClock.now() + 500L))
        // Second LOW permit sees 16/20 == 80% cap (wait ~10s > 500ms budget) -> rejected!
        assertFalse(prioLimiter.tryAcquirePermit("na1", ApiPriority.LOW, "match-timeline-v5", fakeClock.now() + 500L))
        // Foreground HIGH user search STILL succeeds immediately using reserved 20% headroom (17/20)!
        assertTrue(prioLimiter.tryAcquirePermit("na1", ApiPriority.HIGH, "summoner-v4", fakeClock.now() + 500L))

        // And because HIGH just ran at t=now on "na1", a LOW request on "na1" yields for ~250ms clearance (failing fast if budget is 80ms)
        prioLimiter.updateLimits("kr", "20:10", "1:10")
        assertTrue(prioLimiter.tryAcquirePermit("kr", ApiPriority.HIGH, "summoner-v4", fakeClock.now() + 500L))
        assertFalse(
            prioLimiter.tryAcquirePermit("kr", ApiPriority.LOW, "match-timeline-v5", fakeClock.now() + 80L),
            "LOW priority timeline fetch must yield during 250ms foreground HIGH clearance window"
        )
    }

    @Test
    fun `pre-header uninterruptible socket stall returns near deadline via 50ms join cap while holding global concurrency slot until lingering worker exits`() = runBlocking {
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        val timeProvider = SystemTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)
        val rateLimiter = RiotApiRateLimiter(timeProvider)

        val enteredUninterruptibleIo = CountDownLatch(1)
        val releaseUninterruptibleIo = CountDownLatch(1)

        val uninterruptibleFactory = org.springframework.http.client.ClientHttpRequestFactory { _, _ ->
            object : org.springframework.http.client.ClientHttpRequest {
                private val reqHeaders = org.springframework.http.HttpHeaders()
                override fun getMethod() = org.springframework.http.HttpMethod.GET
                override fun getURI() = java.net.URI.create("https://europe.api.riotgames.com/lol/match/v5/matches/EUW1_UNINT/timeline")
                override fun getHeaders() = reqHeaders
                override fun getBody() = ByteArrayOutputStream()
                override fun getAttributes() = mutableMapOf<String, Any>()
                override fun execute(): org.springframework.http.client.ClientHttpResponse {
                    enteredUninterruptibleIo.countDown()
                    // Ignore Thread.interrupt() to simulate an uninterruptible native/DNS/TLS stall
                    val endAt = System.currentTimeMillis() + 5_000L
                    while (System.currentTimeMillis() < endAt) {
                        try {
                            if (releaseUninterruptibleIo.await(50L, TimeUnit.MILLISECONDS)) break
                        } catch (_: InterruptedException) {
                            // Intentionally swallow interrupt until releaseUninterruptibleIo opens
                        }
                    }
                    throw java.io.IOException("Socket closed")
                }
            }
        }
        val restClient = org.springframework.web.client.RestClient.builder().requestFactory(uninterruptibleFactory).build()
        val riotApiService = RiotApiService(restClient, rateLimiter, restClient, timeProvider)
        val service = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            rateLimiter = rateLimiter,
            timeProvider = timeProvider,
            memoryStatsProvider = memoryProvider,
            prepDeadlineMs = 150L,
            maxConcurrentPreparations = 1
        )

        try {
            val wallStart = System.currentTimeMillis()
            val firstRes = service.prepareCoachTimelines(
                "euw1",
                puuid,
                listOf("EUW1_UNINT"),
                listOf(buildMatchDto("EUW1_UNINT"))
            )
            val wallElapsed = System.currentTimeMillis() - wallStart

            assertEquals("BLOCKED", firstRes.status)
            assertEquals("DEADLINE_EXCEEDED", firstRes.stopReason)
            assertTrue(wallElapsed < 600L, "Caller must return near 150ms + 50ms join cap without waiting 5s, took ${wallElapsed}ms")

            // Because the uninterruptible worker is still alive, the 1 global slot MUST remain held so new callers cannot pile up!
            assertEquals(1, service.getActiveGlobalPreparations())
            val secondCallerWhileLingering = service.prepareCoachTimelines(
                "euw1",
                "puuid-second-caller",
                listOf("EUW1_UNINT_2"),
                listOf(buildMatchDto("EUW1_UNINT_2", targetPuuid = "puuid-second-caller"))
            )
            assertEquals("BLOCKED", secondCallerWhileLingering.status)
            assertEquals("CONCURRENCY_LIMIT_REACHED", secondCallerWhileLingering.stopReason)

            // Release the lingering worker and verify global slot drops cleanly back to 0!
            releaseUninterruptibleIo.countDown()
            val waitStart = System.currentTimeMillis()
            while (service.getActiveGlobalPreparations() > 0 && System.currentTimeMillis() - waitStart < 1_000L) {
                delay(15L)
            }
            assertEquals(0, service.getActiveGlobalPreparations())
            assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
        } finally {
            releaseUninterruptibleIo.countDown()
        }
    }

    @Test
    fun `prepareCoachTimelines handles 404, zero-retry 429 stop, active cooldown, and deadline exceeded honestly`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 315L * 1024L * 1024L)
        val rateLimiter = RiotApiRateLimiter(timeProvider)

        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        coEvery {
            riotApiService.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } coAnswers {
            val matchId = firstArg<String>()
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
        coVerify(exactly = 0) {
            riotApiService.getMatchTimelineBounded("EUW1_RL_4", any(), any(), any(), any(), any(), any(), any())
        }
    }

    @Test
    fun `legacy cached timeline is compacted and raw 10-player timeline is garbage-collectable before replacement fetch runs`() = runBlocking {
        val riotApiService = mockk<RiotApiService>()
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        val timeProvider = FakeTimeProvider()
        val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)

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
            val failureHolder = arg<AtomicReference<String?>?>(5)
            io.mockk.clearMocks(redisCacheService, spyAnalysisService, answers = false, recordedCalls = true, childMocks = false)
            repeat(3) {
                System.gc()
                System.runFinalization()
                Thread.sleep(5)
            }
            if (legacyWeakRefs.last().get() == null) {
                wasLegacyCollectedDuringReplacementFetch.incrementAndGet()
            }
            if (matchId == "EUW1_LEG_1") {
                buildRichRawTimeline(matchId, seed = 10)
            } else {
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
        assertEquals(2, wasLegacyCollectedDuringReplacementFetch.get())
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

        assertNull(analysisService.compactTimelineForPlayer(validRaw.copy(info = null), validMatchDto, puuid))
        assertNull(analysisService.compactTimelineForPlayer(validRaw.copy(info = validRaw.info!!.copy(frames = emptyList())), validMatchDto, puuid))
        assertNull(analysisService.compactTimelineForPlayer(validRaw, validMatchDto, "puuid-not-in-match"))

        val framesWithoutPlayer = validRaw.info!!.frames!!.map { frame ->
            frame.copy(participantFrames = frame.participantFrames?.filterKeys { it != "1" })
        }
        assertNull(
            analysisService.compactTimelineForPlayer(
                validRaw.copy(info = validRaw.info!!.copy(frames = framesWithoutPlayer)),
                validMatchDto,
                puuid
            )
        )

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

        io.mockk.clearMocks(redisCacheService, answers = false, recordedCalls = true, childMocks = false)
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
        assertTrue(rawWeakRefs.all { it.get() == null })
    }

    @Test
    fun `production ReactorNettyClientRequestFactory aborts delayed headers, delayed body, and mid-body headroom stop with zero worker or raw retention after caller returns`() = runBlocking {
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null

        val validRaw = buildRichRawTimeline("EUW1_NETTY_1")
        val serializedTimelineBytes = mapper.writeValueAsBytes(validRaw)
        assertTrue(serializedTimelineBytes.size > 4_096)

        val delayedHeaderRequests = AtomicInteger(0)
        val delayedBodyRequests = AtomicInteger(0)
        val delayedBodySecondHalfCompleted = AtomicInteger(0)
        val midBodyHeadroomRequests = AtomicInteger(0)
        val midBodySecondHalfCompleted = AtomicInteger(0)
        val midBodyMemoryProviderRef = AtomicReference<MutableMemoryStatsProvider?>(null)

        val server = com.sun.net.httpserver.HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = java.util.concurrent.Executors.newCachedThreadPool { r ->
            Thread(r, "local-test-riot-server").apply { isDaemon = true }
        }
        server.createContext("/lol/match/v5/matches/") { exchange ->
            val path = exchange.requestURI.path
            try {
                when {
                    path.contains("EUW1_NETTY_HDR_1") -> {
                        delayedHeaderRequests.incrementAndGet()
                        Thread.sleep(5_000L)
                        exchange.responseHeaders.add("Content-Type", "application/json")
                        exchange.sendResponseHeaders(200, serializedTimelineBytes.size.toLong())
                        exchange.responseBody.use { it.write(serializedTimelineBytes) }
                    }
                    path.contains("EUW1_NETTY_BODY_1") -> {
                        delayedBodyRequests.incrementAndGet()
                        exchange.responseHeaders.add("Content-Type", "application/json")
                        exchange.sendResponseHeaders(200, 0L)
                        val out = exchange.responseBody
                        val splitIndex = serializedTimelineBytes.size / 2
                        out.write(serializedTimelineBytes, 0, splitIndex)
                        out.flush()
                        Thread.sleep(5_000L)
                        out.write(serializedTimelineBytes, splitIndex, serializedTimelineBytes.size - splitIndex)
                        out.flush()
                        delayedBodySecondHalfCompleted.incrementAndGet()
                    }
                    path.contains("EUW1_NETTY_MEM_1") -> {
                        midBodyHeadroomRequests.incrementAndGet()
                        exchange.responseHeaders.add("Content-Type", "application/json")
                        exchange.sendResponseHeaders(200, 0L)
                        val out = exchange.responseBody
                        val firstChunkLen = 1_024
                        val secondChunkLen = 1_024
                        out.write(serializedTimelineBytes, 0, firstChunkLen)
                        out.flush()
                        Thread.sleep(80L)
                        midBodyMemoryProviderRef.get()?.cgroupUsedBytes = 362L * 1024L * 1024L
                        out.write(serializedTimelineBytes, firstChunkLen, secondChunkLen)
                        out.flush()
                        Thread.sleep(5_000L)
                        val remainingOffset = firstChunkLen + secondChunkLen
                        out.write(serializedTimelineBytes, remainingOffset, serializedTimelineBytes.size - remainingOffset)
                        out.flush()
                        midBodySecondHalfCompleted.incrementAndGet()
                    }
                    else -> exchange.sendResponseHeaders(404, -1L)
                }
            } catch (_: Throwable) {
            } finally {
                runCatching { exchange.close() }
            }
        }
        server.start()
        val port = server.address.port

        try {
            val prodRestClient = com.pride.summoner_searcher_api.config.RestClientConfig.buildRiotRestClient("test-riot-key")

            // 1. Delayed-Header test
            run {
                val timeProvider = SystemTimeProvider()
                val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
                val rateLimiter = RiotApiRateLimiter(timeProvider)
                val riotApiService = RiotApiService(prodRestClient, rateLimiter, prodRestClient, timeProvider).apply {
                    matchBaseUrlResolver = { "http://127.0.0.1:$port" }
                }
                val service = TimelineService(
                    riotApiService = riotApiService,
                    redisCacheService = redisCacheService,
                    timelineAnalysisService = TimelineAnalysisService(),
                    rateLimiter = rateLimiter,
                    timeProvider = timeProvider,
                    memoryStatsProvider = memoryProvider,
                    prepDeadlineMs = 600L
                )

                val matchIds = listOf("EUW1_NETTY_HDR_1")
                val matchDtos = matchIds.map { buildMatchDto(it) }
                val wallStart = System.currentTimeMillis()
                val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
                val wallElapsed = System.currentTimeMillis() - wallStart

                assertEquals(1, delayedHeaderRequests.get())
                assertEquals("BLOCKED", res.status)
                assertEquals("DEADLINE_EXCEEDED", res.stopReason)
                assertTrue(res.coveredMatchIds.isEmpty())
                assertNull(res.analysis)
                assertEquals(0, res.peakRetainedRawTimelines)
                assertTrue(wallElapsed < 2_200L, "Delayed-header request should abort near 600ms deadline, took ${wallElapsed}ms")
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
                assertEquals(0, service.getActiveGlobalPreparations())
            }

            // 2. Delayed-Body test
            run {
                val timeProvider = SystemTimeProvider()
                val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
                val rateLimiter = RiotApiRateLimiter(timeProvider)
                val riotApiService = RiotApiService(prodRestClient, rateLimiter, prodRestClient, timeProvider).apply {
                    matchBaseUrlResolver = { "http://127.0.0.1:$port" }
                }
                val service = TimelineService(
                    riotApiService = riotApiService,
                    redisCacheService = redisCacheService,
                    timelineAnalysisService = TimelineAnalysisService(),
                    rateLimiter = rateLimiter,
                    timeProvider = timeProvider,
                    memoryStatsProvider = memoryProvider,
                    prepDeadlineMs = 600L
                )

                val matchIds = listOf("EUW1_NETTY_BODY_1")
                val matchDtos = matchIds.map { buildMatchDto(it) }
                val wallStart = System.currentTimeMillis()
                val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
                val wallElapsed = System.currentTimeMillis() - wallStart

                assertEquals(1, delayedBodyRequests.get())
                assertEquals("BLOCKED", res.status)
                assertEquals("DEADLINE_EXCEEDED", res.stopReason)
                assertTrue(res.coveredMatchIds.isEmpty())
                assertNull(res.analysis)
                assertEquals(0, res.peakRetainedRawTimelines)
                assertEquals(0, delayedBodySecondHalfCompleted.get())
                assertTrue(wallElapsed < 2_200L, "Delayed-body stream should abort near 600ms deadline, took ${wallElapsed}ms")
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
                assertEquals(0, service.getActiveGlobalPreparations())
                io.mockk.verify(exactly = 0) { redisCacheService.set(any<String>(), any<MatchTimelineDto>(), any<Duration>()) }
            }

            // 3. Mid-Body 360 MiB Memory Headroom Stop test
            run {
                val timeProvider = SystemTimeProvider()
                val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 325L * 1024L * 1024L)
                midBodyMemoryProviderRef.set(memoryProvider)
                val rateLimiter = RiotApiRateLimiter(timeProvider)
                val riotApiService = RiotApiService(prodRestClient, rateLimiter, prodRestClient, timeProvider).apply {
                    matchBaseUrlResolver = { "http://127.0.0.1:$port" }
                }
                val service = TimelineService(
                    riotApiService = riotApiService,
                    redisCacheService = redisCacheService,
                    timelineAnalysisService = TimelineAnalysisService(),
                    rateLimiter = rateLimiter,
                    timeProvider = timeProvider,
                    memoryStatsProvider = memoryProvider,
                    prepDeadlineMs = 5_000L
                )

                val matchIds = listOf("EUW1_NETTY_MEM_1")
                val matchDtos = matchIds.map { buildMatchDto(it) }
                val wallStart = System.currentTimeMillis()
                val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
                val wallElapsed = System.currentTimeMillis() - wallStart

                assertEquals(1, midBodyHeadroomRequests.get())
                assertEquals("BLOCKED", res.status)
                assertEquals("MEMORY_HEADROOM_STOP", res.stopReason)
                assertTrue(res.coveredMatchIds.isEmpty())
                assertNull(res.analysis)
                assertEquals(0, res.peakRetainedRawTimelines)
                assertEquals(0, midBodySecondHalfCompleted.get())
                assertTrue(wallElapsed < 1_800L, "Mid-body headroom stop should abort near 80ms without waiting 5s, took ${wallElapsed}ms")
                assertEquals(0, riotApiService.getActiveBoundedHttpWorkers())
                assertEquals(0, service.getActiveGlobalPreparations())
                io.mockk.verify(exactly = 0) { redisCacheService.set(any<String>(), any<MatchTimelineDto>(), any<Duration>()) }
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `rate limiter bounds both initial mutex acquisition and post-delay mutex reacquisition by remaining deadline budget`() = runBlocking {
        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), MatchTimelineDto::class.java) } returns null
        val matchIds = listOf("EUW1_MTX_1")
        val matchDtos = matchIds.map { buildMatchDto(it) }

        // 1. Initial bucket mutex acquisition contention exceeds remaining budget
        run {
            val timeProvider = SystemTimeProvider()
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(timeProvider)
            rateLimiter.updateLimits("euw1", "20:1", "0:1")

            val httpCallCount = AtomicInteger(0)
            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { uri, _ ->
                httpCallCount.incrementAndGet()
                throw AssertionError("HTTP should not execute when bucket mutex acquisition times out: $uri")
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
                prepDeadlineMs = 200L
            )

            val lockAcquiredLatch = CountDownLatch(1)
            val releaseLockLatch = CountDownLatch(1)
            val holderJob = launch(Dispatchers.Default) {
                rateLimiter.withRegionAppBucketLockForTest("euw1", 1) {
                    lockAcquiredLatch.countDown()
                    withContext(Dispatchers.IO) {
                        releaseLockLatch.await(5_000L, TimeUnit.MILLISECONDS)
                    }
                }
            }
            assertTrue(lockAcquiredLatch.await(2_000L, TimeUnit.MILLISECONDS))

            try {
                val wallStart = System.currentTimeMillis()
                val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
                val wallElapsed = System.currentTimeMillis() - wallStart

                assertEquals("BLOCKED", res.status)
                assertEquals("DEADLINE_EXCEEDED", res.stopReason)
                assertEquals(0, httpCallCount.get())
                assertTrue(wallElapsed < 1_500L, "Initial mutex contention must abort near 200ms budget, took ${wallElapsed}ms")
            } finally {
                releaseLockLatch.countDown()
                holderJob.join()
            }
        }

        // 2. Post-delay mutex reacquisition contention
        run {
            val startWall = System.currentTimeMillis()
            val customTimeProvider = object : TimeProvider {
                @Volatile
                var offsetMs: Long = 0L
                override fun now(): Long = System.currentTimeMillis() + offsetMs
            }
            val memoryProvider = MutableMemoryStatsProvider(cgroupUsedBytes = 320L * 1024L * 1024L)
            val rateLimiter = RiotApiRateLimiter(customTimeProvider)
            customTimeProvider.offsetMs = -910L
            rateLimiter.updateLimits("euw1", "1:1", "1:1")
            customTimeProvider.offsetMs = 0L

            val httpCallCount = AtomicInteger(0)
            val requestFactory = org.springframework.http.client.ClientHttpRequestFactory { uri, _ ->
                httpCallCount.incrementAndGet()
                throw AssertionError("HTTP should not execute when post-delay mutex reacquisition times out: $uri")
            }
            val restClient = org.springframework.web.client.RestClient.builder().requestFactory(requestFactory).build()
            val riotApiService = RiotApiService(restClient, rateLimiter, restClient, customTimeProvider)
            val service = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = customTimeProvider,
                memoryStatsProvider = memoryProvider,
                prepDeadlineMs = 280L
            )

            val releaseLockLatch = CountDownLatch(1)
            val contenderJob = launch(Dispatchers.Default) {
                delay(35L)
                rateLimiter.withRegionAppBucketLockForTest("euw1", 1) {
                    withContext(Dispatchers.IO) {
                        releaseLockLatch.await(5_000L, TimeUnit.MILLISECONDS)
                    }
                }
            }

            try {
                val res = service.prepareCoachTimelines("euw1", puuid, matchIds, matchDtos)
                val wallElapsed = System.currentTimeMillis() - startWall

                assertEquals("BLOCKED", res.status)
                assertEquals("DEADLINE_EXCEEDED", res.stopReason)
                assertEquals(0, httpCallCount.get())
                assertTrue(wallElapsed < 1_500L, "Post-delay mutex reacquisition must abort near 280ms budget, took ${wallElapsed}ms")
            } finally {
                releaseLockLatch.countDown()
                contenderJob.join()
            }
        }
    }

    @Test
    fun `AiAnalysisService formats PARTIAL and BLOCKED timelineCoverage boundaries without unrelated turret-plate prompt drift and preserves schema-leak checks`() {
        val exampleRepo = mockk<AiLearnedExampleRepository>(relaxed = true)
        val interactionRepo = mockk<AiInteractionRepository>(relaxed = true)
        val patternRepo = mockk<DiscoveredPatternRepository>(relaxed = true)
        val promptVersionRepo = mockk<PromptVersionRepository>(relaxed = true)
        every { exampleRepo.findByIsActiveTrueAndQuestionCategoryOrderByQualityScoreDesc(any(), any()) } returns emptyList()
        every { patternRepo.findByTriggerCategoryAndIsActiveTrue(any()) } returns emptyList()
        every { promptVersionRepo.findByIsActiveTrueAndCategoryScope(any()) } returns emptyList()
        every { promptVersionRepo.findByIsActiveTrueAndCategoryScopeIsNull() } returns emptyList()

        val aiService = AiAnalysisService(
            apiKey = "test-key",
            exampleRepository = exampleRepo,
            interactionRepository = interactionRepo,
            patternRepository = patternRepo,
            promptVersionRepository = promptVersionRepo
        )

        val constructPromptMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "constructPrompt",
            Map::class.java,
            List::class.java,
            String::class.java,
            AiAnalysisService.IntentResult::class.java,
            com.pride.summoner_searcher_api.model.PromptVersion::class.java
        ).apply { isAccessible = true }

        val partialContext = mapOf<String, Any>(
            "summonerName" to "Spear Shot",
            "rank" to "CHALLENGER I",
            "primaryRole" to "TOP",
            "timelineStatus" to "PARTIAL",
            "timelineCoverage" to mapOf(
                "status" to "PARTIAL",
                "requested" to 20,
                "covered" to 12,
                "missing" to 8,
                "stopReason" to "RATE_LIMIT_COOLDOWN"
            ),
            "timelineInsights" to mapOf(
                "gamesAnalyzed" to 12,
                "avgGoldLeadAt10" to 320
            )
        )
        val partialPrompt = constructPromptMethod.invoke(
            aiService,
            partialContext,
            emptyList<Map<String, String>>(),
            "How is my early laning?",
            null,
            null
        ) as String

        assertTrue(partialPrompt.contains("Timeline (partial timeline analyzed)"))
        assertTrue(partialPrompt.contains("timelineStatus: PARTIAL"))
        assertTrue(partialPrompt.contains("timelineCoverage: {status=PARTIAL, covered=12/20 games, stopReason=RATE_LIMIT_COOLDOWN}"))
        assertTrue(partialPrompt.contains("When `timelineStatus` is `PARTIAL`, cite the exact `covered`/`requested` timeline denominator from `timelineCoverage`"))
        assertFalse(partialPrompt.contains("5 plates per turret across 9 lane turrets"), "Unrelated Season 2026 turret-plate prompt hunk must be omitted")

        // Verify BLOCKED context with null timelineInsights still emits TIMELINE COVERAGE BOUNDARY
        val blockedContext = mapOf<String, Any>(
            "summonerName" to "Spear Shot",
            "rank" to "CHALLENGER I",
            "timelineStatus" to "BLOCKED",
            "timelineCoverage" to mapOf(
                "status" to "BLOCKED",
                "requested" to 20,
                "covered" to 0,
                "stopReason" to "MEMORY_HEADROOM_ADMISSION_BLOCKED"
            )
        )
        val blockedPrompt = constructPromptMethod.invoke(
            aiService,
            blockedContext,
            emptyList<Map<String, String>>(),
            "How is my early laning?",
            null,
            null
        ) as String
        assertTrue(blockedPrompt.contains("timelineCoverage: {status=BLOCKED, covered=0/20 games, stopReason=MEMORY_HEADROOM_ADMISSION_BLOCKED}"))
        assertTrue(blockedPrompt.contains("TIMELINE COVERAGE BOUNDARY"))

        // Verify #38 schema-leak detector remains intact
        assertTrue(AiAnalysisService.containsInternalSchemaLeak("Your timelineStatus is NOT_ANALYZED"))
        assertFalse(AiAnalysisService.containsInternalSchemaLeak("Minute-by-minute timeline data is available for 12 of your last 20 games."))
    }
}
