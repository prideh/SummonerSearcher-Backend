package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchInfo
import com.pride.summoner_searcher_api.dto.MatchMetadata
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.MatchTimelineInfo
import com.pride.summoner_searcher_api.dto.MatchTimelineMetadata
import com.pride.summoner_searcher_api.dto.ParticipantDto
import com.pride.summoner_searcher_api.dto.ParticipantFrameDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.dto.TimelineEventDto
import com.pride.summoner_searcher_api.dto.TimelineFrameDto
import com.pride.summoner_searcher_api.dto.TimelineParticipantDto
import com.pride.summoner_searcher_api.dto.TimelinePositionDto
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class CoachTimelineOptionAPreAdmissionTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    private val playerPuuid = "fj7q0_L_QDjddr9QbLBu_i616eJPg3OH3qrevxj834XoQQJ15qV5pIPCz7aV2mfRuE-xejxcPmMuPA"
    private val oppPuuid = "opp-puuid-sensitive-99887766"
    private val gameName = "PrideSensitiveSummoner"
    private val tagLine = "PERSI"
    private val matchIds = listOf("EUW1_7698327758", "EUW1_7682617257")

    private class MutableClock(initialMs: Long = 5_000L) : TimeProvider {
        private val nowRef = AtomicLong(initialMs)
        override fun now(): Long = nowRef.get()
        fun advanceBy(deltaMs: Long) {
            nowRef.addAndGet(deltaMs)
        }
    }

    private fun buildSample(
        cgroupCurrentBytes: Long,
        heapUsedBytes: Long,
        heapCommittedBytes: Long,
        heapMaxBytes: Long = 214_748_364L,
        anonBytes: Long = 335_953_920L,
        fileBytes: Long = 32_636_928L,
        inactiveFileBytes: Long = 28_598_272L,
        oldGcCount: Long = 4L
    ): RawMemorySample = RawMemorySample(
        jvmHeapUsedBytes = heapUsedBytes,
        jvmHeapCommittedBytes = heapCommittedBytes,
        jvmHeapMaxBytes = heapMaxBytes,
        jvmNonHeapUsedBytes = 109_051_904L,
        jvmMetaspaceUsedBytes = 79_691_776L,
        jvmMetaspaceCommittedBytes = 82_837_504L,
        jvmCompressedClassSpaceUsedBytes = 9_437_184L,
        jvmCompressedClassSpaceCommittedBytes = 10_485_760L,
        jvmCodeCacheUsedBytes = 19_922_944L,
        jvmCodeCacheCommittedBytes = 20_971_520L,
        directBufferUsedBytes = 8_388_608L,
        directBufferCapacityBytes = 8_388_608L,
        mappedBufferUsedBytes = 0L,
        jvmThreadCount = 35,
        cgroupMemoryCurrentBytes = cgroupCurrentBytes,
        cgroupMemoryMaxBytes = 536_870_912L,
        cgroupMemoryPeakBytes = maxOf(cgroupCurrentBytes, 372_162_560L),
        cgroupAnonBytes = anonBytes,
        cgroupFileCacheBytes = fileBytes,
        cgroupInactiveFileBytes = inactiveFileBytes,
        oldGenGcCount = oldGcCount
    )

    private fun buildParticipant(
        puuidValue: String,
        pid: Int,
        teamId: Int,
        champId: Int,
        champName: String,
        rGameName: String,
        rTag: String,
        won: Boolean
    ): ParticipantDto = objectMapper.convertValue(
        mapOf(
            "puuid" to puuidValue,
            "participantId" to pid,
            "teamId" to teamId,
            "teamPosition" to "TOP",
            "individualPosition" to "TOP",
            "championId" to champId,
            "championName" to champName,
            "riotIdGameName" to rGameName,
            "riotIdTagline" to rTag,
            "summonerName" to rGameName,
            "win" to won,
            "kills" to if (won) 6 else 2,
            "deaths" to if (won) 2 else 6,
            "assists" to 5
        ),
        ParticipantDto::class.java
    )

    private fun buildProfile(): SummonerProfileDto {
        val matches = matchIds.mapIndexed { idx, matchId ->
            MatchDto(
                metadata = MatchMetadata(matchId = matchId, participants = listOf(playerPuuid, oppPuuid)),
                info = MatchInfo(
                    gameId = 7_698_000_000L + idx,
                    gameCreation = 1_700_000_000_000L,
                    gameDuration = 1_800L,
                    gameMode = "CLASSIC",
                    queueId = 420,
                    participants = listOf(
                        buildParticipant(playerPuuid, 1, 100, 24, "Jax", gameName, tagLine, true),
                        buildParticipant(oppPuuid, 6, 200, 122, "Darius", "OpponentSecretName", "EUW", false)
                    )
                )
            )
        }
        return SummonerProfileDto(
            puuid = playerPuuid,
            gameName = gameName,
            tagLine = tagLine,
            summonerLevel = 412L,
            profileIconId = 1,
            soloQueueRank = null,
            recentMatches = matches,
            allMatchIds = matchIds
        )
    }

    private fun buildTimeline(matchId: String): MatchTimelineDto {
        val frames = (0..15).map { minute ->
            val pFrames = mapOf(
                "1" to ParticipantFrameDto(
                    participantId = 1,
                    totalGold = 500 + minute * 360,
                    currentGold = 300,
                    level = (1 + minute / 2).coerceAtMost(18),
                    xp = minute * 450,
                    minionsKilled = minute * 7,
                    jungleMinionsKilled = 0,
                    position = TimelinePositionDto(x = 2200 + minute * 150, y = 11800 - minute * 150)
                ),
                "6" to ParticipantFrameDto(
                    participantId = 6,
                    totalGold = 500 + minute * 320,
                    currentGold = 250,
                    level = (1 + minute / 2).coerceAtMost(18),
                    xp = minute * 420,
                    minionsKilled = minute * 6,
                    jungleMinionsKilled = 0,
                    position = TimelinePositionDto(x = 2400 + minute * 150, y = 11600 - minute * 150)
                )
            )
            val events = if (minute == 6) {
                listOf(
                    TimelineEventDto(
                        type = "CHAMPION_KILL",
                        timestamp = 360_000L,
                        participantId = null,
                        killerId = 1,
                        victimId = 6,
                        assistingParticipantIds = emptyList(),
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
                        position = TimelinePositionDto(x = 3100, y = 10900),
                        bounty = 300,
                        shutdownBounty = 0,
                        goldGain = null
                    )
                )
            } else {
                emptyList()
            }
            TimelineFrameDto(timestamp = minute * 60_000L, participantFrames = pFrames, events = events)
        }
        return MatchTimelineDto(
            metadata = MatchTimelineMetadata(matchId = matchId, participants = listOf(playerPuuid, oppPuuid)),
            info = MatchTimelineInfo(
                frameInterval = 60_000L,
                frames = frames,
                participants = listOf(
                    TimelineParticipantDto(participantId = 1, puuid = playerPuuid),
                    TimelineParticipantDto(participantId = 6, puuid = oppPuuid)
                )
            )
        )
    }

    private fun newMockedTimelineDeps(): Pair<RiotApiService, RedisCacheService> {
        val riotApi = mockk<RiotApiService>(relaxed = true)
        val redisCache = mockk<RedisCacheService>(relaxed = true)
        every { redisCache.get(any(), MatchTimelineDto::class.java) } answers {
            val key = firstArg<String>()
            val matchId = key.substringAfterLast(':')
            buildTimeline(matchId)
        }
        coEvery {
            riotApi.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
        } answers {
            buildTimeline(firstArg())
        }
        return riotApi to redisCache
    }

    private fun assertZeroPlayerPayloadLeakage(summaryLine: String) {
        assertFalse(summaryLine.contains(playerPuuid), "Summary leaked player PUUID: $summaryLine")
        assertFalse(summaryLine.contains(oppPuuid), "Summary leaked opponent PUUID: $summaryLine")
        assertFalse(summaryLine.contains(gameName, ignoreCase = true), "Summary leaked gameName: $summaryLine")
        assertFalse(summaryLine.contains(tagLine, ignoreCase = true), "Summary leaked tagLine: $summaryLine")
        for (mid in matchIds) {
            assertFalse(summaryLine.contains(mid), "Summary leaked matchId $mid: $summaryLine")
        }
    }

    @DisplayName("default config OFF runs zero GC on 354.92 MiB blocked admission and emits payload-free pool breakdown")
    @Test
    fun defaultConfigOffRunsZeroGcAndLogsPayloadFreeTelemetry() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(1_000L)
        val gcCalls = AtomicInteger(0)
        val summaries = Collections.synchronizedList(mutableListOf<String>())

        // Default constructor uses preAdmissionGcEnabled = false while unusedCommitted = 95 MiB (>= 24 MiB)
        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = buildSample(
                    cgroupCurrentBytes = 372_162_560L, // 354.92 MiB (> 350 MiB admission)
                    heapUsedBytes = 90L * 1024L * 1024L,
                    heapCommittedBytes = 185L * 1024L * 1024L
                )
            }
        )
        service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
        service.stageSummaryObserver = { summaries.add(it) }

        val outcome = service.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = { buildProfile() }
        )
        val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("BLOCKED", ready.result.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", ready.result.stopReason)
        assertEquals(0, gcCalls.get(), "Default OFF must never invoke System.gc()")
        assertEquals(1, summaries.size)

        val line = summaries.single()
        assertZeroPlayerPayloadLeakage(line)
        assertTrue(line.contains("stage=pre_admission"))
        assertTrue(line.contains("gcEnabled=false gcAttempted=false gcStatus=DISABLED_BY_CONFIG"))
        assertTrue(line.contains("cgroupCur=372162560"))
        assertTrue(line.contains("anon=335953920"))
        assertTrue(line.contains("file=32636928"))
        assertTrue(line.contains("inactiveFile=28598272"))
        assertTrue(line.contains("heapUsed=${90L * 1024L * 1024L}"))
        assertTrue(line.contains("heapComm=${185L * 1024L * 1024L}"))
        assertTrue(line.contains("heapUnusedComm=${95L * 1024L * 1024L}"))
        assertTrue(line.contains("memPostGc={none}"))
    }

    @DisplayName("healthy admission runs zero GC even when enabled and unusedCommitted exceeds 24 MiB")
    @Test
    fun healthyAdmissionNeverRunsGc() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(2_000L)
        val gcCalls = AtomicInteger(0)
        val summaries = Collections.synchronizedList(mutableListOf<String>())
        val callCounter = AtomicInteger(0)

        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample {
                    val n = callCounter.incrementAndGet()
                    val current = if (n == 1) 352_321_536L else 354_418_688L // 336 MiB -> 338 MiB (< 350/360 MiB)
                    return buildSample(
                        cgroupCurrentBytes = current,
                        heapUsedBytes = 85L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L
                    )
                }
            },
            preAdmissionGcEnabled = true
        )
        service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
        service.stageSummaryObserver = { summaries.add(it) }

        val outcome = service.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = { buildProfile() }
        )
        val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("COMPLETE", ready.result.status)
        assertEquals(2, ready.result.coveredMatchIds.size)
        assertEquals(0, gcCalls.get(), "Healthy admission must never run GC")

        val line = summaries.single()
        assertZeroPlayerPayloadLeakage(line)
        assertTrue(line.contains("status=COMPLETE stage=timeline_loop"))
        assertTrue(line.contains("gcEnabled=true gcAttempted=false gcStatus=NOT_NEEDED"))
        assertTrue(line.contains("memPre={cgroupCur=352321536,"))
        assertTrue(line.contains("memPostGc={none}"))
        assertTrue(line.contains("memEnd={cgroupCur=354418688,"))
    }

    @DisplayName("blocked admission with live heap (unusedCommitted < 24 MiB) skips GC even when enabled")
    @Test
    fun blockedAdmissionWithLiveHeapSkipsGc() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(3_000L)
        val gcCalls = AtomicInteger(0)
        val summaries = Collections.synchronizedList(mutableListOf<String>())

        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = buildSample(
                    cgroupCurrentBytes = 372_162_560L, // 354.92 MiB blocked
                    heapUsedBytes = 160L * 1024L * 1024L,
                    heapCommittedBytes = 175L * 1024L * 1024L // 15 MiB unused (< 24 MiB threshold)
                )
            },
            preAdmissionGcEnabled = true
        )
        service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
        service.stageSummaryObserver = { summaries.add(it) }

        val outcome = service.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = { buildProfile() }
        )
        val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("BLOCKED", ready.result.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", ready.result.stopReason)
        assertEquals(0, gcCalls.get(), "Live heap with < 24 MiB unused committed must skip GC")

        val line = summaries.single()
        assertZeroPlayerPayloadLeakage(line)
        assertTrue(line.contains("gcEnabled=true gcAttempted=false gcStatus=SKIPPED_LOW_UNUSED_COMMITTED"))
        assertTrue(line.contains("heapUnusedComm=${15L * 1024L * 1024L}"))
    }

    @DisplayName("enabled pre-admission GC attempts once and still blocks at 350 MiB when memory is not reclaimed")
    @Test
    fun guardStillBlocksAfterSingleGcWhenMemoryNotReclaimed() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(4_000L)
        val gcRan = AtomicBoolean(false)
        val gcCalls = AtomicInteger(0)
        val sampleCalls = AtomicInteger(0)
        val summaries = Collections.synchronizedList(mutableListOf<String>())

        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample {
                    sampleCalls.incrementAndGet()
                    return if (!gcRan.get()) {
                        buildSample(
                            cgroupCurrentBytes = 372_162_560L, // 354.92 MiB pre-GC
                            heapUsedBytes = 92L * 1024L * 1024L,
                            heapCommittedBytes = 190L * 1024L * 1024L
                        )
                    } else {
                        buildSample(
                            cgroupCurrentBytes = 368_574_464L, // 351.50 MiB post-GC (still >= 350 MiB!)
                            heapUsedBytes = 88L * 1024L * 1024L,
                            heapCommittedBytes = 170L * 1024L * 1024L,
                            oldGcCount = 5L
                        )
                    }
                }
            },
            preAdmissionGcEnabled = true
        )
        service.preAdmissionGcRunner = {
            gcCalls.incrementAndGet()
            clock.advanceBy(18L)
            gcRan.set(true)
        }
        service.stageSummaryObserver = { summaries.add(it) }

        val outcome = service.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = { buildProfile() }
        )
        val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("BLOCKED", ready.result.status)
        assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", ready.result.stopReason)
        assertEquals(1, gcCalls.get(), "Exactly one GC attempt allowed")
        assertEquals(2, sampleCalls.get(), "Exactly one initial sample + one post-GC recheck (zero retry loop)")

        val line = summaries.single()
        assertZeroPlayerPayloadLeakage(line)
        assertTrue(line.contains("stage=pre_admission"))
        assertTrue(line.contains("gcEnabled=true gcAttempted=true gcStatus=STILL_BLOCKED_AFTER_GC gcMs=18"))
        assertTrue(line.contains("memPre={cgroupCur=372162560,"))
        assertTrue(line.contains("memPostGc={cgroupCur=368574464,"))
        assertTrue(line.contains("memEnd={cgroupCur=368574464,"))
    }

    @DisplayName("enabled pre-admission GC admits and completes when unshrunk committed heap reclaims below 350 MiB")
    @Test
    fun admitsAndCompletesWhenSinglePreAdmissionGcReclaimsBelow350MiB() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(6_000L)
        val gcRan = AtomicBoolean(false)
        val gcCalls = AtomicInteger(0)
        val summaries = Collections.synchronizedList(mutableListOf<String>())

        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample {
                    return if (!gcRan.get()) {
                        buildSample(
                            cgroupCurrentBytes = 372_162_560L, // 354.92 MiB pre-GC
                            heapUsedBytes = 90L * 1024L * 1024L,
                            heapCommittedBytes = 192L * 1024L * 1024L // 102 MiB unused committed
                        )
                    } else {
                        buildSample(
                            cgroupCurrentBytes = 350_224_384L, // 334.00 MiB post-GC (< 350 MiB admission)
                            heapUsedBytes = 86L * 1024L * 1024L,
                            heapCommittedBytes = 128L * 1024L * 1024L,
                            anonBytes = 314_015_744L,
                            oldGcCount = 5L
                        )
                    }
                }
            },
            preAdmissionGcEnabled = true
        )
        service.preAdmissionGcRunner = {
            gcCalls.incrementAndGet()
            clock.advanceBy(22L)
            gcRan.set(true)
        }
        service.stageSummaryObserver = { summaries.add(it) }

        val outcome = service.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = { buildProfile() }
        )
        val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("COMPLETE", ready.result.status)
        assertEquals(2, ready.result.coveredMatchIds.size)
        assertNotNull(ready.result.analysis)
        assertEquals(1, gcCalls.get())

        val line = summaries.single()
        assertZeroPlayerPayloadLeakage(line)
        assertTrue(line.contains("status=COMPLETE stage=timeline_loop covered=2/2"))
        assertTrue(line.contains("gcEnabled=true gcAttempted=true gcStatus=RECLAIMED_ADMITTED gcMs=22"))
        assertTrue(line.contains("memPre={cgroupCur=372162560,"))
        assertTrue(line.contains("memPostGc={cgroupCur=350224384,"))
        assertTrue(line.contains("memEnd={cgroupCur=350224384,"))
    }

    @DisplayName("concurrent blocked requests + 30s cooldown + midstream 360 MiB stop enforce strictly bounded pre-admission-only GC")
    @Test
    fun enforcesConcurrentSingleGcCooldownAndZeroMidstreamGc() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()
        val clock = MutableClock(10_000L)
        val gcCalls = AtomicInteger(0)
        val gcEntered = CountDownLatch(1)
        val releaseGc = CountDownLatch(1)
        val summaries = Collections.synchronizedList(mutableListOf<String>())

        val blockedProvider = object : MemoryStatsProvider {
            override fun sample(): RawMemorySample = buildSample(
                cgroupCurrentBytes = 372_162_560L, // stays > 350 MiB
                heapUsedBytes = 90L * 1024L * 1024L,
                heapCommittedBytes = 170L * 1024L * 1024L // 80 MiB unused committed
            )
        }

        val service = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = clock,
            memoryStatsProvider = blockedProvider,
            preAdmissionGcEnabled = true,
            preAdmissionGcCooldownMs = 30_000L
        )
        service.preAdmissionGcRunner = {
            val attempt = gcCalls.incrementAndGet()
            if (attempt == 1) {
                gcEntered.countDown()
                assertTrue(releaseGc.await(5, TimeUnit.SECONDS))
            }
        }
        service.stageSummaryObserver = { summaries.add(it) }

        // 1. Fire 8 concurrent requests while thread 1 is inside preAdmissionGcRunner
        val winnerDeferred = async(Dispatchers.IO) {
            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
        }
        assertTrue(gcEntered.await(5, TimeUnit.SECONDS))

        val concurrentPeers = (1..7).map { idx ->
            async(Dispatchers.IO) {
                service.prepareCoachTimelinesForPlayer("euw1", "$playerPuuid-$idx", matchIds) { buildProfile() }
            }
        }
        val peerOutcomes = concurrentPeers.awaitAll()
        releaseGc.countDown()
        val winnerOutcome = winnerDeferred.await()

        assertEquals(1, gcCalls.get(), "8 concurrent blocked requests must trigger at most 1 GC")
        assertEquals("BLOCKED", (winnerOutcome as TimelineService.PlayerPreparationOutcome.Ready).result.status)
        assertTrue(peerOutcomes.all { (it as TimelineService.PlayerPreparationOutcome.Ready).result.status == "BLOCKED" })
        assertTrue(summaries.any { it.contains("gcStatus=STILL_BLOCKED_AFTER_GC") })
        assertTrue(summaries.count { it.contains("gcStatus=SKIPPED_CONCURRENT_GC") } == 7)

        // 2. Within 30s cooldown (t = +29,999 ms): zero additional GC
        clock.advanceBy(29_999L)
        summaries.clear()
        val cooldownOutcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
        assertEquals("BLOCKED", (cooldownOutcome as TimelineService.PlayerPreparationOutcome.Ready).result.status)
        assertEquals(1, gcCalls.get(), "Request before 30s cooldown expiry must not trigger GC")
        assertTrue(summaries.single().contains("gcStatus=SKIPPED_COOLDOWN"))

        // 3. At 30s cooldown boundary (t = +30,000 ms): exactly 1 new attempt allowed
        clock.advanceBy(1L)
        summaries.clear()
        service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
        assertEquals(2, gcCalls.get(), "Request after 30s cooldown expiry may trigger single GC")
        assertTrue(summaries.single().contains("gcStatus=STILL_BLOCKED_AFTER_GC"))

        // 4. Midstream 360 MiB stop (healthy at pre-admission -> spikes to 362 MiB in profile decode): ZERO GC!
        val midstreamGcCalls = AtomicInteger(0)
        val midstreamSampleIndex = AtomicInteger(0)
        val midstreamClock = MutableClock(100_000L)
        val midstreamService = TimelineService(
            riotApiService = riotApi,
            redisCacheService = redisCache,
            timeProvider = midstreamClock,
            memoryStatsProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample {
                    val idx = midstreamSampleIndex.incrementAndGet()
                    // 1 = pre_admission, 2 = slot_admission, 3 = post_slot_admission (< 350 MiB); >= 4 = midstream (> 360 MiB)
                    val cgroup = if (idx <= 3) 351_272_960L else 379_584_512L // 335 MiB -> 362 MiB
                    return buildSample(
                        cgroupCurrentBytes = cgroup,
                        heapUsedBytes = 90L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L // 90 MiB unused committed, cooldown fresh
                    )
                }
            },
            preAdmissionGcEnabled = true
        )
        midstreamService.preAdmissionGcRunner = { midstreamGcCalls.incrementAndGet() }

        val midstreamOutcome = midstreamService.prepareCoachTimelinesForPlayer(
            region = "euw1",
            puuid = playerPuuid,
            requestedMatchIds = matchIds,
            profileLoader = {
                // Force midstream headroom check inside profileLoader
                if (!midstreamService.hasCoachContinueHeadroom()) {
                    throw RedisCacheService.BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
                }
                buildProfile()
            }
        )
        val midstreamReady = midstreamOutcome as TimelineService.PlayerPreparationOutcome.Ready
        assertEquals("BLOCKED", midstreamReady.result.status)
        assertEquals("MEMORY_HEADROOM_STOP", midstreamReady.result.stopReason)
        assertEquals(0, midstreamGcCalls.get(), "Midstream MEMORY_HEADROOM_STOP must NEVER trigger GC")
    }

    @DisplayName("negative, zero, or lower positive config overrides cannot weaken 24 MiB unused-committed or 30s cooldown floors")
    @Test
    fun configFloorOverridesCannotWeaken24MiBUnusedOr30sCooldownMinima() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()

        // 1. Unused-committed floor (24 MiB): 20 MiB actual unused MUST skip GC even with -1, 0, 1 KiB, or 16 MiB overrides
        for (overrideBytes in listOf(-1L, 0L, 1_024L, 16L * 1024L * 1024L)) {
            val clock = MutableClock(20_000L)
            val gcCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = clock,
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = buildSample(
                        cgroupCurrentBytes = 372_162_560L,
                        heapUsedBytes = 160L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L // 20 MiB unused (< 24 MiB floor)
                    )
                },
                preAdmissionGcEnabled = true,
                preAdmissionGcMinUnusedCommittedBytes = overrideBytes
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            val outcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
            assertEquals("BLOCKED", ready.result.status)
            assertEquals(0, gcCalls.get(), "Override $overrideBytes must not weaken 24 MiB floor")
            assertTrue(summaries.single().contains("gcStatus=SKIPPED_LOW_UNUSED_COMMITTED"))
            assertZeroPlayerPayloadLeakage(summaries.single())
        }

        // At exact 24 MiB unused committed, a lower override clamps to 24 MiB and permits single attempt;
        // a stricter override (48 MiB) still enforces 48 MiB.
        run {
            val gcCallsAt24MiB = AtomicInteger(0)
            val serviceAt24MiB = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(25_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = buildSample(
                        cgroupCurrentBytes = 372_162_560L,
                        heapUsedBytes = 156L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L // exact 24 MiB unused
                    )
                },
                preAdmissionGcEnabled = true,
                preAdmissionGcMinUnusedCommittedBytes = 0L
            )
            serviceAt24MiB.preAdmissionGcRunner = { gcCallsAt24MiB.incrementAndGet() }
            serviceAt24MiB.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(1, gcCallsAt24MiB.get(), "Exact 24 MiB unused should satisfy clamped 24 MiB floor")

            val gcCallsStricter = AtomicInteger(0)
            val stricterService = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(26_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = buildSample(
                        cgroupCurrentBytes = 372_162_560L,
                        heapUsedBytes = 148L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L // 32 MiB unused (< 48 MiB stricter override)
                    )
                },
                preAdmissionGcEnabled = true,
                preAdmissionGcMinUnusedCommittedBytes = 48L * 1024L * 1024L
            )
            stricterService.preAdmissionGcRunner = { gcCallsStricter.incrementAndGet() }
            stricterService.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(0, gcCallsStricter.get(), "Stricter >24 MiB override must remain respected")
        }

        // 2. Cooldown floor (30_000 ms): -5_000, 0, 1, and 5_000 ms overrides MUST still enforce 30_000 ms cooldown
        for (overrideCooldownMs in listOf(-5_000L, 0L, 1L, 5_000L)) {
            val clock = MutableClock(50_000L)
            val gcCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = clock,
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = buildSample(
                        cgroupCurrentBytes = 372_162_560L,
                        heapUsedBytes = 90L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L
                    )
                },
                preAdmissionGcEnabled = true,
                preAdmissionGcCooldownMs = overrideCooldownMs
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(1, gcCalls.get())

            // Immediate retry (+0 ms), +5,000 ms, and +29,999 ms must ALL be blocked by the 30s floor
            summaries.clear()
            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(1, gcCalls.get(), "Override $overrideCooldownMs must not allow immediate retry")
            assertTrue(summaries.single().contains("gcStatus=SKIPPED_COOLDOWN"))

            clock.advanceBy(5_000L)
            summaries.clear()
            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(1, gcCalls.get(), "Override $overrideCooldownMs must not allow +5s retry")
            assertTrue(summaries.single().contains("gcStatus=SKIPPED_COOLDOWN"))

            clock.advanceBy(24_999L) // total +29,999 ms
            summaries.clear()
            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(1, gcCalls.get(), "Override $overrideCooldownMs must not allow +29,999ms retry")
            assertTrue(summaries.single().contains("gcStatus=SKIPPED_COOLDOWN"))

            clock.advanceBy(1L) // total +30,000 ms
            summaries.clear()
            service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals(2, gcCalls.get(), "Clamped 30s cooldown permits single attempt at +30,000ms")
            assertTrue(summaries.single().contains("gcStatus=STILL_BLOCKED_AFTER_GC"))
        }
    }

    @DisplayName("invalid samples, sampler exceptions, and GC runner exceptions fail closed without unsafe GC or spin loops")
    @Test
    fun invalidSamplesSamplerExceptionsAndGcRunnerExceptionsFailClosedWithoutUnsafeGc() = runBlocking {
        val (riotApi, redisCache) = newMockedTimelineDeps()

        // 1. Invalid initial samples MUST fail closed BEFORE GC eligibility (zero GC calls),
        // including negative heapUsed with positive heapCommitted (must NOT coerce used to 0 and compute >24 MiB unused!)
        val invalidInitialSamples = listOf(
            "negative_heap_used_positive_committed" to RawMemorySample(
                jvmHeapUsedBytes = -1L,
                jvmHeapCommittedBytes = 180L * 1024L * 1024L,
                jvmHeapMaxBytes = 214_748_364L,
                cgroupMemoryCurrentBytes = 372_162_560L,
                cgroupMemoryMaxBytes = 536_870_912L
            ),
            "zero_heap_max" to RawMemorySample(
                jvmHeapUsedBytes = 0L,
                jvmHeapCommittedBytes = 180L * 1024L * 1024L,
                jvmHeapMaxBytes = 0L,
                cgroupMemoryCurrentBytes = 372_162_560L,
                cgroupMemoryMaxBytes = 536_870_912L
            ),
            "committed_below_used" to RawMemorySample(
                jvmHeapUsedBytes = 170L * 1024L * 1024L,
                jvmHeapCommittedBytes = 120L * 1024L * 1024L,
                jvmHeapMaxBytes = 214_748_364L,
                cgroupMemoryCurrentBytes = 372_162_560L,
                cgroupMemoryMaxBytes = 536_870_912L
            ),
            "missing_cgroup_current_in_bounded_container" to RawMemorySample(
                jvmHeapUsedBytes = 85L * 1024L * 1024L,
                jvmHeapCommittedBytes = 180L * 1024L * 1024L,
                jvmHeapMaxBytes = 214_748_364L,
                cgroupMemoryCurrentBytes = null,
                cgroupMemoryMaxBytes = 536_870_912L
            ),
            "zero_cgroup_current_in_bounded_container" to RawMemorySample(
                jvmHeapUsedBytes = 85L * 1024L * 1024L,
                jvmHeapCommittedBytes = 180L * 1024L * 1024L,
                jvmHeapMaxBytes = 214_748_364L,
                cgroupMemoryCurrentBytes = 0L,
                cgroupMemoryMaxBytes = 536_870_912L
            )
        )

        for ((caseLabel, badSample) in invalidInitialSamples) {
            val gcCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(80_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = badSample
                },
                preAdmissionGcEnabled = true
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            val outcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
            assertEquals("BLOCKED", ready.result.status, "Case $caseLabel must block")
            assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", ready.result.stopReason)
            assertEquals(0, gcCalls.get(), "Case $caseLabel must NEVER invoke GC on invalid sample")
            val line = summaries.single()
            assertZeroPlayerPayloadLeakage(line)
            assertTrue(line.contains("gcAttempted=false gcStatus=INVALID_INITIAL_SAMPLE"), "Unexpected summary for $caseLabel: $line")
            if (caseLabel == "negative_heap_used_positive_committed") {
                assertTrue(line.contains("heapUsed=-1,heapComm=${180L * 1024L * 1024L},heapUnusedComm=-1,"))
            }
        }

        // 2. Initial sample() throwing exception -> BLOCKED, INITIAL_SAMPLE_ERROR, 0 GC calls
        run {
            val gcCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(90_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample {
                        throw IllegalStateException("simulated cgroupfs read failure")
                    }
                },
                preAdmissionGcEnabled = true
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            val outcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            val ready = outcome as TimelineService.PlayerPreparationOutcome.Ready
            assertEquals("BLOCKED", ready.result.status)
            assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", ready.result.stopReason)
            assertEquals(0, gcCalls.get())
            val line = summaries.single()
            assertZeroPlayerPayloadLeakage(line)
            assertTrue(line.contains("gcAttempted=false gcStatus=INITIAL_SAMPLE_ERROR"))
            assertTrue(line.contains("memPre={unavailable}"))
        }

        // 3. preAdmissionGcRunner throwing exception -> BLOCKED, GC_RUNNER_ERROR, lock released + 30s cooldown armed
        run {
            val clock = MutableClock(100_000L)
            val gcCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = clock,
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = buildSample(
                        cgroupCurrentBytes = 372_162_560L,
                        heapUsedBytes = 90L * 1024L * 1024L,
                        heapCommittedBytes = 180L * 1024L * 1024L
                    )
                },
                preAdmissionGcEnabled = true
            )
            service.preAdmissionGcRunner = {
                gcCalls.incrementAndGet()
                clock.advanceBy(7L)
                throw RuntimeException("synthetic gc runner failure")
            }
            service.stageSummaryObserver = { summaries.add(it) }

            val firstOutcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            val firstReady = firstOutcome as TimelineService.PlayerPreparationOutcome.Ready
            assertEquals("BLOCKED", firstReady.result.status)
            assertEquals("MEMORY_HEADROOM_ADMISSION_BLOCKED", firstReady.result.stopReason)
            assertEquals(1, gcCalls.get())
            assertTrue(summaries.single().contains("gcAttempted=true gcStatus=GC_RUNNER_ERROR gcMs=7"))
            assertZeroPlayerPayloadLeakage(summaries.single())

            // Immediate follow-up within 30s must hit SKIPPED_COOLDOWN (not stuck in SKIPPED_CONCURRENT_GC and not retrying GC)
            summaries.clear()
            clock.advanceBy(1_000L)
            val secondOutcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals("BLOCKED", (secondOutcome as TimelineService.PlayerPreparationOutcome.Ready).result.status)
            assertEquals(1, gcCalls.get(), "Failed GC attempt must still arm 30s cooldown to prevent retry loops")
            assertTrue(summaries.single().contains("gcAttempted=false gcStatus=SKIPPED_COOLDOWN"))
        }

        // 4. Post-GC sample() exception and invalid post-GC sample both fail closed
        run {
            val gcCalls = AtomicInteger(0)
            val sampleCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(120_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample {
                        if (sampleCalls.incrementAndGet() == 1) {
                            return buildSample(
                                cgroupCurrentBytes = 372_162_560L,
                                heapUsedBytes = 90L * 1024L * 1024L,
                                heapCommittedBytes = 180L * 1024L * 1024L
                            )
                        }
                        throw IllegalStateException("post-gc sample read failed")
                    }
                },
                preAdmissionGcEnabled = true
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            val outcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals("BLOCKED", (outcome as TimelineService.PlayerPreparationOutcome.Ready).result.status)
            assertEquals(1, gcCalls.get())
            assertTrue(summaries.single().contains("gcAttempted=true gcStatus=POST_GC_SAMPLE_ERROR"))
            assertZeroPlayerPayloadLeakage(summaries.single())
        }

        run {
            val gcCalls = AtomicInteger(0)
            val sampleCalls = AtomicInteger(0)
            val summaries = Collections.synchronizedList(mutableListOf<String>())
            val service = TimelineService(
                riotApiService = riotApi,
                redisCacheService = redisCache,
                timeProvider = MutableClock(140_000L),
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample {
                        return if (sampleCalls.incrementAndGet() == 1) {
                            buildSample(
                                cgroupCurrentBytes = 372_162_560L,
                                heapUsedBytes = 90L * 1024L * 1024L,
                                heapCommittedBytes = 180L * 1024L * 1024L
                            )
                        } else {
                            RawMemorySample(
                                jvmHeapUsedBytes = -5L,
                                jvmHeapCommittedBytes = 120L * 1024L * 1024L,
                                jvmHeapMaxBytes = 214_748_364L,
                                cgroupMemoryCurrentBytes = 340_000_000L,
                                cgroupMemoryMaxBytes = 536_870_912L
                            )
                        }
                    }
                },
                preAdmissionGcEnabled = true
            )
            service.preAdmissionGcRunner = { gcCalls.incrementAndGet() }
            service.stageSummaryObserver = { summaries.add(it) }

            val outcome = service.prepareCoachTimelinesForPlayer("euw1", playerPuuid, matchIds) { buildProfile() }
            assertEquals("BLOCKED", (outcome as TimelineService.PlayerPreparationOutcome.Ready).result.status)
            assertEquals(1, gcCalls.get())
            assertTrue(summaries.single().contains("gcAttempted=true gcStatus=INVALID_POST_GC_SAMPLE"))
            assertZeroPlayerPayloadLeakage(summaries.single())
        }
    }
}

