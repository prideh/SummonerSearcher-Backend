package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.doubles.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class Workstream4QuestsAndCacheBehaviorTest : BehaviorSpec({
    val mapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    val timelineAnalysisService = TimelineAnalysisService()

    fun memProvider(sample: RawMemorySample): MemoryStatsProvider = object : MemoryStatsProvider {
        override fun sample(): RawMemorySample = sample
    }

    fun clockProvider(nowFn: () -> Long): TimeProvider = object : TimeProvider {
        override fun now(): Long = nowFn()
    }

    fun tlPFrame(
        participantId: Int,
        totalGold: Int,
        currentGold: Int = 500,
        level: Int = 1,
        xp: Int = 0,
        minionsKilled: Int = 0,
        jungleMinionsKilled: Int = 0,
        position: TimelinePositionDto = TimelinePositionDto(7500, 7500)
    ): ParticipantFrameDto = ParticipantFrameDto(
        participantId = participantId,
        totalGold = totalGold,
        level = level,
        xp = xp,
        currentGold = currentGold,
        minionsKilled = minionsKilled,
        jungleMinionsKilled = jungleMinionsKilled,
        position = position
    )

    fun tlEvent(
        type: String,
        timestamp: Long,
        participantId: Int? = null,
        creatorId: Int? = null,
        itemId: Int? = null,
        beforeId: Int? = null,
        afterId: Int? = null,
        skillSlot: Int? = null,
        levelUpType: String? = null,
        wardType: String? = null
    ): TimelineEventDto = TimelineEventDto(
        type = type,
        timestamp = timestamp,
        participantId = participantId,
        killerId = null,
        victimId = null,
        assistingParticipantIds = null,
        itemId = itemId,
        afterId = afterId,
        beforeId = beforeId,
        skillSlot = skillSlot,
        levelUpType = levelUpType,
        wardType = wardType,
        creatorId = creatorId,
        level = null,
        buildingType = null,
        laneType = null,
        towerType = null,
        teamId = null,
        killerTeamId = null,
        monsterType = null,
        monsterSubType = null,
        position = null,
        bounty = null,
        shutdownBounty = null,
        goldGain = null
    )

    fun buildTenParticipants(roleBoundItemBySlot: (Int) -> Int? = { null }): List<ParticipantDto> {
        val roles = listOf("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY")
        return (1..10).map { pid ->
            val teamId = if (pid <= 5) 100 else 200
            val role = roles[(pid - 1) % 5]
            ParticipantDto(
                puuid = "puuid-$pid",
                participantId = pid,
                championId = 100 + pid,
                championName = "Champ$pid",
                teamId = teamId,
                teamPosition = role,
                individualPosition = role,
                win = teamId == 100,
                kills = 5,
                deaths = 2,
                assists = 7,
                totalMinionsKilled = 180,
                neutralMinionsKilled = 12,
                roleBoundItem = roleBoundItemBySlot(pid),
                gameEndedInEarlySurrender = false
            )
        }
    }

    fun buildMatchDto(
        matchId: String,
        gameVersion: String = "16.22.1.1234",
        queueId: Int = 420,
        gameDuration: Long = 1800L,
        dtoSchemaVersion: Int? = null,
        roleBoundItemBySlot: (Int) -> Int? = { null }
    ): MatchDto = MatchDto(
        metadata = MatchMetadata(matchId = matchId, participants = (1..10).map { "puuid-$it" }),
        info = MatchInfo(
            gameCreation = 1_762_800_000_000L,
            gameDuration = gameDuration,
            gameId = matchId.filter { it.isDigit() }.toLongOrNull() ?: 1L,
            gameMode = "CLASSIC",
            gameVersion = gameVersion,
            queueId = queueId,
            participants = buildTenParticipants(roleBoundItemBySlot),
            teams = listOf(
                TeamDto(teamId = 100, win = true, objectives = TeamObjectivesDto(tower = ObjectiveStatsDto(first = true, kills = 8))),
                TeamDto(teamId = 200, win = false, objectives = TeamObjectivesDto(tower = ObjectiveStatsDto(first = false, kills = 3)))
            )
        ),
        dtoSchemaVersion = dtoSchemaVersion
    )

    Given("Season 2026 Match-V5 JSON payloads with roleBoundItem, float ChallengesDto fields, and IGNB flags (#14)") {
        When("Jackson deserializes legacy, modern, zero, unknown-item, float-challenge, and IGNB payloads") {
            Then("legacy missing roleBoundItem stays null (never 0), positive unseen IDs survive, floats parse without truncation, and IGNB stays capture-only") {
                val json = """
                    {
                      "metadata": { "matchId": "EUW1_8000000001", "participants": ["p-legacy", "p-bot", "p-zero", "p-unknown", "p-ignb"] },
                      "info": {
                        "gameCreation": 1762800000000,
                        "gameDuration": 1820,
                        "gameId": 8000000001,
                        "gameMode": "CLASSIC",
                        "gameVersion": "16.22.1.9000",
                        "queueId": 420,
                        "participants": [
                          {
                            "puuid": "p-legacy",
                            "participantId": 1,
                            "championName": "Ornn",
                            "teamPosition": "TOP",
                            "teamId": 100,
                            "win": true,
                            "kills": 4, "deaths": 2, "assists": 9
                          },
                          {
                            "puuid": "p-bot",
                            "participantId": 4,
                            "championName": "Jinx",
                            "teamPosition": "BOTTOM",
                            "teamId": 100,
                            "win": true,
                            "kills": 11, "deaths": 3, "assists": 6,
                            "roleBoundItem": 3006,
                            "eligibleForProgression": true,
                            "totalAllyJungleMinionsKilled": 12,
                            "totalEnemyJungleMinionsKilled": 4,
                            "damageDealtToEpicMonsters": 18450,
                            "challenges": {
                              "earliestBaron": 1345.67,
                              "earliestDragonTakedown": 372.45,
                              "earlyLaningPhaseGoldExpAdvantage": 0.0,
                              "laningPhaseGoldExpAdvantage": 1.0,
                              "bountyGold": 450.75
                            }
                          },
                          {
                            "puuid": "p-zero",
                            "participantId": 5,
                            "championName": "Lulu",
                            "teamPosition": "UTILITY",
                            "teamId": 100,
                            "win": true,
                            "roleBoundItem": 0
                          },
                          {
                            "puuid": "p-unknown",
                            "participantId": 6,
                            "championName": "Aatrox",
                            "teamPosition": "TOP",
                            "teamId": 200,
                            "win": false,
                            "roleBoundItem": 9999
                          },
                          {
                            "puuid": "p-ignb",
                            "participantId": 9,
                            "championName": "Ezreal",
                            "teamPosition": "BOTTOM",
                            "teamId": 200,
                            "win": false,
                            "kills": 1, "deaths": 5, "assists": 2,
                            "roleBoundItem": 1202,
                            "gameEndedInIGNBSurrender": true,
                            "teamIGNBSurrendered": true,
                            "causedGameEndFromIGNBSurrender": false,
                            "wasSevereTransgressor": false
                          }
                        ]
                      }
                    }
                """.trimIndent()

                val parsed = mapper.readValue(json, MatchDto::class.java)
                val parts = parsed.info!!.participants

                // Legacy missing roleBoundItem MUST stay null, never coerced to 0
                parts[0].roleBoundItem.shouldBeNull()

                // Bot Carry 7th-slot boots + widened ChallengesDto floats
                parts[1].roleBoundItem shouldBe 3006
                parts[1].eligibleForProgression shouldBe true
                parts[1].totalAllyJungleMinionsKilled shouldBe 12
                parts[1].totalEnemyJungleMinionsKilled shouldBe 4
                parts[1].damageDealtToEpicMonsters shouldBe 18450
                parts[1].challenges!!.earliestBaron!! shouldBeGreaterThan 1345.0
                parts[1].challenges!!.earliestDragonTakedown!! shouldBeGreaterThan 372.0
                parts[1].challenges!!.bountyGold shouldBe 450.75

                // Explicit 0 stays 0 (distinct from legacy null); unknown positive ID stays 9999
                parts[2].roleBoundItem shouldBe 0
                parts[3].roleBoundItem shouldBe 9999

                // IGNB is nullable capture ONLY — must NOT trigger remake/winrate exclusion in MatchStatsAccumulator
                parts[4].gameEndedInIGNBSurrender shouldBe true
                parts[4].teamIGNBSurrendered shouldBe true
                val acc = MatchStatsAccumulator("p-ignb")
                acc.addMatchIfEligible(parsed) shouldBe true
                val (_, overall) = acc.getResult()
                (overall.wins + overall.losses) shouldBe 1
                overall.winRate shouldBe 0.0
            }
        }
    }

    Given("PlayerCacheService bounded legacy MatchDto Role Quest backfill (#14)") {
        val healthyMemory = RawMemorySample(
            jvmHeapUsedBytes = 120L * 1024L * 1024L,
            jvmHeapCommittedBytes = 256L * 1024L * 1024L,
            jvmHeapMaxBytes = 512L * 1024L * 1024L,
            cgroupMemoryCurrentBytes = 220L * 1024L * 1024L,
            cgroupMemoryMaxBytes = 512L * 1024L * 1024L
        )

        When("legacyRoleQuestBackfillEnabled is false (default)") {
            Then("existing cached MatchDto entries are NOT mutated and zero Riot API calls occur") {
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                val service = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory)
                )

                val legacyMatches = (1..5).map { buildMatchDto("EUW1_$it") }
                val (result, updated) = service.refreshLegacyRoleQuestMatchesBounded(legacyMatches, "euw1")

                updated shouldBe 0
                result shouldBe legacyMatches
                coVerify(exactly = 0) { riot.getMatchByIdLowPriority(any(), any(), any(), any(), any()) }
                verify(exactly = 0) { redis.set(any(), any<MatchDto>(), any()) }
            }
        }

        When("legacyRoleQuestBackfillEnabled is true with global bounds, singleflight, failed-attempt cooldown, and TTL preservation") {
            Then("enforces per-call cap (3), global cap (10/min), singleflight deduplication, 30m failure cooldown, remaining TTL preservation, and safe profile sync") {
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                var nowMs = 1_762_800_000_000L

                every { redis.get(any(), MatchDto::class.java) } returns null
                every { redis.getRemainingTtl("match:euw1:EUW1_1") } returns Duration.ofDays(19)
                every { redis.getRemainingTtl("match:euw1:EUW1_2") } returns null
                every { redis.getRemainingTtl("match:euw1:EUW1_3") } returns null
                every { riot.getActiveRetryAfterWaitMs(any()) } returns 0L

                val service = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    timeProvider = clockProvider { nowMs },
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )

                // Match 1 succeeds (preserves 19d TTL); Match 2 fails (null -> original retained, 30m cooldown); Match 3 succeeds; Match 4+5 skipped by per-call cap (3)
                coEvery { riot.getMatchByIdLowPriority("EUW1_1", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_1", dtoSchemaVersion = 2) { 3006 }
                coEvery { riot.getMatchByIdLowPriority("EUW1_2", "euw1", 0, any(), any()) } returns null
                coEvery { riot.getMatchByIdLowPriority("EUW1_3", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_3", dtoSchemaVersion = 2) { 1206 }

                val batch = (1..5).map { buildMatchDto("EUW1_$it") }
                val (refreshed, updatedCount) = service.refreshLegacyRoleQuestMatchesBounded(batch, "euw1")

                updatedCount shouldBe 2
                refreshed[0].info!!.participants[0].roleBoundItem shouldBe 3006
                refreshed[1].info!!.participants[0].roleBoundItem.shouldBeNull() // Failure retains original
                refreshed[2].info!!.participants[0].roleBoundItem shouldBe 1206

                // Match 1 preserved 19d remaining TTL (NOT reset to 60d); failed Match 2 NEVER wrote to Redis
                verify(exactly = 1) { redis.set("match:euw1:EUW1_1", any<MatchDto>(), Duration.ofDays(19)) }
                verify(exactly = 0) { redis.set("match:euw1:EUW1_2", any<MatchDto>(), any()) }
                verify(exactly = 1) { redis.set("match:euw1:EUW1_3", any<MatchDto>(), Duration.ofDays(60)) }

                val telemetryAfterFirst = service.getDiscoveryAndSeasonWriteTelemetry()
                telemetryAfterFirst["legacyRoleQuestBackfillAttempts"] shouldBe 3L
                telemetryAfterFirst["legacyRoleQuestBackfillUpdated"] shouldBe 2L
                telemetryAfterFirst["legacyRoleQuestBackfillFailed"] shouldBe 1L
                telemetryAfterFirst["legacyRoleQuestBackfillSkippedBudget"] shouldBe 2L

                // Repeated call within 30m: EUW1_2 is in failed-attempt cooldown -> 0 retry calls on EUW1_2!
                coEvery { riot.getMatchByIdLowPriority("EUW1_4", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_4", dtoSchemaVersion = 2) { 1220 }
                val (_, secondUpdated) = service.refreshLegacyRoleQuestMatchesBounded(
                    listOf(refreshed[1], refreshed[3]), // EUW1_2 (failed earlier) + EUW1_4
                    "euw1"
                )
                secondUpdated shouldBe 1
                coVerify(exactly = 1) { riot.getMatchByIdLowPriority("EUW1_2", "euw1", 0, any(), any()) }
                service.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSkippedMatchCooldown"] shouldBe 1L

                // Singleflight test: two concurrent callers requesting EUW1_99 make strictly 1 Riot API call
                val inflightCalls = AtomicInteger(0)
                coEvery { riot.getMatchByIdLowPriority("EUW1_99", "euw1", 0, any(), any()) } coAnswers {
                    inflightCalls.incrementAndGet()
                    delay(40)
                    buildMatchDto("EUW1_99", dtoSchemaVersion = 2) { 1208 }
                }
                coroutineScope {
                    val c1 = async { service.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_99")), "euw1") }
                    val c2 = async { service.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_99")), "euw1") }
                    c1.await().second shouldBe 1
                    c2.await().second shouldBe 1
                }
                inflightCalls.get() shouldBe 1
                service.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSingleflightJoins"] shouldBe 1L

                // Global window cap (10/min): we already consumed 5 attempts above; 5 more reach 10, 11th skips globally
                for (k in 10..20) {
                    coEvery { riot.getMatchByIdLowPriority("EUW1_G$k", "euw1", 0, any(), any()) } returns
                        buildMatchDto("EUW1_G$k", dtoSchemaVersion = 2) { 1206 }
                }
                service.refreshLegacyRoleQuestMatchesBounded((10..12).map { buildMatchDto("EUW1_G$it") }, "euw1")
                service.refreshLegacyRoleQuestMatchesBounded((13..15).map { buildMatchDto("EUW1_G$it") }, "euw1")
                val teleGlobal = service.getDiscoveryAndSeasonWriteTelemetry()
                teleGlobal["legacyRoleQuestBackfillAttempts"] shouldBe 10L
                teleGlobal["legacyRoleQuestBackfillSkippedGlobalBudget"] shouldBe 1L
            }

            Then("aborts on low memory headroom, active Retry-After cooldown, and propagates CancellationException without writing to Redis") {
                val lowMemory = RawMemorySample(
                    jvmHeapUsedBytes = 430L * 1024L * 1024L,
                    jvmHeapCommittedBytes = 480L * 1024L * 1024L,
                    jvmHeapMaxBytes = 512L * 1024L * 1024L,
                    cgroupMemoryCurrentBytes = 440L * 1024L * 1024L,
                    cgroupMemoryMaxBytes = 512L * 1024L * 1024L
                )
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                every { redis.get(any(), MatchDto::class.java) } returns null

                val lowMemService = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(lowMemory),
                    legacyRoleQuestBackfillEnabled = true
                )
                lowMemService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_MEM")), "euw1").second shouldBe 0
                lowMemService.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSkippedHeadroom"] shouldBe 1L

                // CancellationException must propagate without writing to Redis
                val cancelRiot = mockk<RiotApiService>(relaxed = true)
                every { cancelRiot.getActiveRetryAfterWaitMs(any()) } returns 0L
                coEvery { cancelRiot.getMatchByIdLowPriority("EUW1_CANCEL", "euw1", 0, any(), any()) } throws CancellationException("cancelled")
                val cancelService = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = cancelRiot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )
                shouldThrow<CancellationException> {
                    cancelService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_CANCEL")), "euw1")
                }
                verify(exactly = 0) { redis.set("match:euw1:EUW1_CANCEL", any<MatchDto>(), any()) }

                // Profile embedded recentMatches sync preserves newer lastRefreshed & remaining profile TTL
                every { redis.setIfAbsent("lock:profile:euw1:puuid-1", any(), any()) } returns true
                every { redis.getRemainingTtl("player:profile:euw1:puuid-1") } returns Duration.ofDays(9)
                val newerRefreshedAt = Instant.parse("2026-11-11T17:00:00Z")
                val existingProfile = SummonerProfileDto(
                    puuid = "puuid-1",
                    gameName = "Tester",
                    tagLine = "EUW",
                    summonerLevel = 250,
                    profileIconId = 10,
                    soloQueueRank = null,
                    recentMatches = listOf(buildMatchDto("EUW1_SYNC")),
                    allMatchIds = listOf("EUW1_SYNC"),
                    championStats = emptyList(),
                    overallStats = null,
                    improvementStats = null,
                    totalMatches = 1,
                    lastRefreshed = newerRefreshedAt
                )
                every { redis.get("player:profile:euw1:puuid-1", SummonerProfileDto::class.java) } returns existingProfile
                every { redis.multiGet(listOf("match:euw1:EUW1_SYNC"), MatchDto::class.java) } returns
                    mapOf("match:euw1:EUW1_SYNC" to buildMatchDto("EUW1_SYNC"))

                val syncRiot = mockk<RiotApiService>(relaxed = true)
                every { syncRiot.getActiveRetryAfterWaitMs(any()) } returns 0L
                coEvery { syncRiot.getMatchByIdLowPriority("EUW1_SYNC", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_SYNC", dtoSchemaVersion = 2) { 3006 }
                val syncService = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = syncRiot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )
                val pageResult = syncService.getMatchesByIds(listOf("EUW1_SYNC"), "euw1", ownerPuuid = "puuid-1")
                pageResult.first().info!!.participants.first().roleBoundItem shouldBe 3006

                val savedProfileSlot = slot<SummonerProfileDto>()
                verify { redis.set("player:profile:euw1:puuid-1", capture(savedProfileSlot), Duration.ofDays(9)) }
                savedProfileSlot.captured.lastRefreshed shouldBe newerRefreshedAt
                savedProfileSlot.captured.recentMatches!!.first().info!!.participants.first().roleBoundItem shouldBe 3006
            }
        }
    }

    Given("TimelineAnalysisService Season 2026 accuracy rules (#14)") {
        When("analyzing UNDEFINED wards, non-uniform timestamps, SKILL_LEVEL_UP EVOLVE/#1100 bursts, ITEM_UNDO (#1174), and Role Quest completion timings") {
            Then("excludes UNDEFINED wards, selects 10m/15m by timestamp, filters NORMAL+dedup without 18 cap, reconciles ITEM_UNDO safely, and keeps ambiguous quest timings/aggregates null") {
                val matchComplete = buildMatchDto("EUW1_Q1") { pid ->
                    when (pid) {
                        3 -> 1206 // Player Mid completed quest
                        8 -> 1206 // Opponent Mid completed quest
                        else -> 1208
                    }
                }
                // Insert an extra pause/sub-minute frame at index 2 so 10:00 (600_000ms) is at index 11 and 15:00 (900_000ms) is at index 16!
                val frames = mutableListOf<TimelineFrameDto>()
                for (m in 0..16) {
                    val ts = m * 60_000L
                    frames.add(
                        TimelineFrameDto(
                            timestamp = ts,
                            participantFrames = mapOf(
                                "3" to tlPFrame(
                                    participantId = 3,
                                    totalGold = if (m == 10) 3800 else if (m == 15) 5900 else 500 + m * 300,
                                    currentGold = 600,
                                    level = (1 + m).coerceAtMost(18),
                                    xp = m * 400,
                                    minionsKilled = m * 8,
                                    jungleMinionsKilled = 0,
                                    position = TimelinePositionDto(7500, 7500)
                                ),
                                "8" to tlPFrame(
                                    participantId = 8,
                                    totalGold = if (m == 10) 3300 else if (m == 15) 5100 else 500 + m * 280,
                                    currentGold = 500,
                                    level = (1 + m).coerceAtMost(18),
                                    xp = m * 380,
                                    minionsKilled = m * 7,
                                    jungleMinionsKilled = 0,
                                    position = TimelinePositionDto(7600, 7600)
                                )
                            ),
                            events = if (m == 5) {
                                listOf(
                                    // Valid ward vs UNDEFINED ward (must be excluded!)
                                    tlEvent(type = "WARD_PLACED", timestamp = 305_000L, creatorId = 3, wardType = "YELLOW_TRINKET"),
                                    tlEvent(type = "WARD_PLACED", timestamp = 310_000L, creatorId = 3, wardType = "UNDEFINED"),
                                    // Buy 1056 -> Buy 1082 -> Undo 1082 (beforeId=1082) -> #1174 Boot undo (beforeId=0, must NOT pop 1056!)
                                    tlEvent(type = "ITEM_PURCHASED", timestamp = 301_000L, participantId = 3, itemId = 1056),
                                    tlEvent(type = "ITEM_PURCHASED", timestamp = 302_000L, participantId = 3, itemId = 1082),
                                    tlEvent(type = "ITEM_UNDO", timestamp = 303_000L, participantId = 3, beforeId = 1082, afterId = 0),
                                    tlEvent(type = "ITEM_UNDO", timestamp = 304_000L, participantId = 3, beforeId = 0, afterId = 0),
                                    // SKILL_LEVEL_UP: 19 valid NORMAL skill-ups (e.g. Udyr/Lv20 exception preserved!), 1 EVOLVE excluded, and 1 duplicate (#1100) deduplicated
                                    tlEvent(type = "SKILL_LEVEL_UP", timestamp = 315_000L, participantId = 3, skillSlot = 1, levelUpType = "EVOLVE"),
                                    tlEvent(type = "SKILL_LEVEL_UP", timestamp = 320_000L, participantId = 3, skillSlot = 1, levelUpType = "NORMAL"),
                                    tlEvent(type = "SKILL_LEVEL_UP", timestamp = 320_000L, participantId = 3, skillSlot = 1, levelUpType = "NORMAL")
                                ) + (1..18).map { idx ->
                                    tlEvent(
                                        type = "SKILL_LEVEL_UP",
                                        timestamp = 321_000L + idx * 1000L,
                                        participantId = 3,
                                        skillSlot = (idx % 4) + 1,
                                        levelUpType = "NORMAL"
                                    )
                                }
                            } else if (m == 13) {
                                listOf(
                                    tlEvent(type = "ITEM_DESTROYED", timestamp = 780_000L, participantId = 3, itemId = 1201), // 13.0m
                                    tlEvent(type = "ITEM_DESTROYED", timestamp = 810_000L, participantId = 8, itemId = 1201)  // 13.5m
                                )
                            } else emptyList()
                        )
                    )
                    if (m == 1) {
                        // Extra non-minute frame shifting all subsequent frame indices by +1!
                        frames.add(
                            TimelineFrameDto(
                                timestamp = 95_000L,
                                participantFrames = mapOf(
                                    "3" to tlPFrame(participantId = 3, totalGold = 99999, position = TimelinePositionDto(7500, 7500)),
                                    "8" to tlPFrame(participantId = 8, totalGold = 1000, position = TimelinePositionDto(7600, 7600))
                                ),
                                events = emptyList()
                            )
                        )
                    }
                }

                val tlComplete = MatchTimelineDto(
                    metadata = MatchTimelineMetadata(matchId = "EUW1_Q1", participants = null),
                    info = MatchTimelineInfo(
                        frameInterval = 60_000L,
                        frames = frames,
                        participants = listOf(TimelineParticipantDto(3, "puuid-3"), TimelineParticipantDto(8, "puuid-8"))
                    )
                )

                val compacted = timelineAnalysisService.compactTimelineForPlayer(tlComplete, matchComplete, "puuid-3")!!
                val analysis = timelineAnalysisService.analyze(
                    listOf(Triple("EUW1_Q1", compacted, matchComplete)),
                    "puuid-3"
                )

                // 1. UNDEFINED ward excluded from total, buckets, heatmap, and eventTimelines
                analysis.aggregateSummary.wardsPlacedTotal shouldBe 1
                analysis.aggregateSummary.wardsByTimeBucket shouldBe WardTimeBucketsDto(pre10 = 1, min10To20 = 0, post20 = 0)
                analysis.wardPositions.size shouldBe 1
                analysis.eventTimelines.first().events.count { it.type == "WARD_PLACED" } shouldBe 1

                // 2. Timestamp-based 10m/15m lookup picked 600_000ms (+500g) & 900_000ms (+800g) despite shifted frame indices
                analysis.aggregateSummary.avgGoldLeadAt10 shouldBe 500.0
                analysis.aggregateSummary.avgGoldLeadAt15 shouldBe 800.0

                // 3. ITEM_UNDO popped 1082 while #1174 (beforeId=0) left 1056 intact
                analysis.buildOrders.first().items.map { it.itemId } shouldBe listOf(1056)

                // 4. SKILL_LEVEL_UP kept 19 valid NORMAL skill-ups (not truncated at 18!), excluded EVOLVE, and deduplicated #1100 burst
                analysis.skillOrders.first().levelUps.size shouldBe 19

                // 5. Unambiguous Role Quest timing: 13.0m vs 13.5m (-30.0s delta)
                val qt = analysis.roleQuestTimings.first()
                qt.playerQuestCompleted shouldBe true
                qt.playerCompletionMinute shouldBe 13.0
                qt.opponentCompletionMinute shouldBe 13.5
                qt.completionDeltaSeconds shouldBe -30.0
                analysis.aggregateSummary.avgRoleQuestCompletionMinute shouldBe 13.0
                analysis.aggregateSummary.avgRoleQuestDeltaSeconds shouldBe -30.0

                // 6. Ambiguous / legacy / 0 / unknown cases MUST return null timing AND block whole-sample aggregate!
                val legacyUnknownMatch = buildMatchDto("EUW1_Q2") { null } // legacy cached match with null roleBoundItem
                val mixedAnalysis = timelineAnalysisService.analyze(
                    listOf(
                        Triple("EUW1_Q1", compacted, matchComplete),
                        Triple("EUW1_Q2", compacted, legacyUnknownMatch)
                    ),
                    "puuid-3"
                )
                mixedAnalysis.roleQuestTimings[1].playerQuestCompleted.shouldBeNull()
                mixedAnalysis.roleQuestTimings[1].playerCompletionMinute.shouldBeNull()
                mixedAnalysis.aggregateSummary.roleQuestAmbiguousGames shouldBe 1
                mixedAnalysis.aggregateSummary.avgRoleQuestCompletionMinute.shouldBeNull()
                mixedAnalysis.aggregateSummary.avgRoleQuestDeltaSeconds.shouldBeNull()
            }
        }
    }
})
