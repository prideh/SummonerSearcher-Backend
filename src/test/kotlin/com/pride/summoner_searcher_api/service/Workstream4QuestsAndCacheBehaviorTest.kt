package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.doubles.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.data.redis.core.script.RedisScript
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class Workstream4QuestsAndCacheBehaviorTest : BehaviorSpec({
    val mapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    val timelineAnalysisService = TimelineAnalysisService()

    fun memProvider(sampleFn: () -> RawMemorySample): MemoryStatsProvider = object : MemoryStatsProvider {
        override fun sample(): RawMemorySample = sampleFn()
    }

    fun memProvider(sample: RawMemorySample): MemoryStatsProvider = memProvider { sample }

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
        killerId: Int? = null,
        victimId: Int? = null,
        assistingParticipantIds: List<Int>? = null,
        itemId: Int? = null,
        beforeId: Int? = null,
        afterId: Int? = null,
        skillSlot: Int? = null,
        levelUpType: String? = null,
        wardType: String? = null,
        buildingType: String? = null,
        laneType: String? = null,
        monsterType: String? = null,
        monsterSubType: String? = null,
        position: TimelinePositionDto? = null,
        killerTeamId: Int? = null,
        bounty: Int? = null,
        shutdownBounty: Int? = null
    ): TimelineEventDto = TimelineEventDto(
        type = type,
        timestamp = timestamp,
        participantId = participantId,
        killerId = killerId,
        victimId = victimId,
        assistingParticipantIds = assistingParticipantIds,
        itemId = itemId,
        afterId = afterId,
        beforeId = beforeId,
        skillSlot = skillSlot,
        levelUpType = levelUpType,
        wardType = wardType,
        creatorId = creatorId,
        level = null,
        buildingType = buildingType,
        laneType = laneType,
        towerType = null,
        teamId = null,
        killerTeamId = killerTeamId,
        monsterType = monsterType,
        monsterSubType = monsterSubType,
        position = position,
        bounty = bounty,
        shutdownBounty = shutdownBounty,
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

    Given("RedisCacheService atomic compareAndSetPreservingTtl and getVersioned (#14)") {
        When("executing optimistic CAS with PTTL preservation against StringRedisTemplate") {
            Then("returns true ONLY on Lua 1L and fails closed on expired (0L), concurrent writer (-1L), persistent/zero PTTL (-2L), or exception") {
                val template = mockk<StringRedisTemplate>()
                val valueOps = mockk<ValueOperations<String, String>>()
                every { template.opsForValue() } returns valueOps
                val cache = RedisCacheService(template, mapper)

                val legacyMatch = buildMatchDto("EUW1_CAS_UNIT")
                val rawStored = mapper.writeValueAsString(legacyMatch)
                every { valueOps.get("match:euw1:EUW1_CAS_UNIT") } returns rawStored

                val versioned = cache.getVersioned("match:euw1:EUW1_CAS_UNIT", MatchDto::class.java)
                versioned.shouldNotBeNull()
                versioned.rawPayload shouldBe rawStored
                versioned.value.metadata?.matchId shouldBe "EUW1_CAS_UNIT"

                val upgraded = buildMatchDto("EUW1_CAS_UNIT", dtoSchemaVersion = 2) { 3006 }

                // 1L -> atomic CAS + PX pttl succeeded
                every {
                    template.execute(any<RedisScript<Long>>(), listOf("match:euw1:EUW1_CAS_UNIT"), rawStored, any<String>())
                } returns 1L
                cache.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UNIT", rawStored, upgraded) shouldBe true

                // 0L -> key expired before CAS; must NOT resurrect
                every {
                    template.execute(any<RedisScript<Long>>(), listOf("match:euw1:EUW1_CAS_UNIT"), rawStored, any<String>())
                } returns 0L
                cache.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UNIT", rawStored, upgraded) shouldBe false

                // -1L -> concurrent writer modified payload; must NOT overwrite
                every {
                    template.execute(any<RedisScript<Long>>(), listOf("match:euw1:EUW1_CAS_UNIT"), rawStored, any<String>())
                } returns -1L
                cache.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UNIT", rawStored, upgraded) shouldBe false

                // -2L -> persistent (-1) or non-positive PTTL; must fail closed rather than resetting TTL
                every {
                    template.execute(any<RedisScript<Long>>(), listOf("match:euw1:EUW1_CAS_UNIT"), rawStored, any<String>())
                } returns -2L
                cache.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UNIT", rawStored, upgraded) shouldBe false

                // Redis exception -> fails closed
                every {
                    template.execute(any<RedisScript<Long>>(), listOf("match:euw1:EUW1_CAS_UNIT"), rawStored, any<String>())
                } throws RuntimeException("redis unavailable")
                cache.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UNIT", rawStored, upgraded) shouldBe false

                // Never falls back to non-atomic ValueOperations.set
                verify(exactly = 0) { valueOps.set(any(), any()) }
                verify(exactly = 0) { valueOps.set(any(), any(), any<Duration>()) }
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
        val lowMemory = RawMemorySample(
            jvmHeapUsedBytes = 430L * 1024L * 1024L,
            jvmHeapCommittedBytes = 480L * 1024L * 1024L,
            jvmHeapMaxBytes = 512L * 1024L * 1024L,
            cgroupMemoryCurrentBytes = 440L * 1024L * 1024L,
            cgroupMemoryMaxBytes = 512L * 1024L * 1024L
        )

        fun stubExistingLegacyMatchInRedis(
            redis: RedisCacheService,
            matchId: String,
            ttl: Duration? = Duration.ofDays(19),
            casResult: Boolean = true
        ) {
            val key = "match:euw1:$matchId"
            val legacy = buildMatchDto(matchId)
            val raw = "raw:$matchId:v1"
            every { redis.getVersioned(key, MatchDto::class.java) } returns RedisCacheService.VersionedCacheEntry(legacy, raw)
            every { redis.getRemainingTtl(key) } returns ttl
            every { redis.compareAndSetPreservingTtl(key, raw, any<MatchDto>()) } returns casResult
        }

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
                verify(exactly = 0) { redis.compareAndSetPreservingTtl(any(), any(), any<MatchDto>()) }
            }
        }

        When("legacyRoleQuestBackfillEnabled is true with global bounds, singleflight, failed-attempt cooldown, and atomic CAS TTL preservation") {
            Then("enforces per-call cap (3), global cap (10/min), singleflight deduplication, 30m failure cooldown, and zero non-atomic set calls") {
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                val nowMs = 1_762_800_000_000L

                for (i in 1..5) stubExistingLegacyMatchInRedis(redis, "EUW1_$i")
                stubExistingLegacyMatchInRedis(redis, "EUW1_99")
                for (k in 10..20) stubExistingLegacyMatchInRedis(redis, "EUW1_G$k")
                every { riot.getActiveRetryAfterWaitMs(any()) } returns 0L

                val service = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    timeProvider = clockProvider { nowMs },
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )

                // Match 1 succeeds (atomic CAS); Match 2 fails (null -> original retained, 30m cooldown); Match 3 succeeds; Match 4+5 skipped by per-call cap (3)
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

                // Written strictly via atomic compareAndSetPreservingTtl; NEVER via non-atomic set(..., 60d)!
                verify(exactly = 1) { redis.compareAndSetPreservingTtl("match:euw1:EUW1_1", "raw:EUW1_1:v1", any<MatchDto>()) }
                verify(exactly = 0) { redis.compareAndSetPreservingTtl("match:euw1:EUW1_2", any(), any<MatchDto>()) }
                verify(exactly = 1) { redis.compareAndSetPreservingTtl("match:euw1:EUW1_3", "raw:EUW1_3:v1", any<MatchDto>()) }
                verify(exactly = 0) { redis.set(any(), any<MatchDto>(), any()) }

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

            Then("fails closed when match or profile TTL is missing/persistent/error or when concurrent writer wins CAS (never resets 60d/14d TTL)") {
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                every { riot.getActiveRetryAfterWaitMs(any()) } returns 0L

                // 1. Missing/persistent/error TTL on match key -> fails closed BEFORE outbound Riot call, 0 Redis writes
                stubExistingLegacyMatchInRedis(redis, "EUW1_NO_TTL", ttl = null, casResult = false)
                // 2. Missing entry in Redis (expired) -> fails closed BEFORE outbound Riot call, 0 resurrection writes
                every { redis.getVersioned("match:euw1:EUW1_EXPIRED", MatchDto::class.java) } returns null
                every { redis.getRemainingTtl("match:euw1:EUW1_EXPIRED") } returns null
                // 3. Concurrent writer / expiry during outbound fetch (CAS returns false, key now gone) -> fails closed, 0 set calls
                val rawRace = "raw:EUW1_CAS_LOST:v1"
                val legacyRace = buildMatchDto("EUW1_CAS_LOST")
                var casLostFetchDone = false
                every { redis.getVersioned("match:euw1:EUW1_CAS_LOST", MatchDto::class.java) } answers {
                    if (!casLostFetchDone) RedisCacheService.VersionedCacheEntry(legacyRace, rawRace) else null
                }
                every { redis.getRemainingTtl("match:euw1:EUW1_CAS_LOST") } returns Duration.ofDays(12)
                every { redis.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_LOST", rawRace, any<MatchDto>()) } returns false
                coEvery { riot.getMatchByIdLowPriority("EUW1_CAS_LOST", "euw1", 0, any(), any()) } answers {
                    casLostFetchDone = true
                    buildMatchDto("EUW1_CAS_LOST", dtoSchemaVersion = 2) { 3006 }
                }

                // 4. Concurrent writer already upgraded match during outbound fetch -> adopts concurrent writer's upgraded entry without overwriting
                val rawUpgradedRace = "raw:EUW1_CAS_UPGRADED:v1"
                val legacyUpgradedRace = buildMatchDto("EUW1_CAS_UPGRADED")
                val concurrentUpgraded = buildMatchDto("EUW1_CAS_UPGRADED", dtoSchemaVersion = 2) { 1206 }
                var upgradedFetchDone = false
                every { redis.getVersioned("match:euw1:EUW1_CAS_UPGRADED", MatchDto::class.java) } answers {
                    if (!upgradedFetchDone) {
                        RedisCacheService.VersionedCacheEntry(legacyUpgradedRace, rawUpgradedRace)
                    } else {
                        RedisCacheService.VersionedCacheEntry(concurrentUpgraded, "raw:EUW1_CAS_UPGRADED:v2")
                    }
                }
                every { redis.getRemainingTtl("match:euw1:EUW1_CAS_UPGRADED") } returns Duration.ofDays(15)
                every { redis.compareAndSetPreservingTtl("match:euw1:EUW1_CAS_UPGRADED", rawUpgradedRace, any<MatchDto>()) } returns false
                coEvery { riot.getMatchByIdLowPriority("EUW1_CAS_UPGRADED", "euw1", 0, any(), any()) } answers {
                    upgradedFetchDone = true
                    buildMatchDto("EUW1_CAS_UPGRADED", dtoSchemaVersion = 2) { 3006 }
                }

                val service = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )

                val (res, count) = service.refreshLegacyRoleQuestMatchesBounded(
                    listOf(
                        buildMatchDto("EUW1_NO_TTL"),
                        buildMatchDto("EUW1_EXPIRED"),
                        legacyRace,
                        legacyUpgradedRace
                    ),
                    "euw1"
                )

                // EUW1_NO_TTL and EUW1_EXPIRED failed closed with ZERO Riot API calls
                coVerify(exactly = 0) { riot.getMatchByIdLowPriority("EUW1_NO_TTL", any(), any(), any(), any()) }
                coVerify(exactly = 0) { riot.getMatchByIdLowPriority("EUW1_EXPIRED", any(), any(), any(), any()) }
                // EUW1_CAS_LOST failed closed on CAS false (retained legacy null); EUW1_CAS_UPGRADED adopted concurrent writer's 1206
                count shouldBe 1
                res[0].info!!.participants[0].roleBoundItem.shouldBeNull()
                res[1].info!!.participants[0].roleBoundItem.shouldBeNull()
                res[2].info!!.participants[0].roleBoundItem.shouldBeNull()
                res[3].info!!.participants[0].roleBoundItem shouldBe 1206

                // Profile sync with unknown/null TTL MUST fail closed and NEVER call set(..., 14d) or compareAndSetPreservingTtl!
                stubExistingLegacyMatchInRedis(redis, "EUW1_PROF_NOTTL", ttl = Duration.ofDays(20), casResult = true)
                coEvery { riot.getMatchByIdLowPriority("EUW1_PROF_NOTTL", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_PROF_NOTTL", dtoSchemaVersion = 2) { 3006 }
                every { redis.multiGet(listOf("match:euw1:EUW1_PROF_NOTTL"), MatchDto::class.java) } returns
                    mapOf("match:euw1:EUW1_PROF_NOTTL" to buildMatchDto("EUW1_PROF_NOTTL"))
                every { redis.setIfAbsent("lock:profile:euw1:puuid-nottl", any(), any()) } returns true
                val profNoTtl = SummonerProfileDto(
                    puuid = "puuid-nottl",
                    gameName = "NoTtl",
                    tagLine = "EUW",
                    summonerLevel = 100,
                    profileIconId = 1,
                    soloQueueRank = null,
                    recentMatches = listOf(buildMatchDto("EUW1_PROF_NOTTL")),
                    allMatchIds = listOf("EUW1_PROF_NOTTL"),
                    championStats = emptyList(),
                    overallStats = null,
                    improvementStats = null,
                    totalMatches = 1,
                    lastRefreshed = Instant.parse("2026-11-11T16:00:00Z")
                )
                every { redis.getVersioned("player:profile:euw1:puuid-nottl", SummonerProfileDto::class.java) } returns
                    RedisCacheService.VersionedCacheEntry(profNoTtl, "raw:prof-nottl:v1")
                every { redis.getRemainingTtl("player:profile:euw1:puuid-nottl") } returns null

                service.getMatchesByIds(listOf("EUW1_PROF_NOTTL"), "euw1", ownerPuuid = "puuid-nottl")
                verify(exactly = 0) { redis.compareAndSetPreservingTtl("player:profile:euw1:puuid-nottl", any(), any<SummonerProfileDto>()) }
                verify(exactly = 0) { redis.set(any(), any<SummonerProfileDto>(), any()) }
                verify(exactly = 0) { redis.set(any(), any<MatchDto>(), any()) }
                service.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSkippedCasOrTtl"] shouldBe 4L
            }

            Then("preserves existing timeline evidence in SummonerProfileDto.improvementStats during syncEmbeddedProfileRecentMatchesIfSafe") {
                val redis = mockk<RedisCacheService>(relaxed = true)
                val riot = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                every { riot.getActiveRetryAfterWaitMs(any()) } returns 0L

                val matchId = "EUW1_TL_PRESERVE"
                val legacyMatch = buildMatchDto(matchId)
                val upgradedMatch = buildMatchDto(matchId, dtoSchemaVersion = 2) { 3006 }

                // Build authoritative timeline frames (0..22m) with CS, leads, high unspent gold before purchase, death bounty, and dragon kill
                val tlFrames = (0..22).map { m ->
                    val ts = m * 60_000L
                    TimelineFrameDto(
                        timestamp = ts,
                        participantFrames = mapOf(
                            "1" to tlPFrame(
                                participantId = 1,
                                totalGold = 500 + m * 380,
                                currentGold = if (m == 11) 1800 else 450,
                                level = (1 + m / 2).coerceAtMost(18),
                                xp = m * 460,
                                minionsKilled = m * 8,
                                jungleMinionsKilled = m * 1
                            ),
                            "6" to tlPFrame(
                                participantId = 6,
                                totalGold = 500 + m * 300,
                                currentGold = 400,
                                level = (1 + m / 2).coerceAtMost(18),
                                xp = m * 400,
                                minionsKilled = m * 6,
                                jungleMinionsKilled = 0
                            )
                        ),
                        events = when (m) {
                            12 -> listOf(
                                tlEvent(type = "ITEM_PURCHASED", timestamp = 725_000L, participantId = 1, itemId = 3031),
                                tlEvent(
                                    type = "CHAMPION_KILL",
                                    timestamp = 730_000L,
                                    killerId = 6,
                                    victimId = 1,
                                    bounty = 300,
                                    shutdownBounty = 150
                                ),
                                tlEvent(
                                    type = "ELITE_MONSTER_KILL",
                                    timestamp = 740_000L,
                                    killerId = 1,
                                    killerTeamId = 100,
                                    monsterType = "DRAGON"
                                )
                            )
                            else -> emptyList()
                        }
                    )
                }
                val authoritativeTimeline = MatchTimelineDto(
                    metadata = MatchTimelineMetadata(matchId = matchId, participants = null),
                    info = MatchTimelineInfo(
                        frameInterval = 60_000L,
                        frames = tlFrames,
                        participants = listOf(TimelineParticipantDto(1, "puuid-1"), TimelineParticipantDto(6, "puuid-6"))
                    )
                )
                val richImprovementStats = timelineAnalysisService.computeImprovementStats(
                    timelines = listOf(Triple(matchId, authoritativeTimeline, legacyMatch)),
                    puuid = "puuid-1",
                    allMatches = listOf(legacyMatch)
                )
                // Sanity-check that richImprovementStats actually holds non-empty timeline evidence
                richImprovementStats.excludedMissingTimelineGames shouldBe 0
                richImprovementStats.phaseFarming.min10To20.eligibleGames shouldBe 1
                richImprovementStats.leadRetention.eligibleGamesAt10 shouldBe 1
                richImprovementStats.unspentGold.eligibleGames shouldBe 1
                richImprovementStats.deathCost.bountyOnPlayerDeaths.totalCombinedBountyGold shouldBe 450
                richImprovementStats.teamObjectives.dragon.timelinePersonalInvolvements shouldBe 1

                stubExistingLegacyMatchInRedis(redis, matchId, ttl = Duration.ofDays(19), casResult = true)
                every { redis.multiGet(listOf("match:euw1:$matchId"), MatchDto::class.java) } returns
                    mapOf("match:euw1:$matchId" to legacyMatch)
                coEvery { riot.getMatchByIdLowPriority(matchId, "euw1", 0, any(), any()) } returns upgradedMatch

                val newerRefreshedAt = Instant.parse("2026-11-11T17:00:00Z")
                val existingProfile = SummonerProfileDto(
                    puuid = "puuid-1",
                    gameName = "Tester",
                    tagLine = "EUW",
                    summonerLevel = 250,
                    profileIconId = 10,
                    soloQueueRank = null,
                    recentMatches = listOf(legacyMatch),
                    allMatchIds = listOf(matchId),
                    championStats = emptyList(),
                    overallStats = null,
                    improvementStats = richImprovementStats,
                    totalMatches = 1,
                    lastRefreshed = newerRefreshedAt
                )
                every { redis.setIfAbsent("lock:profile:euw1:puuid-1", any(), any()) } returns true
                every { redis.getVersioned("player:profile:euw1:puuid-1", SummonerProfileDto::class.java) } returns
                    RedisCacheService.VersionedCacheEntry(existingProfile, "raw:profile:puuid-1:v1")
                every { redis.getRemainingTtl("player:profile:euw1:puuid-1") } returns Duration.ofDays(9)
                val savedProfileSlot = slot<SummonerProfileDto>()
                every {
                    redis.compareAndSetPreservingTtl(
                        "player:profile:euw1:puuid-1",
                        "raw:profile:puuid-1:v1",
                        capture(savedProfileSlot)
                    )
                } returns true

                val syncService = PlayerCacheService(
                    redisCacheService = redis,
                    riotApiService = riot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )

                val pageResult = syncService.getMatchesByIds(listOf(matchId), "euw1", ownerPuuid = "puuid-1")
                pageResult.first().info!!.participants.first().roleBoundItem shouldBe 3006

                verify(exactly = 0) { redis.set(any(), any<SummonerProfileDto>(), any()) }
                val savedProfile = savedProfileSlot.captured
                savedProfile.lastRefreshed shouldBe newerRefreshedAt
                savedProfile.recentMatches!!.first().info!!.participants.first().roleBoundItem shouldBe 3006

                // ALL timeline-derived evidence MUST still be intact on overall, byChampion, and byRole!
                val savedImp = savedProfile.improvementStats.shouldNotBeNull()
                savedImp.excludedMissingTimelineGames shouldBe 0
                savedImp.phaseFarming shouldBe richImprovementStats.phaseFarming
                savedImp.leadRetention shouldBe richImprovementStats.leadRetention
                savedImp.unspentGold shouldBe richImprovementStats.unspentGold
                savedImp.deathCost.bountyOnPlayerDeaths shouldBe richImprovementStats.deathCost.bountyOnPlayerDeaths
                savedImp.deathCost.perMatch.first().hasTimeline shouldBe true
                savedImp.deathCost.perMatch.first().recordedDeaths shouldBe richImprovementStats.deathCost.perMatch.first().recordedDeaths
                savedImp.teamObjectives.dragon.timelineEligibleGames shouldBe 1
                savedImp.teamObjectives.dragon.timelinePersonalInvolvements shouldBe 1
                savedImp.byChampion["Champ1"]!!.phaseFarming shouldBe richImprovementStats.byChampion["Champ1"]!!.phaseFarming
                savedImp.byRole["TOP"]!!.leadRetention shouldBe richImprovementStats.byRole["TOP"]!!.leadRetention
            }

            Then("re-checks memory headroom and Retry-After INSIDE Semaphore.withPermit when queued behind saturated permits, and propagates CancellationException") {
                // Scenario 1: Queued caller waiting behind Semaphore(2) aborts when memory headroom drops while waiting
                val dropHeadroom = AtomicBoolean(false)
                val redis1 = mockk<RedisCacheService>(relaxed = true)
                val riot1 = mockk<RiotApiService>(relaxed = true)
                val indexRepo = mockk<IndexedPlayerRepository>(relaxed = true)
                every { riot1.getActiveRetryAfterWaitMs(any()) } returns 0L
                for (id in listOf("EUW1_S1", "EUW1_S2", "EUW1_S3")) {
                    stubExistingLegacyMatchInRedis(redis1, id)
                }

                val p1Entered = CompletableDeferred<Unit>()
                val p2Entered = CompletableDeferred<Unit>()
                val releasePermits1 = CompletableDeferred<Unit>()
                coEvery { riot1.getMatchByIdLowPriority("EUW1_S1", "euw1", 0, any(), any()) } coAnswers {
                    p1Entered.complete(Unit)
                    releasePermits1.await()
                    buildMatchDto("EUW1_S1", dtoSchemaVersion = 2) { 3006 }
                }
                coEvery { riot1.getMatchByIdLowPriority("EUW1_S2", "euw1", 0, any(), any()) } coAnswers {
                    p2Entered.complete(Unit)
                    releasePermits1.await()
                    buildMatchDto("EUW1_S2", dtoSchemaVersion = 2) { 3006 }
                }
                coEvery { riot1.getMatchByIdLowPriority("EUW1_S3", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_S3", dtoSchemaVersion = 2) { 3006 }

                val queuedMemService = PlayerCacheService(
                    redisCacheService = redis1,
                    riotApiService = riot1,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider { if (dropHeadroom.get()) lowMemory else healthyMemory },
                    legacyRoleQuestBackfillEnabled = true
                )

                coroutineScope {
                    val c1 = async { queuedMemService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_S1")), "euw1") }
                    val c2 = async { queuedMemService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_S2")), "euw1") }
                    p1Entered.await()
                    p2Entered.await()

                    // Semaphore(2) is now full; launch c3 while memory is STILL healthy so c3 passes pre-queue check and parks on Semaphore(2)
                    val c3 = async { queuedMemService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_S3")), "euw1") }
                    delay(30)

                    // Memory drops BEFORE permits release!
                    dropHeadroom.set(true)
                    releasePermits1.complete(Unit)

                    c1.await().second shouldBe 1
                    c2.await().second shouldBe 1
                    c3.await().second shouldBe 0
                }
                // EUW1_S3 MUST NOT make an outbound Riot call or consume a global budget attempt!
                coVerify(exactly = 0) { riot1.getMatchByIdLowPriority("EUW1_S3", any(), any(), any(), any()) }
                queuedMemService.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillAttempts"] shouldBe 2L
                queuedMemService.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSkippedHeadroom"] shouldBe 1L

                // Scenario 2: Queued caller waiting behind Semaphore(2) aborts when Retry-After trips while waiting
                val activeRetryAfter = AtomicBoolean(false)
                val redis2 = mockk<RedisCacheService>(relaxed = true)
                val riot2 = mockk<RiotApiService>(relaxed = true)
                every { riot2.getActiveRetryAfterWaitMs(any()) } answers { if (activeRetryAfter.get()) 5_000L else 0L }
                for (id in listOf("EUW1_R1", "EUW1_R2", "EUW1_R3")) {
                    stubExistingLegacyMatchInRedis(redis2, id)
                }

                val r1Entered = CompletableDeferred<Unit>()
                val r2Entered = CompletableDeferred<Unit>()
                val releasePermits2 = CompletableDeferred<Unit>()
                coEvery { riot2.getMatchByIdLowPriority("EUW1_R1", "euw1", 0, any(), any()) } coAnswers {
                    r1Entered.complete(Unit)
                    releasePermits2.await()
                    buildMatchDto("EUW1_R1", dtoSchemaVersion = 2) { 3006 }
                }
                coEvery { riot2.getMatchByIdLowPriority("EUW1_R2", "euw1", 0, any(), any()) } coAnswers {
                    r2Entered.complete(Unit)
                    releasePermits2.await()
                    null // e.g., R2 hit HTTP 429 and tripped Retry-After
                }
                coEvery { riot2.getMatchByIdLowPriority("EUW1_R3", "euw1", 0, any(), any()) } returns
                    buildMatchDto("EUW1_R3", dtoSchemaVersion = 2) { 3006 }

                val queuedRateService = PlayerCacheService(
                    redisCacheService = redis2,
                    riotApiService = riot2,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )

                coroutineScope {
                    val c1 = async { queuedRateService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_R1")), "euw1") }
                    val c2 = async { queuedRateService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_R2")), "euw1") }
                    r1Entered.await()
                    r2Entered.await()

                    // c3 passes pre-queue check (Retry-After == 0) and parks on Semaphore(2)
                    val c3 = async { queuedRateService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_R3")), "euw1") }
                    delay(30)

                    // Retry-After trips while c3 is queued!
                    activeRetryAfter.set(true)
                    releasePermits2.complete(Unit)

                    c1.await().second shouldBe 1
                    c2.await().second shouldBe 0
                    c3.await().second shouldBe 0
                }
                coVerify(exactly = 0) { riot2.getMatchByIdLowPriority("EUW1_R3", any(), any(), any(), any()) }
                queuedRateService.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillAttempts"] shouldBe 2L
                queuedRateService.getDiscoveryAndSeasonWriteTelemetry()["legacyRoleQuestBackfillSkippedRateCooldown"] shouldBe 1L

                // Scenario 3: CancellationException propagates cleanly without writing to Redis
                val cancelRedis = mockk<RedisCacheService>(relaxed = true)
                stubExistingLegacyMatchInRedis(cancelRedis, "EUW1_CANCEL")
                val cancelRiot = mockk<RiotApiService>(relaxed = true)
                every { cancelRiot.getActiveRetryAfterWaitMs(any()) } returns 0L
                coEvery { cancelRiot.getMatchByIdLowPriority("EUW1_CANCEL", "euw1", 0, any(), any()) } throws CancellationException("cancelled")
                val cancelService = PlayerCacheService(
                    redisCacheService = cancelRedis,
                    riotApiService = cancelRiot,
                    indexedPlayerRepository = indexRepo,
                    memoryStatsProvider = memProvider(healthyMemory),
                    legacyRoleQuestBackfillEnabled = true
                )
                shouldThrow<CancellationException> {
                    cancelService.refreshLegacyRoleQuestMatchesBounded(listOf(buildMatchDto("EUW1_CANCEL")), "euw1")
                }
                verify(exactly = 0) { cancelRedis.compareAndSetPreservingTtl("match:euw1:EUW1_CANCEL", any(), any<MatchDto>()) }
                verify(exactly = 0) { cancelRedis.set("match:euw1:EUW1_CANCEL", any<MatchDto>(), any()) }
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

    Given("Plain League language output mapping & human missing-vs-zero labels (Owner 19:08)") {
        When("TimelineAnalysisService formats event targets, missing champion fallbacks, dangerous zones, and Role Quest status labels") {
            Then("never exposes raw schema/enum/variable names or participant indices, and distinguishes missing/unrecorded from zero in human words") {
                val forbiddenTokens = listOf(
                    "roleBoundItem",
                    "dtoSchemaVersion",
                    "BARON_NASHOR",
                    "RIFTHERALD",
                    "HORDE",
                    "TOP_LANE",
                    "MID_LANE",
                    "BOT_LANE",
                    "YELLOW_TRINKET",
                    "CONTROL_WARD",
                    "BLUE_TRINKET",
                    "SIGHT_WARD",
                    "UNDEFINED",
                    "UNKNOWN",
                    "OBJECTIVE",
                    "Participant "
                )

                // 0 analyzed timelines -> "Not available" (missing/no sample), NOT "Unknown" or "No deaths recorded"
                val emptyAnalysis = timelineAnalysisService.analyze(emptyList(), "puuid-3")
                emptyAnalysis.aggregateSummary.mostDangerousZone shouldBe "Not available"

                // Build a match where assisting jungler (participant 2) has null championName, player (pid 3) has 0 deaths,
                // player Role Quest is completed (1206), and opponent (pid 8) has explicit 0 Role Quest item
                val baseMatch = buildMatchDto("EUW1_HUMAN_1") { pid ->
                    when (pid) {
                        3 -> 1206
                        8 -> 0
                        else -> 1208
                    }
                }
                val matchWithMissingChamp = baseMatch.copy(
                    info = baseMatch.info!!.copy(
                        participants = baseMatch.info!!.participants.map { p ->
                            if (p.participantId == 2) p.copy(championName = null) else p
                        }
                    )
                )

                val richEventsFrame = TimelineFrameDto(
                    timestamp = 840_000L, // 14:00
                    participantFrames = mapOf(
                        "3" to tlPFrame(participantId = 3, totalGold = 5200, position = TimelinePositionDto(7500, 7500)),
                        "8" to tlPFrame(participantId = 8, totalGold = 4800, position = TimelinePositionDto(7600, 7600))
                    ),
                    events = listOf(
                        tlEvent(
                            type = "CHAMPION_KILL",
                            timestamp = 845_000L,
                            killerId = 3,
                            victimId = 8,
                            assistingParticipantIds = listOf(2),
                            position = TimelinePositionDto(7550, 7550)
                        ),
                        tlEvent(
                            type = "TURRET_PLATE_DESTROYED",
                            timestamp = 780_000L,
                            killerId = 3,
                            laneType = "TOP_LANE",
                            assistingParticipantIds = listOf(2)
                        ),
                        tlEvent(
                            type = "BUILDING_KILL",
                            timestamp = 850_000L,
                            killerId = 3,
                            laneType = null,
                            buildingType = "TOWER_BUILDING"
                        ),
                        tlEvent(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 855_000L,
                            killerId = 3,
                            monsterType = "HORDE"
                        ),
                        tlEvent(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 860_000L,
                            killerId = 3,
                            monsterType = "RIFTHERALD"
                        ),
                        tlEvent(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 865_000L,
                            killerId = 3,
                            monsterType = "BARON_NASHOR"
                        ),
                        tlEvent(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 870_000L,
                            killerId = 3,
                            monsterType = "DRAGON",
                            monsterSubType = "FIRE_DRAGON"
                        ),
                        tlEvent(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 875_000L,
                            killerId = 3,
                            monsterType = null
                        ),
                        tlEvent(
                            type = "WARD_PLACED",
                            timestamp = 880_000L,
                            creatorId = 3,
                            wardType = "YELLOW_TRINKET"
                        ),
                        tlEvent(
                            type = "WARD_KILL",
                            timestamp = 885_000L,
                            killerId = 3,
                            wardType = "CONTROL_WARD"
                        ),
                        tlEvent(
                            type = "ITEM_DESTROYED",
                            timestamp = 780_000L, // 13:00
                            participantId = 3,
                            itemId = 1201
                        )
                    )
                )

                val tl = MatchTimelineDto(
                    metadata = MatchTimelineMetadata(matchId = "EUW1_HUMAN_1", participants = null),
                    info = MatchTimelineInfo(
                        frameInterval = 60_000L,
                        frames = listOf(richEventsFrame),
                        participants = listOf(TimelineParticipantDto(3, "puuid-3"), TimelineParticipantDto(8, "puuid-8"))
                    )
                )

                val analyzed = timelineAnalysisService.analyze(
                    listOf(Triple("EUW1_HUMAN_1", tl, matchWithMissingChamp)),
                    "puuid-3"
                )

                // >=1 analyzed game with 0 player deaths -> "No deaths recorded" (explicit zero, distinct from "Not available")
                analyzed.aggregateSummary.mostDangerousZone shouldBe "No deaths recorded"

                val evs = analyzed.eventTimelines.first().events
                evs.first { it.type == "KILL" }.assistants shouldBe listOf("Unknown Champion")

                val plateTargets = evs.filter { it.type == "PLATE" }.map { it.target }
                plateTargets shouldBe listOf("Top Lane Turret Plate", "Enemy Turret")

                val objTargets = evs.filter { it.type == "OBJECTIVE" }.map { it.target }
                objTargets shouldBe listOf("Void Grubs", "Rift Herald", "Baron Nashor", "Infernal Drake", "Epic Monster")

                evs.first { it.type == "WARD_PLACED" }.target shouldBe "Stealth Ward"
                evs.first { it.type == "WARD_PLACED" }.wardType shouldBe "YELLOW_TRINKET"
                evs.first { it.type == "WARD_KILLED" }.target shouldBe "Control Ward"
                evs.first { it.type == "WARD_KILLED" }.wardType shouldBe "CONTROL_WARD"

                // Quest human labels distinguish completed (with timestamp), zero (0 completed), missing (not recorded), starter, ambiguous, and unseen ID
                val qt1 = analyzed.roleQuestTimings.first()
                qt1.playerQuestStatusLabel shouldBe "Mid Lane Quest Reward completed at 13:00"
                qt1.opponentQuestStatusLabel shouldBe "No Role Quest item (0 completed)"

                val legacyNullMatch = buildMatchDto("EUW1_HUMAN_NULL") { null }
                val starterAndUnseenMatch = buildMatchDto("EUW1_HUMAN_STARTER") { pid ->
                    when (pid) {
                        3 -> 1201 // Mid starter in progress
                        8 -> 9999 // Unseen positive ID
                        else -> 0
                    }
                }
                val ambiguousCompletedMatch = buildMatchDto("EUW1_HUMAN_AMBIG") { 1206 } // No ITEM_DESTROYED 1201 in empty timeline
                val emptyFrameTl = MatchTimelineDto(
                    metadata = MatchTimelineMetadata(matchId = "EUW1_EMPTY", participants = null),
                    info = MatchTimelineInfo(
                        frameInterval = 60_000L,
                        frames = listOf(
                            TimelineFrameDto(
                                timestamp = 60_000L,
                                participantFrames = mapOf(
                                    "3" to tlPFrame(participantId = 3, totalGold = 500),
                                    "8" to tlPFrame(participantId = 8, totalGold = 500)
                                ),
                                events = emptyList()
                            )
                        ),
                        participants = listOf(TimelineParticipantDto(3, "puuid-3"), TimelineParticipantDto(8, "puuid-8"))
                    )
                )

                val multiStates = timelineAnalysisService.analyze(
                    listOf(
                        Triple("EUW1_HUMAN_NULL", emptyFrameTl, legacyNullMatch),
                        Triple("EUW1_HUMAN_STARTER", emptyFrameTl, starterAndUnseenMatch),
                        Triple("EUW1_HUMAN_AMBIG", emptyFrameTl, ambiguousCompletedMatch)
                    ),
                    "puuid-3"
                )

                multiStates.roleQuestTimings[0].playerQuestStatusLabel shouldBe "Role Quest data not recorded for this match"
                multiStates.roleQuestTimings[1].playerQuestStatusLabel shouldBe "Mid Lane Quest in progress (not completed)"
                multiStates.roleQuestTimings[1].opponentQuestStatusLabel shouldBe "Unrecognized Role Quest slot item (completion unverified)"
                multiStates.roleQuestTimings[2].playerQuestStatusLabel shouldBe "Mid Lane Quest Reward completed (exact time unavailable)"

                // Sweep every human-facing target, assistant, zone, and quest status label for forbidden raw identifiers
                val allHumanStrings = buildList {
                    add(emptyAnalysis.aggregateSummary.mostDangerousZone)
                    add(analyzed.aggregateSummary.mostDangerousZone)
                    evs.forEach { e ->
                        e.target?.let { add(it) }
                        e.actor?.let { add(it) }
                        e.assistants?.forEach { add(it) }
                    }
                    (analyzed.roleQuestTimings + multiStates.roleQuestTimings).forEach { q ->
                        q.playerQuestStatusLabel?.let { add(it) }
                        q.opponentQuestStatusLabel?.let { add(it) }
                    }
                }
                for (s in allHumanStrings) {
                    for (forbidden in forbiddenTokens) {
                        (s.contains(forbidden)) shouldBe false
                    }
                }
            }
        }
    }
})
