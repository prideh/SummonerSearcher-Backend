package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import com.pride.summoner_searcher_api.model.CohortCollectorCheckpointEntity
import com.pride.summoner_searcher_api.repository.CohortBenchmarkCellRepository
import com.pride.summoner_searcher_api.repository.CohortCollectorCheckpointRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.*
import java.time.Instant

class CohortBenchmarkServiceTest : BehaviorSpec({
    val objectMapper = ObjectMapper().findAndRegisterModules()
    val redisCacheService = mockk<RedisCacheService>(relaxed = true)
    val cohortRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
    val riotApiService = mockk<RiotApiService>(relaxed = true)

    val nowInstant = Instant.parse("2026-11-04T12:00:00Z")
    val snapshotTime = Instant.parse("2026-11-04T10:00:00Z")
    val fixedTimeProvider = object : TimeProvider {
        override fun now(): Long = nowInstant.toEpochMilli()
    }

    fun buildParticipant(
        puuid: String,
        role: String,
        laneCs10: Int,
        totalMinions: Int,
        neutralMinions: Int?,
        kills: Int = 6,
        deaths: Int = 3,
        assists: Int = 7,
        goldEarned: Int = 12000,
        damageToChamps: Int = 24000,
        visionScore: Int = 30,
        timeSpentDead: Int = 90
    ): ParticipantDto {
        val neutralJsonField = if (neutralMinions != null) """"neutralMinionsKilled": $neutralMinions,""" else ""
        val jungleChallengeField = if (neutralMinions != null) {
            """"jungleCsBefore10Minutes": ${neutralMinions.coerceAtMost(12).toFloat()},"""
        } else {
            ""
        }
        val json = """
            {
              "puuid": "$puuid",
              "teamId": 100,
              "teamPosition": "$role",
              "individualPosition": "$role",
              "kills": $kills,
              "deaths": $deaths,
              "assists": $assists,
              "totalMinionsKilled": $totalMinions,
              $neutralJsonField
              "goldEarned": $goldEarned,
              "totalDamageDealtToChampions": $damageToChamps,
              "visionScore": $visionScore,
              "totalTimeSpentDead": $timeSpentDead,
              "win": true,
              "challenges": {
                "laneMinionsFirst10Minutes": $laneCs10,
                $jungleChallengeField
                "goldPerMinute": ${(goldEarned / 30.0).toFloat()},
                "damagePerMinute": ${(damageToChamps / 30.0).toFloat()},
                "visionScorePerMinute": ${(visionScore / 30.0).toFloat()},
                "killParticipation": 0.65
              }
            }
        """.trimIndent()
        return objectMapper.readValue(json, ParticipantDto::class.java)
    }

    beforeTest {
        clearMocks(redisCacheService, cohortRepo, riotApiService, answers = false)
        every { cohortRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } returns null
        every { cohortRepo.save(any()) } answers { firstArg() }
    }

    Given("CohortBenchmarkService cache-only extraction on euw1") {

        When("extracting from cached profiles and matches with per-role 114/120 ceilings, missing jungle counters, freshness bounds, and honest patch labeling") {
            val rateLimiter = RiotApiRateLimiter(fixedTimeProvider)
            val service = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = fixedTimeProvider
            )

            val profileKeyMid = "player:profile:euw1:puuid-mid-1"
            val profileKeyTop = "player:profile:euw1:puuid-top-1"
            val profileKeyBot = "player:profile:euw1:puuid-bot-1"
            val profileKeyStale = "player:profile:euw1:puuid-stale-rank"
            val allProfileKeys = listOf(profileKeyMid, profileKeyTop, profileKeyBot, profileKeyStale)

            every { redisCacheService.scanKeys("player:profile:euw1:*", any(), any()) } returns allProfileKeys
            every { redisCacheService.scanKeys("leaderboard:euw1:*", any(), any()) } returns emptyList()
            every { redisCacheService.get(match { it.startsWith("leaderboard:") }, CachedLeaderboardDto::class.java) } returns null

            val rankEmerald = LeagueEntryDto(
                puuid = "puuid-mid-1", leaguePoints = 50, rank = "II", wins = 30, losses = 25,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                leagueId = "l1", queueType = "RANKED_SOLO_5x5", tier = "EMERALD", summonerName = null
            )
            every {
                redisCacheService.multiGet(allProfileKeys, SummonerProfileDto::class.java)
            } returns mapOf(
                profileKeyMid to SummonerProfileDto(
                    puuid = "puuid-mid-1", gameName = "MidOne", tagLine = "EUW",
                    summonerLevel = 200, profileIconId = 1, soloQueueRank = rankEmerald,
                    recentMatches = emptyList(), lastRefreshed = snapshotTime
                ),
                profileKeyTop to SummonerProfileDto(
                    puuid = "puuid-top-1", gameName = "TopOne", tagLine = "EUW",
                    summonerLevel = 180, profileIconId = 2,
                    soloQueueRank = rankEmerald.copy(puuid = "puuid-top-1", tier = "DIAMOND"),
                    recentMatches = emptyList(), lastRefreshed = snapshotTime.plusSeconds(60)
                ),
                profileKeyBot to SummonerProfileDto(
                    puuid = "puuid-bot-1", gameName = "BotOne", tagLine = "EUW",
                    summonerLevel = 190, profileIconId = 3,
                    soloQueueRank = rankEmerald.copy(puuid = "puuid-bot-1", tier = "EMERALD"),
                    recentMatches = emptyList(), lastRefreshed = snapshotTime
                ),
                // Stale rank snapshot (10 days old > 7d bound) -> must be excluded!
                profileKeyStale to SummonerProfileDto(
                    puuid = "puuid-stale-rank", gameName = "StaleRank", tagLine = "EUW",
                    summonerLevel = 150, profileIconId = 4,
                    soloQueueRank = rankEmerald.copy(puuid = "puuid-stale-rank", tier = "PLATINUM"),
                    recentMatches = emptyList(), lastRefreshed = nowInstant.minusSeconds(10 * 24 * 3600L)
                )
            )

            val recentGameMs = snapshotTime.minusSeconds(3600).toEpochMilli()
            val matchKeys = (1..9).map { "match:euw1:EUW1_$it" }
            every { redisCacheService.scanKeys("match:euw1:*", any(), any()) } returns matchKeys
            every { redisCacheService.scanKeys("timeline:euw1:*", any(), any()) } returns emptyList()

            val matchesMap = mapOf(
                "match:euw1:EUW1_1" to MatchDto(
                    metadata = MatchMetadata("EUW1_1", listOf("puuid-mid-1", "puuid-top-1", "puuid-bot-1", "puuid-no-snapshot")),
                    info = MatchInfo(
                        gameId = 1L, gameCreation = recentGameMs + 5000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 84, totalMinions = 240, neutralMinions = 30),
                            // Duplicate (matchId, puuid) entry in same match payload to verify deduplication
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 84, totalMinions = 240, neutralMinions = 30),
                            // TOP and BOTTOM on patch 16.7 with NULL jungle counters -> must have 114 ceiling and null totalCsWithCamps!
                            buildParticipant("puuid-top-1", "TOP", laneCs10 = 79, totalMinions = 210, neutralMinions = null),
                            buildParticipant("puuid-bot-1", "BOTTOM", laneCs10 = 82, totalMinions = 225, neutralMinions = null),
                            // Participant with no cached rank snapshot + JUNGLE participant
                            buildParticipant("puuid-no-snapshot", "BOTTOM", laneCs10 = 70, totalMinions = 200, neutralMinions = 0),
                            buildParticipant("puuid-jg", "JUNGLE", laneCs10 = 10, totalMinions = 30, neutralMinions = 180)
                        )
                    )
                ),
                "match:euw1:EUW1_2" to MatchDto(
                    metadata = MatchMetadata("EUW1_2", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 2L, gameCreation = recentGameMs + 4000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 78, totalMinions = 210, neutralMinions = 15)
                        )
                    )
                ),
                "match:euw1:EUW1_3" to MatchDto(
                    metadata = MatchMetadata("EUW1_3", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 3L, gameCreation = recentGameMs + 3000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 90, totalMinions = 270, neutralMinions = 0)
                        )
                    )
                ),
                "match:euw1:EUW1_4" to MatchDto(
                    metadata = MatchMetadata("EUW1_4", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 4L, gameCreation = recentGameMs + 2000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            // Older game beyond maxGamesPerPlayer=3 -> excluded by per-player cap
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 30, totalMinions = 90, neutralMinions = 0)
                        )
                    )
                ),
                "match:euw1:EUW1_5" to MatchDto(
                    metadata = MatchMetadata("EUW1_5", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 5L, gameCreation = recentGameMs + 1000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            // Older game beyond maxGamesPerPlayer=3 -> excluded by per-player cap
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 20, totalMinions = 60, neutralMinions = 0)
                        )
                    )
                ),
                "match:euw1:EUW1_6" to MatchDto(
                    metadata = MatchMetadata("EUW1_6", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 6L, gameCreation = recentGameMs + 6000L, gameDuration = 1200L, gameMode = "ARAM",
                        gameVersion = "16.7.612.1111", queueId = 450,
                        participants = emptyList()
                    )
                ),
                "match:euw1:EUW1_7" to MatchDto(
                    metadata = MatchMetadata("EUW1_7", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 7L, gameCreation = recentGameMs + 6000L, gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.6.600.1000", queueId = 420,
                        participants = emptyList()
                    )
                ),
                // Match 8: 9 days before rankSnapshot (within 14d match age, but > 7d skew from rankSnapshot) -> participant excluded!
                "match:euw1:EUW1_8" to MatchDto(
                    metadata = MatchMetadata("EUW1_8", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 8L, gameCreation = snapshotTime.minusSeconds(9 * 24 * 3600L).toEpochMilli(),
                        gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 99, totalMinions = 300, neutralMinions = 10)
                        )
                    )
                ),
                // Match 9: 20 days old (> 14d match age bound) -> match excluded as stale!
                "match:euw1:EUW1_9" to MatchDto(
                    metadata = MatchMetadata("EUW1_9", listOf("puuid-mid-1")),
                    info = MatchInfo(
                        gameId = 9L, gameCreation = nowInstant.minusSeconds(20 * 24 * 3600L).toEpochMilli(),
                        gameDuration = 1800L, gameMode = "CLASSIC",
                        gameVersion = "16.7.612.1111", queueId = 420,
                        participants = listOf(
                            buildParticipant("puuid-mid-1", "MIDDLE", laneCs10 = 99, totalMinions = 300, neutralMinions = 10)
                        )
                    )
                )
            )
            every { redisCacheService.multiGet(matchKeys, MatchDto::class.java) } returns matchesMap

            val report = service.extractCachedCohortsAndReport(
                region = "euw1",
                targetPatch = null,
                maxGamesPerPlayer = 3
            )

            Then("it should enforce per-role 114/120 ceilings, keep missing jungle counters null, bound snapshot/match freshness, and label latestCachedPatch honestly") {
                coVerify(exactly = 0) { riotApiService.getMatchById(any(), any()) }
                coVerify(exactly = 0) { riotApiService.getMatchIdsByPuuid(any(), any(), any(), any(), any(), any(), any()) }

                report.inventory.latestCachedPatch shouldBe "16.7"
                report.inventory.selectedTargetPatch shouldBe "16.7"
                report.inventory.patchSelectionSource shouldBe "LATEST_CACHED_PATCH_IN_REDIS"
                report.inventory.excludedStaleRankSnapshots shouldBe 1
                report.inventory.excludedStaleOrUnboundedMatches shouldBe 1
                report.inventory.excludedParticipantsOutsideSnapshotWindow shouldBe 1
                report.inventory.soloQueue420MatchesOnTargetPatch shouldBe 6
                report.inventory.excludedNonSoloQueueMatches shouldBe 1
                report.inventory.excludedOtherPatchMatches shouldBe 1
                report.inventory.excludedOtherRoleParticipants shouldBe 1
                report.inventory.participantsWithoutRankSnapshot shouldBe 1
                report.inventory.deduplicatedParticipantEntries shouldBe 1
                report.inventory.excludedOverPlayerCapSamples shouldBe 2
                report.inventory.eligibleCohortSamples shouldBe 5

                report.cells shouldHaveSize 3
                val midCell = report.cells.first { it.tier == "EMERALD" && it.role == "MIDDLE" }
                val botCell = report.cells.first { it.tier == "EMERALD" && it.role == "BOTTOM" }
                val topCell = report.cells.first { it.tier == "DIAMOND" && it.role == "TOP" }

                midCell.patchSelectionSource shouldBe "LATEST_CACHED_PATCH_IN_REDIS"
                midCell.distinctPlayers shouldBe 1
                midCell.eligibleGames shouldBe 3
                midCell.excludedOverPlayerCapGames shouldBe 2
                midCell.passesQualityGate shouldBe false
                midCell.colorBandsEnabled shouldBe false

                // MIDDLE on patch 16.7 has 120 lane minion ceiling
                midCell.metrics.laneCsAt10.unit shouldBe "lane_cs"
                midCell.metrics.laneCsAt10.includesJungleCamps shouldBe false
                midCell.metrics.laneCsAt10.theoreticalSupplyCeiling shouldBe 120
                midCell.metrics.laneCsAt10.median shouldBe 84.0
                midCell.metrics.laneCsAt10.p25 shouldBe 81.0
                midCell.metrics.laneCsAt10.p75 shouldBe 87.0
                midCell.metrics.laneCsAt10.iqr shouldBe 6.0

                // TOP and BOTTOM on patch 16.7 MUST have 114 lane minion ceiling (never universal 120!)
                topCell.metrics.laneCsAt10.theoreticalSupplyCeiling shouldBe 114
                topCell.metrics.laneCsAt10.median shouldBe 79.0
                botCell.metrics.laneCsAt10.theoreticalSupplyCeiling shouldBe 114
                botCell.metrics.laneCsAt10.median shouldBe 82.0

                // Missing jungle counter (neutralMinionsKilled = null, jungleCsBefore10Minutes = null) on TOP/BOTTOM
                // MUST stay null (sampleGames = 0, median = null), NEVER default to 0!
                topCell.metrics.totalCsPerMinWithCamps.sampleGames shouldBe 0
                topCell.metrics.totalCsPerMinWithCamps.median shouldBe null
                topCell.metrics.timelineTotalCsAt10WithCamps.sampleGames shouldBe 0
                topCell.metrics.timelineTotalCsAt10WithCamps.median shouldBe null
                // While lane-only CS/min is still computed accurately from totalMinionsKilled (210 / 30 = 7.0)
                topCell.metrics.laneOnlyCsPerMin.median shouldBe 7.0

                // MIDDLE had non-null neutralMinionsKilled, so totalCsPerMinWithCamps and timelineTotalCsAt10WithCamps are populated
                midCell.metrics.totalCsPerMinWithCamps.includesJungleCamps shouldBe true
                midCell.metrics.totalCsPerMinWithCamps.median shouldBe 9.0
                midCell.metrics.timelineTotalCsAt10WithCamps.includesJungleCamps shouldBe true
                midCell.metrics.timelineTotalCsAt10WithCamps.median shouldBe 90.0
            }
        }

        When("a cohort cell reaches >= 50 distinct players and >= 200 eligible games on patch 14.24 (114 lane minion ceiling)") {
            val rateLimiter = RiotApiRateLimiter(fixedTimeProvider)
            val savedEntities = mutableListOf<CohortBenchmarkCellEntity>()
            every { cohortRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } returns null
            every { cohortRepo.save(any()) } answers {
                val e = firstArg<CohortBenchmarkCellEntity>()
                savedEntities.add(e)
                e
            }

            val service = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = fixedTimeProvider
            )

            // 50 distinct DIAMOND TOP players, 4 games each = 200 eligible games
            val playerIds = (1..50).map { "puuid-dia-$it" }
            val profileKeys = playerIds.map { "player:profile:euw1:$it" }
            every { redisCacheService.scanKeys("player:profile:euw1:*", any(), any()) } returns profileKeys
            every { redisCacheService.scanKeys("leaderboard:euw1:*", any(), any()) } returns emptyList()
            every { redisCacheService.get(match { it.startsWith("leaderboard:") }, CachedLeaderboardDto::class.java) } returns null

            every { redisCacheService.multiGet(any(), SummonerProfileDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { key ->
                    val pid = key.substringAfterLast(":")
                    SummonerProfileDto(
                        puuid = pid, gameName = pid, tagLine = "EUW",
                        summonerLevel = 300, profileIconId = 1,
                        soloQueueRank = LeagueEntryDto(
                            puuid = pid, leaguePoints = 75, rank = "I", wins = 50, losses = 40,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                            leagueId = "dia", queueType = "RANKED_SOLO_5x5", tier = "DIAMOND", summonerName = null
                        ),
                        recentMatches = emptyList(),
                        lastRefreshed = snapshotTime
                    )
                }
            }

            val baseGameCreation = snapshotTime.minusSeconds(7200).toEpochMilli()
            val matchKeys = (1..200).map { "match:euw1:EUW1_M$it" }
            every { redisCacheService.scanKeys("match:euw1:*", any(), any()) } returns matchKeys
            every { redisCacheService.scanKeys("timeline:euw1:*", any(), any()) } returns emptyList()

            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { key ->
                    val matchId = key.substringAfterLast(":")
                    val idx = matchId.substringAfter("EUW1_M").toInt()
                    val pid = playerIds[(idx - 1) % 50]
                    MatchDto(
                        metadata = MatchMetadata(matchId, listOf(pid)),
                        info = MatchInfo(
                            gameId = idx.toLong(),
                            gameCreation = baseGameCreation + idx * 1000L,
                            gameDuration = 1800L,
                            gameMode = "CLASSIC",
                            gameVersion = "14.24.500.1000",
                            queueId = 420,
                            participants = listOf(
                                buildParticipant(pid, "TOP", laneCs10 = 76, totalMinions = 228, neutralMinions = 12)
                            )
                        )
                    )
                }
            }

            val report = service.extractCachedCohortsAndReport("euw1")

            Then("it should pass the 50-player / 200-game quality gate, enable colorBandsEnabled, use the 114 ceiling, and persist de-identified aggregates") {
                report.cells shouldHaveSize 1
                val cell = report.cells.first()
                cell.distinctPlayers shouldBe 50
                cell.eligibleGames shouldBe 200
                cell.passesQualityGate shouldBe true
                cell.colorBandsEnabled shouldBe true
                cell.qualityGateStatus shouldBe "READY_FOR_COLOR_BANDS"
                cell.metrics.laneCsAt10.theoreticalSupplyCeiling shouldBe 114

                savedEntities shouldHaveSize 1
                val entity = savedEntities.first()
                entity.passesQualityGate shouldBe true
                entity.colorBandsEnabled shouldBe true
                entity.metricsJson.contains("puuid-dia-") shouldBe false
            }
        }

        When("serving public GET /cohorts/{region}/report with cached report, recompute cooldown guard, and input validation") {
            val rateLimiter = RiotApiRateLimiter(fixedTimeProvider)
            val service = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = fixedTimeProvider
            )

            // 1. Invalid region -> rejected with 0 Redis scans
            val invalidRegionReport = service.getPublicCohortReport("invalid_region")

            // 2. Invalid patch format -> rejected with 0 Redis scans
            val invalidPatchReport = service.getPublicCohortReport("euw1", "16.7;DROP")

            // 3. Cached report present -> served from Redis with 0 scans
            val cachedReportFixture = CohortExtractionReportDto(
                reportSource = "FRESH_BOUNDED_EXTRACTION",
                inventory = CohortCacheInventoryDto(
                    region = "euw1",
                    scannedAt = snapshotTime,
                    cachedProfileCount = 10,
                    profilesWithSoloQueueRankSnapshot = 10,
                    cachedChallengerLeaderboardPlayers = 0,
                    totalDistinctRankedPlayersInCache = 10,
                    ranksByTier = mapOf("EMERALD" to 10),
                    cachedMatchCount = 25,
                    cachedTimelineCount = 0,
                    matchesByPatch = mapOf("16.7" to 25),
                    latestCachedPatch = "16.7",
                    selectedTargetPatch = "16.7",
                    patchSelectionSource = "LATEST_CACHED_PATCH_IN_REDIS",
                    soloQueue420MatchesOnTargetPatch = 25,
                    excludedNonSoloQueueMatches = 0,
                    excludedMissingGameVersionMatches = 0,
                    excludedOtherPatchMatches = 0,
                    excludedShortOrRemakeMatches = 0,
                    topMidBotParticipantCandidates = 25,
                    excludedOtherRoleParticipants = 0,
                    participantsWithRankSnapshot = 25,
                    participantsWithoutRankSnapshot = 0,
                    deduplicatedParticipantEntries = 0,
                    excludedOverPlayerCapSamples = 0,
                    eligibleCohortSamples = 25,
                    distinctEligibleCohortPlayers = 10,
                    cellsTotal = 1,
                    cellsPassingQualityGate = 0
                ),
                rateLimitVerification = rateLimiter.getVerificationSnapshot("euw1", false),
                cells = emptyList(),
                methodologyNotes = emptyList()
            )
            every {
                redisCacheService.get("cohort:report:euw1:latest", CohortExtractionReportDto::class.java)
            } returns cachedReportFixture

            val cachedHitReport = service.getPublicCohortReport("euw1", null)

            // 4. Cache miss while recompute guard lock is held/on cooldown -> serves persisted cells with 0 scans
            every {
                redisCacheService.get("cohort:report:euw1:16.7", CohortExtractionReportDto::class.java)
            } returns null
            every {
                redisCacheService.setIfAbsent("lock:cohort:report:euw1:16.7", "LOCKED", any())
            } returns false
            every {
                redisCacheService.get("cohort:benchmarks:euw1:16.7", Array<CohortBenchmarkCellDto>::class.java)
            } returns emptyArray()
            every {
                cohortRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc("euw1", "16.7", 420)
            } returns emptyList()

            val guardedFallbackReport = service.getPublicCohortReport("euw1", "16.7")

            Then("it should never execute Redis scanKeys or DB saves on invalid input, cached report hits, or guarded cooldown hits") {
                invalidRegionReport.reportSource shouldBe "INVALID_REGION"
                invalidPatchReport.reportSource shouldBe "INVALID_PATCH_FORMAT"
                cachedHitReport.reportSource shouldBe "CACHED_REPORT"
                guardedFallbackReport.reportSource shouldBe "GUARDED_COOLDOWN_FALLBACK"

                verify(exactly = 0) { redisCacheService.scanKeys(any(), any(), any()) }
                verify(exactly = 0) { cohortRepo.save(any()) }
            }
        }

        When("verifying rate-limit guardrails for low-priority network collection") {
            val rateLimiter = RiotApiRateLimiter(fixedTimeProvider)
            val serviceHeld = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = fixedTimeProvider
            )
            val serviceEnabled = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = fixedTimeProvider
            )

            val heldResult = serviceHeld.runGuardedLowPriorityCollectionStep("euw1", listOf("p1"))
            val unverifiedResult = serviceEnabled.runGuardedLowPriorityCollectionStep("euw1", listOf("p1"))

            // Now feed real App + Method headers and verify snapshot
            rateLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "2:1,15:120",
                endpointKey = "match-v5-match-by-id",
                methodLimitHeader = "2000:10",
                methodCountHeader = "5:10"
            )
            val verifiedSnapshot = rateLimiter.getVerificationSnapshot("euw1", networkCollectionEnabled = true)

            // Mark an active HIGH-priority user request and verify LOW-priority readiness pauses
            rateLimiter.beginUserRequest("euw1")
            val pausedDuringUserReq = rateLimiter.getVerificationSnapshot("euw1", networkCollectionEnabled = true)
            rateLimiter.endUserRequest("euw1")

            // Record a 429 Retry-After window and verify LOW-priority readiness pauses
            rateLimiter.recordRetryAfter("euw1", 5_000L)
            val pausedDuringRetryAfter = rateLimiter.getVerificationSnapshot("euw1", networkCollectionEnabled = true)

            Then("it should hold network collection when disabled or unverified, track both app and method limits, and pause on user requests or 429 Retry-After") {
                heldResult["executed"] shouldBe false
                heldResult["reason"] shouldBe "NETWORK_COLLECTION_DISABLED_BY_CONFIG"

                unverifiedResult["executed"] shouldBe false
                unverifiedResult["reason"] shouldBe "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY"

                verifiedSnapshot.appLimitsVerifiedFromHeaders shouldBe true
                verifiedSnapshot.appRateLimitHeader shouldBe "20:1,100:120"
                verifiedSnapshot.methodLimitsByEndpoint["match-v5-match-by-id"]?.methodRateLimitHeader shouldBe "2000:10"
                verifiedSnapshot.readyForLowPriorityNetworkCollection shouldBe true

                pausedDuringUserReq.activeUserRequests shouldBe 1
                pausedDuringUserReq.foregroundYieldActive shouldBe true
                pausedDuringUserReq.readyForLowPriorityNetworkCollection shouldBe false

                pausedDuringRetryAfter.retryAfterUntil shouldNotBe null
                pausedDuringRetryAfter.readyForLowPriorityNetworkCollection shouldBe false
            }
        }

        When("running the resumable checkpointed LOW-priority collector across disabled, unverified, foreground-yield, service-429, and verified resume states") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val rateLimiter = RiotApiRateLimiter(testClock)
            val localRedis = mockk<RedisCacheService>(relaxed = true)

            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedRankSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            val savedMatches = mutableMapOf<String, MatchDto>()

            every { localRedis.scanKeys(any(), any(), any()) } answers {
                val pattern = firstArg<String>()
                when {
                    pattern.startsWith("cohort:rank:snapshot:euw1:") -> savedRankSnapshots.keys.toList()
                    pattern.startsWith("match:euw1:") -> savedMatches.keys.toList()
                    else -> emptyList()
                }
            }
            every { localRedis.multiGet(any(), CohortRankSnapshotDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { savedRankSnapshots[it] }
            }
            every { localRedis.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { savedMatches[it] }
            }
            every { localRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { localRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                savedRankSnapshots[firstArg()]
            }
            every { localRedis.get(any(), MatchDto::class.java) } answers {
                savedMatches[firstArg()]
            }
            every { localRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { localRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { localRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedRankSnapshots[key] = value
                    is MatchDto -> savedMatches[key] = value
                }
            }

            val disabledCollector = CohortBenchmarkService(
                redisCacheService = localRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = testClock
            )
            val enabledCollector = CohortBenchmarkService(
                redisCacheService = localRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = rateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock
            )

            // 1. Disabled config -> 0 network calls
            val cpDisabled = disabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                seedPuuids = listOf("seed-p1", "seed-p2")
            )

            // 2. Enabled config but unverified rate-limit headers -> 0 network calls
            val cpUnverified = enabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                seedPuuids = listOf("seed-p1", "seed-p2")
            )

            // 3. Verify App + Method headers for collector endpoints
            rateLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "2:1,10:120",
                endpointKey = "league-v4-entries-by-puuid",
                methodLimitHeader = "500:10",
                methodCountHeader = "5:10"
            )
            rateLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "2:1,10:120",
                endpointKey = "match-v5-ids-by-puuid",
                methodLimitHeader = "2000:10",
                methodCountHeader = "5:10"
            )
            rateLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "2:1,10:120",
                endpointKey = "match-v5-match-by-id",
                methodLimitHeader = "2000:10",
                methodCountHeader = "5:10"
            )

            // 4. Foreground user request active -> yields immediately with 0 network calls
            rateLimiter.beginUserRequest("euw1")
            val cpForegroundYield = enabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                seedPuuids = listOf("seed-p1", "seed-p2")
            )
            rateLimiter.endUserRequest("euw1")
            // Advance past the 2,000ms foreground yield window
            clockMs.addAndGet(3_000L)

            // 5. Service-level 429 backoff (X-Rate-Limit-Type: service with 0s Retry-After enforces 10s minimum backoff)
            rateLimiter.recordRetryAfter("euw1", 0L, "service")
            val cpService429 = enabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                seedPuuids = listOf("seed-p1", "seed-p2")
            )
            // Advance past the 10s service backoff window
            clockMs.addAndGet(11_000L)

            // 6. Verified & clear: batch 1 processes maxPlayersPerBatch=1 ("seed-p1"), caps at 10 games/player,
            // stops when encountering an older patch (16.6), and discovers "discovered-p3"
            coEvery { riotApiService.fetchLeagueRankLowPriority("seed-p1", "euw1", any(), any(), any()) } returns LeagueEntryDto(
                leagueId = "l1", queueType = "RANKED_SOLO_5x5", tier = "EMERALD", rank = "II",
                summonerName = "P1", puuid = "seed-p1", leaguePoints = 50, wins = 20, losses = 18,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false
            )
            coEvery { riotApiService.fetchLeagueRankLowPriority("seed-p2", "euw1", any(), any(), any()) } returns LeagueEntryDto(
                leagueId = "l2", queueType = "RANKED_SOLO_5x5", tier = "EMERALD", rank = "I",
                summonerName = "P2", puuid = "seed-p2", leaguePoints = 75, wins = 30, losses = 20,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false
            )
            // Return 10 match IDs for seed-p1: first 2 are on 16.7, 3rd is on older patch 16.6 (should stop at 3rd!)
            coEvery {
                riotApiService.getMatchIdsByPuuidLowPriority("seed-p1", "euw1", 420, 10, 0, any(), any(), any())
            } returns (1..10).map { "EUW1_C$it" }
            coEvery {
                riotApiService.getMatchIdsByPuuidLowPriority("seed-p2", "euw1", 420, 10, 0, any(), any(), any())
            } returns listOf("EUW1_C1") // Already cached from batch 1!

            val fetchedMatchCalls = mutableListOf<String>()
            coEvery { riotApiService.getMatchByIdLowPriority(any(), "euw1", any(), any(), any()) } answers {
                val mId = firstArg<String>()
                fetchedMatchCalls.add(mId)
                val idx = mId.removePrefix("EUW1_C").toInt()
                val version = if (idx <= 2) "16.7.600.1234" else "16.6.590.1111"
                MatchDto(
                    metadata = MatchMetadata(mId, listOf("seed-p1", "seed-p2", "discovered-p3")),
                    info = MatchInfo(
                        gameCreation = clockMs.get() - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = version,
                        participants = listOf(
                            buildParticipant(puuid = "seed-p1", role = "TOP", laneCs10 = 84, totalMinions = 210, neutralMinions = 8),
                            buildParticipant(puuid = "seed-p2", role = "MIDDLE", laneCs10 = 92, totalMinions = 230, neutralMinions = 12),
                            buildParticipant(puuid = "discovered-p3", role = "BOTTOM", laneCs10 = 88, totalMinions = 225, neutralMinions = 4)
                        )
                    )
                )
            }

            val cpBatch1 = enabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                seedPuuids = listOf("seed-p1", "seed-p2"),
                maxPlayersPerBatch = 1,
                maxGamesPerPlayer = 10
            )

            // 7. Batch 2 resumes from Redis checkpoint without re-processing "seed-p1" and processes "seed-p2"
            val cpBatch2 = enabledCollector.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.7",
                maxPlayersPerBatch = 1,
                maxGamesPerPlayer = 10
            )

            Then("it should enforce all pre-flight/live rate-limit guardrails, stop on older patches, persist checkpoints, and resume without repeating processed PUUIDs") {
                cpDisabled.lastStopReason shouldBe "NETWORK_COLLECTION_DISABLED_BY_CONFIG"
                cpDisabled.totalNetworkCallsMade shouldBe 0

                cpUnverified.lastStopReason shouldBe "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY"
                cpUnverified.totalNetworkCallsMade shouldBe 0

                cpForegroundYield.lastStopReason shouldBe "PAUSED_FOREGROUND_USER_REQUEST"
                cpForegroundYield.yieldedToForegroundCount shouldBe 1
                cpForegroundYield.totalNetworkCallsMade shouldBe 0

                cpService429.lastStopReason shouldBe "PAUSED_RETRY_AFTER_BACKOFF"
                cpService429.pausedForRetryAfterCount shouldBe 1
                cpService429.totalNetworkCallsMade shouldBe 0

                // Batch 1 processed seed-p1 only, stopped match loop at EUW1_C3 (patch 16.6), and discovered discovered-p3
                cpBatch1.processedPuuids shouldBe setOf("seed-p1")
                cpBatch1.pendingCandidatePuuids.contains("seed-p2") shouldBe true
                cpBatch1.pendingCandidatePuuids.contains("discovered-p3") shouldBe true
                fetchedMatchCalls shouldBe listOf("EUW1_C1", "EUW1_C2", "EUW1_C3")
                cpBatch1.playerCellGameCounts["EMERALD:TOP:seed-p1"] shouldBe 2
                cpBatch1.targetCellsCount shouldBe 30

                // Batch 2 resumed from checkpoint, skipped seed-p1, processed seed-p2 using already-cached EUW1_C1
                cpBatch2.processedPuuids shouldBe setOf("seed-p1", "seed-p2")
                cpBatch2.fetchedRankSnapshots shouldBe 2
                cpBatch2.fetchedMatchIdLists shouldBe 2
                // No additional match detail calls needed for EUW1_C1 because it was cached in Batch 1
                fetchedMatchCalls.size shouldBe 3
                cpBatch2.cellProgress["EMERALD:TOP"]?.eligibleGames shouldBe 2
                cpBatch2.cellProgress["EMERALD:MIDDLE"]?.eligibleGames shouldBe 2
                cpBatch2.cellProgress["EMERALD:TOP"]?.passesQualityGate shouldBe false
            }
        }

        When("controlling the collector via the same-process guarded runner and controller endpoints") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            // Shared in-process rate limiter (same instance used by foreground traffic and collector)
            val sharedRateLimiter = RiotApiRateLimiter(testClock)
            val localRedis = mockk<RedisCacheService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            var lockHeld = false

            every { localRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { localRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { localRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { localRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:active:") }, any(), any()) } answers {
                if (lockHeld) false else {
                    lockHeld = true
                    true
                }
            }
            every { localRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            every { localRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                val value = secondArg<Any>()
                if (value is CohortCollectorCheckpointDto) {
                    savedCheckpoints[key] = value
                }
            }

            val sameProcessService = CohortBenchmarkService(
                redisCacheService = localRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = sharedRateLimiter,
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = testClock
            )
            val runnerLoopback = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = sameProcessService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )
            val runnerTokenProtected = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = sameProcessService,
                configuredOperatorToken = "secret-operator-token",
                timeProvider = testClock
            )
            val controller = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = riotApiService,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = localRedis,
                cohortBenchmarkService = sameProcessService,
                overnightCohortCollectorRunner = runnerLoopback
            )

            // 1. Authorization guard checks: reject external IP and proxied X-Forwarded-For when no token is configured
            val externalReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "203.0.113.42"
            }
            val proxiedLoopbackReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "127.0.0.1"
                addHeader("X-Forwarded-For", "198.51.100.9")
            }
            val directLoopbackReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "127.0.0.1"
            }

            val forbiddenExternal = controller.startOrResumeCohortCollector(
                region = "euw1",
                async = false,
                httpRequest = externalReq
            )
            val forbiddenProxied = controller.getCohortCollectorStatus(
                region = "euw1",
                httpRequest = proxiedLoopbackReq
            )

            // 2. Simulate foreground user traffic in the SAME process populating headers & activeUserRequests
            sharedRateLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "3:1,12:120",
                endpointKey = "league-v4-entries-by-puuid",
                methodLimitHeader = "500:10",
                methodCountHeader = "4:10"
            )
            sharedRateLimiter.beginUserRequest("euw1")
            val liveStatusDuringUserReq = controller.getCohortCollectorStatus(
                region = "euw1",
                httpRequest = directLoopbackReq
            )
            sharedRateLimiter.endUserRequest("euw1")

            // 3. Synchronous start/resume in same process (network OFF by default -> cache-only + checkpoint)
            val startResp = controller.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 5,
                pauseDelayMs = 0L,
                async = false,
                httpRequest = directLoopbackReq
            )

            // 4. Operator stop marks checkpoint as STOPPED_BY_OPERATOR
            val stopResp = controller.stopCohortCollector(
                region = "euw1",
                patch = "16.7",
                httpRequest = directLoopbackReq
            )

            // 5. Invalid region / patch validation without touching Redis
            val invalidRegionResp = runnerLoopback.startOrResumeGuarded(region = "bad_region", async = false)
            val invalidPatchResp = runnerLoopback.startOrResumeGuarded(region = "euw1", targetPatch = "bad-patch", async = false)

            Then("it should enforce loopback/token authorization, share live in-process rate-limit headers and foreground counts, and checkpoint start/stop state cleanly") {
                forbiddenExternal.statusCode shouldBe org.springframework.http.HttpStatus.FORBIDDEN
                forbiddenProxied.statusCode shouldBe org.springframework.http.HttpStatus.FORBIDDEN

                runnerTokenProtected.isAuthorizedOperatorRequest("127.0.0.1", providedToken = null) shouldBe false
                runnerTokenProtected.isAuthorizedOperatorRequest("127.0.0.1", providedToken = "wrong-token") shouldBe false
                runnerTokenProtected.isAuthorizedOperatorRequest("203.0.113.42", providedToken = "secret-operator-token") shouldBe true

                // Same-process status sees live headers and foreground request count from sharedRateLimiter
                val liveBody = liveStatusDuringUserReq.body!!
                liveBody.executionMode shouldBe "SAME_PROCESS_GUARDED"
                liveBody.rateLimitVerification.appLimitsVerifiedFromHeaders shouldBe true
                liveBody.rateLimitVerification.verifiedMethodEndpoints shouldBe setOf("league-v4-entries-by-puuid")
                liveBody.rateLimitVerification.missingRequiredMethodEndpoints shouldBe setOf("match-v5-ids-by-puuid", "match-v5-match-by-id")
                liveBody.rateLimitVerification.activeUserRequests shouldBe 1
                liveBody.rateLimitVerification.foregroundYieldActive shouldBe true

                val startedBody = startResp.body!!
                startedBody.action shouldBe "COMPLETED_SYNCHRONOUS_RUN"
                startedBody.runningInProcess shouldBe false
                startedBody.checkpoint?.lastStopReason shouldBe "NETWORK_COLLECTION_DISABLED_BY_CONFIG"
                startedBody.checkpoint?.totalNetworkCallsMade shouldBe 0

                val stoppedBody = stopResp.body!!
                stoppedBody.action shouldBe "STOPPED"
                stoppedBody.checkpoint?.lastStopReason shouldBe "STOPPED_BY_OPERATOR"

                invalidRegionResp.action shouldBe "INVALID_PARAMETERS"
                invalidPatchResp.action shouldBe "INVALID_PARAMETERS"
            }
        }

        When("running the guarded bounded diagnostic probe from cached EUW seed before enabling bulk network collection") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val probeRateLimiter = RiotApiRateLimiter(testClock)
            val probeRedis = mockk<RedisCacheService>(relaxed = true)
            val probeRiotApi = mockk<RiotApiService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedRankSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            val savedMatches = mutableMapOf<String, MatchDto>()
            var cachedLb: CachedLeaderboardDto? = null
            var probeLockHeld = false

            every { probeRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { probeRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { probeRedis.get(match { it.startsWith("leaderboard:euw1:") }, CachedLeaderboardDto::class.java) } answers {
                cachedLb
            }
            every { probeRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:probe:") }, any(), any()) } answers {
                if (probeLockHeld) false else {
                    probeLockHeld = true
                    true
                }
            }
            every { probeRedis.delete(match { it.startsWith("lock:cohort:collector:probe:") }) } answers {
                probeLockHeld = false
            }
            every { probeRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedRankSnapshots[key] = value
                    is MatchDto -> savedMatches[key] = value
                }
            }

            // Network collection remains OFF (false) while observing real headers via bounded probe
            val probeService = CohortBenchmarkService(
                redisCacheService = probeRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = probeRateLimiter,
                riotApiService = probeRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = testClock
            )
            val probeRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = probeService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )
            val probeController = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = probeRiotApi,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = probeRedis,
                cohortBenchmarkService = probeService,
                overnightCohortCollectorRunner = probeRunner
            )

            val externalReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "198.51.100.77"
            }
            val loopbackReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "127.0.0.1"
            }

            // 1. External caller is rejected with 403 Forbidden
            val forbiddenProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = externalReq
            )

            // 2. No cached seed in Redis -> 0 Riot calls, initializes baseline checkpoint
            val noSeedProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = loopbackReq
            ).body!!

            // 3. Populate cached EUW Challenger leaderboard seed ("leaderboard:euw1::RANKED_SOLO_5x5")
            cachedLb = CachedLeaderboardDto(
                lastRefreshed = nowInstant,
                leaderboard = LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "euw-challenger",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger",
                    entries = listOf(
                        LeagueEntryDto(
                            puuid = "euw-seed-puuid-1",
                            leaguePoints = 1200,
                            rank = "I",
                            wins = 150,
                            losses = 100,
                            veteran = true,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = true,
                            leagueId = "euw-challenger",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "CHALLENGER",
                            summonerName = "Seed1"
                        )
                    )
                )
            )

            // 4. Foreground user request active -> aborts before making any call
            probeRateLimiter.beginUserRequest("euw1")
            val fgPausedProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = loopbackReq
            ).body!!
            probeRateLimiter.endUserRequest("euw1")
            clockMs.addAndGet(3_000L)

            // 5. Service 429 Retry-After active -> aborts before making any call
            probeRateLimiter.recordRetryAfter("euw1", 0L, "service")
            val retryPausedProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = loopbackReq
            ).body!!
            clockMs.addAndGet(11_000L)

            // 6. Clear to probe: mock the 3 low-priority Riot calls to update real headers in probeRateLimiter
            var leagueCalls = 0
            var matchIdCalls = 0
            var matchDetailCalls = 0

            coEvery { probeRiotApi.fetchLeagueRankLowPriority("euw-seed-puuid-1", "euw1", any(), any(), any()) } coAnswers {
                leagueCalls++
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                probeRateLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,1:120",
                    endpointKey = "league-v4-entries-by-puuid",
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
                LeagueEntryDto(
                    leagueId = "l-euw", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", rank = "I",
                    summonerName = "Seed1", puuid = "euw-seed-puuid-1", leaguePoints = 1200, wins = 150, losses = 100,
                    veteran = true, inactive = false, freshBlood = false, hotStreak = true
                )
            }
            coEvery { probeRiotApi.getMatchIdsByPuuidLowPriority("euw-seed-puuid-1", "euw1", 420, 1, 0, any(), any(), any()) } coAnswers {
                matchIdCalls++
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                probeRateLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "2:1,2:120",
                    endpointKey = "match-v5-ids-by-puuid",
                    methodLimitHeader = "2000:10",
                    methodCountHeader = "1:10"
                )
                listOf("EUW1_PROBE_M1")
            }
            coEvery { probeRiotApi.getMatchByIdLowPriority("EUW1_PROBE_M1", "euw1", any(), any(), any()) } coAnswers {
                matchDetailCalls++
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                probeRateLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "3:1,3:120",
                    endpointKey = "match-v5-match-by-id",
                    methodLimitHeader = "2000:10",
                    methodCountHeader = "1:10"
                )
                null
            }

            val verifiedProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = loopbackReq
            ).body!!

            // 7. Second probe call when already verified makes 0 Riot API calls
            val repeatProbe = probeController.probeCohortCollectorRateLimits(
                region = "euw1",
                httpRequest = loopbackReq
            ).body!!

            Then("it should enforce max 1 call per required endpoint, respect foreground and Retry-After pauses, verify all 3 method headers with network still OFF, and make 0 calls once verified") {
                forbiddenProbe.statusCode shouldBe org.springframework.http.HttpStatus.FORBIDDEN

                noSeedProbe.probeStatus shouldBe "NO_CACHED_SEED_PUUID_AVAILABLE"
                noSeedProbe.totalProbeCallsMade shouldBe 0
                noSeedProbe.totalHttpAttemptsMade shouldBe 0
                noSeedProbe.checkpoint shouldNotBe null
                noSeedProbe.checkpoint?.lastStopReason shouldBe "CACHE_BASELINE_INITIALIZED"

                fgPausedProbe.probeStatus shouldBe "PAUSED_FOREGROUND_USER_REQUEST"
                fgPausedProbe.totalProbeCallsMade shouldBe 0
                fgPausedProbe.totalHttpAttemptsMade shouldBe 0

                retryPausedProbe.probeStatus shouldBe "PAUSED_RETRY_AFTER_BACKOFF"
                retryPausedProbe.totalProbeCallsMade shouldBe 0
                retryPausedProbe.totalHttpAttemptsMade shouldBe 0

                verifiedProbe.probeStatus shouldBe "ALL_REQUIRED_HEADERS_VERIFIED"
                verifiedProbe.seedSource shouldBe "CACHED_CHALLENGER_LEADERBOARD"
                verifiedProbe.maxCallsPerEndpoint shouldBe 1
                verifiedProbe.maxHttpAttemptsPerEndpoint shouldBe 1
                verifiedProbe.maxRetriesConfigured shouldBe 0
                verifiedProbe.totalProbeCallsMade shouldBe 3
                verifiedProbe.totalHttpAttemptsMade shouldBe 3
                verifiedProbe.httpAttemptsByEndpoint shouldBe mapOf(
                    "league-v4-entries-by-puuid" to 1,
                    "match-v5-ids-by-puuid" to 1,
                    "match-v5-match-by-id" to 1
                )
                verifiedProbe.stoppedOnRateLimit429 shouldBe false
                leagueCalls shouldBe 1
                matchIdCalls shouldBe 1
                matchDetailCalls shouldBe 1
                verifiedProbe.rateLimitVerification.appLimitsVerifiedFromHeaders shouldBe true
                verifiedProbe.rateLimitVerification.appRateLimitHeader shouldBe "20:1,100:120"
                verifiedProbe.rateLimitVerification.verifiedMethodEndpoints shouldBe setOf(
                    "league-v4-entries-by-puuid",
                    "match-v5-ids-by-puuid",
                    "match-v5-match-by-id"
                )
                verifiedProbe.rateLimitVerification.missingRequiredMethodEndpoints shouldBe emptySet()
                // Bulk network collection remains OFF until operator explicitly enables it
                verifiedProbe.rateLimitVerification.networkCollectionEnabled shouldBe false
                verifiedProbe.rateLimitVerification.readyForLowPriorityNetworkCollection shouldBe false

                repeatProbe.probeStatus shouldBe "ALREADY_VERIFIED_ZERO_CALLS"
                repeatProbe.totalProbeCallsMade shouldBe 0
                repeatProbe.totalHttpAttemptsMade shouldBe 0
                leagueCalls shouldBe 1
                matchIdCalls shouldBe 1
                matchDetailCalls shouldBe 1
            }
        }

        When("diagnostic probe encounters HTTP 429 TooManyRequests or timeout through real RiotApiService.executeRateLimitedCall") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val realLimiter = RiotApiRateLimiter(testClock)
            val mockRestClient = mockk<org.springframework.web.client.RestClient>()
            val mockReqHeadersUriSpec = mockk<org.springframework.web.client.RestClient.RequestHeadersUriSpec<*>>()
            val mockReqHeadersSpec = mockk<org.springframework.web.client.RestClient.RequestHeadersSpec<*>>()
            val mockResponseSpec = mockk<org.springframework.web.client.RestClient.ResponseSpec>()

            every { mockRestClient.get() } returns mockReqHeadersUriSpec
            every { mockReqHeadersUriSpec.uri(any<String>(), *anyVararg()) } returns mockReqHeadersSpec
            every { mockReqHeadersUriSpec.uri(any<String>(), any<Map<String, Any>>()) } returns mockReqHeadersSpec
            every { mockReqHeadersSpec.retrieve() } returns mockResponseSpec

            var rawHttpAttempts = 0
            every {
                mockResponseSpec.toEntity(any<org.springframework.core.ParameterizedTypeReference<List<LeagueEntryDto>>>())
            } answers {
                rawHttpAttempts++
                val headers = org.springframework.http.HttpHeaders().apply {
                    add("X-App-Rate-Limit", "20:1,100:120")
                    add("X-App-Rate-Limit-Count", "20:1,20:120")
                    add("X-Method-Rate-Limit", "500:10")
                    add("X-Method-Rate-Limit-Count", "1:10")
                    add("X-Rate-Limit-Type", "application")
                    add("Retry-After", "8")
                }
                throw org.springframework.web.client.HttpClientErrorException.create(
                    org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,
                    "Too Many Requests",
                    headers,
                    ByteArray(0),
                    null
                )
            }

            // Real RiotApiService instance (exercises executeRateLimitedCall directly!)
            val realRiotApiService = RiotApiService(mockRestClient, realLimiter)
            val probeRedis = mockk<RedisCacheService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            every { probeRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { probeRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { probeRedis.get(any(), CohortRankSnapshotDto::class.java) } returns null
            every { probeRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { probeRedis.get(any(), MatchDto::class.java) } returns null
            every { probeRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { probeRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                val value = secondArg<Any>()
                if (value is CohortCollectorCheckpointDto) {
                    savedCheckpoints[key] = value
                }
            }

            val probeService = CohortBenchmarkService(
                redisCacheService = probeRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = realLimiter,
                riotApiService = realRiotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = testClock
            )

            // 1. Run diagnostic probe when first endpoint returns 429 with Retry-After: 8
            val probeOn429 = probeService.runBoundedDiagnosticRateLimitProbe(
                region = "euw1",
                explicitSeedPuuid = "euw-seed-429-test"
            )

            // 2. Advance past the 8s Retry-After window and test a single-attempt timeout (ResourceAccessException)
            clockMs.addAndGet(9_000L)
            var timeoutHttpAttempts = 0
            every {
                mockResponseSpec.toEntity(any<org.springframework.core.ParameterizedTypeReference<List<String>>>())
            } answers {
                timeoutHttpAttempts++
                throw org.springframework.web.client.ResourceAccessException("Read timed out")
            }
            val probeOnTimeout = probeService.runBoundedDiagnosticRateLimitProbe(
                region = "euw1",
                explicitSeedPuuid = "euw-seed-429-test"
            )

            Then("it should execute exactly 1 HTTP attempt (zero hidden retries), record Retry-After and headers, and immediately stop the probe on the first 429 or timeout") {
                // Exactly 1 HTTP attempt made on RestClient (NOT 4 attempts!)
                rawHttpAttempts shouldBe 1
                probeOn429.probeStatus shouldBe "STOPPED_ON_FIRST_429_RETRY_AFTER"
                probeOn429.stoppedOnRateLimit429 shouldBe true
                probeOn429.maxRetriesConfigured shouldBe 0
                probeOn429.maxHttpAttemptsPerEndpoint shouldBe 1
                probeOn429.totalProbeCallsMade shouldBe 1
                probeOn429.totalHttpAttemptsMade shouldBe 1
                probeOn429.httpAttemptsByEndpoint shouldBe mapOf(
                    "league-v4-entries-by-puuid" to 1,
                    "match-v5-ids-by-puuid" to 0,
                    "match-v5-match-by-id" to 0
                )
                probeOn429.endpointOutcomes["league-v4-entries-by-puuid"] shouldBe "STOPPED_ON_FIRST_429_RETRY_AFTER"
                probeOn429.endpointOutcomes["match-v5-ids-by-puuid"] shouldBe "SKIPPED_DUE_TO_STOPPED_ON_FIRST_429_RETRY_AFTER"
                probeOn429.endpointOutcomes["match-v5-match-by-id"] shouldBe "SKIPPED_DUE_TO_STOPPED_ON_FIRST_429_RETRY_AFTER"
                // Headers and Retry-After from the 429 response were recorded in realLimiter
                probeOn429.rateLimitVerification.appLimitsVerifiedFromHeaders shouldBe true
                probeOn429.rateLimitVerification.verifiedMethodEndpoints shouldBe setOf("league-v4-entries-by-puuid")
                probeOn429.rateLimitVerification.retryAfterUntil shouldNotBe null
                probeOn429.checkpoint?.pausedForRetryAfterCount shouldBe 1
                probeOn429.checkpoint?.lastStopReason shouldBe "PAUSED_RETRY_AFTER_BACKOFF"

                // After Retry-After expires: league-v4-entries-by-puuid is already verified (0 attempts),
                // match-v5-ids-by-puuid times out on attempt 1 with 0 hidden retries and aborts remaining endpoints
                rawHttpAttempts shouldBe 1
                timeoutHttpAttempts shouldBe 1
                probeOnTimeout.probeStatus shouldBe "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                probeOnTimeout.totalHttpAttemptsMade shouldBe 1
                probeOnTimeout.httpAttemptsByEndpoint shouldBe mapOf(
                    "league-v4-entries-by-puuid" to 0,
                    "match-v5-ids-by-puuid" to 1,
                    "match-v5-match-by-id" to 0
                )
                probeOnTimeout.endpointOutcomes["league-v4-entries-by-puuid"] shouldBe "SKIPPED_ALREADY_VERIFIED"
                probeOnTimeout.endpointOutcomes["match-v5-ids-by-puuid"] shouldBe "STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
                probeOnTimeout.endpointOutcomes["match-v5-match-by-id"] shouldBe "SKIPPED_DUE_TO_STOPPED_ON_FIRST_TIMEOUT_NO_RETRY"
            }
        }

        When("overnight runner experiences >20 pause cycles during a long rate-limit window and cooldown auto-resume") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val durableLimiter = RiotApiRateLimiter(testClock)
            val durableRedis = mockk<RedisCacheService>(relaxed = true)
            val durableRiotApi = mockk<RiotApiService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            var lockHeld = false

            every { durableRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { durableRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { durableRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                savedSnapshots[firstArg()]
            }
            every { durableRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { durableRedis.get(any(), MatchDto::class.java) } returns null
            every { durableRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { durableRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:active:") }, any(), any()) } answers {
                if (lockHeld) false else {
                    lockHeld = true
                    true
                }
            }
            every { durableRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            every { durableRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedSnapshots[key] = value
                }
            }

            // Verify all 3 required endpoints in durableLimiter
            for (ep in CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS) {
                durableLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,1:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val durableService = CohortBenchmarkService(
                redisCacheService = durableRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = durableLimiter,
                riotApiService = durableRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock
            )
            val durableRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = durableService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )

            // Seed checkpoint with 2 candidates ("p-durable-1", "p-durable-2")
            durableService.saveCollectorCheckpoint(
                CohortCollectorCheckpointDto(
                    region = "euw1",
                    targetPatch = "16.7",
                    patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                    pendingCandidatePuuids = listOf("p-durable-1", "p-durable-2"),
                    startedAt = nowInstant,
                    updatedAt = nowInstant
                )
            )

            coEvery { durableRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                LeagueEntryDto(
                    leagueId = "l-$pid", queueType = "RANKED_SOLO_5x5", tier = "DIAMOND", rank = "I",
                    summonerName = pid, puuid = pid, leaguePoints = 60, wins = 25, losses = 20,
                    veteran = false, inactive = false, freshBlood = false, hotStreak = false
                )
            }
            coEvery { durableRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                emptyList()
            }

            // Scenario A: Simulate a long 60s Retry-After window where 25 consecutive checks happen before the window clears.
            // Previously, 20 pauses x 2.5s = 50s exhausted maxBatches=20 and silently quit with 0 productive batches!
            durableLimiter.recordRetryAfter("euw1", 60_000L, "application")
            var checkpointSaveCallsDuringPause = 0
            every { durableRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> {
                        savedCheckpoints[key] = value
                        if (value.lastStopReason == "PAUSED_RETRY_AFTER_BACKOFF" && key.endsWith(":16.7")) {
                            checkpointSaveCallsDuringPause++
                            if (checkpointSaveCallsDuringPause == 25) {
                                // After 25 pause cycles (>20!), advance clock past the 60s Retry-After window
                                clockMs.addAndGet(61_000L)
                            }
                        }
                    }
                    is CohortRankSnapshotDto -> savedSnapshots[key] = value
                }
            }

            val statusAfterLongWindow = durableRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 50,
                async = false
            )

            // Scenario B: Re-seed a candidate and set a Retry-After window that outlasts maxPauseCycles=3,
            // verifying the runner saves PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE (not silently done), keeps
            // overnightSessionArmed=true, and auto-resumes via checkAndAutoResumeOvernightSession once the window clears.
            durableService.saveCollectorCheckpoint(
                statusAfterLongWindow.checkpoint!!.copy(
                    pendingCandidatePuuids = listOf("p-durable-3"),
                    lastStopReason = "BATCH_BUDGET_REACHED_RESUMABLE"
                )
            )
            durableLimiter.recordRetryAfter("euw1", 120_000L, "application")

            val statusPausedAtCooldownCap = durableRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 3,
                async = false
            )

            // While still inside the 120s Retry-After window, auto-resume tick is a no-op (returns null)
            val prematureAutoResume = durableRunner.checkAndAutoResumeOvernightSession(async = false)

            // Advance clock past the 120s Retry-After window -> auto-resume tick automatically resumes and finishes p-durable-3!
            clockMs.addAndGet(125_000L)
            val resumedAndFinishedStatus = durableRunner.checkAndAutoResumeOvernightSession(async = false)!!

            Then("pause cycles should never consume productive batch budget, cooldown budget exhaustion should remain armed/resumable, and auto-resume should complete the run once the window clears") {
                // Scenario A: 25 pause cycles did NOT consume the 2 productive batch slots
                statusAfterLongWindow.pauseCyclesCount shouldBe 25
                statusAfterLongWindow.productiveBatchesCompleted shouldBe 2
                statusAfterLongWindow.checkpoint?.processedPuuids shouldBe setOf("p-durable-1", "p-durable-2")
                statusAfterLongWindow.checkpoint?.lastStopReason shouldBe "NO_CANDIDATES_REMAINING"
                statusAfterLongWindow.overnightSessionArmed shouldBe false

                // Scenario B: Hitting maxPauseCycles=3 records explicit PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE and stays armed
                statusPausedAtCooldownCap.productiveBatchesCompleted shouldBe 0
                statusPausedAtCooldownCap.pauseCyclesCount shouldBe 3
                statusPausedAtCooldownCap.checkpoint?.lastStopReason shouldBe "PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE"
                statusPausedAtCooldownCap.overnightSessionArmed shouldBe true

                prematureAutoResume shouldBe null

                // Once the rate-limit window clears, checkAndAutoResumeOvernightSession resumes and completes p-durable-3
                resumedAndFinishedStatus.productiveBatchesCompleted shouldBe 1
                resumedAndFinishedStatus.checkpoint?.processedPuuids shouldBe setOf("p-durable-1", "p-durable-2", "p-durable-3")
                resumedAndFinishedStatus.checkpoint?.lastStopReason shouldBe "NO_CANDIDATES_REMAINING"
                resumedAndFinishedStatus.overnightSessionArmed shouldBe false
            }
        }

        When("enforcing the 512MB container memory headroom safety gate (~67% idle vs >=78% near-cap stop/yield) and streaming non-accumulating cache scans") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val memLimiter = RiotApiRateLimiter(testClock)
            val memRedis = mockk<RedisCacheService>(relaxed = true)
            val memRiotApi = mockk<RiotApiService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            var lockHeld = false

            // Track chunk sizes passed to multiGet to verify bounded non-accumulating streaming scans
            val profileChunkSizes = mutableListOf<Int>()
            val matchChunkSizes = mutableListOf<Int>()
            var bulkTimelineMultiGetCalls = 0

            val profileKeys = (1..45).map { "player:profile:euw1:User$it:EUW" }
            val matchKeys = (1..60).map { "match:euw1:EUW1_STREAM_$it" }

            every { memRedis.scanKeys(any(), any(), any()) } answers {
                val pattern = firstArg<String>()
                when {
                    pattern.startsWith("player:profile:euw1:") -> profileKeys
                    pattern.startsWith("match:euw1:") -> matchKeys
                    pattern.startsWith("cohort:rank:snapshot:euw1:") -> savedSnapshots.keys.toList()
                    else -> emptyList()
                }
            }
            every { memRedis.multiGet(any(), SummonerProfileDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                profileChunkSizes.add(keys.size)
                keys.associateWith { key ->
                    val idx = key.substringAfter("User").substringBefore(":").toIntOrNull() ?: 1
                    SummonerProfileDto(
                        puuid = "stream-puuid-$idx",
                        gameName = "User$idx",
                        tagLine = "EUW",
                        profileIconId = 1,
                        summonerLevel = 100L,
                        soloQueueRank = LeagueEntryDto(
                            leagueId = "l-$idx",
                            queueType = "RANKED_SOLO_5x5",
                            tier = "EMERALD",
                            rank = "II",
                            summonerName = "User$idx",
                            puuid = "stream-puuid-$idx",
                            leaguePoints = 50,
                            wins = 20,
                            losses = 18,
                            veteran = false,
                            inactive = false,
                            freshBlood = false,
                            hotStreak = false
                        ),
                        recentMatches = emptyList(),
                        lastRefreshed = snapshotTime
                    )
                }
            }
            every { memRedis.multiGet(any(), CohortRankSnapshotDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { savedSnapshots[it] }
            }
            every { memRedis.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                matchChunkSizes.add(keys.size)
                keys.associateWith { key ->
                    val idx = key.substringAfterLast("_").toIntOrNull() ?: 1
                    val pId = "stream-puuid-${((idx - 1) % 45) + 1}"
                    MatchDto(
                        metadata = MatchMetadata("EUW1_STREAM_$idx", listOf(pId)),
                        info = MatchInfo(
                            gameCreation = clockMs.get() - idx * 60_000L,
                            gameDuration = 1800L,
                            gameId = idx.toLong(),
                            queueId = 420,
                            gameMode = "CLASSIC",
                            gameVersion = "16.7.600.1234",
                            participants = listOf(
                                buildParticipant(
                                    puuid = pId,
                                    role = "MIDDLE",
                                    laneCs10 = 88,
                                    totalMinions = 220,
                                    neutralMinions = 8
                                )
                            )
                        )
                    )
                }
            }
            every { memRedis.multiGet(any(), MatchTimelineDto::class.java) } answers {
                bulkTimelineMultiGetCalls++
                emptyMap()
            }
            every { memRedis.get(any(), MatchTimelineDto::class.java) } returns null
            every { memRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { memRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                savedSnapshots[firstArg()]
            }
            every { memRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { memRedis.get(any(), MatchDto::class.java) } returns null
            every { memRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { memRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:") }, any(), any()) } answers {
                val key = firstArg<String>()
                if (key.contains(":active:")) {
                    if (lockHeld) false else {
                        lockHeld = true
                        true
                    }
                } else {
                    true
                }
            }
            every { memRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            every { memRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedSnapshots[key] = value
                }
            }

            // Controllable 512MB container memory stats provider:
            // 512MB = 536,870,912 bytes.
            // Idle (~67%): 360,000,000 bytes cgroup used, 180MB heap used / 268MB maxHeap.
            val containerMaxBytes = 536_870_912L
            val currentCgroupBytes = java.util.concurrent.atomic.AtomicLong(360_000_000L) // ~67.06%
            val currentHeapUsedBytes = java.util.concurrent.atomic.AtomicLong(180_000_000L)
            val maxHeapBytes = 268_435_456L // 256MB (-XX:MaxRAMPercentage=50.0)

            val mockMemoryProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = RawMemorySample(
                    jvmHeapUsedBytes = currentHeapUsedBytes.get(),
                    jvmHeapCommittedBytes = 220_000_000L,
                    jvmHeapMaxBytes = maxHeapBytes,
                    jvmNonHeapUsedBytes = 95_000_000L,
                    jvmMetaspaceUsedBytes = 68_000_000L,
                    jvmMetaspaceCommittedBytes = 72_000_000L,
                    jvmCompressedClassSpaceUsedBytes = 8_000_000L,
                    jvmCompressedClassSpaceCommittedBytes = 10_000_000L,
                    jvmCodeCacheUsedBytes = 19_000_000L,
                    jvmCodeCacheCommittedBytes = 22_000_000L,
                    directBufferUsedBytes = 5_000_000L,
                    directBufferCapacityBytes = 8_000_000L,
                    mappedBufferUsedBytes = 0L,
                    jvmThreadCount = 24,
                    cgroupMemoryCurrentBytes = currentCgroupBytes.get(),
                    cgroupMemoryMaxBytes = containerMaxBytes
                )
            }

            for (ep in CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS) {
                memLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,1:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val memService = CohortBenchmarkService(
                redisCacheService = memRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = memLimiter,
                riotApiService = memRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = containerMaxBytes,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L, // 64MB
                memoryStatsProvider = mockMemoryProvider
            )
            val memRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = memService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )

            // 1. At ~67% container idle metric (360MB / 512MB), memory headroom is healthy (< 78% cap, > 64MB free)
            val idleMemorySnap = memService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)

            // 2. Run extractCachedCohortsAndReport and verify bounded streaming chunk sizes (<=20 profiles, <=25 matches, 0 bulk timelines)
            val reportAt67Pct = memService.extractCachedCohortsAndReport(region = "euw1", targetPatch = "16.7")
            val reportProfileChunkSizes = profileChunkSizes.toList()
            val reportMatchChunkSizes = matchChunkSizes.toList()

            // 3. Simulate container memory rising near 512MB cap: 430MB / 512MB = 80.09% (>= 78% cap!)
            currentCgroupBytes.set(430_000_000L)
            val nearCapSnap = memService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)

            // 4. Diagnostic probe under memory pressure (>= 78%) stops immediately with 0 HTTP attempts
            val probePausedForMemory = memService.runBoundedDiagnosticRateLimitProbe(
                region = "euw1",
                targetPatch = "16.7",
                explicitSeedPuuid = "stream-puuid-1"
            )

            // 5. Collector run under memory pressure (>= 78%) pauses immediately with PAUSED_MEMORY_HEADROOM (0 network calls, 0 productive batches consumed)
            memService.saveCollectorCheckpoint(
                CohortCollectorCheckpointDto(
                    region = "euw1",
                    targetPatch = "16.7",
                    patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                    pendingCandidatePuuids = listOf("stream-puuid-1"),
                    startedAt = nowInstant,
                    updatedAt = nowInstant
                )
            )
            val runnerPausedForMemory = memRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 2,
                async = false
            )

            // While memory is still at 80.09%, auto-resume refuses to start
            val autoResumeWhileNearCap = memRunner.checkAndAutoResumeOvernightSession(async = false)

            // 6. Drop container memory back to ~67% idle (360MB / 512MB) -> auto-resume succeeds and finishes candidate!
            currentCgroupBytes.set(360_000_000L)
            coEvery { memRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                LeagueEntryDto(
                    leagueId = "l-mem", queueType = "RANKED_SOLO_5x5", tier = "EMERALD", rank = "II",
                    summonerName = "User1", puuid = "stream-puuid-1", leaguePoints = 50, wins = 20, losses = 18,
                    veteran = false, inactive = false, freshBlood = false, hotStreak = false
                )
            }
            coEvery { memRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                emptyList()
            }
            val autoResumedAfterMemoryRecovery = memRunner.checkAndAutoResumeOvernightSession(async = false)!!

            Then("it should stream profiles/matches in small chunks without bulk timeline loading, stop/yield near the 512MB cap with PAUSED_MEMORY_HEADROOM, and auto-resume once memory drops back to ~67%") {
                // Verify ~67% idle snapshot and pool breakdown
                idleMemorySnap.hasHeadroomForLowPriority shouldBe true
                idleMemorySnap.nearCapStopTriggered shouldBe false
                (idleMemorySnap.effectiveUtilizationRatio in 0.66..0.68) shouldBe true
                idleMemorySnap.jvmHeapUnusedCommittedBytes shouldBe 40_000_000L
                idleMemorySnap.jvmMetaspaceUsedBytes shouldBe 68_000_000L
                idleMemorySnap.jvmCompressedClassSpaceUsedBytes shouldBe 8_000_000L
                idleMemorySnap.jvmCodeCacheUsedBytes shouldBe 19_000_000L
                idleMemorySnap.directBufferUsedBytes shouldBe 5_000_000L
                idleMemorySnap.jvmThreadCount shouldBe 24
                // 360MB cgroup - 220MB heapCommitted - 95MB nonHeap - 5MB direct = 40MB native residual
                idleMemorySnap.estimatedNativeResidualBytes shouldBe 40_000_000L

                // Verify streaming chunk bounds: 45 profiles -> [10, 10, 10, 10, 5]; 60 matches -> [10, 10, 10, 10, 10, 10]; 0 bulk timeline multiGets
                reportProfileChunkSizes shouldBe listOf(10, 10, 10, 10, 5)
                reportMatchChunkSizes shouldBe listOf(10, 10, 10, 10, 10, 10)
                profileChunkSizes.all { it <= CohortBenchmarkService.PROFILE_SCAN_CHUNK_SIZE } shouldBe true
                matchChunkSizes.all { it <= CohortBenchmarkService.MATCH_SCAN_CHUNK_SIZE } shouldBe true
                bulkTimelineMultiGetCalls shouldBe 0
                reportAt67Pct.memoryHeadroom?.hasHeadroomForLowPriority shouldBe true
                reportAt67Pct.inventory.distinctEligibleCohortPlayers shouldBe 45

                // Verify near-cap (>= 78%) snapshot triggers stop/yield
                nearCapSnap.hasHeadroomForLowPriority shouldBe false
                nearCapSnap.nearCapStopTriggered shouldBe true
                (nearCapSnap.effectiveUtilizationRatio >= 0.78) shouldBe true

                // Verify probe stops with 0 HTTP attempts when near memory cap
                probePausedForMemory.probeStatus shouldBe "PAUSED_MEMORY_HEADROOM"
                probePausedForMemory.totalHttpAttemptsMade shouldBe 0
                probePausedForMemory.memoryHeadroom?.nearCapStopTriggered shouldBe true

                // Verify runner pauses without consuming productive batch budget when near memory cap
                runnerPausedForMemory.productiveBatchesCompleted shouldBe 0
                runnerPausedForMemory.pauseCyclesCount shouldBe 2
                runnerPausedForMemory.overnightSessionArmed shouldBe true
                runnerPausedForMemory.checkpoint?.pausedForMemoryHeadroomCount shouldBe 2
                runnerPausedForMemory.checkpoint?.lastStopReason shouldBe "PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE"

                // Auto-resume stays blocked while memory is at 80%
                autoResumeWhileNearCap shouldBe null

                // Once memory recovers to ~67%, auto-resume runs and completes stream-puuid-1
                autoResumedAfterMemoryRecovery.productiveBatchesCompleted shouldBe 1
                autoResumedAfterMemoryRecovery.checkpoint?.processedPuuids shouldBe setOf("stream-puuid-1")
                autoResumedAfterMemoryRecovery.checkpoint?.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
            }
        }

        When("persisting collector checkpoints to Postgres and recovering after a Redis flush") {
            val pgRedis = mockk<RedisCacheService>(relaxed = true)
            val pgCheckpointRepo = mockk<CohortCollectorCheckpointRepository>(relaxed = true)
            val redisStore = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val pgStore = mutableMapOf<String, CohortCollectorCheckpointEntity>()

            every { pgRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                val value = secondArg<Any>()
                if (value is CohortCollectorCheckpointDto) {
                    redisStore[key] = value
                }
            }
            every { pgRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                redisStore[firstArg()]
            }
            every { pgCheckpointRepo.findByRegionAndTargetPatchAndQueueId(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgStore["$r:$p:$q"]
            }
            every { pgCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(any(), any()) } answers {
                val r = firstArg<String>()
                val q = secondArg<Int>()
                pgStore.values.filter { it.region == r && it.queueId == q }.maxByOrNull { it.updatedAt }
            }
            every { pgCheckpointRepo.save(any<CohortCollectorCheckpointEntity>()) } answers {
                val ent = firstArg<CohortCollectorCheckpointEntity>()
                pgStore["${ent.region}:${ent.targetPatch}:${ent.queueId}"] = ent
                ent
            }

            val pgService = CohortBenchmarkService(
                redisCacheService = pgRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = RiotApiRateLimiter(fixedTimeProvider),
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = fixedTimeProvider,
                cohortCollectorCheckpointRepository = pgCheckpointRepo
            )

            val savedCp = CohortCollectorCheckpointDto(
                region = "euw1",
                targetPatch = "16.7",
                patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                processedPuuids = setOf("puuid-a", "puuid-b"),
                pendingCandidatePuuids = listOf("puuid-c"),
                processedMatchIds = setOf("EUW1_101", "EUW1_102"),
                playerCellGameCounts = mapOf("EMERALD:TOP:puuid-a" to 4),
                totalNetworkCallsMade = 12,
                fetchedRankSnapshots = 2,
                fetchedMatchIdLists = 2,
                fetchedMatches = 8,
                lastStopReason = "BATCH_BUDGET_REACHED_RESUMABLE",
                startedAt = nowInstant,
                updatedAt = nowInstant
            )
            pgService.saveCollectorCheckpoint(savedCp)

            // Simulate complete Redis cache eviction / restart flush
            redisStore.clear()
            val recoveredExplicitPatch = pgService.getCollectorCheckpoint("euw1", "16.7")
            val rewarmedInRedisAfterExplicit = redisStore["cohort:collector:checkpoint:euw1:16.7"]

            redisStore.clear()
            val recoveredLatestPatch = pgService.getCollectorCheckpoint("euw1", null)

            Then("it should write through to cohort_collector_checkpoints in Postgres, recover full checkpoint state on Redis miss, and re-warm Redis") {
                pgStore["euw1:16.7:420"] shouldNotBe null
                pgStore["euw1:latest:420"] shouldNotBe null
                pgStore["euw1:16.7:420"]?.processedPuuidsCount shouldBe 2
                pgStore["euw1:16.7:420"]?.pendingCandidatesCount shouldBe 1
                pgStore["euw1:16.7:420"]?.totalNetworkCallsMade shouldBe 12
                pgStore["euw1:16.7:420"]?.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"

                recoveredExplicitPatch shouldNotBe null
                recoveredExplicitPatch?.processedPuuids shouldBe setOf("puuid-a", "puuid-b")
                recoveredExplicitPatch?.pendingCandidatePuuids shouldBe listOf("puuid-c")
                recoveredExplicitPatch?.playerCellGameCounts?.get("EMERALD:TOP:puuid-a") shouldBe 4
                rewarmedInRedisAfterExplicit?.totalNetworkCallsMade shouldBe 12

                recoveredLatestPatch shouldNotBe null
                recoveredLatestPatch?.targetPatch shouldBe "16.7"
                recoveredLatestPatch?.processedMatchIds shouldBe setOf("EUW1_101", "EUW1_102")
            }
        }

        When("enforcing session-level hard caps (maxTotalProductiveBatches and maxTotalNetworkCalls) across auto-resume ticks") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val capLimiter = RiotApiRateLimiter(testClock)
            val capRedis = mockk<RedisCacheService>(relaxed = true)
            val capRiotApi = mockk<RiotApiService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            var lockHeld = false

            every { capRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { capRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { capRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                savedSnapshots[firstArg()]
            }
            every { capRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { capRedis.get(any(), MatchDto::class.java) } returns null
            every { capRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { capRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:") }, any(), any()) } answers {
                val key = firstArg<String>()
                if (key.contains(":active:")) {
                    if (lockHeld) false else {
                        lockHeld = true
                        true
                    }
                } else {
                    true
                }
            }
            every { capRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            every { capRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedSnapshots[key] = value
                }
            }

            val capMemoryProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = RawMemorySample(
                    jvmHeapUsedBytes = 180_000_000L,
                    jvmHeapCommittedBytes = 220_000_000L,
                    jvmHeapMaxBytes = 268_435_456L,
                    jvmNonHeapUsedBytes = 95_000_000L,
                    jvmMetaspaceUsedBytes = 68_000_000L,
                    jvmMetaspaceCommittedBytes = 72_000_000L,
                    jvmCompressedClassSpaceUsedBytes = 8_000_000L,
                    jvmCompressedClassSpaceCommittedBytes = 10_000_000L,
                    jvmCodeCacheUsedBytes = 19_000_000L,
                    jvmCodeCacheCommittedBytes = 22_000_000L,
                    directBufferUsedBytes = 5_000_000L,
                    directBufferCapacityBytes = 8_000_000L,
                    mappedBufferUsedBytes = 0L,
                    jvmThreadCount = 24,
                    cgroupMemoryCurrentBytes = 360_000_000L,
                    cgroupMemoryMaxBytes = 536_870_912L
                )
            }

            CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS.forEach { ep ->
                capLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,5:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val capService = CohortBenchmarkService(
                redisCacheService = capRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = capLimiter,
                riotApiService = capRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = 536_870_912L,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L,
                memoryStatsProvider = capMemoryProvider
            )
            val capRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = capService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )

            capService.saveCollectorCheckpoint(
                CohortCollectorCheckpointDto(
                    region = "euw1",
                    targetPatch = "16.7",
                    patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                    pendingCandidatePuuids = listOf("cap-p-1", "cap-p-2", "cap-p-3", "cap-p-4", "cap-p-5", "cap-p-6"),
                    startedAt = nowInstant,
                    updatedAt = nowInstant
                )
            )

            val totalRiotHttpCalls = java.util.concurrent.atomic.AtomicInteger(0)
            coEvery { capRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                totalRiotHttpCalls.incrementAndGet()
                LeagueEntryDto(
                    leagueId = "l-$pid", queueType = "RANKED_SOLO_5x5", tier = "DIAMOND", rank = "I",
                    summonerName = pid, puuid = pid, leaguePoints = 60, wins = 25, losses = 20,
                    veteran = false, inactive = false, freshBlood = false, hotStreak = false
                )
            }
            coEvery { capRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                totalRiotHttpCalls.incrementAndGet()
                if (pid == "cap-p-3") {
                    // Simulate a 60s Retry-After window starting right after cap-p-3 completes so the chunk pauses before cap-p-4
                    capLimiter.recordRetryAfter("euw1", 60_000L, "application")
                }
                emptyList()
            }

            // Part 1: Start with per-chunk maxBatches=1 and session cap maxTotalProductiveBatches=2 (6 candidates available).
            val chunk1Status = capRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 3,
                maxTotalProductiveBatches = 2,
                maxTotalNetworkCalls = 60,
                async = false
            )
            // Auto-resume tick #1 runs the 2nd productive batch (sessionProductiveBatchesCompleted reaches 2/2) -> disarms and stops with SESSION_BATCH_CAP_REACHED!
            val autoResumeBatch2Status = capRunner.checkAndAutoResumeOvernightSession(async = false)!!
            // Subsequent auto-resume ticks (#2 and scheduledAutoResumeTick) must be blocked and make 0 additional calls!
            val autoResumeBlockedAfterBatchCap = capRunner.checkAndAutoResumeOvernightSession(async = false)
            capRunner.scheduledAutoResumeTick()
            val callsAfterBatchCap = totalRiotHttpCalls.get()

            // Part 2: Start a new session on the existing checkpoint (baseline=4 calls) with maxTotalNetworkCalls=3 and maxTotalProductiveBatches=5.
            // cap-p-3 consumes 2 calls (sessionNetworkCallsMade=2/3) and triggers a 60s Retry-After pause before cap-p-4.
            val netCapChunk1Status = capRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 2,
                maxTotalProductiveBatches = 5,
                maxTotalNetworkCalls = 3,
                async = false
            )
            // Advance clock past the 60s Retry-After window -> auto-resume tick resumes with 1 remaining call in the 3-call session cap!
            clockMs.addAndGet(65_000L)
            val netCapAutoResumedStatus = capRunner.checkAndAutoResumeOvernightSession(async = false)!!
            val autoResumeBlockedAfterNetCap = capRunner.checkAndAutoResumeOvernightSession(async = false)
            capRunner.scheduledAutoResumeTick()
            val callsAfterNetCap = totalRiotHttpCalls.get()

            Then("auto-resume should never reset session counters, should disarm and persist SESSION_BATCH_CAP_REACHED or SESSION_NETWORK_CALL_CAP_REACHED when reached, and should make zero calls past the cap") {
                // Part 1 assertions: maxTotalProductiveBatches = 2
                chunk1Status.productiveBatchesCompleted shouldBe 1
                chunk1Status.sessionProductiveBatchesCompleted shouldBe 1
                chunk1Status.overnightSessionArmed shouldBe true
                chunk1Status.checkpoint?.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"

                autoResumeBatch2Status.productiveBatchesCompleted shouldBe 1
                autoResumeBatch2Status.sessionProductiveBatchesCompleted shouldBe 2
                autoResumeBatch2Status.maxSessionProductiveBatchesConfigured shouldBe 2
                autoResumeBatch2Status.overnightSessionArmed shouldBe false
                autoResumeBatch2Status.checkpoint?.lastStopReason shouldBe "SESSION_BATCH_CAP_REACHED"
                autoResumeBatch2Status.checkpoint?.sessionProductiveBatchesCompleted shouldBe 2
                autoResumeBatch2Status.checkpoint?.maxSessionProductiveBatches shouldBe 2
                autoResumeBatch2Status.checkpoint?.processedPuuids shouldBe setOf("cap-p-1", "cap-p-2")

                autoResumeBlockedAfterBatchCap shouldBe null
                callsAfterBatchCap shouldBe 4

                // Part 2 assertions: maxTotalNetworkCalls = 3 (starting from baseline=4 -> ceiling=7 total calls)
                netCapChunk1Status.productiveBatchesCompleted shouldBe 0
                netCapChunk1Status.sessionProductiveBatchesCompleted shouldBe 0
                netCapChunk1Status.sessionNetworkCallsMade shouldBe 2
                netCapChunk1Status.overnightSessionArmed shouldBe true
                netCapChunk1Status.checkpoint?.lastStopReason shouldBe "PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE"

                netCapAutoResumedStatus.overnightSessionArmed shouldBe false
                netCapAutoResumedStatus.sessionNetworkCallsMade shouldBe 3
                netCapAutoResumedStatus.maxSessionNetworkCallsConfigured shouldBe 3
                netCapAutoResumedStatus.checkpoint?.lastStopReason shouldBe "SESSION_NETWORK_CALL_CAP_REACHED"
                netCapAutoResumedStatus.checkpoint?.sessionNetworkCallsMade shouldBe 3
                netCapAutoResumedStatus.checkpoint?.maxSessionNetworkCalls shouldBe 3
                netCapAutoResumedStatus.checkpoint?.totalNetworkCallsMade shouldBe 7

                autoResumeBlockedAfterNetCap shouldBe null
                callsAfterNetCap shouldBe 7
            }
        }

        When("a low-priority Riot API call throws an exception (5xx or non-429 error) at any of the 3 call sites") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val errLimiter = RiotApiRateLimiter(testClock)
            val errRedis = mockk<RedisCacheService>(relaxed = true)
            val errRiotApi = mockk<RiotApiService>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            var lockHeld = false

            every { errRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { errRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { errRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                savedSnapshots[firstArg()]
            }
            every { errRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { errRedis.get(any(), MatchDto::class.java) } returns null
            every { errRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { errRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:") }, any(), any()) } answers {
                val key = firstArg<String>()
                if (key.contains(":active:")) {
                    if (lockHeld) false else {
                        lockHeld = true
                        true
                    }
                } else {
                    true
                }
            }
            every { errRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            every { errRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is CohortRankSnapshotDto -> savedSnapshots[key] = value
                }
            }

            val errMemoryProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = RawMemorySample(
                    jvmHeapUsedBytes = 180_000_000L,
                    jvmHeapCommittedBytes = 220_000_000L,
                    jvmHeapMaxBytes = 268_435_456L,
                    jvmNonHeapUsedBytes = 95_000_000L,
                    jvmMetaspaceUsedBytes = 68_000_000L,
                    jvmMetaspaceCommittedBytes = 72_000_000L,
                    jvmCompressedClassSpaceUsedBytes = 8_000_000L,
                    jvmCompressedClassSpaceCommittedBytes = 10_000_000L,
                    jvmCodeCacheUsedBytes = 19_000_000L,
                    jvmCodeCacheCommittedBytes = 22_000_000L,
                    directBufferUsedBytes = 5_000_000L,
                    directBufferCapacityBytes = 8_000_000L,
                    mappedBufferUsedBytes = 0L,
                    jvmThreadCount = 24,
                    cgroupMemoryCurrentBytes = 360_000_000L,
                    cgroupMemoryMaxBytes = 536_870_912L
                )
            }

            CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS.forEach { ep ->
                errLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,5:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val errService = CohortBenchmarkService(
                redisCacheService = errRedis,
                cohortBenchmarkCellRepository = cohortRepo,
                riotApiRateLimiter = errLimiter,
                riotApiService = errRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = 536_870_912L,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L,
                memoryStatsProvider = errMemoryProvider
            )
            val errRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = errService,
                configuredOperatorToken = "",
                timeProvider = testClock
            )

            errService.saveCollectorCheckpoint(
                CohortCollectorCheckpointDto(
                    region = "euw1",
                    targetPatch = "16.7",
                    patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                    pendingCandidatePuuids = listOf("err-p-1", "err-p-2"),
                    startedAt = nowInstant,
                    updatedAt = nowInstant
                )
            )

            val totalHttpAttempts = java.util.concurrent.atomic.AtomicInteger(0)

            // Site 1: fetchLeagueRankLowPriority throws 502 Bad Gateway (with maxTotalNetworkCalls = 1)
            coEvery { errRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                totalHttpAttempts.incrementAndGet()
                throw RuntimeException("502 Bad Gateway from Riot league-v4")
            }
            val site1Status = errRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 2,
                pauseDelayMs = 0L,
                maxPauseCycles = 2,
                maxTotalProductiveBatches = 4,
                maxTotalNetworkCalls = 1,
                async = false
            )
            val site1AutoResume = errRunner.checkAndAutoResumeOvernightSession(async = false)
            errRunner.scheduledAutoResumeTick()
            val callsAfterSite1 = totalHttpAttempts.get()

            // Site 2: fetchLeagueRankLowPriority succeeds (call 1), getMatchIdsByPuuidLowPriority throws 503 (call 2, maxTotalNetworkCalls = 2)
            coEvery { errRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                totalHttpAttempts.incrementAndGet()
                LeagueEntryDto(
                    leagueId = "l-$pid", queueType = "RANKED_SOLO_5x5", tier = "DIAMOND", rank = "I",
                    summonerName = pid, puuid = pid, leaguePoints = 60, wins = 25, losses = 20,
                    veteran = false, inactive = false, freshBlood = false, hotStreak = false
                )
            }
            coEvery { errRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                totalHttpAttempts.incrementAndGet()
                throw RuntimeException("503 Service Unavailable from Riot match-v5 ids")
            }
            val site2Status = errRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 2,
                pauseDelayMs = 0L,
                maxPauseCycles = 2,
                maxTotalProductiveBatches = 4,
                maxTotalNetworkCalls = 2,
                async = false
            )
            val site2AutoResume = errRunner.checkAndAutoResumeOvernightSession(async = false)
            errRunner.scheduledAutoResumeTick()
            val callsAfterSite2 = totalHttpAttempts.get()

            // Site 3: rank is now cached for err-p-1; getMatchIdsByPuuidLowPriority returns 1 match (call 1), getMatchByIdLowPriority throws 500 (call 2, maxTotalNetworkCalls = 2)
            coEvery { errRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                totalHttpAttempts.incrementAndGet()
                listOf("EUW1_ERR_MATCH_1")
            }
            coEvery { errRiotApi.getMatchByIdLowPriority("EUW1_ERR_MATCH_1", "euw1", any(), any(), any()) } coAnswers {
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                totalHttpAttempts.incrementAndGet()
                throw RuntimeException("500 Internal Server Error from Riot match-v5 detail")
            }
            val site3Status = errRunner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.7",
                maxBatches = 2,
                batchSize = 2,
                pauseDelayMs = 0L,
                maxPauseCycles = 2,
                maxTotalProductiveBatches = 4,
                maxTotalNetworkCalls = 2,
                async = false
            )
            val site3AutoResume = errRunner.checkAndAutoResumeOvernightSession(async = false)
            errRunner.scheduledAutoResumeTick()
            val callsAfterSite3 = totalHttpAttempts.get()

            Then("it should count the failed attempt in finally at all 3 call sites, hit the session cap, persist SESSION_ERROR_ABORT, disarm, and make 0 calls on auto-resume ticks") {
                // Site 1: rank fetch threw on attempt 1 -> counted in finally (1/1 cap hit), persisted SESSION_ERROR_ABORT, disarmed, tick made 0 calls
                site1Status.overnightSessionArmed shouldBe false
                site1Status.sessionNetworkCallsMade shouldBe 1
                site1Status.maxSessionNetworkCallsConfigured shouldBe 1
                site1Status.checkpoint?.totalNetworkCallsMade shouldBe 1
                site1Status.checkpoint?.sessionNetworkCallsMade shouldBe 1
                site1Status.checkpoint?.lastStopReason shouldBe "SESSION_ERROR_ABORT"
                site1AutoResume shouldBe null
                callsAfterSite1 shouldBe 1

                // Site 2: rank fetch succeeded (1) + match IDs fetch threw (2) -> counted in finally (2/2 cap hit, total=3), persisted SESSION_ERROR_ABORT, disarmed, tick made 0 calls
                site2Status.overnightSessionArmed shouldBe false
                site2Status.sessionNetworkCallsMade shouldBe 2
                site2Status.maxSessionNetworkCallsConfigured shouldBe 2
                site2Status.checkpoint?.totalNetworkCallsMade shouldBe 3
                site2Status.checkpoint?.sessionNetworkCallsMade shouldBe 2
                site2Status.checkpoint?.lastStopReason shouldBe "SESSION_ERROR_ABORT"
                site2AutoResume shouldBe null
                callsAfterSite2 shouldBe 3

                // Site 3: match IDs fetch succeeded (1) + match detail fetch threw (2) -> counted in finally (2/2 cap hit, total=5), persisted SESSION_ERROR_ABORT, disarmed, tick made 0 calls
                site3Status.overnightSessionArmed shouldBe false
                site3Status.sessionNetworkCallsMade shouldBe 2
                site3Status.maxSessionNetworkCallsConfigured shouldBe 2
                site3Status.checkpoint?.totalNetworkCallsMade shouldBe 5
                site3Status.checkpoint?.sessionNetworkCallsMade shouldBe 2
                site3Status.checkpoint?.lastStopReason shouldBe "SESSION_ERROR_ABORT"
                site3AutoResume shouldBe null
                callsAfterSite3 shouldBe 5
            }
        }

        When("collector accumulates de-identified per-cell aggregates durably in Postgres across simulated Redis evictions, deduplicates matches, enforces the 10-game cap, and refuses non-current patches") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val evictLimiter = RiotApiRateLimiter(testClock)
            val evictRedis = mockk<RedisCacheService>(relaxed = true)
            val evictRiotApi = mockk<RiotApiService>(relaxed = true)
            val pgCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val pgSampleRepo = mockk<com.pride.summoner_searcher_api.repository.CohortCellSampleStateRepository>(relaxed = true)
            val pgCheckpointRepo = mockk<CohortCollectorCheckpointRepository>(relaxed = true)

            // Volatile Redis store (simulated 25 MB allkeys-lru where matches/snapshots/checkpoints can be evicted at any time)
            val redisCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val redisSnapshots = mutableMapOf<String, CohortRankSnapshotDto>()
            val redisMatches = mutableMapOf<String, MatchDto>()
            val redisBenchmarkCells = mutableMapOf<String, Array<CohortBenchmarkCellDto>>()
            val redisReports = mutableMapOf<String, CohortExtractionReportDto>()

            // Durable Postgres stores (cohort_benchmark_cells, cohort_cell_sample_states, cohort_collector_checkpoints)
            val pgBenchmarkCells = mutableMapOf<String, CohortBenchmarkCellEntity>()
            val pgSampleStates = mutableMapOf<String, com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
            val pgCheckpoints = mutableMapOf<String, CohortCollectorCheckpointEntity>()
            var lockHeld = false

            every { evictRedis.scanKeys(any(), any(), any()) } answers {
                val pattern = firstArg<String>()
                when {
                    pattern.startsWith("cohort:rank:snapshot:euw1:") -> redisSnapshots.keys.toList()
                    pattern.startsWith("match:euw1:") -> redisMatches.keys.toList()
                    else -> emptyList()
                }
            }
            every { evictRedis.multiGet(any(), CohortRankSnapshotDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { redisSnapshots[it] }
            }
            every { evictRedis.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { redisMatches[it] }
            }
            every { evictRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                redisCheckpoints[firstArg()]
            }
            every { evictRedis.get(any(), CohortRankSnapshotDto::class.java) } answers {
                redisSnapshots[firstArg()]
            }
            every { evictRedis.get(any(), MatchDto::class.java) } answers {
                redisMatches[firstArg()]
            }
            every { evictRedis.get(any(), Array<CohortBenchmarkCellDto>::class.java) } answers {
                redisBenchmarkCells[firstArg()]
            }
            every { evictRedis.get(any(), CohortExtractionReportDto::class.java) } answers {
                redisReports[firstArg()]
            }
            every { evictRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { evictRedis.get(any(), CachedLeaderboardDto::class.java) } returns null
            every { evictRedis.setIfAbsent(any(), any(), any()) } answers {
                val key = firstArg<String>()
                if (key.startsWith("lock:cohort:collector:active:")) {
                    if (lockHeld) false else {
                        lockHeld = true
                        true
                    }
                } else {
                    true
                }
            }
            every { evictRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }
            @Suppress("UNCHECKED_CAST")
            every { evictRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> redisCheckpoints[key] = value
                    is CohortRankSnapshotDto -> redisSnapshots[key] = value
                    is MatchDto -> redisMatches[key] = value
                    is List<*> -> {
                        val list = value.filterIsInstance<CohortBenchmarkCellDto>()
                        if (list.isNotEmpty()) {
                            redisBenchmarkCells[key] = list.toTypedArray()
                        }
                    }
                    is CohortExtractionReportDto -> redisReports[key] = value
                }
            }

            // Wire durable Postgres repositories
            every { pgCellRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                val t = arg<String>(3)
                val rl = arg<String>(4)
                pgBenchmarkCells["$r:$p:$q:$t:$rl"]
            }
            every { pgCellRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgBenchmarkCells.values.filter { it.region == r && it.patch == p && it.queueId == q }
            }
            every { pgCellRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) } answers {
                val r = firstArg<String>()
                pgBenchmarkCells.values.filter { it.region == r }
            }
            every { pgCellRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                pgBenchmarkCells.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { pgCellRepo.save(any<CohortBenchmarkCellEntity>()) } answers {
                val ent = firstArg<CohortBenchmarkCellEntity>()
                pgBenchmarkCells["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }

            every { pgSampleRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                val t = arg<String>(3)
                val rl = arg<String>(4)
                pgSampleStates["$r:$p:$q:$t:$rl"]
            }
            every { pgSampleRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgSampleStates.values.filter { it.region == r && it.patch == p && it.queueId == q }
            }
            every { pgSampleRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) } answers {
                val r = firstArg<String>()
                pgSampleStates.values.filter { it.region == r }
            }
            every { pgSampleRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                pgSampleStates.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { pgSampleRepo.save(any<com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()) } answers {
                val ent = firstArg<com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
                pgSampleStates["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }

            every { pgCheckpointRepo.findByRegionAndTargetPatchAndQueueId(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgCheckpoints["$r:$p:$q"]
            }
            every { pgCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(any(), any()) } answers {
                val r = firstArg<String>()
                val q = secondArg<Int>()
                pgCheckpoints.values.filter { it.region == r && it.queueId == q }.maxByOrNull { it.updatedAt }
            }
            every { pgCheckpointRepo.save(any<CohortCollectorCheckpointEntity>()) } answers {
                val ent = firstArg<CohortCollectorCheckpointEntity>()
                pgCheckpoints["${ent.region}:${ent.targetPatch}:${ent.queueId}"] = ent
                ent
            }

            for (ep in CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS) {
                evictLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,2:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val durableService = CohortBenchmarkService(
                redisCacheService = evictRedis,
                cohortBenchmarkCellRepository = pgCellRepo,
                riotApiRateLimiter = evictLimiter,
                riotApiService = evictRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = 536_870_912L,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L,
                cohortCollectorCheckpointRepository = pgCheckpointRepo,
                cohortCellSampleStateRepository = pgSampleRepo
            )
            val controller = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = evictRiotApi,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = evictRedis,
                cohortBenchmarkService = durableService,
                overnightCohortCollectorRunner = null
            )

            // 50 distinct DIAMOND TOP players:
            // - Player 1 ("dia-top-1") plays 12 matches (EUW1_D_1_1..EUW1_D_1_12) + 1 duplicate match ID ("EUW1_D_1_1")
            //   to prove match deduplication and the 10-game per-player cap (10 kept, 2 excludedOverPlayerCapGames).
            // - Players 2..50 ("dia-top-2".."dia-top-50") play 4 matches each (49 * 4 = 196 games).
            // Total eligible games in DIAMOND:TOP = 10 + 196 = 206 (>= 200) across 50 distinct players (>= 50) -> passes quality gate!
            val allPlayers = (1..50).map { "dia-top-$it" }
            every { evictRiotApi.fetchDataDragonVersions(any()) } returns listOf("16.22.1", "16.21.1")
            coEvery { evictRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                LeagueEntryDto(
                    leagueId = "l-$pid", queueType = "RANKED_SOLO_5x5", tier = "DIAMOND", rank = "I",
                    summonerName = pid, puuid = pid, leaguePoints = 75, wins = 40, losses = 30,
                    veteran = false, inactive = false, freshBlood = false, hotStreak = false
                )
            }
            coEvery { evictRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } coAnswers {
                val pid = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(6)?.incrementAndGet()
                val pIdx = pid.substringAfter("dia-top-").toInt()
                if (pIdx == 1) {
                    // Return 10 matches in collector list (plus we will also feed 2 more via extractCachedCohortsAndReport + duplicate)
                    (1..10).map { "EUW1_D_1_$it" }
                } else {
                    (1..4).map { "EUW1_D_${pIdx}_$it" }
                }
            }
            coEvery { evictRiotApi.getMatchByIdLowPriority(any(), "euw1", any(), any(), any()) } coAnswers {
                val mId = firstArg<String>()
                arg<java.util.concurrent.atomic.AtomicInteger?>(3)?.incrementAndGet()
                val parts = mId.removePrefix("EUW1_D_").split("_")
                val pIdx = parts[0].toInt()
                val gIdx = parts[1].toInt()
                val pid = "dia-top-$pIdx"
                MatchDto(
                    metadata = MatchMetadata(mId, listOf(pid)),
                    info = MatchInfo(
                        gameCreation = clockMs.get() - (pIdx * 100 + gIdx) * 1_000L,
                        gameDuration = 1800L,
                        gameId = (pIdx * 1000 + gIdx).toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.22.615.9999",
                        participants = listOf(
                            buildParticipant(
                                puuid = pid,
                                role = "TOP",
                                laneCs10 = 80 + (gIdx % 5),
                                totalMinions = 210 + gIdx,
                                neutralMinions = 8
                            )
                        )
                    )
                )
            }

            // Pre-seed an older checkpoint pinned to stale patch "16.21" while Redis has 0 matches initially:
            // Data Dragon versions.json ("16.22.1") grounds the current patch to "16.22" before any 16.22 match is stored!
            durableService.saveCollectorCheckpoint(
                CohortCollectorCheckpointDto(
                    region = "euw1",
                    targetPatch = "16.21",
                    patchSelectionSource = "EXPLICIT_TARGET_PATCH",
                    pendingCandidatePuuids = allPlayers,
                    startedAt = nowInstant,
                    updatedAt = nowInstant
                )
            )

            // Batch 1 (processes players 1..25 on auto-grounded patch 16.22), then simulate complete Redis allkeys-lru eviction!
            val cpBatch1 = durableService.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = null,
                maxPlayersPerBatch = 25,
                maxGamesPerPlayer = 10
            )
            // Simulate Free 25 MB Redis allkeys-lru evicting all matches, rank snapshots, benchmark cache, and reports!
            redisMatches.clear()
            redisSnapshots.clear()
            redisBenchmarkCells.clear()
            redisReports.clear()

            // Batch 2 (processes remaining players 26..50), then simulate another complete Redis eviction!
            val cpBatch2 = durableService.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = null,
                maxPlayersPerBatch = 25,
                maxGamesPerPlayer = 10
            )
            val dbWriteCountsAfter50Players = durableService.getDurableDbWriteCounts()

            // Also test feeding duplicate match EUW1_D_1_1 and extra over-cap matches EUW1_D_1_11 and EUW1_D_1_12 for dia-top-1
            // before clearing Redis completely again
            durableService.cacheRankSnapshot("euw1", "dia-top-1", "DIAMOND", "I", 75, Instant.ofEpochMilli(clockMs.get()))
            for (extraIdx in listOf(1, 11, 12)) {
                val mId = "EUW1_D_1_$extraIdx"
                redisMatches["match:euw1:$mId"] = MatchDto(
                    metadata = MatchMetadata(mId, listOf("dia-top-1")),
                    info = MatchInfo(
                        gameCreation = clockMs.get() - (100 + extraIdx) * 1_000L,
                        gameDuration = 1800L,
                        gameId = (1000 + extraIdx).toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.22.615.9999",
                        participants = listOf(
                            buildParticipant(puuid = "dia-top-1", role = "TOP", laneCs10 = 75, totalMinions = 200, neutralMinions = 4)
                        )
                    )
                )
            }
            durableService.extractCachedCohortsAndReport(region = "euw1", targetPatch = null)

            // Now wipe ALL Redis keys (0 matches, 0 snapshots, 0 cached reports, 0 cached benchmarks in Redis!)
            redisMatches.clear()
            redisSnapshots.clear()
            redisBenchmarkCells.clear()
            redisReports.clear()
            redisCheckpoints.clear()

            // Create a fresh service instance (simulating a container restart with empty in-memory maps and empty Redis, backed only by Postgres)
            val restartedService = CohortBenchmarkService(
                redisCacheService = evictRedis,
                cohortBenchmarkCellRepository = pgCellRepo,
                riotApiRateLimiter = evictLimiter,
                riotApiService = evictRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = 536_870_912L,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L,
                cohortCollectorCheckpointRepository = pgCheckpointRepo,
                cohortCellSampleStateRepository = pgSampleRepo
            )
            val restartedController = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = evictRiotApi,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = evictRedis,
                cohortBenchmarkService = restartedService,
                overnightCohortCollectorRunner = null
            )

            // Query /cohorts/euw1 and /cohorts/euw1/report when Redis has 0 matches!
            val cohortsAfterEviction = controller.getCohortBenchmarks("euw1", null).body!!
            val reportAfterEviction = controller.getCohortExtractionReport("euw1", null).body!!
            val restartedCohortsAfterEviction = restartedController.getCohortBenchmarks("euw1", null).body!!
            val restartedReportAfterEviction = restartedController.getCohortExtractionReport("euw1", null).body!!

            // Test non-current patch refusal: both older ("16.21") and ahead-of-source ("16.23") when grounded patch is "16.22"
            val refusedOlderCollectorCp = restartedService.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.21",
                maxPlayersPerBatch = 5,
                maxGamesPerPlayer = 10
            )
            val refusedAheadCollectorCp = restartedService.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = "16.23",
                maxPlayersPerBatch = 5,
                maxGamesPerPlayer = 10
            )
            val refusedOlderReport = restartedController.getCohortExtractionReport("euw1", "16.21").body!!
            val refusedAheadReport = restartedController.getCohortExtractionReport("euw1", "16.23").body!!
            val refusedOlderCohorts = restartedController.getCohortBenchmarks("euw1", "16.21").body!!
            val refusedAheadCohorts = restartedController.getCohortBenchmarks("euw1", "16.23").body!!

            val memHeadroom = restartedService.getMemoryHeadroomSnapshot(allowGcHintOnPressure = false)

            Then("it should advance from stale patch 16.21 to grounded 16.22, batch Postgres writes, persist de-identified cell aggregates across Redis evictions, enforce dedupe and 10-game cap, pass the 50-player/200-game quality gate, refuse both older and ahead-of-source patches, and keep the 78%/64MiB guard unchanged") {
                // 1. Grounded patch aligned both collector checkpoint and persisted cells to 16.22 (never 16.21 vs 16.22)
                cpBatch1.targetPatch shouldBe "16.22"
                cpBatch2.targetPatch shouldBe "16.22"
                restartedService.resolveGroundedCurrentPatch("euw1") shouldBe "16.22"

                // 2. Write amplification eliminated: across 50 players (2 batches of 25 with flush interval N=10),
                // each 25-player batch flushes at player 10, player 20, and end-of-batch (player 25) = 3 flushes/batch = 6 total writes
                // instead of 50 per-player rewrites of samples_json and cohort_benchmark_cells!
                dbWriteCountsAfter50Players["cohortCellSampleStatesWrites"] shouldBe 6L
                dbWriteCountsAfter50Players["cohortBenchmarkCellsWrites"] shouldBe 6L

                // 3. Checkpoint cellsPassingQualityGate matches persisted cohort_benchmark_cells and reportAfterEviction
                cpBatch2.cellsPassingQualityGate shouldBe 1
                cpBatch2.targetCellsCount shouldBe 30
                val cpDiaTop = cpBatch2.cellProgress["DIAMOND:TOP"]!!
                cpDiaTop.distinctPlayers shouldBe 50
                cpDiaTop.eligibleGames shouldBe 206
                cpDiaTop.passesQualityGate shouldBe true

                // 4. Even after complete Redis eviction (0 match:* keys in Redis) and JVM restart, /cohorts and /report read the durable Postgres store
                cohortsAfterEviction shouldHaveSize 1
                restartedCohortsAfterEviction shouldHaveSize 1
                val persistedDiaTop = restartedCohortsAfterEviction.first()
                persistedDiaTop.patch shouldBe "16.22"
                persistedDiaTop.tier shouldBe "DIAMOND"
                persistedDiaTop.role shouldBe "TOP"
                persistedDiaTop.distinctPlayers shouldBe 50
                persistedDiaTop.eligibleGames shouldBe 206
                // dia-top-1 had 12 unique matches + 1 duplicate match EUW1_D_1_1: duplicate was deduped, 10 kept, 2 excludedOverPlayerCapGames!
                persistedDiaTop.excludedOverPlayerCapGames shouldBe 2
                persistedDiaTop.passesQualityGate shouldBe true
                persistedDiaTop.colorBandsEnabled shouldBe true
                persistedDiaTop.metrics.laneCsAt10.sampleGames shouldBe 206
                persistedDiaTop.metrics.laneCsAt10.distinctPlayers shouldBe 50
                persistedDiaTop.metrics.laneCsAt10.median shouldNotBe null

                reportAfterEviction.inventory.selectedTargetPatch shouldBe "16.22"
                reportAfterEviction.inventory.eligibleCohortSamples shouldBe 206
                reportAfterEviction.inventory.distinctEligibleCohortPlayers shouldBe 50
                reportAfterEviction.inventory.cellsPassingQualityGate shouldBe 1
                restartedReportAfterEviction.inventory.eligibleCohortSamples shouldBe 206
                restartedReportAfterEviction.inventory.cellsPassingQualityGate shouldBe 1

                // 5. Verify durable Postgres tables contain zero raw PUUIDs or raw match IDs (de-identified hashes only)
                val sampleEntity = pgSampleStates["euw1:16.22:420:DIAMOND:TOP"]!!
                sampleEntity.distinctPlayers shouldBe 50
                sampleEntity.eligibleGames shouldBe 206
                sampleEntity.passesQualityGate shouldBe true
                sampleEntity.samplesJson.contains("dia-top-") shouldBe false
                sampleEntity.samplesJson.contains("EUW1_D_") shouldBe false

                // 6. Both older ("16.21") and ahead-of-source ("16.23") patches are refused across collector, /report, and /cohorts
                refusedOlderCollectorCp.lastStopReason shouldBe "NON_CURRENT_PATCH_REFUSED"
                refusedOlderCollectorCp.targetPatch shouldBe "16.22"
                refusedAheadCollectorCp.lastStopReason shouldBe "NON_CURRENT_PATCH_REFUSED"
                refusedAheadCollectorCp.targetPatch shouldBe "16.22"
                refusedOlderReport.reportSource shouldBe "NON_CURRENT_PATCH_REFUSED"
                refusedOlderReport.cells shouldBe emptyList()
                refusedAheadReport.reportSource shouldBe "NON_CURRENT_PATCH_REFUSED"
                refusedAheadReport.cells shouldBe emptyList()
                refusedOlderCohorts shouldBe emptyList()
                refusedAheadCohorts shouldBe emptyList()

                // 7. 78% / 64 MiB memory guard remains unchanged
                memHeadroom.maxAllowedUtilizationRatio shouldBe 0.78
                memHeadroom.minRequiredFreeBytes shouldBe 67_108_864L
            }
        }

        When("starting with production legacy rows (14 cells on 16.22 with 341 games/218 players/0 passing, checkpoint pinned to 16.21 with 7/30 passing, empty cohort_cell_sample_states) and testing external grounding fallback and stray high patch rejection") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val legacyLimiter = RiotApiRateLimiter(testClock)
            val legacyRedis = mockk<RedisCacheService>(relaxed = true)
            val legacyRiotApi = mockk<RiotApiService>(relaxed = true)
            val pgCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val pgSampleRepo = mockk<com.pride.summoner_searcher_api.repository.CohortCellSampleStateRepository>(relaxed = true)
            val pgCheckpointRepo = mockk<CohortCollectorCheckpointRepository>(relaxed = true)

            val pgBenchmarkCells = mutableMapOf<String, CohortBenchmarkCellEntity>()
            val pgSampleStates = mutableMapOf<String, com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
            val pgCheckpoints = mutableMapOf<String, CohortCollectorCheckpointEntity>()
            var lockHeld = false

            every { legacyRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { legacyRedis.get(any(), any<Class<Any>>()) } returns null
            every { legacyRedis.setIfAbsent(any(), any(), any()) } answers {
                val key = firstArg<String>()
                if (key.startsWith("lock:cohort:collector:active:")) {
                    if (lockHeld) false else {
                        lockHeld = true
                        true
                    }
                } else {
                    true
                }
            }
            every { legacyRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                lockHeld = false
            }

            every { pgCellRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } answers {
                pgBenchmarkCells["${firstArg<String>()}:${secondArg<String>()}:${thirdArg<Int>()}:${arg<String>(3)}:${arg<String>(4)}"]
            }
            every { pgCellRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgBenchmarkCells.values.filter { it.region == r && it.patch == p && it.queueId == q }
            }
            every { pgCellRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) } answers {
                val r = firstArg<String>()
                pgBenchmarkCells.values.filter { it.region == r }
            }
            every { pgCellRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                pgBenchmarkCells.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { pgCellRepo.save(any<CohortBenchmarkCellEntity>()) } answers {
                val ent = firstArg<CohortBenchmarkCellEntity>()
                pgBenchmarkCells["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }

            every { pgSampleRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any()) } answers {
                pgSampleStates["${firstArg<String>()}:${secondArg<String>()}:${thirdArg<Int>()}:${arg<String>(3)}:${arg<String>(4)}"]
            }
            every { pgSampleRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(any(), any(), any()) } answers {
                val r = firstArg<String>()
                val p = secondArg<String>()
                val q = thirdArg<Int>()
                pgSampleStates.values.filter { it.region == r && it.patch == p && it.queueId == q }
            }
            every { pgSampleRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) } answers {
                val r = firstArg<String>()
                pgSampleStates.values.filter { it.region == r }
            }
            every { pgSampleRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                pgSampleStates.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { pgSampleRepo.save(any<com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()) } answers {
                val ent = firstArg<com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
                pgSampleStates["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }

            every { pgCheckpointRepo.findByRegionAndTargetPatchAndQueueId(any(), any(), any()) } answers {
                pgCheckpoints["${firstArg<String>()}:${secondArg<String>()}:${thirdArg<Int>()}"]
            }
            every { pgCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(any(), any()) } answers {
                val r = firstArg<String>()
                val q = secondArg<Int>()
                pgCheckpoints.values.filter { it.region == r && it.queueId == q }.maxByOrNull { it.updatedAt }
            }
            every { pgCheckpointRepo.save(any<CohortCollectorCheckpointEntity>()) } answers {
                val ent = firstArg<CohortCollectorCheckpointEntity>()
                pgCheckpoints["${ent.region}:${ent.targetPatch}:${ent.queueId}"] = ent
                ent
            }

            for (ep in CohortBenchmarkService.REQUIRED_COLLECTOR_ENDPOINTS) {
                legacyLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,2:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            // Seed exact production legacy state:
            // - 14 cells on patch 16.22 in cohort_benchmark_cells totaling 341 games and 218 players (0 passing quality gate)
            //   (13 cells with 24 games / 15 players = 312 games / 195 players, plus 1 cell MASTER:MID with 29 games / 23 players = 341 games / 218 players)
            // - 0 rows in cohort_cell_sample_states (brand new table created by ddl-auto=update)
            // - Legacy checkpoint row pinned to targetPatch = "16.21" with cellsPassingQualityGate = 7 and 2 processed PUUIDs
            val legacyCellPairs = listOf(
                "IRON" to "TOP", "IRON" to "JUNGLE",
                "BRONZE" to "TOP", "BRONZE" to "JUNGLE",
                "SILVER" to "TOP", "SILVER" to "JUNGLE",
                "GOLD" to "TOP", "GOLD" to "JUNGLE",
                "PLATINUM" to "TOP", "PLATINUM" to "JUNGLE",
                "EMERALD" to "TOP", "EMERALD" to "JUNGLE",
                "DIAMOND" to "TOP", "MASTER" to "MID"
            )
            val emptyQuantilesJson = objectMapper.writeValueAsString(CohortMetricsBundleDto())
            legacyCellPairs.forEachIndexed { idx, (tier, role) ->
                val games = if (idx == 13) 29 else 24
                val players = if (idx == 13) 23 else 15
                pgBenchmarkCells["euw1:16.22:420:$tier:$role"] = CohortBenchmarkCellEntity(
                    region = "euw1",
                    patch = "16.22",
                    queueId = 420,
                    tier = tier,
                    role = role,
                    eligibleGames = games,
                    distinctPlayers = players,
                    excludedOverPlayerCapGames = 0,
                    passesQualityGate = false,
                    colorBandsEnabled = false,
                    metricsJson = emptyQuantilesJson,
                    computedAt = nowInstant
                )
            }
            val legacyCp1621 = CohortCollectorCheckpointDto(
                region = "euw1",
                targetPatch = "16.21",
                patchSelectionSource = "AUTO_LATEST_EXACT_PATCH",
                processedPuuids = setOf("legacy-p1", "legacy-p2"),
                pendingCandidatePuuids = emptyList(),
                cellsPassingQualityGate = 7,
                targetCellsCount = 30,
                startedAt = nowInstant.minusSeconds(3600),
                updatedAt = nowInstant.minusSeconds(60)
            )
            pgCheckpoints["euw1:16.21:420"] = CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "16.21",
                queueId = 420,
                processedPuuidsCount = 2,
                pendingCandidatesCount = 0,
                cellsPassingQualityGate = 7,
                totalNetworkCallsMade = 150,
                lastStopReason = "BATCH_COMPLETED",
                checkpointJson = objectMapper.writeValueAsString(legacyCp1621),
                startedAt = legacyCp1621.startedAt,
                updatedAt = legacyCp1621.updatedAt
            )
            pgCheckpoints["euw1:latest:420"] = CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "latest",
                queueId = 420,
                processedPuuidsCount = 2,
                pendingCandidatesCount = 0,
                cellsPassingQualityGate = 7,
                totalNetworkCallsMade = 150,
                lastStopReason = "BATCH_COMPLETED",
                checkpointJson = objectMapper.writeValueAsString(legacyCp1621),
                startedAt = legacyCp1621.startedAt,
                updatedAt = legacyCp1621.updatedAt
            )

            // Also seed a stray poisoned checkpoint with patch "99.1" to verify external grounding rejects it
            val strayHighCp = legacyCp1621.copy(targetPatch = "99.1", updatedAt = nowInstant)
            pgCheckpoints["euw1:99.1:420"] = CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "99.1",
                queueId = 420,
                processedPuuidsCount = 0,
                pendingCandidatesCount = 0,
                cellsPassingQualityGate = 0,
                totalNetworkCallsMade = 0,
                lastStopReason = "IDLE",
                checkpointJson = objectMapper.writeValueAsString(strayHighCp),
                startedAt = nowInstant,
                updatedAt = nowInstant
            )

            every { legacyRiotApi.fetchDataDragonVersions(any()) } returns listOf("16.22.1", "16.21.1")

            val legacyStartupService = CohortBenchmarkService(
                redisCacheService = legacyRedis,
                cohortBenchmarkCellRepository = pgCellRepo,
                riotApiRateLimiter = legacyLimiter,
                riotApiService = legacyRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                configuredContainerLimitBytes = 536_870_912L,
                maxMemoryUtilizationRatio = 0.78,
                minFreeMemoryBytes = 67_108_864L,
                cohortCollectorCheckpointRepository = pgCheckpointRepo,
                cohortCellSampleStateRepository = pgSampleRepo
            )

            // 1. Immediately on first start (before collector runs, with 0 rows in cohort_cell_sample_states):
            val groundedOnStart = legacyStartupService.resolveGroundedCurrentPatch("euw1")
            val cellsOnStart = legacyStartupService.getPersistedCohortCells("euw1", null)
            val reportOnStart = legacyStartupService.getPublicCohortReport("euw1", null)

            // 2. Test safe fallback when Data Dragon lookup fails (throws exception) after cache TTL expires:
            clockMs.addAndGet(31 * 60 * 1_000L) // advance past 30m Data Dragon cache TTL
            every { legacyRiotApi.fetchDataDragonVersions(any()) } throws RuntimeException("Data Dragon timeout")
            val groundedDuringOutage = legacyStartupService.resolveGroundedCurrentPatch("euw1")

            // 3. Restore Data Dragon and run 1 collector batch: verify legacy 16.21 checkpoint advances to 16.22,
            // resets stale 7/30 passing count to 0/30, re-queues legacy-p1 & legacy-p2, and preserves all 14 legacy 16.22 cells!
            clockMs.addAndGet(31 * 60 * 1_000L)
            every { legacyRiotApi.fetchDataDragonVersions(any()) } returns listOf("16.22.1", "16.21.1")
            // Restore latest pointer to legacyCp1621 so we test advancing from the 16.21 prod checkpoint
            pgCheckpoints["euw1:latest:420"] = pgCheckpoints["euw1:16.21:420"]!!

            coEvery { legacyRiotApi.fetchLeagueRankLowPriority(any(), "euw1", any(), any(), any()) } returns LeagueEntryDto(
                leagueId = "l-1", queueType = "RANKED_SOLO_5x5", tier = "GOLD", rank = "II",
                summonerName = "legacy-p1", puuid = "legacy-p1", leaguePoints = 50, wins = 20, losses = 20,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false
            )
            coEvery { legacyRiotApi.getMatchIdsByPuuidLowPriority(any(), "euw1", 420, any(), 0, any(), any(), any()) } returns listOf("EUW1_LEGACY_1")
            coEvery { legacyRiotApi.getMatchByIdLowPriority("EUW1_LEGACY_1", "euw1", any(), any(), any()) } returns MatchDto(
                metadata = MatchMetadata("EUW1_LEGACY_1", listOf("legacy-p1")),
                info = MatchInfo(
                    gameCreation = clockMs.get() - 10_000L,
                    gameDuration = 1800L,
                    gameId = 999001L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.9999",
                    participants = listOf(
                        buildParticipant(puuid = "legacy-p1", role = "MIDDLE", laneCs10 = 76, totalMinions = 195, neutralMinions = 4)
                    )
                )
            )

            val cpAfterLegacyAdvance = legacyStartupService.runResumableLowPriorityCollectorBatch(
                region = "euw1",
                targetPatch = null,
                maxPlayersPerBatch = 1,
                maxGamesPerPlayer = 10
            )
            val cellsAfterFirstBatch = legacyStartupService.getPersistedCohortCells("euw1", null)

            Then("it should preserve all 14 legacy 16.22 cells (341 games, 218 players) on first start, reject stray patch 99.1, survive Data Dragon outage without crashing or advancing to 99.1, and advance the 16.21 checkpoint to 16.22 on the first batch") {
                // Grounded patch on start is 16.22 (stray "99.1" checkpoint rejected!)
                groundedOnStart shouldBe "16.22"

                // All 14 legacy 16.22 cells (341 games, 218 players, 0 passing) are preserved on first start even with 0 cohort_cell_sample_states rows
                cellsOnStart shouldHaveSize 14
                cellsOnStart.sumOf { it.eligibleGames } shouldBe 341
                cellsOnStart.sumOf { it.distinctPlayers } shouldBe 218
                cellsOnStart.count { it.passesQualityGate } shouldBe 0
                reportOnStart.inventory.selectedTargetPatch shouldBe "16.22"
                reportOnStart.inventory.eligibleCohortSamples shouldBe 341
                reportOnStart.inventory.distinctEligibleCohortPlayers shouldBe 218
                reportOnStart.inventory.cellsPassingQualityGate shouldBe 0

                // Safe fallback when Data Dragon throws: does not crash and does not advance to unbacked "99.1" checkpoint
                groundedDuringOutage shouldBe "16.22"

                // First collector batch advances from legacy 16.21 checkpoint to 16.22, resets stale 7/30 passing count to 0/30,
                // processes legacy-p1 into GOLD:MIDDLE (a 15th cell on 16.22), and preserves all 14 existing 16.22 cells!
                cpAfterLegacyAdvance.targetPatch shouldBe "16.22"
                cpAfterLegacyAdvance.cellsPassingQualityGate shouldBe 0
                cpAfterLegacyAdvance.processedPuuids shouldBe setOf("legacy-p1")
                cpAfterLegacyAdvance.pendingCandidatePuuids shouldBe listOf("legacy-p2")
                pgSampleStates.containsKey("euw1:16.22:420:GOLD:MIDDLE") shouldBe true
                cellsAfterFirstBatch shouldHaveSize 15
            }
        }

        When("fetching Data Dragon versions.json via RiotApiService.fetchDataDragonVersions") {
            val riotClientCalls = java.util.concurrent.atomic.AtomicInteger(0)
            val ddragonClientCalls = java.util.concurrent.atomic.AtomicInteger(0)
            val capturedDdragonUris = mutableListOf<String>()
            val capturedDdragonHeaders = mutableListOf<org.springframework.http.HttpHeaders>()

            fun buildMockJsonResponse(jsonBody: String): org.springframework.http.client.ClientHttpResponse {
                val bytes = jsonBody.toByteArray(Charsets.UTF_8)
                return object : org.springframework.http.client.ClientHttpResponse {
                    override fun getStatusCode(): org.springframework.http.HttpStatusCode = org.springframework.http.HttpStatus.OK
                    override fun getStatusText(): String = "OK"
                    override fun close() {}
                    override fun getBody(): java.io.InputStream = java.io.ByteArrayInputStream(bytes)
                    override fun getHeaders(): org.springframework.http.HttpHeaders = org.springframework.http.HttpHeaders().apply {
                        contentType = org.springframework.http.MediaType.APPLICATION_JSON
                        contentLength = bytes.size.toLong()
                    }
                }
            }

            // Authenticated riotRestClient with defaultHeader("X-Riot-Token", ...)
            val authenticatedRiotRestClient = org.springframework.web.client.RestClient.builder()
                .defaultHeader("X-Riot-Token", "RGAPI-secret-token-must-never-leak-to-ddragon")
                .requestInterceptor { _, _, _ ->
                    riotClientCalls.incrementAndGet()
                    buildMockJsonResponse("""["should-not-be-called"]""")
                }
                .build()

            // Separate unauthenticated dataDragonRestClient (matching RestClientConfig.dataDragonRestClient)
            val unauthenticatedDdragonRestClient = org.springframework.web.client.RestClient.builder()
                .defaultHeader(org.springframework.http.HttpHeaders.USER_AGENT, "SummonerSearcher/1.0")
                .requestInterceptor { request, _, _ ->
                    ddragonClientCalls.incrementAndGet()
                    capturedDdragonUris.add(request.uri.toString())
                    val headersSnapshot = org.springframework.http.HttpHeaders()
                    headersSnapshot.putAll(request.headers)
                    capturedDdragonHeaders.add(headersSnapshot)
                    buildMockJsonResponse("""["16.22.1", "16.21.1", "16.20.1"]""")
                }
                .build()

            val serviceWithSeparateClients = RiotApiService(
                riotRestClient = authenticatedRiotRestClient,
                rateLimiter = RiotApiRateLimiter(SystemTimeProvider()),
                dataDragonRestClient = unauthenticatedDdragonRestClient
            )
            val versions = serviceWithSeparateClients.fetchDataDragonVersions(maxEntries = 2)

            // Also instantiate the real RestClientConfig beans (riotRestClient vs dataDragonRestClient)
            // and verify that riotRestClient has the default X-Riot-Token header while dataDragonRestClient has none!
            val restClientConfig = com.pride.summoner_searcher_api.config.RestClientConfig()
            org.springframework.test.util.ReflectionTestUtils.setField(
                restClientConfig,
                "riotApiKey",
                "RGAPI-secret-token-must-never-leak-to-ddragon"
            )
            val realConfigDdragonHeaders = mutableListOf<org.springframework.http.HttpHeaders>()
            val realConfigRiotHeaders = mutableListOf<org.springframework.http.HttpHeaders>()
            val realConfigRiotClient = restClientConfig.riotRestClient().mutate()
                .requestInterceptor { request, _, _ ->
                    val headersSnapshot = org.springframework.http.HttpHeaders()
                    headersSnapshot.putAll(request.headers)
                    realConfigRiotHeaders.add(headersSnapshot)
                    buildMockJsonResponse("""["should-not-be-called"]""")
                }
                .build()
            val realConfigDdragonClient = restClientConfig.dataDragonRestClient().mutate()
                .requestInterceptor { request, _, _ ->
                    val headersSnapshot = org.springframework.http.HttpHeaders()
                    headersSnapshot.putAll(request.headers)
                    realConfigDdragonHeaders.add(headersSnapshot)
                    buildMockJsonResponse("""["16.22.1"]""")
                }
                .build()
            val realConfigService = RiotApiService(
                riotRestClient = realConfigRiotClient,
                rateLimiter = RiotApiRateLimiter(SystemTimeProvider()),
                dataDragonRestClient = realConfigDdragonClient
            )
            val realConfigVersions = realConfigService.fetchDataDragonVersions(maxEntries = 1)

            Then("it should use the separate dataDragonRestClient, make zero calls on riotRestClient, and send no X-Riot-Token header to ddragon.leagueoflegends.com") {
                versions shouldBe listOf("16.22.1", "16.21.1")
                riotClientCalls.get() shouldBe 0
                ddragonClientCalls.get() shouldBe 1
                capturedDdragonUris.single() shouldBe "https://ddragon.leagueoflegends.com/api/versions.json"

                val sentHeaders = capturedDdragonHeaders.single()
                sentHeaders.containsKey("X-Riot-Token") shouldBe false
                sentHeaders["X-Riot-Token"] shouldBe null
                sentHeaders.keys.none { it.equals("X-Riot-Token", ignoreCase = true) } shouldBe true

                // Verify real RestClientConfig.dataDragonRestClient bean sends zero X-Riot-Token headers
                // and zero requests through RestClientConfig.riotRestClient
                realConfigVersions shouldBe listOf("16.22.1")
                realConfigRiotHeaders shouldBe emptyList()
                val configDdragonSentHeaders = realConfigDdragonHeaders.single()
                configDdragonSentHeaders.containsKey("X-Riot-Token") shouldBe false
                configDdragonSentHeaders["X-Riot-Token"] shouldBe null
                configDdragonSentHeaders.keys.none { it.equals("X-Riot-Token", ignoreCase = true) } shouldBe true
            }
        }

        When("capturing collector session memory series and delta across start, batches, peak, pre-GC end, passive post-GC, and >32-point ring buffer overflow") {
            val mib = 1_048_576L
            var currentSample = RawMemorySample(
                jvmHeapUsedBytes = 60L * mib,
                jvmHeapCommittedBytes = 90L * mib,
                jvmHeapMaxBytes = 204L * mib,
                jvmNonHeapUsedBytes = 115L * mib,
                jvmMetaspaceUsedBytes = 110L * mib,
                jvmMetaspaceCommittedBytes = 112L * mib,
                directBufferUsedBytes = 2L * mib,
                cgroupMemoryCurrentBytes = 327L * mib,
                cgroupMemoryMaxBytes = 512L * mib,
                cgroupMemoryPeakBytes = 330L * mib,
                cgroupAnonBytes = 304L * mib,
                cgroupFileCacheBytes = 23L * mib,
                oldGenGcCount = 5L
            )
            val memProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = currentSample
            }
            var nowEpochMs = 1_760_000_000_000L
            val timeProvider = object : TimeProvider {
                override fun now(): Long = nowEpochMs
            }

            val redisStore = java.util.concurrent.ConcurrentHashMap<String, Any>()
            val locks = java.util.concurrent.ConcurrentHashMap<String, Boolean>()
            val memRedis = mockk<RedisCacheService>(relaxed = true)
            every { memRedis.setIfAbsent(any(), any(), any()) } answers {
                val key = firstArg<String>()
                locks.putIfAbsent(key, true) == null
            }
            every { memRedis.delete(any()) } answers {
                locks.remove(firstArg<String>())
            }
            every { memRedis.set<Any>(any(), any(), any()) } answers {
                redisStore[firstArg<String>()] = secondArg<Any>()
            }
            every { memRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                redisStore[firstArg<String>()] as? CohortCollectorCheckpointDto
            }
            every { memRedis.scanKeys(any(), any()) } returns emptyList()

            val memLimiter = RiotApiRateLimiter(timeProvider)
            val memRiotApi = mockk<RiotApiService>(relaxed = true)
            every { memRiotApi.fetchDataDragonVersions(any()) } returns listOf("16.22.1")

            val memService = CohortBenchmarkService(
                redisCacheService = memRedis,
                cohortBenchmarkCellRepository = mockk(relaxed = true),
                riotApiRateLimiter = memLimiter,
                riotApiService = memRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                memoryStatsProvider = memProvider
            )
            val runner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = memService,
                configuredOperatorToken = "test-op-token",
                timeProvider = timeProvider
            )

            // 1. Start session 1 (networkCollectionEnabled=false causes immediate terminal stop after batch 1)
            // Start sample (T0): cgroupCurrent=327, anon=304, file=23, heapUsed=60, heapCommitted=90, metaspaceCommitted=112, direct=2
            // -> nativeResidual = 327 - 90 - 112 - 2 = 123.0 MiB
            // -> nativeAnonResidual = 304 - 90 - 112 - 2 = 100.0 MiB
            runner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.22",
                maxBatches = 1,
                batchSize = 5,
                maxTotalProductiveBatches = 3,
                maxTotalNetworkCalls = 38,
                async = false
            )

            // Simulate additional batches and peak (356 MiB current, 333 MiB anon) followed by pre-GC end (351 MiB current, 328 MiB anon)
            nowEpochMs += 10_000L
            currentSample = currentSample.copy(
                jvmHeapUsedBytes = 88L * mib,
                jvmHeapCommittedBytes = 115L * mib,
                cgroupMemoryCurrentBytes = 356L * mib,
                cgroupMemoryPeakBytes = 356L * mib,
                cgroupAnonBytes = 333L * mib
            )
            runner.recordBatchEndMemorySample(batchIndex = 2, totalRiotNetworkCalls = 25)

            // Simulate pre-GC end state: heapUsed dropped to 72 MiB after minor GC, heapCommitted stayed at 115 MiB (43 MiB unused),
            // cgroupCurrent=351 MiB, cgroupAnon=328 MiB, oldGenGcCount still 5L (no natural Full GC happened)
            nowEpochMs += 10_000L
            currentSample = currentSample.copy(
                jvmHeapUsedBytes = 72L * mib,
                jvmHeapCommittedBytes = 115L * mib,
                cgroupMemoryCurrentBytes = 351L * mib,
                cgroupMemoryPeakBytes = 356L * mib,
                cgroupAnonBytes = 328L * mib,
                oldGenGcCount = 5L
            )
            // Force overwrite pre-GC end sample at this state to test exact T0 -> T2 -> T3 math
            val preGcField = runner.javaClass.getDeclaredField("sessionEndPreGcMemoryPoint").apply { isAccessible = true }
            preGcField.set(runner, null)
            runner.recordSessionEndPreGcMemorySampleIfNeeded(batchIndex = 3, totalRiotNetworkCalls = 38)

            val statusBeforeFullGc = runner.getStatus(region = "euw1", targetPatch = "16.22")

            // 2. Now simulate a natural Old-Gen / Full GC occurring (oldGenGcCount advances 5 -> 6):
            // SerialGC shrinks heapCommitted from 115 -> 85 MiB and heapUsed from 72 -> 64 MiB;
            // cgroupAnon drops from 328 -> 298 MiB, cgroupCurrent drops from 351 -> 321 MiB
            nowEpochMs += 15_000L
            currentSample = currentSample.copy(
                jvmHeapUsedBytes = 64L * mib,
                jvmHeapCommittedBytes = 85L * mib,
                cgroupMemoryCurrentBytes = 321L * mib,
                cgroupMemoryPeakBytes = 356L * mib,
                cgroupAnonBytes = 298L * mib,
                oldGenGcCount = 6L
            )
            val statusAfterNaturalFullGc = runner.getStatus(region = "euw1", targetPatch = "16.22")

            // 3. Now push 40 batch-end samples in the session to test the 32-point ring buffer bound
            // Including a mid-session spike of 390 MiB at batch 10 (which will be evicted from the rolling BATCH_END window
            // as batches 11..40 arrive, but MUST remain captured in SESSION_PEAK!)
            for (b in 4..40) {
                nowEpochMs += 1_000L
                val isSpikeBatch = (b == 10)
                currentSample = currentSample.copy(
                    jvmHeapUsedBytes = (if (isSpikeBatch) 110L else 70L) * mib,
                    jvmHeapCommittedBytes = (if (isSpikeBatch) 130L else 95L) * mib,
                    cgroupMemoryCurrentBytes = (if (isSpikeBatch) 390L else 335L) * mib,
                    cgroupMemoryPeakBytes = 390L * mib,
                    cgroupAnonBytes = (if (isSpikeBatch) 367L else 312L) * mib
                )
                runner.recordBatchEndMemorySample(batchIndex = b, totalRiotNetworkCalls = b * 5)
            }
            val statusAfterOverflow = runner.getStatus(region = "euw1", targetPatch = "16.22")

            // 4. Start a brand-new session (isAutoResume=false) and verify the ring buffer resets cleanly
            nowEpochMs += 5_000L
            currentSample = currentSample.copy(
                jvmHeapUsedBytes = 58L * mib,
                jvmHeapCommittedBytes = 80L * mib,
                cgroupMemoryCurrentBytes = 318L * mib,
                cgroupMemoryPeakBytes = 390L * mib,
                cgroupAnonBytes = 295L * mib,
                oldGenGcCount = 6L
            )
            val statusAfterNewSessionStart = runner.startOrResumeGuarded(
                region = "euw1",
                targetPatch = "16.22",
                maxBatches = 1,
                batchSize = 5,
                maxTotalProductiveBatches = 2,
                maxTotalNetworkCalls = 20,
                async = false
            )

            Then("it should enforce the 32-point bound, compute exact pre/post-GC MiB deltas, leave post-GC null until a natural Old-Gen GC occurs, and reset on new session start") {
                // Verify pre-Full-GC status: postGcCaptured is false and all post-GC delta fields are null
                val deltaPre = statusBeforeFullGc.sessionMemoryDelta!!
                deltaPre.postGcCaptured shouldBe false
                deltaPre.startAnonMiB shouldBe 304.0
                deltaPre.peakCurrentMiB shouldBe 356.0
                deltaPre.endPreGcAnonMiB shouldBe 328.0
                deltaPre.totalRetainedAnonPreGcMiB shouldBe 24.0
                deltaPre.deltaHeapUsedPreGcMiB shouldBe 12.0      // 72 - 60
                deltaPre.deltaHeapCommittedPreGcMiB shouldBe 25.0 // 115 - 90
                // Start nativeAnonResidual = 304 - 90 - 112 - 2 = 100.0
                // EndPreGc nativeAnonResidual = 328 - 115 - 112 - 2 = 99.0 -> delta = -1.0
                deltaPre.deltaNativeAnonResidualPreGcMiB shouldBe -1.0
                deltaPre.endPostGcAnonMiB shouldBe null
                deltaPre.totalRetainedAnonPostGcMiB shouldBe null
                deltaPre.reclaimedByEndFullGcMiB shouldBe null
                deltaPre.deltaHeapUsedPostGcMiB shouldBe null
                deltaPre.deltaHeapCommittedPostGcMiB shouldBe null
                deltaPre.deltaMetaspaceCommittedMiB shouldBe null
                deltaPre.deltaNativeAnonResidualPostGcMiB shouldBe null
                statusBeforeFullGc.sessionMemorySeries.none { it.phase == "SESSION_END_POST_GC" } shouldBe true

                val startPt = statusBeforeFullGc.sessionMemorySeries.first { it.phase == "SESSION_START" }
                startPt.cgroupCurrentMiB shouldBe 327.0
                startPt.cgroupPeakMiB shouldBe 330.0
                startPt.cgroupAnonMiB shouldBe 304.0
                startPt.cgroupFileCacheMiB shouldBe 23.0
                startPt.heapUsedMiB shouldBe 60.0
                startPt.heapCommittedMiB shouldBe 90.0
                startPt.heapUnusedCommittedMiB shouldBe 30.0
                startPt.metaspaceUsedMiB shouldBe 110.0
                startPt.metaspaceCommittedMiB shouldBe 112.0
                startPt.directBuffersUsedMiB shouldBe 2.0
                startPt.nativeResidualMiB shouldBe 123.0
                startPt.nativeAnonResidualMiB shouldBe 100.0

                // Verify passive post-Full-GC capture: once oldGenGcCount increments, SESSION_END_POST_GC is captured
                val deltaPost = statusAfterNaturalFullGc.sessionMemoryDelta!!
                deltaPost.postGcCaptured shouldBe true
                deltaPost.endPostGcAnonMiB shouldBe 298.0
                deltaPost.totalRetainedAnonPostGcMiB shouldBe -6.0   // 298 - 304
                deltaPost.reclaimedByEndFullGcMiB shouldBe 30.0      // 115 - 85
                deltaPost.deltaHeapUsedPostGcMiB shouldBe 4.0        // 64 - 60
                deltaPost.deltaHeapCommittedPostGcMiB shouldBe -5.0  // 85 - 90
                deltaPost.deltaMetaspaceCommittedMiB shouldBe 0.0    // 112 - 112
                // EndPostGc nativeAnonResidual = 298 - 85 - 112 - 2 = 99.0 -> delta vs 100.0 = -1.0
                deltaPost.deltaNativeAnonResidualPostGcMiB shouldBe -1.0
                statusAfterNaturalFullGc.sessionMemorySeries.any { it.phase == "SESSION_END_POST_GC" } shouldBe true

                // Verify 32-point ring buffer bound after 40 batches
                val overflowSeries = statusAfterOverflow.sessionMemorySeries
                overflowSeries.size shouldBe 32
                overflowSeries.first().phase shouldBe "SESSION_START"
                val peakAfterOverflow = overflowSeries.single { it.phase == "SESSION_PEAK" }
                peakAfterOverflow.cgroupCurrentMiB shouldBe 390.0
                peakAfterOverflow.batchIndex shouldBe 10
                statusAfterOverflow.sessionMemoryDelta!!.peakCurrentMiB shouldBe 390.0
                val batchEndPoints = overflowSeries.filter { it.phase == "BATCH_END" }
                batchEndPoints.size shouldBe 28 // 32 total - 4 (START, PEAK, END_PRE_GC, END_POST_GC)
                batchEndPoints.first().batchIndex shouldBe 13
                batchEndPoints.last().batchIndex shouldBe 40

                // Verify only allowed enum phase strings exist in sessionMemorySeries (zero identifiers)
                val allowedPhases = setOf(
                    "SESSION_START",
                    "BATCH_END",
                    "SESSION_PEAK",
                    "SESSION_END_PRE_GC",
                    "SESSION_END_POST_GC"
                )
                overflowSeries.all { it.phase in allowedPhases } shouldBe true

                // Verify starting a new session resets the ring buffer cleanly
                statusAfterNewSessionStart.sessionMemorySeries.size shouldBe 4 // SESSION_START, BATCH_END(1), SESSION_PEAK, SESSION_END_PRE_GC
                statusAfterNewSessionStart.sessionMemoryDelta!!.startAnonMiB shouldBe 295.0
                statusAfterNewSessionStart.sessionMemoryDelta!!.peakCurrentMiB shouldBe 318.0
                statusAfterNewSessionStart.sessionMemoryDelta!!.postGcCaptured shouldBe false
            }
        }

        When("resolving grounded current patch using scalar metadata queries without hydrating full sample or benchmark entities") {
            var nowMs = 1_760_000_000_000L
            val timeProvider = object : TimeProvider {
                override fun now(): Long = nowMs
            }
            val scalarRedis = mockk<RedisCacheService>(relaxed = true)
            val scalarCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val scalarSampleRepo = mockk<com.pride.summoner_searcher_api.repository.CohortCellSampleStateRepository>(relaxed = true)
            val scalarCheckpointRepo = mockk<CohortCollectorCheckpointRepository>(relaxed = true)
            val scalarRiotApi = mockk<RiotApiService>(relaxed = true)
            val scalarLimiter = RiotApiRateLimiter(timeProvider)

            every { scalarRedis.get(any(), any<Class<Any>>()) } returns null
            every { scalarRedis.scanKeys(any(), any()) } returns emptyList()

            fun newColdService() = CohortBenchmarkService(
                redisCacheService = scalarRedis,
                cohortBenchmarkCellRepository = scalarCellRepo,
                riotApiRateLimiter = scalarLimiter,
                riotApiService = scalarRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = scalarCheckpointRepo,
                cohortCellSampleStateRepository = scalarSampleRepo
            )

            // 1. DataDragon upper bound (16.22) caps stray high store patches (16.99) while using scalar queries
            every { scalarRiotApi.fetchDataDragonVersions(any()) } returns listOf("16.22.1", "16.21.1")
            every { scalarSampleRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns listOf("16.21", "16.22", "16.99")
            every { scalarCellRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns listOf("16.20", "16.99")
            val groundedWithDdragon = newColdService().resolveGroundedCurrentPatch("euw1")

            // 2. DataDragon unavailable on cold start: selects highest verified store patch (16.22 over 16.21)
            //    and ignores unbacked higher checkpoint patch (16.99)
            nowMs += 31 * 60 * 1_000L
            every { scalarRiotApi.fetchDataDragonVersions(any()) } throws RuntimeException("DataDragon unreachable")
            every { scalarSampleRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns listOf("16.21")
            every { scalarCellRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns listOf("16.22")
            val strayCpDto = CohortCollectorCheckpointDto(
                region = "euw1",
                targetPatch = "16.99",
                patchSelectionSource = "AUTO_LATEST_EXACT_PATCH",
                startedAt = Instant.ofEpochMilli(nowMs),
                updatedAt = Instant.ofEpochMilli(nowMs)
            )
            every { scalarCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc("euw1", 420) } returns CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "latest",
                queueId = 420,
                checkpointJson = objectMapper.writeValueAsString(strayCpDto),
                startedAt = Instant.ofEpochMilli(nowMs),
                updatedAt = Instant.ofEpochMilli(nowMs)
            )
            val groundedStoreFallback = newColdService().resolveGroundedCurrentPatch("euw1")

            // 3. Empty verified stores + DataDragon unavailable: falls back to latest Postgres checkpoint row with targetPatch="latest"
            //    by parsing checkpointJson.targetPatch ("16.21")
            nowMs += 31 * 60 * 1_000L
            every { scalarSampleRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns emptyList()
            every { scalarCellRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } returns emptyList()
            val validCpDto = CohortCollectorCheckpointDto(
                region = "euw1",
                targetPatch = "16.21",
                patchSelectionSource = "AUTO_LATEST_EXACT_PATCH",
                startedAt = Instant.ofEpochMilli(nowMs),
                updatedAt = Instant.ofEpochMilli(nowMs)
            )
            every { scalarCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc("euw1", 420) } returns CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "latest",
                queueId = 420,
                checkpointJson = objectMapper.writeValueAsString(validCpDto),
                startedAt = Instant.ofEpochMilli(nowMs),
                updatedAt = Instant.ofEpochMilli(nowMs)
            )
            val groundedFromCheckpointJson = newColdService().resolveGroundedCurrentPatch("euw1")

            // 4. Repository exceptions are safely caught by runCatching and fall back to observed/in-memory patch
            every { scalarSampleRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } throws RuntimeException("DB timeout")
            every { scalarCellRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") } throws RuntimeException("DB timeout")
            every { scalarCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc("euw1", 420) } throws RuntimeException("DB timeout")
            val groundedOnRepoError = newColdService().resolveGroundedCurrentPatch("euw1", observedCandidatePatch = "16.23")

            Then("it should preserve all grounding and fallback semantics while making zero full-entity findByRegionOrderByPatchDescTierAscRoleAsc calls") {
                groundedWithDdragon shouldBe "16.22"
                groundedStoreFallback shouldBe "16.22"
                groundedFromCheckpointJson shouldBe "16.21"
                groundedOnRepoError shouldBe "16.23"

                verify(exactly = 0) { scalarSampleRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) }
                verify(exactly = 0) { scalarCellRepo.findByRegionOrderByPatchDescTierAscRoleAsc(any()) }
                verify(atLeast = 4) { scalarSampleRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") }
                verify(atLeast = 4) { scalarCellRepo.findDistinctPatchesByRegionWithEligibleGames("euw1") }
            }
        }

        When("ingesting a single cached player by exact match IDs with fail-closed transaction guards, divergent memory refusal, and full-RMW gate serialization") {
            val nowMs = 1_760_000_000_000L
            val nowInst = Instant.ofEpochMilli(nowMs)
            val timeProvider = object : TimeProvider {
                override fun now(): Long = nowMs
            }

            val exactRedis = mockk<RedisCacheService>(relaxed = true)
            val exactCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val exactSampleRepo = mockk<com.pride.summoner_searcher_api.repository.CohortCellSampleStateRepository>(relaxed = true)
            val exactCheckpointRepo = mockk<CohortCollectorCheckpointRepository>(relaxed = true)
            val exactRiotApi = mockk<RiotApiService>(relaxed = true)
            val exactLimiter = RiotApiRateLimiter(timeProvider)

            val savedSampleEntities = LinkedHashMap<String, com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
            val savedBenchmarkEntities = LinkedHashMap<String, CohortBenchmarkCellEntity>()
            val redisMap = LinkedHashMap<String, Any>()
            val activeRedisLocks = LinkedHashMap<String, String>()

            val failPostCommitCacheRefresh = java.util.concurrent.atomic.AtomicBoolean(false)

            every { exactRedis.get(any(), any<Class<Any>>()) } answers {
                val key = firstArg<String>()
                val clazz = secondArg<Class<Any>>()
                val lockVal = activeRedisLocks[key]
                if (clazz == String::class.java && lockVal != null) {
                    lockVal
                } else {
                    val v = redisMap[key]
                    if (v != null && clazz.isInstance(v)) v else null
                }
            }
            every { exactRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                if (failPostCommitCacheRefresh.get() && key.startsWith("cohort:benchmarks:")) {
                    throw RuntimeException("Simulated Redis benchmark cache write timeout after DB commit")
                }
                redisMap[key] = secondArg<Any>()
            }
            every { exactRedis.setIfAbsent(any(), any(), any()) } answers {
                val key = firstArg<String>()
                val value = secondArg<Any>().toString()
                if (activeRedisLocks.containsKey(key)) {
                    false
                } else {
                    activeRedisLocks[key] = value
                    true
                }
            }
            every { exactRedis.delete(any()) } answers {
                val key = firstArg<String>()
                activeRedisLocks.remove(key)
                if (failPostCommitCacheRefresh.get() && key.startsWith("cohort:report:")) {
                    throw RuntimeException("Simulated Redis report cache eviction failure after DB commit")
                }
                redisMap.remove(key)
            }
            every { exactSampleRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                savedSampleEntities.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { exactCellRepo.findDistinctPatchesByRegionWithEligibleGames(any()) } answers {
                val r = firstArg<String>()
                savedBenchmarkEntities.values.filter { it.region == r && it.eligibleGames > 0 }.map { it.patch }.distinct()
            }
            every { exactCheckpointRepo.findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(any(), any()) } returns null

            every {
                exactSampleRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any())
            } answers {
                val r = arg<String>(0)
                val p = arg<String>(1)
                val q = arg<Int>(2)
                val t = arg<String>(3)
                val role = arg<String>(4)
                savedSampleEntities["$r:$p:$q:$t:$role"]
            }
            every { exactSampleRepo.save(any()) } answers {
                val ent = firstArg<com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity>()
                savedSampleEntities["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }
            every {
                exactCellRepo.findByRegionAndPatchAndQueueIdAndTierAndRole(any(), any(), any(), any(), any())
            } answers {
                val r = arg<String>(0)
                val p = arg<String>(1)
                val q = arg<Int>(2)
                val t = arg<String>(3)
                val role = arg<String>(4)
                savedBenchmarkEntities["$r:$p:$q:$t:$role"]
            }
            every {
                exactCellRepo.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(any(), any(), any())
            } answers {
                val r = arg<String>(0)
                val p = arg<String>(1)
                val q = arg<Int>(2)
                savedBenchmarkEntities.values.filter { it.region == r && it.patch == p && it.queueId == q }
            }
            every { exactCellRepo.save(any()) } answers {
                val ent = firstArg<CohortBenchmarkCellEntity>()
                savedBenchmarkEntities["${ent.region}:${ent.patch}:${ent.queueId}:${ent.tier}:${ent.role}"] = ent
                ent
            }

            // No-op in-memory PlatformTransactionManager for unit tests (real H2 rollback is tested in CohortPatchMetadataSqlShapeTest)
            val unitTxManager = object : org.springframework.transaction.support.AbstractPlatformTransactionManager() {
                override fun doGetTransaction(): Any = Any()
                override fun doBegin(transaction: Any, definition: org.springframework.transaction.TransactionDefinition) {}
                override fun doCommit(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
                override fun doRollback(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
            }
            val validTxTemplate = org.springframework.transaction.support.TransactionTemplate(unitTxManager)

            val selectedPuuid = "cached-challenger-top-1"
            val coParticipantPuuid = "co-participant-must-not-be-ingested"

            // Cache fresh profile with Solo Queue CHALLENGER rank and 12 match IDs (to test <= 10 bound)
            val all12MatchIds = (1..12).map { "EUW1_EXACT_$it" }
            redisMap["player:profile:euw1:$selectedPuuid"] = SummonerProfileDto(
                puuid = selectedPuuid,
                gameName = "SpearTest",
                tagLine = "EUW",
                summonerLevel = 500,
                profileIconId = 1,
                soloQueueRank = LeagueEntryDto(
                    puuid = selectedPuuid,
                    leaguePoints = 1250,
                    rank = "I",
                    wins = 200,
                    losses = 150,
                    veteran = true,
                    inactive = false,
                    freshBlood = false,
                    hotStreak = false,
                    leagueId = "c-1",
                    queueType = "RANKED_SOLO_5x5",
                    tier = "CHALLENGER",
                    summonerName = null
                ),
                recentMatches = emptyList(),
                allMatchIds = all12MatchIds,
                lastRefreshed = nowInst.minusSeconds(300)
            )
            // Also cache a rank snapshot for the co-participant to prove co-participant is NEVER looked up or folded
            redisMap["cohort:rank:snapshot:euw1:$coParticipantPuuid"] = CohortRankSnapshotDto(
                puuid = coParticipantPuuid,
                region = "euw1",
                tier = "GOLD",
                rank = "I",
                leaguePoints = 50,
                queueType = "RANKED_SOLO_5x5",
                snapshotAt = nowInst.minusSeconds(300)
            )

            // Match 1: Qualified Ranked Solo (420) on patch 16.22, role TOP for selectedPuuid + MIDDLE for coParticipantPuuid
            redisMap["match:euw1:EUW1_EXACT_1"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_1", listOf(selectedPuuid, coParticipantPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 3_600_000L,
                    gameDuration = 1800L,
                    gameId = 1001L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 86, totalMinions = 230, neutralMinions = 8),
                        buildParticipant(puuid = coParticipantPuuid, role = "MIDDLE", laneCs10 = 78, totalMinions = 200, neutralMinions = 4)
                    )
                )
            )
            // Match 2: Wrong patch (16.21) -> excludedWrongPatchMatches
            redisMap["match:euw1:EUW1_EXACT_2"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_2", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 7_200_000L,
                    gameDuration = 1800L,
                    gameId = 1002L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.21.610.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 80, totalMinions = 210, neutralMinions = 4)
                    )
                )
            )
            // Match 3: Stale / skew (> 7 days before rank snapshot) -> excludedStaleOrSkewMatches
            redisMap["match:euw1:EUW1_EXACT_3"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_3", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 9L * 24L * 3_600_000L,
                    gameDuration = 1800L,
                    gameId = 1003L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 82, totalMinions = 215, neutralMinions = 4)
                    )
                )
            )
            // Match 4: Remake (< 600s) -> excludedNonSoloOrRemakeMatches
            redisMap["match:euw1:EUW1_EXACT_4"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_4", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 5_000_000L,
                    gameDuration = 180L,
                    gameId = 1004L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 20, totalMinions = 20, neutralMinions = 0)
                    )
                )
            )
            // Match 5: Ineligible role (JUNGLE) -> excludedIneligibleRoleMatches
            redisMap["match:euw1:EUW1_EXACT_5"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_5", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 6_000_000L,
                    gameDuration = 1800L,
                    gameId = 1005L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "JUNGLE", laneCs10 = 10, totalMinions = 30, neutralMinions = 160)
                    )
                )
            )
            // Matches 6..10 are uncached in Redis -> missingOrUncachedMatches = 5
            // Match 11 is valid on patch 16.22, but beyond the 10-match bound so it MUST NOT be inspected when taking top 10 from profile!
            redisMap["match:euw1:EUW1_EXACT_11"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_11", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 8_000_000L,
                    gameDuration = 1800L,
                    gameId = 1011L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 90, totalMinions = 250, neutralMinions = 8)
                    )
                )
            )
            // Match 12 is valid on patch 16.22 for testing post-commit cache failure warning
            redisMap["match:euw1:EUW1_EXACT_12"] = MatchDto(
                metadata = MatchMetadata("EUW1_EXACT_12", listOf(selectedPuuid)),
                info = MatchInfo(
                    gameCreation = nowMs - 9_000_000L,
                    gameDuration = 1800L,
                    gameId = 1012L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildParticipant(puuid = selectedPuuid, role = "TOP", laneCs10 = 94, totalMinions = 260, neutralMinions = 8)
                    )
                )
            )

            // 1. Fail-closed test when CohortCellSampleStateRepository is unwired (null)
            val serviceUnwiredRepo = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = null,
                transactionTemplate = validTxTemplate
            )
            val unwiredRepoResult = serviceUnwiredRepo.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )

            // 2. Fail-closed test when TransactionTemplate is null or has null PlatformTransactionManager
            val serviceMissingTx = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = exactSampleRepo,
                transactionTemplate = null
            )
            val missingTxResult = serviceMissingTx.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )
            val serviceEmptyTxTemplate = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = exactSampleRepo,
                transactionTemplate = org.springframework.transaction.support.TransactionTemplate()
            )
            val emptyTxManagerResult = serviceEmptyTxTemplate.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )

            // 2b. Network-free fail-closed patch resolver on completely cold cache (no Redis ddragon key, no DB grounded patches):
            //     Must return GROUNDED_PATCH_UNAVAILABLE with 0 writes and 0 RiotApiService interactions (no fetchDataDragonVersions call!)
            val coldCacheService = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = exactSampleRepo,
                transactionTemplate = validTxTemplate
            )
            val coldCacheUnavailablePatchResult = coldCacheService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = null
            )

            // Now seed grounded patch in Redis ("cohort:patch:external:ddragon" -> "16.22") while in-memory DataDragon cache on new services is still cold
            redisMap["cohort:patch:external:ddragon"] = "16.22"

            // 3. Properly wired service: test divergent unsaved in-memory state refusal first
            val wiredService = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = exactSampleRepo,
                transactionTemplate = validTxTemplate
            )
            val divergentState = CohortBenchmarkService.DurableCellSampleStateDto(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP",
                maxGamesPerPlayerCap = 10,
                samplesByPlayerHash = mapOf(
                    "unsavedplayerhash" to listOf(
                        CohortBenchmarkService.DeidentifiedMetricSampleDto(
                            matchIdHash = "unsavedmatchhash",
                            playerHash = "unsavedplayerhash",
                            gameCreation = nowMs - 1000L,
                            rankSnapshotEpochMilli = nowMs - 2000L,
                            laneCsAt10 = 91.0
                        )
                    )
                ),
                updatedAtEpochMilli = nowMs
            )
            wiredService.putInMemoryDurableCellStateForTest(divergentState)
            val divergentRefusedResult = wiredService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )
            val savedSampleEmptyAfterDivergentRefusal = savedSampleEntities.isEmpty()
            val savedBenchmarkEmptyAfterDivergentRefusal = savedBenchmarkEntities.isEmpty()

            // Clear divergent state by creating a fresh wired service (cold in-memory DataDragon cache!) and run happy-path ingestion
            val cleanWiredService = CohortBenchmarkService(
                redisCacheService = exactRedis,
                cohortBenchmarkCellRepository = exactCellRepo,
                riotApiRateLimiter = exactLimiter,
                riotApiService = exactRiotApi,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = exactCheckpointRepo,
                cohortCellSampleStateRepository = exactSampleRepo,
                transactionTemplate = validTxTemplate
            )

            val firstIngestResult = cleanWiredService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )

            // 4. Repeat call with same cached match -> deduplicated (NO_NEW_ELIGIBLE_SAMPLES, 0 additional DB writes)
            val repeatIngestResult = cleanWiredService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )

            // 5. Exclusive gate & nested getPersistedCohortCells / getPublicCohortReport contention test:
            //    Remove benchmark Redis cache so getPersistedCohortCells enters cache-miss path while regionWriteGate is held
            redisMap.remove("cohort:benchmarks:euw1:16.22")
            redisMap.remove("cohort:report:euw1:16.22")
            cleanWiredService.tryAcquireRegionWriteGate("euw1") shouldBe true
            val ingestWhileGateHeld = cleanWiredService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = selectedPuuid,
                targetPatch = "16.22"
            )
            val reportWhileGateHeld = cleanWiredService.getPublicCohortReport("euw1", "16.22")
            val persistedCellsWhileGateHeld = cleanWiredService.getPersistedCohortCells("euw1", "16.22")
            cleanWiredService.releaseRegionWriteGate("euw1")

            // 6. Controller POST-only endpoint & operator auth verification
            val runner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = cleanWiredService,
                configuredOperatorToken = "secret-ingest-token",
                timeProvider = timeProvider
            )
            val controller = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = exactRiotApi,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = exactRedis,
                cohortBenchmarkService = cleanWiredService,
                overnightCohortCollectorRunner = runner
            )
            val request = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "203.0.113.50"
            }
            val forbiddenControllerResp = controller.ingestCachedPlayerForCohort(
                region = "euw1",
                puuid = selectedPuuid,
                patch = "16.22",
                operatorToken = "wrong-token",
                httpRequest = request
            )
            val authorizedControllerResp = controller.ingestCachedPlayerForCohort(
                region = "euw1",
                puuid = selectedPuuid,
                patch = "16.22",
                matchIds = listOf("EUW1_EXACT_11"),
                operatorToken = "secret-ingest-token",
                httpRequest = request
            )
            val ingestEndpointMethod = com.pride.summoner_searcher_api.controller.RiotApiController::class.java.methods
                .single { it.name == "ingestCachedPlayerForCohort" }
            val postMappingAnn = ingestEndpointMethod.getAnnotation(org.springframework.web.bind.annotation.PostMapping::class.java)
            val reqMappingAnn = ingestEndpointMethod.getAnnotation(org.springframework.web.bind.annotation.RequestMapping::class.java)

            // 7. Post-commit Redis cache refresh/eviction failure reports COMMITTED_WITH_CACHE_REFRESH_WARNING instead of silently COMPLETED
            failPostCommitCacheRefresh.set(true)
            val postCommitCacheWarnResult = try {
                cleanWiredService.ingestCachedPlayerByExactIds(
                    region = "euw1",
                    puuid = selectedPuuid,
                    targetPatch = "16.22",
                    explicitMatchIds = listOf("EUW1_EXACT_12")
                )
            } finally {
                failPostCommitCacheRefresh.set(false)
            }

            Then("it should fail closed without TransactionTemplate/repo/groundedPatch, refuse divergent unsaved in-memory state, bound inspection to <=10 own matches with 0 entire-RiotApiService/SCAN/checkpoint calls, report post-commit cache failure as COMMITTED_WITH_CACHE_REFRESH_WARNING, serialize full RMW intervals, and enforce POST operator auth") {
                unwiredRepoResult.status shouldBe "SAMPLE_STATE_REPOSITORY_UNWIRED"
                unwiredRepoResult.netNewSamplesPersisted shouldBe 0
                missingTxResult.status shouldBe "TRANSACTION_MANAGER_REQUIRED"
                missingTxResult.netNewSamplesPersisted shouldBe 0
                emptyTxManagerResult.status shouldBe "TRANSACTION_MANAGER_REQUIRED"
                emptyTxManagerResult.netNewSamplesPersisted shouldBe 0

                coldCacheUnavailablePatchResult.status shouldBe "GROUNDED_PATCH_UNAVAILABLE"
                coldCacheUnavailablePatchResult.netNewSamplesPersisted shouldBe 0

                divergentRefusedResult.status shouldBe "REFUSED_DIVERGENT_IN_MEMORY_CELL_STATE"
                divergentRefusedResult.netNewSamplesPersisted shouldBe 0
                savedSampleEmptyAfterDivergentRefusal shouldBe true
                savedBenchmarkEmptyAfterDivergentRefusal shouldBe true

                firstIngestResult.status shouldBe "COMPLETED"
                firstIngestResult.tier shouldBe "CHALLENGER"
                firstIngestResult.inspectedMatchIdsCount shouldBe 10 // capped at 10 despite 12 in profile.allMatchIds
                firstIngestResult.qualifiedSamplesExtracted shouldBe 1
                firstIngestResult.netNewSamplesPersisted shouldBe 1
                firstIngestResult.excludedWrongPatchMatches shouldBe 1
                firstIngestResult.excludedStaleOrSkewMatches shouldBe 1
                firstIngestResult.excludedNonSoloOrRemakeMatches shouldBe 1
                firstIngestResult.excludedIneligibleRoleMatches shouldBe 1
                firstIngestResult.missingOrUncachedMatches shouldBe 5
                firstIngestResult.updatedCells.size shouldBe 1
                firstIngestResult.updatedCells.single().tier shouldBe "CHALLENGER"
                firstIngestResult.updatedCells.single().role shouldBe "TOP"
                firstIngestResult.updatedCells.single().eligibleGames shouldBe 1
                firstIngestResult.updatedCells.single().distinctPlayers shouldBe 1
                // De-identified playerHash: never contains raw PUUID
                firstIngestResult.playerHash shouldNotBe null
                firstIngestResult.playerHash!!.contains(selectedPuuid) shouldBe false
                // Co-participant (GOLD:MIDDLE) was NOT ingested
                savedSampleEntities.keys shouldBe setOf("euw1:16.22:420:CHALLENGER:TOP")
                savedBenchmarkEntities.keys shouldBe setOf("euw1:16.22:420:CHALLENGER:TOP")
                // Because inMemoryDurableCellStates did not contain CHALLENGER:TOP before firstIngestResult, it remains unpopulated
                cleanWiredService.getInMemoryDurableCellStateForTest("euw1", "16.22", "CHALLENGER", "TOP") shouldBe null

                // Repeat call deduplicates with 0 new DB writes
                repeatIngestResult.status shouldBe "NO_NEW_ELIGIBLE_SAMPLES"
                repeatIngestResult.netNewSamplesPersisted shouldBe 0

                // Exclusive gate contention: refuses concurrent ingest, serves non-empty persisted cells on report & getPersistedCohortCells
                ingestWhileGateHeld.status shouldBe "LOCKED_BY_ACTIVE_WRITER"
                reportWhileGateHeld.reportSource shouldBe "GUARDED_COOLDOWN_FALLBACK"
                reportWhileGateHeld.cells.size shouldBe 1
                reportWhileGateHeld.cells.single().eligibleGames shouldBe 1
                persistedCellsWhileGateHeld.size shouldBe 1
                persistedCellsWhileGateHeld.single().eligibleGames shouldBe 1

                // Controller POST-only & operator auth
                postMappingAnn shouldNotBe null
                reqMappingAnn shouldBe null
                forbiddenControllerResp.statusCode shouldBe org.springframework.http.HttpStatus.FORBIDDEN
                authorizedControllerResp.statusCode shouldBe org.springframework.http.HttpStatus.OK
                authorizedControllerResp.body!!.status shouldBe "COMPLETED"
                authorizedControllerResp.body!!.netNewSamplesPersisted shouldBe 1
                authorizedControllerResp.body!!.updatedCells.single().eligibleGames shouldBe 2
                // Runner session was NOT armed or started by /ingest-cached
                runner.getStatus("euw1", "16.22").overnightSessionArmed shouldBe false
                runner.getStatus("euw1", "16.22").runningInProcess shouldBe false

                // Post-commit Redis cache refresh/eviction failure reports COMMITTED_WITH_CACHE_REFRESH_WARNING while preserving committed DB state
                postCommitCacheWarnResult.status shouldBe "COMMITTED_WITH_CACHE_REFRESH_WARNING"
                postCommitCacheWarnResult.netNewSamplesPersisted shouldBe 1
                postCommitCacheWarnResult.updatedCells.single().eligibleGames shouldBe 3
                postCommitCacheWarnResult.notes.any { it.contains("post-commit Redis cache refresh/eviction encountered warning") } shouldBe true
                cleanWiredService.getDurableDbWriteCounts()["cohortCellSampleStatesWrites"] shouldBe 3L
                cleanWiredService.getDurableDbWriteCounts()["cohortBenchmarkCellsWrites"] shouldBe 3L
                cleanWiredService.getDurableDbWriteCounts()["cohortCollectorCheckpointsWrites"] shouldBe 0L

                // Zero interactions on the ENTIRE RiotApiService mock (including fetchDataDragonVersions on cold cache!), zero Redis SCANs, zero checkpoint saves
                verify { exactRiotApi wasNot Called }
                verify(exactly = 0) { exactRedis.scanKeys(any(), any(), any()) }
                verify(exactly = 0) { exactCheckpointRepo.save(any()) }
            }
        }

        When("collector batches and low-priority steps resolve MatchDtos with IndexedPlayerRepository wired, unwired, failing, or cancelled") {
            val nowMs = nowInstant.toEpochMilli()
            val timeProvider = object : TimeProvider {
                override fun now(): Long = nowMs
            }

            fun buildNamedParticipant(
                puuid: String,
                role: String,
                gameName: String,
                tagLine: String,
                profileIcon: Int = 55,
                summonerLevel: Int = 250
            ): ParticipantDto {
                val json = """
                    {
                      "puuid": "$puuid",
                      "riotIdGameName": "$gameName",
                      "riotIdTagline": "$tagLine",
                      "profileIcon": $profileIcon,
                      "summonerLevel": $summonerLevel,
                      "teamId": 100,
                      "teamPosition": "$role",
                      "individualPosition": "$role",
                      "kills": 7,
                      "deaths": 2,
                      "assists": 9,
                      "totalMinionsKilled": 220,
                      "neutralMinionsKilled": 10,
                      "goldEarned": 14000,
                      "totalDamageDealtToChampions": 28000,
                      "visionScore": 36,
                      "totalTimeSpentDead": 45,
                      "win": true,
                      "challenges": {
                        "laneMinionsFirst10Minutes": 86,
                        "jungleCsBefore10Minutes": 4.0,
                        "goldPerMinute": 466.0,
                        "damagePerMinute": 933.0,
                        "visionScorePerMinute": 1.2,
                        "killParticipation": 0.72
                      }
                    }
                """.trimIndent()
                return objectMapper.readValue(json, ParticipantDto::class.java)
            }

            val inMemoryIndexStore = java.util.concurrent.ConcurrentHashMap<String, com.pride.summoner_searcher_api.model.IndexedPlayer>()
            val mockIndexedRepo = mockk<com.pride.summoner_searcher_api.repository.IndexedPlayerRepository>()
            every {
                mockIndexedRepo.updatePlayerIfNewer(any(), any(), any(), any(), any(), any(), any())
            } answers {
                val pid = arg<String>(0)
                val gName = arg<String>(1)
                val tLine = arg<String>(2)
                val reg = arg<String>(3)
                val icon = arg<Int>(4)
                val lvl = arg<Long>(5)
                val seen = arg<Instant>(6)
                val existing = inMemoryIndexStore[pid]
                if (existing != null && !seen.isBefore(existing.lastSeenAt)) {
                    inMemoryIndexStore[pid] = com.pride.summoner_searcher_api.model.IndexedPlayer(
                        puuid = pid,
                        gameName = gName,
                        tagLine = tLine,
                        region = reg,
                        profileIconId = icon,
                        summonerLevel = lvl,
                        lastSeenAt = seen
                    )
                    1
                } else {
                    0
                }
            }
            every {
                mockIndexedRepo.insertPlayerIfAbsent(any(), any(), any(), any(), any(), any(), any())
            } answers {
                val pid = arg<String>(0)
                val gName = arg<String>(1)
                val tLine = arg<String>(2)
                val reg = arg<String>(3)
                val icon = arg<Int>(4)
                val lvl = arg<Long>(5)
                val seen = arg<Instant>(6)
                val inserted = inMemoryIndexStore.putIfAbsent(
                    pid,
                    com.pride.summoner_searcher_api.model.IndexedPlayer(
                        puuid = pid,
                        gameName = gName,
                        tagLine = tLine,
                        region = reg,
                        profileIconId = icon,
                        summonerLevel = lvl,
                        lastSeenAt = seen
                    )
                )
                if (inserted == null) 1 else 0
            }

            val stepRedis = mockk<RedisCacheService>(relaxed = true)
            val stepRiot = mockk<RiotApiService>(relaxed = true)
            val stepCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val stepLimiter = RiotApiRateLimiter(timeProvider)
            for (ep in listOf("league-v4-entries-by-puuid", "match-v5-ids-by-puuid", "match-v5-match-by-id")) {
                stepLimiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,5:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val mOld = MatchDto(
                metadata = MatchMetadata("EUW1_STEP_OLD", listOf("step-seed", "step-sup")),
                info = MatchInfo(
                    gameCreation = nowMs - 20_000L,
                    gameDuration = 1800L,
                    gameId = 9001L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildNamedParticipant("step-seed", "TOP", "StepSeedOld", "OLD"),
                        buildNamedParticipant("step-sup", "UTILITY", "StepSup", "EUW")
                    )
                )
            )
            val mNew = MatchDto(
                metadata = MatchMetadata("EUW1_STEP_NEW", listOf("step-seed", "step-jg")),
                info = MatchInfo(
                    gameCreation = nowMs - 5_000L,
                    gameDuration = 1800L,
                    gameId = 9002L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildNamedParticipant("step-seed", "TOP", "StepSeedNew", "NEW"),
                        buildNamedParticipant("step-jg", "JUNGLE", "StepJg", "EUW")
                    )
                )
            )

            every { stepRedis.get(any(), any<Class<Any>>()) } returns null
            every { stepRedis.get("match:euw1:EUW1_STEP_OLD", MatchDto::class.java) } returns mOld
            every { stepRedis.get("match:euw1:EUW1_STEP_NEW", MatchDto::class.java) } returns null
            coEvery {
                stepRiot.getMatchIdsByPuuidLowPriority("step-seed", "euw1", 420, any(), 0)
            } returns listOf("EUW1_STEP_OLD", "EUW1_STEP_NEW")
            coEvery {
                stepRiot.getMatchByIdLowPriority("EUW1_STEP_NEW", "euw1")
            } returns mNew

            val stepService = CohortBenchmarkService(
                redisCacheService = stepRedis,
                cohortBenchmarkCellRepository = stepCellRepo,
                riotApiRateLimiter = stepLimiter,
                riotApiService = stepRiot,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = timeProvider,
                indexedPlayerRepository = mockIndexedRepo
            )

            val stepResult = stepService.runGuardedLowPriorityCollectionStep(
                region = "euw1",
                candidatePuuids = listOf("step-seed")
            )

            // Verify diagnostic probe also indexes resolved MatchDto participants safely
            val probeMatch = MatchDto(
                metadata = MatchMetadata("EUW1_PROBE_1", listOf("probe-mid")),
                info = MatchInfo(
                    gameCreation = nowMs - 1_000L,
                    gameDuration = 1800L,
                    gameId = 9003L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.22.615.1000",
                    participants = listOf(
                        buildNamedParticipant("probe-mid", "MIDDLE", "ProbeMid", "EUW")
                    )
                )
            )
            val probeLimiter = RiotApiRateLimiter(timeProvider)
            probeLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "1:1,5:120",
                endpointKey = "league-v4-entries-by-puuid",
                methodLimitHeader = "500:10",
                methodCountHeader = "1:10"
            )
            probeLimiter.updateLimits(
                region = "euw1",
                limitHeader = "20:1,100:120",
                countHeader = "1:1,5:120",
                endpointKey = "match-v5-ids-by-puuid",
                methodLimitHeader = "500:10",
                methodCountHeader = "1:10"
            )
            coEvery {
                stepRiot.getMatchByIdLowPriority("EUW1_PROBE_1", "euw1", 0, any(), any())
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(3).incrementAndGet()
                kotlinx.coroutines.runBlocking {
                    probeLimiter.updateLimits(
                        region = "euw1",
                        limitHeader = "20:1,100:120",
                        countHeader = "1:1,5:120",
                        endpointKey = "match-v5-match-by-id",
                        methodLimitHeader = "500:10",
                        methodCountHeader = "1:10"
                    )
                }
                probeMatch
            }
            val probeService = CohortBenchmarkService(
                redisCacheService = stepRedis,
                cohortBenchmarkCellRepository = stepCellRepo,
                riotApiRateLimiter = probeLimiter,
                riotApiService = stepRiot,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = timeProvider,
                indexedPlayerRepository = mockIndexedRepo
            )
            val probeResult = probeService.runBoundedDiagnosticRateLimitProbe(
                region = "euw1",
                explicitSeedPuuid = "step-seed",
                explicitSeedMatchId = "EUW1_PROBE_1"
            )

            Then("low-priority step and diagnostic probe should index deduplicated newest participant identities across all roles with zero extra network calls") {
                stepResult["executed"] shouldBe true
                stepResult["fetchedMatches"] shouldBe 1
                inMemoryIndexStore["step-seed"]?.gameName shouldBe "StepSeedNew"
                inMemoryIndexStore["step-seed"]?.tagLine shouldBe "NEW"
                inMemoryIndexStore["step-seed"]?.lastSeenAt?.toEpochMilli() shouldBe (nowMs - 5_000L)
                inMemoryIndexStore["step-sup"]?.gameName shouldBe "StepSup"
                inMemoryIndexStore["step-jg"]?.gameName shouldBe "StepJg"
                stepService.getCollectorParticipantIndexingCounts()["collectorIndexedPlayersUpserted"] shouldBe 3L

                probeResult.totalProbeCallsMade shouldBe 1
                inMemoryIndexStore["probe-mid"]?.gameName shouldBe "ProbeMid"
                probeService.getCollectorParticipantIndexingCounts()["collectorIndexedPlayersUpserted"] shouldBe 1L
            }
        }

        When("starting the guarded collector with operator seedPuuid control through RiotApiController and OvernightCohortCollectorRunner") {
            val clockMs = java.util.concurrent.atomic.AtomicLong(nowInstant.toEpochMilli())
            val testClock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val limiter = RiotApiRateLimiter(testClock)
            for (ep in listOf("league-v4-entries-by-puuid", "match-v5-ids-by-puuid", "match-v5-match-by-id")) {
                limiter.updateLimits(
                    region = "euw1",
                    limitHeader = "20:1,100:120",
                    countHeader = "1:1,5:120",
                    endpointKey = ep,
                    methodLimitHeader = "500:10",
                    methodCountHeader = "1:10"
                )
            }

            val seedRedis = mockk<RedisCacheService>(relaxed = true)
            val seedRiot = mockk<RiotApiService>(relaxed = true)
            val seedCellRepo = mockk<CohortBenchmarkCellRepository>(relaxed = true)
            val savedCheckpoints = mutableMapOf<String, CohortCollectorCheckpointDto>()
            val savedMatches = mutableMapOf<String, MatchDto>()
            val inMemoryIndexStore = java.util.concurrent.ConcurrentHashMap<String, com.pride.summoner_searcher_api.model.IndexedPlayer>()
            var activeLockHeld = false
            var lockAcquireCount = 0

            val mockIndexedRepo = mockk<com.pride.summoner_searcher_api.repository.IndexedPlayerRepository>(relaxed = true)
            every {
                mockIndexedRepo.updatePlayerIfNewer(any(), any(), any(), any(), any(), any(), any())
            } answers {
                val pid = arg<String>(0)
                val gName = arg<String>(1)
                val tLine = arg<String>(2)
                val reg = arg<String>(3)
                val icon = arg<Int>(4)
                val lvl = arg<Long>(5)
                val seen = arg<Instant>(6)
                val current = inMemoryIndexStore[pid]
                if (current != null && !seen.isBefore(current.lastSeenAt)) {
                    inMemoryIndexStore[pid] = com.pride.summoner_searcher_api.model.IndexedPlayer(
                        puuid = pid,
                        gameName = gName,
                        tagLine = tLine,
                        region = reg,
                        profileIconId = icon,
                        summonerLevel = lvl,
                        lastSeenAt = seen
                    )
                    1
                } else {
                    0
                }
            }
            every {
                mockIndexedRepo.insertPlayerIfAbsent(any(), any(), any(), any(), any(), any(), any())
            } answers {
                val pid = arg<String>(0)
                val gName = arg<String>(1)
                val tLine = arg<String>(2)
                val reg = arg<String>(3)
                val icon = arg<Int>(4)
                val lvl = arg<Long>(5)
                val seen = arg<Instant>(6)
                val inserted = inMemoryIndexStore.putIfAbsent(
                    pid,
                    com.pride.summoner_searcher_api.model.IndexedPlayer(
                        puuid = pid,
                        gameName = gName,
                        tagLine = tLine,
                        region = reg,
                        profileIconId = icon,
                        summonerLevel = lvl,
                        lastSeenAt = seen
                    )
                )
                if (inserted == null) 1 else 0
            }

            // Live EUW equivalent state: CHALLENGER:MIDDLE and CHALLENGER:BOTTOM pass (50 players, 200 games),
            // while CHALLENGER:TOP fails at 192 games (< 200).
            val dummyMetric = CohortPercentileDistributionDto(
                metricKey = "laneCsAt10", unit = "lane_cs", includesJungleCamps = false,
                theoreticalSupplyCeiling = 114, sampleGames = 200, distinctPlayers = 50,
                p25 = 80.0, median = 86.0, p75 = 92.0, iqr = 12.0
            )
            val dummyBundle = CohortMetricsBundleDto(
                laneCsAt10 = dummyMetric, laneOnlyCsPerMin = dummyMetric, totalCsPerMinWithCamps = dummyMetric,
                timelineTotalCsAt10WithCamps = dummyMetric, goldPerMin = dummyMetric, damagePerMin = dummyMetric,
                visionScorePerMin = dummyMetric, killParticipation = dummyMetric, deathsPerGame = dummyMetric,
                timeSpentDeadShare = dummyMetric
            )
            val initialPersistedCells = listOf(
                CohortBenchmarkCellDto(
                    region = "euw1", patch = "16.7", patchSelectionSource = "GROUNDED_CURRENT_PATCH_STORE",
                    queueId = 420, tier = "CHALLENGER", role = "TOP", distinctPlayers = 50, eligibleGames = 192,
                    maxGamesPerPlayerCap = 10, excludedOverPlayerCapGames = 0, passesQualityGate = false,
                    colorBandsEnabled = false, qualityGateStatus = "INSUFFICIENT_SAMPLE",
                    earliestRankSnapshotAt = nowInstant, latestRankSnapshotAt = nowInstant, computedAt = nowInstant,
                    metrics = dummyBundle
                ),
                CohortBenchmarkCellDto(
                    region = "euw1", patch = "16.7", patchSelectionSource = "GROUNDED_CURRENT_PATCH_STORE",
                    queueId = 420, tier = "CHALLENGER", role = "MIDDLE", distinctPlayers = 50, eligibleGames = 210,
                    maxGamesPerPlayerCap = 10, excludedOverPlayerCapGames = 0, passesQualityGate = true,
                    colorBandsEnabled = true, qualityGateStatus = "READY_FOR_COLOR_BANDS",
                    earliestRankSnapshotAt = nowInstant, latestRankSnapshotAt = nowInstant, computedAt = nowInstant,
                    metrics = dummyBundle
                ),
                CohortBenchmarkCellDto(
                    region = "euw1", patch = "16.7", patchSelectionSource = "GROUNDED_CURRENT_PATCH_STORE",
                    queueId = 420, tier = "CHALLENGER", role = "BOTTOM", distinctPlayers = 50, eligibleGames = 205,
                    maxGamesPerPlayerCap = 10, excludedOverPlayerCapGames = 0, passesQualityGate = true,
                    colorBandsEnabled = true, qualityGateStatus = "READY_FOR_COLOR_BANDS",
                    earliestRankSnapshotAt = nowInstant, latestRankSnapshotAt = nowInstant, computedAt = nowInstant,
                    metrics = dummyBundle
                )
            )

            val freshLeaderboard = CachedLeaderboardDto(
                lastRefreshed = nowInstant,
                leaderboard = LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "euw-challenger",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger",
                    entries = listOf(
                        LeagueEntryDto(
                            puuid = "already-done-seed", leaguePoints = 1100, rank = "I", wins = 100, losses = 80,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                            leagueId = "euw-challenger", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", summonerName = "DoneSeed"
                        ),
                        LeagueEntryDto(
                            puuid = "fresh-challenger-seed", leaguePoints = 1250, rank = "I", wins = 120, losses = 90,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = true,
                            leagueId = "euw-challenger", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", summonerName = "FreshChal"
                        ),
                        LeagueEntryDto(
                            puuid = "manual-chunk-only-seed", leaguePoints = 1190, rank = "I", wins = 115, losses = 85,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                            leagueId = "euw-challenger", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", summonerName = "ManualOnly"
                        ),
                        LeagueEntryDto(
                            puuid = "seed-429-test", leaguePoints = 1180, rank = "I", wins = 110, losses = 85,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                            leagueId = "euw-challenger", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", summonerName = "Seed429"
                        ),
                        LeagueEntryDto(
                            puuid = "async-challenger-seed", leaguePoints = 1170, rank = "I", wins = 108, losses = 82,
                            veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                            leagueId = "euw-challenger", queueType = "RANKED_SOLO_5x5", tier = "CHALLENGER", summonerName = "AsyncChal"
                        )
                    )
                )
            )

            every { seedRedis.scanKeys(any(), any(), any()) } returns emptyList()
            every { seedRedis.get(any(), CohortCollectorCheckpointDto::class.java) } answers {
                savedCheckpoints[firstArg()]
            }
            every { seedRedis.get("cohort:benchmarks:euw1:16.7", Array<CohortBenchmarkCellDto>::class.java) } returns initialPersistedCells.toTypedArray()
            every { seedRedis.get(match { it.startsWith("leaderboard:euw1:") }, CachedLeaderboardDto::class.java) } returns freshLeaderboard
            every { seedRedis.get(any(), CohortRankSnapshotDto::class.java) } returns null
            every { seedRedis.get(any(), SummonerProfileDto::class.java) } returns null
            every { seedRedis.get(match { it.startsWith("match:euw1:") }, MatchDto::class.java) } answers {
                savedMatches[firstArg()]
            }
            every { seedRedis.get("cohort:patch:external:ddragon", String::class.java) } returns "16.7"
            every { seedRedis.setIfAbsent(match { it.startsWith("lock:cohort:collector:active:") }, any(), any()) } answers {
                if (activeLockHeld) false else {
                    activeLockHeld = true
                    lockAcquireCount++
                    true
                }
            }
            every { seedRedis.delete(match { it.startsWith("lock:cohort:collector:active:") }) } answers {
                activeLockHeld = false
            }
            every { seedRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> savedCheckpoints[key] = value
                    is MatchDto -> savedMatches[key] = value
                }
            }

            fun buildNamedParticipant(puuid: String, role: String, gameName: String, tagLine: String): ParticipantDto {
                return buildParticipant(puuid = puuid, role = role, laneCs10 = 90, totalMinions = 220, neutralMinions = 8).copy(
                    riotIdGameName = gameName,
                    riotIdTagline = tagLine,
                    profileIcon = 42,
                    summonerLevel = 400
                )
            }

            val seedService = CohortBenchmarkService(
                redisCacheService = seedRedis,
                cohortBenchmarkCellRepository = seedCellRepo,
                riotApiRateLimiter = limiter,
                riotApiService = seedRiot,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = testClock,
                indexedPlayerRepository = mockIndexedRepo
            )
            val seedRunner = com.pride.summoner_searcher_api.scheduler.OvernightCohortCollectorRunner(
                cohortBenchmarkService = seedService,
                configuredOperatorToken = "operator-secret-123",
                timeProvider = testClock
            )
            val seedController = com.pride.summoner_searcher_api.controller.RiotApiController(
                riotApiService = seedRiot,
                challengerLeagueService = mockk(relaxed = true),
                userService = mockk(relaxed = true),
                summonerProfileService = mockk(relaxed = true),
                redisCacheService = seedRedis,
                cohortBenchmarkService = seedService,
                overnightCohortCollectorRunner = seedRunner
            )
            val httpReq = org.springframework.mock.web.MockHttpServletRequest().apply {
                remoteAddr = "203.0.113.10"
            }

            val tail499 = (1..499).map { "uncached-pending-$it" }
            val baselineCheckpoint499 = CohortCollectorCheckpointDto(
                region = "euw1",
                targetPatch = "16.7",
                patchSelectionSource = "GROUNDED_CURRENT_PATCH_STORE",
                queueId = 420,
                roles = listOf("TOP", "MIDDLE", "BOTTOM"),
                maxGamesPerPlayerCap = 10,
                targetPlayersPerCell = 50,
                targetGamesPerCell = 200,
                processedPuuids = setOf("already-done-seed"),
                pendingCandidatePuuids = tail499,
                processedMatchIds = setOf("EUW1_PREV_1"),
                playerCellGameCounts = mapOf("CHALLENGER:TOP:already-done-seed" to 4),
                cellProgress = emptyMap(),
                cellsPassingQualityGate = 2,
                targetCellsCount = 30,
                totalNetworkCallsMade = 100,
                startedAt = nowInstant,
                updatedAt = nowInstant
            )
            savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"] = baselineCheckpoint499
            savedCheckpoints["cohort:collector:checkpoint:euw1:latest"] = baselineCheckpoint499

            // 1. Auth guard on seeded start
            val unauthorizedResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("fresh-challenger-seed"),
                async = false,
                operatorToken = "wrong-token",
                httpRequest = httpReq
            )

            // 2. Named seed limits (25 max count, 250 max length, format checks) rejected before lock/mutation
            val tooManySeedsResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                seedPuuids = (1..26).map { "seed-$it" },
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            val overLengthSeedResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                seedPuuids = listOf("x".repeat(251)),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            val malformedSeedResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                seedPuuids = listOf("bad,seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            val lockAcquireCountAfterInvalidFormat = lockAcquireCount

            // 3. Reject seeds exceeding 500 total queue BEFORE mutation (499 pending + 2 net-new seeds = 501 > 500)
            val exceed500From499Resp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("fresh-challenger-seed", "extra-new-seed-2"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!

            // Also test 500 full pending queue + 1 net-new seed (500 + 1 = 501 > 500) rejected before mutation
            val tail500 = (1..500).map { "uncached-pending-$it" }
            val full500Checkpoint = baselineCheckpoint499.copy(pendingCandidatePuuids = tail500)
            savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"] = full500Checkpoint
            savedCheckpoints["cohort:collector:checkpoint:euw1:latest"] = full500Checkpoint
            val exceed500From500Resp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("fresh-challenger-seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            val checkpointAfterCapacityReject = savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"]!!

            // 3b. Stale-patch advance (16.6 -> 16.7) capacity test + seed-only runner latest fallback:
            //     Checkpoint on 16.6 (stored only at "latest", with "16.7" key absent) has 490 pending + 10 processedPuuids.
            //     On patch advance to 16.7, all 10 old processedPuuids are requeued into pendingQueue (490 + 10 = 500).
            //     Adding 1 net-new seed ("fresh-challenger-seed") would make 501 > 500 and truncate "old-proc-10", so both
            //     wouldExceedPendingQueueCap(..., checkpointStalePatchAdvance = true) and startOrResumeCohortCollector MUST refuse before mutation.
            val staleAdvancePending490 = (1..490).map { "uncached-pending-$it" }
            val staleAdvanceProcessed10 = (1..10).map { "old-proc-$it" }.toSet()
            val stalePatch166Checkpoint = baselineCheckpoint499.copy(
                targetPatch = "16.6",
                pendingCandidatePuuids = staleAdvancePending490,
                processedPuuids = staleAdvanceProcessed10,
                totalNetworkCallsMade = 42
            )
            savedCheckpoints.remove("cohort:collector:checkpoint:euw1:16.7")
            savedCheckpoints["cohort:collector:checkpoint:euw1:16.6"] = stalePatch166Checkpoint
            savedCheckpoints["cohort:collector:checkpoint:euw1:latest"] = stalePatch166Checkpoint

            val directStaleCapExceedsWithNetNew = seedService.wouldExceedPendingQueueCap(
                existingCheckpoint = stalePatch166Checkpoint,
                seedPuuids = listOf("fresh-challenger-seed"),
                checkpointStalePatchAdvance = true
            )
            val directStaleCapFitsWithExistingOldProcessed = seedService.wouldExceedPendingQueueCap(
                existingCheckpoint = stalePatch166Checkpoint,
                seedPuuids = listOf("old-proc-1"),
                checkpointStalePatchAdvance = true
            )
            val exceed500OnStalePatchAdvanceResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("fresh-challenger-seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            val patch167CheckpointStillAbsentAfterStaleReject = !savedCheckpoints.containsKey("cohort:collector:checkpoint:euw1:16.7")

            // Verify unseeded startOrResumeGuarded does NOT use the latest fallback in startOrResumeInternal when patch="16.7" is absent:
            // Because existingCheckpoint for ("euw1", "16.7") is null, sessionBaselineNetworkCalls is set to 0 (not 42 from "latest"),
            // so when runResumableLowPriorityCollectorBatch loads "latest" (totalNetworkCallsMade = 42) with maxTotalNetworkCalls = 10,
            // currentSessionNetworkCalls() is 42 - 0 = 42 >= 10 -> immediately stops with SESSION_NETWORK_CALL_CAP_REACHED and 0 new Riot calls.
            val unseededNoLatestFallbackResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 10,
                seedPuuids = emptyList(),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!

            // Restore 499 pending queue for the valid 499 + 1 net-new seed (== 500) + 1 already-processed seed (0 net-new) run
            savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"] = baselineCheckpoint499
            savedCheckpoints["cohort:collector:checkpoint:euw1:latest"] = baselineCheckpoint499

            // 4. Lock guard when Redis lock is already held
            activeLockHeld = true
            val lockedResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                seedPuuids = listOf("fresh-challenger-seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            activeLockHeld = false

            // 5. Seeded 1-batch / 15-call finite run with 1 initial pause retry:
            //    - Attempt 1 pauses due to active foreground user request (verifies 500-entry queue has fresh-challenger-seed at index 0 and uncached-pending-499 at index 499 with zero tail loss).
            //    - During pauseDelayMs, foreground request ends; Attempt 2 retains seedPuuids, ignores already-done-seed without erasing progress,
            //      resolves fresh-challenger-seed tier from cached leaderboard (0 league-v4 calls), fetches 1 match list + 10 matches (11 <= 15 calls),
            //      folds CHALLENGER:TOP samples, and triggers the normal collector DTO participant indexing hook.
            var rankLookupCalls = 0
            coEvery { seedRiot.fetchLeagueRankLowPriority(any(), any(), any(), any(), any()) } answers {
                rankLookupCalls++
                arg<java.util.concurrent.atomic.AtomicInteger>(3).incrementAndGet()
                null
            }
            val matchIdsForFreshSeed = (1..10).map { "EUW1_SEED_M$it" }
            coEvery {
                seedRiot.getMatchIdsByPuuidLowPriority("fresh-challenger-seed", "euw1", 420, 10, 0, 0, any(), any())
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                matchIdsForFreshSeed
            }
            var matchDetailCalls = 0
            coEvery {
                seedRiot.getMatchByIdLowPriority(match { it.startsWith("EUW1_SEED_M") }, "euw1", 0, any(), any())
            } answers {
                val mId = firstArg<String>()
                val idx = mId.removePrefix("EUW1_SEED_M").toInt()
                matchDetailCalls++
                arg<java.util.concurrent.atomic.AtomicInteger>(3).incrementAndGet()
                MatchDto(
                    metadata = MatchMetadata(mId, listOf("fresh-challenger-seed", "co-part-$idx")),
                    info = MatchInfo(
                        gameCreation = clockMs.get() - idx * 1_000L,
                        gameDuration = 1800L,
                        gameId = 7000L + idx,
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.7.600.1000",
                        participants = listOf(
                            buildNamedParticipant("fresh-challenger-seed", "TOP", "FreshChal", "EUW"),
                            buildNamedParticipant("co-part-$idx", "JUNGLE", "CoPart$idx", "EUW")
                        )
                    )
                )
            }

            val initialChalTopSamples = LinkedHashMap<String, List<CohortBenchmarkService.DeidentifiedMetricSampleDto>>()
            for (pIdx in 1..50) {
                val pHash = seedService.hashIdentifier("player:euw1:existing-chal-top-$pIdx")
                val gamesForPlayer = if (pIdx <= 42) 4 else 3 // 42*4 + 8*3 = 168 + 24 = 192 games across 50 players
                initialChalTopSamples[pHash] = (1..gamesForPlayer).map { gIdx ->
                    CohortBenchmarkService.DeidentifiedMetricSampleDto(
                        matchIdHash = seedService.hashIdentifier("match:euw1:EUW1_INIT_${pIdx}_$gIdx"),
                        playerHash = pHash,
                        gameCreation = clockMs.get() - 50_000L - (pIdx * 100L + gIdx),
                        rankSnapshotEpochMilli = clockMs.get() - 10_000L,
                        laneCsAt10 = 88.0,
                        laneOnlyCsPerMin = 8.2,
                        totalCsPerMinWithCamps = 8.5,
                        timelineTotalCsAt10WithCamps = 90.0,
                        goldPerMin = 430.0,
                        damagePerMin = 680.0,
                        visionScorePerMin = 1.2,
                        killParticipation = 0.58,
                        deathsPerGame = 3.0,
                        timeSpentDeadShare = 0.06
                    )
                }
            }
            val initialChalTopState = CohortBenchmarkService.DurableCellSampleStateDto(
                region = "euw1",
                patch = "16.7",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP",
                maxGamesPerPlayerCap = 10,
                samplesByPlayerHash = initialChalTopSamples,
                updatedAtEpochMilli = clockMs.get()
            )
            seedService.putInMemoryDurableCellStateForTest(initialChalTopState)

            // Trigger 1 pause cycle on Attempt 1, then clear foreground request when checkpoint is saved on pause
            var pausedQueueSnapshotOnAttempt1: List<String>? = null
            limiter.beginUserRequest("euw1")
            every { seedRedis.set(any(), any<Any>(), any()) } answers {
                val key = firstArg<String>()
                when (val value = secondArg<Any>()) {
                    is CohortCollectorCheckpointDto -> {
                        savedCheckpoints[key] = value
                        if (value.lastStopReason == "PAUSED_FOREGROUND_USER_REQUEST" && pausedQueueSnapshotOnAttempt1 == null) {
                            pausedQueueSnapshotOnAttempt1 = value.pendingCandidatePuuids
                            limiter.endUserRequest("euw1")
                            clockMs.addAndGet(3_000L)
                        }
                    }
                    is MatchDto -> savedMatches[key] = value
                }
            }

            val seededRunStatus = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 1L,
                maxPauseCycles = 3,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("already-done-seed", "fresh-challenger-seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!

            // Restore CHALLENGER:TOP to 192 games so subsequent Challenger seeds in Steps 6-8 also enter Step C
            seedService.putInMemoryDurableCellStateForTest(initialChalTopState)

            // 6. First manual chunk only & no scheduled auto-resume replay:
            //    Run manual chunk with maxBatches=1, maxTotalProductiveBatches=2 and seedPuuids=["manual-chunk-only-seed"].
            //    Batch 1 processes "manual-chunk-only-seed" and leaves session armed (BATCH_BUDGET_REACHED_RESUMABLE).
            //    Then remove "manual-chunk-only-seed" from processedPuuids in the saved checkpoint before calling checkAndAutoResumeOvernightSession
            //    to prove scheduled auto-resume passes emptyList() and processes "uncached-pending-1" instead of replaying "manual-chunk-only-seed".
            val processedInManualVsScheduled = mutableListOf<String>()
            coEvery {
                seedRiot.getMatchIdsByPuuidLowPriority("manual-chunk-only-seed", "euw1", 420, 10, 0, 0, any(), any())
            } answers {
                processedInManualVsScheduled.add("manual-chunk-only-seed")
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                emptyList()
            }
            coEvery {
                seedRiot.fetchLeagueRankLowPriority("uncached-pending-1", "euw1", 0, any(), any())
            } answers {
                processedInManualVsScheduled.add("uncached-pending-1")
                arg<java.util.concurrent.atomic.AtomicInteger>(3).incrementAndGet()
                null
            }
            val manualChunk1Status = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 1L,
                maxPauseCycles = 2,
                maxTotalProductiveBatches = 2,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("manual-chunk-only-seed"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            // Remove manual-chunk-only-seed from processedPuuids in checkpoint so if scheduled auto-resume re-injected seedPuuids it would run again
            val cpAfterManualChunk1 = savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"]!!
            val cpWithoutManualProcessed = cpAfterManualChunk1.copy(
                processedPuuids = cpAfterManualChunk1.processedPuuids - "manual-chunk-only-seed"
            )
            savedCheckpoints["cohort:collector:checkpoint:euw1:16.7"] = cpWithoutManualProcessed
            savedCheckpoints["cohort:collector:checkpoint:euw1:latest"] = cpWithoutManualProcessed
            val autoResumedStatus = seedRunner.checkAndAutoResumeOvernightSession(async = false)!!

            // 7. 429 Retry-After backoff on seeded run (maxPauseCycles=1 -> PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE)
            coEvery {
                seedRiot.getMatchIdsByPuuidLowPriority("seed-429-test", "euw1", 420, 10, 0, 0, any(), any())
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                arg<java.util.concurrent.atomic.AtomicReference<String?>>(7).set("HTTP_429_TOO_MANY_REQUESTS")
                limiter.recordRetryAfter("euw1", 5L, "application")
                emptyList()
            }
            val status429 = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("seed-429-test"),
                async = false,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            seedRunner.requestStop(region = "euw1", targetPatch = "16.7")
            clockMs.addAndGet(10_000L)

            // 8. Async seeded run (async = true) retains seedPuuids and completes 1 batch cleanly
            coEvery {
                seedRiot.getMatchIdsByPuuidLowPriority("async-challenger-seed", "euw1", 420, 10, 0, 0, any(), any())
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                emptyList()
            }
            val asyncStartResp = seedController.startOrResumeCohortCollector(
                region = "euw1",
                patch = "16.7",
                maxBatches = 1,
                batchSize = 1,
                pauseDelayMs = 0L,
                maxPauseCycles = 1,
                maxTotalProductiveBatches = 1,
                maxTotalNetworkCalls = 15,
                seedPuuids = listOf("async-challenger-seed"),
                async = true,
                operatorToken = "operator-secret-123",
                httpRequest = httpReq
            ).body!!
            var waitLoops = 0
            while (seedRunner.getStatus("euw1", "16.7").runningInProcess && waitLoops < 100) {
                kotlinx.coroutines.delay(10L)
                waitLoops++
            }
            val asyncFinalStatus = seedRunner.getStatus("euw1", "16.7")

            Then("it should enforce auth/locks/25-250-500 bounds before mutation, preserve processed progress and all 499 tail entries, retain seeds across pause retry without scheduled replay, and execute the DTO hook within 1 batch and 15 calls") {
                unauthorizedResp.statusCode shouldBe org.springframework.http.HttpStatus.FORBIDDEN

                CohortBenchmarkService.MAX_EXPLICIT_SEED_PUUIDS shouldBe 25
                CohortBenchmarkService.MAX_CANDIDATE_PUUID_LENGTH shouldBe 250
                tooManySeedsResp.action shouldBe "INVALID_PARAMETERS"
                overLengthSeedResp.action shouldBe "INVALID_PARAMETERS"
                malformedSeedResp.action shouldBe "INVALID_PARAMETERS"
                lockAcquireCountAfterInvalidFormat shouldBe 0

                exceed500From499Resp.action shouldBe "INVALID_PARAMETERS"
                exceed500From499Resp.runningInProcess shouldBe false
                exceed500From499Resp.overnightSessionArmed shouldBe false
                exceed500From499Resp.checkpoint?.pendingCandidatePuuids?.size shouldBe 499

                exceed500From500Resp.action shouldBe "INVALID_PARAMETERS"
                exceed500From500Resp.runningInProcess shouldBe false
                exceed500From500Resp.overnightSessionArmed shouldBe false
                checkpointAfterCapacityReject.pendingCandidatePuuids.size shouldBe 500
                checkpointAfterCapacityReject.pendingCandidatePuuids.first() shouldBe "uncached-pending-1"
                checkpointAfterCapacityReject.pendingCandidatePuuids.last() shouldBe "uncached-pending-500"

                directStaleCapExceedsWithNetNew shouldBe true
                directStaleCapFitsWithExistingOldProcessed shouldBe false
                exceed500OnStalePatchAdvanceResp.action shouldBe "INVALID_PARAMETERS"
                exceed500OnStalePatchAdvanceResp.runningInProcess shouldBe false
                exceed500OnStalePatchAdvanceResp.overnightSessionArmed shouldBe false
                exceed500OnStalePatchAdvanceResp.checkpoint?.pendingCandidatePuuids?.size shouldBe 490
                exceed500OnStalePatchAdvanceResp.checkpoint?.processedPuuids?.size shouldBe 10
                patch167CheckpointStillAbsentAfterStaleReject shouldBe true

                unseededNoLatestFallbackResp.checkpoint?.lastStopReason shouldBe "SESSION_NETWORK_CALL_CAP_REACHED"
                unseededNoLatestFallbackResp.sessionNetworkCallsMade shouldBe 42
                unseededNoLatestFallbackResp.overnightSessionArmed shouldBe false

                seededAdmissionThrown.message shouldBe "simulated-ddragon-redis-failure"
                lockReleasedAfterSeededAdmissionException shouldBe true
                runningResetAfterSeededAdmissionException shouldBe true
                unseededMemPausedCheckpoint.lastStopReason shouldBe "PAUSED_MEMORY_HEADROOM"

                lockedResp.action shouldBe "LOCKED_BY_ANOTHER_INSTANCE"

                // Attempt 1 paused with 500 queue entries (fresh-challenger-seed + 499 existing tail, zero tail loss)
                pausedQueueSnapshotOnAttempt1 shouldNotBe null
                pausedQueueSnapshotOnAttempt1!!.size shouldBe 500
                pausedQueueSnapshotOnAttempt1!!.first() shouldBe "fresh-challenger-seed"
                pausedQueueSnapshotOnAttempt1!!.last() shouldBe "uncached-pending-499"

                // Attempt 2 completed in 1 productive batch and 11 Riot calls (<= 15 cap) with 0 league-v4 calls
                seededRunStatus.action shouldBe "COMPLETED_SYNCHRONOUS_RUN"
                seededRunStatus.runningInProcess shouldBe false
                seededRunStatus.overnightSessionArmed shouldBe false
                seededRunStatus.pauseCyclesCount shouldBe 1
                seededRunStatus.productiveBatchesCompleted shouldBe 1
                seededRunStatus.sessionProductiveBatchesCompleted shouldBe 1
                seededRunStatus.sessionNetworkCallsMade shouldBe 11
                rankLookupCalls shouldBe 0
                matchDetailCalls shouldBe 10

                val seededCp = seededRunStatus.checkpoint!!
                seededCp.lastStopReason shouldBe "SESSION_BATCH_CAP_REACHED"
                // Processed progress from prior runs is preserved, not reset
                seededCp.processedPuuids.contains("already-done-seed") shouldBe true
                seededCp.processedPuuids.contains("fresh-challenger-seed") shouldBe true
                seededCp.processedMatchIds.contains("EUW1_PREV_1") shouldBe true
                seededCp.playerCellGameCounts["CHALLENGER:TOP:already-done-seed"] shouldBe 4
                seededCp.playerCellGameCounts["CHALLENGER:TOP:fresh-challenger-seed"] shouldBe 10
                seededCp.cellProgress["CHALLENGER:TOP"]?.passesQualityGate shouldBe true
                // All 499 existing pending entries remain preserved without tail loss
                seededCp.pendingCandidatePuuids.size shouldBe 499
                seededCp.pendingCandidatePuuids.first() shouldBe "uncached-pending-1"
                seededCp.pendingCandidatePuuids.last() shouldBe "uncached-pending-499"

                // Normal collector DTO hook indexed FreshChal + 10 co-participants (11 unique players across 10 matches)
                inMemoryIndexStore["fresh-challenger-seed"]?.gameName shouldBe "FreshChal"
                inMemoryIndexStore["co-part-1"]?.gameName shouldBe "CoPart1"
                inMemoryIndexStore["co-part-10"]?.gameName shouldBe "CoPart10"
                seedService.getCollectorParticipantIndexingCounts()["collectorIndexedPlayersUpserted"] shouldBe 11L

                // First manual chunk only; scheduled auto-resume does NOT replay manual seedPuuids
                manualChunk1Status.checkpoint?.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
                manualChunk1Status.overnightSessionArmed shouldBe true
                autoResumedStatus.checkpoint?.lastStopReason shouldBe "SESSION_BATCH_CAP_REACHED"
                autoResumedStatus.overnightSessionArmed shouldBe false
                processedInManualVsScheduled shouldBe listOf("manual-chunk-only-seed", "uncached-pending-1")

                // 429 backoff recorded cleanly with 0 retries
                status429.checkpoint?.lastStopReason shouldBe "PAUSED_COOLDOWN_BUDGET_REACHED_RESUMABLE"
                status429.checkpoint?.pausedForRetryAfterCount shouldBe 1

                // Async run retained seedPuuids and completed cleanly
                asyncStartResp.action shouldBe "STARTED_OR_RESUMED_IN_PROCESS"
                asyncFinalStatus.runningInProcess shouldBe false
                asyncFinalStatus.overnightSessionArmed shouldBe false
                asyncFinalStatus.checkpoint?.processedPuuids?.contains("async-challenger-seed") shouldBe true
                asyncFinalStatus.checkpoint?.lastStopReason shouldBe "SESSION_BATCH_CAP_REACHED"
            }
        }
    }
})






