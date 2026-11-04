package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import com.pride.summoner_searcher_api.repository.CohortBenchmarkCellRepository
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
    }
})

