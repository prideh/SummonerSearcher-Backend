package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchInfo
import com.pride.summoner_searcher_api.dto.MatchMetadata
import com.pride.summoner_searcher_api.dto.SummonerDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import java.time.Duration

class PlayerCacheServiceTest : BehaviorSpec({
    val redisCacheService = mockk<RedisCacheService>(relaxed = true)
    val riotApiService = mockk<RiotApiService>(relaxed = true)
    val indexedPlayerRepository = mockk<IndexedPlayerRepository>(relaxed = true)
    val playerCacheService = PlayerCacheService(redisCacheService, riotApiService, indexedPlayerRepository)

    beforeTest {
        io.mockk.clearMocks(redisCacheService, riotApiService, indexedPlayerRepository, answers = false)
    }

    Given("A player cache service") {
        val puuid = "test-puuid"
        val region = "euw1"
        val summonerName = "Test Summoner"
        val tagLine = "EUW"

        When("fetching a profile on cache miss") {
            // Mock Lock Acquisition
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            
            // Mock Redis miss for profile
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
            
            // Mock Riot API responses
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns LeagueEntryDto(
                puuid = puuid, leaguePoints = 100, rank = "I", wins = 10, losses = 5,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                leagueId = "league-id", queueType = "RANKED_SOLO_5x5", tier = "GOLD", summonerName = "OldName"
            )
            
            // Mock 25 match IDs
            val matchIds = (1..25).map { "EUW1_$it" }
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns matchIds
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 100L, 123)

            // Mock missing matches in Redis (all miss initially)
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { null }
            }

            // Mock fetching matches from API
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                MatchDto(
                    metadata = MatchMetadata(id, listOf(puuid)),
                    info = MatchInfo(
                        gameCreation = System.currentTimeMillis(), gameDuration = 1200L, gameId = id.hashCode().toLong(),
                        queueId = 420, gameMode = "CLASSIC", gameVersion = "16.20.826.3891",
                        participants = listOf()
                    )
                )
            }

            // Manual Spy for Cache Sets
            val cachedMatches = mutableListOf<String>()
            var profileCached = false
            
            // Match caching spy
            every { redisCacheService.set<MatchDto>(match { it.startsWith("match:euw1:") }, any<MatchDto>(), any<Duration>()) } answers {
                val key = firstArg<String>()
                cachedMatches.add(key)
            }
            
            // Profile caching spy
            every { redisCacheService.set<SummonerProfileDto>(match { it.startsWith("player:profile:") }, any<SummonerProfileDto>(), any<Duration>()) } answers {
                profileCached = true
            }

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should return a profile with the correct data") {
                result?.gameName shouldBe summonerName
                result?.allMatchIds?.size shouldBe 25
                result?.recentMatches?.size shouldBe 20 // Should be truncated
                result?.recentMatches?.first()?.info?.gameVersion shouldBe "16.20.826.3891"
                result?.totalMatches shouldBe 25
            }

            Then("it should cache the individual matches") {
                // We expect 25 individual match cache writes
                cachedMatches.size shouldBe 25
            }

            Then("it should cache the profile") {
                profileCached shouldBe true
            }
        }

        When("fetching a profile with mix of new and old matches") {
            // Mock Redis miss for profile
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            
            // Mock Riot API responses
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns LeagueEntryDto(
                puuid = puuid, leaguePoints = 100, rank = "I", wins = 10, losses = 5,
                veteran = false, inactive = false, freshBlood = false, hotStreak = false,
                leagueId = "league-id", queueType = "RANKED_SOLO_5x5", tier = "GOLD", summonerName = "OldName"
            )
            
            // Mock 10 match IDs
            val matchIds = (1..10).map { "EUW1_$it" }
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns matchIds
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 100L, 123)

            // Mock missing matches in Redis
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { null }
            }

            // Mock fetching matches from API - First 5 are NEW, Last 5 are OLD
            coEvery { riotApiService.getMatchById(any(), any()) } answers {
                val id = firstArg<String>()
                val index = id.split("_")[1].toInt()
                val isNew = index <= 5
                
                MatchDto(
                    metadata = MatchMetadata(id, listOf(puuid)),
                    info = MatchInfo(
                        gameCreation = if (isNew) System.currentTimeMillis() else 1000L, // New vs Old
                        gameDuration = 1200L, gameId = id.hashCode().toLong(),
                        queueId = 420, gameMode = "CLASSIC",
                        participants = listOf()
                    )
                )
            }
            
            // Manual Spy for Cache Sets
            val cachedMatches = mutableListOf<String>()
            coEvery { redisCacheService.set(any(), any<MatchDto>(), any()) } answers {
                val key = firstArg<String>()
                if (key.startsWith("match:euw1:")) {
                    cachedMatches.add(key)
                }
            }

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should only include the 5 new matches") {
                result?.allMatchIds?.size shouldBe 5
                result?.recentMatches?.size shouldBe 5
                result?.totalMatches shouldBe 5
            }

            Then("it should Only cache the 5 new matches") {
                // Verify the manually captured list
                cachedMatches.size shouldBe 5
                cachedMatches.any { it.contains("EUW1_1") } shouldBe true
                cachedMatches.any { it.contains("EUW1_6") } shouldBe false
            }
        }

        When("fetching a profile on cache hit with no new matches") {
            val existingMatchIds = listOf("EUW1_2", "EUW1_1")
            val (_, existingOverallStats) = MatchStatsAccumulator(puuid).getResult()
            val cachedProfile = SummonerProfileDto(
                puuid = puuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = 100L,
                profileIconId = 123,
                soloQueueRank = null,
                recentMatches = emptyList(),
                allMatchIds = existingMatchIds,
                championStats = emptyList(),
                overallStats = existingOverallStats,
                totalMatches = 2,
                lastRefreshed = java.time.Instant.now().minusSeconds(300)
            )

            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns cachedProfile
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns null
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 125L, 456)
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns existingMatchIds

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should refresh summonerLevel and profileIconId without re-processing matches") {
                result?.summonerLevel shouldBe 125L
                result?.profileIconId shouldBe 456
                verify(exactly = 0) { redisCacheService.multiGet(any(), MatchDto::class.java) }
            }
        }

        When("fetching a cached profile whose recentMatches predate gameVersion (gameVersion == null)") {
            val existingMatchIds = listOf("EUW1_2", "EUW1_1")
            val legacyMatch2 = MatchDto(
                metadata = MatchMetadata("EUW1_2", listOf(puuid)),
                info = MatchInfo(
                    gameCreation = System.currentTimeMillis(), gameDuration = 1200L, gameId = 2L,
                    queueId = 420, gameMode = "CLASSIC", gameVersion = null,
                    participants = emptyList()
                )
            )
            val currentMatch1 = MatchDto(
                metadata = MatchMetadata("EUW1_1", listOf(puuid)),
                info = MatchInfo(
                    gameCreation = System.currentTimeMillis() - 3600_000L, gameDuration = 1200L, gameId = 1L,
                    queueId = 420, gameMode = "CLASSIC", gameVersion = "16.19.820.1111",
                    participants = emptyList(),
                    teams = emptyList()
                )
            )
            val (_, existingOverallStats) = MatchStatsAccumulator(puuid).getResult()
            val cachedProfile = SummonerProfileDto(
                puuid = puuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = 100L,
                profileIconId = 123,
                soloQueueRank = null,
                recentMatches = listOf(legacyMatch2, currentMatch1),
                allMatchIds = existingMatchIds,
                championStats = emptyList(),
                overallStats = existingOverallStats,
                totalMatches = 2,
                lastRefreshed = java.time.Instant.now().minusSeconds(300)
            )

            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns cachedProfile
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns null
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 125L, 456)
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns existingMatchIds

            // Slow-path chunk multiGet returns legacyMatch2 (gameVersion == null) and currentMatch1 (gameVersion != null, teams != null)
            every {
                redisCacheService.multiGet(listOf("match:euw1:EUW1_2", "match:euw1:EUW1_1"), MatchDto::class.java)
            } returns mapOf(
                "match:euw1:EUW1_2" to legacyMatch2,
                "match:euw1:EUW1_1" to currentMatch1
            )

            val backfilledMatch2 = legacyMatch2.copy(
                info = legacyMatch2.info?.copy(gameVersion = "16.20.826.3891", teams = emptyList())
            )
            val fetchedMatchIds = mutableListOf<String>()
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                fetchedMatchIds.add(id)
                if (id == "EUW1_2") backfilledMatch2 else null
            }

            val cachedMatchKeys = mutableListOf<String>()
            var savedProfile: SummonerProfileDto? = null
            every { redisCacheService.set<MatchDto>(match { it.startsWith("match:euw1:") }, any<MatchDto>(), any<Duration>()) } answers {
                cachedMatchKeys.add(firstArg())
            }
            every { redisCacheService.set<SummonerProfileDto>(match { it.startsWith("player:profile:") }, any<SummonerProfileDto>(), any<Duration>()) } answers {
                savedProfile = secondArg()
            }

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should fall through fast-path to slow-path, refetch only the match with gameVersion==null, and update Redis in place") {
                result?.recentMatches?.size shouldBe 2
                result?.recentMatches?.get(0)?.info?.gameVersion shouldBe "16.20.826.3891"
                result?.recentMatches?.get(1)?.info?.gameVersion shouldBe "16.19.820.1111"
                fetchedMatchIds shouldBe listOf("EUW1_2")
                cachedMatchKeys shouldBe listOf("match:euw1:EUW1_2")
                savedProfile?.recentMatches?.get(0)?.info?.gameVersion shouldBe "16.20.826.3891"
                savedProfile?.recentMatches?.get(1)?.info?.gameVersion shouldBe "16.19.820.1111"
            }
        }

        When("fetching matches by IDs where a cached MatchDto entry predates gameVersion") {
            val legacyMatch = MatchDto(
                metadata = MatchMetadata("EUW1_10", listOf(puuid)),
                info = MatchInfo(
                    gameCreation = System.currentTimeMillis(), gameDuration = 1200L, gameId = 10L,
                    queueId = 420, gameMode = "CLASSIC", gameVersion = null,
                    participants = emptyList()
                )
            )
            val freshMatch = legacyMatch.copy(
                info = legacyMatch.info?.copy(gameVersion = "16.20.826.3891", teams = emptyList())
            )

            every { redisCacheService.multiGet(listOf("match:euw1:EUW1_10"), MatchDto::class.java) } returns mapOf(
                "match:euw1:EUW1_10" to legacyMatch
            )
            val fetchedMatchIds = mutableListOf<String>()
            coEvery { riotApiService.getMatchById("EUW1_10", region) } answers {
                fetchedMatchIds.add(firstArg())
                freshMatch
            }
            val cachedMatchKeys = mutableListOf<String>()
            every { redisCacheService.set<MatchDto>(match { it.startsWith("match:euw1:") }, any<MatchDto>(), any<Duration>()) } answers {
                cachedMatchKeys.add(firstArg())
            }

            val matches = playerCacheService.getMatchesByIds(listOf("EUW1_10"), region)

            Then("it should re-fetch from Riot API, update Redis, and return the match with gameVersion") {
                matches.size shouldBe 1
                matches.first().info?.gameVersion shouldBe "16.20.826.3891"
                fetchedMatchIds shouldBe listOf("EUW1_10")
                cachedMatchKeys shouldBe listOf("match:euw1:EUW1_10")
            }
        }

        When("fetching matches by IDs where a cached MatchDto entry has gameVersion but predates info.teams (teams == null)") {
            val legacyNoTeamsMatch = MatchDto(
                metadata = MatchMetadata("EUW1_11", listOf(puuid)),
                info = MatchInfo(
                    gameCreation = System.currentTimeMillis(), gameDuration = 1800L, gameId = 11L,
                    queueId = 420, gameMode = "CLASSIC", gameVersion = "16.20.826.3891",
                    participants = emptyList(),
                    teams = null
                )
            )
            val freshTeams = listOf(
                com.pride.summoner_searcher_api.dto.TeamDto(
                    teamId = 100,
                    win = true,
                    objectives = com.pride.summoner_searcher_api.dto.TeamObjectivesDto(
                        tower = com.pride.summoner_searcher_api.dto.ObjectiveStatsDto(first = true, kills = 9),
                        dragon = com.pride.summoner_searcher_api.dto.ObjectiveStatsDto(first = true, kills = 3),
                        baron = com.pride.summoner_searcher_api.dto.ObjectiveStatsDto(first = true, kills = 1),
                        riftHerald = com.pride.summoner_searcher_api.dto.ObjectiveStatsDto(first = false, kills = 0),
                        horde = com.pride.summoner_searcher_api.dto.ObjectiveStatsDto(first = true, kills = 6)
                    )
                )
            )
            val backfilledTeamsMatch = legacyNoTeamsMatch.copy(
                info = legacyNoTeamsMatch.info?.copy(teams = freshTeams)
            )

            every { redisCacheService.multiGet(listOf("match:euw1:EUW1_11"), MatchDto::class.java) } returns mapOf(
                "match:euw1:EUW1_11" to legacyNoTeamsMatch
            )
            val fetchedMatchIds = mutableListOf<String>()
            coEvery { riotApiService.getMatchById("EUW1_11", region) } answers {
                fetchedMatchIds.add(firstArg())
                backfilledTeamsMatch
            }
            val cachedMatchKeys = mutableListOf<String>()
            every { redisCacheService.set<MatchDto>(match { it.startsWith("match:euw1:") }, any<MatchDto>(), any<Duration>()) } answers {
                cachedMatchKeys.add(firstArg())
            }

            val matches = playerCacheService.getMatchesByIds(listOf("EUW1_11"), region)

            Then("it should lazily backfill info.teams from Riot API and update Redis in place") {
                matches.size shouldBe 1
                matches.first().info?.teams?.first()?.objectives?.tower?.kills shouldBe 9
                matches.first().info?.teams?.first()?.objectives?.horde?.kills shouldBe 6
                fetchedMatchIds shouldBe listOf("EUW1_11")
                cachedMatchKeys shouldBe listOf("match:euw1:EUW1_11")
            }
        }

        When("fetching a profile for a Riot ID that has no summoner on the selected region") {
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
            coEvery { riotApiService.getSummonerByPuuid(puuid, "kr") } returns null

            val result = playerCacheService.getPlayerProfile(puuid, "kr", summonerName, tagLine)

            Then("it should return null without caching a ghost profile") {
                result shouldBe null
                coVerify(exactly = 0) { riotApiService.fetchMatchIdsSince(puuid, "kr", any()) }
            }
        }

        When("fetching a 60-match seasonal history where all 60 cached matches lack teams (recent20-only enrichment + full-season correctness)") {
            val matchIds = (1..60).map { "EUW1_$it" }
            val nowMs = System.currentTimeMillis()
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()

            fun buildParticipant(): com.pride.summoner_searcher_api.dto.ParticipantDto =
                mapper.readValue(
                    """
                    {
                      "puuid": "$puuid",
                      "championName": "Orianna",
                      "teamId": 100,
                      "teamPosition": "MIDDLE",
                      "win": true,
                      "kills": 8,
                      "deaths": 2,
                      "assists": 10,
                      "totalMinionsKilled": 220,
                      "neutralMinionsKilled": 10,
                      "visionScore": 25
                    }
                    """.trimIndent(),
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )

            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 200L, 55)
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns matchIds

            // All 60 matches exist in Redis with participant stats, but teams == null (legacy cache)
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { key ->
                    val id = key.substringAfterLast(":")
                    val idx = id.substringAfter("_").toInt()
                    MatchDto(
                        metadata = MatchMetadata(id, listOf(puuid)),
                        info = MatchInfo(
                            gameCreation = nowMs - idx * 60_000L,
                            gameDuration = 1800L,
                            gameId = idx.toLong(),
                            queueId = 420,
                            gameMode = "CLASSIC",
                            gameVersion = "16.7.1",
                            participants = listOf(buildParticipant()),
                            teams = null
                        )
                    )
                }
            }

            val fetchedMatchIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                fetchedMatchIds.add(id)
                val idx = id.substringAfter("_").toInt()
                MatchDto(
                    metadata = MatchMetadata(id, listOf(puuid)),
                    info = MatchInfo(
                        gameCreation = nowMs - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.7.1",
                        participants = listOf(buildParticipant()),
                        teams = emptyList()
                    )
                )
            }

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should enrich only the recent 20 matches from Riot API while accumulating full-season stats across all 60 matches") {
                fetchedMatchIds.size shouldBe 20
                fetchedMatchIds.toSet() shouldBe (1..20).map { "EUW1_$it" }.toSet()
                result?.recentMatches?.size shouldBe 20
                result?.recentMatches?.all { it.info?.teams != null } shouldBe true
                result?.allMatchIds?.size shouldBe 60
                result?.totalMatches shouldBe 60
                result?.overallStats?.wins shouldBe 60
                result?.championStats?.firstOrNull()?.games shouldBe 60
            }
        }

        When("fetching a 60-match seasonal history where recent 20 are enriched, 21..55 are legacy cached, and 56..60 are missing from Redis") {
            val matchIds = (1..60).map { "EUW1_$it" }
            val nowMs = System.currentTimeMillis()
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()

            fun buildParticipant(): com.pride.summoner_searcher_api.dto.ParticipantDto =
                mapper.readValue(
                    """
                    {
                      "puuid": "$puuid",
                      "championName": "Syndra",
                      "teamId": 100,
                      "teamPosition": "MIDDLE",
                      "win": true,
                      "kills": 6,
                      "deaths": 1,
                      "assists": 8,
                      "totalMinionsKilled": 200,
                      "neutralMinionsKilled": 0,
                      "visionScore": 20
                    }
                    """.trimIndent(),
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )

            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
            coEvery { riotApiService.getSummonerByPuuid(puuid, region) } returns SummonerDto("id", puuid, 200L, 55)
            coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(puuid, region, any()) } returns matchIds

            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { key ->
                    val id = key.substringAfterLast(":")
                    val idx = id.substringAfter("_").toInt()
                    when {
                        idx > 55 -> null // Missing from Redis in chunk 2
                        idx <= 20 -> MatchDto(
                            metadata = MatchMetadata(id, listOf(puuid)),
                            info = MatchInfo(
                                gameCreation = nowMs - idx * 60_000L,
                                gameDuration = 1800L,
                                gameId = idx.toLong(),
                                queueId = 420,
                                gameMode = "CLASSIC",
                                gameVersion = "16.7.1",
                                participants = listOf(buildParticipant()),
                                teams = emptyList()
                            )
                        )
                        else -> MatchDto(
                            metadata = MatchMetadata(id, listOf(puuid)),
                            info = MatchInfo(
                                gameCreation = nowMs - idx * 60_000L,
                                gameDuration = 1800L,
                                gameId = idx.toLong(),
                                queueId = 420,
                                gameMode = "CLASSIC",
                                gameVersion = "16.7.1",
                                participants = listOf(buildParticipant()),
                                teams = null
                            )
                        )
                    }
                }
            }

            val fetchedMatchIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                fetchedMatchIds.add(id)
                val idx = id.substringAfter("_").toInt()
                MatchDto(
                    metadata = MatchMetadata(id, listOf(puuid)),
                    info = MatchInfo(
                        gameCreation = nowMs - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.7.1",
                        participants = listOf(buildParticipant()),
                        teams = emptyList()
                    )
                )
            }

            val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

            Then("it should fetch only the 5 missing older matches (56..60) from Riot API and include all 60 in season totals") {
                fetchedMatchIds.toSet() shouldBe (56..60).map { "EUW1_$it" }.toSet()
                result?.totalMatches shouldBe 60
                result?.overallStats?.wins shouldBe 60
                result?.championStats?.firstOrNull()?.games shouldBe 60
            }
        }

        When("reading a warm cached profile repeatedly across fresh, stale, and explicitRefresh states") {
            val baseTimeMs = java.util.concurrent.atomic.AtomicLong(1_762_250_000_000L)
            val controllableClock = object : TimeProvider {
                override fun now(): Long = baseTimeMs.get()
            }
            val scopedService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = controllableClock
            )

            val freshPuuid = "warm-fresh-puuid"
            val enrichedRecentMatch = MatchDto(
                metadata = MatchMetadata("EUW1_1", listOf(freshPuuid)),
                info = MatchInfo(
                    gameCreation = baseTimeMs.get() - 120_000L,
                    gameDuration = 1800L,
                    gameId = 1L,
                    queueId = 420,
                    gameMode = "CLASSIC",
                    gameVersion = "16.7.1",
                    participants = emptyList(),
                    teams = emptyList()
                )
            )
            val (_, existingOverallStats) = MatchStatsAccumulator(freshPuuid).getResult()
            val freshCachedProfile = SummonerProfileDto(
                puuid = freshPuuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = 150L,
                profileIconId = 77,
                soloQueueRank = null,
                recentMatches = listOf(enrichedRecentMatch),
                allMatchIds = listOf("EUW1_1"),
                championStats = emptyList(),
                overallStats = existingOverallStats,
                improvementStats = com.pride.summoner_searcher_api.dto.ImprovementStatsDto(),
                totalMatches = 1,
                // Refreshed 30s ago (within both 5m freshness window and 60s explicitRefresh cooldown)
                lastRefreshed = java.time.Instant.ofEpochMilli(baseTimeMs.get() - 30_000L)
            )

            val summonerApiCalls = java.util.concurrent.atomic.AtomicInteger(0)
            every { redisCacheService.get("player:profile:euw1:$freshPuuid", SummonerProfileDto::class.java) } returns freshCachedProfile
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            coEvery { riotApiService.getSummonerByPuuid(freshPuuid, region) } coAnswers {
                summonerApiCalls.incrementAndGet()
                SummonerDto("id", freshPuuid, 151L, 88)
            }
            coEvery { riotApiService.fetchLeagueRank(freshPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(freshPuuid, region, any()) } returns listOf("EUW1_1")

            // 1. 5 repeated reads within the 5-minute freshness window + 1 explicitRefresh within 60s cooldown
            val repeatedFreshReads = (1..5).map {
                scopedService.getPlayerProfile(freshPuuid, region, summonerName, tagLine, explicitRefresh = false)
            }
            val cooldownExplicitRead = scopedService.getPlayerProfile(
                freshPuuid, region, summonerName, tagLine, explicitRefresh = true
            )
            val callsAfterFreshReads = summonerApiCalls.get()

            // 2. Advance clock by 2 minutes (profile is now 2m30s old: still < 5m fresh, but > 60s explicit cooldown)
            baseTimeMs.addAndGet(120_000L)
            val explicitRefreshedProfile = scopedService.getPlayerProfile(
                freshPuuid, region, summonerName, tagLine, explicitRefresh = true
            )
            val callsAfterExplicitRefresh = summonerApiCalls.get()

            // 3. Advance clock by 10 minutes so cached profile is stale (> 5m old) and read 3 times
            baseTimeMs.addAndGet(600_000L)
            val backgroundSaved = kotlinx.coroutines.CompletableDeferred<SummonerProfileDto>()
            every {
                redisCacheService.set<SummonerProfileDto>("player:profile:euw1:$freshPuuid", any(), any())
            } answers {
                backgroundSaved.complete(secondArg())
            }
            val staleRead1 = scopedService.getPlayerProfile(freshPuuid, region, summonerName, tagLine, explicitRefresh = false)
            val staleRead2 = scopedService.getPlayerProfile(freshPuuid, region, summonerName, tagLine, explicitRefresh = false)
            val staleRead3 = scopedService.getPlayerProfile(freshPuuid, region, summonerName, tagLine, explicitRefresh = false)
            val refreshedInBackground = kotlinx.coroutines.withTimeout(2_000L) { backgroundSaved.await() }
            val totalCallsAfterStaleReads = summonerApiCalls.get()

            Then("it should make 0 Riot calls on repeated fresh reads, honor explicitRefresh after 60s, and trigger only 1 background refresh across repeated stale reads") {
                repeatedFreshReads.all { it?.summonerLevel == 150L } shouldBe true
                cooldownExplicitRead?.summonerLevel shouldBe 150L
                callsAfterFreshReads shouldBe 0

                explicitRefreshedProfile?.summonerLevel shouldBe 151L
                callsAfterExplicitRefresh shouldBe 1

                staleRead1?.summonerLevel shouldBe 150L
                staleRead2?.summonerLevel shouldBe 150L
                staleRead3?.summonerLevel shouldBe 150L
                refreshedInBackground.summonerLevel shouldBe 151L
                // 1 from explicitRefresh + 1 from the single cooldown-guarded background refresh = 2 total
                totalCallsAfterStaleReads shouldBe 2
            }
        }

        When("running a 450-match cached seasonal refresh vs a 450-match cold seasonal load (measured regression)") {
            val matchIds450 = (1..450).map { "EUW1_$it" }
            val nowMs = System.currentTimeMillis()
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()
            val sampleParticipant = mapper.readValue(
                """
                {
                  "puuid": "$puuid",
                  "championName": "Ahri",
                  "teamId": 100,
                  "teamPosition": "MIDDLE",
                  "win": true,
                  "kills": 7,
                  "deaths": 2,
                  "assists": 9,
                  "totalMinionsKilled": 215,
                  "neutralMinionsKilled": 12,
                  "visionScore": 28
                }
                """.trimIndent(),
                com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
            )

            // Part A: 450-match cached seasonal refresh (recent 20 lack teams, older 430 cached without teams)
            val cachedPuuid = "seasonal-450-cached-puuid"
            val cachedParticipant = mapper.readValue(
                mapper.writeValueAsString(sampleParticipant).replace(puuid, cachedPuuid),
                com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
            )
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get("player:profile:euw1:$cachedPuuid", SummonerProfileDto::class.java) } returns null
            coEvery { riotApiService.getSummonerByPuuid(cachedPuuid, region) } returns SummonerDto("id", cachedPuuid, 250L, 10)
            coEvery { riotApiService.fetchLeagueRank(cachedPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(cachedPuuid, region, any()) } returns matchIds450

            val cachedMultiGetChunks = java.util.concurrent.atomic.AtomicInteger(0)
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                cachedMultiGetChunks.incrementAndGet()
                keys.associateWith { key ->
                    val id = key.substringAfterLast(":")
                    val idx = id.substringAfter("_").toInt()
                    MatchDto(
                        metadata = MatchMetadata(id, listOf(cachedPuuid)),
                        info = MatchInfo(
                            gameCreation = nowMs - idx * 60_000L,
                            gameDuration = 1800L,
                            gameId = idx.toLong(),
                            queueId = 420,
                            gameMode = "CLASSIC",
                            gameVersion = "16.7.1",
                            participants = listOf(cachedParticipant),
                            teams = null
                        )
                    )
                }
            }

            val cachedModeFetchedIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                cachedModeFetchedIds.add(id)
                val idx = id.substringAfter("_").toInt()
                MatchDto(
                    metadata = MatchMetadata(id, listOf(cachedPuuid)),
                    info = MatchInfo(
                        gameCreation = nowMs - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.7.1",
                        participants = listOf(cachedParticipant),
                        teams = emptyList()
                    )
                )
            }

            val cachedStartNs = System.nanoTime()
            val cached450Result = playerCacheService.getPlayerProfile(cachedPuuid, region, summonerName, tagLine)
            val cachedElapsedMs = (System.nanoTime() - cachedStartNs) / 1_000_000

            // Part B: 450-match cold seasonal load (all 450 matches missing from Redis)
            val cold450Puuid = "seasonal-450-cold-puuid"
            val coldParticipant = mapper.readValue(
                mapper.writeValueAsString(sampleParticipant).replace(puuid, cold450Puuid),
                com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
            )
            every { redisCacheService.get("player:profile:euw1:$cold450Puuid", SummonerProfileDto::class.java) } returns null
            coEvery { riotApiService.getSummonerByPuuid(cold450Puuid, region) } returns SummonerDto("id", cold450Puuid, 260L, 11)
            coEvery { riotApiService.fetchLeagueRank(cold450Puuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(cold450Puuid, region, any()) } returns matchIds450

            val coldMultiGetChunks = java.util.concurrent.atomic.AtomicInteger(0)
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                coldMultiGetChunks.incrementAndGet()
                keys.associateWith { null }
            }

            val coldModeFetchedIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            val coldMatchWrites = java.util.concurrent.atomic.AtomicInteger(0)
            every {
                redisCacheService.set<MatchDto>(match { it.startsWith("match:euw1:") }, any<MatchDto>(), any<Duration>())
            } answers {
                coldMatchWrites.incrementAndGet()
            }
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                coldModeFetchedIds.add(id)
                val idx = id.substringAfter("_").toInt()
                MatchDto(
                    metadata = MatchMetadata(id, listOf(cold450Puuid)),
                    info = MatchInfo(
                        gameCreation = nowMs - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.7.1",
                        participants = listOf(coldParticipant),
                        teams = emptyList()
                    )
                )
            }

            val coldStartNs = System.nanoTime()
            val cold450Result = playerCacheService.getPlayerProfile(cold450Puuid, region, summonerName, tagLine)
            val coldElapsedMs = (System.nanoTime() - coldStartNs) / 1_000_000

            println(
                "[MEASURED_450_REGRESSION] cached450: getMatchById=${cachedModeFetchedIds.size}, multiGetChunks=${cachedMultiGetChunks.get()}, totalMatches=${cached450Result?.totalMatches}, elapsedMs=${cachedElapsedMs}ms | " +
                    "cold450: getMatchById=${coldModeFetchedIds.size}, multiGetChunks=${coldMultiGetChunks.get()}, matchRedisWrites=${coldMatchWrites.get()}, totalMatches=${cold450Result?.totalMatches}, elapsedMs=${coldElapsedMs}ms"
            )

            Then("cached 450-match refresh should fetch only 20 matches (skipping 430 older backfills) while cold 450-match load fetches and caches all 450") {
                cachedModeFetchedIds.size shouldBe 20
                cachedMultiGetChunks.get() shouldBe 9
                cached450Result?.recentMatches?.size shouldBe 20
                cached450Result?.totalMatches shouldBe 450
                cached450Result?.overallStats?.wins shouldBe 450

                coldModeFetchedIds.size shouldBe 450
                coldMultiGetChunks.get() shouldBe 9
                coldMatchWrites.get() shouldBe 450
                cold450Result?.recentMatches?.size shouldBe 20
                cold450Result?.totalMatches shouldBe 450
                cold450Result?.overallStats?.wins shouldBe 450
            }
        }

        When("two concurrent requests arrive for the same cold uncached profile in the same JVM (in-process single-flight)") {
            val coldPuuid = "cold-concurrent-puuid"
            val summonerCalls = java.util.concurrent.atomic.AtomicInteger(0)
            val matchIdsCalls = java.util.concurrent.atomic.AtomicInteger(0)
            every { redisCacheService.get("player:profile:euw1:$coldPuuid", SummonerProfileDto::class.java) } returns null
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            coEvery { riotApiService.getSummonerByPuuid(coldPuuid, region) } coAnswers {
                summonerCalls.incrementAndGet()
                kotlinx.coroutines.delay(100)
                SummonerDto("id", coldPuuid, 300L, 99)
            }
            coEvery { riotApiService.fetchLeagueRank(coldPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(coldPuuid, region, any()) } coAnswers {
                matchIdsCalls.incrementAndGet()
                emptyList()
            }

            val (res1, res2) = kotlinx.coroutines.runBlocking {
                val d1 = async(kotlinx.coroutines.Dispatchers.Default) {
                    playerCacheService.getPlayerProfile(coldPuuid, region, summonerName, tagLine)
                }
                val d2 = async(kotlinx.coroutines.Dispatchers.Default) {
                    kotlinx.coroutines.delay(15)
                    playerCacheService.getPlayerProfile(coldPuuid, region, summonerName, tagLine)
                }
                d1.await() to d2.await()
            }

            Then("both callers should receive the non-null profile and Riot API should only be queried once") {
                res1?.summonerLevel shouldBe 300L
                res2?.summonerLevel shouldBe 300L
                summonerCalls.get() shouldBe 1
                matchIdsCalls.get() shouldBe 1
            }
        }

        When("a cross-instance 60s lock is held vs when it expires with empty cache") {
            val fastPollService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                lockPollIntervalMs = 1L,
                lockMaxPollAttempts = 240
            )

            // Case 1: Lock remains held by another instance across all 240 poll attempts (setIfAbsent returns false both before and after wait)
            val heldLockPuuid = "cross-instance-held-lock-puuid"
            val heldLockSummonerCalls = java.util.concurrent.atomic.AtomicInteger(0)
            every { redisCacheService.get("player:profile:euw1:$heldLockPuuid", SummonerProfileDto::class.java) } returns null
            every { redisCacheService.setIfAbsent("lock:profile:euw1:$heldLockPuuid", "LOCKED", any()) } returns false
            coEvery { riotApiService.getSummonerByPuuid(heldLockPuuid, region) } coAnswers {
                heldLockSummonerCalls.incrementAndGet()
                SummonerDto("id", heldLockPuuid, 400L, 12)
            }

            val heldLockResult = fastPollService.getPlayerProfile(heldLockPuuid, region, summonerName, tagLine)

            // Case 2: Lock was held initially (false), then expired after the 240-attempt wait so the second setIfAbsent returns true
            val expiredLockPuuid = "cross-instance-expired-lock-puuid"
            val expiredLockSummonerCalls = java.util.concurrent.atomic.AtomicInteger(0)
            val lockAcquireCalls = java.util.concurrent.atomic.AtomicInteger(0)
            val lockDeleted = java.util.concurrent.atomic.AtomicBoolean(false)
            every { redisCacheService.get("player:profile:euw1:$expiredLockPuuid", SummonerProfileDto::class.java) } returns null
            every { redisCacheService.setIfAbsent("lock:profile:euw1:$expiredLockPuuid", "LOCKED", any()) } answers {
                // 1st call -> false (held by crashed instance); 2nd call after 240 polls -> true (lock expired)
                lockAcquireCalls.incrementAndGet() >= 2
            }
            every { redisCacheService.delete("lock:profile:euw1:$expiredLockPuuid") } answers {
                lockDeleted.set(true)
            }
            coEvery { riotApiService.getSummonerByPuuid(expiredLockPuuid, region) } coAnswers {
                expiredLockSummonerCalls.incrementAndGet()
                SummonerDto("id", expiredLockPuuid, 410L, 13)
            }
            coEvery { riotApiService.fetchLeagueRank(expiredLockPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(expiredLockPuuid, region, any()) } returns emptyList()

            val expiredLockResult = fastPollService.getPlayerProfile(expiredLockPuuid, region, summonerName, tagLine)

            Then("it must never bypass a still-held 60s lock (0 Riot calls), and only fallback-refresh after acquiring an expired lock") {
                heldLockResult shouldBe null
                heldLockSummonerCalls.get() shouldBe 0
                verify(exactly = 0) { redisCacheService.delete("lock:profile:euw1:$heldLockPuuid") }

                expiredLockResult?.summonerLevel shouldBe 410L
                expiredLockSummonerCalls.get() shouldBe 1
                lockAcquireCalls.get() shouldBe 2
                lockDeleted.get() shouldBe true
            }
        }

        When("incrementally refreshing 3 new matches on top of 900 durably folded season matches and recovering after Redis match/profile expiry") {
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()
            val fixedNowMs = java.time.Instant.parse("2026-11-06T12:00:00Z").toEpochMilli()
            val clock = object : TimeProvider {
                override fun now(): Long = fixedNowMs
            }
            val seasonPuuid = "durable-900-plus-3-puuid"
            val champions = listOf("Orianna", "Ahri", "Syndra", "Viktor", "Azir", "Taliyah")

            fun buildMatch(idx: Int): MatchDto {
                val champ = champions[idx % champions.size]
                val win = idx % 3 != 0
                val p = mapper.readValue(
                    """
                    {
                      "puuid": "$seasonPuuid",
                      "championName": "$champ",
                      "teamId": 100,
                      "teamPosition": "MIDDLE",
                      "win": $win,
                      "kills": ${5 + (idx % 7)},
                      "deaths": ${1 + (idx % 4)},
                      "assists": ${6 + (idx % 8)},
                      "totalMinionsKilled": ${190 + (idx % 40)},
                      "neutralMinionsKilled": ${4 + (idx % 8)},
                      "visionScore": ${20 + (idx % 15)}
                    }
                    """.trimIndent(),
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )
                return MatchDto(
                    metadata = MatchMetadata("EUW1_$idx", listOf(seasonPuuid)),
                    info = MatchInfo(
                        gameCreation = fixedNowMs - (1_000 - idx) * 60_000L,
                        gameDuration = 1500L + (idx % 300),
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.22.1",
                        participants = listOf(p),
                        teams = emptyList()
                    )
                )
            }

            // Build reference 900-game durable state and 903-game full-accumulation reference
            val baseAcc900 = MatchStatsAccumulator(seasonPuuid)
            val fullAcc903 = MatchStatsAccumulator(seasonPuuid)
            val ids900NewestFirst = (900 downTo 1).map { "EUW1_$it" }
            val ids903NewestFirst = (903 downTo 1).map { "EUW1_$it" }
            for (idx in 900 downTo 1) {
                val m = buildMatch(idx)
                baseAcc900.add(m)
                fullAcc903.add(m)
            }
            for (idx in 903 downTo 901) {
                fullAcc903.add(buildMatch(idx))
            }
            val (expectedChamp903, expectedOverall903) = fullAcc903.getResult()
            val (baseOverallSnap, baseChampSnaps) = baseAcc900.toAdditiveSnapshots()

            val initialDurableDto = DurablePlayerSeasonStatsDto(
                schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
                region = region,
                puuid = seasonPuuid,
                seasonYear = 2026,
                queueId = 420,
                historyComplete = true,
                hitPreSeasonBoundary = true,
                firstPreSeasonMatchId = "EUW1_0",
                processedMatchIds = ids900NewestFirst,
                pendingMatchIds = emptyList(),
                excludedMatchIds = emptyList(),
                overallCounters = baseOverallSnap,
                championCounters = baseChampSnaps,
                updatedAtEpochMilli = fixedNowMs - 600_000L
            )

            var storedEntity = com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                id = java.util.UUID.randomUUID(),
                region = region,
                puuid = seasonPuuid,
                seasonYear = 2026,
                queueId = 420,
                schemaVersion = 1,
                historyComplete = true,
                foldedGamesCount = 900,
                pendingGamesCount = 0,
                stateJson = mapper.writeValueAsString(initialDurableDto),
                updatedAt = java.time.Instant.ofEpochMilli(fixedNowMs - 600_000L),
                version = 0L
            )

            val seasonRepo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every {
                seasonRepo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, seasonPuuid, 2026, 420)
            } answers { storedEntity }
            every { seasonRepo.saveAndFlush(any()) } answers {
                val arg = firstArg<com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
                arg.version += 1L
                storedEntity = arg
                arg
            }

            val txManager = object : org.springframework.transaction.support.AbstractPlatformTransactionManager() {
                override fun doGetTransaction(): Any = Any()
                override fun doBegin(transaction: Any, definition: org.springframework.transaction.TransactionDefinition) {}
                override fun doCommit(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
                override fun doRollback(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
            }
            val txTemplate = org.springframework.transaction.support.TransactionTemplate(txManager)

            val durableService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = seasonRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )

            // Warm cached profile has recentMatches for 900..881 so recent-20 cards only need the 3 new matches + 17 already-in-profile cards!
            val cachedRecent20For900 = (900 downTo 881).map { buildMatch(it) }
            val (baseChamp900, baseOverall900) = baseAcc900.getResult()
            val warmProfile900 = SummonerProfileDto(
                puuid = seasonPuuid,
                gameName = summonerName,
                tagLine = tagLine,
                summonerLevel = 500L,
                profileIconId = 29,
                soloQueueRank = null,
                recentMatches = cachedRecent20For900,
                allMatchIds = ids900NewestFirst,
                championStats = baseChamp900,
                overallStats = baseOverall900,
                totalMatches = 900,
                lastRefreshed = java.time.Instant.ofEpochMilli(fixedNowMs - 600_000L)
            )

            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get("player:profile:euw1:$seasonPuuid", SummonerProfileDto::class.java) } returns warmProfile900
            coEvery { riotApiService.getSummonerByPuuid(seasonPuuid, region) } returns SummonerDto("id", seasonPuuid, 501L, 29)
            coEvery { riotApiService.fetchLeagueRank(seasonPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(seasonPuuid, region, any()) } returns ids903NewestFirst

            val multiGetKeysRequested = java.util.Collections.synchronizedList(mutableListOf<String>())
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                multiGetKeysRequested.addAll(keys)
                keys.associateWith { null }
            }

            val riotFetchedIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                riotFetchedIds.add(id)
                val idx = id.substringAfter("_").toInt()
                buildMatch(idx)
            }

            // 1. Incremental refresh with 3 new matches on top of 900 folded matches
            val incrementalResult = durableService.getPlayerProfile(
                puuid = seasonPuuid,
                region = region,
                summonerName = summonerName,
                tagLine = tagLine,
                explicitRefresh = true
            )

            val multiGetAfterIncremental = multiGetKeysRequested.toList()
            val riotFetchedAfterIncremental = riotFetchedIds.toList()

            // 2. Simulate full Redis expiry (both player:profile and all match:euw1:* keys expired from Redis)
            multiGetKeysRequested.clear()
            riotFetchedIds.clear()
            every { redisCacheService.get("player:profile:euw1:$seasonPuuid", SummonerProfileDto::class.java) } returns null
            val recoveredAfterRedisExpiry = durableService.getPlayerProfile(
                puuid = seasonPuuid,
                region = region,
                summonerName = summonerName,
                tagLine = tagLine,
                explicitRefresh = true
            )

            Then("incremental refresh decodes 0 of the 900 folded matches from Redis, and Redis expiry recovery restores identical 903-game seasonal stats from Postgres") {
                // Zero of the 900 old matches were requested via Redis multiGet or Riot getMatchById during the 3-match incremental update!
                multiGetAfterIncremental shouldBe listOf(
                    "match:euw1:EUW1_903",
                    "match:euw1:EUW1_902",
                    "match:euw1:EUW1_901"
                )
                riotFetchedAfterIncremental shouldBe listOf("EUW1_903", "EUW1_902", "EUW1_901")

                incrementalResult?.totalMatches shouldBe 903
                incrementalResult?.overallStats shouldBe expectedOverall903
                incrementalResult?.championStats shouldBe expectedChamp903
                incrementalResult?.recentMatches?.size shouldBe 20
                incrementalResult?.recentMatches?.first()?.metadata?.matchId shouldBe "EUW1_903"

                // After Redis profile + match keys expire completely, seasonal totals (903 games) are restored intact from durable state
                // while only the 20 recent card matches are re-fetched for the UI card list.
                recoveredAfterRedisExpiry?.totalMatches shouldBe 903
                recoveredAfterRedisExpiry?.overallStats shouldBe expectedOverall903
                recoveredAfterRedisExpiry?.championStats shouldBe expectedChamp903
                recoveredAfterRedisExpiry?.recentMatches?.size shouldBe 20
                riotFetchedIds.size shouldBe 20
            }
        }

        When("handling partial match fetch failures, memory headroom pauses, duplicates, out-of-order backfills, and year rollover") {
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()
            val clockMs = java.util.concurrent.atomic.AtomicLong(java.time.Instant.parse("2026-11-06T12:00:00Z").toEpochMilli())
            val clock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val gapPuuid = "durable-gap-backfill-puuid"

            fun buildMatchForClock(id: String, creationMs: Long, kills: Int = 6): MatchDto {
                val p = mapper.readValue(
                    """
                    {
                      "puuid": "$gapPuuid",
                      "championName": "Sylas",
                      "teamId": 100,
                      "teamPosition": "MIDDLE",
                      "win": true,
                      "kills": $kills,
                      "deaths": 2,
                      "assists": 7,
                      "totalMinionsKilled": 205,
                      "neutralMinionsKilled": 8,
                      "visionScore": 24
                    }
                    """.trimIndent(),
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )
                return MatchDto(
                    metadata = MatchMetadata(id, listOf(gapPuuid)),
                    info = MatchInfo(
                        gameCreation = creationMs,
                        gameDuration = 1800L,
                        gameId = id.hashCode().toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.22.1",
                        participants = listOf(p),
                        teams = emptyList()
                    )
                )
            }

            val rowsByYear = mutableMapOf<Int, com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
            val seasonRepo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every {
                seasonRepo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, gapPuuid, any(), 420)
            } answers {
                val yr = thirdArg<Int>()
                rowsByYear[yr]
            }
            every { seasonRepo.saveAndFlush(any()) } answers {
                val entity = firstArg<com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
                entity.version += 1L
                rowsByYear[entity.seasonYear] = entity
                entity
            }

            val txManager = object : org.springframework.transaction.support.AbstractPlatformTransactionManager() {
                override fun doGetTransaction(): Any = Any()
                override fun doBegin(transaction: Any, definition: org.springframework.transaction.TransactionDefinition) {}
                override fun doCommit(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
                override fun doRollback(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
            }
            val txTemplate = org.springframework.transaction.support.TransactionTemplate(txManager)

            val durableService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = seasonRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )

            var cachedProfileInRedis: SummonerProfileDto? = null
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get("player:profile:euw1:$gapPuuid", SummonerProfileDto::class.java) } answers { cachedProfileInRedis }
            every {
                redisCacheService.set<SummonerProfileDto>("player:profile:euw1:$gapPuuid", any(), any())
            } answers {
                cachedProfileInRedis = secondArg()
            }
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                firstArg<List<String>>().associateWith { null }
            }
            coEvery { riotApiService.getSummonerByPuuid(gapPuuid, region) } returns SummonerDto("id", gapPuuid, 310L, 44)
            coEvery { riotApiService.fetchLeagueRank(gapPuuid, region) } returns null

            // Step 1: Duplicate IDs ("EUW1_5" twice) + transient fetch failure on "EUW1_2"
            coEvery { riotApiService.fetchMatchIdsSince(gapPuuid, region, any()) } returns listOf(
                "EUW1_5", "EUW1_5", "EUW1_4", "EUW1_2", "EUW1_1"
            )
            val failMatch2 = java.util.concurrent.atomic.AtomicBoolean(true)
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                if (id == "EUW1_2" && failMatch2.get()) {
                    null
                } else {
                    buildMatchForClock(id, clockMs.get() - 60_000L)
                }
            }

            val step1Profile = durableService.getPlayerProfile(gapPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val step1State = mapper.readValue(rowsByYear[2026]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            // Step 1b: Next refresh omits "EUW1_2" from fetchMatchIdsSince window while "EUW1_2" still fails to resolve;
            // unresolved pending ID "EUW1_2" MUST be preserved (never silently dropped) and historyComplete MUST stay false!
            clockMs.addAndGet(120_000L)
            coEvery { riotApiService.fetchMatchIdsSince(gapPuuid, region, any()) } returns listOf(
                "EUW1_5", "EUW1_4", "EUW1_1"
            )
            val step1bProfile = durableService.getPlayerProfile(gapPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val step1bState = mapper.readValue(rowsByYear[2026]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            // Step 2: Transient failure heals AND out-of-order backfilled match "EUW1_3" appears in the middle (while newest ID is still "EUW1_5")
            failMatch2.set(false)
            clockMs.addAndGet(120_000L)
            coEvery { riotApiService.fetchMatchIdsSince(gapPuuid, region, any()) } returns listOf(
                "EUW1_5", "EUW1_4", "EUW1_3", "EUW1_2", "EUW1_1"
            )
            val step2FetchedIds = java.util.Collections.synchronizedList(mutableListOf<String>())
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                step2FetchedIds.add(id)
                buildMatchForClock(id, clockMs.get() - 60_000L)
            }

            val step2Profile = durableService.getPlayerProfile(gapPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val step2FetchedSet = step2FetchedIds.toSet()
            val step2State = mapper.readValue(rowsByYear[2026]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            // Step 3: Advance clock to 2027-01-15 (Season year rollover) with 2 new 2027 matches
            clockMs.set(java.time.Instant.parse("2027-01-15T12:00:00Z").toEpochMilli())
            coEvery { riotApiService.fetchMatchIdsSince(gapPuuid, region, any()) } returns listOf("EUW1_2027_2", "EUW1_2027_1")
            val step3Profile = durableService.getPlayerProfile(gapPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val step3State2027 = mapper.readValue(rowsByYear[2027]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            // Measure actual serialized JSON payload sizes for 150-game and 924-game (Kaori-scale) durable states
            val acc150 = MatchStatsAccumulator(gapPuuid)
            for (i in 1..150) {
                val champName = "Champ_${i % 20}"
                val p = mapper.readValue(
                    """{"puuid":"$gapPuuid","championName":"$champName","teamId":100,"teamPosition":"BOTTOM","win":${i % 2 == 0},"kills":7,"deaths":3,"assists":8,"totalMinionsKilled":230,"neutralMinionsKilled":8,"visionScore":22}""",
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )
                acc150.add(
                    MatchDto(
                        metadata = MatchMetadata("EUW1_${7000000000L + i}", listOf(gapPuuid)),
                        info = MatchInfo(
                            gameCreation = clockMs.get() - i * 1000L,
                            gameDuration = 1800L,
                            gameId = i.toLong(),
                            queueId = 420,
                            gameMode = "CLASSIC",
                            gameVersion = "16.22.1",
                            participants = listOf(p)
                        )
                    )
                )
            }
            val (overall150, champs150) = acc150.toAdditiveSnapshots()
            val durable150Bytes = mapper.writeValueAsBytes(
                DurablePlayerSeasonStatsDto(
                    region = "euw1",
                    puuid = gapPuuid,
                    seasonYear = 2026,
                    queueId = 420,
                    historyComplete = true,
                    hitPreSeasonBoundary = true,
                    processedMatchIds = (150 downTo 1).map { "EUW1_${7000000000L + it}" },
                    overallCounters = overall150,
                    championCounters = champs150,
                    updatedAtEpochMilli = clockMs.get()
                )
            ).size

            val kaoriAcc = MatchStatsAccumulator(gapPuuid)
            for (i in 1..924) {
                val champName = "Champ_${i % 40}"
                val p = mapper.readValue(
                    """{"puuid":"$gapPuuid","championName":"$champName","teamId":100,"teamPosition":"BOTTOM","win":${i % 2 == 0},"kills":7,"deaths":3,"assists":8,"totalMinionsKilled":230,"neutralMinionsKilled":8,"visionScore":22}""",
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )
                kaoriAcc.add(
                    MatchDto(
                        metadata = MatchMetadata("EUW1_${7000000000L + i}", listOf(gapPuuid)),
                        info = MatchInfo(
                            gameCreation = clockMs.get() - i * 1000L,
                            gameDuration = 1800L,
                            gameId = i.toLong(),
                            queueId = 420,
                            gameMode = "CLASSIC",
                            gameVersion = "16.22.1",
                            participants = listOf(p)
                        )
                    )
                )
            }
            val (kaoriOverall, kaoriChamps) = kaoriAcc.toAdditiveSnapshots()
            val kaoriDurableDto = DurablePlayerSeasonStatsDto(
                region = "euw1",
                puuid = gapPuuid,
                seasonYear = 2026,
                queueId = 420,
                historyComplete = true,
                hitPreSeasonBoundary = true,
                processedMatchIds = (924 downTo 1).map { "EUW1_${7000000000L + it}" },
                overallCounters = kaoriOverall,
                championCounters = kaoriChamps,
                updatedAtEpochMilli = clockMs.get()
            )
            val kaoriBytes = mapper.writeValueAsBytes(kaoriDurableDto).size
            println("[MEASURED_DURABLE_SEASON_STATE_BYTES] 150-game/20-champ UTF-8 bytes = $durable150Bytes bytes | 924-game/40-champ state_json UTF-8 bytes = $kaoriBytes bytes")

            Then("it tracks pending gaps with historyComplete=false, preserves unresolved pending IDs absent from current window, heals gaps + out-of-order backfills once, and isolates 2027 rollover") {
                // Step 1: Duplicate EUW1_5 folded once; failed EUW1_2 recorded as pending gap; historyComplete is FALSE
                step1Profile?.totalMatches shouldBe 3
                step1State.historyComplete shouldBe false
                step1State.processedMatchIds shouldBe listOf("EUW1_5", "EUW1_4", "EUW1_1")
                step1State.pendingMatchIds shouldBe listOf("EUW1_2")

                // Step 1b: Even when EUW1_2 is absent from the returned match ID window, unresolved EUW1_2 is preserved
                step1bProfile?.totalMatches shouldBe 3
                step1bState.historyComplete shouldBe false
                step1bState.pendingMatchIds shouldBe listOf("EUW1_2")

                // Step 2: Only the missing gap ("EUW1_2") and the out-of-order backfill ("EUW1_3") are fetched and folded;
                // historyComplete flips to TRUE with all 5 unique matches folded once
                step2FetchedSet shouldBe setOf("EUW1_2", "EUW1_3")
                step2Profile?.totalMatches shouldBe 5
                step2Profile?.overallStats?.wins shouldBe 5
                step2State.historyComplete shouldBe true
                step2State.pendingMatchIds shouldBe emptyList()
                step2State.processedMatchIds shouldBe listOf("EUW1_5", "EUW1_4", "EUW1_3", "EUW1_2", "EUW1_1")

                // Step 3: 2027 rollover starts clean 2-game 2027 state while 2026 row remains intact at 5 games
                step3Profile?.totalMatches shouldBe 2
                step3State2027.seasonYear shouldBe 2027
                step3State2027.processedMatchIds shouldBe listOf("EUW1_2027_2", "EUW1_2027_1")
                rowsByYear[2026]?.foldedGamesCount shouldBe 5

                // Kaori-scale 924-game / 40-champion state_json is compact (< 30 KB)
                (kaoriBytes in 10_000..30_000) shouldBe true
            }
        }

        When("fetching >2000 matches (2100-match pagination safety cap), rolling forward past cap, merging concurrent writers on conflict, and handling durability failure") {
            val mapper = com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules()
            val clockMs = java.util.concurrent.atomic.AtomicLong(java.time.Instant.parse("2026-11-06T12:00:00Z").toEpochMilli())
            val clock = object : TimeProvider {
                override fun now(): Long = clockMs.get()
            }
            val capPuuid = "durable-2100-cap-puuid"

            fun buildMatch(id: String, idx: Int, forPuuid: String = capPuuid): MatchDto {
                val p = mapper.readValue(
                    """
                    {
                      "puuid": "$forPuuid",
                      "championName": "Orianna",
                      "teamId": 100,
                      "teamPosition": "MIDDLE",
                      "win": true,
                      "kills": 8,
                      "deaths": 2,
                      "assists": 9,
                      "totalMinionsKilled": 210,
                      "neutralMinionsKilled": 10,
                      "visionScore": 25
                    }
                    """.trimIndent(),
                    com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                )
                return MatchDto(
                    metadata = MatchMetadata(id, listOf(forPuuid)),
                    info = MatchInfo(
                        gameCreation = clockMs.get() - idx * 60_000L,
                        gameDuration = 1800L,
                        gameId = idx.toLong(),
                        queueId = 420,
                        gameMode = "CLASSIC",
                        gameVersion = "16.22.1",
                        participants = listOf(p),
                        teams = emptyList()
                    )
                )
            }

            var storedCapEntity: com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity? = null
            val capSeasonRepo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every {
                capSeasonRepo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, capPuuid, 2026, 420)
            } answers { storedCapEntity }
            every { capSeasonRepo.saveAndFlush(any()) } answers {
                val arg = firstArg<com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
                arg.version += 1L
                storedCapEntity = arg
                arg
            }

            val txManager = object : org.springframework.transaction.support.AbstractPlatformTransactionManager() {
                override fun doGetTransaction(): Any = Any()
                override fun doBegin(transaction: Any, definition: org.springframework.transaction.TransactionDefinition) {}
                override fun doCommit(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
                override fun doRollback(status: org.springframework.transaction.support.DefaultTransactionStatus) {}
            }
            val txTemplate = org.springframework.transaction.support.TransactionTemplate(txManager)

            val capService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = capSeasonRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )

            var cachedCapProfile: SummonerProfileDto? = null
            every { redisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { redisCacheService.get("player:profile:euw1:$capPuuid", SummonerProfileDto::class.java) } answers { cachedCapProfile }
            every {
                redisCacheService.set<SummonerProfileDto>("player:profile:euw1:$capPuuid", any(), any())
            } answers {
                cachedCapProfile = secondArg()
            }
            coEvery { riotApiService.getSummonerByPuuid(capPuuid, region) } returns SummonerDto("id", capPuuid, 600L, 88)
            coEvery { riotApiService.fetchLeagueRank(capPuuid, region) } returns null

            // 1. Real >2000 fetch path: RiotApiService.fetchMatchIdsSince returns 2,100 match IDs (21 pages of 100 stopping at >2000 guard)
            val ids2100 = (2100 downTo 1).map { "EUW1_$it" }
            coEvery { riotApiService.fetchMatchIdsSince(capPuuid, region, any()) } returns ids2100
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                val keys = firstArg<List<String>>()
                keys.associateWith { key ->
                    val id = key.substringAfterLast(":")
                    val idx = id.substringAfter("_").toInt()
                    buildMatch(id, idx)
                }
            }
            coEvery { riotApiService.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                val idx = id.substringAfter("_").toInt()
                buildMatch(id, idx)
            }

            val profile2100 = capService.getPlayerProfile(capPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val state2100 = mapper.readValue(storedCapEntity!!.stateJson, DurablePlayerSeasonStatsDto::class.java)
            val peakRetainedMatchDtosDuring2100 = capService.getDiscoveryAndSeasonWriteTelemetry()["lastRefreshPeakRetainedMatchDtos"] ?: 0L
            val retainedDeltasDuring2100 = capService.getLastRefreshRetainedDeltasForTest()
            val deltaFieldTypes = SingleMatchAdditiveDelta::class.java.declaredFields.map { it.type }

            // 2. Rollforward past 2,100 cap (3 brand new matches arrive: 2103, 2102, 2101)
            clockMs.addAndGet(120_000L)
            val idsRollforward = ((2103 downTo 4).map { "EUW1_$it" })
            coEvery { riotApiService.fetchMatchIdsSince(capPuuid, region, any()) } returns idsRollforward
            val profileAfterRollforward = capService.getPlayerProfile(capPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val stateAfterRollforward = mapper.readValue(storedCapEntity!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            // 3. Concurrent writer merge: verify returned profile AND Redis profile reflect COMMITTED MERGED state (4 games), not stale local totals (3 games)
            val mergePuuid = "concurrent-merge-redis-puuid"
            val baseAcc2 = MatchStatsAccumulator(mergePuuid)
            baseAcc2.add(buildMatch("EUW1_1", 1, mergePuuid))
            baseAcc2.add(buildMatch("EUW1_2", 2, mergePuuid))
            val (snap2Overall, snap2Champs) = baseAcc2.toAdditiveSnapshots()
            val initial2Dto = DurablePlayerSeasonStatsDto(
                region = region,
                puuid = mergePuuid,
                seasonYear = 2026,
                queueId = 420,
                processedMatchIds = listOf("EUW1_2", "EUW1_1"),
                hitPreSeasonBoundary = true,
                firstPreSeasonMatchId = "EUW1_0",
                historyComplete = true,
                overallCounters = snap2Overall,
                championCounters = snap2Champs,
                updatedAtEpochMilli = clockMs.get()
            )
            // Concurrent writer already committed EUW1_3 (version = 1L), while this caller loaded version = 0L and folds EUW1_4
            val concurrentAcc3 = MatchStatsAccumulator(mergePuuid)
            concurrentAcc3.add(buildMatch("EUW1_1", 1, mergePuuid))
            concurrentAcc3.add(buildMatch("EUW1_2", 2, mergePuuid))
            concurrentAcc3.add(buildMatch("EUW1_3", 3, mergePuuid))
            val (snap3Overall, snap3Champs) = concurrentAcc3.toAdditiveSnapshots()
            val concurrent3Dto = initial2Dto.copy(
                processedMatchIds = listOf("EUW1_3", "EUW1_2", "EUW1_1"),
                overallCounters = snap3Overall,
                championCounters = snap3Champs
            )

            val findCallCount = java.util.concurrent.atomic.AtomicInteger(0)
            var mergeSavedEntity: com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity? = null
            val mergeRepo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every {
                mergeRepo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, mergePuuid, 2026, 420)
            } answers {
                val c = findCallCount.incrementAndGet()
                if (c == 1) {
                    // Initial load outside tx sees version = 0L (2 games: EUW1_2, EUW1_1)
                    com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                        region = region,
                        puuid = mergePuuid,
                        seasonYear = 2026,
                        queueId = 420,
                        schemaVersion = 1,
                        foldedGamesCount = 2,
                        pendingGamesCount = 0,
                        historyComplete = true,
                        stateJson = mapper.writeValueAsString(initial2Dto),
                        updatedAt = java.time.Instant.ofEpochMilli(clockMs.get()),
                        version = 0L
                    )
                } else {
                    // Inside tx, concurrent writer has already advanced version to 1L with 3 games (EUW1_3, EUW1_2, EUW1_1)
                    com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                        region = region,
                        puuid = mergePuuid,
                        seasonYear = 2026,
                        queueId = 420,
                        schemaVersion = 1,
                        foldedGamesCount = 3,
                        pendingGamesCount = 0,
                        historyComplete = true,
                        stateJson = mapper.writeValueAsString(concurrent3Dto),
                        updatedAt = java.time.Instant.ofEpochMilli(clockMs.get()),
                        version = 1L
                    )
                }
            }
            every { mergeRepo.saveAndFlush(any()) } answers {
                val arg = firstArg<com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
                arg.version += 1L
                mergeSavedEntity = arg
                arg
            }

            var redisSavedMergeProfile: SummonerProfileDto? = null
            every { redisCacheService.get("player:profile:euw1:$mergePuuid", SummonerProfileDto::class.java) } returns null
            every {
                redisCacheService.set<SummonerProfileDto>("player:profile:euw1:$mergePuuid", any(), any())
            } answers {
                redisSavedMergeProfile = secondArg()
            }
            every { redisCacheService.multiGet(any(), MatchDto::class.java) } answers {
                firstArg<List<String>>().associateWith { null }
            }
            coEvery { riotApiService.getSummonerByPuuid(mergePuuid, region) } returns SummonerDto("id", mergePuuid, 450L, 22)
            coEvery { riotApiService.fetchLeagueRank(mergePuuid, region) } returns null
            // Caller only sees [EUW1_4, EUW1_2, EUW1_1] in its local window, so locally it folded 3 games (2 baseline + EUW1_4),
            // while DB had 3 games (EUW1_3, EUW1_2, EUW1_1). Committed merge produces 4 games!
            coEvery { riotApiService.fetchMatchIdsSince(mergePuuid, region, any()) } returns listOf("EUW1_4", "EUW1_2", "EUW1_1")
            coEvery { riotApiService.getMatchById("EUW1_4", region) } returns buildMatch("EUW1_4", 4, mergePuuid)
            coEvery { riotApiService.getMatchById("EUW1_3", region) } returns buildMatch("EUW1_3", 3, mergePuuid)
            coEvery { riotApiService.getMatchById("EUW1_2", region) } returns buildMatch("EUW1_2", 2, mergePuuid)
            coEvery { riotApiService.getMatchById("EUW1_1", region) } returns buildMatch("EUW1_1", 1, mergePuuid)

            val mergeService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = mergeRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )
            val mergedReturnedProfile = mergeService.getPlayerProfile(mergePuuid, region, summonerName, tagLine, explicitRefresh = true)

            // 4. Durability failure MUST fail closed and NEVER write uncommitted profile to Redis
            val failDbPuuid = "durable-db-fail-puuid"
            val failingRepo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every { failingRepo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, failDbPuuid, 2026, 420) } returns null
            every { failingRepo.saveAndFlush(any()) } throws org.springframework.dao.DataIntegrityViolationException("Simulated DB write failure")

            var redisSavedOnDbFail: SummonerProfileDto? = null
            every { redisCacheService.get("player:profile:euw1:$failDbPuuid", SummonerProfileDto::class.java) } returns null
            every {
                redisCacheService.set<SummonerProfileDto>("player:profile:euw1:$failDbPuuid", any(), any())
            } answers {
                redisSavedOnDbFail = secondArg()
            }
            coEvery { riotApiService.getSummonerByPuuid(failDbPuuid, region) } returns SummonerDto("id", failDbPuuid, 450L, 22)
            coEvery { riotApiService.fetchLeagueRank(failDbPuuid, region) } returns null
            coEvery { riotApiService.fetchMatchIdsSince(failDbPuuid, region, any()) } returns listOf("EUW1_1")
            coEvery { riotApiService.getMatchById("EUW1_1", region) } returns buildMatch("EUW1_1", 1, failDbPuuid)

            val failingDurableService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = failingRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )
            val dbFailException = runCatching {
                failingDurableService.getPlayerProfile(failDbPuuid, region, summonerName, tagLine, explicitRefresh = true)
            }.exceptionOrNull()

            // 5. Real RiotApiService.fetchMatchIdsSince pagination loop:
            //    (a) Page-0 null failure -> stoppedOnPageFailure=true, exhaustedCleanly=false; cold profile refresh returns null and writes 0 DB rows
            //    (b) Page-1 null failure after 100 IDs on page 0 -> stoppedOnPageFailure=true, exhaustedCleanly=false; folds 100 games with historyComplete=false
            //    (c) 21 pages of 100 IDs (2,100 total) stopping at >2000 guard -> hitSafetyCap=true, exhaustedCleanly=false
            val realRiotSpy = spyk(
                RiotApiService(
                    riotRestClient = mockk(relaxed = true),
                    rateLimiter = RiotApiRateLimiter(clock)
                )
            )
            val page0FailPuuid = "page0-null-fail-puuid"
            coEvery { realRiotSpy.getSummonerByPuuid(page0FailPuuid, region) } returns SummonerDto("id", page0FailPuuid, 300L, 11)
            coEvery { realRiotSpy.fetchLeagueRank(page0FailPuuid, region) } returns null
            coEvery { realRiotSpy.getMatchIdsByPuuid(page0FailPuuid, region, 420, any(), any(), 100, 0) } returns null

            var page0SavedEntity: com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity? = null
            val page0Repo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every { page0Repo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, page0FailPuuid, 2026, 420) } returns null
            every { page0Repo.saveAndFlush(any()) } answers {
                page0SavedEntity = firstArg()
                page0SavedEntity!!
            }
            every { redisCacheService.get("player:profile:euw1:$page0FailPuuid", SummonerProfileDto::class.java) } returns null
            val page0Service = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = realRiotSpy,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = page0Repo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )
            val page0FailProfile = page0Service.getPlayerProfile(page0FailPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val page0Outcome = realRiotSpy.getLastMatchIdsPaginationOutcome(page0FailPuuid, region)

            val page1FailPuuid = "page1-null-fail-puuid"
            coEvery { realRiotSpy.getSummonerByPuuid(page1FailPuuid, region) } returns SummonerDto("id", page1FailPuuid, 300L, 11)
            coEvery { realRiotSpy.fetchLeagueRank(page1FailPuuid, region) } returns null
            coEvery {
                realRiotSpy.getMatchIdsByPuuid(page1FailPuuid, region, 420, any(), any(), 100, 0)
            } returns (100 downTo 1).map { "EUW1_$it" }
            coEvery {
                realRiotSpy.getMatchIdsByPuuid(page1FailPuuid, region, 420, any(), any(), 100, 100)
            } returns null
            coEvery { realRiotSpy.getMatchById(any(), region) } answers {
                val id = firstArg<String>()
                val idx = id.substringAfter("_").toInt()
                buildMatch(id, idx, page1FailPuuid)
            }
            var page1SavedEntity: com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity? = null
            val page1Repo = mockk<com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository>()
            every { page1Repo.findByRegionAndPuuidAndSeasonYearAndQueueId(region, page1FailPuuid, 2026, 420) } returns null
            every { page1Repo.saveAndFlush(any()) } answers {
                val arg = firstArg<com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity>()
                arg.version += 1L
                page1SavedEntity = arg
                arg
            }
            every { redisCacheService.get("player:profile:euw1:$page1FailPuuid", SummonerProfileDto::class.java) } returns null
            val page1Service = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = realRiotSpy,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = page1Repo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )
            val page1FailProfile = page1Service.getPlayerProfile(page1FailPuuid, region, summonerName, tagLine, explicitRefresh = true)
            val page1Outcome = realRiotSpy.getLastMatchIdsPaginationOutcome(page1FailPuuid, region)
            val page1State = mapper.readValue(page1SavedEntity!!.stateJson, DurablePlayerSeasonStatsDto::class.java)

            val real2100Puuid = "real-pagination-2100-puuid"
            coEvery {
                realRiotSpy.getMatchIdsByPuuid(real2100Puuid, region, 420, any(), any(), 100, any())
            } answers {
                val pageStart = arg<Int>(6)
                val firstNum = 2100 - pageStart
                (firstNum downTo (firstNum - 99)).map { "EUW1_$it" }
            }
            val real2100Outcome = realRiotSpy.fetchMatchIdsWithOutcomeSince(real2100Puuid, region, 0L)

            // 6. Memory headroom guard MUST fail safe (false) when memoryStatsProvider throws or returns non-positive heap limits
            val brokenMemoryService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample {
                        throw RuntimeException("Simulated cgroup/heap metric read error")
                    }
                }
            )
            val zeroLimitMemoryService = PlayerCacheService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                memoryStatsProvider = object : MemoryStatsProvider {
                    override fun sample(): RawMemorySample = RawMemorySample(
                        jvmHeapUsedBytes = 0L,
                        jvmHeapCommittedBytes = 0L,
                        jvmHeapMaxBytes = 0L,
                        cgroupMemoryCurrentBytes = null,
                        cgroupMemoryMaxBytes = null
                    )
                }
            )
            val brokenMemoryGuardResult = brokenMemoryService.hasMemoryHeadroomForSeasonBackfill()
            val zeroLimitMemoryGuardResult = zeroLimitMemoryService.hasMemoryHeadroomForSeasonBackfill()

            Then("it preserves 2100 coverage with historyCapped=true/historyComplete=false, bounds retained MatchDto instances to <=40, keeps counters equal to processedMatchIds ledger, returns committed merged state for Redis, and fails closed on DB/pagination/memory errors") {
                // 1. 2,100-match fetch preserves all 2,100 matches while marking historyCapped=true and historyComplete=false
                profile2100?.totalMatches shouldBe 2100
                state2100.processedMatchIds.size shouldBe 2100
                state2100.overallCounters.totalGames shouldBe 2100
                state2100.historyCapped shouldBe true
                state2100.historyComplete shouldBe false

                // 2. Prove retained MatchDto bound: <= 40 MatchDto instances retained across 2,100 matches (20 recent cards + 20 participant index)
                //    and SingleMatchAdditiveDelta holds zero MatchDto/MatchInfo/ParticipantDto fields
                (peakRetainedMatchDtosDuring2100 in 1L..40L) shouldBe true
                retainedDeltasDuring2100.size shouldBe 2100
                deltaFieldTypes.none {
                    it == MatchDto::class.java ||
                        it == MatchInfo::class.java ||
                        it == com.pride.summoner_searcher_api.dto.ParticipantDto::class.java
                } shouldBe true

                // 3. Rollforward past 2,100 cap: counters NEVER outgrow saved processedMatchIds ledger (both stay 2,100)
                stateAfterRollforward.processedMatchIds.size shouldBe 2100
                stateAfterRollforward.overallCounters.totalGames shouldBe 2100
                stateAfterRollforward.historyCapped shouldBe true
                stateAfterRollforward.historyComplete shouldBe false
                profileAfterRollforward?.totalMatches shouldBe 2100
                capService.isValidDurableSeasonState(stateAfterRollforward, region, capPuuid, 2026) shouldBe true

                // 4. Concurrent merge: both returned profile and Redis cached profile reflect committed 4-game merged state (not 3-game local state)
                mergeSavedEntity?.foldedGamesCount shouldBe 4
                mergedReturnedProfile?.totalMatches shouldBe 4
                mergedReturnedProfile?.overallStats?.wins shouldBe 4
                mergedReturnedProfile?.allMatchIds shouldBe listOf("EUW1_4", "EUW1_2", "EUW1_1", "EUW1_3")
                redisSavedMergeProfile?.totalMatches shouldBe 4
                redisSavedMergeProfile?.overallStats?.wins shouldBe 4
                redisSavedMergeProfile?.allMatchIds shouldBe listOf("EUW1_4", "EUW1_2", "EUW1_1", "EUW1_3")

                // 5. Durability failure fails closed and never writes uncommitted profile to Redis
                (dbFailException is IllegalStateException) shouldBe true
                redisSavedOnDbFail shouldBe null

                // 6. Real RiotApiService pagination outcome checks:
                page0Outcome?.stoppedOnPageFailure shouldBe true
                page0Outcome?.exhaustedCleanly shouldBe false
                page0Outcome?.pagesFetched shouldBe 0
                page0FailProfile shouldBe null
                page0SavedEntity shouldBe null

                page1Outcome?.stoppedOnPageFailure shouldBe true
                page1Outcome?.exhaustedCleanly shouldBe false
                page1Outcome?.pagesFetched shouldBe 1
                page1FailProfile?.totalMatches shouldBe 100
                page1State.processedMatchIds.size shouldBe 100
                page1State.historyComplete shouldBe false

                real2100Outcome.matchIds.size shouldBe 2100
                real2100Outcome.pagesFetched shouldBe 21
                real2100Outcome.hitSafetyCap shouldBe true
                real2100Outcome.exhaustedCleanly shouldBe false
                real2100Outcome.stoppedOnPageFailure shouldBe false

                // 7. Memory headroom guard fails safe (false) on metric exception or zero heap limit
                brokenMemoryGuardResult shouldBe false
                zeroLimitMemoryGuardResult shouldBe false
            }
        }
    }
})


