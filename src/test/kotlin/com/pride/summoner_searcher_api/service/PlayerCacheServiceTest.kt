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
    }
})
