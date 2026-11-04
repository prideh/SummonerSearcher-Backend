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
                    participants = emptyList()
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

            // Slow-path chunk multiGet returns legacyMatch2 (gameVersion == null) and currentMatch1 (gameVersion != null)
            every {
                redisCacheService.multiGet(listOf("match:euw1:EUW1_2", "match:euw1:EUW1_1"), MatchDto::class.java)
            } returns mapOf(
                "match:euw1:EUW1_2" to legacyMatch2,
                "match:euw1:EUW1_1" to currentMatch1
            )

            val backfilledMatch2 = legacyMatch2.copy(
                info = legacyMatch2.info?.copy(gameVersion = "16.20.826.3891")
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
                info = legacyMatch.info?.copy(gameVersion = "16.20.826.3891")
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
    }
})
