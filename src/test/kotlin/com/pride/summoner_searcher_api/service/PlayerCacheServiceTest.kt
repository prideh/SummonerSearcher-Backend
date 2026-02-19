package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchInfo
import com.pride.summoner_searcher_api.dto.MatchMetadata
import com.pride.summoner_searcher_api.dto.SummonerDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.mockk.*
import java.time.Duration

class PlayerCacheServiceTest : BehaviorSpec({
    val redisCacheService = mockk<RedisCacheService>(relaxed = true)
    val riotApiService = mockk<RiotApiService>(relaxed = true)
    val playerCacheService = PlayerCacheService(redisCacheService, riotApiService)

    beforeTest {
        io.mockk.clearMocks(redisCacheService, riotApiService, answers = false)
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
                        queueId = 420, gameMode = "CLASSIC",
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
    }
})
