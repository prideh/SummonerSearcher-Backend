package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class PlayerCacheServiceSpec {

    private lateinit var redisCacheService: RedisCacheService
    private lateinit var riotApiService: RiotApiService
    private lateinit var statsCalculator: StatsCalculator
    private lateinit var playerCacheService: PlayerCacheService

    private val puuid = "test-puuid"
    private val region = "na1"
    private val summonerName = "TestSummoner"
    private val tagLine = "NA1"

    @BeforeEach
    fun setup() {
        redisCacheService = mockk(relaxed = true)
        riotApiService = mockk(relaxed = true)
        statsCalculator = mockk(relaxed = true)
        playerCacheService = PlayerCacheService(redisCacheService, riotApiService, statsCalculator)
    }

    @Test
    fun `getPlayerProfile - Cache Miss - Fetches full profile and saves`() = runBlocking {
        // Mock Cache Miss
        every { redisCacheService.get(any<String>(), eq(SummonerProfileDto::class.java)) } returns null
        
        // Mock Riot API Profile
        val mockProfile = SummonerProfileDto(
            puuid = puuid, gameName = summonerName, tagLine = tagLine,
            summonerLevel = 100, profileIconId = 1, soloQueueRank = null, recentMatches = null
        )
        coEvery { riotApiService.fetchSummonerProfile(puuid, region) } returns mockProfile
        
        // Mock Matches
        val mockMatches = listOf(
            mockk<MatchDto>(relaxed = true)
        )
        coEvery { riotApiService.fetchAllMatchesSince(puuid, region, any<Long>()) } returns mockMatches
        
        // Mock Stats
        every { statsCalculator.calculateStats(any<List<MatchDto>>(), any<String>()) } returns Pair(emptyList(), mockk(relaxed = true))

        // Execute
        val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

        // Verify
        assertNotNull(result)
        assertEquals(1, result?.totalMatches)
        
        // Verify Redis save
        verify { redisCacheService.set(eq("player:profile:$region:$puuid"), any<SummonerProfileDto>(), any<Duration>()) }
    }

    @Test
    fun `getPlayerProfile - Cache Hit - Updates with new matches`() = runBlocking {
        // Create a dummy LeagueEntryDto for rank
        val rankDto = LeagueEntryDto(
            puuid = puuid, leaguePoints = 50, rank = "IV", wins = 10, losses = 10, 
            veteran = false, inactive = false, freshBlood = false, hotStreak = false, 
            leagueId = "123", queueType = "RANKED_SOLO_5x5", tier = "GOLD", summonerName = "Test"
        )

        // Mock Cache Hit with existing profile
        val cachedMatches = listOf(mockk<MatchDto>(relaxed = true) {
            every { info } returns mockk { every { gameId } returns 100L; every { gameCreation } returns 1000000L }
        })
        val cachedProfile = SummonerProfileDto(
            puuid = puuid, gameName = summonerName, tagLine = tagLine,
            summonerLevel = 100, profileIconId = 1, soloQueueRank = rankDto, 
            recentMatches = cachedMatches, totalMatches = 1, championStats = emptyList(), overallStats = mockk(relaxed=true)
        )
        every { redisCacheService.get(any<String>(), eq(SummonerProfileDto::class.java)) } returns cachedProfile
        
        // Mock New Matches found
        val newMatch = mockk<MatchDto>(relaxed = true) {
            every { info } returns mockk { every { gameId } returns 200L; every { gameCreation } returns 2000000L }
        }
        coEvery { riotApiService.fetchNewMatches(puuid, region, any<Long>()) } returns listOf(newMatch)
        
        // Mock Rank Check (unchanged)
        coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns rankDto
        
        // Mock Stats Recalculation
        every { statsCalculator.calculateStats(any<List<MatchDto>>(), any<String>()) } returns Pair(emptyList(), mockk(relaxed = true))

        // Execute
        val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

        // Verify
        assertNotNull(result)
        assertEquals(2, result?.recentMatches?.size) // 1 cached + 1 new
        
        // Verify Redis updated
        verify { redisCacheService.set(eq("player:profile:$region:$puuid"), any<SummonerProfileDto>(), any<Duration>()) }
    }

    @Test
    fun `getPlayerProfile - Cache Hit - No new matches - Returns cached`() = runBlocking {
         // Create a dummy LeagueEntryDto for rank
        val rankDto = LeagueEntryDto(
            puuid = puuid, leaguePoints = 50, rank = "IV", wins = 10, losses = 10, 
            veteran = false, inactive = false, freshBlood = false, hotStreak = false, 
            leagueId = "123", queueType = "RANKED_SOLO_5x5", tier = "GOLD", summonerName = "Test"
        )

        // Mock Cache Hit
         val cachedMatches = listOf(mockk<MatchDto>(relaxed = true) {
            every { info } returns mockk { every { gameId } returns 100L; every { gameCreation } returns 1000000L }
        })
        val cachedProfile = SummonerProfileDto(
            puuid = puuid, gameName = summonerName, tagLine = tagLine,
            summonerLevel = 100, profileIconId = 1, soloQueueRank = rankDto,
            recentMatches = cachedMatches, totalMatches = 1, 
            championStats = listOf(mockk(relaxed=true)), // NON-EMPTY to avoid migration update
            overallStats = mockk(relaxed=true)
        )
        every { redisCacheService.get(any<String>(), eq(SummonerProfileDto::class.java)) } returns cachedProfile
        
        // Mock No New Matches
        coEvery { riotApiService.fetchNewMatches(puuid, region, any<Long>()) } returns emptyList()
        coEvery { riotApiService.fetchLeagueRank(puuid, region) } returns rankDto // Rank unchanged

        // Execute
        val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

        // Verify
        assertNotNull(result)
        assertEquals(1, result?.recentMatches?.size)
        
        // Verify Redis NOT written (aside from maybe logging or if name changed, here name is same)
        verify(exactly = 0) { redisCacheService.set(eq("player:profile:$region:$puuid"), any<SummonerProfileDto>(), any<Duration>()) }
    }
}
