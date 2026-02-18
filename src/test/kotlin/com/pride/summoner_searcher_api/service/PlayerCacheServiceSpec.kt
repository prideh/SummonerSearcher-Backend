package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import io.mockk.*
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class PlayerCacheServiceSpec {

    private lateinit var redisCacheService: RedisCacheService
    private lateinit var riotApiService: RiotApiService
    private lateinit var playerCacheService: PlayerCacheService

    private val puuid = "test-puuid"
    private val region = "na1"
    private val summonerName = "TestSummoner"
    private val tagLine = "NA1"

    @BeforeEach
    fun setup() {
        redisCacheService = mockk(relaxed = true)
        riotApiService = mockk(relaxed = true)
        playerCacheService = PlayerCacheService(redisCacheService, riotApiService)
    }

    @Test
    fun `getPlayerProfile - Cache Miss - Streams matches and saves profile`() = runBlocking {
        // Mock Cache Miss
        every { redisCacheService.get(any(), SummonerProfileDto::class.java) } returns null
        
        // Mock Riot API Profile
        val mockProfile = SummonerProfileDto(
            puuid = puuid, gameName = summonerName, tagLine = tagLine,
            summonerLevel = 100, profileIconId = 1, soloQueueRank = null, recentMatches = null
        )
        coEvery { riotApiService.fetchSummonerProfile(puuid, region) } returns mockProfile
        
        // Mock Stream Matches (50 matches)
        val mockMatches = (1..50).map { i ->
            val p = mockk<ParticipantDto>(relaxed = true) {
                every { puuid } returns this@PlayerCacheServiceSpec.puuid
                every { win } returns true
                every { kills } returns 1
                every { deaths } returns 0
                every { assists } returns 5
                every { championName } returns "Ahri"
                every { teamId } returns 100
                every { teamPosition } returns "MIDDLE"
                every { totalMinionsKilled } returns 100
                every { neutralMinionsKilled } returns 0
                every { visionScore } returns 10
                every { challenges } returns mockk(relaxed=true) {
                     every { soloKills } returns 0
                     every { turretPlatesTaken } returns 0
                }
            }
            
            val info = mockk<MatchInfo>(relaxed = true) {
                every { gameCreation } returns (System.currentTimeMillis() - (i * 100000))
                every { gameDuration } returns 1800
                every { gameId } returns i.toLong()
                every { participants } returns listOf(p)
            }
            
            mockk<MatchDto>(relaxed = true) {
                every { this@mockk.info } returns info
            }
        }
        
        coEvery { riotApiService.streamMatches(eq(puuid), eq(region), any()) } returns flowOf(*mockMatches.toTypedArray())

        // Execute
        val result = playerCacheService.getPlayerProfile(puuid, region, summonerName, tagLine)

        // Verify
        assertNotNull(result)
        assertEquals(20, result?.recentMatches?.size) // Top 20 kept
        assertEquals(50, result?.totalMatches) 
        assertEquals(50, result?.overallStats?.wins)

        // Verify Redis interactions
        verify { 
            redisCacheService.pushToList(
                eq("player:matches:$region:$puuid"), 
                match<List<MatchDto>> { it.size == 50 }
            ) 
        }
        // Check if profile was saved
        verify { redisCacheService.set(eq("player:profile:$region:$puuid"), any(), any()) }
    }

    @Test
    fun `getMatches - Returns paginated list from Redis`() {
        val start = 0
        val end = 19
        val mockList = listOf(mockk<MatchDto>(relaxed = true))
        
        every { redisCacheService.getFromList(any(), start.toLong(), end.toLong(), MatchDto::class.java) } returns mockList

        val result = playerCacheService.getMatches(puuid, region, start, end)
        
        assertEquals(mockList, result)
        verify { redisCacheService.getFromList(eq("player:matches:$region:$puuid"), start.toLong(), end.toLong(), MatchDto::class.java) }
    }
}
