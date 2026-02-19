package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class StatsCalculatorTest {

    private val statsCalculator = StatsCalculator()

    @Test
    fun `should calculate stats correctly for a single match`() {
        val puuid = "test-puuid"
        val match = createMockMatch(puuid, true, 10, 2, 5, 200, 100)
        
        val accumulator = MatchStatsAccumulator(puuid)
        accumulator.add(match)
        val (champs, overall) = accumulator.getResult()

        assertEquals(1, overall.wins + overall.losses)
        assertEquals(1, overall.wins)
        assertEquals(100.0, overall.winRate)
        assertEquals(7.5, overall.kda) // (10+5)/2
        assertEquals(1, overall.blueSide.wins)
    }

    @Test
    fun `should accumulate stats incrementally`() {
        val puuid = "test-puuid"
        val match1 = createMockMatch(puuid, true, 10, 2, 5, 200, 100)
        val match2 = createMockMatch(puuid, false, 5, 5, 5, 150, 200)
        
        val accumulator = MatchStatsAccumulator(puuid)
        accumulator.add(match1)
        accumulator.add(match2)
        val (champs, overall) = accumulator.getResult()

        assertEquals(2, overall.wins + overall.losses)
        assertEquals(1, overall.wins)
        assertEquals(50.0, overall.winRate)
        assertEquals((15.0 + 10.0) / 7.0, overall.kda, 0.01)
        assertEquals(1, overall.blueSide.wins)
        assertEquals(0, overall.redSide.wins) // Lost on red side
    }

    // initializeFrom not needed with new PlayerCacheService logic, so checking incremental accumulation is enough.

    private fun createMockMatch(puuid: String, win: Boolean, kills: Int, deaths: Int, assists: Int, cs: Int, teamId: Int): MatchDto {
        val match = mockk<MatchDto>()
        val info = mockk<MatchInfo>()
        val participant = mockk<ParticipantDto>(relaxed = true)
        val challenges = mockk<ChallengesDto>(relaxed = true)

        every { match.info } returns info
        every { info.gameDuration } returns 1800L
        every { info.participants } returns listOf(participant)
        
        every { participant.puuid } returns puuid
        every { participant.win } returns win
        every { participant.teamId } returns teamId
        every { participant.teamPosition } returns "MIDDLE"
        every { participant.kills } returns kills
        every { participant.deaths } returns deaths
        every { participant.assists } returns assists
        every { participant.totalMinionsKilled } returns cs
        every { participant.neutralMinionsKilled } returns 0
        every { participant.championName } returns "Ahri"
        every { participant.visionScore } returns 20
        every { participant.challenges } returns challenges
        every { challenges.soloKills } returns 0
        every { challenges.turretPlatesTaken } returns 0

        return match
    }
}
