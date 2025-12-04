package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.ChampionStatsDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.OverallStatsDto
import com.pride.summoner_searcher_api.dto.SideStatsDto
import org.springframework.stereotype.Service

@Service
class StatsCalculator {

    fun calculateStats(matches: List<MatchDto>, puuid: String): Pair<List<ChampionStatsDto>, OverallStatsDto> {
        val championStatsMap = mutableMapOf<String, MutableChampionStats>()
        
        var totalGames = 0
        var totalWins = 0
        var totalKills = 0
        var totalDeaths = 0
        var totalAssists = 0
        
        var totalCs = 0
        var totalDurationInMinutes = 0.0
        var totalKillParticipation = 0.0
        var totalSoloKills = 0
        var totalTurretPlates = 0
        var totalVisionScore = 0

        // Opponent Stats
        var oppTotalKills = 0
        var oppTotalDeaths = 0
        var oppTotalAssists = 0
        var oppTotalCs = 0
        var oppTotalSoloKills = 0
        var oppTotalTurretPlates = 0
        var oppTotalKillParticipation = 0.0
        var oppTotalVisionScore = 0

        var blueSideGames = 0
        var blueSideWins = 0
        var redSideGames = 0
        var redSideWins = 0

        matches.forEach { match ->
            val info = match.info ?: return@forEach
            val participants = info.participants
            val player = participants.find { it.puuid == puuid } ?: return@forEach
            
            if (info.gameDuration == 0L) return@forEach

            totalGames++
            if (player.win == true) totalWins++

            if (player.teamId == 100) {
                blueSideGames++
                if (player.win == true) blueSideWins++
            } else if (player.teamId == 200) {
                redSideGames++
                if (player.win == true) redSideWins++
            }

            // Team stats for KP
            val teamKills = participants.filter { it.teamId == player.teamId }.sumOf { it.kills ?: 0 }
            
            // Find opponent
            val opponent = participants.find { 
                it.teamPosition == player.teamPosition && it.teamId != player.teamId && player.teamPosition != "NONE"
            }
            val oppTeamKills = if (opponent != null) {
                participants.filter { it.teamId == opponent.teamId }.sumOf { it.kills ?: 0 }
            } else 0

            // Accumulate Player Stats
            val kills = player.kills ?: 0
            val deaths = player.deaths ?: 0
            val assists = player.assists ?: 0
            val cs = (player.totalMinionsKilled ?: 0) + (player.neutralMinionsKilled ?: 0)
            val soloKills = player.challenges?.soloKills ?: 0
            val turretPlates = player.challenges?.turretPlatesTaken ?: 0
            val visionScore = player.visionScore ?: 0
            val durationMin = (info.gameDuration ?: 0L) / 60.0

            totalKills += kills
            totalDeaths += deaths
            totalAssists += assists
            totalCs += cs
            totalSoloKills += soloKills
            totalTurretPlates += turretPlates.toInt() // turretPlatesTaken is Double? in some DTOs, assuming Int or Double
            totalVisionScore += visionScore
            totalDurationInMinutes += durationMin

            if (teamKills > 0) {
                totalKillParticipation += (kills + assists).toDouble() / teamKills
            }

            // Accumulate Opponent Stats
            if (opponent != null) {
                val oppKills = opponent.kills ?: 0
                val oppDeaths = opponent.deaths ?: 0
                val oppAssists = opponent.assists ?: 0
                val oppCs = (opponent.totalMinionsKilled ?: 0) + (opponent.neutralMinionsKilled ?: 0)
                
                oppTotalKills += oppKills
                oppTotalDeaths += oppDeaths
                oppTotalAssists += oppAssists
                oppTotalCs += oppCs
                oppTotalSoloKills += (opponent.challenges?.soloKills ?: 0)
                oppTotalTurretPlates += (opponent.challenges?.turretPlatesTaken ?: 0).toInt()
                oppTotalVisionScore += (opponent.visionScore ?: 0)

                if (oppTeamKills > 0) {
                    oppTotalKillParticipation += (oppKills + oppAssists).toDouble() / oppTeamKills
                }
            }

            // Champion Stats
            val champName = player.championName ?: "Unknown"
            val champStats = championStatsMap.getOrPut(champName) { MutableChampionStats(champName) }
            champStats.games++
            if (player.win == true) champStats.wins++ else champStats.losses++
            champStats.kills += kills
            champStats.deaths += deaths
            champStats.assists += assists
            champStats.soloKills += soloKills
            champStats.turretPlates += turretPlates.toInt()
            champStats.cs += cs
            champStats.durationMinutes += durationMin
        }

        // Calculate Overall Averages
        val overallStats = OverallStatsDto(
            winRate = if (totalGames > 0) (totalWins.toDouble() / totalGames) * 100 else 0.0,
            kda = if (totalDeaths > 0) (totalKills + totalAssists).toDouble() / totalDeaths else Double.POSITIVE_INFINITY,
            wins = totalWins,
            losses = totalGames - totalWins,
            avgCsPerMinute = if (totalDurationInMinutes > 0) totalCs / totalDurationInMinutes else 0.0,
            avgKillParticipation = if (totalGames > 0) (totalKillParticipation / totalGames) * 100 else 0.0,
            avgSoloKills = if (totalGames > 0) totalSoloKills.toDouble() / totalGames else 0.0,
            avgTurretPlates = if (totalGames > 0) totalTurretPlates.toDouble() / totalGames else 0.0,
            avgKills = if (totalGames > 0) totalKills.toDouble() / totalGames else 0.0,
            avgDeaths = if (totalGames > 0) totalDeaths.toDouble() / totalGames else 0.0,
            avgAssists = if (totalGames > 0) totalAssists.toDouble() / totalGames else 0.0,
            
            oppAvgKda = if (oppTotalDeaths > 0) (oppTotalKills + oppTotalAssists).toDouble() / oppTotalDeaths else Double.POSITIVE_INFINITY,
            oppAvgCsPerMinute = if (totalDurationInMinutes > 0) oppTotalCs / totalDurationInMinutes else 0.0,
            oppAvgKillParticipation = if (totalGames > 0) (oppTotalKillParticipation / totalGames) * 100 else 0.0,
            oppAvgSoloKills = if (totalGames > 0) oppTotalSoloKills.toDouble() / totalGames else 0.0,
            oppAvgTurretPlates = if (totalGames > 0) oppTotalTurretPlates.toDouble() / totalGames else 0.0,
            avgVisionScore = if (totalGames > 0) totalVisionScore.toDouble() / totalGames else 0.0,
            oppAvgVisionScore = if (totalGames > 0) oppTotalVisionScore.toDouble() / totalGames else 0.0,

            blueSide = SideStatsDto(
                games = blueSideGames,
                wins = blueSideWins,
                winRate = if (blueSideGames > 0) (blueSideWins.toDouble() / blueSideGames) * 100 else 0.0
            ),
            redSide = SideStatsDto(
                games = redSideGames,
                wins = redSideWins,
                winRate = if (redSideGames > 0) (redSideWins.toDouble() / redSideGames) * 100 else 0.0
            )
        )

        val championStatsList = championStatsMap.values.map { 
            ChampionStatsDto(
                championName = it.championName,
                games = it.games,
                wins = it.wins,
                losses = it.losses,
                kills = it.kills,
                deaths = it.deaths,
                assists = it.assists,
                soloKills = it.soloKills,
                winRate = if (it.games > 0) (it.wins.toDouble() / it.games) * 100 else 0.0,
                kda = if (it.deaths > 0) (it.kills + it.assists).toDouble() / it.deaths else Double.POSITIVE_INFINITY,
                averageKills = if (it.games > 0) it.kills.toDouble() / it.games else 0.0,
                averageDeaths = if (it.games > 0) it.deaths.toDouble() / it.games else 0.0,
                averageAssists = if (it.games > 0) it.assists.toDouble() / it.games else 0.0,
                averageCsPerMinute = if (it.durationMinutes > 0) it.cs / it.durationMinutes else 0.0,
                averageSoloKills = if (it.games > 0) it.soloKills.toDouble() / it.games else 0.0,
                averageTurretPlates = if (it.games > 0) it.turretPlates.toDouble() / it.games else 0.0
            )
        }.sortedByDescending { it.games }

        return Pair(championStatsList, overallStats)
    }

    private data class MutableChampionStats(
        val championName: String,
        var games: Int = 0,
        var wins: Int = 0,
        var losses: Int = 0,
        var kills: Int = 0,
        var deaths: Int = 0,
        var assists: Int = 0,
        var soloKills: Int = 0,
        var turretPlates: Int = 0,
        var cs: Int = 0,
        var durationMinutes: Double = 0.0
    )
}
