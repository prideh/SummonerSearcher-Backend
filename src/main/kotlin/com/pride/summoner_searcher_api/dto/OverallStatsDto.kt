package com.pride.summoner_searcher_api.dto

data class OverallStatsDto(
    val winRate: Double,
    val kda: Double,
    val wins: Int,
    val losses: Int,
    val avgCsPerMinute: Double,
    val avgKillParticipation: Double,
    val avgSoloKills: Double,
    val avgTurretPlates: Double,
    val avgKills: Double,
    val avgDeaths: Double,
    val avgAssists: Double,
    val oppAvgKda: Double,
    val oppAvgCsPerMinute: Double,
    val oppAvgKillParticipation: Double,
    val oppAvgSoloKills: Double,
    val oppAvgTurretPlates: Double,
    val avgVisionScore: Double,
    val oppAvgVisionScore: Double,
    val blueSide: SideStatsDto,
    val redSide: SideStatsDto
)

data class SideStatsDto(
    val games: Int,
    val wins: Int,
    val winRate: Double
)
