package com.pride.summoner_searcher_api.dto

data class ChampionStatsDto(
    val championName: String,
    val games: Int,
    val wins: Int,
    val losses: Int,
    val kills: Int,
    val deaths: Int,
    val assists: Int,
    val soloKills: Int,
    val winRate: Double,
    val kda: Double,
    val averageKills: Double,
    val averageDeaths: Double,
    val averageAssists: Double,
    val averageCsPerMinute: Double,
    val averageSoloKills: Double,
    val averageTurretPlates: Double
)
