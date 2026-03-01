package com.pride.summoner_searcher_api.dto

/**
 * Data transfer object representing a player search autocomplete suggestion.
 * This strips away database-specific fields like 'lastSeenAt' and only returns what the frontend needs.
 */
data class AutocompletePlayerDto(
    val puuid: String,
    val gameName: String,
    val tagLine: String,
    val region: String,
    val profileIconId: Int,
    val summonerLevel: Long
)
