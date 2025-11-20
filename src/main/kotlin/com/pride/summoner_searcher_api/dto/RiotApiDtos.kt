package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

// This file contains all Data Transfer Objects (DTOs) used for interacting with the Riot Games API.
// They are separated into two categories:
// 1. Clean DTOs: Well-structured objects sent to our frontend.
// 2. Raw DTOs: Objects that exactly match the messy, complex structure of the Riot API responses.

// --- Clean DTOs (for Frontend) ---

/**
 * A clean data structure representing the server status, sent to the frontend.
 * This is a simplified and processed version of the raw [RiotPlatformData].
 */
data class PlatformStatusDto(
    val name: String,
    val maintenances: List<StatusItemDto>,
    val incidents: List<StatusItemDto>
)

/**
 * A clean, reusable data structure for a single status item (either an incident or a maintenance).
 */
data class StatusItemDto(
    val status: String?,
    val severity: String?,
    val title: String,
    val description: String?,
    val platforms: List<String>
)

/**
 * The primary, clean data structure for a complete player profile, sent to the frontend.
 * This object is assembled from multiple Riot API calls and is the main object stored in the player cache.
 */
data class SummonerProfileDto(
    val puuid: String?,
    val gameName: String?,
    val tagLine: String?,
    val summonerLevel: Long?,
    val profileIconId: Int?,
    val soloQueueRank: LeagueEntryDto?,
    val recentMatches: List<MatchDto>?,
    val lastRefreshed: Instant? = null
)


// --- Raw DTOs (for Riot API Deserialization) ---

/**
 * Represents the response from the Riot Account API (`/riot/account/v1/accounts/...`).
 * This is the primary way to resolve a Riot ID (name + tagline) into a permanent PUUID.
 */
data class AccountDto(
    val puuid: String?,
    val gameName: String?,
    val tagLine: String?
)

/**
 * Represents the response from the Summoner API (`/lol/summoner/v4/summoners/by-puuid/...`).
 * This provides core, non-changing summoner information like level and icon.
 */
data class SummonerDto(
    val id: String?, // This is the encryptedSummonerId, which is different from the PUUID.
    val puuid: String?,
    val summonerLevel: Long?,
    val profileIconId: Int?
)

/**
 * A flexible DTO representing a player's entry in a league.
 * It's used for both the challenger leaderboard and for individual player rank lookups.
 */
data class LeagueEntryDto(
    // Common fields
    val puuid: String?,
    val leaguePoints: Int?,
    val rank: String?,
    val wins: Int?,
    val losses: Int?,
    val veteran: Boolean?,
    val inactive: Boolean?,
    val freshBlood: Boolean?,
    val hotStreak: Boolean?,

    // Fields only present in the by-summoner lookup
    val leagueId: String?,
    val queueType: String?,
    val tier: String?,
    val summonerName: String?, // Legacy summoner name, not the Riot ID

    // Fields that are enriched by our backend before sending to the frontend
    var gameName: String? = null,
    var tagLine: String? = null
)

/**
 * Represents the top-level response from the Challenger League API (`/lol/league/v4/challengerleagues/by-queue/...`).
 */
data class LeagueListDTO(
    val tier: String?,
    val leagueId: String?,
    val queue: String?,
    val name: String?,
    val entries: List<LeagueEntryDto> = listOf()
)

/**
 * Represents the top-level response from the LoL Status API (`/lol/status/v4/platform-data`).
 * This is a complex object that our backend simplifies into a [PlatformStatusDto].
 */
data class RiotPlatformData(
    val name: String,
    val locales: List<String>,
    val maintenances: List<RiotMaintenance>,
    val incidents: List<RiotIncident>
)

/**
 * Represents a single maintenance event from the LoL Status API.
 */
data class RiotMaintenance(
    @JsonProperty("maintenance_status") val maintenanceStatus: String,
    @JsonProperty("incident_severity") val incidentSeverity: String?,
    val titles: List<RiotContent>,
    val updates: List<RiotUpdate>,
    val platforms: List<String>
)

/**
 * Represents a single incident from the LoL Status API.
 */
data class RiotIncident(
    val active: Boolean,
    @JsonProperty("incident_severity") val incidentSeverity: String?,
    val titles: List<RiotContent>,
    val updates: List<RiotUpdate>,
    val platforms: List<String>
)

/**
 * Represents a single update message within an incident or maintenance event.
 */
data class RiotUpdate(
    val id: Int,
    val author: String,
    val translations: List<RiotContent>
)

/**
 * Represents a piece of translated text content from the LoL Status API.
 * This is used for both titles and update messages.
 */
data class RiotContent(
    val locale: String,
    val content: String
)
