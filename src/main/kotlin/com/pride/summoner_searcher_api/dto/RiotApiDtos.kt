package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

// --- DTOs for the clean data sent to our frontend ---

data class PlatformStatusDto(
    val name: String,
    val maintenances: List<StatusItemDto>,
    val incidents: List<StatusItemDto>
)

data class StatusItemDto(
    val status: String?,
    val severity: String?,
    val title: String,
    val platforms: List<String>
)

data class SummonerProfileDto(
    val puuid: String?,
    val gameName: String?,
    val tagLine: String?,
    val summonerLevel: Long?,
    val profileIconId: Int?,
    val soloQueueRank: LeagueEntryDto?,
    val recentMatches: List<MatchDto>?,
    val lastRefreshed: Instant? = null // Timestamp for revalidation
)

// --- DTOs that match the full response from the Riot API ---

// For /riot/account/v1/accounts/by-riot-id and by-puuid
data class AccountDto(
    val puuid: String?,
    val gameName: String?,
    val tagLine: String?
)

// For /lol/summoner/v4/summoners/by-puuid
data class SummonerDto(
    val id: String?, // This is the encryptedSummonerId
    val puuid: String?,
    val summonerLevel: Long?,
    val profileIconId: Int?
)

// This DTO is now a flexible container for rank information.
// It's used for the challenger list AND the by-summoner call.
data class LeagueEntryDto(
    // Fields from Challenger List & by-summoner call
    val puuid: String?,
    val leaguePoints: Int?,
    val rank: String?,
    val wins: Int?,
    val losses: Int?,
    val veteran: Boolean?,
    val inactive: Boolean?,
    val freshBlood: Boolean?,
    val hotStreak: Boolean?,

    // Fields that are ONLY in the by-summoner call
    val leagueId: String?,
    val queueType: String?,
    val tier: String?,
    val summonerName: String?, // Legacy summoner name

    // Fields to be enriched for the frontend
    var gameName: String? = null,
    var tagLine: String? = null
)

// For /lol/league/v4/challengerleagues/by-queue
data class LeagueListDTO(
    val tier: String? = null,
    val leagueId: String? = null,
    val queue: String? = null,
    val name: String? = null,
    val entries: List<LeagueEntryDto> = listOf()
)

data class RiotPlatformData(
    val name: String,
    val locales: List<String>,
    val maintenances: List<RiotMaintenance>,
    val incidents: List<RiotIncident>
)

data class RiotMaintenance(
    @JsonProperty("maintenance_status")
    val maintenanceStatus: String,
    @JsonProperty("incident_severity")
    val incidentSeverity: String?,
    val titles: List<RiotContent>,
    val platforms: List<String>
)

data class RiotIncident(
    val active: Boolean,
    @JsonProperty("incident_severity")
    val incidentSeverity: String?,
    val titles: List<RiotContent>,
    val platforms: List<String>
)

data class RiotContent(
    val content: String
)
