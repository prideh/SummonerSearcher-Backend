package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class CurrentGameInfo(
    val gameId: Long,
    val gameType: String,
    val gameStartTime: Long,
    val mapId: Long,
    val gameLength: Long,
    val platformId: String,
    val gameMode: String,
    val bannedChampions: List<BannedChampion>,
    val gameQueueConfigId: Long?,
    val observers: Observer?,
    val participants: List<CurrentGameParticipant>
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class BannedChampion(
    val pickTurn: Int,
    val championId: Long,
    val teamId: Long
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Observer(
    val encryptionKey: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CurrentGameParticipant(
    val championId: Long,
    val perks: Perks,
    val profileIconId: Long,
    val bot: Boolean,
    val teamId: Long,
    val riotId: String?,
    val puuid: String?,
    val spell1Id: Long,
    val spell2Id: Long,
    val tier: String? = null,
    val rank: String? = null,
    val leaguePoints: Int? = null,
    val wins: Int? = null,
    val losses: Int? = null,
    val gameCustomizationObjects: List<GameCustomizationObject>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Perks(
    val perkIds: List<Long>,
    val perkStyle: Long,
    val perkSubStyle: Long
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GameCustomizationObject(
    val category: String,
    val content: String
)
