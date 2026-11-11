package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

// This file contains all the DTOs needed to deserialize the complex response from Riot's Match API.
// The `@JsonIgnoreProperties(ignoreUnknown = true)` annotation is crucial here. It tells the JSON parser
// to simply ignore any fields in the JSON response that we haven't defined in our data classes.
// This makes our DTOs resilient to changes in the Riot API; if Riot adds new fields, our app won't crash.

/**
 * The top-level object for a single match response from the Riot API.
 * We are primarily interested in the `info` property.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchDto(
    val metadata: MatchMetadata?,
    val info: MatchInfo?,
    @param:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    @get:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    val dtoSchemaVersion: Int? = null
) {
    companion object {
        /**
         * Internal cache-compatibility marker stamped strictly on freshly decoded Riot Match-V5 HTTP responses.
         * Ignored on rollback by `@JsonIgnoreProperties(ignoreUnknown = true)` and never exposed as a game fact.
         */
        const val CURRENT_DTO_SCHEMA_VERSION: Int = 2
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchMetadata(
    val matchId: String?,
    val participants: List<String>?
)

/**
 * Contains the core metadata and participant list for a match.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchInfo(
    val gameId: Long?,
    val gameCreation: Long?,
    val gameDuration: Long?,
    val gameMode: String?,
    val gameVersion: String? = null,
    val queueId: Int?,
    val participants: List<ParticipantDto> = listOf(),
    val teams: List<TeamDto>? = null
)

/**
 * Team-level outcome and objective summary from Riot Match-v5 `info.teams`.
 * All fields are nullable so absent fields on legacy caches or custom modes stay `null` rather than `0`.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class TeamDto(
    val teamId: Int? = null,
    val win: Boolean? = null,
    val bans: List<TeamBanDto>? = null,
    val objectives: TeamObjectivesDto? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TeamBanDto(
    val championId: Int? = null,
    val pickTurn: Int? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TeamObjectivesDto(
    val baron: ObjectiveStatsDto? = null,
    val champion: ObjectiveStatsDto? = null,
    val dragon: ObjectiveStatsDto? = null,
    @com.fasterxml.jackson.annotation.JsonAlias("grubs")
    val horde: ObjectiveStatsDto? = null,
    val inhibitor: ObjectiveStatsDto? = null,
    @com.fasterxml.jackson.annotation.JsonAlias("herald")
    val riftHerald: ObjectiveStatsDto? = null,
    val tower: ObjectiveStatsDto? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ObjectiveStatsDto(
    val first: Boolean? = null,
    val kills: Int? = null
)

/**
 * Represents a single participant in a match. This is a massive object containing
 * all the stats for one player in one game.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ParticipantDto(
    val assists: Int? = null,
    val baronKills: Int? = null,
    val bountyLevel: Int? = null,
    @JsonProperty("challenges") val challenges: ChallengesDto? = null,
    val champExperience: Int? = null,
    val champLevel: Int? = null,
    val championId: Int? = null,
    val championName: String? = null,
    val championTransform: Int? = null,
    val consumablesPurchased: Int? = null,
    val damageDealtToBuildings: Int? = null,
    val damageDealtToObjectives: Int? = null,
    val damageDealtToTurrets: Int? = null,
    val damageSelfMitigated: Int? = null,
    val deaths: Int? = null,
    val detectorWardsPlaced: Int? = null,
    val doubleKills: Int? = null,
    val dragonKills: Int? = null,
    val eligibleForMythic: Boolean? = null,
    val firstBloodAssist: Boolean? = null,
    val firstBloodKill: Boolean? = null,
    val firstTowerAssist: Boolean? = null,
    val firstTowerKill: Boolean? = null,
    val gameEndedInEarlySurrender: Boolean? = null,
    val gameEndedInSurrender: Boolean? = null,
    val goldEarned: Int? = null,
    val goldSpent: Int? = null,
    val individualPosition: String? = null,
    val inhibitorKills: Int? = null,
    val inhibitorTakedowns: Int? = null,
    val inhibitorsLost: Int? = null,
    val item0: Int? = null,
    val item1: Int? = null,
    val item2: Int? = null,
    val item3: Int? = null,
    val item4: Int? = null,
    val item5: Int? = null,
    val item6: Int? = null,
    val itemsPurchased: Int? = null,
    val killingSprees: Int? = null,
    val kills: Int? = null,
    val lane: String? = null,
    val largestCriticalStrike: Int? = null,
    val largestKillingSpree: Int? = null,
    val largestMultiKill: Int? = null,
    val longestTimeSpentLiving: Int? = null,
    val magicDamageDealt: Int? = null,
    val magicDamageDealtToChampions: Int? = null,
    val magicDamageTaken: Int? = null,
    val neutralMinionsKilled: Int? = null,
    val nexusKills: Int? = null,
    val nexusTakedowns: Int? = null,
    val nexusLost: Int? = null,
    val objectivesStolen: Int? = null,
    val objectivesStolenAssists: Int? = null,
    val participantId: Int? = null,
    val pentaKills: Int? = null,
    @JsonProperty("perks") val perks: PerksDto? = null,
    val physicalDamageDealt: Int? = null,
    val physicalDamageDealtToChampions: Int? = null,
    val physicalDamageTaken: Int? = null,
    val profileIcon: Int? = null,
    val puuid: String? = null,
    val quadraKills: Int? = null,
    val riotIdGameName: String? = null,
    val riotIdTagline: String? = null,
    val role: String? = null,
    val sightWardsBoughtInGame: Int? = null,
    val spell1Casts: Int? = null,
    val spell2Casts: Int? = null,
    val spell3Casts: Int? = null,
    val spell4Casts: Int? = null,
    val summoner1Casts: Int? = null,
    val summoner1Id: Int? = null,
    val summoner2Casts: Int? = null,
    val summoner2Id: Int? = null,
    val summonerId: String? = null,
    val summonerLevel: Int? = null,
    val summonerName: String? = null,
    val teamEarlySurrendered: Boolean? = null,
    val teamId: Int? = null,
    val teamPosition: String? = null,
    val timeCCingOthers: Int? = null,
    val timePlayed: Int? = null,
    val totalDamageDealt: Int? = null,
    val totalDamageDealtToChampions: Int? = null,
    val totalDamageShieldedOnTeammates: Int? = null,
    val totalDamageTaken: Int? = null,
    val totalHeal: Int? = null,
    val totalHealsOnTeammates: Int? = null,
    val totalMinionsKilled: Int? = null,
    val totalTimeCCDealt: Int? = null,
    val totalTimeSpentDead: Int? = null,
    val totalUnitsHealed: Int? = null,
    val tripleKills: Int? = null,
    val trueDamageDealt: Int? = null,
    val trueDamageDealtToChampions: Int? = null,
    val trueDamageTaken: Int? = null,
    val turretKills: Int? = null,
    val turretTakedowns: Int? = null,
    val turretsLost: Int? = null,
    val unrealKills: Int? = null,
    val visionScore: Int? = null,
    val visionWardsBoughtInGame: Int? = null,
    val wardsKilled: Int? = null,
    val wardsPlaced: Int? = null,
    val win: Boolean? = null,
    // Season 2026 Role Quest slot (`1200..1222` or Bot Carry boots).
    // MUST remain `null` when absent (legacy cache / non-quest mode), NEVER coerced to `0`.
    val roleBoundItem: Int? = null,
    val eligibleForProgression: Boolean? = null,
    val totalAllyJungleMinionsKilled: Int? = null,
    val totalEnemyJungleMinionsKilled: Int? = null,
    val damageDealtToEpicMonsters: Int? = null,
    // Season 2026 Behavioral / IGNB Surrender fields — NULLABLE CAPTURE ONLY.
    // Do NOT apply winrate, remake, or cohort-exclusion policy changes without verified Riot semantics.
    val causedGameEndFromIGNBSurrender: Boolean? = null,
    val gameEndedInIGNBSurrender: Boolean? = null,
    val teamIGNBSurrendered: Boolean? = null,
    val wasPremadeWithIGNBGameEndCauser: Boolean? = null,
    val wasSevereTransgressor: Boolean? = null,
    val wasPremadeWithSevereTransgressor: Boolean? = null
)

/**
 * Represents the "challenges" object within a participant's data, containing highly specific in-game stats.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ChallengesDto(
    val controlWardTimeCoverageInRiverOrEnemyHalf: Float? = null,
    val earliestBaron: Double? = null,
    val earliestDragonTakedown: Double? = null,
    val earlyLaningPhaseGoldExpAdvantage: Double? = null,
    val fasterSupportQuestCompletion: Int? = null,
    val highestChampionDamage: Int? = null,
    val highestCrowdControlScore: Int? = null,
    val junglerKillsEarlyJungle: Int? = null,
    val killsOnLanersEarlyJungleAsJungler: Int? = null,
    val laningPhaseGoldExpAdvantage: Double? = null,
    val legendaryCount: Int? = null,
    val maxCsAdvantageOnLaneOpponent: Float? = null,
    val maxLevelLeadLaneOpponent: Int? = null,
    val mostWardsDestroyedOneSweeper: Int? = null,
    val playedChampSelectPosition: Int? = null,
    val soloTurretsLategame: Int? = null,
    val takedownsFirst25Minutes: Int? = null,
    val teleportTakedowns: Int? = null,
    val threeWardsOneSweeperCount: Int? = null,
    val visionScoreAdvantageLaneOpponent: Float? = null,
    @JsonProperty("InfernalScalePickup") val infernalScalePickup: Int? = null,
    val voidMonsterKill: Int? = null,
    val abilityUses: Int? = null,
    val acesBefore15Minutes: Int? = null,
    val alliedJungleMonsterKills: Float? = null,
    val bountyGold: Double? = null,
    val buffsStolen: Int? = null,
    val completeSupportQuestInTime: Int? = null,
    val controlWardsPlaced: Int? = null,
    val damagePerMinute: Float? = null,
    val damageTakenOnTeamPercentage: Float? = null,
    val dodgeSkillShotsSmallWindow: Int? = null,
    val dragonTakedowns: Int? = null,
    val legendaryItemUsed: List<Int>? = null,
    val effectiveHealAndShielding: Float? = null,
    val enemyChampionImmobilizations: Int? = null,
    val enemyJungleMonsterKills: Float? = null,
    val epicMonsterKillsNearEnemyJungler: Int? = null,
    val epicMonsterKillsWithin30SecondsOfSpawn: Int? = null,
    val epicMonsterSteals: Int? = null,
    val epicMonsterStolenWithoutSmite: Int? = null,
    val firstTurretKilled: Int? = null,
    val firstTurretKilledTime: Float? = null,
    val flawlessAces: Int? = null,
    val fullTeamTakedown: Int? = null,
    val getTakedownsInAllLanesEarlyJungleAsLaner: Int? = null,
    val soloKills: Int? = null,
    val turretPlatesTaken: Int? = null,
    val laneMinionsFirst10Minutes: Int? = null,
    val jungleCsBefore10Minutes: Float? = null,
    val scuttleCrabKills: Int? = null,
    val wardTakedowns: Int? = null,
    val kda: Float? = null,
    val killParticipation: Float? = null,
    val goldPerMinute: Float? = null,
    val visionScorePerMinute: Float? = null,
)

/**
 * Represents the runes and perks selected by a participant.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class PerksDto(
    @JsonProperty("statPerks") val statPerks: PerkStatsDto?,
    val styles: List<PerkStyleDto>?
)

/**
 * The small, tertiary runes (e.g., Attack Speed, Armor).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStatsDto(
    val defense: Int?,
    val flex: Int?,
    val offense: Int?
)

/**
 * Represents a single rune tree (e.g., Precision, Domination).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStyleDto(
    val description: String?,
    val style: Int?,
    val selections: List<PerkStyleSelectionDto>?
)

/**
 * Represents a single rune selection within a tree.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStyleSelectionDto(
    val perk: Int?,
    val var1: Int?,
    val var2: Int?,
    val var3: Int?
)
