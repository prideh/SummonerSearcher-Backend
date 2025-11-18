package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchDto(
    val info: MatchInfo?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchInfo(
    val gameId: Long?,
    val gameCreation: Long?,
    val gameDuration: Long?,
    val gameMode: String?,
    val queueId: Int?,
    val participants: List<ParticipantDto> = listOf()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ParticipantDto(
    val assists: Int?,
    val baronKills: Int?,
    val bountyLevel: Int?,
    @JsonProperty("challenges") val challenges: ChallengesDto?,
    val champExperience: Int?,
    val champLevel: Int?,
    val championId: Int?,
    val championName: String?,
    val championTransform: Int?,
    val consumablesPurchased: Int?,
    val damageDealtToBuildings: Int?,
    val damageDealtToObjectives: Int?,
    val damageDealtToTurrets: Int?,
    val damageSelfMitigated: Int?,
    val deaths: Int?,
    val detectorWardsPlaced: Int?,
    val doubleKills: Int?,
    val dragonKills: Int?,
    val eligibleForMythic: Boolean?,
    val firstBloodAssist: Boolean?,
    val firstBloodKill: Boolean?,
    val firstTowerAssist: Boolean?,
    val firstTowerKill: Boolean?,
    val gameEndedInEarlySurrender: Boolean?,
    val gameEndedInSurrender: Boolean?,
    val goldEarned: Int?,
    val goldSpent: Int?,
    val individualPosition: String?,
    val inhibitorKills: Int?,
    val inhibitorTakedowns: Int?,
    val inhibitorsLost: Int?,
    val item0: Int?,
    val item1: Int?,
    val item2: Int?,
    val item3: Int?,
    val item4: Int?,
    val item5: Int?,
    val item6: Int?,
    val itemsPurchased: Int?,
    val killingSprees: Int?,
    val kills: Int?,
    val lane: String?,
    val largestCriticalStrike: Int?,
    val largestKillingSpree: Int?,
    val largestMultiKill: Int?,
    val longestTimeSpentLiving: Int?,
    val magicDamageDealt: Int?,
    val magicDamageDealtToChampions: Int?,
    val magicDamageTaken: Int?,
    val neutralMinionsKilled: Int?,
    val nexusKills: Int?,
    val nexusTakedowns: Int?,
    val nexusLost: Int?,
    val objectivesStolen: Int?,
    val objectivesStolenAssists: Int?,
    val participantId: Int?,
    val pentaKills: Int?,
    @JsonProperty("perks") val perks: PerksDto?,
    val physicalDamageDealt: Int?,
    val physicalDamageDealtToChampions: Int?,
    val physicalDamageTaken: Int?,
    val profileIcon: Int?,
    val puuid: String?,
    val quadraKills: Int?,
    val riotIdGameName: String?,
    val riotIdTagline: String?,
    val role: String?,
    val sightWardsBoughtInGame: Int?,
    val spell1Casts: Int?,
    val spell2Casts: Int?,
    val spell3Casts: Int?,
    val spell4Casts: Int?,
    val summoner1Casts: Int?,
    val summoner1Id: Int?,
    val summoner2Casts: Int?,
    val summoner2Id: Int?,
    val summonerId: String?,
    val summonerLevel: Int?,
    val summonerName: String?,
    val teamEarlySurrendered: Boolean?,
    val teamId: Int?,
    val teamPosition: String?,
    val timeCCingOthers: Int?,
    val timePlayed: Int?,
    val totalDamageDealt: Int?,
    val totalDamageDealtToChampions: Int?,
    val totalDamageShieldedOnTeammates: Int?,
    val totalDamageTaken: Int?,
    val totalHeal: Int?,
    val totalHealsOnTeammates: Int?,
    val totalMinionsKilled: Int?,
    val totalTimeCCDealt: Int?,
    val totalTimeSpentDead: Int?,
    val totalUnitsHealed: Int?,
    val tripleKills: Int?,
    val trueDamageDealt: Int?,
    val trueDamageDealtToChampions: Int?,
    val trueDamageTaken: Int?,
    val turretKills: Int?,
    val turretTakedowns: Int?,
    val turretsLost: Int?,
    val unrealKills: Int?,
    val visionScore: Int?,
    val visionWardsBoughtInGame: Int?,
    val wardsKilled: Int?,
    val wardsPlaced: Int?,
    val win: Boolean?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ChallengesDto(
    val controlWardTimeCoverageInRiverOrEnemyHalf: Float?,
    val earliestBaron: Int?,
    val earliestDragonTakedown: Int?,
    val earlyLaningPhaseGoldExpAdvantage: Int?,
    val fasterSupportQuestCompletion: Int?,
    val highestChampionDamage: Int?,
    val highestCrowdControlScore: Int?,
    val junglerKillsEarlyJungle: Int?,
    val killsOnLanersEarlyJungleAsJungler: Int?,
    val laningPhaseGoldExpAdvantage: Int?,
    val legendaryCount: Int?,
    val maxCsAdvantageOnLaneOpponent: Float?,
    val maxLevelLeadLaneOpponent: Int?,
    val mostWardsDestroyedOneSweeper: Int?,
    val playedChampSelectPosition: Int?,
    val soloTurretsLategame: Int?,
    val takedownsFirst25Minutes: Int?,
    val teleportTakedowns: Int?,
    val threeWardsOneSweeperCount: Int?,
    val visionScoreAdvantageLaneOpponent: Float?,
    @JsonProperty("InfernalScalePickup") val infernalScalePickup: Int?,
    val voidMonsterKill: Int?,
    val abilityUses: Int?,
    val acesBefore15Minutes: Int?,
    val alliedJungleMonsterKills: Float?,
    val bountyGold: Int?,
    val buffsStolen: Int?,
    val completeSupportQuestInTime: Int?,
    val controlWardsPlaced: Int?,
    val damagePerMinute: Float?,
    val damageTakenOnTeamPercentage: Float?,
    val dodgeSkillShotsSmallWindow: Int?,
    val dragonTakedowns: Int?,
    val legendaryItemUsed: List<Int>?,
    val effectiveHealAndShielding: Float?,
    val enemyChampionImmobilizations: Int?,
    val enemyJungleMonsterKills: Float?,
    val epicMonsterKillsNearEnemyJungler: Int?,
    val epicMonsterKillsWithin30SecondsOfSpawn: Int?,
    val epicMonsterSteals: Int?,
    val epicMonsterStolenWithoutSmite: Int?,
    val firstTurretKilled: Int?,
    val firstTurretKilledTime: Float?,
    val flawlessAces: Int?,
    val fullTeamTakedown: Int?,
    val getTakedownsInAllLanesEarlyJungleAsLaner: Int?,
    val soloKills: Int?,
    val turretPlatesTaken: Int?,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PerksDto(
    @JsonProperty("statPerks") val statPerks: PerkStatsDto?,
    val styles: List<PerkStyleDto>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStatsDto(
    val defense: Int?,
    val flex: Int?,
    val offense: Int?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStyleDto(
    val description: String?,
    val style: Int?,
    val selections: List<PerkStyleSelectionDto>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PerkStyleSelectionDto(
    val perk: Int?,
    val var1: Int?,
    val var2: Int?,
    val var3: Int?
)
