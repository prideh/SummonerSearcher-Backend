package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Nullable, documented improvement statistics contract derived strictly from Riot Match-v5
 * (`MatchDto`) and Timeline-v5 (`MatchTimelineDto`) sources:
 *  1. [phaseFarming]: lane (`minionsKilled`) and jungle (`jungleMinionsKilled`) CS gained per minute
 *     across `10-20m` and `20m-end` using actual timestamped checkpoints. Shortened games not reaching
 *     a checkpoint are excluded (`excludedShortGames`) and never extrapolated.
 *  2. [leadRetention]: player-vs-role-opponent gold and XP leads at `10m` and `20m`, count of eligible
 *     games ahead at `10m` and still ahead at `20m`, plus all available relevant objective/tower events
 *     in `10-20m` (not restricted to personally last-hit events), explicitly marked as temporal association.
 *  3. [deathCost]: recorded `totalTimeSpentDead` seconds and game-duration share from `MatchDto`, plus
 *     recorded `bounty` (base kill gold) and `shutdownBounty` (streak bonus gold) on player deaths from
 *     timeline `CHAMPION_KILL` events without double-counting. Unknown/absent values remain `null`, not `0`.
 *  4. [unspentGold]: median and max available 1-minute `currentGold` snapshots, sample count, and sampled
 *     timestamps carrying high balances (`>= highBalanceThresholdGold`) prior to an `ITEM_PURCHASED` event.
 *  5. [teamObjectives]: team-level objective outcomes (`first` flags and `kills` for `tower`, `dragon`,
 *     `baron`, `riftHerald`, `horde` [grubs]) from `MatchDto.info.teams`, kept distinct from personal
 *     takedowns and distinguishing absent fields (`null`) from `0`.
 *
 * Includes overall aggregation as well as [byChampion] and [byRole] breakdowns with explicit sample counts,
 * eligible denominators, and exclusion counts.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ImprovementStatsDto(
    val totalGamesConsidered: Int = 0,
    val excludedInvalidDurationGames: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val phaseFarming: PhaseFarmingStatsDto = PhaseFarmingStatsDto(),
    val leadRetention: LeadRetentionStatsDto = LeadRetentionStatsDto(),
    val deathCost: DeathCostStatsDto = DeathCostStatsDto(),
    val unspentGold: UnspentGoldStatsDto = UnspentGoldStatsDto(),
    val teamObjectives: TeamObjectivesStatsDto = TeamObjectivesStatsDto(),
    val byChampion: Map<String, ImprovementStatsBreakdownDto> = emptyMap(),
    val byRole: Map<String, ImprovementStatsBreakdownDto> = emptyMap()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ImprovementStatsBreakdownDto(
    val key: String,
    val totalGamesConsidered: Int = 0,
    val excludedInvalidDurationGames: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val phaseFarming: PhaseFarmingStatsDto = PhaseFarmingStatsDto(),
    val leadRetention: LeadRetentionStatsDto = LeadRetentionStatsDto(),
    val deathCost: DeathCostStatsDto = DeathCostStatsDto(),
    val unspentGold: UnspentGoldStatsDto = UnspentGoldStatsDto(),
    val teamObjectives: TeamObjectivesStatsDto = TeamObjectivesStatsDto()
)

// ─────────────────────────────────────────────────────────────────────────────
// 1. Phase Farming (10-20m and 20m-end)
// ─────────────────────────────────────────────────────────────────────────────

@JsonIgnoreProperties(ignoreUnknown = true)
data class PhaseFarmingStatsDto(
    val totalGamesConsidered: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val min10To20: PhaseFarmingWindowDto = PhaseFarmingWindowDto(windowLabel = "10-20m"),
    val min20ToEnd: PhaseFarmingWindowDto = PhaseFarmingWindowDto(windowLabel = "20m-end"),
    val perMatch: List<MatchPhaseFarmingDto> = emptyList(),
    val methodologyNote: String = "Computed from actual timestamped timeline checkpoints using separate minionsKilled (lane) and jungleMinionsKilled (jungle) counters. Games ending before a checkpoint are excluded and never extrapolated; individual missed CS is never claimed."
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PhaseFarmingWindowDto(
    val windowLabel: String,
    val eligibleGames: Int = 0,
    val excludedShortGames: Int = 0,
    val excludedMissingFrameCounters: Int = 0,
    val avgLaneCsGained: Double? = null,
    val avgJungleCsGained: Double? = null,
    val avgTotalCsGained: Double? = null,
    val avgWindowMinutes: Double? = null,
    val avgLaneCsPerMin: Double? = null,
    val avgJungleCsPerMin: Double? = null,
    val avgTotalCsPerMin: Double? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchPhaseFarmingDto(
    val matchId: String,
    val championName: String,
    val role: String,
    val gameDurationSeconds: Long,
    val reached10To20: Boolean,
    val checkpoint10TimestampMs: Long? = null,
    val checkpoint20TimestampMs: Long? = null,
    val laneCsAt10: Int? = null,
    val jungleCsAt10: Int? = null,
    val laneCsAt20: Int? = null,
    val jungleCsAt20: Int? = null,
    val laneCsGained10To20: Int? = null,
    val jungleCsGained10To20: Int? = null,
    val laneCsPerMin10To20: Double? = null,
    val jungleCsPerMin10To20: Double? = null,
    val totalCsPerMin10To20: Double? = null,
    val reached20ToEnd: Boolean,
    val checkpointEndTimestampMs: Long? = null,
    val laneCsAtEnd: Int? = null,
    val jungleCsAtEnd: Int? = null,
    val laneCsGained20ToEnd: Int? = null,
    val jungleCsGained20ToEnd: Int? = null,
    val laneCsPerMin20ToEnd: Double? = null,
    val jungleCsPerMin20ToEnd: Double? = null,
    val totalCsPerMin20ToEnd: Double? = null
)

// ─────────────────────────────────────────────────────────────────────────────
// 2. Lead Retention (10m vs 20m Gold/XP Leads & Temporal Objective Events)
// ─────────────────────────────────────────────────────────────────────────────

@JsonIgnoreProperties(ignoreUnknown = true)
data class LeadRetentionStatsDto(
    val totalGamesConsidered: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val excludedMissingOpponentGames: Int = 0,
    val excludedShortGamesBefore10: Int = 0,
    val excludedShortGamesBefore20: Int = 0,
    val eligibleGamesAt10: Int = 0,
    val eligibleGamesAt20: Int = 0,
    val avgGoldLeadAt10: Double? = null,
    val avgXpLeadAt10: Double? = null,
    val avgGoldLeadAt20: Double? = null,
    val avgXpLeadAt20: Double? = null,
    val gamesAheadInGoldAt10: Int = 0,
    val gamesAheadInGoldAt10Reaching20: Int = 0,
    val gamesRetainedGoldLeadAt20: Int = 0,
    val goldLeadRetentionRate: Double? = null,
    val gamesAheadInXpAt10: Int = 0,
    val gamesAheadInXpAt10Reaching20: Int = 0,
    val gamesRetainedXpLeadAt20: Int = 0,
    val xpLeadRetentionRate: Double? = null,
    val allyObjectiveEvents10To20: Int = 0,
    val enemyObjectiveEvents10To20: Int = 0,
    val allyTowerEvents10To20: Int = 0,
    val enemyTowerEvents10To20: Int = 0,
    val perMatch: List<MatchLeadRetentionDto> = emptyList(),
    val associationNote: String = "Objective and tower events in the 10-20m window include all available team events (not only personally last-hit events) and represent temporal association only, not personal player credit or proof of a throw."
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchLeadRetentionDto(
    val matchId: String,
    val championName: String,
    val opponentChampionName: String?,
    val role: String,
    val win: Boolean,
    val goldLeadAt10: Int? = null,
    val xpLeadAt10: Int? = null,
    val goldLeadAt20: Int? = null,
    val xpLeadAt20: Int? = null,
    val aheadInGoldAt10: Boolean? = null,
    val retainedGoldLeadAt20: Boolean? = null,
    val aheadInXpAt10: Boolean? = null,
    val retainedXpLeadAt20: Boolean? = null,
    val goldLeadDelta10To20: Int? = null,
    val xpLeadDelta10To20: Int? = null,
    val temporalEvents10To20: List<TemporalObjectiveEventDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TemporalObjectiveEventDto(
    val eventCategory: String, // "ELITE_MONSTER_KILL", "BUILDING_KILL", "TURRET_PLATE_DESTROYED"
    val timestampMs: Long,
    val minuteMark: Int,
    val secondMark: Int,
    val objectiveType: String, // e.g., "DRAGON", "HORDE", "RIFTHERALD", "TOWER_BUILDING", "TURRET_PLATE"
    val monsterSubType: String? = null,
    val laneType: String? = null,
    val towerType: String? = null,
    val securingTeam: String, // "ALLY_TEAM", "ENEMY_TEAM", "UNKNOWN"
    val killerChampion: String? = null,
    val assistingChampions: List<String> = emptyList(),
    val playerPersonallyInvolved: Boolean = false
)

// ─────────────────────────────────────────────────────────────────────────────
// 3. Death Cost (totalTimeSpentDead & Recorded Death Bounties)
// ─────────────────────────────────────────────────────────────────────────────

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeathCostStatsDto(
    val totalGamesConsidered: Int = 0,
    val eligibleTimeDeadGames: Int = 0,
    val excludedMissingTimeSpentDeadGames: Int = 0,
    val totalTimeSpentDeadSeconds: Int? = null,
    val avgTimeSpentDeadSeconds: Double? = null,
    val avgDeadTimeGameShare: Double? = null,
    val avgDeadTimeGameSharePct: Double? = null,
    val overallDeadTimeGameShare: Double? = null,
    val overallDeadTimeGameSharePct: Double? = null,
    val bountyOnPlayerDeaths: DeathCostBountySummaryDto = DeathCostBountySummaryDto(),
    val perMatch: List<MatchDeathCostDto> = emptyList(),
    val goldComponentDefinition: String = "baseKillBountyGold (Riot event.bounty) is the base kill reward to the killer; shutdownBountyGold (Riot event.shutdownBounty) is the additional streak shutdown bonus. combinedBountyGold = baseKillBountyGold + shutdownBountyGold (disjoint components, never double-counted). Unknown fields remain null, not 0; no lost farm or death avoidability is inferred."
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeathCostBountySummaryDto(
    val eligibleTimelineGames: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val totalRecordedDeaths: Int? = null,
    val deathsWithBaseBountyData: Int = 0,
    val deathsWithShutdownBountyData: Int = 0,
    val deathsWithAnyBountyData: Int = 0,
    val deathsMissingBountyData: Int = 0,
    val totalBaseKillBountyGold: Int? = null,
    val totalShutdownBountyGold: Int? = null,
    val totalCombinedBountyGold: Int? = null,
    val avgBaseKillBountyPerDeath: Double? = null,
    val avgShutdownBountyPerDeath: Double? = null,
    val avgCombinedBountyPerDeath: Double? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchDeathCostDto(
    val matchId: String,
    val championName: String,
    val role: String,
    val gameDurationSeconds: Long,
    val deathsFromMatchDto: Int? = null,
    val totalTimeSpentDeadSeconds: Int? = null,
    val deadTimeGameShare: Double? = null,
    val deadTimeGameSharePct: Double? = null,
    val hasTimeline: Boolean = false,
    val recordedDeathsCount: Int? = null,
    val totalBaseKillBountyGold: Int? = null,
    val totalShutdownBountyGold: Int? = null,
    val totalCombinedBountyGold: Int? = null,
    val recordedDeaths: List<RecordedDeathBountyEventDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RecordedDeathBountyEventDto(
    val timestampMs: Long,
    val minuteMark: Int,
    val secondMark: Int,
    val killerChampion: String? = null,
    val assistingChampions: List<String> = emptyList(),
    val baseKillBountyGold: Int? = null,
    val shutdownBountyGold: Int? = null,
    val combinedBountyGold: Int? = null
)

// ─────────────────────────────────────────────────────────────────────────────
// 4. Unspent Gold (1-Minute currentGold Snapshots)
// ─────────────────────────────────────────────────────────────────────────────

@JsonIgnoreProperties(ignoreUnknown = true)
data class UnspentGoldStatsDto(
    val totalGamesConsidered: Int = 0,
    val eligibleGames: Int = 0,
    val excludedMissingTimelineGames: Int = 0,
    val excludedNoCurrentGoldSamplesGames: Int = 0,
    val sampleCount: Int = 0,
    val medianCurrentGold: Double? = null,
    val maxCurrentGold: Int? = null,
    val avgCurrentGold: Double? = null,
    val highBalanceThresholdGold: Int = HIGH_UNSPENT_GOLD_THRESHOLD,
    val highBalanceSampleCount: Int? = null,
    val highBalanceBeforePurchaseSampleCount: Int? = null,
    val highBalanceBeforePurchaseSamples: List<UnspentGoldHighBalanceSampleDto> = emptyList(),
    val perMatch: List<MatchUnspentGoldDto> = emptyList(),
    val samplingSemanticsNote: String = "Derived from discrete 1-minute participantFrame.currentGold snapshots (not continuous duration). High balances before purchases are descriptive timestamp samples, not an automatic mistake score or invented recall window."
) {
    companion object {
        const val HIGH_UNSPENT_GOLD_THRESHOLD = 1300
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class UnspentGoldHighBalanceSampleDto(
    val matchId: String,
    val championName: String,
    val role: String,
    val sampledMinute: Int,
    val sampledTimestampMs: Long,
    val currentGoldAtSample: Int,
    val nextPurchaseTimestampMs: Long? = null,
    val nextPurchaseMinute: Int? = null,
    val nextPurchaseItemId: Int? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchUnspentGoldDto(
    val matchId: String,
    val championName: String,
    val role: String,
    val sampleCount: Int,
    val medianCurrentGold: Double? = null,
    val maxCurrentGold: Int? = null,
    val avgCurrentGold: Double? = null,
    val highBalanceSampleCount: Int? = null,
    val highBalanceBeforePurchaseSampleCount: Int? = null
)

// ─────────────────────────────────────────────────────────────────────────────
// 5. Team Objectives (MatchDto.info.teams vs Personal Takedowns)
// ─────────────────────────────────────────────────────────────────────────────

@JsonIgnoreProperties(ignoreUnknown = true)
data class TeamObjectivesStatsDto(
    val totalGamesConsidered: Int = 0,
    val eligibleTeamObjectiveGames: Int = 0,
    val excludedMissingTeamsDataGames: Int = 0,
    val tower: ObjectiveTypeSummaryDto = ObjectiveTypeSummaryDto(objectiveKey = "tower"),
    val dragon: ObjectiveTypeSummaryDto = ObjectiveTypeSummaryDto(objectiveKey = "dragon"),
    val baron: ObjectiveTypeSummaryDto = ObjectiveTypeSummaryDto(objectiveKey = "baron"),
    val riftHerald: ObjectiveTypeSummaryDto = ObjectiveTypeSummaryDto(objectiveKey = "riftHerald"),
    val horde: ObjectiveTypeSummaryDto = ObjectiveTypeSummaryDto(objectiveKey = "horde"),
    val perMatch: List<MatchTeamObjectivesDto> = emptyList(),
    val attributionNote: String = "Team objective outcomes (first flags and kills from MatchDto.info.teams) are reported separately from personal last-hits/takedowns. Absent fields remain null and are excluded from denominators rather than treated as 0."
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ObjectiveTypeSummaryDto(
    val objectiveKey: String,
    val eligibleGamesWithKills: Int = 0,
    val eligibleGamesWithFirstFlag: Int = 0,
    val excludedMissingObjectiveGames: Int = 0,
    val teamFirstCount: Int? = null,
    val teamFirstRate: Double? = null,
    val teamTotalKills: Int? = null,
    val teamAvgKills: Double? = null,
    val enemyEligibleGamesWithKills: Int = 0,
    val enemyTeamTotalKills: Int? = null,
    val enemyTeamAvgKills: Double? = null,
    val personalLastHitEligibleGames: Int = 0,
    val personalTotalLastHits: Int? = null,
    val personalAvgLastHits: Double? = null,
    val personalTakedownEligibleGames: Int = 0,
    val personalTotalTakedowns: Int? = null,
    val personalAvgTakedowns: Double? = null,
    val timelineEligibleGames: Int = 0,
    val timelinePersonalInvolvements: Int? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchTeamObjectivesDto(
    val matchId: String,
    val championName: String,
    val role: String,
    val teamId: Int?,
    val win: Boolean?,
    val hasTeamsData: Boolean,
    val allyObjectives: TeamObjectivesDto? = null,
    val enemyObjectives: TeamObjectivesDto? = null,
    val personalTurretKills: Int? = null,
    val personalTurretTakedowns: Int? = null,
    val personalDragonKills: Int? = null,
    val personalDragonTakedowns: Int? = null,
    val personalBaronKills: Int? = null,
    val personalVoidMonsterKills: Int? = null
)
