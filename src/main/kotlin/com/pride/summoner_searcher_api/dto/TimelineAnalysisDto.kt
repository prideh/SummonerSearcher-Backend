package com.pride.summoner_searcher_api.dto

/**
 * Aggregated analysis result for a player's last N match timelines.
 */
data class TimelineAnalysisDto(
    /** All positions the player occupied (sampled each frame), for heatmap display */
    val heatmapPositions: List<HeatmapPoint>,
    /** Positions where the player was killed (for death cluster layer) */
    val deathPositions: List<HeatmapPoint>,
    /** Positions where the player placed wards */
    val wardPositions: List<HeatmapPoint>,
    /** Positions where the player got kills */
    val killPositions: List<HeatmapPoint>,
    /** Per-match power spike data (player gold vs opponent gold over time) */
    val powerSpikeTimelines: List<MatchPowerSpikeDto>,
    /** Per-match build order */
    val buildOrders: List<BuildOrderDto>,
    /** Per-match skill order */
    val skillOrders: List<SkillOrderDto>,
    /** Per-match event timeline (kills, deaths, plates, objectives for player + opponent) */
    val eventTimelines: List<MatchEventTimelineDto>,
    /** Aggregate summary stats */
    val aggregateSummary: TimelineAggregateSummary
)

data class HeatmapPoint(
    val x: Int,
    val y: Int,
    val matchId: String
)

/** Minute-by-minute gold/level comparison between player and lane opponent */
data class PowerSpikePoint(
    val minute: Int,
    val playerGold: Int,
    val opponentGold: Int,
    val playerGoldLead: Int,
    val playerItems: List<Int>,     // item IDs purchased by the minute mark
    val opponentItems: List<Int>
)

data class MatchPowerSpikeDto(
    val matchId: String,
    val playerChampion: String,
    val opponentChampion: String,
    val win: Boolean,
    val points: List<PowerSpikePoint>
)

data class BuildOrderDto(
    val matchId: String,
    val playerChampion: String,
    val win: Boolean,
    val items: List<BuildItem>
)

data class BuildItem(
    val itemId: Int,
    val timestamp: Long,
    val minuteMark: Int
)

data class SkillOrderDto(
    val matchId: String,
    val playerChampion: String,
    val win: Boolean,
    /** First skill maxed, second, third */
    val maxOrder: List<Int>,
    val levelUps: List<SkillUp>
)

data class SkillUp(
    val skillSlot: Int,   // 1=Q, 2=W, 3=E, 4=R
    val level: Int,
    val minuteMark: Int
)

data class MatchEventTimelineDto(
    val matchId: String,
    val playerChampion: String,
    val opponentChampion: String,
    val win: Boolean,
    val events: List<MatchEventEntry>
)

data class MatchEventEntry(
    val type: String,          // KILL, DEATH, ASSIST, PLATE, OBJECTIVE, WARD_PLACED, WARD_KILLED
    val minuteMark: Int,
    val secondMark: Int,       // full seconds within the minute
    val actor: String?,        // champion name of the actor
    val target: String?,       // champion name of the target, or objective name
    val isPlayer: Boolean,     // true if the tracked player is the actor
    val isOpponent: Boolean,   // true if the tracked opponent is the actor
    val laneType: String?,
    val monsterType: String?,
    val position: TimelinePositionDto?
)

data class TimelineAggregateSummary(
    val avgFirstDeathMinute: Double,
    val mostDangerousZone: String,   // map quadrant name
    val avgGoldLeadAt10: Double,
    val avgGoldLeadAt15: Double,
    val wardsPlacedTotal: Int,
    val deathsTotal: Int,
    val killsTotal: Int,
    val gamesAnalyzed: Int
)
