package com.pride.summoner_searcher_api.prototype.probenchmark

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsWindowResponseDto(
    val esportsGameId: String? = null,
    val esportsMatchId: String? = null,
    val gameMetadata: LivestatsGameMetadataDto? = null,
    val frames: List<LivestatsWindowFrameDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsGameMetadataDto(
    val patchVersion: String? = null,
    val blueTeamMetadata: LivestatsTeamMetadataDto? = null,
    val redTeamMetadata: LivestatsTeamMetadataDto? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsTeamMetadataDto(
    val esportsTeamId: String? = null,
    val participantMetadata: List<LivestatsParticipantMetadataDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsParticipantMetadataDto(
    val participantId: Int,
    val esportsPlayerId: String? = null,
    val summonerName: String,
    val championId: String,
    val role: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsWindowFrameDto(
    val rfc460Timestamp: String,
    val gameState: String,
    val blueTeam: LivestatsWindowTeamFrameDto,
    val redTeam: LivestatsWindowTeamFrameDto
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsWindowTeamFrameDto(
    val totalGold: Int = 0,
    val inhibitors: Int = 0,
    val towers: Int = 0,
    val barons: Int = 0,
    val totalKills: Int = 0,
    val participants: List<LivestatsWindowParticipantFrameDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsWindowParticipantFrameDto(
    val participantId: Int,
    val totalGold: Int = 0,
    val level: Int = 1,
    val kills: Int = 0,
    val deaths: Int = 0,
    val assists: Int = 0,
    val creepScore: Int = 0,
    val currentHealth: Int = 0,
    val maxHealth: Int = 0
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsDetailsResponseDto(
    val frames: List<LivestatsDetailsFrameDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsDetailsFrameDto(
    val rfc460Timestamp: String,
    val participants: List<LivestatsDetailsParticipantFrameDto> = emptyList()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LivestatsDetailsParticipantFrameDto(
    val participantId: Int,
    val level: Int = 1,
    val kills: Int = 0,
    val deaths: Int = 0,
    val assists: Int = 0,
    val totalGoldEarned: Int = 0,
    val creepScore: Int = 0,
    val killParticipation: Double = 0.0,
    val championDamageShare: Double = 0.0,
    val wardsPlaced: Int = 0,
    val wardsDestroyed: Int = 0,
    val items: List<Int> = emptyList(),
    val abilities: List<String> = emptyList()
)

data class PauseIntervalDto(
    val pauseStartIso: String,
    val pauseEndIso: String,
    val durationSeconds: Double,
    val elapsedFromSpawnAtStartSeconds: Double,
    val detectedBy: String
)

data class ClockAlignmentResultDto(
    val tFirstFrameIso: String,
    val tSpawnIso: String,
    val tPassiveGoldIso: String?,
    val tFirstMidCsIso: String?,
    val tFirstSideCsIso: String?,
    val tFirstJungleCsIso: String?,
    val tFinishedIso: String,
    val preSpawnOffsetSeconds: Double,
    val passiveGoldFromSpawnSeconds: Double?,
    val passiveGoldErrorVsTheorySeconds: Double?,
    val firstMidCsFromSpawnSeconds: Double?,
    val firstSideCsFromSpawnSeconds: Double?,
    val firstJungleCsFromSpawnSeconds: Double?,
    val pauses: List<PauseIntervalDto>,
    val totalPauseSeconds: Double,
    val trueInGameDurationSeconds: Double
)

data class ParticipantCheckpointSnapshotDto(
    val participantId: Int,
    val creepScore: Int,
    val totalGold: Int,
    val level: Int
)

data class ClockAnchoredSnapshotDto(
    val frameTimestampIso: String,
    val sampleQuantizationErrorSeconds: Double,
    val clockBiasVsTrueInGameSeconds: Double,
    val participants: Map<Int, ParticipantCheckpointSnapshotDto>
)

data class CheckpointComparisonDto(
    val markMinutes: Int,
    val naiveFirstFrame: ClockAnchoredSnapshotDto,
    val spawnUnadjusted: ClockAnchoredSnapshotDto,
    val trueInGameClock: ClockAnchoredSnapshotDto
)

data class ItemCompletionTimingDto(
    val itemId: Int,
    val itemName: String,
    val isBoots: Boolean,
    val isCompletedLegendary: Boolean,
    val elapsedFromFirstFrameSeconds: Double,
    val elapsedFromSpawnSeconds: Double,
    val inGameClockSeconds: Double
)

data class ProPlayerObservationDto(
    val gameId: String,
    val summonerName: String,
    val role: String,
    val championId: String,
    val csAt10: Int,
    val goldAt10: Int,
    val levelAt10: Int,
    val naiveCsAt10: Int,
    val naiveGoldAt10: Int,
    val csAt15: Int?,
    val goldAt15: Int?,
    val levelAt15: Int?,
    val bootsInGameClockSeconds: Double?,
    val firstItemInGameClockSeconds: Double?,
    val secondItemInGameClockSeconds: Double?,
    val skillOrderFirst6: List<String>
)

data class RoleChampionBaselineDto(
    val role: String,
    val championId: String,
    val sampleCount: Int,
    val players: List<String>,
    val avgCsAt10: Double,
    val avgGoldAt10: Double,
    val avgLevelAt10: Double,
    val avgNaiveCsAt10: Double,
    val avgNaiveGoldAt10: Double,
    val avgCsAt15: Double?,
    val avgGoldAt15: Double?,
    val avgLevelAt15: Double?,
    val avgBootsCompletionSeconds: Double?,
    val avgFirstItemCompletionSeconds: Double?,
    val avgSecondItemCompletionSeconds: Double?
)
