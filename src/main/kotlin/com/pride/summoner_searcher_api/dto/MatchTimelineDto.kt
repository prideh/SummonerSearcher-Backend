package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Top-level timeline response from GET /lol/match/v5/matches/{matchId}/timeline
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchTimelineDto(
    val metadata: MatchTimelineMetadata?,
    val info: MatchTimelineInfo?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchTimelineMetadata(
    val matchId: String?,
    val participants: List<String>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatchTimelineInfo(
    val frameInterval: Long?,
    val frames: List<TimelineFrameDto>?,
    val participants: List<TimelineParticipantDto>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TimelineParticipantDto(
    val participantId: Int?,
    val puuid: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TimelineFrameDto(
    val timestamp: Long?,
    val participantFrames: Map<String, ParticipantFrameDto>?,
    val events: List<TimelineEventDto>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ParticipantFrameDto(
    val participantId: Int?,
    val totalGold: Int?,
    val level: Int?,
    val xp: Int?,
    val currentGold: Int?,
    val minionsKilled: Int?,
    val jungleMinionsKilled: Int?,
    val position: TimelinePositionDto?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class TimelinePositionDto(
    val x: Int?,
    val y: Int?
)

/**
 * A single in-game event from the timeline.
 * Types of interest: CHAMPION_KILL, ITEM_PURCHASED, ITEM_DESTROYED, ITEM_SOLD,
 * SKILL_LEVEL_UP, WARD_PLACED, WARD_KILL, BUILDING_KILL, ELITE_MONSTER_KILL, LEVEL_UP
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class TimelineEventDto(
    val type: String?,
    val timestamp: Long?,
    val participantId: Int?,
    val killerId: Int?,
    val victimId: Int?,
    val assistingParticipantIds: List<Int>?,
    val itemId: Int?,
    val afterId: Int?,   // item after transformation/upgrade
    val beforeId: Int?,  // item before transformation
    val skillSlot: Int?,
    val levelUpType: String?,
    val wardType: String?,
    val level: Int?,
    val buildingType: String?,
    val laneType: String?,
    val towerType: String?,
    val teamId: Int?,
    val monsterType: String?,
    val monsterSubType: String?,
    val position: TimelinePositionDto?,
    val bounty: Int?,
    val shutdownBounty: Int?,
    val goldGain: Int?
)
