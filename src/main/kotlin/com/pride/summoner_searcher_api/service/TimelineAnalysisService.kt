package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * Analyzes a collection of match timelines for a specific player (identified by PUUID),
 * focusing on them and their direct lane opponent.
 *
 * Produces heatmap positions, power spike timelines, build orders, skill orders,
 * per-match event timelines, and an aggregate summary.
 */
@Service
class TimelineAnalysisService {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Riot map coordinates range 0..15000 for both X and Y.
    private val MAP_SIZE = 15000.0

    /**
     * Entry point: given timelines + the base match info (to get champion names, participant IDs, and win/loss),
     * produce the full [TimelineAnalysisDto].
     *
     * @param timelines List of (matchId, timeline, matchInfo) triples
     * @param puuid     The searched player's PUUID
     */
    fun analyze(
        timelines: List<Triple<String, MatchTimelineDto, MatchDto>>,
        puuid: String
    ): TimelineAnalysisDto {
        val heatmap = mutableListOf<HeatmapPoint>()
        val deaths = mutableListOf<HeatmapPoint>()
        val wards = mutableListOf<HeatmapPoint>()
        val kills = mutableListOf<HeatmapPoint>()
        val powerSpikes = mutableListOf<MatchPowerSpikeDto>()
        val buildOrders = mutableListOf<BuildOrderDto>()
        val skillOrders = mutableListOf<SkillOrderDto>()
        val eventTimelines = mutableListOf<MatchEventTimelineDto>()

        var totalFirstDeathMinute = 0.0
        var gamesWithDeath = 0
        var totalGoldLeadAt10 = 0.0
        var totalGoldLeadAt15 = 0.0
        var gamesWithGoldData = 0
        var totalWards = 0
        var totalDeaths = 0
        var totalKills = 0

        for ((matchId, timeline, matchDto) in timelines) {
            val info = timeline.info ?: continue
            val matchInfo = matchDto.info ?: continue

            // Resolve the tracked player's participantId within the timeline
            val playerParticipantId = info.participants?.find { it.puuid == puuid }?.participantId ?: continue

            // Find the match participant (for champion name, teamId, win)
            val playerMatchParticipant = matchInfo.participants.find { it.puuid == puuid } ?: continue
            val playerChampion = playerMatchParticipant.championName ?: "Unknown"
            val playerTeamId = playerMatchParticipant.teamId ?: 100
            val playerWin = playerMatchParticipant.win ?: false

            // Find lane opponent
            val opponentMatchParticipant = matchInfo.participants.find { p ->
                p.teamId != playerTeamId &&
                p.teamPosition == playerMatchParticipant.teamPosition &&
                !playerMatchParticipant.teamPosition.isNullOrBlank() &&
                playerMatchParticipant.teamPosition != "NONE"
            }
            val opponentChampion = opponentMatchParticipant?.championName ?: "Unknown"
            val opponentParticipantId = info.participants?.find { it.puuid == opponentMatchParticipant?.puuid }?.participantId

            val frames = info.frames ?: continue

            // ── Heatmap: collect player positions per frame ──
            for (frame in frames) {
                val playerFrame = frame.participantFrames?.get(playerParticipantId.toString()) ?: continue
                val pos = playerFrame.position ?: continue
                if (pos.x != null && pos.y != null) {
                    heatmap.add(HeatmapPoint(pos.x, pos.y, matchId))
                }
            }

            // ── Power spike timeline: minute-by-minute gold comparison ──
            val powerSpikePoints = buildPowerSpikePoints(frames, playerParticipantId, opponentParticipantId, matchId)
            powerSpikes.add(MatchPowerSpikeDto(matchId, playerChampion, opponentChampion, playerWin, powerSpikePoints))

            // Track gold lead at 10/15
            val minuteFrame10 = frames.getOrNull(10)
            val minuteFrame15 = frames.getOrNull(15)
            if (minuteFrame10 != null && opponentParticipantId != null) {
                val pg10 = minuteFrame10.participantFrames?.get(playerParticipantId.toString())?.totalGold ?: 0
                val og10 = minuteFrame10.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
                totalGoldLeadAt10 += (pg10 - og10)
                gamesWithGoldData++
            }
            if (minuteFrame15 != null && opponentParticipantId != null) {
                val pg15 = minuteFrame15.participantFrames?.get(playerParticipantId.toString())?.totalGold ?: 0
                val og15 = minuteFrame15.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
                totalGoldLeadAt15 += (pg15 - og15)
            }

            // ── Events: build all structures in one pass ──
            val allEvents = frames.flatMap { it.events ?: emptyList() }
            val buyItems = mutableListOf<BuildItem>()
            val skillLevelUps = mutableListOf<SkillUp>()
            val matchEvents = mutableListOf<MatchEventEntry>()

            for (event in allEvents) {
                val minute = ((event.timestamp ?: 0) / 60000).toInt()
                val second = (((event.timestamp ?: 0) / 1000) % 60).toInt()

                when (event.type) {
                    "ITEM_PURCHASED" -> {
                        if (event.participantId == playerParticipantId && event.itemId != null) {
                            buyItems.add(BuildItem(event.itemId, event.timestamp ?: 0, minute))
                        }
                    }
                    "SKILL_LEVEL_UP" -> {
                        if (event.participantId == playerParticipantId && event.skillSlot != null && event.level != null) {
                            skillLevelUps.add(SkillUp(event.skillSlot, event.level, minute))
                        }
                    }
                    "CHAMPION_KILL" -> {
                        val isPlayerKiller = event.killerId == playerParticipantId
                        val isPlayerVictim = event.victimId == playerParticipantId
                        val isPlayerAssist = event.assistingParticipantIds?.contains(playerParticipantId) == true

                        val isOpponentKiller = opponentParticipantId != null && event.killerId == opponentParticipantId
                        val isOpponentVictim = opponentParticipantId != null && event.victimId == opponentParticipantId

                        // Only include events where player or opponent is directly involved
                        if (isPlayerKiller || isPlayerVictim || isPlayerAssist || isOpponentKiller || isOpponentVictim) {
                            val eventType = when {
                                isPlayerKiller -> "KILL"
                                isPlayerVictim -> "DEATH"
                                isPlayerAssist -> "ASSIST"
                                isOpponentKiller -> "KILL"
                                isOpponentVictim -> "DEATH"
                                else -> "KILL"
                            }

                            matchEvents.add(MatchEventEntry(
                                type = eventType,
                                minuteMark = minute,
                                secondMark = second,
                                actor = if (isPlayerKiller || isOpponentKiller) {
                                    if (isPlayerKiller) playerChampion else opponentChampion
                                } else null,
                                target = when {
                                    isPlayerVictim -> playerChampion
                                    isOpponentVictim -> opponentChampion
                                    else -> null
                                },
                                isPlayer = isPlayerKiller || isPlayerVictim || isPlayerAssist,
                                isOpponent = isOpponentKiller || isOpponentVictim,
                                laneType = null,
                                monsterType = null,
                                position = event.position
                            ))

                            if (isPlayerKiller) {
                                kills.add(HeatmapPoint(event.position?.x ?: 0, event.position?.y ?: 0, matchId))
                                totalKills++
                            }
                            if (isPlayerVictim) {
                                deaths.add(HeatmapPoint(event.position?.x ?: 0, event.position?.y ?: 0, matchId))
                                totalDeaths++
                                if (gamesWithDeath == 0 || minute < Int.MAX_VALUE) {
                                    // We'll compute first death below
                                }
                            }
                        }
                    }
                    "BUILDING_KILL" -> {
                        // Include turret plate kills that involve player's team or opponent team
                        val involvedTeamId = event.teamId
                        val playerTeamTook = involvedTeamId != null && involvedTeamId == playerTeamId
                        val opponentTeamTook = involvedTeamId != null && involvedTeamId != playerTeamId

                        // Only include the plate if player or opponent is the killer or assist
                        val playerInvolved = event.killerId == playerParticipantId ||
                                event.assistingParticipantIds?.contains(playerParticipantId) == true
                        val opponentInvolved = opponentParticipantId != null && (
                                event.killerId == opponentParticipantId ||
                                event.assistingParticipantIds?.contains(opponentParticipantId) == true)

                        if (playerInvolved || opponentInvolved) {
                            matchEvents.add(MatchEventEntry(
                                type = "PLATE",
                                minuteMark = minute,
                                secondMark = second,
                                actor = when {
                                    playerInvolved -> playerChampion
                                    opponentInvolved -> opponentChampion
                                    else -> null
                                },
                                target = event.laneType?.replace("_LANE", "") ?: "UNKNOWN",
                                isPlayer = playerInvolved,
                                isOpponent = opponentInvolved,
                                laneType = event.laneType,
                                monsterType = null,
                                position = event.position
                            ))
                        }
                    }
                    "ELITE_MONSTER_KILL" -> {
                        val playerInvolved = event.killerId == playerParticipantId ||
                                event.assistingParticipantIds?.contains(playerParticipantId) == true
                        val opponentInvolved = opponentParticipantId != null && (
                                event.killerId == opponentParticipantId ||
                                event.assistingParticipantIds?.contains(opponentParticipantId) == true)

                        if (playerInvolved || opponentInvolved) {
                            matchEvents.add(MatchEventEntry(
                                type = "OBJECTIVE",
                                minuteMark = minute,
                                secondMark = second,
                                actor = when {
                                    playerInvolved -> playerChampion
                                    else -> opponentChampion
                                },
                                target = event.monsterType ?: "OBJECTIVE",
                                isPlayer = playerInvolved,
                                isOpponent = opponentInvolved,
                                laneType = null,
                                monsterType = event.monsterType,
                                position = event.position
                            ))
                        }
                    }
                    "WARD_PLACED" -> {
                        if (event.participantId == playerParticipantId) {
                            val pos = event.position
                            if (pos?.x != null && pos.y != null) {
                                wards.add(HeatmapPoint(pos.x, pos.y, matchId))
                                totalWards++
                            }
                        }
                    }
                }
            }

            // Compute first death minute for summary
            val firstDeath = allEvents.filter { it.type == "CHAMPION_KILL" && it.victimId == playerParticipantId }
                .minByOrNull { it.timestamp ?: Long.MAX_VALUE }
            if (firstDeath != null) {
                totalFirstDeathMinute += ((firstDeath.timestamp ?: 0) / 60000.0)
                gamesWithDeath++
            }

            buildOrders.add(BuildOrderDto(matchId, playerChampion, playerWin, buyItems))

            // Determine max order (first time each slot is leveled up)
            val maxOrder = listOf(1, 2, 3).sortedBy { slot ->
                skillLevelUps.filter { it.skillSlot == slot && it.skillSlot != 4 }.count().let { count ->
                    -count // higher count = maxed first
                }
            }
            skillOrders.add(SkillOrderDto(matchId, playerChampion, playerWin, maxOrder, skillLevelUps))

            eventTimelines.add(MatchEventTimelineDto(matchId, playerChampion, opponentChampion, playerWin, matchEvents.sortedBy { it.minuteMark * 60 + it.secondMark }))
        }

        // ── Aggregate summary ──
        val deathZone = computeMostDangerousZone(deaths)
        val avgFirstDeath = if (gamesWithDeath > 0) totalFirstDeathMinute / gamesWithDeath else 0.0
        val avgGold10 = if (gamesWithGoldData > 0) totalGoldLeadAt10 / gamesWithGoldData else 0.0
        val avgGold15 = if (gamesWithGoldData > 0) totalGoldLeadAt15 / gamesWithGoldData else 0.0

        return TimelineAnalysisDto(
            heatmapPositions = heatmap,
            deathPositions = deaths,
            wardPositions = wards,
            killPositions = kills,
            powerSpikeTimelines = powerSpikes,
            buildOrders = buildOrders,
            skillOrders = skillOrders,
            eventTimelines = eventTimelines,
            aggregateSummary = TimelineAggregateSummary(
                avgFirstDeathMinute = (avgFirstDeath * 10.0).roundToInt() / 10.0,
                mostDangerousZone = deathZone,
                avgGoldLeadAt10 = (avgGold10 * 10.0).roundToInt() / 10.0,
                avgGoldLeadAt15 = (avgGold15 * 10.0).roundToInt() / 10.0,
                wardsPlacedTotal = totalWards,
                deathsTotal = totalDeaths,
                killsTotal = totalKills,
                gamesAnalyzed = timelines.size
            )
        )
    }

    private fun buildPowerSpikePoints(
        frames: List<TimelineFrameDto>,
        playerParticipantId: Int,
        opponentParticipantId: Int?,
        matchId: String
    ): List<PowerSpikePoint> {
        val playerItems = mutableListOf<Int>()
        val opponentItems = mutableListOf<Int>()
        // We collect items from ITEM_PURCHASED events grouped by minute
        val playerItemsByMinute = mutableMapOf<Int, MutableList<Int>>()
        val opponentItemsByMinute = mutableMapOf<Int, MutableList<Int>>()

        for (frame in frames) {
            val minute = ((frame.timestamp ?: 0) / 60000).toInt()
            frame.events?.forEach { event ->
                if (event.type == "ITEM_PURCHASED") {
                    if (event.participantId == playerParticipantId && event.itemId != null) {
                        playerItemsByMinute.getOrPut(minute) { mutableListOf() }.add(event.itemId)
                    }
                    if (opponentParticipantId != null && event.participantId == opponentParticipantId && event.itemId != null) {
                        opponentItemsByMinute.getOrPut(minute) { mutableListOf() }.add(event.itemId)
                    }
                }
            }
        }

        val points = mutableListOf<PowerSpikePoint>()
        for (frame in frames) {
            val minute = ((frame.timestamp ?: 0) / 60000).toInt()
            val playerFrame = frame.participantFrames?.get(playerParticipantId.toString()) ?: continue
            val pg = playerFrame.totalGold ?: 0
            val og = if (opponentParticipantId != null)
                frame.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
            else 0

            // Accumulate items up to this minute
            playerItemsByMinute[minute]?.let { playerItems.addAll(it) }
            opponentItemsByMinute[minute]?.let { opponentItems.addAll(it) }

            // Only emit one point per minute (skip duplicate timestamps)
            if (points.none { it.minute == minute }) {
                points.add(PowerSpikePoint(
                    minute = minute,
                    playerGold = pg,
                    opponentGold = og,
                    playerGoldLead = pg - og,
                    playerItems = playerItems.toList(),
                    opponentItems = opponentItems.toList()
                ))
            }
        }
        return points
    }

    /**
     * Determines which map quadrant (named) had the most deaths.
     * Map is divided into 4 quadrants based on the 15000x15000 coordinate space.
     */
    private fun computeMostDangerousZone(deathPositions: List<HeatmapPoint>): String {
        if (deathPositions.isEmpty()) return "Unknown"

        data class Zone(val name: String, val count: Int)

        val half = MAP_SIZE / 2
        val zones = mapOf(
            "Blue Side Jungle" to deathPositions.count { it.x < half && it.y < half },
            "Red Side Jungle" to deathPositions.count { it.x >= half && it.y >= half },
            "Top Side River" to deathPositions.count { it.x < half && it.y >= half },
            "Bot Side River" to deathPositions.count { it.x >= half && it.y < half }
        )
        return zones.maxByOrNull { it.value }?.key ?: "Unknown"
    }
}
