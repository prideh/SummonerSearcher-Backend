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

    companion object {
        private val RELEVANT_TIMELINE_EVENT_TYPES = setOf(
            "ITEM_PURCHASED",
            "ITEM_DESTROYED",
            "ITEM_UNDO",
            "ITEM_SOLD",
            "LEVEL_UP",
            "SKILL_LEVEL_UP",
            "CHAMPION_KILL",
            "BUILDING_KILL",
            "TURRET_PLATE_DESTROYED",
            "ELITE_MONSTER_KILL",
            "WARD_PLACED",
            "WARD_KILL"
        )

        private val ROLE_QUEST_QUEUE_IDS = setOf(400, 420, 440, 700)
        private val STARTER_ROLE_QUEST_IDS = setOf(1200, 1201, 1202, 1203, 1204, 1222)
        private val COMPLETED_ROLE_QUEST_REWARD_IDS = setOf(1205, 1206, 1207, 1208, 1209, 1210, 1211, 1220, 1221)
        private val VERIFIED_BOT_QUEST_BOOT_IDS = setOf(
            1001, 2422,
            3006, 3009, 3010, 3013, 3020, 3047, 3111, 3117, 3158,
            3170, 3171, 3172, 3173, 3174, 3175, 3176
        )

        internal fun isUndefinedWardType(wardType: String?): Boolean =
            wardType != null && wardType.trim().equals("UNDEFINED", ignoreCase = true)
    }

    /**
     * Strips a single raw [MatchTimelineDto] down to ONLY the participant frame entries and timeline events
     * read by [analyze] and [computeImprovementStats] for [puuid] and their assigned/lane opponent,
     * copying into compact collections so the full 10-player raw frame/event graph can be garbage-collected
     * immediately before the next match is decoded/fetched.
     */
    fun compactTimelineForPlayer(
        timeline: MatchTimelineDto,
        matchDto: MatchDto,
        puuid: String
    ): MatchTimelineDto? {
        val info = timeline.info ?: return null
        val rawFrames = info.frames?.takeIf { it.isNotEmpty() } ?: return null
        val matchInfo = matchDto.info ?: return null
        val playerMatchParticipant = matchInfo.participants.find { it.puuid == puuid } ?: return null

        val analyzePlayerPid = (info.participants?.find { it.puuid == puuid }?.participantId
            ?: playerMatchParticipant.participantId)?.takeIf { it in 1..10 }
        val improvementPlayerPid = resolveTimelineParticipantId(info, playerMatchParticipant, matchInfo.participants)
            ?.takeIf { it in 1..10 }
        if (analyzePlayerPid == null && improvementPlayerPid == null) return null

        val playerFrameKeys = setOfNotNull(analyzePlayerPid?.toString(), improvementPlayerPid?.toString())
        val hasAnyPlayerFrame = rawFrames.any { frame ->
            val pFrames = frame.participantFrames ?: return@any false
            playerFrameKeys.any { key -> pFrames.containsKey(key) }
        }
        if (!hasAnyPlayerFrame) return null

        val playerTeamId = playerMatchParticipant.teamId ?: 100
        val analyzeOpponent = matchInfo.participants.find { p ->
            p.teamId != playerTeamId &&
                p.teamPosition == playerMatchParticipant.teamPosition &&
                !playerMatchParticipant.teamPosition.isNullOrBlank() &&
                playerMatchParticipant.teamPosition != "NONE"
        }
        val analyzeOpponentPid = (info.participants?.find { it.puuid == analyzeOpponent?.puuid }?.participantId
            ?: analyzeOpponent?.participantId)?.takeIf { it in 1..10 }

        val playerRole = normalizeRole(playerMatchParticipant.teamPosition ?: playerMatchParticipant.individualPosition)
        val improvementOpponent = matchInfo.participants.find { p ->
            p.teamId != playerTeamId &&
                normalizeRole(p.teamPosition ?: p.individualPosition) == playerRole &&
                playerRole != "UNKNOWN"
        }
        val improvementOpponentPid = resolveTimelineParticipantId(info, improvementOpponent, matchInfo.participants)
            ?.takeIf { it in 1..10 }

        val keptParticipantIds = setOfNotNull(
            analyzePlayerPid,
            improvementPlayerPid,
            analyzeOpponentPid,
            improvementOpponentPid
        )
        if (keptParticipantIds.isEmpty()) return null
        val keptFrameKeys = keptParticipantIds.map { it.toString() }.toSet()

        val compactedFrames = rawFrames.map { frame ->
            val filteredParticipantFrames = frame.participantFrames
                ?.entries
                ?.filter { (key, _) -> key in keptFrameKeys }
                ?.associate { (key, value) -> key to value }
            val filteredEvents = frame.events
                ?.filter { ev ->
                    val type = ev.type ?: return@filter false
                    if (type !in RELEVANT_TIMELINE_EVENT_TYPES) return@filter false
                    when (type) {
                        "ITEM_PURCHASED", "ITEM_DESTROYED", "ITEM_UNDO", "ITEM_SOLD", "LEVEL_UP", "SKILL_LEVEL_UP" ->
                            ev.participantId != null && ev.participantId in keptParticipantIds
                        "WARD_PLACED" ->
                            !isUndefinedWardType(ev.wardType) && (
                                (ev.creatorId != null && ev.creatorId in keptParticipantIds) ||
                                    (ev.participantId != null && ev.participantId in keptParticipantIds)
                            )
                        "WARD_KILL" ->
                            (ev.killerId != null && ev.killerId in keptParticipantIds) ||
                                (ev.participantId != null && ev.participantId in keptParticipantIds)
                        "CHAMPION_KILL" ->
                            (ev.killerId != null && ev.killerId in keptParticipantIds) ||
                                (ev.victimId != null && ev.victimId in keptParticipantIds) ||
                                (ev.assistingParticipantIds?.any { it in keptParticipantIds } == true)
                        else -> true
                    }
                }
                ?.toList()
            TimelineFrameDto(
                timestamp = frame.timestamp,
                participantFrames = filteredParticipantFrames,
                events = filteredEvents
            )
        }.toList()

        return MatchTimelineDto(
            metadata = MatchTimelineMetadata(
                matchId = timeline.metadata?.matchId,
                participants = null
            ),
            info = MatchTimelineInfo(
                frameInterval = info.frameInterval,
                frames = compactedFrames,
                participants = info.participants?.filter { it.participantId in keptParticipantIds }
            )
        )
    }


    /**
     * Entry point: given timelines + the base match info (to get champion names, participant IDs, and win/loss),
     * produce the full [TimelineAnalysisDto].
     *
     * @param timelines List of (matchId, timeline, matchInfo) triples
     * @param puuid     The searched player's PUUID
     */
    @JvmOverloads
    fun analyze(
        timelines: List<Triple<String, MatchTimelineDto, MatchDto>>,
        puuid: String,
        allMatches: List<MatchDto>? = null
    ): TimelineAnalysisDto {
        val heatmap = mutableListOf<HeatmapPoint>()
        val deaths = mutableListOf<HeatmapPoint>()
        val wards = mutableListOf<HeatmapPoint>()
        val kills = mutableListOf<HeatmapPoint>()
        val assists = mutableListOf<HeatmapPoint>()
        val powerSpikes = mutableListOf<MatchPowerSpikeDto>()
        val buildOrders = mutableListOf<BuildOrderDto>()
        val skillOrders = mutableListOf<SkillOrderDto>()
        val eventTimelines = mutableListOf<MatchEventTimelineDto>()
        val roleQuestTimings = mutableListOf<MatchRoleQuestTimingDto>()

        var totalFirstDeathMinute = 0.0
        var gamesWithDeath = 0
        var totalGoldLeadAt10 = 0.0
        var totalGoldLeadAt15 = 0.0
        var gamesWithGold10 = 0
        var gamesWithGold15 = 0
        var totalWards = 0
        var wardsPre10 = 0
        var wardsMin10To20 = 0
        var wardsPost20 = 0
        var totalDeaths = 0
        var totalDeathsBefore10 = 0
        var totalAssistedDeaths = 0
        var totalUnassistedDeaths = 0
        var totalDeathsNearObjectives = 0
        var totalKills = 0
        var totalAssists = 0

        var roleQuestEligibleGames = 0
        var roleQuestCompletedGames = 0
        var roleQuestAmbiguousGames = 0
        var roleQuestOpponentAmbiguousOnCompleted = 0
        var sumRoleQuestCompletionMinute = 0.0
        var sumRoleQuestDeltaSeconds = 0.0
        var roleQuestDeltaCount = 0

        for ((matchId, timeline, matchDto) in timelines) {
            val info = timeline.info ?: continue
            val matchInfo = matchDto.info ?: continue

            // Resolve the tracked player's participantId within the timeline
            val playerMatchParticipant = matchInfo.participants.find { it.puuid == puuid } ?: continue
            val playerParticipantId = info.participants?.find { it.puuid == puuid }?.participantId
                ?: playerMatchParticipant.participantId
                ?: continue

            val playerChampion = playerMatchParticipant.championName ?: "Unknown"
            val playerTeamId = playerMatchParticipant.teamId ?: 100
            val playerWin = playerMatchParticipant.win ?: false
            val normalizedPlayerRole = normalizeRole(playerMatchParticipant.teamPosition ?: playerMatchParticipant.individualPosition)

            // Find lane opponent
            val opponentMatchParticipant = matchInfo.participants.find { p ->
                p.teamId != playerTeamId &&
                p.teamPosition == playerMatchParticipant.teamPosition &&
                !playerMatchParticipant.teamPosition.isNullOrBlank() &&
                playerMatchParticipant.teamPosition != "NONE"
            }
            val opponentChampion = opponentMatchParticipant?.championName ?: "Unknown"
            val opponentParticipantId = info.participants?.find { it.puuid == opponentMatchParticipant?.puuid }?.participantId
                ?: opponentMatchParticipant?.participantId
            
            // Map of participantId -> Champion name for all 10 players (with index and PUUID fallbacks)
            val puuidToChampion = matchInfo.participants.mapNotNull { p ->
                p.puuid?.let { it to (p.championName ?: "Unknown") }
            }.toMap()
            val participantToChampion = mutableMapOf<Int, String>()
            matchInfo.participants.forEachIndexed { idx, p ->
                val pid = p.participantId ?: (idx + 1)
                participantToChampion[pid] = p.championName ?: "Unknown"
            }
            info.participants?.forEach { tp ->
                val pid = tp.participantId
                val champ = tp.puuid?.let { puuidToChampion[it] }
                if (pid != null && champ != null && !participantToChampion.containsKey(pid)) {
                    participantToChampion[pid] = champ
                }
            }

            val frames = info.frames ?: continue

            // ── Heatmap: collect player positions per frame ──
            for (frame in frames) {
                val playerFrame = frame.participantFrames?.get(playerParticipantId.toString()) ?: continue
                val pos = playerFrame.position ?: continue
                if (pos.x != null && pos.y != null) {
                    heatmap.add(HeatmapPoint(pos.x, pos.y, matchId, ((frame.timestamp ?: 0) / 60000).toInt()))
                }
            }

            // Track gold lead at 10/15 using actual frame timestamps (falling back to index only if all timestamps are null)
            val hasTimestampedFrames = frames.any { it.timestamp != null }
            val minuteFrame10 = if (hasTimestampedFrames) findCheckpointFrame(frames, 10) else frames.getOrNull(10)
            val minuteFrame15 = if (hasTimestampedFrames) findCheckpointFrame(frames, 15) else frames.getOrNull(15)
            if (minuteFrame10 != null && opponentParticipantId != null) {
                val pg10 = minuteFrame10.participantFrames?.get(playerParticipantId.toString())?.totalGold ?: 0
                val og10 = minuteFrame10.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
                totalGoldLeadAt10 += (pg10 - og10)
                gamesWithGold10++
            }
            if (minuteFrame15 != null && opponentParticipantId != null) {
                val pg15 = minuteFrame15.participantFrames?.get(playerParticipantId.toString())?.totalGold ?: 0
                val og15 = minuteFrame15.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
                totalGoldLeadAt15 += (pg15 - og15)
                gamesWithGold15++
            }

            // ── Events: build all structures in one pass ──
            val allEvents = frames.flatMap { it.events ?: emptyList() }
            val objectiveMinutesInMatch = allEvents
                .filter { it.type == "ELITE_MONSTER_KILL" }
                .map { (it.timestamp ?: 0L) / 60000.0 }
            val buyItems = mutableListOf<BuildItem>()
            val skillLevelUps = mutableListOf<SkillUp>()
            val seenSkillSlotTimestampPairs = HashSet<Pair<Int, Long>>()
            val matchEvents = mutableListOf<MatchEventEntry>()
            var playerCurrentLevel = 1

            for (event in allEvents) {
                val ts = event.timestamp ?: 0L
                val minute = (ts / 60000L).toInt()
                val second = ((ts / 1000L) % 60L).toInt()

                when (event.type) {
                    "ITEM_PURCHASED" -> {
                        if (event.participantId == playerParticipantId && event.itemId != null && event.itemId > 0) {
                            buyItems.add(BuildItem(event.itemId, ts, minute))
                        }
                    }
                    "ITEM_UNDO" -> {
                        val undoneItemId = event.beforeId
                        // Reconcile shop undo when beforeId > 0; safely ignore #1174 Tier 3 boot undo where beforeId == 0
                        if (event.participantId == playerParticipantId && undoneItemId != null && undoneItemId > 0) {
                            val lastIdx = buyItems.indexOfLast { it.itemId == undoneItemId }
                            if (lastIdx >= 0) {
                                buyItems.removeAt(lastIdx)
                            }
                        }
                    }
                    "LEVEL_UP" -> {
                        if (event.participantId == playerParticipantId && event.level != null) {
                            playerCurrentLevel = event.level
                        }
                    }
                    "SKILL_LEVEL_UP" -> {
                        // Accept NORMAL (or legacy null) skill-ups only (excluding EVOLVE) and deduplicate
                        // identical (skillSlot, timestamp) bursts (#1100) without imposing an 18-point hard cap
                        // that would truncate Udyr, transformation champions, or Season 2026 Top Quest Lv19-20.
                        val isNormalLevelUp = event.levelUpType == null || event.levelUpType.equals("NORMAL", ignoreCase = true)
                        if (event.participantId == playerParticipantId && event.skillSlot != null && isNormalLevelUp) {
                            val dedupKey = event.skillSlot to ts
                            if (event.timestamp == null || seenSkillSlotTimestampPairs.add(dedupKey)) {
                                skillLevelUps.add(SkillUp(event.skillSlot, playerCurrentLevel, minute))
                            }
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
                            val killerChamp = event.killerId?.let { participantToChampion[it] }
                            val assistingChamps = event.assistingParticipantIds
                                ?.map { participantToChampion[it] ?: "Participant $it" }
                                ?: emptyList()

                            matchEvents.add(MatchEventEntry(
                                type = eventType,
                                minuteMark = minute,
                                secondMark = second,
                                actor = killerChamp,
                                target = event.victimId?.let { participantToChampion[it] },
                                isPlayer = isPlayerKiller || isPlayerVictim || isPlayerAssist,
                                isOpponent = isOpponentKiller || isOpponentVictim,
                                laneType = null,
                                monsterType = null,
                                position = event.position,
                                killer = killerChamp,
                                assistants = assistingChamps
                            ))

                            if (isPlayerKiller) {
                                kills.add(HeatmapPoint(event.position?.x ?: 0, event.position?.y ?: 0, matchId, minute))
                                totalKills++
                            }
                            if (isPlayerVictim) {
                                deaths.add(HeatmapPoint(event.position?.x ?: 0, event.position?.y ?: 0, matchId, minute))
                                totalDeaths++
                                if (minute < 10) {
                                    totalDeathsBefore10++
                                }
                                if (assistingChamps.isNotEmpty()) {
                                    totalAssistedDeaths++
                                } else {
                                    totalUnassistedDeaths++
                                }
                                val exactDeathMin = minute + second / 60.0
                                if (objectiveMinutesInMatch.any { objMin -> abs(objMin - exactDeathMin) <= 1.0 }) {
                                    totalDeathsNearObjectives++
                                }
                            }
                            if (isPlayerAssist) {
                                assists.add(HeatmapPoint(event.position?.x ?: 0, event.position?.y ?: 0, matchId, minute))
                                totalAssists++
                            }
                        }
                    }
                    "BUILDING_KILL", "TURRET_PLATE_DESTROYED" -> {
                        // Only include the plate if player or opponent is the killer or assist
                        val playerInvolved = event.killerId == playerParticipantId ||
                                event.assistingParticipantIds?.contains(playerParticipantId) == true
                        val opponentInvolved = opponentParticipantId != null && (
                                event.killerId == opponentParticipantId ||
                                event.assistingParticipantIds?.contains(opponentParticipantId) == true)

                        if (playerInvolved || opponentInvolved) {
                            val assistingChamps = event.assistingParticipantIds
                                ?.map { participantToChampion[it] ?: "Participant $it" }
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
                                position = event.position,
                                assistants = assistingChamps
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
                            val assistingChamps = event.assistingParticipantIds
                                ?.map { participantToChampion[it] ?: "Participant $it" }
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
                                position = event.position,
                                killer = event.killerId?.let { participantToChampion[it] },
                                assistants = assistingChamps
                            ))
                        }
                    }
                    "WARD_PLACED" -> {
                        // Exclude WARD_PLACED events with wardType == "UNDEFINED" (which include Season 2026
                        // Faelight vision interactions and non-ward entities/traps, without assuming all UNDEFINED are Faelights).
                        if (!isUndefinedWardType(event.wardType)) {
                            val isPlayerWard = event.creatorId == playerParticipantId || event.participantId == playerParticipantId
                            val isOpponentWard = opponentParticipantId != null &&
                                    (event.creatorId == opponentParticipantId || event.participantId == opponentParticipantId)

                            if (isPlayerWard || isOpponentWard) {
                                matchEvents.add(MatchEventEntry(
                                    type = "WARD_PLACED",
                                    minuteMark = minute,
                                    secondMark = second,
                                    actor = if (isPlayerWard) playerChampion else opponentChampion,
                                    target = event.wardType ?: "WARD",
                                    isPlayer = isPlayerWard,
                                    isOpponent = isOpponentWard,
                                    laneType = null,
                                    monsterType = null,
                                    position = null,
                                    wardType = event.wardType
                                ))
                            }

                            if (isPlayerWard) {
                                totalWards++
                                when {
                                    minute < 10 -> wardsPre10++
                                    minute < 20 -> wardsMin10To20++
                                    else -> wardsPost20++
                                }
                                val nearestFrame = findNearestFrame(frames, ts)
                                val pos = event.position ?: nearestFrame?.participantFrames?.get(playerParticipantId.toString())?.position
                                if (pos?.x != null && pos.y != null) {
                                    wards.add(HeatmapPoint(pos.x, pos.y, matchId, minute))
                                }
                            }
                        }
                    }
                    "WARD_KILL" -> {
                        val isPlayerKiller = event.killerId == playerParticipantId || event.participantId == playerParticipantId
                        val isOpponentKiller = opponentParticipantId != null &&
                                (event.killerId == opponentParticipantId || event.participantId == opponentParticipantId)

                        if (isPlayerKiller || isOpponentKiller) {
                            matchEvents.add(MatchEventEntry(
                                type = "WARD_KILLED",
                                minuteMark = minute,
                                secondMark = second,
                                actor = if (isPlayerKiller) playerChampion else opponentChampion,
                                target = event.wardType ?: "WARD",
                                isPlayer = isPlayerKiller,
                                isOpponent = isOpponentKiller,
                                laneType = null,
                                monsterType = null,
                                position = null,
                                killer = if (isPlayerKiller) playerChampion else opponentChampion,
                                wardType = event.wardType
                            ))
                        }
                    }
                }
            }

            // Compute conservative Season 2026 Role Quest timing (null whenever legacy/0/unknown/uncompleted/ambiguous)
            val questEval = evaluateMatchRoleQuestTiming(
                matchId = matchId,
                matchInfo = matchInfo,
                playerParticipant = playerMatchParticipant,
                playerParticipantId = playerParticipantId,
                playerRole = normalizedPlayerRole,
                opponentParticipant = opponentMatchParticipant,
                opponentParticipantId = opponentParticipantId,
                allEvents = allEvents
            )
            roleQuestTimings.add(questEval.dto)
            if (questEval.isSeasonQuestEligible) {
                roleQuestEligibleGames++
                if (questEval.isPlayerAmbiguous) {
                    roleQuestAmbiguousGames++
                } else if (questEval.dto.playerQuestCompleted == true && questEval.dto.playerCompletionMinute != null) {
                    roleQuestCompletedGames++
                    sumRoleQuestCompletionMinute += questEval.dto.playerCompletionMinute
                    if (questEval.isOpponentAmbiguous) {
                        roleQuestOpponentAmbiguousOnCompleted++
                    } else if (questEval.dto.completionDeltaSeconds != null) {
                        sumRoleQuestDeltaSeconds += questEval.dto.completionDeltaSeconds
                        roleQuestDeltaCount++
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

            // ── Power spike timeline: minute-by-minute stats comparison ──
            val powerSpikePoints = buildPowerSpikePoints(frames, playerParticipantId, opponentParticipantId, matchId, matchEvents)
            powerSpikes.add(MatchPowerSpikeDto(matchId, playerChampion, opponentChampion, playerWin, powerSpikePoints))

            eventTimelines.add(MatchEventTimelineDto(matchId, playerChampion, opponentChampion, playerWin, matchEvents.sortedBy { it.minuteMark * 60 + it.secondMark }))
        }

        // ── Aggregate summary ──
        val deathZone = computeMostDangerousZone(deaths)
        val avgFirstDeath = if (gamesWithDeath > 0) totalFirstDeathMinute / gamesWithDeath else 0.0
        val avgGold10 = if (gamesWithGold10 > 0) totalGoldLeadAt10 / gamesWithGold10 else 0.0
        val avgGold15 = if (gamesWithGold15 > 0) totalGoldLeadAt15 / gamesWithGold15 else 0.0

        // Never compute whole-sample Role Quest timing averages when any eligible match in the sample is ambiguous/unknown
        val avgQuestMin = if (roleQuestEligibleGames > 0 && roleQuestAmbiguousGames == 0 && roleQuestCompletedGames > 0) {
            round1(sumRoleQuestCompletionMinute / roleQuestCompletedGames)
        } else null
        val avgQuestDeltaSec = if (
            roleQuestEligibleGames > 0 &&
            roleQuestAmbiguousGames == 0 &&
            roleQuestOpponentAmbiguousOnCompleted == 0 &&
            roleQuestDeltaCount > 0
        ) {
            round1(sumRoleQuestDeltaSeconds / roleQuestDeltaCount)
        } else null

        val improvementStats = computeImprovementStats(timelines, puuid, allMatches)

        return TimelineAnalysisDto(
            heatmapPositions = heatmap,
            deathPositions = deaths,
            wardPositions = wards,
            killPositions = kills,
            assistPositions = assists,
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
                assistsTotal = totalAssists,
                gamesAnalyzed = timelines.size,
                deathsBefore10 = totalDeathsBefore10,
                assistedDeaths = totalAssistedDeaths,
                unassistedDeaths = totalUnassistedDeaths,
                deathsNearObjectiveWindows = totalDeathsNearObjectives,
                wardsByTimeBucket = WardTimeBucketsDto(
                    pre10 = wardsPre10,
                    min10To20 = wardsMin10To20,
                    post20 = wardsPost20
                ),
                roleQuestEligibleGames = if (roleQuestEligibleGames > 0) roleQuestEligibleGames else null,
                roleQuestCompletedGames = if (roleQuestEligibleGames > 0 && roleQuestAmbiguousGames == 0) roleQuestCompletedGames else null,
                roleQuestAmbiguousGames = if (roleQuestEligibleGames > 0) roleQuestAmbiguousGames else null,
                avgRoleQuestCompletionMinute = avgQuestMin,
                avgRoleQuestDeltaSeconds = avgQuestDeltaSec
            ),
            improvementStats = improvementStats,
            roleQuestTimings = roleQuestTimings
        )
    }

    /**
     * Computes the nullable, documented [ImprovementStatsDto] contract across all supplied matches and timelines,
     * including overall aggregation and `byChampion` / `byRole` breakdowns.
     *
     * Can be invoked with both timelines and matches (from [analyze]), or with `timelines = emptyList()` and
     * `allMatches = recentMatches` (from profile caching, populating match-sourced `deathCost` and `teamObjectives`
     * while leaving timeline-sourced fields `null` with `excludedMissingTimelineGames = N`).
     */
    @JvmOverloads
    fun computeImprovementStats(
        timelines: List<Triple<String, MatchTimelineDto, MatchDto>>,
        puuid: String,
        allMatches: List<MatchDto>? = null
    ): ImprovementStatsDto {
        val timelineByMatchId = mutableMapOf<String, MatchTimelineDto>()
        val orderedMatches = mutableListOf<Pair<String, MatchDto>>()
        val seenMatchIds = mutableSetOf<String>()

        for ((matchId, timeline, matchDto) in timelines) {
            timelineByMatchId[matchId] = timeline
            if (seenMatchIds.add(matchId)) {
                orderedMatches.add(matchId to matchDto)
            }
        }
        allMatches?.forEachIndexed { idx, matchDto ->
            val mid = matchDto.metadata?.matchId ?: "match_$idx"
            if (seenMatchIds.add(mid)) {
                orderedMatches.add(mid to matchDto)
            }
        }

        val parsedGames = mutableListOf<ParsedGameInput>()
        var invalidDurationGames = 0

        for ((matchId, matchDto) in orderedMatches) {
            val matchInfo = matchDto.info
            val durationSec = matchInfo?.gameDuration ?: 0L
            if (matchInfo == null || durationSec <= 0L) {
                invalidDurationGames++
                continue
            }
            val player = matchInfo.participants.find { it.puuid == puuid } ?: continue
            val championName = player.championName?.takeIf { it.isNotBlank() } ?: "Unknown"
            val role = normalizeRole(player.teamPosition ?: player.individualPosition)
            val timeline = timelineByMatchId[matchId]
            val hasValidTimeline = timeline?.info?.frames?.isNotEmpty() == true

            parsedGames.add(
                ParsedGameInput(
                    matchId = matchId,
                    matchDto = matchDto,
                    matchInfo = matchInfo,
                    player = player,
                    championName = championName,
                    role = role,
                    gameDurationSeconds = durationSec,
                    timeline = if (hasValidTimeline) timeline else null
                )
            )
        }

        val overallSlice = buildImprovementSlice(
            key = "ALL",
            games = parsedGames,
            invalidDurationGames = invalidDurationGames
        )

        val byChampion = parsedGames
            .groupBy { it.championName }
            .entries
            .sortedByDescending { it.value.size }
            .associate { (champ, champGames) ->
                champ to buildImprovementSlice(
                    key = champ,
                    games = champGames,
                    invalidDurationGames = 0
                )
            }

        val byRole = parsedGames
            .groupBy { it.role }
            .entries
            .sortedByDescending { it.value.size }
            .associate { (roleKey, roleGames) ->
                roleKey to buildImprovementSlice(
                    key = roleKey,
                    games = roleGames,
                    invalidDurationGames = 0
                )
            }

        return ImprovementStatsDto(
            totalGamesConsidered = overallSlice.totalGamesConsidered,
            excludedInvalidDurationGames = overallSlice.excludedInvalidDurationGames,
            excludedMissingTimelineGames = overallSlice.excludedMissingTimelineGames,
            phaseFarming = overallSlice.phaseFarming,
            leadRetention = overallSlice.leadRetention,
            deathCost = overallSlice.deathCost,
            unspentGold = overallSlice.unspentGold,
            teamObjectives = overallSlice.teamObjectives,
            byChampion = byChampion,
            byRole = byRole
        )
    }

    private data class ParsedGameInput(
        val matchId: String,
        val matchDto: MatchDto,
        val matchInfo: MatchInfo,
        val player: ParticipantDto,
        val championName: String,
        val role: String,
        val gameDurationSeconds: Long,
        val timeline: MatchTimelineDto?
    )

    private fun normalizeRole(rawRole: String?): String {
        val upper = rawRole?.trim()?.uppercase() ?: return "UNKNOWN"
        return when (upper) {
            "TOP" -> "TOP"
            "JUNGLE" -> "JUNGLE"
            "MIDDLE", "MID" -> "MIDDLE"
            "BOTTOM", "BOT", "ADC" -> "BOTTOM"
            "UTILITY", "SUPPORT", "SUP" -> "UTILITY"
            "", "NONE", "INVALID" -> "UNKNOWN"
            else -> upper
        }
    }

    private fun round1(v: Double): Double = (v * 10.0).roundToInt() / 10.0
    private fun round2(v: Double): Double = (v * 100.0).roundToInt() / 100.0
    private fun round4(v: Double): Double = (v * 10000.0).roundToInt() / 10000.0

    /**
     * Finds the timeline frame representing [targetMinute] (in minutes) using actual frame timestamps.
     * Accepts a frame within +/- 30 seconds of `targetMinute * 60_000L` (or falls back to index if timestamps are exact).
     */
    private fun findCheckpointFrame(frames: List<TimelineFrameDto>, targetMinute: Int): TimelineFrameDto? {
        val targetMs = targetMinute * 60_000L
        return frames
            .filter { it.timestamp != null && abs(it.timestamp - targetMs) <= 30_000L }
            .minByOrNull { abs((it.timestamp ?: 0L) - targetMs) }
    }

    private fun findNearestFrame(frames: List<TimelineFrameDto>, timestampMs: Long): TimelineFrameDto? {
        val timestamped = frames.filter { it.timestamp != null }
        if (timestamped.isEmpty()) {
            val approxIndex = ((timestampMs + 30_000L) / 60_000L).toInt()
            return frames.getOrNull(approxIndex)
        }
        return timestamped.minByOrNull { abs((it.timestamp ?: 0L) - timestampMs) }
    }

    private fun reconcileParticipantPurchaseEvents(
        frames: List<TimelineFrameDto>,
        participantId: Int
    ): List<TimelineEventDto> {
        val kept = mutableListOf<TimelineEventDto>()
        for (frame in frames) {
            for (ev in frame.events.orEmpty()) {
                if (ev.participantId != participantId) continue
                when (ev.type) {
                    "ITEM_PURCHASED" -> {
                        if (ev.itemId != null && ev.itemId > 0 && ev.timestamp != null) {
                            kept.add(ev)
                        }
                    }
                    "ITEM_UNDO" -> {
                        val undoneId = ev.beforeId
                        if (undoneId != null && undoneId > 0) {
                            val lastIdx = kept.indexOfLast { it.itemId == undoneId }
                            if (lastIdx >= 0) {
                                kept.removeAt(lastIdx)
                            }
                        }
                    }
                }
            }
        }
        return kept.sortedBy { it.timestamp ?: 0L }
    }

    private data class SingleParticipantQuestEval(
        val questCompleted: Boolean?,
        val completionTimestampMs: Long?,
        val completionMinute: Double?,
        val isAmbiguous: Boolean
    )

    private data class RoleQuestMatchEvaluation(
        val dto: MatchRoleQuestTimingDto,
        val isSeasonQuestEligible: Boolean,
        val isPlayerAmbiguous: Boolean,
        val isOpponentAmbiguous: Boolean
    )

    private fun evaluateSingleParticipantRoleQuest(
        participant: ParticipantDto?,
        participantId: Int?,
        normalizedRole: String,
        allEvents: List<TimelineEventDto>
    ): SingleParticipantQuestEval {
        if (participant == null || participantId == null) {
            return SingleParticipantQuestEval(null, null, null, isAmbiguous = true)
        }
        val rbi = participant.roleBoundItem
            ?: return SingleParticipantQuestEval(null, null, null, isAmbiguous = true) // legacy unknown
        if (rbi <= 0 || rbi in STARTER_ROLE_QUEST_IDS) {
            return SingleParticipantQuestEval(false, null, null, isAmbiguous = false)
        }
        val isVerifiedCompleted = rbi in COMPLETED_ROLE_QUEST_REWARD_IDS ||
            (normalizedRole == "BOTTOM" && rbi in VERIFIED_BOT_QUEST_BOOT_IDS)
        if (!isVerifiedCompleted) {
            // Unseen/unknown positive item ID: preserve ID on DTO, never claim quest completion
            return SingleParticipantQuestEval(null, null, null, isAmbiguous = true)
        }

        val starterDestroys = allEvents.filter { ev ->
            ev.type == "ITEM_DESTROYED" &&
                ev.participantId == participantId &&
                ev.itemId in STARTER_ROLE_QUEST_IDS &&
                (ev.timestamp ?: 0L) > 60_000L
        }
        if (starterDestroys.size != 1) {
            return SingleParticipantQuestEval(true, null, null, isAmbiguous = true)
        }
        val destroyTs = starterDestroys.first().timestamp ?: return SingleParticipantQuestEval(true, null, null, isAmbiguous = true)
        val hasLaterStarterPurchase = allEvents.any { ev ->
            ev.type == "ITEM_PURCHASED" &&
                ev.participantId == participantId &&
                ev.itemId in STARTER_ROLE_QUEST_IDS &&
                (ev.timestamp ?: 0L) > destroyTs
        }
        if (hasLaterStarterPurchase) {
            return SingleParticipantQuestEval(true, null, null, isAmbiguous = true)
        }
        return SingleParticipantQuestEval(
            questCompleted = true,
            completionTimestampMs = destroyTs,
            completionMinute = round1(destroyTs / 60_000.0),
            isAmbiguous = false
        )
    }

    private fun evaluateMatchRoleQuestTiming(
        matchId: String,
        matchInfo: MatchInfo,
        playerParticipant: ParticipantDto,
        playerParticipantId: Int,
        playerRole: String,
        opponentParticipant: ParticipantDto?,
        opponentParticipantId: Int?,
        allEvents: List<TimelineEventDto>
    ): RoleQuestMatchEvaluation {
        val majorPatch = matchInfo.gameVersion?.trim()?.substringBefore('.')?.toIntOrNull()
        val isEligible = (majorPatch != null && majorPatch >= 16) &&
            (matchInfo.queueId == null || matchInfo.queueId in ROLE_QUEST_QUEUE_IDS) &&
            (matchInfo.gameDuration ?: 0L) >= 180L &&
            playerParticipant.gameEndedInEarlySurrender != true

        if (!isEligible) {
            return RoleQuestMatchEvaluation(
                dto = MatchRoleQuestTimingDto(
                    matchId = matchId,
                    role = playerRole,
                    playerRoleBoundItem = playerParticipant.roleBoundItem,
                    opponentRoleBoundItem = opponentParticipant?.roleBoundItem
                ),
                isSeasonQuestEligible = false,
                isPlayerAmbiguous = false,
                isOpponentAmbiguous = false
            )
        }

        val playerEval = evaluateSingleParticipantRoleQuest(playerParticipant, playerParticipantId, playerRole, allEvents)
        val oppRole = normalizeRole(opponentParticipant?.teamPosition ?: opponentParticipant?.individualPosition).let {
            if (it == "UNKNOWN") playerRole else it
        }
        val oppEval = if (opponentParticipant != null && opponentParticipantId != null) {
            evaluateSingleParticipantRoleQuest(opponentParticipant, opponentParticipantId, oppRole, allEvents)
        } else {
            SingleParticipantQuestEval(null, null, null, isAmbiguous = false)
        }

        val deltaSeconds = if (playerEval.completionTimestampMs != null && oppEval.completionTimestampMs != null) {
            round1((playerEval.completionTimestampMs - oppEval.completionTimestampMs) / 1000.0)
        } else null

        return RoleQuestMatchEvaluation(
            dto = MatchRoleQuestTimingDto(
                matchId = matchId,
                role = playerRole,
                playerRoleBoundItem = playerParticipant.roleBoundItem,
                opponentRoleBoundItem = opponentParticipant?.roleBoundItem,
                playerQuestCompleted = playerEval.questCompleted,
                opponentQuestCompleted = oppEval.questCompleted,
                playerCompletionTimestampMs = playerEval.completionTimestampMs,
                playerCompletionMinute = playerEval.completionMinute,
                opponentCompletionTimestampMs = oppEval.completionTimestampMs,
                opponentCompletionMinute = oppEval.completionMinute,
                completionDeltaSeconds = deltaSeconds
            ),
            isSeasonQuestEligible = true,
            isPlayerAmbiguous = playerEval.isAmbiguous,
            isOpponentAmbiguous = opponentParticipant != null && oppEval.isAmbiguous
        )
    }

    private fun buildImprovementSlice(
        key: String,
        games: List<ParsedGameInput>,
        invalidDurationGames: Int
    ): ImprovementStatsBreakdownDto {
        val totalGames = games.size
        val missingTimelineGames = games.count { it.timeline == null }

        val phaseFarming = computePhaseFarmingStats(games, missingTimelineGames)
        val leadRetention = computeLeadRetentionStats(games, missingTimelineGames)
        val deathCost = computeDeathCostStats(games, missingTimelineGames)
        val unspentGold = computeUnspentGoldStats(games, missingTimelineGames)
        val teamObjectives = computeTeamObjectivesStats(games)

        return ImprovementStatsBreakdownDto(
            key = key,
            totalGamesConsidered = totalGames,
            excludedInvalidDurationGames = invalidDurationGames,
            excludedMissingTimelineGames = missingTimelineGames,
            phaseFarming = phaseFarming,
            leadRetention = leadRetention,
            deathCost = deathCost,
            unspentGold = unspentGold,
            teamObjectives = teamObjectives
        )
    }

    private fun resolveTimelineParticipantId(
        timelineInfo: MatchTimelineInfo,
        participant: ParticipantDto?,
        matchParticipants: List<ParticipantDto>
    ): Int? {
        if (participant == null) return null
        val byPuuid = participant.puuid?.let { targetPuuid ->
            timelineInfo.participants?.find { it.puuid == targetPuuid }?.participantId
        }
        if (byPuuid != null) return byPuuid
        if (participant.participantId != null) return participant.participantId
        val idx = matchParticipants.indexOfFirst { it === participant || (it.puuid != null && it.puuid == participant.puuid) }
        return if (idx >= 0) idx + 1 else null
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Phase Farming (10-20m and 20m-end)
    // ─────────────────────────────────────────────────────────────────────────

    private fun computePhaseFarmingStats(
        games: List<ParsedGameInput>,
        missingTimelineGames: Int
    ): PhaseFarmingStatsDto {
        var eligible10To20 = 0
        var shortGames10To20 = 0
        var missingCounters10To20 = 0
        var sumLaneGained10To20 = 0.0
        var sumJungleGained10To20 = 0.0
        var sumWindowMin10To20 = 0.0
        var sumLaneRate10To20 = 0.0
        var sumJungleRate10To20 = 0.0
        var sumTotalRate10To20 = 0.0

        var eligible20ToEnd = 0
        var shortGames20ToEnd = 0
        var missingCounters20ToEnd = 0
        var sumLaneGained20ToEnd = 0.0
        var sumJungleGained20ToEnd = 0.0
        var sumWindowMin20ToEnd = 0.0
        var sumLaneRate20ToEnd = 0.0
        var sumJungleRate20ToEnd = 0.0
        var sumTotalRate20ToEnd = 0.0

        val perMatchList = mutableListOf<MatchPhaseFarmingDto>()

        for (game in games) {
            val tInfo = game.timeline?.info ?: continue
            val frames = tInfo.frames ?: continue
            val playerPid = resolveTimelineParticipantId(tInfo, game.player, game.matchInfo.participants)
            if (playerPid == null) {
                missingCounters10To20++
                missingCounters20ToEnd++
                continue
            }

            val frame10 = if (game.gameDurationSeconds >= 600L) findCheckpointFrame(frames, 10) else null
            val frame20 = if (game.gameDurationSeconds >= 1200L) findCheckpointFrame(frames, 20) else null
            val endFrame = frames.lastOrNull { it.timestamp != null && it.participantFrames?.get(playerPid.toString()) != null }

            val pf10 = frame10?.participantFrames?.get(playerPid.toString())
            val pf20 = frame20?.participantFrames?.get(playerPid.toString())
            val pfEnd = endFrame?.participantFrames?.get(playerPid.toString())

            // --- 10-20m window ---
            var reached10To20 = false
            var t10Ms: Long? = null
            var t20Ms: Long? = null
            var lane10: Int? = null
            var jungle10: Int? = null
            var lane20: Int? = null
            var jungle20: Int? = null
            var laneGain10To20: Int? = null
            var jungleGain10To20: Int? = null
            var laneRate10To20: Double? = null
            var jungleRate10To20: Double? = null
            var totalRate10To20: Double? = null

            if (game.gameDurationSeconds < 1200L || frame10?.timestamp == null || frame20?.timestamp == null || frame20.timestamp <= frame10.timestamp) {
                shortGames10To20++
            } else if (
                pf10?.minionsKilled == null || pf10.jungleMinionsKilled == null ||
                pf20?.minionsKilled == null || pf20.jungleMinionsKilled == null
            ) {
                missingCounters10To20++
            } else {
                reached10To20 = true
                eligible10To20++
                t10Ms = frame10.timestamp
                t20Ms = frame20.timestamp
                lane10 = pf10.minionsKilled
                jungle10 = pf10.jungleMinionsKilled
                lane20 = pf20.minionsKilled
                jungle20 = pf20.jungleMinionsKilled

                val windowMin = (t20Ms - t10Ms) / 60_000.0
                val lGain = lane20 - lane10
                val jGain = jungle20 - jungle10
                val lRate = lGain / windowMin
                val jRate = jGain / windowMin
                val totRate = (lGain + jGain) / windowMin

                laneGain10To20 = lGain
                jungleGain10To20 = jGain
                laneRate10To20 = round2(lRate)
                jungleRate10To20 = round2(jRate)
                totalRate10To20 = round2(totRate)

                sumLaneGained10To20 += lGain
                sumJungleGained10To20 += jGain
                sumWindowMin10To20 += windowMin
                sumLaneRate10To20 += lRate
                sumJungleRate10To20 += jRate
                sumTotalRate10To20 += totRate
            }

            // --- 20m-end window ---
            var reached20ToEnd = false
            var tEndMs: Long? = null
            var laneEnd: Int? = null
            var jungleEnd: Int? = null
            var laneGain20ToEnd: Int? = null
            var jungleGain20ToEnd: Int? = null
            var laneRate20ToEnd: Double? = null
            var jungleRate20ToEnd: Double? = null
            var totalRate20ToEnd: Double? = null

            if (
                game.gameDurationSeconds <= 1200L ||
                frame20?.timestamp == null ||
                endFrame?.timestamp == null ||
                endFrame.timestamp <= frame20.timestamp
            ) {
                shortGames20ToEnd++
            } else if (
                pf20?.minionsKilled == null || pf20.jungleMinionsKilled == null ||
                pfEnd?.minionsKilled == null || pfEnd.jungleMinionsKilled == null
            ) {
                missingCounters20ToEnd++
            } else {
                reached20ToEnd = true
                eligible20ToEnd++
                tEndMs = endFrame.timestamp
                laneEnd = pfEnd.minionsKilled
                jungleEnd = pfEnd.jungleMinionsKilled
                if (lane20 == null) lane20 = pf20.minionsKilled
                if (jungle20 == null) jungle20 = pf20.jungleMinionsKilled
                if (t20Ms == null) t20Ms = frame20.timestamp

                val windowMin = (tEndMs - frame20.timestamp) / 60_000.0
                val lGain = laneEnd - pf20.minionsKilled
                val jGain = jungleEnd - pf20.jungleMinionsKilled
                val lRate = lGain / windowMin
                val jRate = jGain / windowMin
                val totRate = (lGain + jGain) / windowMin

                laneGain20ToEnd = lGain
                jungleGain20ToEnd = jGain
                laneRate20ToEnd = round2(lRate)
                jungleRate20ToEnd = round2(jRate)
                totalRate20ToEnd = round2(totRate)

                sumLaneGained20ToEnd += lGain
                sumJungleGained20ToEnd += jGain
                sumWindowMin20ToEnd += windowMin
                sumLaneRate20ToEnd += lRate
                sumJungleRate20ToEnd += jRate
                sumTotalRate20ToEnd += totRate
            }

            perMatchList.add(
                MatchPhaseFarmingDto(
                    matchId = game.matchId,
                    championName = game.championName,
                    role = game.role,
                    gameDurationSeconds = game.gameDurationSeconds,
                    reached10To20 = reached10To20,
                    checkpoint10TimestampMs = t10Ms,
                    checkpoint20TimestampMs = t20Ms,
                    laneCsAt10 = lane10,
                    jungleCsAt10 = jungle10,
                    laneCsAt20 = lane20,
                    jungleCsAt20 = jungle20,
                    laneCsGained10To20 = laneGain10To20,
                    jungleCsGained10To20 = jungleGain10To20,
                    laneCsPerMin10To20 = laneRate10To20,
                    jungleCsPerMin10To20 = jungleRate10To20,
                    totalCsPerMin10To20 = totalRate10To20,
                    reached20ToEnd = reached20ToEnd,
                    checkpointEndTimestampMs = tEndMs,
                    laneCsAtEnd = laneEnd,
                    jungleCsAtEnd = jungleEnd,
                    laneCsGained20ToEnd = laneGain20ToEnd,
                    jungleCsGained20ToEnd = jungleGain20ToEnd,
                    laneCsPerMin20ToEnd = laneRate20ToEnd,
                    jungleCsPerMin20ToEnd = jungleRate20ToEnd,
                    totalCsPerMin20ToEnd = totalRate20ToEnd
                )
            )
        }

        val window10To20 = PhaseFarmingWindowDto(
            windowLabel = "10-20m",
            eligibleGames = eligible10To20,
            excludedShortGames = shortGames10To20,
            excludedMissingFrameCounters = missingCounters10To20,
            avgLaneCsGained = if (eligible10To20 > 0) round2(sumLaneGained10To20 / eligible10To20) else null,
            avgJungleCsGained = if (eligible10To20 > 0) round2(sumJungleGained10To20 / eligible10To20) else null,
            avgTotalCsGained = if (eligible10To20 > 0) round2((sumLaneGained10To20 + sumJungleGained10To20) / eligible10To20) else null,
            avgWindowMinutes = if (eligible10To20 > 0) round2(sumWindowMin10To20 / eligible10To20) else null,
            avgLaneCsPerMin = if (eligible10To20 > 0) round2(sumLaneRate10To20 / eligible10To20) else null,
            avgJungleCsPerMin = if (eligible10To20 > 0) round2(sumJungleRate10To20 / eligible10To20) else null,
            avgTotalCsPerMin = if (eligible10To20 > 0) round2(sumTotalRate10To20 / eligible10To20) else null
        )

        val window20ToEnd = PhaseFarmingWindowDto(
            windowLabel = "20m-end",
            eligibleGames = eligible20ToEnd,
            excludedShortGames = shortGames20ToEnd,
            excludedMissingFrameCounters = missingCounters20ToEnd,
            avgLaneCsGained = if (eligible20ToEnd > 0) round2(sumLaneGained20ToEnd / eligible20ToEnd) else null,
            avgJungleCsGained = if (eligible20ToEnd > 0) round2(sumJungleGained20ToEnd / eligible20ToEnd) else null,
            avgTotalCsGained = if (eligible20ToEnd > 0) round2((sumLaneGained20ToEnd + sumJungleGained20ToEnd) / eligible20ToEnd) else null,
            avgWindowMinutes = if (eligible20ToEnd > 0) round2(sumWindowMin20ToEnd / eligible20ToEnd) else null,
            avgLaneCsPerMin = if (eligible20ToEnd > 0) round2(sumLaneRate20ToEnd / eligible20ToEnd) else null,
            avgJungleCsPerMin = if (eligible20ToEnd > 0) round2(sumJungleRate20ToEnd / eligible20ToEnd) else null,
            avgTotalCsPerMin = if (eligible20ToEnd > 0) round2(sumTotalRate20ToEnd / eligible20ToEnd) else null
        )

        return PhaseFarmingStatsDto(
            totalGamesConsidered = games.size,
            excludedMissingTimelineGames = missingTimelineGames,
            min10To20 = window10To20,
            min20ToEnd = window20ToEnd,
            perMatch = perMatchList
        )
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Lead Retention (10m vs 20m Gold/XP Leads & Temporal Objective Events)
    // ─────────────────────────────────────────────────────────────────────────

    private fun computeLeadRetentionStats(
        games: List<ParsedGameInput>,
        missingTimelineGames: Int
    ): LeadRetentionStatsDto {
        var missingOpponentGames = 0
        var shortBefore10 = 0
        var shortBefore20 = 0
        var eligibleAt10 = 0
        var eligibleAt20 = 0

        var sumGoldLead10 = 0.0
        var sumXpLead10 = 0.0
        var sumGoldLead20 = 0.0
        var sumXpLead20 = 0.0

        var aheadGoldAt10 = 0
        var aheadGoldAt10Reaching20 = 0
        var retainedGoldAt20 = 0

        var aheadXpAt10 = 0
        var aheadXpAt10Reaching20 = 0
        var retainedXpAt20 = 0

        var allyObj10To20 = 0
        var enemyObj10To20 = 0
        var allyTower10To20 = 0
        var enemyTower10To20 = 0

        val perMatchList = mutableListOf<MatchLeadRetentionDto>()

        for (game in games) {
            val tInfo = game.timeline?.info ?: continue
            val frames = tInfo.frames ?: continue

            val playerTeamId = game.player.teamId ?: 100
            val opponent = game.matchInfo.participants.find { p ->
                p.teamId != playerTeamId &&
                normalizeRole(p.teamPosition ?: p.individualPosition) == game.role &&
                game.role != "UNKNOWN"
            }

            val playerPid = resolveTimelineParticipantId(tInfo, game.player, game.matchInfo.participants)
            val opponentPid = resolveTimelineParticipantId(tInfo, opponent, game.matchInfo.participants)

            if (playerPid == null || opponent == null || opponentPid == null) {
                missingOpponentGames++
                continue
            }

            // Build participantId -> champion and participantId -> teamId maps for event attribution
            val pidToChamp = mutableMapOf<Int, String>()
            val pidToTeam = mutableMapOf<Int, Int>()
            game.matchInfo.participants.forEachIndexed { idx, p ->
                val pid = p.participantId ?: (idx + 1)
                pidToChamp[pid] = p.championName ?: "Participant $pid"
                pidToTeam[pid] = p.teamId ?: if (pid <= 5) 100 else 200
            }

            // Collect ALL relevant objective & building events in [10m, 20m] (not only personally last-hit)
            val temporalEvents = mutableListOf<TemporalObjectiveEventDto>()
            val allEvents = frames.flatMap { it.events ?: emptyList() }
            for (ev in allEvents) {
                val ts = ev.timestamp ?: continue
                if (ts < 600_000L || ts > 1_200_000L) continue
                val minMark = (ts / 60_000L).toInt()
                val secMark = ((ts / 1000L) % 60L).toInt()
                val killerChamp = ev.killerId?.let { pidToChamp[it] }
                val assistants = ev.assistingParticipantIds?.map { pidToChamp[it] ?: "Participant $it" } ?: emptyList()
                val playerInvolved = ev.killerId == playerPid || ev.assistingParticipantIds?.contains(playerPid) == true

                when (ev.type) {
                    "ELITE_MONSTER_KILL" -> {
                        val killerTeam = ev.killerTeamId
                            ?: ev.killerId?.let { pidToTeam[it] }
                            ?: ev.teamId
                        val securing = when (killerTeam) {
                            playerTeamId -> "ALLY_TEAM"
                            null -> "UNKNOWN"
                            else -> "ENEMY_TEAM"
                        }
                        if (securing == "ALLY_TEAM") allyObj10To20++
                        else if (securing == "ENEMY_TEAM") enemyObj10To20++

                        temporalEvents.add(
                            TemporalObjectiveEventDto(
                                eventCategory = "ELITE_MONSTER_KILL",
                                timestampMs = ts,
                                minuteMark = minMark,
                                secondMark = secMark,
                                objectiveType = ev.monsterType ?: "ELITE_MONSTER",
                                monsterSubType = ev.monsterSubType,
                                laneType = ev.laneType,
                                towerType = null,
                                securingTeam = securing,
                                killerChampion = killerChamp,
                                assistingChampions = assistants,
                                playerPersonallyInvolved = playerInvolved
                            )
                        )
                    }
                    "BUILDING_KILL", "TURRET_PLATE_DESTROYED" -> {
                        val killerTeam = if (ev.killerId != null && ev.killerId > 0) {
                            pidToTeam[ev.killerId]
                        } else if (ev.teamId != null) {
                            // On BUILDING_KILL / TURRET_PLATE_DESTROYED, Riot teamId is the team whose structure was destroyed
                            if (ev.teamId == 100) 200 else if (ev.teamId == 200) 100 else null
                        } else null

                        val securing = when (killerTeam) {
                            playerTeamId -> "ALLY_TEAM"
                            null -> "UNKNOWN"
                            else -> "ENEMY_TEAM"
                        }
                        if (securing == "ALLY_TEAM") allyTower10To20++
                        else if (securing == "ENEMY_TEAM") enemyTower10To20++

                        temporalEvents.add(
                            TemporalObjectiveEventDto(
                                eventCategory = ev.type,
                                timestampMs = ts,
                                minuteMark = minMark,
                                secondMark = secMark,
                                objectiveType = ev.buildingType ?: if (ev.type == "TURRET_PLATE_DESTROYED") "TURRET_PLATE" else "BUILDING",
                                monsterSubType = null,
                                laneType = ev.laneType,
                                towerType = ev.towerType,
                                securingTeam = securing,
                                killerChampion = killerChamp,
                                assistingChampions = assistants,
                                playerPersonallyInvolved = playerInvolved
                            )
                        )
                    }
                }
            }

            val frame10 = if (game.gameDurationSeconds >= 600L) findCheckpointFrame(frames, 10) else null
            val pf10 = frame10?.participantFrames?.get(playerPid.toString())
            val of10 = frame10?.participantFrames?.get(opponentPid.toString())

            if (
                game.gameDurationSeconds < 600L ||
                frame10 == null ||
                pf10?.totalGold == null || pf10.xp == null ||
                of10?.totalGold == null || of10.xp == null
            ) {
                shortBefore10++
                shortBefore20++
                continue
            }

            eligibleAt10++
            val gLead10 = pf10.totalGold - of10.totalGold
            val xLead10 = pf10.xp - of10.xp
            sumGoldLead10 += gLead10
            sumXpLead10 += xLead10

            val isAheadG10 = gLead10 > 0
            val isAheadX10 = xLead10 > 0
            if (isAheadG10) aheadGoldAt10++
            if (isAheadX10) aheadXpAt10++

            val frame20 = if (game.gameDurationSeconds >= 1200L) findCheckpointFrame(frames, 20) else null
            val pf20 = frame20?.participantFrames?.get(playerPid.toString())
            val of20 = frame20?.participantFrames?.get(opponentPid.toString())

            var gLead20: Int? = null
            var xLead20: Int? = null
            var retainedGold: Boolean? = null
            var retainedXp: Boolean? = null

            if (
                game.gameDurationSeconds < 1200L ||
                frame20 == null ||
                pf20?.totalGold == null || pf20.xp == null ||
                of20?.totalGold == null || of20.xp == null
            ) {
                shortBefore20++
            } else {
                eligibleAt20++
                gLead20 = pf20.totalGold - of20.totalGold
                xLead20 = pf20.xp - of20.xp
                sumGoldLead20 += gLead20
                sumXpLead20 += xLead20

                if (isAheadG10) {
                    aheadGoldAt10Reaching20++
                    retainedGold = gLead20 > 0
                    if (retainedGold) retainedGoldAt20++
                }
                if (isAheadX10) {
                    aheadXpAt10Reaching20++
                    retainedXp = xLead20 > 0
                    if (retainedXp) retainedXpAt20++
                }
            }

            perMatchList.add(
                MatchLeadRetentionDto(
                    matchId = game.matchId,
                    championName = game.championName,
                    opponentChampionName = opponent.championName,
                    role = game.role,
                    win = game.player.win == true,
                    goldLeadAt10 = gLead10,
                    xpLeadAt10 = xLead10,
                    goldLeadAt20 = gLead20,
                    xpLeadAt20 = xLead20,
                    aheadInGoldAt10 = isAheadG10,
                    retainedGoldLeadAt20 = retainedGold,
                    aheadInXpAt10 = isAheadX10,
                    retainedXpLeadAt20 = retainedXp,
                    goldLeadDelta10To20 = if (gLead20 != null) gLead20 - gLead10 else null,
                    xpLeadDelta10To20 = if (xLead20 != null) xLead20 - xLead10 else null,
                    temporalEvents10To20 = temporalEvents
                )
            )
        }

        return LeadRetentionStatsDto(
            totalGamesConsidered = games.size,
            excludedMissingTimelineGames = missingTimelineGames,
            excludedMissingOpponentGames = missingOpponentGames,
            excludedShortGamesBefore10 = shortBefore10,
            excludedShortGamesBefore20 = shortBefore20,
            eligibleGamesAt10 = eligibleAt10,
            eligibleGamesAt20 = eligibleAt20,
            avgGoldLeadAt10 = if (eligibleAt10 > 0) round1(sumGoldLead10 / eligibleAt10) else null,
            avgXpLeadAt10 = if (eligibleAt10 > 0) round1(sumXpLead10 / eligibleAt10) else null,
            avgGoldLeadAt20 = if (eligibleAt20 > 0) round1(sumGoldLead20 / eligibleAt20) else null,
            avgXpLeadAt20 = if (eligibleAt20 > 0) round1(sumXpLead20 / eligibleAt20) else null,
            gamesAheadInGoldAt10 = aheadGoldAt10,
            gamesAheadInGoldAt10Reaching20 = aheadGoldAt10Reaching20,
            gamesRetainedGoldLeadAt20 = retainedGoldAt20,
            goldLeadRetentionRate = if (aheadGoldAt10Reaching20 > 0) round4(retainedGoldAt20.toDouble() / aheadGoldAt10Reaching20) else null,
            gamesAheadInXpAt10 = aheadXpAt10,
            gamesAheadInXpAt10Reaching20 = aheadXpAt10Reaching20,
            gamesRetainedXpLeadAt20 = retainedXpAt20,
            xpLeadRetentionRate = if (aheadXpAt10Reaching20 > 0) round4(retainedXpAt20.toDouble() / aheadXpAt10Reaching20) else null,
            allyObjectiveEvents10To20 = allyObj10To20,
            enemyObjectiveEvents10To20 = enemyObj10To20,
            allyTowerEvents10To20 = allyTower10To20,
            enemyTowerEvents10To20 = enemyTower10To20,
            perMatch = perMatchList
        )
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Death Cost (totalTimeSpentDead & Recorded Death Bounties)
    // ─────────────────────────────────────────────────────────────────────────

    private fun computeDeathCostStats(
        games: List<ParsedGameInput>,
        missingTimelineGames: Int
    ): DeathCostStatsDto {
        var eligibleTimeDeadGames = 0
        var missingTimeDeadGames = 0
        var sumTimeSpentDeadSec = 0
        var sumDurationForDeadGamesSec = 0L
        var sumDeadSharePerGame = 0.0

        var eligibleTimelineGames = 0
        var totalRecordedDeaths = 0
        var deathsWithBaseBounty = 0
        var deathsWithShutdownBounty = 0
        var deathsWithAnyBounty = 0
        var deathsMissingBounty = 0
        var sumBaseBountyGold = 0
        var sumShutdownBountyGold = 0
        var sumCombinedBountyGold = 0

        val perMatchList = mutableListOf<MatchDeathCostDto>()

        for (game in games) {
            val deadSec = game.player.totalTimeSpentDead
            val share: Double?
            val sharePct: Double?
            if (deadSec == null) {
                missingTimeDeadGames++
                share = null
                sharePct = null
            } else {
                eligibleTimeDeadGames++
                sumTimeSpentDeadSec += deadSec
                sumDurationForDeadGamesSec += game.gameDurationSeconds
                val rawShare = deadSec.toDouble() / game.gameDurationSeconds.toDouble()
                sumDeadSharePerGame += rawShare
                share = round4(rawShare)
                sharePct = round2(rawShare * 100.0)
            }

            val tInfo = game.timeline?.info
            val frames = tInfo?.frames
            val playerPid = if (tInfo != null) resolveTimelineParticipantId(tInfo, game.player, game.matchInfo.participants) else null

            if (tInfo == null || frames.isNullOrEmpty() || playerPid == null) {
                perMatchList.add(
                    MatchDeathCostDto(
                        matchId = game.matchId,
                        championName = game.championName,
                        role = game.role,
                        gameDurationSeconds = game.gameDurationSeconds,
                        deathsFromMatchDto = game.player.deaths,
                        totalTimeSpentDeadSeconds = deadSec,
                        deadTimeGameShare = share,
                        deadTimeGameSharePct = sharePct,
                        hasTimeline = false,
                        recordedDeathsCount = null,
                        totalBaseKillBountyGold = null,
                        totalShutdownBountyGold = null,
                        totalCombinedBountyGold = null,
                        recordedDeaths = emptyList()
                    )
                )
                continue
            }

            eligibleTimelineGames++
            val pidToChamp = mutableMapOf<Int, String>()
            game.matchInfo.participants.forEachIndexed { idx, p ->
                val pid = p.participantId ?: (idx + 1)
                pidToChamp[pid] = p.championName ?: "Participant $pid"
            }

            val playerDeathEvents = frames
                .flatMap { it.events ?: emptyList() }
                .filter { it.type == "CHAMPION_KILL" && it.victimId == playerPid }

            var matchBaseSum = 0
            var matchBaseCount = 0
            var matchShutdownSum = 0
            var matchShutdownCount = 0
            var matchCombinedSum = 0
            var matchAnyBountyCount = 0
            val matchDeathDtos = mutableListOf<RecordedDeathBountyEventDto>()

            for (ev in playerDeathEvents) {
                totalRecordedDeaths++
                val ts = ev.timestamp ?: 0L
                val minMark = (ts / 60_000L).toInt()
                val secMark = ((ts / 1000L) % 60L).toInt()
                val baseBounty = ev.bounty
                val shutdownBounty = ev.shutdownBounty

                if (baseBounty != null) {
                    deathsWithBaseBounty++
                    matchBaseCount++
                    sumBaseBountyGold += baseBounty
                    matchBaseSum += baseBounty
                }
                if (shutdownBounty != null) {
                    deathsWithShutdownBounty++
                    matchShutdownCount++
                    sumShutdownBountyGold += shutdownBounty
                    matchShutdownSum += shutdownBounty
                }

                val combined: Int? = if (baseBounty != null || shutdownBounty != null) {
                    val c = (baseBounty ?: 0) + (shutdownBounty ?: 0)
                    deathsWithAnyBounty++
                    matchAnyBountyCount++
                    sumCombinedBountyGold += c
                    matchCombinedSum += c
                    c
                } else {
                    deathsMissingBounty++
                    null
                }

                matchDeathDtos.add(
                    RecordedDeathBountyEventDto(
                        timestampMs = ts,
                        minuteMark = minMark,
                        secondMark = secMark,
                        killerChampion = ev.killerId?.let { pidToChamp[it] },
                        assistingChampions = ev.assistingParticipantIds?.map { pidToChamp[it] ?: "Participant $it" } ?: emptyList(),
                        baseKillBountyGold = baseBounty,
                        shutdownBountyGold = shutdownBounty,
                        combinedBountyGold = combined
                    )
                )
            }

            val mDeaths = playerDeathEvents.size
            perMatchList.add(
                MatchDeathCostDto(
                    matchId = game.matchId,
                    championName = game.championName,
                    role = game.role,
                    gameDurationSeconds = game.gameDurationSeconds,
                    deathsFromMatchDto = game.player.deaths,
                    totalTimeSpentDeadSeconds = deadSec,
                    deadTimeGameShare = share,
                    deadTimeGameSharePct = sharePct,
                    hasTimeline = true,
                    recordedDeathsCount = mDeaths,
                    totalBaseKillBountyGold = when {
                        mDeaths == 0 -> 0
                        matchBaseCount > 0 -> matchBaseSum
                        else -> null
                    },
                    totalShutdownBountyGold = when {
                        mDeaths == 0 -> 0
                        matchShutdownCount > 0 -> matchShutdownSum
                        else -> null
                    },
                    totalCombinedBountyGold = when {
                        mDeaths == 0 -> 0
                        matchAnyBountyCount > 0 -> matchCombinedSum
                        else -> null
                    },
                    recordedDeaths = matchDeathDtos
                )
            )
        }

        val overallShare = if (eligibleTimeDeadGames > 0 && sumDurationForDeadGamesSec > 0L) {
            sumTimeSpentDeadSec.toDouble() / sumDurationForDeadGamesSec.toDouble()
        } else null
        val avgShare = if (eligibleTimeDeadGames > 0) sumDeadSharePerGame / eligibleTimeDeadGames else null

        val bountySummary = DeathCostBountySummaryDto(
            eligibleTimelineGames = eligibleTimelineGames,
            excludedMissingTimelineGames = games.size - eligibleTimelineGames,
            totalRecordedDeaths = if (eligibleTimelineGames > 0) totalRecordedDeaths else null,
            deathsWithBaseBountyData = deathsWithBaseBounty,
            deathsWithShutdownBountyData = deathsWithShutdownBounty,
            deathsWithAnyBountyData = deathsWithAnyBounty,
            deathsMissingBountyData = deathsMissingBounty,
            totalBaseKillBountyGold = when {
                eligibleTimelineGames == 0 -> null
                totalRecordedDeaths == 0 -> 0
                deathsWithBaseBounty > 0 -> sumBaseBountyGold
                else -> null
            },
            totalShutdownBountyGold = when {
                eligibleTimelineGames == 0 -> null
                totalRecordedDeaths == 0 -> 0
                deathsWithShutdownBounty > 0 -> sumShutdownBountyGold
                else -> null
            },
            totalCombinedBountyGold = when {
                eligibleTimelineGames == 0 -> null
                totalRecordedDeaths == 0 -> 0
                deathsWithAnyBounty > 0 -> sumCombinedBountyGold
                else -> null
            },
            avgBaseKillBountyPerDeath = if (deathsWithBaseBounty > 0) round2(sumBaseBountyGold.toDouble() / deathsWithBaseBounty) else null,
            avgShutdownBountyPerDeath = if (deathsWithShutdownBounty > 0) round2(sumShutdownBountyGold.toDouble() / deathsWithShutdownBounty) else null,
            avgCombinedBountyPerDeath = if (deathsWithAnyBounty > 0) round2(sumCombinedBountyGold.toDouble() / deathsWithAnyBounty) else null
        )

        return DeathCostStatsDto(
            totalGamesConsidered = games.size,
            eligibleTimeDeadGames = eligibleTimeDeadGames,
            excludedMissingTimeSpentDeadGames = missingTimeDeadGames,
            totalTimeSpentDeadSeconds = if (eligibleTimeDeadGames > 0) sumTimeSpentDeadSec else null,
            avgTimeSpentDeadSeconds = if (eligibleTimeDeadGames > 0) round2(sumTimeSpentDeadSec.toDouble() / eligibleTimeDeadGames) else null,
            avgDeadTimeGameShare = avgShare?.let { round4(it) },
            avgDeadTimeGameSharePct = avgShare?.let { round2(it * 100.0) },
            overallDeadTimeGameShare = overallShare?.let { round4(it) },
            overallDeadTimeGameSharePct = overallShare?.let { round2(it * 100.0) },
            bountyOnPlayerDeaths = bountySummary,
            perMatch = perMatchList
        )
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Unspent Gold (1-Minute currentGold Snapshots)
    // ─────────────────────────────────────────────────────────────────────────

    private fun computeUnspentGoldStats(
        games: List<ParsedGameInput>,
        missingTimelineGames: Int
    ): UnspentGoldStatsDto {
        val threshold = UnspentGoldStatsDto.HIGH_UNSPENT_GOLD_THRESHOLD
        var eligibleGames = 0
        var noCurrentGoldGames = 0

        val allGoldSamples = mutableListOf<Int>()
        var highBalanceSamplesTotal = 0
        val highBalanceBeforePurchaseList = mutableListOf<UnspentGoldHighBalanceSampleDto>()
        val perMatchList = mutableListOf<MatchUnspentGoldDto>()

        for (game in games) {
            val tInfo = game.timeline?.info ?: continue
            val frames = tInfo.frames ?: continue
            val playerPid = resolveTimelineParticipantId(tInfo, game.player, game.matchInfo.participants)
            if (playerPid == null) {
                noCurrentGoldGames++
                continue
            }

            data class GoldSample(val timestampMs: Long, val minute: Int, val currentGold: Int)
            val matchSamples = mutableListOf<GoldSample>()
            for (frame in frames) {
                val ts = frame.timestamp ?: continue
                val cg = frame.participantFrames?.get(playerPid.toString())?.currentGold ?: continue
                matchSamples.add(GoldSample(ts, (ts / 60_000L).toInt(), cg))
            }

            if (matchSamples.isEmpty()) {
                noCurrentGoldGames++
                continue
            }

            eligibleGames++
            val playerPurchases = reconcileParticipantPurchaseEvents(frames, playerPid)

            var matchHighCount = 0
            var matchHighBeforePurchaseCount = 0

            for (sample in matchSamples) {
                allGoldSamples.add(sample.currentGold)
                if (sample.currentGold >= threshold) {
                    highBalanceSamplesTotal++
                    matchHighCount++
                    val nextPurchase = playerPurchases.firstOrNull { (it.timestamp ?: 0L) > sample.timestampMs }
                    if (nextPurchase != null) {
                        matchHighBeforePurchaseCount++
                        val nextTs = nextPurchase.timestamp!!
                        highBalanceBeforePurchaseList.add(
                            UnspentGoldHighBalanceSampleDto(
                                matchId = game.matchId,
                                championName = game.championName,
                                role = game.role,
                                sampledMinute = sample.minute,
                                sampledTimestampMs = sample.timestampMs,
                                currentGoldAtSample = sample.currentGold,
                                nextPurchaseTimestampMs = nextTs,
                                nextPurchaseMinute = (nextTs / 60_000L).toInt(),
                                nextPurchaseItemId = nextPurchase.itemId
                            )
                        )
                    }
                }
            }

            val matchGoldValues = matchSamples.map { it.currentGold }
            perMatchList.add(
                MatchUnspentGoldDto(
                    matchId = game.matchId,
                    championName = game.championName,
                    role = game.role,
                    sampleCount = matchGoldValues.size,
                    medianCurrentGold = computeMedian(matchGoldValues),
                    maxCurrentGold = matchGoldValues.maxOrNull(),
                    avgCurrentGold = round1(matchGoldValues.average()),
                    highBalanceSampleCount = matchHighCount,
                    highBalanceBeforePurchaseSampleCount = matchHighBeforePurchaseCount
                )
            )
        }

        val totalSamples = allGoldSamples.size
        return UnspentGoldStatsDto(
            totalGamesConsidered = games.size,
            eligibleGames = eligibleGames,
            excludedMissingTimelineGames = missingTimelineGames,
            excludedNoCurrentGoldSamplesGames = noCurrentGoldGames,
            sampleCount = totalSamples,
            medianCurrentGold = computeMedian(allGoldSamples),
            maxCurrentGold = allGoldSamples.maxOrNull(),
            avgCurrentGold = if (totalSamples > 0) round1(allGoldSamples.average()) else null,
            highBalanceThresholdGold = threshold,
            highBalanceSampleCount = if (totalSamples > 0) highBalanceSamplesTotal else null,
            highBalanceBeforePurchaseSampleCount = if (totalSamples > 0) highBalanceBeforePurchaseList.size else null,
            highBalanceBeforePurchaseSamples = highBalanceBeforePurchaseList,
            perMatch = perMatchList
        )
    }

    private fun computeMedian(values: List<Int>): Double? {
        if (values.isEmpty()) return null
        val sorted = values.sorted()
        val mid = sorted.size / 2
        val raw = if (sorted.size % 2 == 1) {
            sorted[mid].toDouble()
        } else {
            (sorted[mid - 1] + sorted[mid]) / 2.0
        }
        return round1(raw)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Team Objectives (MatchDto.info.teams vs Personal Takedowns)
    // ─────────────────────────────────────────────────────────────────────────

    private fun computeTeamObjectivesStats(games: List<ParsedGameInput>): TeamObjectivesStatsDto {
        var eligibleTeamGames = 0
        var missingTeamsGames = 0
        val perMatchList = mutableListOf<MatchTeamObjectivesDto>()

        for (game in games) {
            val playerTeamId = game.player.teamId
            val teams = game.matchInfo.teams
            val allyTeam = if (playerTeamId != null) teams?.find { it.teamId == playerTeamId } else null
            val enemyTeam = if (playerTeamId != null) teams?.find { it.teamId != null && it.teamId != playerTeamId } else null

            val hasTeamsData = allyTeam?.objectives != null
            if (hasTeamsData) {
                eligibleTeamGames++
            } else {
                missingTeamsGames++
            }

            perMatchList.add(
                MatchTeamObjectivesDto(
                    matchId = game.matchId,
                    championName = game.championName,
                    role = game.role,
                    teamId = playerTeamId,
                    win = allyTeam?.win ?: game.player.win,
                    hasTeamsData = hasTeamsData,
                    allyObjectives = allyTeam?.objectives,
                    enemyObjectives = enemyTeam?.objectives,
                    personalTurretKills = game.player.turretKills,
                    personalTurretTakedowns = game.player.turretTakedowns,
                    personalDragonKills = game.player.dragonKills,
                    personalDragonTakedowns = game.player.challenges?.dragonTakedowns,
                    personalBaronKills = game.player.baronKills,
                    personalVoidMonsterKills = game.player.challenges?.voidMonsterKill
                )
            )
        }

        val towerSummary = buildObjectiveSummary(
            key = "tower",
            games = games,
            allyExtractor = { it?.tower },
            enemyExtractor = { it?.tower },
            personalLastHitExtractor = { it.turretKills },
            personalTakedownExtractor = { it.turretTakedowns },
            timelineEventMatcher = { ev -> ev.type == "BUILDING_KILL" && (ev.buildingType == null || ev.buildingType == "TOWER_BUILDING") }
        )

        val dragonSummary = buildObjectiveSummary(
            key = "dragon",
            games = games,
            allyExtractor = { it?.dragon },
            enemyExtractor = { it?.dragon },
            personalLastHitExtractor = { it.dragonKills },
            personalTakedownExtractor = { it.challenges?.dragonTakedowns },
            timelineEventMatcher = { ev -> ev.type == "ELITE_MONSTER_KILL" && ev.monsterType == "DRAGON" }
        )

        val baronSummary = buildObjectiveSummary(
            key = "baron",
            games = games,
            allyExtractor = { it?.baron },
            enemyExtractor = { it?.baron },
            personalLastHitExtractor = { it.baronKills },
            personalTakedownExtractor = { null },
            timelineEventMatcher = { ev -> ev.type == "ELITE_MONSTER_KILL" && ev.monsterType == "BARON_NASHOR" }
        )

        val riftHeraldSummary = buildObjectiveSummary(
            key = "riftHerald",
            games = games,
            allyExtractor = { it?.riftHerald },
            enemyExtractor = { it?.riftHerald },
            personalLastHitExtractor = { null },
            personalTakedownExtractor = { null },
            timelineEventMatcher = { ev -> ev.type == "ELITE_MONSTER_KILL" && ev.monsterType == "RIFTHERALD" }
        )

        val hordeSummary = buildObjectiveSummary(
            key = "horde",
            games = games,
            allyExtractor = { it?.horde },
            enemyExtractor = { it?.horde },
            personalLastHitExtractor = { null },
            personalTakedownExtractor = { it.challenges?.voidMonsterKill },
            timelineEventMatcher = { ev -> ev.type == "ELITE_MONSTER_KILL" && ev.monsterType == "HORDE" }
        )

        return TeamObjectivesStatsDto(
            totalGamesConsidered = games.size,
            eligibleTeamObjectiveGames = eligibleTeamGames,
            excludedMissingTeamsDataGames = missingTeamsGames,
            tower = towerSummary,
            dragon = dragonSummary,
            baron = baronSummary,
            riftHerald = riftHeraldSummary,
            horde = hordeSummary,
            perMatch = perMatchList
        )
    }

    private fun buildObjectiveSummary(
        key: String,
        games: List<ParsedGameInput>,
        allyExtractor: (TeamObjectivesDto?) -> ObjectiveStatsDto?,
        enemyExtractor: (TeamObjectivesDto?) -> ObjectiveStatsDto?,
        personalLastHitExtractor: (ParticipantDto) -> Int?,
        personalTakedownExtractor: (ParticipantDto) -> Int?,
        timelineEventMatcher: (TimelineEventDto) -> Boolean
    ): ObjectiveTypeSummaryDto {
        var eligibleKills = 0
        var eligibleFirst = 0
        var excludedMissingObj = 0
        var firstCount = 0
        var teamKillsSum = 0

        var enemyEligibleKills = 0
        var enemyKillsSum = 0

        var personalLastHitGames = 0
        var personalLastHitSum = 0

        var personalTakedownGames = 0
        var personalTakedownSum = 0

        var timelineGames = 0
        var timelineInvolvements = 0

        for (game in games) {
            val playerTeamId = game.player.teamId
            val teams = game.matchInfo.teams
            val allyObjectives = if (playerTeamId != null) teams?.find { it.teamId == playerTeamId }?.objectives else null
            val enemyObjectives = if (playerTeamId != null) teams?.find { it.teamId != null && it.teamId != playerTeamId }?.objectives else null

            val allyObj = allyExtractor(allyObjectives)
            if (allyObj?.kills != null) {
                eligibleKills++
                teamKillsSum += allyObj.kills
            } else {
                excludedMissingObj++
            }
            if (allyObj?.first != null) {
                eligibleFirst++
                if (allyObj.first) firstCount++
            }

            val enemyObj = enemyExtractor(enemyObjectives)
            if (enemyObj?.kills != null) {
                enemyEligibleKills++
                enemyKillsSum += enemyObj.kills
            }

            val lastHit = personalLastHitExtractor(game.player)
            if (lastHit != null) {
                personalLastHitGames++
                personalLastHitSum += lastHit
            }

            val takedown = personalTakedownExtractor(game.player)
            if (takedown != null) {
                personalTakedownGames++
                personalTakedownSum += takedown
            }

            val tInfo = game.timeline?.info
            val frames = tInfo?.frames
            val playerPid = if (tInfo != null) resolveTimelineParticipantId(tInfo, game.player, game.matchInfo.participants) else null
            if (tInfo != null && !frames.isNullOrEmpty() && playerPid != null) {
                timelineGames++
                val involvedEvents = frames
                    .flatMap { it.events ?: emptyList() }
                    .count { ev ->
                        timelineEventMatcher(ev) &&
                            (ev.killerId == playerPid || ev.assistingParticipantIds?.contains(playerPid) == true)
                    }
                timelineInvolvements += involvedEvents
            }
        }

        return ObjectiveTypeSummaryDto(
            objectiveKey = key,
            eligibleGamesWithKills = eligibleKills,
            eligibleGamesWithFirstFlag = eligibleFirst,
            excludedMissingObjectiveGames = excludedMissingObj,
            teamFirstCount = if (eligibleFirst > 0) firstCount else null,
            teamFirstRate = if (eligibleFirst > 0) round4(firstCount.toDouble() / eligibleFirst) else null,
            teamTotalKills = if (eligibleKills > 0) teamKillsSum else null,
            teamAvgKills = if (eligibleKills > 0) round2(teamKillsSum.toDouble() / eligibleKills) else null,
            enemyEligibleGamesWithKills = enemyEligibleKills,
            enemyTeamTotalKills = if (enemyEligibleKills > 0) enemyKillsSum else null,
            enemyTeamAvgKills = if (enemyEligibleKills > 0) round2(enemyKillsSum.toDouble() / enemyEligibleKills) else null,
            personalLastHitEligibleGames = personalLastHitGames,
            personalTotalLastHits = if (personalLastHitGames > 0) personalLastHitSum else null,
            personalAvgLastHits = if (personalLastHitGames > 0) round2(personalLastHitSum.toDouble() / personalLastHitGames) else null,
            personalTakedownEligibleGames = personalTakedownGames,
            personalTotalTakedowns = if (personalTakedownGames > 0) personalTakedownSum else null,
            personalAvgTakedowns = if (personalTakedownGames > 0) round2(personalTakedownSum.toDouble() / personalTakedownGames) else null,
            timelineEligibleGames = timelineGames,
            timelinePersonalInvolvements = if (timelineGames > 0) timelineInvolvements else null
        )
    }

    private fun buildPowerSpikePoints(
        frames: List<TimelineFrameDto>,
        playerParticipantId: Int,
        opponentParticipantId: Int?,
        matchId: String,
        matchEvents: List<MatchEventEntry>
    ): List<PowerSpikePoint> {
        val playerItems = mutableListOf<Int>()
        val opponentItems = mutableListOf<Int>()

        val points = mutableListOf<PowerSpikePoint>()
        for (frame in frames) {
            val minute = ((frame.timestamp ?: 0) / 60000).toInt()
            frame.events?.forEach { event ->
                when (event.type) {
                    "ITEM_PURCHASED" -> {
                        if (event.participantId == playerParticipantId && event.itemId != null && event.itemId > 0) {
                            playerItems.add(event.itemId)
                        }
                        if (opponentParticipantId != null && event.participantId == opponentParticipantId && event.itemId != null && event.itemId > 0) {
                            opponentItems.add(event.itemId)
                        }
                    }
                    "ITEM_UNDO" -> {
                        val undoneId = event.beforeId
                        if (undoneId != null && undoneId > 0) {
                            if (event.participantId == playerParticipantId) {
                                val lastIdx = playerItems.indexOfLast { it == undoneId }
                                if (lastIdx >= 0) playerItems.removeAt(lastIdx)
                            }
                            if (opponentParticipantId != null && event.participantId == opponentParticipantId) {
                                val lastIdx = opponentItems.indexOfLast { it == undoneId }
                                if (lastIdx >= 0) opponentItems.removeAt(lastIdx)
                            }
                        }
                    }
                }
            }

            val playerFrame = frame.participantFrames?.get(playerParticipantId.toString()) ?: continue
            val pg = playerFrame.totalGold ?: 0
            val pxp = playerFrame.xp ?: 0
            val pcs = (playerFrame.minionsKilled ?: 0) + (playerFrame.jungleMinionsKilled ?: 0)

            val og = if (opponentParticipantId != null)
                frame.participantFrames?.get(opponentParticipantId.toString())?.totalGold ?: 0
            else 0
            val oFrame = if (opponentParticipantId != null) frame.participantFrames?.get(opponentParticipantId.toString()) else null
            val oxp = oFrame?.xp ?: 0
            val ocs = (oFrame?.minionsKilled ?: 0) + (oFrame?.jungleMinionsKilled ?: 0)

            // Events occurring in this minute
            val eventsThisMinute = matchEvents.filter {
                it.minuteMark == minute && (
                    it.type == "KILL" || it.type == "DEATH" || it.type == "ASSIST" ||
                    it.type == "PLATE" || it.type == "OBJECTIVE"
                )
            }

            // Only emit one point per minute (skip duplicate timestamps)
            if (points.none { it.minute == minute }) {
                points.add(PowerSpikePoint(
                    minute = minute,
                    playerGold = pg,
                    opponentGold = og,
                    playerGoldLead = pg - og,
                    playerXp = pxp,
                    opponentXp = oxp,
                    playerCs = pcs,
                    opponentCs = ocs,
                    playerItems = playerItems.toList(),
                    opponentItems = opponentItems.toList(),
                    events = eventsThisMinute
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

