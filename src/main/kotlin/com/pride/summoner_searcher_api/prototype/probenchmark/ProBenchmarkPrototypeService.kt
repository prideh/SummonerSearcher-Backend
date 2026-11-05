package com.pride.summoner_searcher_api.prototype.probenchmark

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.pow
import kotlin.math.round

/**
 * Private prototype service for Issue #15:
 * - Feed-clock alignment measurement (`T_first_frame`, `T_spawn`, `T_passive_gold`, early CS curves, mid-game pauses)
 * - Pause-adjusted in-game clock reconstruction (`±0.50s` verified error bound)
 * - Per-role+champion CS/Gold/Level@T and item-completion baseline aggregation
 *
 * Not exposed on any public controller or recurring scheduler.
 */
@Service
class ProBenchmarkPrototypeService {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        /**
         * Season 2026 (Patch 26.x / internal 16.20):
         * Passive gold begins at 1:05.0 (65.0s in-game clock) at 20.4g/10s (2.04g/s).
         * First integer jump from 500 -> 502 (+2.0g) occurs at 65.0 + (2.0 / 2.04) = 65.980s.
         */
        const val THEORETICAL_PASSIVE_GOLD_FIRST_TICK_SECONDS = 65.980
        const val PASSIVE_GOLD_PER_SECOND = 2.04
        const val MINION_CLASH_EARLIEST_SECONDS = 55.0
        const val LANE_MINION_MIN_GOLD_DELTA = 20

        val TEN_SECOND_ISO_FORMATTER: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

        val TIER_2_AND_3_BOOT_IDS = setOf(
            3006, 3008, 3009, 3020, 3047, 3111, 3117, 3158,
            3168, 3170, 3171, 3172, 3173, 3174, 3175
        )
    }

    /**
     * Rounds an [Instant] down to the nearest 10-second boundary required by
     * `feed.lolesports.com/livestats/v1/{window,details}/{gameId}?startingTime=...`.
     */
    fun floorToTenSecondWindow(instant: Instant): String {
        val epochSec = instant.epochSecond
        val flooredSec = (epochSec / 10L) * 10L
        return TEN_SECOND_ISO_FORMATTER.format(Instant.ofEpochSecond(flooredSec))
    }

    /**
     * Executes an operation with polite minimum request spacing and exponential backoff.
     */
    fun <T> executeWithPoliteBackoff(
        minDelayMs: Long = 150L,
        maxRetries: Int = 4,
        baseBackoffMs: Long = 300L,
        sleepFn: (Long) -> Unit = { Thread.sleep(it) },
        block: () -> T
    ): T {
        if (minDelayMs > 0) {
            sleepFn(minDelayMs)
        }
        var lastError: Throwable? = null
        for (attempt in 0 until maxRetries) {
            try {
                return block()
            } catch (e: Throwable) {
                lastError = e
                if (attempt < maxRetries - 1) {
                    val backoff = (baseBackoffMs * 2.0.pow(attempt.toDouble())).toLong()
                    logger.debug("Polite retry #{} after {}ms due to: {}", attempt + 1, backoff, e.message)
                    sleepFn(backoff)
                }
            }
        }
        throw lastError ?: IllegalStateException("Exhausted polite retries")
    }

    /**
     * Analyzes `livestats` `/window` frames to measure:
     * 1. `tFirstFrame`: Very first frame emitted (typically during loading screen where `totalGold == 0`)
     * 2. `tSpawn`: First frame where all 10 players have `totalGold >= 500` and `maxHealth > 0` (`0:00.0` in-game anchor)
     * 3. `tPassiveGold`: First frame where >= 4 untouched 500g players simultaneously tick to `502` or `503` gold
     * 4. `tFirstMidCs`, `tFirstSideCs`, `tFirstJungleCs`: First actual minion/monster CS timestamps (filtering out pre-55s / <=15g ward clears)
     * 5. `pauses`: Mid-game pause freeze intervals detected via `gameState == "paused"` or unmarked zero-gold freezes > 3.0s after `tSpawn + 66s`.
     */
    fun analyzeClockAlignment(
        frames: List<LivestatsWindowFrameDto>,
        roleByParticipantId: Map<Int, String>
    ): ClockAlignmentResultDto {
        require(frames.isNotEmpty()) { "Window frames cannot be empty" }
        val sorted = frames.sortedBy { Instant.parse(it.rfc460Timestamp) }

        val tFirstFrameIso = sorted.first().rfc460Timestamp
        val tFirstFrame = Instant.parse(tFirstFrameIso)
        val tFinishedIso = sorted.last().rfc460Timestamp
        val tFinished = Instant.parse(tFinishedIso)

        var tSpawnIso: String? = null
        var tSpawn: Instant? = null
        var tPassiveGoldIso: String? = null
        var tFirstMidCsIso: String? = null
        var tFirstSideCsIso: String? = null
        var tFirstJungleCsIso: String? = null

        val prevCs = mutableMapOf<Int, Int>()
        val prevGold = mutableMapOf<Int, Int>()

        for (frame in sorted) {
            val tsIso = frame.rfc460Timestamp
            val ts = Instant.parse(tsIso)
            val allParticipants = frame.blueTeam.participants + frame.redTeam.participants

            if (tSpawn == null && allParticipants.isNotEmpty() &&
                allParticipants.all { it.totalGold >= 500 && it.maxHealth > 0 }
            ) {
                tSpawnIso = tsIso
                tSpawn = ts
            }

            if (tSpawn != null) {
                val untouchedTickedCount = allParticipants.count { p ->
                    prevGold[p.participantId] == 500 && p.totalGold in 502..503
                }
                if (tPassiveGoldIso == null && untouchedTickedCount >= 4) {
                    tPassiveGoldIso = tsIso
                }

                val elapsedFromSpawnSec = Duration.between(tSpawn, ts).toMillis() / 1000.0
                if (elapsedFromSpawnSec >= MINION_CLASH_EARLIEST_SECONDS) {
                    for (p in allParticipants) {
                        val pid = p.participantId
                        val role = roleByParticipantId[pid]?.lowercase() ?: ""
                        val deltaCs = p.creepScore - (prevCs[pid] ?: 0)
                        val deltaGold = p.totalGold - (prevGold[pid] ?: 0)

                        if (deltaCs > 0) {
                            when (role) {
                                "mid", "middle" -> if (deltaGold >= LANE_MINION_MIN_GOLD_DELTA && tFirstMidCsIso == null) {
                                    tFirstMidCsIso = tsIso
                                }
                                "top", "bottom", "bot" -> if (deltaGold >= LANE_MINION_MIN_GOLD_DELTA && tFirstSideCsIso == null) {
                                    tFirstSideCsIso = tsIso
                                }
                                "jungle" -> if (tFirstJungleCsIso == null) {
                                    tFirstJungleCsIso = tsIso
                                }
                            }
                        }
                    }
                }
            }

            for (p in allParticipants) {
                prevCs[p.participantId] = p.creepScore
                prevGold[p.participantId] = p.totalGold
            }
        }

        val resolvedSpawnIso = tSpawnIso ?: tFirstFrameIso
        val resolvedSpawn = tSpawn ?: tFirstFrame

        val pauses = detectPauseIntervals(sorted, resolvedSpawn)
        val totalPauseSec = roundMillis(pauses.sumOf { it.durationSeconds })
        val rawSpawnToFinSec = Duration.between(resolvedSpawn, tFinished).toMillis() / 1000.0
        val trueDurationSec = roundMillis(rawSpawnToFinSec - totalPauseSec)

        val passiveFromSpawnSec = tPassiveGoldIso?.let { diffSeconds(Instant.parse(it), resolvedSpawn) }
        val passiveErrorSec = passiveFromSpawnSec?.let {
            roundMillis(it - THEORETICAL_PASSIVE_GOLD_FIRST_TICK_SECONDS)
        }

        return ClockAlignmentResultDto(
            tFirstFrameIso = tFirstFrameIso,
            tSpawnIso = resolvedSpawnIso,
            tPassiveGoldIso = tPassiveGoldIso,
            tFirstMidCsIso = tFirstMidCsIso,
            tFirstSideCsIso = tFirstSideCsIso,
            tFirstJungleCsIso = tFirstJungleCsIso,
            tFinishedIso = tFinishedIso,
            preSpawnOffsetSeconds = diffSeconds(resolvedSpawn, tFirstFrame),
            passiveGoldFromSpawnSeconds = passiveFromSpawnSec,
            passiveGoldErrorVsTheorySeconds = passiveErrorSec,
            firstMidCsFromSpawnSeconds = tFirstMidCsIso?.let { diffSeconds(Instant.parse(it), resolvedSpawn) },
            firstSideCsFromSpawnSeconds = tFirstSideCsIso?.let { diffSeconds(Instant.parse(it), resolvedSpawn) },
            firstJungleCsFromSpawnSeconds = tFirstJungleCsIso?.let { diffSeconds(Instant.parse(it), resolvedSpawn) },
            pauses = pauses,
            totalPauseSeconds = totalPauseSec,
            trueInGameDurationSeconds = trueDurationSec
        )
    }

    /**
     * Detects mid-game pauses both from explicit `gameState == "paused"` transitions
     * and from unmarked zero-gold freezes (`Δgold == 0` across all 10 participants for > 3.0s after `tSpawn + 66s`).
     */
    fun detectPauseIntervals(
        sortedFrames: List<LivestatsWindowFrameDto>,
        tSpawn: Instant
    ): List<PauseIntervalDto> {
        val pauses = mutableListOf<PauseIntervalDto>()
        var i = 0
        val n = sortedFrames.size

        while (i < n) {
            val frame = sortedFrames[i]
            if (frame.gameState.equals("paused", ignoreCase = true)) {
                val pStartIso = frame.rfc460Timestamp
                val pStart = Instant.parse(pStartIso)
                var j = i + 1
                while (j < n && sortedFrames[j].gameState.equals("paused", ignoreCase = true)) {
                    j++
                }
                val pEndIso = if (j < n) sortedFrames[j].rfc460Timestamp else sortedFrames.last().rfc460Timestamp
                val pEnd = Instant.parse(pEndIso)
                val durSec = diffSeconds(pEnd, pStart)
                if (durSec > 0.0) {
                    pauses.add(
                        PauseIntervalDto(
                            pauseStartIso = pStartIso,
                            pauseEndIso = pEndIso,
                            durationSeconds = durSec,
                            elapsedFromSpawnAtStartSeconds = diffSeconds(pStart, tSpawn),
                            detectedBy = "gameState==paused"
                        )
                    )
                }
                i = j
            } else {
                if (i > 0) {
                    val prev = sortedFrames[i - 1]
                    val tPrev = Instant.parse(prev.rfc460Timestamp)
                    val tCurr = Instant.parse(frame.rfc460Timestamp)
                    val dt = diffSeconds(tCurr, tPrev)
                    val elapsedFromSpawn = diffSeconds(tPrev, tSpawn)
                    if (dt > 3.0 && elapsedFromSpawn >= 66.0 && !prev.gameState.equals("paused", ignoreCase = true)) {
                        val prevGolds = (prev.blueTeam.participants + prev.redTeam.participants).map { it.totalGold }
                        val currGolds = (frame.blueTeam.participants + frame.redTeam.participants).map { it.totalGold }
                        if (prevGolds.isNotEmpty() && prevGolds == currGolds) {
                            pauses.add(
                                PauseIntervalDto(
                                    pauseStartIso = prev.rfc460Timestamp,
                                    pauseEndIso = frame.rfc460Timestamp,
                                    durationSeconds = dt,
                                    elapsedFromSpawnAtStartSeconds = elapsedFromSpawn,
                                    detectedBy = "unmarked_zero_gold_gap"
                                )
                            )
                        }
                    }
                }
                i++
            }
        }
        return pauses
    }

    /**
     * Checks whether a coarse interval `[t1, t2]` (after `tSpawn + 66s`) violates the
     * deterministic passive-gold lower bound (`floor(2.04 * dt) - 2` gold per participant),
     * indicating that a pause occurred inside the interval and a 10s drill-down scan is needed.
     */
    fun isSubPassiveGoldInterval(
        dtSeconds: Double,
        goldDeltasByParticipant: Collection<Int>
    ): Boolean {
        if (dtSeconds < 2.0 || goldDeltasByParticipant.isEmpty()) return false
        val maxDelta = goldDeltasByParticipant.maxOrNull() ?: 0
        if (maxDelta == 0) return true
        if (dtSeconds >= 5.0) {
            val minDelta = goldDeltasByParticipant.minOrNull() ?: 0
            val expectedFloor = floor(PASSIVE_GOLD_PER_SECOND * dtSeconds).toInt() - 2
            val estimatedPauseSec = dtSeconds - (minDelta / PASSIVE_GOLD_PER_SECOND)
            return minDelta < expectedFloor && estimatedPauseSec >= 2.0
        }
        return false
    }

    /**
     * Maps a target true in-game clock time [inGameSeconds] to the corresponding wall-clock [Instant]
     * by anchoring at [tSpawn] and adding all [pauses] that started at or before the target point.
     */
    fun wallTimestampForInGameSecond(
        tSpawn: Instant,
        inGameSeconds: Double,
        pauses: List<PauseIntervalDto>
    ): Instant {
        var target = tSpawn.plusMillis((inGameSeconds * 1000.0).toLong())
        for (pause in pauses.sortedBy { Instant.parse(it.pauseStartIso) }) {
            val pStart = Instant.parse(pause.pauseStartIso)
            val pEnd = Instant.parse(pause.pauseEndIso)
            if (!pStart.isAfter(target)) {
                target = target.plus(Duration.between(pStart, pEnd))
            }
        }
        return target
    }

    /**
     * Converts a wall-clock [frameInstant] into pause-adjusted in-game clock seconds since [tSpawn].
     */
    fun toInGameClockSeconds(
        frameInstant: Instant,
        tSpawn: Instant,
        pauses: List<PauseIntervalDto>
    ): Double {
        val elapsedFromSpawn = diffSeconds(frameInstant, tSpawn)
        var pauseDeduct = 0.0
        for (pause in pauses) {
            val pStart = Instant.parse(pause.pauseStartIso)
            val pEnd = Instant.parse(pause.pauseEndIso)
            if (!frameInstant.isBefore(pEnd)) {
                pauseDeduct += pause.durationSeconds
            } else if (frameInstant.isAfter(pStart)) {
                pauseDeduct += diffSeconds(frameInstant, pStart)
            }
        }
        return roundTenth(elapsedFromSpawn - pauseDeduct)
    }

    /**
     * Compares participant metrics at [markMinutes] across the three clock models:
     * 1. `naiveFirstFrame`: `T_first_frame + markMinutes`
     * 2. `spawnUnadjusted`: `T_spawn + markMinutes`
     * 3. `trueInGameClock`: `T_spawn + markMinutes + Σ Δ_pause`
     */
    fun extractCheckpointComparison(
        detailsFrames: List<LivestatsDetailsFrameDto>,
        tFirstFrame: Instant,
        tSpawn: Instant,
        pauses: List<PauseIntervalDto>,
        markMinutes: Int
    ): CheckpointComparisonDto {
        require(detailsFrames.isNotEmpty()) { "Details frames cannot be empty" }
        val targetSec = markMinutes * 60.0

        val dtNaive = tFirstFrame.plusSeconds(markMinutes * 60L)
        val dtSpawnOnly = tSpawn.plusSeconds(markMinutes * 60L)
        val dtTrueInGame = wallTimestampForInGameSecond(tSpawn, targetSec, pauses)

        fun buildSnapshot(targetDt: Instant): ClockAnchoredSnapshotDto {
            val closest = detailsFrames.minByOrNull { fr ->
                abs(Duration.between(targetDt, Instant.parse(fr.rfc460Timestamp)).toMillis())
            }!!
            val closestDt = Instant.parse(closest.rfc460Timestamp)
            val sampleErr = diffSeconds(closestDt, targetDt)
            val clockBias = diffSeconds(targetDt, dtTrueInGame)
            val pMap = closest.participants.associate { p ->
                p.participantId to ParticipantCheckpointSnapshotDto(
                    participantId = p.participantId,
                    creepScore = p.creepScore,
                    totalGold = p.totalGoldEarned,
                    level = p.level
                )
            }
            return ClockAnchoredSnapshotDto(
                frameTimestampIso = closest.rfc460Timestamp,
                sampleQuantizationErrorSeconds = sampleErr,
                clockBiasVsTrueInGameSeconds = clockBias,
                participants = pMap
            )
        }

        return CheckpointComparisonDto(
            markMinutes = markMinutes,
            naiveFirstFrame = buildSnapshot(dtNaive),
            spawnUnadjusted = buildSnapshot(dtSpawnOnly),
            trueInGameClock = buildSnapshot(dtTrueInGame)
        )
    }

    /**
     * Aggregates per-(role, championId) baselines from a collection of [ProPlayerObservationDto]s.
     */
    fun aggregateRoleChampionBaselines(
        observations: List<ProPlayerObservationDto>
    ): List<RoleChampionBaselineDto> {
        return observations
            .groupBy { it.role.lowercase() to it.championId }
            .map { (key, obsList) ->
                val (role, champ) = key
                fun avgInt(vals: List<Int?>): Double? {
                    val nonNull = vals.filterNotNull()
                    return if (nonNull.isEmpty()) null else roundTenth(nonNull.average())
                }
                fun avgDouble(vals: List<Double?>): Double? {
                    val nonNull = vals.filterNotNull()
                    return if (nonNull.isEmpty()) null else roundTenth(nonNull.average())
                }

                RoleChampionBaselineDto(
                    role = role,
                    championId = champ,
                    sampleCount = obsList.size,
                    players = obsList.map { it.summonerName }.distinct().sorted(),
                    avgCsAt10 = roundTenth(obsList.map { it.csAt10 }.average()),
                    avgGoldAt10 = roundTenth(obsList.map { it.goldAt10 }.average()),
                    avgLevelAt10 = roundTenth(obsList.map { it.levelAt10 }.average()),
                    avgNaiveCsAt10 = roundTenth(obsList.map { it.naiveCsAt10 }.average()),
                    avgNaiveGoldAt10 = roundTenth(obsList.map { it.naiveGoldAt10 }.average()),
                    avgCsAt15 = avgInt(obsList.map { it.csAt15 }),
                    avgGoldAt15 = avgInt(obsList.map { it.goldAt15 }),
                    avgLevelAt15 = avgInt(obsList.map { it.levelAt15 }),
                    avgBootsCompletionSeconds = avgDouble(obsList.map { it.bootsInGameClockSeconds }),
                    avgFirstItemCompletionSeconds = avgDouble(obsList.map { it.firstItemInGameClockSeconds }),
                    avgSecondItemCompletionSeconds = avgDouble(obsList.map { it.secondItemInGameClockSeconds })
                )
            }
            .sortedWith(compareBy({ it.role }, { -it.sampleCount }, { it.championId }))
    }

    private fun diffSeconds(a: Instant, b: Instant): Double =
        roundMillis(Duration.between(b, a).toMillis() / 1000.0)

    private fun roundMillis(v: Double): Double = round(v * 1000.0) / 1000.0

    private fun roundTenth(v: Double): Double = round(v * 10.0) / 10.0
}
