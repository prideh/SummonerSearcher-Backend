package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.ChampionStatsDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.OverallStatsDto
import com.pride.summoner_searcher_api.dto.SideStatsDto

/**
 * A stateful accumulator for calculating player statistics incrementally.
 * This allows processing matches one by one (streaming) without holding the entire list in memory.
 */
class MatchStatsAccumulator(private val puuid: String) {

    // --- Overall Stats ---
    private var totalGames = 0
    private var totalWins = 0
    private var totalKills = 0
    private var totalDeaths = 0
    private var totalAssists = 0
    private var totalCs = 0
    private var totalDurationInMinutes = 0.0
    private var totalKillParticipation = 0.0
    private var totalSoloKills = 0
    private var totalTurretPlates = 0
    private var totalVisionScore = 0

    // --- Opponent Stats (for comparison) ---
    private var oppTotalKills = 0
    private var oppTotalDeaths = 0
    private var oppTotalAssists = 0
    private var oppTotalCs = 0
    private var oppTotalSoloKills = 0
    private var oppTotalTurretPlates = 0
    private var oppTotalVisionScore = 0
    private var oppTotalKillParticipation = 0.0

    // --- Side Stats ---
    private var blueSideGames = 0
    private var blueSideWins = 0
    private var redSideGames = 0
    private var redSideWins = 0

    // --- Champion Stats ---
    private val championStatsMap = mutableMapOf<String, MutableChampionStats>()

    private data class MutableChampionStats(
        val championName: String,
        var games: Int = 0,
        var wins: Int = 0,
        var losses: Int = 0,
        var kills: Int = 0,
        var deaths: Int = 0,
        var assists: Int = 0,
        var soloKills: Int = 0,
        var turretPlates: Int = 0,
        var cs: Int = 0,
        var durationMinutes: Double = 0.0
    )

    /**
     * Process a single match and update internal counters.
     */
    fun add(match: MatchDto) {
        addMatchIfEligible(match)
    }

    /**
     * Processes a single match and returns `true` iff the match contained valid non-zero duration
     * participant data for [puuid] and was folded into the additive counters.
     */
    fun addMatchIfEligible(match: MatchDto): Boolean {
        return addMatchAndExtractDelta("", match) != null
    }

    /**
     * Extracts a compact, DTO-free [SingleMatchAdditiveDelta] for [puuid] from [match] without
     * retaining any reference to [MatchDto], [MatchInfo], or [ParticipantDto].
     */
    fun extractDelta(matchId: String, match: MatchDto): SingleMatchAdditiveDelta? {
        val info = match.info ?: return null
        if ((info.gameDuration ?: 0L) <= 0L) return null

        val participants = info.participants
        val player = participants.find { it.puuid == puuid } ?: return null

        val teamKills = participants.filter { it.teamId == player.teamId }.sumOf { it.kills ?: 0 }
        val opponent = participants.find {
            it.teamPosition == player.teamPosition && it.teamId != player.teamId && player.teamPosition != "NONE"
        }
        val oppTeamKills = if (opponent != null) {
            participants.filter { it.teamId == opponent.teamId }.sumOf { it.kills ?: 0 }
        } else 0

        val kills = player.kills ?: 0
        val deaths = player.deaths ?: 0
        val assists = player.assists ?: 0
        val cs = (player.totalMinionsKilled ?: 0) + (player.neutralMinionsKilled ?: 0)
        val soloKills = player.challenges?.soloKills ?: 0
        val turretPlates = (player.challenges?.turretPlatesTaken ?: 0).toInt()
        val visionScore = player.visionScore ?: 0
        val durationMin = (info.gameDuration ?: 0L) / 60.0
        val kp = if (teamKills > 0) (kills + assists).toDouble() / teamKills else 0.0

        val oppKills = opponent?.kills ?: 0
        val oppDeaths = opponent?.deaths ?: 0
        val oppAssists = opponent?.assists ?: 0
        val oppCs = if (opponent != null) {
            (opponent.totalMinionsKilled ?: 0) + (opponent.neutralMinionsKilled ?: 0)
        } else 0
        val oppSoloKills = opponent?.challenges?.soloKills ?: 0
        val oppTurretPlates = (opponent?.challenges?.turretPlatesTaken ?: 0).toInt()
        val oppVisionScore = opponent?.visionScore ?: 0
        val oppKp = if (opponent != null && oppTeamKills > 0) {
            (oppKills + oppAssists).toDouble() / oppTeamKills
        } else 0.0

        return SingleMatchAdditiveDelta(
            matchId = matchId,
            championName = player.championName ?: "Unknown",
            win = player.win == true,
            teamId = player.teamId ?: 0,
            kills = kills,
            deaths = deaths,
            assists = assists,
            cs = cs,
            durationMinutes = durationMin,
            killParticipation = kp,
            soloKills = soloKills,
            turretPlates = turretPlates,
            visionScore = visionScore,
            hasOpponent = opponent != null,
            oppKills = oppKills,
            oppDeaths = oppDeaths,
            oppAssists = oppAssists,
            oppCs = oppCs,
            oppSoloKills = oppSoloKills,
            oppTurretPlates = oppTurretPlates,
            oppVisionScore = oppVisionScore,
            oppKillParticipation = oppKp
        )
    }

    /**
     * Folds a pre-extracted [SingleMatchAdditiveDelta] into the accumulator.
     */
    fun addDelta(delta: SingleMatchAdditiveDelta) {
        totalGames++
        if (delta.win) totalWins++

        if (delta.teamId == 100) {
            blueSideGames++
            if (delta.win) blueSideWins++
        } else if (delta.teamId == 200) {
            redSideGames++
            if (delta.win) redSideWins++
        }

        totalKills += delta.kills
        totalDeaths += delta.deaths
        totalAssists += delta.assists
        totalCs += delta.cs
        totalSoloKills += delta.soloKills
        totalTurretPlates += delta.turretPlates
        totalVisionScore += delta.visionScore
        totalDurationInMinutes += delta.durationMinutes
        totalKillParticipation += delta.killParticipation

        if (delta.hasOpponent) {
            oppTotalKills += delta.oppKills
            oppTotalDeaths += delta.oppDeaths
            oppTotalAssists += delta.oppAssists
            oppTotalCs += delta.oppCs
            oppTotalSoloKills += delta.oppSoloKills
            oppTotalTurretPlates += delta.oppTurretPlates
            oppTotalVisionScore += delta.oppVisionScore
            oppTotalKillParticipation += delta.oppKillParticipation
        }

        val champStats = championStatsMap.getOrPut(delta.championName) { MutableChampionStats(delta.championName) }
        champStats.games++
        if (delta.win) champStats.wins++ else champStats.losses++
        champStats.kills += delta.kills
        champStats.deaths += delta.deaths
        champStats.assists += delta.assists
        champStats.soloKills += delta.soloKills
        champStats.turretPlates += delta.turretPlates
        champStats.cs += delta.cs
        champStats.durationMinutes += delta.durationMinutes
    }

    /**
     * Subtracts a pre-extracted [SingleMatchAdditiveDelta] from the accumulator when evicting
     * the oldest match from the capped 2,100-match moving window so counters never drift from
     * `processedMatchIds`.
     */
    fun subtractDelta(delta: SingleMatchAdditiveDelta) {
        if (totalGames <= 0) return
        totalGames = (totalGames - 1).coerceAtLeast(0)
        if (delta.win) {
            totalWins = (totalWins - 1).coerceAtLeast(0)
        }

        if (delta.teamId == 100) {
            blueSideGames = (blueSideGames - 1).coerceAtLeast(0)
            if (delta.win) {
                blueSideWins = (blueSideWins - 1).coerceAtLeast(0)
            }
        } else if (delta.teamId == 200) {
            redSideGames = (redSideGames - 1).coerceAtLeast(0)
            if (delta.win) {
                redSideWins = (redSideWins - 1).coerceAtLeast(0)
            }
        }

        totalKills = (totalKills - delta.kills).coerceAtLeast(0)
        totalDeaths = (totalDeaths - delta.deaths).coerceAtLeast(0)
        totalAssists = (totalAssists - delta.assists).coerceAtLeast(0)
        totalCs = (totalCs - delta.cs).coerceAtLeast(0)
        totalSoloKills = (totalSoloKills - delta.soloKills).coerceAtLeast(0)
        totalTurretPlates = (totalTurretPlates - delta.turretPlates).coerceAtLeast(0)
        totalVisionScore = (totalVisionScore - delta.visionScore).coerceAtLeast(0)
        totalDurationInMinutes = if (totalGames == 0) 0.0 else (totalDurationInMinutes - delta.durationMinutes).coerceAtLeast(0.0)
        totalKillParticipation = if (totalGames == 0) 0.0 else (totalKillParticipation - delta.killParticipation).coerceAtLeast(0.0)

        if (delta.hasOpponent) {
            oppTotalKills = (oppTotalKills - delta.oppKills).coerceAtLeast(0)
            oppTotalDeaths = (oppTotalDeaths - delta.oppDeaths).coerceAtLeast(0)
            oppTotalAssists = (oppTotalAssists - delta.oppAssists).coerceAtLeast(0)
            oppTotalCs = (oppTotalCs - delta.oppCs).coerceAtLeast(0)
            oppTotalSoloKills = (oppTotalSoloKills - delta.oppSoloKills).coerceAtLeast(0)
            oppTotalTurretPlates = (oppTotalTurretPlates - delta.oppTurretPlates).coerceAtLeast(0)
            oppTotalVisionScore = (oppTotalVisionScore - delta.oppVisionScore).coerceAtLeast(0)
            oppTotalKillParticipation = if (totalGames == 0) 0.0 else (oppTotalKillParticipation - delta.oppKillParticipation).coerceAtLeast(0.0)
        }

        val champStats = championStatsMap[delta.championName]
        if (champStats != null) {
            champStats.games = (champStats.games - 1).coerceAtLeast(0)
            if (champStats.games <= 0) {
                championStatsMap.remove(delta.championName)
            } else {
                if (delta.win) {
                    champStats.wins = (champStats.wins - 1).coerceAtLeast(0)
                } else {
                    champStats.losses = (champStats.losses - 1).coerceAtLeast(0)
                }
                champStats.kills = (champStats.kills - delta.kills).coerceAtLeast(0)
                champStats.deaths = (champStats.deaths - delta.deaths).coerceAtLeast(0)
                champStats.assists = (champStats.assists - delta.assists).coerceAtLeast(0)
                champStats.soloKills = (champStats.soloKills - delta.soloKills).coerceAtLeast(0)
                champStats.turretPlates = (champStats.turretPlates - delta.turretPlates).coerceAtLeast(0)
                champStats.cs = (champStats.cs - delta.cs).coerceAtLeast(0)
                champStats.durationMinutes = (champStats.durationMinutes - delta.durationMinutes).coerceAtLeast(0.0)
            }
        }
    }

    /**
     * Extracts and immediately folds a compact [SingleMatchAdditiveDelta] for [match] if eligible.
     */
    fun addMatchAndExtractDelta(matchId: String, match: MatchDto): SingleMatchAdditiveDelta? {
        val delta = extractDelta(matchId, match) ?: return null
        addDelta(delta)
        return delta
    }

    fun getTotalGames(): Int = totalGames

    /**
     * Exports exact unrounded additive counters for durable persistence.
     * Never derives counters from rounded DTO averages.
     */
    fun toAdditiveSnapshots(): Pair<OverallAdditiveCountersSnapshot, List<ChampionAdditiveCountersSnapshot>> {
        val overall = OverallAdditiveCountersSnapshot(
            totalGames = totalGames,
            totalWins = totalWins,
            totalKills = totalKills,
            totalDeaths = totalDeaths,
            totalAssists = totalAssists,
            totalCs = totalCs,
            totalDurationInMinutes = totalDurationInMinutes,
            totalKillParticipation = totalKillParticipation,
            totalSoloKills = totalSoloKills,
            totalTurretPlates = totalTurretPlates,
            totalVisionScore = totalVisionScore,
            oppTotalKills = oppTotalKills,
            oppTotalDeaths = oppTotalDeaths,
            oppTotalAssists = oppTotalAssists,
            oppTotalCs = oppTotalCs,
            oppTotalSoloKills = oppTotalSoloKills,
            oppTotalTurretPlates = oppTotalTurretPlates,
            oppTotalVisionScore = oppTotalVisionScore,
            oppTotalKillParticipation = oppTotalKillParticipation,
            blueSideGames = blueSideGames,
            blueSideWins = blueSideWins,
            redSideGames = redSideGames,
            redSideWins = redSideWins
        )
        val champions = championStatsMap.values
            .sortedByDescending { it.games }
            .map {
                ChampionAdditiveCountersSnapshot(
                    championName = it.championName,
                    games = it.games,
                    wins = it.wins,
                    losses = it.losses,
                    kills = it.kills,
                    deaths = it.deaths,
                    assists = it.assists,
                    soloKills = it.soloKills,
                    turretPlates = it.turretPlates,
                    cs = it.cs,
                    durationMinutes = it.durationMinutes
                )
            }
        return overall to champions
    }

    /**
     * Restores exact unrounded additive counters from a validated durable snapshot.
     */
    fun restoreFromAdditiveSnapshots(
        overall: OverallAdditiveCountersSnapshot,
        champions: List<ChampionAdditiveCountersSnapshot>
    ) {
        totalGames = overall.totalGames
        totalWins = overall.totalWins
        totalKills = overall.totalKills
        totalDeaths = overall.totalDeaths
        totalAssists = overall.totalAssists
        totalCs = overall.totalCs
        totalDurationInMinutes = overall.totalDurationInMinutes
        totalKillParticipation = overall.totalKillParticipation
        totalSoloKills = overall.totalSoloKills
        totalTurretPlates = overall.totalTurretPlates
        totalVisionScore = overall.totalVisionScore
        oppTotalKills = overall.oppTotalKills
        oppTotalDeaths = overall.oppTotalDeaths
        oppTotalAssists = overall.oppTotalAssists
        oppTotalCs = overall.oppTotalCs
        oppTotalSoloKills = overall.oppTotalSoloKills
        oppTotalTurretPlates = overall.oppTotalTurretPlates
        oppTotalVisionScore = overall.oppTotalVisionScore
        oppTotalKillParticipation = overall.oppTotalKillParticipation
        blueSideGames = overall.blueSideGames
        blueSideWins = overall.blueSideWins
        redSideGames = overall.redSideGames
        redSideWins = overall.redSideWins

        championStatsMap.clear()
        for (c in champions) {
            if (c.championName.isBlank() || c.games <= 0) continue
            championStatsMap[c.championName] = MutableChampionStats(
                championName = c.championName,
                games = c.games,
                wins = c.wins,
                losses = c.losses,
                kills = c.kills,
                deaths = c.deaths,
                assists = c.assists,
                soloKills = c.soloKills,
                turretPlates = c.turretPlates,
                cs = c.cs,
                durationMinutes = c.durationMinutes
            )
        }
    }

    /**
     * Compute and return the final DTOs based on accumulated data.
     */
    fun getResult(): Pair<List<ChampionStatsDto>, OverallStatsDto> {
        val overallStats = OverallStatsDto(
            winRate = if (totalGames > 0) (totalWins.toDouble() / totalGames) * 100 else 0.0,
            kda = if (totalDeaths > 0) (totalKills + totalAssists).toDouble() / totalDeaths else Double.POSITIVE_INFINITY,
            wins = totalWins,
            losses = totalGames - totalWins,
            avgCsPerMinute = if (totalDurationInMinutes > 0) totalCs / totalDurationInMinutes else 0.0,
            avgKillParticipation = if (totalGames > 0) (totalKillParticipation / totalGames) * 100 else 0.0,
            avgSoloKills = if (totalGames > 0) totalSoloKills.toDouble() / totalGames else 0.0,
            avgTurretPlates = if (totalGames > 0) totalTurretPlates.toDouble() / totalGames else 0.0,
            avgKills = if (totalGames > 0) totalKills.toDouble() / totalGames else 0.0,
            avgDeaths = if (totalGames > 0) totalDeaths.toDouble() / totalGames else 0.0,
            avgAssists = if (totalGames > 0) totalAssists.toDouble() / totalGames else 0.0,

            oppAvgKda = if (oppTotalDeaths > 0) (oppTotalKills + oppTotalAssists).toDouble() / oppTotalDeaths else Double.POSITIVE_INFINITY,
            oppAvgCsPerMinute = if (totalDurationInMinutes > 0) oppTotalCs / totalDurationInMinutes else 0.0,
            oppAvgKillParticipation = if (totalGames > 0) (oppTotalKillParticipation / totalGames) * 100 else 0.0,
            oppAvgSoloKills = if (totalGames > 0) oppTotalSoloKills.toDouble() / totalGames else 0.0,
            oppAvgTurretPlates = if (totalGames > 0) oppTotalTurretPlates.toDouble() / totalGames else 0.0,
            avgVisionScore = if (totalGames > 0) totalVisionScore.toDouble() / totalGames else 0.0,
            oppAvgVisionScore = if (totalGames > 0) oppTotalVisionScore.toDouble() / totalGames else 0.0,

            blueSide = SideStatsDto(
                games = blueSideGames,
                wins = blueSideWins,
                winRate = if (blueSideGames > 0) (blueSideWins.toDouble() / blueSideGames) * 100 else 0.0
            ),
            redSide = SideStatsDto(
                games = redSideGames,
                wins = redSideWins,
                winRate = if (redSideGames > 0) (redSideWins.toDouble() / redSideGames) * 100 else 0.0
            )
        )

        val championStatsList = championStatsMap.values.map {
            ChampionStatsDto(
                championName = it.championName,
                games = it.games,
                wins = it.wins,
                losses = it.losses,
                kills = it.kills,
                deaths = it.deaths,
                assists = it.assists,
                soloKills = it.soloKills,
                winRate = if (it.games > 0) (it.wins.toDouble() / it.games) * 100 else 0.0,
                kda = if (it.deaths > 0) (it.kills + it.assists).toDouble() / it.deaths else Double.POSITIVE_INFINITY,
                averageKills = if (it.games > 0) it.kills.toDouble() / it.games else 0.0,
                averageDeaths = if (it.games > 0) it.deaths.toDouble() / it.games else 0.0,
                averageAssists = if (it.games > 0) it.assists.toDouble() / it.games else 0.0,
                averageCsPerMinute = if (it.durationMinutes > 0) it.cs / it.durationMinutes else 0.0,
                averageSoloKills = if (it.games > 0) it.soloKills.toDouble() / it.games else 0.0,
                averageTurretPlates = if (it.games > 0) it.turretPlates.toDouble() / it.games else 0.0
            )
        }.sortedByDescending { it.games }

        return Pair(championStatsList, overallStats)
    }
}

data class OverallAdditiveCountersSnapshot(
    val totalGames: Int = 0,
    val totalWins: Int = 0,
    val totalKills: Int = 0,
    val totalDeaths: Int = 0,
    val totalAssists: Int = 0,
    val totalCs: Int = 0,
    val totalDurationInMinutes: Double = 0.0,
    val totalKillParticipation: Double = 0.0,
    val totalSoloKills: Int = 0,
    val totalTurretPlates: Int = 0,
    val totalVisionScore: Int = 0,
    val oppTotalKills: Int = 0,
    val oppTotalDeaths: Int = 0,
    val oppTotalAssists: Int = 0,
    val oppTotalCs: Int = 0,
    val oppTotalSoloKills: Int = 0,
    val oppTotalTurretPlates: Int = 0,
    val oppTotalVisionScore: Int = 0,
    val oppTotalKillParticipation: Double = 0.0,
    val blueSideGames: Int = 0,
    val blueSideWins: Int = 0,
    val redSideGames: Int = 0,
    val redSideWins: Int = 0
)

data class ChampionAdditiveCountersSnapshot(
    val championName: String = "",
    val games: Int = 0,
    val wins: Int = 0,
    val losses: Int = 0,
    val kills: Int = 0,
    val deaths: Int = 0,
    val assists: Int = 0,
    val soloKills: Int = 0,
    val turretPlates: Int = 0,
    val cs: Int = 0,
    val durationMinutes: Double = 0.0
)

data class SingleMatchAdditiveDelta(
    val matchId: String,
    val championName: String,
    val win: Boolean,
    val teamId: Int,
    val kills: Int,
    val deaths: Int,
    val assists: Int,
    val cs: Int,
    val durationMinutes: Double,
    val killParticipation: Double,
    val soloKills: Int,
    val turretPlates: Int,
    val visionScore: Int,
    val hasOpponent: Boolean,
    val oppKills: Int = 0,
    val oppDeaths: Int = 0,
    val oppAssists: Int = 0,
    val oppCs: Int = 0,
    val oppSoloKills: Int = 0,
    val oppTurretPlates: Int = 0,
    val oppVisionScore: Int = 0,
    val oppKillParticipation: Double = 0.0
)

data class DurablePlayerSeasonStatsDto(
    val schemaVersion: Int = CURRENT_SCHEMA_VERSION,
    val region: String = "",
    val puuid: String = "",
    val seasonYear: Int = 0,
    val queueId: Int = 420,
    val processedMatchIds: List<String> = emptyList(),
    val pendingMatchIds: List<String> = emptyList(),
    val excludedMatchIds: List<String> = emptyList(),
    val hitPreSeasonBoundary: Boolean = false,
    val firstPreSeasonMatchId: String? = null,
    val historyCapped: Boolean = false,
    val historyComplete: Boolean = false,
    val overallCounters: OverallAdditiveCountersSnapshot = OverallAdditiveCountersSnapshot(),
    val championCounters: List<ChampionAdditiveCountersSnapshot> = emptyList(),
    val updatedAtEpochMilli: Long = 0L
) {
    companion object {
        const val CURRENT_SCHEMA_VERSION = 1
        /** Matches `RiotApiService.fetchMatchIdsSince` safety break threshold (`if (allMatchIds.size > 2000) break`). */
        const val RIOT_MATCH_IDS_SAFETY_THRESHOLD = 2_000
        /** Preserves full 21-page (`21 * 100 = 2,100`) coverage returned by `RiotApiService.fetchMatchIdsSince`. */
        const val MAX_TRACKED_SEASON_MATCH_IDS = 2_100
        const val MAX_EXCLUDED_MATCH_IDS = 500
    }
}


