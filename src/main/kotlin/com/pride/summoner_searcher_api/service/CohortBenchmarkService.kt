package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import com.pride.summoner_searcher_api.repository.CohortBenchmarkCellRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.roundToLong

@Service
class CohortBenchmarkService(
    private val redisCacheService: RedisCacheService,
    private val cohortBenchmarkCellRepository: CohortBenchmarkCellRepository,
    private val riotApiRateLimiter: RiotApiRateLimiter,
    private val riotApiService: RiotApiService,
    private val objectMapper: ObjectMapper,
    @Value("\${cohort.network-collection.enabled:false}")
    private val networkCollectionEnabled: Boolean = false
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val SOLO_QUEUE_ID = 420
        const val MIN_GAME_DURATION_SECONDS = 600L
        val ELIGIBLE_ROLES = setOf("TOP", "MIDDLE", "BOTTOM")
        val TIER_ORDER = listOf(
            "IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM",
            "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER"
        )
        private val BENCHMARK_CACHE_TTL = Duration.ofHours(24)
        private val MATCH_CACHE_TTL = Duration.ofDays(7)
    }

    private data class RankSnapshot(
        val puuid: String,
        val tier: String,
        val snapshotAt: Instant
    )

    private data class ParticipantSample(
        val matchId: String,
        val puuid: String,
        val tier: String,
        val role: String,
        val gameCreation: Long,
        val rankSnapshotAt: Instant,
        val laneCsAt10: Double?,
        val laneCsCapturePctAt10: Double?,
        val laneOnlyCsPerMin: Double?,
        val totalCsPerMinWithCamps: Double?,
        val timelineTotalCsAt10WithCamps: Double?,
        val goldPerMin: Double?,
        val damagePerMin: Double?,
        val visionScorePerMin: Double?,
        val killParticipation: Double?,
        val deathsPerGame: Double?,
        val timeSpentDeadShare: Double?
    )

    /**
     * Cache-only cohort extraction, inventory, and aggregation for [region] (default `euw1`).
     * Makes ZERO Riot API calls. Scans existing Redis keys, applies strict rank-snapshot timestamping,
     * `(matchId, puuid)` deduplication, per-player game cap (`maxGamesPerPlayer`), and separates
     * lane-only CS (`114`/`120` ceiling) from pro-compatible total CS (camps included).
     */
    fun extractCachedCohortsAndReport(
        region: String = "euw1",
        targetPatch: String? = null,
        maxGamesPerPlayer: Int = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER
    ): CohortExtractionReportDto {
        val normalizedRegion = region.lowercase().trim()
        val effectiveCap = maxGamesPerPlayer.coerceAtLeast(1)
        val scannedAt = Instant.now()

        // 1. Scan cached player profiles & Challenger leaderboard for timestamped current-rank snapshots
        val rankSnapshotsByPuuid = mutableMapOf<String, RankSnapshot>()
        val profileKeys = redisCacheService.scanKeys("player:profile:$normalizedRegion:*")
        var profilesWithSoloQueueRank = 0

        if (profileKeys.isNotEmpty()) {
            val profilesMap = profileKeys.chunked(200).flatMap { chunk ->
                redisCacheService.multiGet(chunk, SummonerProfileDto::class.java).entries
            }
            for ((_, profile) in profilesMap) {
                if (profile == null) continue
                val puuid = profile.puuid?.takeIf { it.isNotBlank() } ?: continue
                val rank = profile.soloQueueRank ?: continue
                val tier = rank.tier?.uppercase()?.trim()?.takeIf { it.isNotBlank() } ?: continue
                if (rank.queueType != null && rank.queueType != "RANKED_SOLO_5x5") continue
                val snapshotAt = profile.lastRefreshed ?: continue
                profilesWithSoloQueueRank++

                val existing = rankSnapshotsByPuuid[puuid]
                if (existing == null || snapshotAt.isAfter(existing.snapshotAt)) {
                    rankSnapshotsByPuuid[puuid] = RankSnapshot(
                        puuid = puuid,
                        tier = tier,
                        snapshotAt = snapshotAt
                    )
                }
            }
        }

        // Also scan Challenger leaderboard cache keys for the region
        val leaderboardKeys = (
            redisCacheService.scanKeys("leaderboard:$normalizedRegion:*") +
                listOf(
                    "leaderboard:$normalizedRegion::RANKED_SOLO_5x5",
                    "leaderboard:$normalizedRegion:RANKED_SOLO_5x5"
                )
            ).distinct()

        var challengerLeaderboardPlayers = 0
        for (lbKey in leaderboardKeys) {
            val cachedLb = redisCacheService.get(lbKey, CachedLeaderboardDto::class.java) ?: continue
            val snapshotAt = cachedLb.lastRefreshed ?: continue
            val queue = cachedLb.leaderboard?.queue
            if (queue != null && queue != "RANKED_SOLO_5x5") continue
            val lbTier = cachedLb.leaderboard?.tier?.uppercase()?.trim()?.takeIf { it.isNotBlank() } ?: "CHALLENGER"
            for (entry in cachedLb.leaderboard?.entries.orEmpty()) {
                val puuid = entry.puuid?.takeIf { it.isNotBlank() } ?: continue
                challengerLeaderboardPlayers++
                val existing = rankSnapshotsByPuuid[puuid]
                if (existing == null || snapshotAt.isAfter(existing.snapshotAt)) {
                    rankSnapshotsByPuuid[puuid] = RankSnapshot(
                        puuid = puuid,
                        tier = lbTier,
                        snapshotAt = snapshotAt
                    )
                }
            }
        }

        val ranksByTier = rankSnapshotsByPuuid.values
            .groupingBy { it.tier }
            .eachCount()
            .toSortedMap(compareBy { tierIndex(it) })

        // 2. Scan cached matches and timelines in Redis (zero network calls)
        val matchKeys = redisCacheService.scanKeys("match:$normalizedRegion:*")
        val timelineKeys = redisCacheService.scanKeys("timeline:$normalizedRegion:*")

        val cachedMatches = if (matchKeys.isNotEmpty()) {
            matchKeys.chunked(100).flatMap { chunk ->
                redisCacheService.multiGet(chunk, MatchDto::class.java).values.filterNotNull()
            }
        } else {
            emptyList()
        }

        val timelinesByMatchId = if (timelineKeys.isNotEmpty()) {
            timelineKeys.chunked(100).flatMap { chunk ->
                redisCacheService.multiGet(chunk, MatchTimelineDto::class.java).entries
            }.mapNotNull { (key, timeline) ->
                val matchId = timeline?.metadata?.matchId ?: key.substringAfterLast(":")
                if (timeline != null && matchId.isNotBlank()) matchId to timeline else null
            }.toMap()
        } else {
            emptyMap()
        }

        // Inventory matches by major.minor patch
        val matchesByPatchCounts = mutableMapOf<String, Int>()
        val soloQueueMatchesByPatch = mutableMapOf<String, Int>()
        for (match in cachedMatches) {
            val patch = extractMajorMinorPatch(match.info?.gameVersion)
            if (patch != null) {
                matchesByPatchCounts[patch] = (matchesByPatchCounts[patch] ?: 0) + 1
                if (match.info?.queueId == SOLO_QUEUE_ID) {
                    soloQueueMatchesByPatch[patch] = (soloQueueMatchesByPatch[patch] ?: 0) + 1
                }
            }
        }

        val resolvedPatch = targetPatch?.trim()?.takeIf { it.isNotBlank() }
            ?: soloQueueMatchesByPatch.keys.maxWithOrNull( patchComparator )
            ?: matchesByPatchCounts.keys.maxWithOrNull( patchComparator )

        val theoreticalLaneCeiling = theoreticalLaneMinionSupplyAt10(resolvedPatch)

        var soloQueue420MatchesOnTargetPatch = 0
        var excludedNonSoloQueueMatches = 0
        var excludedMissingGameVersionMatches = 0
        var excludedOtherPatchMatches = 0
        var excludedShortOrRemakeMatches = 0
        var topMidBotParticipantCandidates = 0
        var excludedOtherRoleParticipants = 0
        var participantsWithRankSnapshot = 0
        var participantsWithoutRankSnapshot = 0
        var deduplicatedParticipantEntries = 0

        val seenMatchPuuid = mutableSetOf<Pair<String, String>>()
        val candidateSamples = mutableListOf<ParticipantSample>()

        for (match in cachedMatches) {
            val info = match.info ?: continue
            if (info.queueId != SOLO_QUEUE_ID) {
                excludedNonSoloQueueMatches++
                continue
            }
            val patch = extractMajorMinorPatch(info.gameVersion)
            if (patch == null) {
                excludedMissingGameVersionMatches++
                continue
            }
            if (resolvedPatch != null && patch != resolvedPatch) {
                excludedOtherPatchMatches++
                continue
            }
            val durationSeconds = info.gameDuration ?: 0L
            val isRemakeOrEarlySurrender = durationSeconds < MIN_GAME_DURATION_SECONDS ||
                info.participants.any { it.gameEndedInEarlySurrender == true || it.teamEarlySurrendered == true }
            if (isRemakeOrEarlySurrender) {
                excludedShortOrRemakeMatches++
                continue
            }

            soloQueue420MatchesOnTargetPatch++
            val matchId = match.metadata?.matchId?.takeIf { it.isNotBlank() }
                ?: info.gameId?.let { "${normalizedRegion.uppercase()}_$it" }
                ?: continue
            val durationMinutes = durationSeconds.toDouble() / 60.0
            val gameCreation = info.gameCreation ?: 0L
            val timeline = timelinesByMatchId[matchId]
            val frameAt10m = findFrameAt10Minutes(timeline)

            val teamKillsByTeamId = info.participants
                .groupBy { it.teamId ?: 0 }
                .mapValues { (_, plist) -> plist.sumOf { it.kills ?: 0 } }

            for (participant in info.participants) {
                val role = resolveNormalizedRole(participant)
                if (role !in ELIGIBLE_ROLES) {
                    excludedOtherRoleParticipants++
                    continue
                }
                topMidBotParticipantCandidates++

                val puuid = participant.puuid?.takeIf { it.isNotBlank() }
                if (puuid == null) {
                    participantsWithoutRankSnapshot++
                    continue
                }

                // Never invent or infer historical rank when a participant has no timestamped rank snapshot
                val rankSnapshot = rankSnapshotsByPuuid[puuid]
                if (rankSnapshot == null) {
                    participantsWithoutRankSnapshot++
                    continue
                }
                participantsWithRankSnapshot++

                val dedupeKey = matchId to puuid
                if (!seenMatchPuuid.add(dedupeKey)) {
                    deduplicatedParticipantEntries++
                    continue
                }

                val participantFrame10m = findParticipantFrame(frameAt10m, participant, timeline)

                // Strictly lane-only CS at 10m (never includes jungle camps)
                val laneCs10 = participant.challenges?.laneMinionsFirst10Minutes?.toDouble()
                    ?: participantFrame10m?.minionsKilled?.toDouble()

                val laneCsCapturePct10 = laneCs10?.let {
                    ((it / theoreticalLaneCeiling.toDouble()) * 100.0).coerceAtLeast(0.0)
                }

                // Strictly lane-only CS/min at end of game (totalMinionsKilled is lane minions only in Riot Match-v5)
                val laneOnlyCsPerMin = participant.totalMinionsKilled?.let {
                    if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                }

                // Separate pro-compatible total CS metrics (including neutral/jungle camps, never mixed into laneCsAt10)
                val totalCsPerMinWithCamps = participant.totalMinionsKilled?.let { laneMinions ->
                    val neutralCamps = participant.neutralMinionsKilled ?: 0
                    if (durationMinutes > 0.0) (laneMinions + neutralCamps).toDouble() / durationMinutes else null
                }

                val timelineTotalCs10WithCamps = if (participantFrame10m?.minionsKilled != null) {
                    (participantFrame10m.minionsKilled + (participantFrame10m.jungleMinionsKilled ?: 0)).toDouble()
                } else if (
                    participant.challenges?.laneMinionsFirst10Minutes != null &&
                    participant.challenges.jungleCsBefore10Minutes != null
                ) {
                    participant.challenges.laneMinionsFirst10Minutes.toDouble() +
                        participant.challenges.jungleCsBefore10Minutes.toDouble()
                } else {
                    null
                }

                val goldPerMin = participant.challenges?.goldPerMinute?.toDouble()
                    ?: participant.goldEarned?.let { if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null }

                val damagePerMin = participant.challenges?.damagePerMinute?.toDouble()
                    ?: participant.totalDamageDealtToChampions?.let {
                        if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                    }

                val visionScorePerMin = participant.challenges?.visionScorePerMinute?.toDouble()
                    ?: participant.visionScore?.let {
                        if (durationMinutes > 0.0) it.toDouble() / durationMinutes else null
                    }

                val kp = participant.challenges?.killParticipation?.toDouble() ?: run {
                    val teamKills = teamKillsByTeamId[participant.teamId ?: 0] ?: 0
                    if (teamKills > 0 && participant.kills != null && participant.assists != null) {
                        ((participant.kills + participant.assists).toDouble() / teamKills.toDouble()).coerceIn(0.0, 1.0)
                    } else {
                        null
                    }
                }

                val deathsPerGame = participant.deaths?.toDouble()
                val timeSpentDeadShare = participant.totalTimeSpentDead?.let { deadSec ->
                    if (durationSeconds > 0L) (deadSec.toDouble() / durationSeconds.toDouble()).coerceIn(0.0, 1.0) else null
                }

                candidateSamples.add(
                    ParticipantSample(
                        matchId = matchId,
                        puuid = puuid,
                        tier = rankSnapshot.tier,
                        role = role,
                        gameCreation = gameCreation,
                        rankSnapshotAt = rankSnapshot.snapshotAt,
                        laneCsAt10 = laneCs10,
                        laneCsCapturePctAt10 = laneCsCapturePct10,
                        laneOnlyCsPerMin = laneOnlyCsPerMin,
                        totalCsPerMinWithCamps = totalCsPerMinWithCamps,
                        timelineTotalCsAt10WithCamps = timelineTotalCs10WithCamps,
                        goldPerMin = goldPerMin,
                        damagePerMin = damagePerMin,
                        visionScorePerMin = visionScorePerMin,
                        killParticipation = kp,
                        deathsPerGame = deathsPerGame,
                        timeSpentDeadShare = timeSpentDeadShare
                    )
                )
            }
        }

        // 3. Group by (tier, role), enforce per-player game cap (newest games first), and compute P25/Median/P75/IQR
        val groupedByCell = candidateSamples.groupBy { it.tier to it.role }
        var totalExcludedOverPlayerCap = 0
        var totalEligibleSamples = 0
        val distinctEligiblePlayers = mutableSetOf<String>()
        val computedCells = mutableListOf<CohortBenchmarkCellDto>()
        val effectivePatchLabel = resolvedPatch ?: "UNKNOWN"

        val sortedCellKeys = groupedByCell.keys.sortedWith(
            compareBy<Pair<String, String>> { tierIndex(it.first) }.thenBy { roleIndex(it.second) }
        )

        for (cellKey in sortedCellKeys) {
            val (tier, role) = cellKey
            val cellCandidates = groupedByCell[cellKey].orEmpty()
            val byPlayer = cellCandidates.groupBy { it.puuid }

            val retainedSamples = mutableListOf<ParticipantSample>()
            var cellExcludedOverCap = 0

            for ((_, playerSamples) in byPlayer) {
                val sortedNewestFirst = playerSamples.sortedWith(
                    compareByDescending<ParticipantSample> { it.gameCreation }.thenByDescending { it.matchId }
                )
                val kept = sortedNewestFirst.take(effectiveCap)
                val dropped = (sortedNewestFirst.size - kept.size).coerceAtLeast(0)
                cellExcludedOverCap += dropped
                retainedSamples.addAll(kept)
            }

            totalExcludedOverPlayerCap += cellExcludedOverCap
            totalEligibleSamples += retainedSamples.size
            retainedSamples.forEach { distinctEligiblePlayers.add(it.puuid) }

            val distinctPlayersCount = byPlayer.keys.size
            val eligibleGamesCount = retainedSamples.size
            val passesGate = distinctPlayersCount >= CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE &&
                eligibleGamesCount >= CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE

            val gateStatus = if (passesGate) {
                "READY_FOR_COLOR_BANDS"
            } else {
                "INSUFFICIENT_SAMPLE (players=$distinctPlayersCount/${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE}, games=$eligibleGamesCount/${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE})"
            }

            val earliestSnapshot = retainedSamples.minOfOrNull { it.rankSnapshotAt }
            val latestSnapshot = retainedSamples.maxOfOrNull { it.rankSnapshotAt }

            val metricsBundle = CohortMetricsBundleDto(
                laneCsAt10 = buildDistribution(
                    metricKey = "laneCsAt10",
                    unit = "lane_cs",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = theoreticalLaneCeiling,
                    samples = retainedSamples,
                    extractor = { it.laneCsAt10 }
                ),
                laneCsCapturePctAt10 = buildDistribution(
                    metricKey = "laneCsCapturePctAt10",
                    unit = "percent_of_spawned_lane_supply",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = theoreticalLaneCeiling,
                    samples = retainedSamples,
                    extractor = { it.laneCsCapturePctAt10 }
                ),
                laneOnlyCsPerMin = buildDistribution(
                    metricKey = "laneOnlyCsPerMin",
                    unit = "lane_cs_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.laneOnlyCsPerMin }
                ),
                totalCsPerMinWithCamps = buildDistribution(
                    metricKey = "totalCsPerMinWithCamps",
                    unit = "total_cs_per_min_including_camps",
                    includesJungleCamps = true,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.totalCsPerMinWithCamps }
                ),
                timelineTotalCsAt10WithCamps = buildDistribution(
                    metricKey = "timelineTotalCsAt10WithCamps",
                    unit = "total_cs_at_10m_including_camps",
                    includesJungleCamps = true,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.timelineTotalCsAt10WithCamps }
                ),
                goldPerMin = buildDistribution(
                    metricKey = "goldPerMin",
                    unit = "gold_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.goldPerMin }
                ),
                damagePerMin = buildDistribution(
                    metricKey = "damagePerMin",
                    unit = "damage_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.damagePerMin }
                ),
                visionScorePerMin = buildDistribution(
                    metricKey = "visionScorePerMin",
                    unit = "vision_score_per_min",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.visionScorePerMin }
                ),
                killParticipation = buildDistribution(
                    metricKey = "killParticipation",
                    unit = "ratio_0_to_1",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.killParticipation }
                ),
                deathsPerGame = buildDistribution(
                    metricKey = "deathsPerGame",
                    unit = "deaths",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.deathsPerGame }
                ),
                timeSpentDeadShare = buildDistribution(
                    metricKey = "timeSpentDeadShare",
                    unit = "ratio_0_to_1",
                    includesJungleCamps = false,
                    theoreticalSupplyCeiling = null,
                    samples = retainedSamples,
                    extractor = { it.timeSpentDeadShare }
                )
            )

            computedCells.add(
                CohortBenchmarkCellDto(
                    region = normalizedRegion,
                    patch = effectivePatchLabel,
                    queueId = SOLO_QUEUE_ID,
                    tier = tier,
                    role = role,
                    distinctPlayers = distinctPlayersCount,
                    eligibleGames = eligibleGamesCount,
                    maxGamesPerPlayerCap = effectiveCap,
                    excludedOverPlayerCapGames = cellExcludedOverCap,
                    passesQualityGate = passesGate,
                    colorBandsEnabled = passesGate,
                    qualityGateStatus = gateStatus,
                    earliestRankSnapshotAt = earliestSnapshot,
                    latestRankSnapshotAt = latestSnapshot,
                    computedAt = scannedAt,
                    metrics = metricsBundle
                )
            )
        }

        // 4. Persist compact de-identified cells to PostgreSQL/JPA and Redis
        if (resolvedPatch != null && computedCells.isNotEmpty()) {
            persistDeidentifiedCells(normalizedRegion, resolvedPatch, computedCells)
        }

        val inventory = CohortCacheInventoryDto(
            region = normalizedRegion,
            scannedAt = scannedAt,
            cachedProfileCount = profileKeys.size,
            profilesWithSoloQueueRankSnapshot = profilesWithSoloQueueRank,
            cachedChallengerLeaderboardPlayers = challengerLeaderboardPlayers,
            totalDistinctRankedPlayersInCache = rankSnapshotsByPuuid.size,
            ranksByTier = ranksByTier,
            cachedMatchCount = cachedMatches.size,
            cachedTimelineCount = timelinesByMatchId.size,
            matchesByPatch = matchesByPatchCounts.toSortedMap(patchComparator.reversed()),
            selectedTargetPatch = resolvedPatch,
            soloQueue420MatchesOnTargetPatch = soloQueue420MatchesOnTargetPatch,
            excludedNonSoloQueueMatches = excludedNonSoloQueueMatches,
            excludedMissingGameVersionMatches = excludedMissingGameVersionMatches,
            excludedOtherPatchMatches = excludedOtherPatchMatches,
            excludedShortOrRemakeMatches = excludedShortOrRemakeMatches,
            topMidBotParticipantCandidates = topMidBotParticipantCandidates,
            excludedOtherRoleParticipants = excludedOtherRoleParticipants,
            participantsWithRankSnapshot = participantsWithRankSnapshot,
            participantsWithoutRankSnapshot = participantsWithoutRankSnapshot,
            deduplicatedParticipantEntries = deduplicatedParticipantEntries,
            excludedOverPlayerCapSamples = totalExcludedOverPlayerCap,
            eligibleCohortSamples = totalEligibleSamples,
            distinctEligibleCohortPlayers = distinctEligiblePlayers.size,
            cellsTotal = computedCells.size,
            cellsPassingQualityGate = computedCells.count { it.passesQualityGate }
        )

        val rateLimitVerification = riotApiRateLimiter.getVerificationSnapshot(
            region = normalizedRegion,
            networkCollectionEnabled = networkCollectionEnabled
        )

        val methodologyNotes = listOf(
            "Cache-only extraction scans Redis keys for region '$normalizedRegion' with zero Riot API requests.",
            "Only Ranked Solo (queueId=420) matches on current patch '${resolvedPatch ?: "NONE"}' with duration >= 600s and roles TOP, MIDDLE, BOTTOM are included.",
            "Rank assignment strictly requires a timestamped current-rank snapshot in cache (player profile lastRefreshed or Challenger leaderboard lastRefreshed); historical rank is never invented or inferred.",
            "Samples are deduplicated by (matchId, puuid) and capped at maxGamesPerPlayer=$effectiveCap newest games per player per (tier, role) cell.",
            "CS metrics are strictly separated: laneCsAt10 and laneCsCapturePctAt10 use lane-only minions (ceiling=$theoreticalLaneCeiling for patch ${resolvedPatch ?: "default"}), while totalCsPerMinWithCamps and timelineTotalCsAt10WithCamps include jungle camps as separate pro-compatible metrics.",
            "Quality gate requires >= ${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE} distinct players AND >= ${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE} games per cell before passesQualityGate / colorBandsEnabled becomes true."
        )

        return CohortExtractionReportDto(
            inventory = inventory,
            rateLimitVerification = rateLimitVerification,
            cells = computedCells,
            methodologyNotes = methodologyNotes
        )
    }

    /**
     * Retrieves persisted de-identified cohort benchmark cells for [region] and optional [patch].
     */
    fun getPersistedCohortCells(region: String, patch: String? = null): List<CohortBenchmarkCellDto> {
        val normalizedRegion = region.lowercase().trim()
        if (!patch.isNullOrBlank()) {
            val cacheKey = benchmarkCacheKey(normalizedRegion, patch.trim())
            val cached = redisCacheService.get(cacheKey, Array<CohortBenchmarkCellDto>::class.java)
            if (cached != null && cached.isNotEmpty()) {
                return cached.toList()
            }
            val entities = cohortBenchmarkCellRepository.findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(
                region = normalizedRegion,
                patch = patch.trim(),
                queueId = SOLO_QUEUE_ID
            )
            return entities.mapNotNull { entityToDto(it) }
        }

        val allForRegion = cohortBenchmarkCellRepository.findByRegionOrderByPatchDescTierAscRoleAsc(normalizedRegion)
        if (allForRegion.isEmpty()) return emptyList()
        val latestPatch = allForRegion.map { it.patch }.maxWithOrNull(patchComparator) ?: allForRegion.first().patch
        return allForRegion.filter { it.patch == latestPatch }.mapNotNull { entityToDto(it) }
    }

    /**
     * Guarded, low-priority network cohort collection step.
     *
     * Refuses to execute unless:
     * 1. `cohort.network-collection.enabled=true` is explicitly configured, AND
     * 2. Actual Riot `X-App-Rate-Limit` and `X-Method-Rate-Limit` headers have been verified on [region]
     *    with sufficient headroom, no active `429 Retry-After` window, and no active `HIGH`-priority user requests.
     */
    suspend fun runGuardedLowPriorityCollectionStep(
        region: String = "euw1",
        candidatePuuids: List<String>,
        maxNewMatchesPerPlayer: Int = 3
    ): Map<String, Any> {
        val normalizedRegion = region.lowercase().trim()
        val verification = riotApiRateLimiter.getVerificationSnapshot(
            region = normalizedRegion,
            networkCollectionEnabled = networkCollectionEnabled
        )

        if (!networkCollectionEnabled) {
            return mapOf(
                "executed" to false,
                "reason" to "NETWORK_COLLECTION_DISABLED_BY_CONFIG",
                "region" to normalizedRegion,
                "fetchedMatches" to 0
            )
        }

        if (!verification.readyForLowPriorityNetworkCollection) {
            return mapOf(
                "executed" to false,
                "reason" to "RATE_LIMIT_VERIFICATION_OR_HEADROOM_NOT_READY",
                "region" to normalizedRegion,
                "appLimitsVerifiedFromHeaders" to verification.appLimitsVerifiedFromHeaders,
                "appHasHeadroomForLowPriority" to verification.appHasHeadroomForLowPriority,
                "activeUserRequests" to verification.activeUserRequests,
                "retryAfterUntil" to (verification.retryAfterUntil?.toString() ?: "NONE"),
                "fetchedMatches" to 0
            )
        }

        var fetchedMatches = 0
        for (puuid in candidatePuuids.distinct()) {
            riotApiRateLimiter.awaitLowPriorityClearance(normalizedRegion)
            val nowVerify = riotApiRateLimiter.getVerificationSnapshot(normalizedRegion, networkCollectionEnabled)
            if (!nowVerify.appHasHeadroomForLowPriority) {
                logger.info("Low-priority cohort worker pausing on {}: headroom below threshold", normalizedRegion)
                break
            }

            val matchIds = riotApiService.getMatchIdsByPuuidLowPriority(
                puuid = puuid,
                region = normalizedRegion,
                queueId = SOLO_QUEUE_ID,
                count = maxNewMatchesPerPlayer.coerceIn(1, 20),
                start = 0
            ).orEmpty()

            for (matchId in matchIds.take(maxNewMatchesPerPlayer.coerceAtLeast(1))) {
                val matchKey = "match:$normalizedRegion:$matchId"
                if (redisCacheService.get(matchKey, MatchDto::class.java) != null) {
                    continue
                }
                riotApiRateLimiter.awaitLowPriorityClearance(normalizedRegion)
                val match = riotApiService.getMatchByIdLowPriority(matchId, normalizedRegion) ?: continue
                redisCacheService.set(matchKey, match, MATCH_CACHE_TTL)
                fetchedMatches++
            }
        }

        return mapOf(
            "executed" to true,
            "reason" to "COMPLETED_LOW_PRIORITY_STEP",
            "region" to normalizedRegion,
            "fetchedMatches" to fetchedMatches
        )
    }

    private fun persistDeidentifiedCells(
        region: String,
        patch: String,
        cells: List<CohortBenchmarkCellDto>
    ) {
        try {
            val cacheKey = benchmarkCacheKey(region, patch)
            redisCacheService.set(cacheKey, cells, BENCHMARK_CACHE_TTL)
        } catch (e: Exception) {
            logger.warn("Failed to cache cohort benchmark cells in Redis for {} {}: {}", region, patch, e.message)
        }

        try {
            for (cell in cells) {
                val metricsJson = objectMapper.writeValueAsString(cell.metrics)
                val existing = cohortBenchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                    region = cell.region,
                    patch = cell.patch,
                    queueId = cell.queueId,
                    tier = cell.tier,
                    role = cell.role
                )
                if (existing != null) {
                    existing.distinctPlayers = cell.distinctPlayers
                    existing.eligibleGames = cell.eligibleGames
                    existing.maxGamesPerPlayerCap = cell.maxGamesPerPlayerCap
                    existing.excludedOverPlayerCapGames = cell.excludedOverPlayerCapGames
                    existing.passesQualityGate = cell.passesQualityGate
                    existing.colorBandsEnabled = cell.colorBandsEnabled
                    existing.earliestRankSnapshotAt = cell.earliestRankSnapshotAt
                    existing.latestRankSnapshotAt = cell.latestRankSnapshotAt
                    existing.computedAt = cell.computedAt
                    existing.metricsJson = metricsJson
                    cohortBenchmarkCellRepository.save(existing)
                } else {
                    cohortBenchmarkCellRepository.save(
                        CohortBenchmarkCellEntity(
                            region = cell.region,
                            patch = cell.patch,
                            queueId = cell.queueId,
                            tier = cell.tier,
                            role = cell.role,
                            distinctPlayers = cell.distinctPlayers,
                            eligibleGames = cell.eligibleGames,
                            maxGamesPerPlayerCap = cell.maxGamesPerPlayerCap,
                            excludedOverPlayerCapGames = cell.excludedOverPlayerCapGames,
                            passesQualityGate = cell.passesQualityGate,
                            colorBandsEnabled = cell.colorBandsEnabled,
                            earliestRankSnapshotAt = cell.earliestRankSnapshotAt,
                            latestRankSnapshotAt = cell.latestRankSnapshotAt,
                            computedAt = cell.computedAt,
                            metricsJson = metricsJson
                        )
                    )
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to persist cohort benchmark cells in DB for {} {}: {}", region, patch, e.message)
        }
    }

    private fun entityToDto(entity: CohortBenchmarkCellEntity): CohortBenchmarkCellDto? {
        return try {
            val metrics = objectMapper.readValue(entity.metricsJson, CohortMetricsBundleDto::class.java)
            val gateStatus = if (entity.passesQualityGate) {
                "READY_FOR_COLOR_BANDS"
            } else {
                "INSUFFICIENT_SAMPLE (players=${entity.distinctPlayers}/${CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE}, games=${entity.eligibleGames}/${CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE})"
            }
            CohortBenchmarkCellDto(
                region = entity.region,
                patch = entity.patch,
                queueId = entity.queueId,
                tier = entity.tier,
                role = entity.role,
                distinctPlayers = entity.distinctPlayers,
                eligibleGames = entity.eligibleGames,
                maxGamesPerPlayerCap = entity.maxGamesPerPlayerCap,
                excludedOverPlayerCapGames = entity.excludedOverPlayerCapGames,
                passesQualityGate = entity.passesQualityGate,
                colorBandsEnabled = entity.colorBandsEnabled,
                qualityGateStatus = gateStatus,
                earliestRankSnapshotAt = entity.earliestRankSnapshotAt,
                latestRankSnapshotAt = entity.latestRankSnapshotAt,
                computedAt = entity.computedAt,
                metrics = metrics
            )
        } catch (e: Exception) {
            logger.warn("Failed to deserialize CohortBenchmarkCellEntity {}: {}", entity.id, e.message)
            null
        }
    }

    private fun buildDistribution(
        metricKey: String,
        unit: String,
        includesJungleCamps: Boolean,
        theoreticalSupplyCeiling: Int?,
        samples: List<ParticipantSample>,
        extractor: (ParticipantSample) -> Double?
    ): CohortPercentileDistributionDto {
        val pairs = samples.mapNotNull { sample ->
            val v = extractor(sample)
            if (v != null && v.isFinite()) sample.puuid to v else null
        }
        if (pairs.isEmpty()) {
            return CohortPercentileDistributionDto(
                metricKey = metricKey,
                unit = unit,
                includesJungleCamps = includesJungleCamps,
                theoreticalSupplyCeiling = theoreticalSupplyCeiling,
                sampleGames = 0,
                distinctPlayers = 0,
                p25 = null,
                median = null,
                p75 = null,
                iqr = null
            )
        }

        val sortedValues = pairs.map { it.second }.sorted()
        val distinctPlayers = pairs.map { it.first }.toSet().size
        val rawP25 = quantile(sortedValues, 0.25)
        val rawMedian = quantile(sortedValues, 0.50)
        val rawP75 = quantile(sortedValues, 0.75)

        return CohortPercentileDistributionDto(
            metricKey = metricKey,
            unit = unit,
            includesJungleCamps = includesJungleCamps,
            theoreticalSupplyCeiling = theoreticalSupplyCeiling,
            sampleGames = sortedValues.size,
            distinctPlayers = distinctPlayers,
            p25 = round2(rawP25),
            median = round2(rawMedian),
            p75 = round2(rawP75),
            iqr = round2(rawP75 - rawP25)
        )
    }

    internal fun quantile(sortedValues: List<Double>, p: Double): Double {
        require(sortedValues.isNotEmpty()) { "sortedValues must not be empty" }
        if (sortedValues.size == 1) return sortedValues.first()
        val clampedP = p.coerceIn(0.0, 1.0)
        val pos = clampedP * (sortedValues.size - 1)
        val lower = floor(pos).toInt()
        val upper = ceil(pos).toInt()
        val weight = pos - lower
        return sortedValues[lower] * (1.0 - weight) + sortedValues[upper] * weight
    }

    private fun round2(value: Double): Double = (value * 100.0).roundToLong() / 100.0

    internal fun extractMajorMinorPatch(gameVersion: String?): String? {
        if (gameVersion.isNullOrBlank()) return null
        val parts = gameVersion.trim().split(".")
        if (parts.size < 2) return null
        val major = parts[0].toIntOrNull() ?: return null
        val minor = parts[1].toIntOrNull() ?: return null
        return "$major.$minor"
    }

    /**
     * Theoretical lane-minion spawn supply in the first 10:00 of Summoner's Rift:
     * - Season 2026 / Patch 16.1+ (or 26.1+): 120 lane minions
     * - Pre-16.1 patches: 114 lane minions
     */
    internal fun theoreticalLaneMinionSupplyAt10(patch: String?): Int {
        val major = patch?.substringBefore(".")?.toIntOrNull() ?: 16
        return if (major >= 16) 120 else 114
    }

    private fun resolveNormalizedRole(participant: ParticipantDto): String {
        val raw = participant.teamPosition?.takeIf { it.isNotBlank() }
            ?: participant.individualPosition?.takeIf { it.isNotBlank() }
            ?: ""
        return raw.uppercase().trim()
    }

    private fun findFrameAt10Minutes(timeline: MatchTimelineDto?): TimelineFrameDto? {
        val frames = timeline?.info?.frames ?: return null
        return frames.filter { frame ->
            val ts = frame.timestamp ?: return@filter false
            ts in 570_000L..630_000L
        }.minByOrNull { frame ->
            kotlin.math.abs((frame.timestamp ?: 0L) - 600_000L)
        }
    }

    private fun findParticipantFrame(
        frame: TimelineFrameDto?,
        participant: ParticipantDto,
        timeline: MatchTimelineDto?
    ): ParticipantFrameDto? {
        if (frame == null) return null
        val participantFrames = frame.participantFrames ?: return null
        val pid = participant.participantId
            ?: timeline?.info?.participants?.find { it.puuid == participant.puuid }?.participantId
            ?: return null
        return participantFrames[pid.toString()]
            ?: participantFrames.values.find { it.participantId == pid }
    }

    private fun benchmarkCacheKey(region: String, patch: String): String =
        "cohort:benchmarks:${region.lowercase()}:$patch"

    private fun tierIndex(tier: String): Int {
        val idx = TIER_ORDER.indexOf(tier.uppercase())
        return if (idx >= 0) idx else TIER_ORDER.size
    }

    private fun roleIndex(role: String): Int = when (role.uppercase()) {
        "TOP" -> 0
        "MIDDLE" -> 1
        "BOTTOM" -> 2
        else -> 99
    }

    private val patchComparator = Comparator<String> { a, b ->
        val aParts = a.split(".").map { it.toIntOrNull() ?: 0 }
        val bParts = b.split(".").map { it.toIntOrNull() ?: 0 }
        val majorCmp = (aParts.getOrElse(0) { 0 }).compareTo(bParts.getOrElse(0) { 0 })
        if (majorCmp != 0) majorCmp else (aParts.getOrElse(1) { 0 }).compareTo(bParts.getOrElse(1) { 0 })
    }
}
