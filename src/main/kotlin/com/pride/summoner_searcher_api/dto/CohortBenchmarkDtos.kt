package com.pride.summoner_searcher_api.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant

/**
 * De-identified percentile distribution (P25, Median/P50, P75, IQR) for a single metric
 * within a (region, patch, queueId=420, tier, role) cohort cell.
 *
 * Strictly separates lane-only CS metrics ([includesJungleCamps] = false, matching the 114/120
 * theoretical lane minion spawn ceiling) from total CS metrics ([includesJungleCamps] = true,
 * compatible with pro total-CS benchmarks where jungle camps are included). The two are never mixed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortPercentileDistributionDto(
    val metricKey: String,
    val unit: String,
    val includesJungleCamps: Boolean = false,
    val theoreticalSupplyCeiling: Int? = null,
    val sampleGames: Int = 0,
    val distinctPlayers: Int = 0,
    val p25: Double? = null,
    val median: Double? = null,
    val p75: Double? = null,
    val iqr: Double? = null
)

/**
 * Bundle of separated cohort metrics for a (tier, role) cell.
 *
 * - [laneCsAt10]: Primary CS@10 metric in RAW lane minions (`challenges.laneMinionsFirst10Minutes`
 *   or timeline 10m `minionsKilled`), never percentages and never mixed with jungle camps.
 *   Carries [CohortPercentileDistributionDto.theoreticalSupplyCeiling] (`114` pre-16.1, `120` for 16.1+/26.1+)
 *   as the lane-only theoretical spawn cutoff, while bands (`p25`, `median`, `p75`) are chosen purely from
 *   sufficient actual `(tier, role)` data (never fixed guesses).
 * - [laneOnlyCsPerMin]: End-of-game lane minions per minute (`totalMinionsKilled / durationMin`).
 * - [totalCsPerMinWithCamps] & [timelineTotalCsAt10WithCamps]: Separate total-CS reference metrics that include
 *   `neutralMinionsKilled` / `jungleMinionsKilled` (`includesJungleCamps = true`) for pro total-CS comparison.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortMetricsBundleDto(
    val laneCsAt10: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "laneCsAt10",
        unit = "lane_cs",
        includesJungleCamps = false
    ),
    val laneOnlyCsPerMin: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "laneOnlyCsPerMin",
        unit = "lane_cs_per_min",
        includesJungleCamps = false
    ),
    val totalCsPerMinWithCamps: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "totalCsPerMinWithCamps",
        unit = "total_cs_per_min_including_camps",
        includesJungleCamps = true
    ),
    val timelineTotalCsAt10WithCamps: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "timelineTotalCsAt10WithCamps",
        unit = "total_cs_at_10m_including_camps",
        includesJungleCamps = true
    ),
    val goldPerMin: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "goldPerMin",
        unit = "gold_per_min",
        includesJungleCamps = false
    ),
    val damagePerMin: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "damagePerMin",
        unit = "damage_per_min",
        includesJungleCamps = false
    ),
    val visionScorePerMin: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "visionScorePerMin",
        unit = "vision_score_per_min",
        includesJungleCamps = false
    ),
    val killParticipation: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "killParticipation",
        unit = "ratio_0_to_1",
        includesJungleCamps = false
    ),
    val deathsPerGame: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "deathsPerGame",
        unit = "deaths",
        includesJungleCamps = false
    ),
    val timeSpentDeadShare: CohortPercentileDistributionDto = CohortPercentileDistributionDto(
        metricKey = "timeSpentDeadShare",
        unit = "ratio_0_to_1",
        includesJungleCamps = false
    )
)

/**
 * Compact, de-identified cohort benchmark cell for a single (region, patch, queueId=420, tier, role).
 * Contains zero PUUIDs, summoner names, or raw match IDs.
 *
 * Enforces the quality gate: [passesQualityGate] and [colorBandsEnabled] are `true` ONLY when
 * [distinctPlayers] >= [minPlayersRequired] (50) AND [eligibleGames] >= [minGamesRequired] (200).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortBenchmarkCellDto(
    val region: String,
    val patch: String,
    val patchSelectionSource: String = "LATEST_CACHED_PATCH_IN_REDIS",
    val queueId: Int = 420,
    val tier: String,
    val role: String,
    val distinctPlayers: Int,
    val eligibleGames: Int,
    val maxGamesPerPlayerCap: Int,
    val excludedOverPlayerCapGames: Int,
    val minPlayersRequired: Int = MIN_PLAYERS_FOR_QUALITY_GATE,
    val minGamesRequired: Int = MIN_GAMES_FOR_QUALITY_GATE,
    val passesQualityGate: Boolean,
    val colorBandsEnabled: Boolean,
    val qualityGateStatus: String,
    val earliestRankSnapshotAt: Instant? = null,
    val latestRankSnapshotAt: Instant? = null,
    val computedAt: Instant,
    val metrics: CohortMetricsBundleDto
) {
    companion object {
        const val MIN_PLAYERS_FOR_QUALITY_GATE = 50
        const val MIN_GAMES_FOR_QUALITY_GATE = 200
        const val DEFAULT_MAX_GAMES_PER_PLAYER = 10
    }
}

/**
 * Cache-only inventory and coverage report for a region.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortCacheInventoryDto(
    val region: String,
    val scannedAt: Instant,
    val scanKeyCapPerNamespace: Int = 2_000,
    val maxRankSnapshotAgeHours: Long = 168,
    val maxMatchAgeHours: Long = 336,
    val maxMatchToSnapshotSkewHours: Long = 168,
    val cachedProfileCount: Int,
    val profilesWithSoloQueueRankSnapshot: Int,
    val excludedStaleRankSnapshots: Int = 0,
    val cachedChallengerLeaderboardPlayers: Int,
    val totalDistinctRankedPlayersInCache: Int,
    val ranksByTier: Map<String, Int>,
    val cachedMatchCount: Int,
    val cachedTimelineCount: Int,
    val matchesByPatch: Map<String, Int>,
    val latestCachedPatch: String? = null,
    val selectedTargetPatch: String?,
    val patchSelectionSource: String = "LATEST_CACHED_PATCH_IN_REDIS",
    val soloQueue420MatchesOnTargetPatch: Int,
    val excludedNonSoloQueueMatches: Int,
    val excludedMissingGameVersionMatches: Int,
    val excludedOtherPatchMatches: Int,
    val excludedShortOrRemakeMatches: Int,
    val excludedStaleOrUnboundedMatches: Int = 0,
    val topMidBotParticipantCandidates: Int,
    val excludedOtherRoleParticipants: Int,
    val participantsWithRankSnapshot: Int,
    val participantsWithoutRankSnapshot: Int,
    val excludedParticipantsOutsideSnapshotWindow: Int = 0,
    val deduplicatedParticipantEntries: Int,
    val excludedOverPlayerCapSamples: Int,
    val eligibleCohortSamples: Int,
    val distinctEligibleCohortPlayers: Int,
    val cellsTotal: Int,
    val cellsPassingQualityGate: Int
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class MethodRateLimitSnapshotDto(
    val endpointKey: String,
    val methodRateLimitHeader: String?,
    val methodRateLimitCountHeader: String?,
    val activeBuckets: List<String>,
    val hasHeadroomForLowPriority: Boolean,
    val lastUpdatedAt: Instant?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RateLimitVerificationSnapshotDto(
    val region: String,
    val appLimitsVerifiedFromHeaders: Boolean,
    val appRateLimitHeader: String?,
    val appRateLimitCountHeader: String?,
    val activeAppBuckets: List<String>,
    val appHasHeadroomForLowPriority: Boolean,
    val methodLimitsByEndpoint: Map<String, MethodRateLimitSnapshotDto>,
    val verifiedMethodEndpoints: Set<String> = emptySet(),
    val missingRequiredMethodEndpoints: Set<String> = emptySet(),
    val lastRateLimitType: String? = null,
    val serviceRateLimitActive: Boolean = false,
    val foregroundYieldActive: Boolean = false,
    val lowPriorityCapRatio: Double,
    val userRequestYieldWindowMs: Long,
    val activeUserRequests: Int,
    val lastHighPriorityRequestAt: Instant?,
    val retryAfterUntil: Instant?,
    val networkCollectionEnabled: Boolean,
    val readyForLowPriorityNetworkCollection: Boolean
)

/**
 * Compact timestamped Solo Queue current-rank snapshot cached at `cohort:rank:snapshot:{region}:{puuid}`.
 * Never fabricates historical rank; carries explicit [snapshotAt] enforced against freshness & match-proximity bounds.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortRankSnapshotDto(
    val puuid: String,
    val region: String,
    val tier: String,
    val rank: String? = null,
    val leaguePoints: Int? = null,
    val queueType: String = "RANKED_SOLO_5x5",
    val snapshotAt: Instant
)

/**
 * Per-(tier, role) cell progress toward the quality gate (`50` distinct players + `200` eligible games).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortCellProgressDto(
    val tier: String,
    val role: String,
    val distinctPlayers: Int,
    val eligibleGames: Int,
    val targetPlayers: Int = CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE,
    val targetGames: Int = CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE,
    val passesQualityGate: Boolean,
    val deficitPlayers: Int,
    val deficitGames: Int
)

/**
 * Resumable, checkpointed state for the LOW-priority cohort collector (`cohort:collector:checkpoint:{region}:{patch}`).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortCollectorCheckpointDto(
    val region: String,
    val targetPatch: String?,
    val patchSelectionSource: String,
    val queueId: Int = 420,
    val roles: List<String> = listOf("TOP", "MIDDLE", "BOTTOM"),
    val maxGamesPerPlayerCap: Int = CohortBenchmarkCellDto.DEFAULT_MAX_GAMES_PER_PLAYER,
    val targetPlayersPerCell: Int = CohortBenchmarkCellDto.MIN_PLAYERS_FOR_QUALITY_GATE,
    val targetGamesPerCell: Int = CohortBenchmarkCellDto.MIN_GAMES_FOR_QUALITY_GATE,
    val processedPuuids: Set<String> = emptySet(),
    val pendingCandidatePuuids: List<String> = emptyList(),
    val processedMatchIds: Set<String> = emptySet(),
    val playerCellGameCounts: Map<String, Int> = emptyMap(),
    val cellProgress: Map<String, CohortCellProgressDto> = emptyMap(),
    val cellsPassingQualityGate: Int = 0,
    val targetCellsCount: Int = 30,
    val totalNetworkCallsMade: Int = 0,
    val fetchedRankSnapshots: Int = 0,
    val fetchedMatchIdLists: Int = 0,
    val fetchedMatches: Int = 0,
    val yieldedToForegroundCount: Int = 0,
    val pausedForRetryAfterCount: Int = 0,
    val pausedForHeadroomCount: Int = 0,
    val lastStopReason: String = "INITIALIZED",
    val startedAt: Instant,
    val updatedAt: Instant
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortExtractionReportDto(
    val reportSource: String = "FRESH_BOUNDED_EXTRACTION",
    val inventory: CohortCacheInventoryDto,
    val rateLimitVerification: RateLimitVerificationSnapshotDto,
    val cells: List<CohortBenchmarkCellDto>,
    val methodologyNotes: List<String>
)

