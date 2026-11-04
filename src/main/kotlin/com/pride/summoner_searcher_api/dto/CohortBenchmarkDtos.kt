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
    val cachedProfileCount: Int,
    val profilesWithSoloQueueRankSnapshot: Int,
    val cachedChallengerLeaderboardPlayers: Int,
    val totalDistinctRankedPlayersInCache: Int,
    val ranksByTier: Map<String, Int>,
    val cachedMatchCount: Int,
    val cachedTimelineCount: Int,
    val matchesByPatch: Map<String, Int>,
    val selectedTargetPatch: String?,
    val soloQueue420MatchesOnTargetPatch: Int,
    val excludedNonSoloQueueMatches: Int,
    val excludedMissingGameVersionMatches: Int,
    val excludedOtherPatchMatches: Int,
    val excludedShortOrRemakeMatches: Int,
    val topMidBotParticipantCandidates: Int,
    val excludedOtherRoleParticipants: Int,
    val participantsWithRankSnapshot: Int,
    val participantsWithoutRankSnapshot: Int,
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
    val lowPriorityCapRatio: Double,
    val userRequestYieldWindowMs: Long,
    val activeUserRequests: Int,
    val lastHighPriorityRequestAt: Instant?,
    val retryAfterUntil: Instant?,
    val networkCollectionEnabled: Boolean,
    val readyForLowPriorityNetworkCollection: Boolean
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CohortExtractionReportDto(
    val inventory: CohortCacheInventoryDto,
    val rateLimitVerification: RateLimitVerificationSnapshotDto,
    val cells: List<CohortBenchmarkCellDto>,
    val methodologyNotes: List<String>
)
