package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Compact, de-identified persistence entity for a single (region, patch, queueId, tier, role)
 * cohort benchmark cell. Contains only aggregated counts, quality-gate flags, rank-snapshot
 * timestamps, and serialized median/IQR metric distributions—never raw PUUIDs or summoner names.
 */
@Entity
@Table(
    name = "cohort_benchmark_cells",
    uniqueConstraints = [
        UniqueConstraint(
            name = "uk_cohort_cell_region_patch_queue_tier_role",
            columnNames = ["region", "patch", "queue_id", "tier", "role"]
        )
    ]
)
class CohortBenchmarkCellEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    @Column(nullable = false, length = 16)
    val region: String,

    @Column(nullable = false, length = 16)
    val patch: String,

    @Column(name = "queue_id", nullable = false)
    val queueId: Int = 420,

    @Column(nullable = false, length = 32)
    val tier: String,

    @Column(nullable = false, length = 16)
    val role: String,

    @Column(nullable = false)
    var distinctPlayers: Int = 0,

    @Column(nullable = false)
    var eligibleGames: Int = 0,

    @Column(nullable = false)
    var maxGamesPerPlayerCap: Int = 10,

    @Column(nullable = false)
    var excludedOverPlayerCapGames: Int = 0,

    @Column(nullable = false)
    var passesQualityGate: Boolean = false,

    @Column(nullable = false)
    var colorBandsEnabled: Boolean = false,

    @Column(nullable = true)
    var earliestRankSnapshotAt: Instant? = null,

    @Column(nullable = true)
    var latestRankSnapshotAt: Instant? = null,

    @Column(nullable = false)
    var computedAt: Instant = Instant.now(),

    @Column(nullable = false, columnDefinition = "TEXT")
    var metricsJson: String = "{}"
)
