package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Durable Postgres persistence entity for de-identified per-cell sample accumulation
 * on `(region, patch, queue_id, tier, role)`.
 *
 * Allows the LOW-priority cohort collector and bounded report extractor to accumulate
 * de-identified participant game samples durably in Postgres as matches are processed,
 * deduplicated by `matchIdHash` and capped at `maxGamesPerPlayerCap` (10) games per
 * de-identified `playerHash` per cell — completely independent of Redis `allkeys-lru`
 * eviction of `match:*` or `cohort:rank:snapshot:*` keys.
 *
 * Contains zero raw PUUIDs, summoner names, or raw match IDs (only one-way SHA-256
 * truncated hashes `playerHash` and `matchIdHash` plus numeric metric values).
 *
 * Schema note: Managed automatically on startup via `spring.jpa.hibernate.ddl-auto=update`
 * (`cohort_cell_sample_states` table).
 */
@Entity
@Table(
    name = "cohort_cell_sample_states",
    uniqueConstraints = [
        UniqueConstraint(
            name = "uk_cohort_cell_sample_state_region_patch_queue_tier_role",
            columnNames = ["region", "patch", "queue_id", "tier", "role"]
        )
    ]
)
class CohortCellSampleStateEntity(
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

    @Column(name = "distinct_players", nullable = false)
    var distinctPlayers: Int = 0,

    @Column(name = "eligible_games", nullable = false)
    var eligibleGames: Int = 0,

    @Column(name = "excluded_over_player_cap_games", nullable = false)
    var excludedOverPlayerCapGames: Int = 0,

    @Column(name = "passes_quality_gate", nullable = false)
    var passesQualityGate: Boolean = false,

    @Column(name = "samples_json", nullable = false, columnDefinition = "TEXT")
    var samplesJson: String = "{}",

    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.now()
)
