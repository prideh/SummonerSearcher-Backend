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
 * ### Per-Cell Player Cap Justification (`MAX_PLAYERS_PER_DURABLE_CELL_STATE = 250`)
 * - The cell quality gate requires `50` distinct players and `200` eligible games per `(tier, role)` cell
 *   (with at most `10` games per player). Once all 3 roles of a tier pass `50 players / 200 games`,
 *   Step B of the collector already skips fetching new players for that tier.
 * - `250` distinct players per cell is **5x the 50-player quality gate** (and up to `2,500` games, or
 *   **12.5x the 200-game quality gate**), giving nonparametric quartile standard error on the median of
 *   $\approx 1.253\sigma / \sqrt{250} \approx 0.079\sigma$, while strictly bounding `samples_json` to
 *   $\le 2,500$ sample records (~820 KB max per cell) and keeping worst-case 30-cell in-memory state
 *   well within the 512 MB container's 78% / 64 MiB guard. Existing players among the 250 continue to
 *   refresh their 10 newest games on the patch.
 *
 * ### Migration & Schema Note (`spring.jpa.hibernate.ddl-auto=update`)
 * - On an existing production database (with existing `cohort_benchmark_cells` and `cohort_collector_checkpoints`
 *   tables), Hibernate's `SchemaUpdate` creates `cohort_cell_sample_states` and unique constraint
 *   `uk_cohort_cell_sample_state_region_patch_queue_tier_role` on first startup without altering or locking
 *   existing tables or dropping legacy rows.
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
