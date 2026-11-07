package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Durable Postgres persistence entity for viewed-player current-year Ranked Solo (`queue_id = 420`)
 * additive statistics and match-ID completeness state on `(region, puuid, season_year, queue_id)`.
 *
 * Stores exact unrounded additive counters ([com.pride.summoner_searcher_api.service.DurablePlayerSeasonStatsDto])
 * along with the exact ordered set of folded `processedMatchIds`, unresolved `pendingMatchIds` gaps,
 * and `historyComplete` flag so incremental profile refreshes only fetch/decode new or pending match deltas
 * instead of re-streaming hundreds of cached matches from Redis or re-fetching expired (>60d) matches from Riot.
 */
@Entity
@Table(
    name = "player_season_stats",
    uniqueConstraints = [
        UniqueConstraint(
            name = "uk_player_season_stats_region_puuid_year_queue",
            columnNames = ["region", "puuid", "season_year", "queue_id"]
        )
    ]
)
class PlayerSeasonStatsEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    @Version
    @Column(name = "version", nullable = false)
    var version: Long = 0L,

    @Column(nullable = false, length = 16)
    val region: String = "",

    @Column(nullable = false, length = 128)
    val puuid: String = "",

    @Column(name = "season_year", nullable = false)
    val seasonYear: Int = 0,

    @Column(name = "queue_id", nullable = false)
    val queueId: Int = 420,

    @Column(name = "schema_version", nullable = false)
    var schemaVersion: Int = 1,

    @Column(name = "folded_games_count", nullable = false)
    var foldedGamesCount: Int = 0,

    @Column(name = "pending_games_count", nullable = false)
    var pendingGamesCount: Int = 0,

    @Column(name = "history_capped", nullable = false)
    var historyCapped: Boolean = false,

    @Column(name = "history_complete", nullable = false)
    var historyComplete: Boolean = false,

    @Column(name = "state_json", nullable = false, columnDefinition = "TEXT")
    var stateJson: String = "{}",

    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.now()
)
