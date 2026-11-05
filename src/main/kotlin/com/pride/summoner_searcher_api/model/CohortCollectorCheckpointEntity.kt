package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Durable Postgres persistence entity for a collector checkpoint on `(region, target_patch, queue_id)`.
 * Ensures collector progress, processed PUUIDs/match IDs, and cell counts survive Redis evictions,
 * TTL expirations, or container restarts.
 *
 * Schema note: Managed via `spring.jpa.hibernate.ddl-auto=update` (`cohort_collector_checkpoints` table).
 */
@Entity
@Table(
    name = "cohort_collector_checkpoints",
    uniqueConstraints = [
        UniqueConstraint(
            name = "uk_cohort_checkpoint_region_patch_queue",
            columnNames = ["region", "target_patch", "queue_id"]
        )
    ]
)
class CohortCollectorCheckpointEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    @Column(nullable = false, length = 16)
    val region: String,

    @Column(name = "target_patch", nullable = false, length = 16)
    val targetPatch: String,

    @Column(name = "queue_id", nullable = false)
    val queueId: Int = 420,

    @Column(name = "last_stop_reason", nullable = false, length = 64)
    var lastStopReason: String = "IDLE",

    @Column(name = "total_network_calls_made", nullable = false)
    var totalNetworkCallsMade: Int = 0,

    @Column(name = "processed_puuids_count", nullable = false)
    var processedPuuidsCount: Int = 0,

    @Column(name = "pending_candidates_count", nullable = false)
    var pendingCandidatesCount: Int = 0,

    @Column(name = "cells_passing_quality_gate", nullable = false)
    var cellsPassingQualityGate: Int = 0,

    @Column(name = "checkpoint_json", nullable = false, columnDefinition = "TEXT")
    var checkpointJson: String = "{}",

    @Column(name = "started_at", nullable = false)
    var startedAt: Instant = Instant.now(),

    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.now()
)
