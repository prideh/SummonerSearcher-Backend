package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Entity tracking each automated example curation job run
 * Stores statistics about what happened during each daily curation
 */
@Entity
@Table(name = "curation_runs")
class CurationRun(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    /** When the curation job ran */
    @Column(nullable = false)
    val runDate: Instant = Instant.now(),

    /** Number of candidate interactions found for potential learning */
    @Column(nullable = false)
    val candidatesFound: Int = 0,

    /** Number of new examples added during this run */
    @Column(nullable = false)
    val examplesAdded: Int = 0,

    /** Number of existing examples replaced with better ones */
    @Column(nullable = false)
    val examplesReplaced: Int = 0,

    /** Average quality score of examples added/replaced (for analytics) */
    val avgQualityScore: Double? = null,

    /** Duration of the curation job in milliseconds */
    val durationMs: Long? = null
)
