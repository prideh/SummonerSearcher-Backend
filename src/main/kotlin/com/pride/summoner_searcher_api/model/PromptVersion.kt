package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Tracks prompt performance over time for A/B testing
 */
@Entity
@Table(name = "prompt_versions")
class PromptVersion(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,
    
    /** Unique version identifier (e.g., "v1.0", "v1.1-rank-specific") */
    @Column(unique = true, nullable = false, length = 50)
    val versionName: String,
    
    /** The prompt template text */
    @Column(nullable = false, columnDefinition = "TEXT")
    val promptTemplate: String,
    
    /** Whether this version is currently active for new interactions */
    @Column(nullable = false)
    var isActive: Boolean = false,
    
    /** Total number of times this prompt was used */
    @Column(nullable = false)
    var totalUses: Int = 0,
    
    /** Count of positive feedback */
    @Column(nullable = false)
    var positiveFeedbackCount: Int = 0,
    
    /** Count of negative feedback */
    @Column(nullable = false)
    var negativeFeedbackCount: Int = 0,
    
    @Column(nullable = false)
    val createdAt: Instant = Instant.now()
)
