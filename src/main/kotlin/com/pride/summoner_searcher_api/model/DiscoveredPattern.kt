package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Stores behavioral patterns discovered from user interaction sequences.
 * Example: "After cs_farming answers, 68% of users ask about wave_management next"
 * These patterns are injected into the AI prompt as hints to pre-emptively address follow-ups.
 */
@Entity
@Table(name = "discovered_patterns")
class DiscoveredPattern(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,
    
    /** The question category that triggers this pattern */
    @Column(nullable = false, length = 100)
    val triggerCategory: String,
    
    /** The category users most commonly ask about next */
    @Column(nullable = false, length = 100)
    val followUpCategory: String,
    
    /** How many times this sequence was observed */
    @Column(nullable = false)
    var occurrenceCount: Int = 0,
    
    /** Percentage of sessions where this follow-up occurred (0.0 - 1.0) */
    @Column(nullable = false)
    var confidenceScore: Double = 0.0,
    
    /** Whether this pattern is currently being injected into prompts */
    @Column(nullable = false)
    var isActive: Boolean = true,
    
    /** When this pattern was last recalculated */
    @Column(nullable = false)
    var lastUpdated: Instant = Instant.now(),
    
    @Column(nullable = false)
    val createdAt: Instant = Instant.now()
)
