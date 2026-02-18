package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

/**
 * Stores curated examples for few-shot learning
 */
@Entity
@Table(name = "ai_learned_examples")
class AiLearnedExample(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,
    
    /** Source interaction that this example came from */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "source_interaction_id")
    val sourceInteraction: AiInteraction? = null,
    
    /** Category for this question (for targeted few-shot learning) */
    @Column(length = 100)
    val questionCategory: String,
    
    /** The user question */
    @Column(nullable = false, columnDefinition = "TEXT")
    val userQuestion: String,
    
    /** The AI response */
    @Column(nullable = false, columnDefinition = "TEXT")
    val aiResponse: String,
    
    /** Quality score (0-100) calculated by curation service */
    @Column(precision = 5, scale = 2)
    var qualityScore: BigDecimal? = null,
    
    /** Rank context this example applies to */
    @Column(length = 50)
    val rankContext: String? = null,
    
    /** Whether this example is active (can be deactivated if becomes outdated) */
    @Column(nullable = false)
    var isActive: Boolean = true,
    
    /** Track how many times this example has been used in prompts */
    @Column(nullable = false)
    var timesUsed: Int = 0,
    
    /** Temperature that produced this example (for learning which temp works best) */
    val temperature: Double? = null,
    
    @Column(nullable = false)
    val createdAt: Instant = Instant.now()
)
