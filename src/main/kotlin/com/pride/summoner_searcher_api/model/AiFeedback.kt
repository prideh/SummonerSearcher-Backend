package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

/**
 * Stores user feedback on AI responses
 */
@Entity
@Table(
    name = "ai_feedback",
    uniqueConstraints = [UniqueConstraint(columnNames = ["interaction_id", "user_id"])]
)
class AiFeedback(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,
    
    /** The interaction being rated */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "interaction_id", nullable = false)
    val interaction: AiInteraction,
    
    /** User who provided feedback (nullable for guest users) */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    val user: User? = null,
    
    /** Type of feedback */
    @Column(nullable = false, length = 20)
    val feedbackType: String, // "positive" or "negative"
    
    /** How long user viewed the response (milliseconds) */
    val engagementTimeMs: Long? = null,
    
    /** Whether this feedback passed abuse detection */
    @Column(nullable = false)
    var isValidated: Boolean = false,
    
    /** Abuse score 0-1, higher = more suspicious */
    @Column(precision = 3, scale = 2)
    var abuseScore: BigDecimal = BigDecimal.ZERO,
    
    @Column(nullable = false)
    val createdAt: Instant = Instant.now()
)
