package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant
import java.util.UUID

/**
 * Stores every AI interaction for learning and analysis
 */
@Entity
@Table(name = "ai_interactions")
class AiInteraction(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,
    
    /** Reference to the user who initiated the interaction (nullable for guest users) */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    val user: User? = null,
    
    /** Summoner being analyzed */
    @Column(nullable = false)
    val summonerName: String,
    
    /** Rank context for later analysis */
    @Column(length = 50)
    val summonerRank: String? = null,
    
    /** Primary role for categorization */
    @Column(length = 20)
    val summonerRole: String? = null,
    
    /** User's question */
    @Column(nullable = false, columnDefinition = "TEXT")
    val userQuestion: String,
    
    /** AI's response */
    @Column(nullable = false, columnDefinition = "TEXT")
    val aiResponse: String,
    
    /** Fingerprint of context for caching (hash of key stats) */
    @Column(length = 255)
    val contextFingerprint: String? = null,
    
    /** Prompt version used (for A/B testing) */
    @Column(length = 50)
    val promptVersion: String = "v1.0",
    
    /** Response time in milliseconds */
    val responseTimeMs: Long? = null,
    
    /** Temperature used for this generation (for A/B learning) */
    val temperature: Double? = null,
    
    @Column(nullable = false)
    val createdAt: Instant = Instant.now()
)
