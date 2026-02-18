package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.AiInteraction
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

@Repository
interface AiInteractionRepository : JpaRepository<AiInteraction, UUID> {
    
    /** Find user's interaction history */
    fun findByUserIdOrderByCreatedAtDesc(userId: Long?, pageable: Pageable): Page<AiInteraction>
    
    /** Count interactions for a specific prompt version */
    fun countByPromptVersion(version: String): Long
    
    /** Find recent interactions with positive feedback for learning */
    @Query("""
        SELECT ai FROM AiInteraction ai 
        INNER JOIN AiFeedback f ON f.interaction.id = ai.id 
        WHERE f.isValidated = true 
        AND f.feedbackType = 'positive' 
        AND ai.createdAt >= :since
        ORDER BY f.createdAt DESC
    """)
    fun findCandidatesForLearning(since: Instant): List<AiInteraction>
    
    /**
     * Count total interactions by user (for abuse protection)
     */
    fun countByUserId(userId: Long): Long
    
    /**
     * Count interactions in a session (for session depth detection)
     */
    fun countByContextFingerprint(sessionId: String): Long
    
    /**
     * Get average engagement time per temperature for a given question category.
     * Used by weighted random temperature selection to bias toward winning temperatures.
     * Returns list of [temperature, avgEngagementMs] pairs.
     */
    @Query("""
        SELECT ai.temperature, AVG(f.engagementTimeMs)
        FROM AiInteraction ai
        INNER JOIN AiFeedback f ON f.interaction.id = ai.id
        WHERE f.feedbackType = 'positive'
        AND f.isValidated = true
        AND ai.temperature IS NOT NULL
        AND ai.summonerRole = :category
        GROUP BY ai.temperature
    """)
    fun findAvgQualityByTemperatureAndCategory(@Param("category") category: String): List<Array<Any>>
    
    /**
     * Find all interactions in a session ordered by time.
     * Used by PatternDiscoveryService to detect follow-up question sequences.
     */
    @Query("""
        SELECT ai FROM AiInteraction ai
        WHERE ai.contextFingerprint = :sessionId
        ORDER BY ai.createdAt ASC
    """)
    fun findBySessionId(@Param("sessionId") sessionId: String): List<AiInteraction>
    
    /**
     * Find all distinct session IDs that have 2+ interactions (multi-turn conversations).
     * Used by PatternDiscoveryService to find sessions worth analyzing.
     */
    @Query("""
        SELECT ai.contextFingerprint
        FROM AiInteraction ai
        WHERE ai.contextFingerprint IS NOT NULL
        AND ai.createdAt >= :since
        GROUP BY ai.contextFingerprint
        HAVING COUNT(ai) >= 2
    """)
    fun findMultiTurnSessionIds(@Param("since") since: Instant): List<String>
}
