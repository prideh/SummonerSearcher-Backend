package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.AiInteraction
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
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
}
