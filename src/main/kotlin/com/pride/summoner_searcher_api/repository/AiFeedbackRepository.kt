package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.AiFeedback
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.Optional
import java.util.UUID

@Repository
interface AiFeedbackRepository : JpaRepository<AiFeedback, UUID> {
    
    /** Find feedback for a specific interaction */
    fun findByInteractionId(interactionId: UUID): Optional<AiFeedback>
    
    /** Find user's recent feedback for pattern analysis */
    fun findByUserIdOrderByCreatedAtDesc(userId: Long?, pageable: Pageable): Page<AiFeedback>
    
    /** Count recent feedback from a user (for rate limiting) */
    fun countByUserIdAndCreatedAtAfter(userId: Long?, after: Instant): Long
    
    /** Find validated feedback by type */
    fun findByIsValidatedTrueAndFeedbackType(feedbackType: String): List<AiFeedback>
    
    /** Count validated feedback */
    fun countByIsValidatedTrue(): Long
    
    /**
     * Find negative feedback rate per question category over a time window.
     * Used by ExampleCurationService to detect underperforming categories.
     * Returns list of [summonerRole (used as category), negativeCount, totalCount].
     */
    @Query("""
        SELECT ai.summonerRole, 
               SUM(CASE WHEN f.feedbackType = 'negative' THEN 1 ELSE 0 END),
               COUNT(f)
        FROM AiFeedback f
        INNER JOIN AiInteraction ai ON f.interaction.id = ai.id
        WHERE f.isValidated = true
        AND f.createdAt >= :since
        AND ai.summonerRole IS NOT NULL
        GROUP BY ai.summonerRole
        HAVING COUNT(f) >= :minInteractions
    """)
    fun findNegativeRateByCategory(
        @Param("since") since: Instant,
        @Param("minInteractions") minInteractions: Int
    ): List<Array<Any>>
}
