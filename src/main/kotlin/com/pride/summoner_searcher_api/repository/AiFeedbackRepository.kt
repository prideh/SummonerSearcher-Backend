package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.AiFeedback
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
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
}
