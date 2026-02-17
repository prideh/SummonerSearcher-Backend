package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Service for detecting and preventing malicious feedback abuse
 */
@Service
class FeedbackProtectionService(
    private val feedbackRepository: AiFeedbackRepository,
    private val userRepository: UserRepository,
    private val interactionRepository: AiInteractionRepository
) {
    
    /**
     * Calculate abuse score (0-1, higher = more suspicious)
     * Multiple detection layers prevent gaming the system
     */
    fun calculateAbuseScore(
        userId: Long?, 
        feedbackType: String, 
        engagementTimeMs: Long?,
        sessionId: String?
    ): BigDecimal {
        var score = 0.0
        
        // Layer 1: Anonymous user penalty (can't track behavioral patterns)
        if (userId == null) {
            score += 0.3
        }
        
        // Layer 2: Instant negative feedback = suspicious (user didn't read response)
        if (feedbackType == "negative" && (engagementTimeMs ?: 0) < 2000) {
            score += 0.4 // Strong penalty for instant thumbs down
        }
        
        // Layer 3: User feedback pattern analysis (only for authenticated users)
        if (userId != null) {
            val recentFeedback = feedbackRepository.findByUserIdOrderByCreatedAtDesc(
                userId, 
                PageRequest.of(0, 10)
            ).content
            
            if (recentFeedback.isNotEmpty()) {
                val negativeCount = recentFeedback.count { it.feedbackType == "negative" }
                val negativeRatio = negativeCount.toDouble() / recentFeedback.size
                
                // Users who spam negative feedback are suspicious
                if (negativeRatio > 0.8) {
                    score += 0.5
                }
            }
        }
        
        // Layer 4: Rate limiting (bot-like rapid feedback)
        if (userId != null) {
            val recentCount = feedbackRepository.countByUserIdAndCreatedAtAfter(
                userId,
                Instant.now().minus(5, ChronoUnit.MINUTES)
            )
            
            if (recentCount > 10) {
                score += 0.4 // 10+ feedbacks in 5 minutes = bot behavior
            }
        }
        
        // Layer 5: Account age (new accounts are more suspicious)
        if (userId != null) {
            val user = userRepository.findById(userId).orElse(null)
            if (user != null && user.createdAt != null) {
                val accountAgeInDays = ChronoUnit.DAYS.between(user.createdAt, Instant.now())
                when {
                    accountAgeInDays < 7 -> score += 0.2 // Brand new account
                    accountAgeInDays > 90 -> score -= 0.1 // Established account (3+ months)
                }
            }
        }
        
        // Layer 6: Total interactions (experienced users are more trustworthy)
        if (userId != null) {
            val totalInteractions = interactionRepository.countByUserId(userId)
            when {
                totalInteractions > 20 -> score -= 0.15 // Very engaged user
                totalInteractions > 10 -> score -= 0.08 // Moderately engaged
            }
        }
        
        // Layer 7: Session depth (multi-turn conversations = genuine engagement)
        if (sessionId != null) {
            val sessionDepth = interactionRepository.countByContextFingerprint(sessionId)
            if (sessionDepth >= 3) {
                score -= 0.1 // Asked 3+ questions in this session
            }
        }
        
        return BigDecimal(score.coerceAtMost(1.0))
    }
    
    /**
     * Determine if feedback should be used for training
     * Only high-confidence feedback improves the AI
     */
    fun isValidForTraining(abuseScore: BigDecimal): Boolean {
        return abuseScore < BigDecimal("0.5")
    }
}
