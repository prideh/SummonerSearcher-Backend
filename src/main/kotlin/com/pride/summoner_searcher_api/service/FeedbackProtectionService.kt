package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
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
    private val feedbackRepository: AiFeedbackRepository
) {
    
    /**
     * Calculate abuse score (0-1, higher = more suspicious)
     * Multiple detection layers prevent gaming the system
     */
    fun calculateAbuseScore(userId: Long?, feedbackType: String, engagementTimeMs: Long?): BigDecimal {
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
