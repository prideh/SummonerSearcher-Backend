package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.Feedback
import com.pride.summoner_searcher_api.repository.FeedbackRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.temporal.ChronoUnit

open class RateLimitException(message: String) : RuntimeException(message)

@Service
class FeedbackService(
    private val feedbackRepository: FeedbackRepository
) {
    // Allows 5 feedbacks per IP address per hour
    private val MAX_FEEDBACKS_PER_HOUR = 5L

    @Transactional
    fun submitFeedback(userId: Long?, ipAddress: String, type: String, content: String): Feedback {
        // Enforce basic content validation
        if (content.isBlank()) {
            throw IllegalArgumentException("Feedback content cannot be empty.")
        }
        if (content.length > 1000) {
            throw IllegalArgumentException("Feedback content cannot exceed 1000 characters.")
        }
        
        // Enforce Rate Limiting (Anti-Spam)
        val oneHourAgo = Instant.now().minus(1, ChronoUnit.HOURS)
        val recentFeedbackCount = feedbackRepository.countByIpAddressAndCreatedAtAfter(ipAddress, oneHourAgo)
        
        if (recentFeedbackCount >= MAX_FEEDBACKS_PER_HOUR) {
            throw RateLimitException("You have submitted too much feedback recently. Please try again later.")
        }

        val feedback = Feedback(
            userId = userId,
            ipAddress = ipAddress,
            type = type,
            content = content
        )

        return feedbackRepository.save(feedback)
    }
}
