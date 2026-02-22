package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.Feedback
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Instant

@Repository
interface FeedbackRepository : JpaRepository<Feedback, Long> {
    
    /**
     * Count how many feedback submissions came from a specific IP address after a given time.
     * Used for rate limiting / anti-spam.
     */
    fun countByIpAddressAndCreatedAtAfter(ipAddress: String, after: Instant): Long
}
