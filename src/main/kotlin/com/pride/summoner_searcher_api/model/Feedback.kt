package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant

/**
 * Represents user feedback submitted via the feedback page.
 * Feedback can be submitted anonymously (null userId).
 */
@Entity
@Table(name = "feedback")
class Feedback(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    /** Optional ID of the user if they were logged in. */
    var userId: Long? = null,

    /** The IP address from which the feedback was submitted, used for rate limiting. */
    @Column(nullable = false)
    var ipAddress: String,

    /** Type of feedback: 'bug', 'feature', 'general'. */
    @Column(nullable = false, length = 50)
    var type: String,

    /** The actual feedback text. */
    @Column(nullable = false, length = 1000)
    var content: String,

    /** Timestamp when the feedback was created. */
    @Column(nullable = false, updatable = false)
    val createdAt: Instant = Instant.now()
)
