package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.service.FeedbackService
import com.pride.summoner_searcher_api.service.RateLimitException
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

data class SubmitFeedbackRequest(
    val type: String,
    val content: String
)

@RestController
@RequestMapping("/api/feedback")
class FeedbackController(
    private val feedbackService: FeedbackService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @PostMapping("/submit")
    fun submitFeedback(
        @CurrentUser user: User?,
        @RequestBody request: SubmitFeedbackRequest,
        servletRequest: HttpServletRequest
    ): ResponseEntity<String> {
        try {
            val ipAddress = getClientIpAddress(servletRequest)
            
            // Validate type
            val validTypes = listOf("bug", "feature", "general")
            val type = if (validTypes.contains(request.type)) request.type else "general"

            feedbackService.submitFeedback(
                userId = user?.id,
                ipAddress = ipAddress,
                type = type,
                content = request.content
            )

            return ResponseEntity.ok("Feedback submitted successfully. Thank you!")
        } catch (e: RateLimitException) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(e.message)
        } catch (e: IllegalArgumentException) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.message)
        } catch (e: Exception) {
            logger.error("Failed to submit feedback", e)
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred.")
        }
    }

    private fun getClientIpAddress(request: HttpServletRequest): String {
        val xForwardedFor = request.getHeader("X-Forwarded-For")
        if (!xForwardedFor.isNullOrBlank()) {
            return xForwardedFor.split(",").first().trim()
        }
        return request.remoteAddr ?: "unknown"
    }
}
