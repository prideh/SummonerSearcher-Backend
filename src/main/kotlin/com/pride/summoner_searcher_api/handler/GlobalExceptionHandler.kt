package com.pride.summoner_searcher_api.handler

import com.pride.summoner_searcher_api.exception.RiotApiStatusException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.client.ResourceAccessException
import java.time.LocalDateTime

@ControllerAdvice
class GlobalExceptionHandler {

    private val logger = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(RiotApiStatusException::class)
    fun handleRiotApiStatusException(ex: RiotApiStatusException): ResponseEntity<Map<String, Any>> {
        val code = ex.statusCode.value()
        logger.warn("Upstream League data request failed (status={}, url={}, reason={})", code, ex.url, ex.reason)

        val (title, playerMessage) = when (code) {
            404 -> "Summoner Not Found" to
                "We couldn't find that Summoner in this region. Double-check the Riot ID and region, then try again."
            429 -> "Match History Busy" to
                "League match history is busy right now. Please wait a moment and refresh."
            502, 503, 504 -> "League Servers Unavailable" to
                "League match history is temporarily unavailable. Please try again in a moment."
            else -> "Profile Unavailable" to
                "We couldn't load this Summoner's match history right now. Please try again shortly."
        }

        val errorDetails = mapOf(
            "timestamp" to LocalDateTime.now().toString(),
            "status" to code,
            "error" to title,
            "message" to playerMessage
        )
        return ResponseEntity.status(ex.statusCode).body(errorDetails)
    }

    @ExceptionHandler(ResourceAccessException::class)
    fun handleResourceAccessException(ex: ResourceAccessException): ResponseEntity<Map<String, Any>> {
        logger.warn("Upstream League data request timed out: {}", ex.message)
        val errorDetails = mapOf(
            "timestamp" to LocalDateTime.now().toString(),
            "status" to HttpStatus.GATEWAY_TIMEOUT.value(),
            "error" to "Match History Timeout",
            "message" to "Loading League match history took too long. Please try again in a moment."
        )
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body(errorDetails)
    }
}
