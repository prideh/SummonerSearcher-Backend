package com.pride.summoner_searcher_api.handler

import com.pride.summoner_searcher_api.exception.RiotApiStatusException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import java.time.LocalDateTime

@ControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(RiotApiStatusException::class)
    fun handleRiotApiStatusException(ex: RiotApiStatusException): ResponseEntity<Map<String, Any>> {
        val errorDetails = mapOf(
            "timestamp" to LocalDateTime.now().toString(),
            "status" to ex.statusCode.value(),
            "error" to "Riot API Error",
            "message" to (ex.reason ?: "An error occurred while calling the Riot API."),
            "failedUrl" to ex.url
        )
        return ResponseEntity.status(ex.statusCode).body(errorDetails)
    }
    @ExceptionHandler(org.springframework.web.client.ResourceAccessException::class)
    fun handleResourceAccessException(ex: org.springframework.web.client.ResourceAccessException): ResponseEntity<Map<String, Any>> {
        val errorDetails = mapOf(
            "timestamp" to LocalDateTime.now().toString(),
            "status" to org.springframework.http.HttpStatus.GATEWAY_TIMEOUT.value(),
            "error" to "Gateway Timeout",
            "message" to "The Riot API is taking too long to respond. Please try again later."
        )
        return ResponseEntity.status(org.springframework.http.HttpStatus.GATEWAY_TIMEOUT).body(errorDetails)
    }
}
