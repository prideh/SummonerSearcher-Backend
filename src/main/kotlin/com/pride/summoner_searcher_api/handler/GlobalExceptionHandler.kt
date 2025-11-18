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
}
