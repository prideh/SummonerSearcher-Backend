package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.system.measureTimeMillis

class RiotApiRateLimiterTest {

    @Test
    fun `should pace requests at 50ms intervals`() = runBlocking {
        val rateLimiter = RiotApiRateLimiter()
        val numberOfRequests = 20
        
        val timeTaken = measureTimeMillis {
            val jobs = (1..numberOfRequests).map {
                async {
                    rateLimiter.acquirePermit(ApiPriority.HIGH)
                }
            }
            jobs.awaitAll()
        }

        // Expected time: (20 - 1) * 50ms = 950ms minimum
        // We allow a small margin of error, but it should be at least 900ms
        println("Processed $numberOfRequests requests in ${timeTaken}ms")
        assertTrue(timeTaken >= 900, "Requests were processed too fast! Expected > 900ms, took ${timeTaken}ms")
    }

    @Test
    fun `should prioritize high priority requests`() = runBlocking {
        val rateLimiter = RiotApiRateLimiter()
        
        // Drain the long term bucket to near the buffer limit
        // Max 100. Buffer 10.
        // We need to consume enough to drop below 11, accounting for refill during the pacing delay.
        // 95 requests * 50ms = 4.75s. Refill ~4 tokens. Net consumption ~91. Remaining ~9.
        repeat(95) {
            rateLimiter.acquirePermit(ApiPriority.HIGH)
        }
        
        // Now we have ~15 tokens left.
        // High priority should pass.
        val highStart = System.currentTimeMillis()
        rateLimiter.acquirePermit(ApiPriority.HIGH)
        val highTime = System.currentTimeMillis() - highStart
        println("High priority took ${highTime}ms")
        assertTrue(highTime < 100, "High priority should be instant")

        // Now we have ~14 tokens left.
        // Low priority should pass (buffer is 10).
        val lowStart = System.currentTimeMillis()
        rateLimiter.acquirePermit(ApiPriority.LOW)
        val lowTime = System.currentTimeMillis() - lowStart
        println("Low priority took ${lowTime}ms")
        assertTrue(lowTime < 100, "Low priority should be instant (tokens > buffer)")

        // Consume down to buffer (10 tokens left)
        repeat(3) { rateLimiter.acquirePermit(ApiPriority.HIGH) }
        
        // Now ~10 tokens left.
        // Low priority should WAIT.
        // Refill rate is slow (0.83 req/s).
        // It should wait for 1 token + buffer.
        
        println("Testing Low Priority blocking...")
        val blockedStart = System.currentTimeMillis()
        rateLimiter.acquirePermit(ApiPriority.LOW) // Should block
        val blockedTime = System.currentTimeMillis() - blockedStart
        
        println("Low priority blocked for ${blockedTime}ms")
        assertTrue(blockedTime > 500, "Low priority should have been blocked")
    }
}
