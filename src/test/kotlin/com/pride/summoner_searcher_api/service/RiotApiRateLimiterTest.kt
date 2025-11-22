package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RiotApiRateLimiterTest {

    @Test
    fun `should pace requests at 55ms intervals`() = runTest {
        // Use virtual time from TestScope
        val timeProvider = object : TimeProvider {
            override fun now() = currentTime
        }
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        
        val start = currentTime
        
        // Simulate 20 requests
        repeat(20) {
            rateLimiter.acquirePermit(ApiPriority.HIGH)
        }
        
        val duration = currentTime - start
        println("Processed 20 requests in ${duration}ms")
        
        // 20 requests * 55ms = 1100ms.
        // First request is instant. 19 intervals * 55 = 1045ms.
        // So expected is around 1045ms.
        assertTrue(duration >= 1045, "Should take at least 1045ms")
    }

    @Test
    fun `should throttle 300 requests over time`() = runTest {
        // Use virtual time from TestScope
        val timeProvider = object : TimeProvider {
            override fun now() = currentTime
        }
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        
        val start = currentTime
        
        println("Starting 300 requests simulation...")
        
        // Simulate 300 requests (User requested "burst calls" scenario)
        repeat(300) { index ->
            rateLimiter.acquirePermit(ApiPriority.LOW)
            if (index % 50 == 0) {
                println("Processed $index requests at ${currentTime}ms")
            }
        }
        
        val duration = currentTime - start
        println("Processed 300 requests in ${duration}ms")
        
        // Analysis:
        // Limit: 90 requests / 2 minutes (120,000ms)
        // Refill Rate: 90 / 120,000 = 0.00075 tokens/ms (or 0.75 tokens/sec)
        // Time per token: 1333ms
        
        // 1. Initial Burst: 90 tokens.
        //    Paced by 55ms. Time = 90 * 55 = 4950ms.
        // 2. Remaining 210 requests:
        //    Must wait for refill.
        //    Time = 210 * 1333ms = 279,930ms (~4.6 minutes)
        
        // Total expected time approx 285,000ms.
        
        assertTrue(duration > 270_000, "Should take at least 270s (4.5m) to process 300 requests. Took: ${duration}ms")
        assertTrue(duration < 300_000, "Should take less than 300s (5m). Took: ${duration}ms")
    }
}
