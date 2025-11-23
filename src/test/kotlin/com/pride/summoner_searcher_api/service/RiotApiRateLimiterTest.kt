package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RiotApiRateLimiterTest {

    @Test
    fun `should pace requests at 55ms intervals`() = runTest {
        // Use virtual time from TestScope
        val timeProvider = object : TimeProvider {
            override fun now() = testScheduler.currentTime
        }
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        
        val start = testScheduler.currentTime
        
        // Simulate 20 requests
        repeat(20) {
            rateLimiter.acquirePermit(ApiPriority.HIGH)
        }
        
        val duration = testScheduler.currentTime - start
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
            override fun now() = testScheduler.currentTime
        }
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        
        val start = testScheduler.currentTime
        
        println("Starting 300 requests simulation...")
        
        // Simulate 300 requests (User requested "burst calls" scenario)
        repeat(300) { index ->
            rateLimiter.acquirePermit(ApiPriority.LOW)
            if (index % 50 == 0) {
                println("Processed $index requests at ${testScheduler.currentTime}ms")
            }
        }
        
        val duration = testScheduler.currentTime - start
        println("Processed 300 requests in ${duration}ms")
        
        // Analysis:
        // Limit: 100 requests / 2 minutes (120,000ms)
        // Config: Capacity 20, Refill 80/2min (1 token every 1500ms)
        
        // 1. Initial Burst: 20 tokens.
        //    Paced by 55ms. Time = 20 * 55 = 1100ms.
        // 2. Remaining 280 requests:
        //    Must wait for refill.
        //    Time = 280 * 1500ms = 420,000ms (7 minutes)
        
        // Total expected time approx 420,000ms + small buffer.
        
        assertTrue(duration > 415_000, "Should take at least 415s (6.9m) to process 300 requests. Took: ${duration}ms")
        assertTrue(duration < 450_000, "Should take less than 450s (7.5m). Took: ${duration}ms")
    }
}
