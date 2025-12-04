package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RiotApiRateLimiterDynamicTest {

    class TestTimeProvider(private val testScope: TestScope) : TimeProvider {
        override fun now(): Long = testScope.currentTime
    }

    @Test
    fun `should update limits dynamically`() = runTest {
        val timeProvider = TestTimeProvider(this)
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        val region = "euw1"

        // 1. Initial state: Default is 20 req / 1s
        // Burst 20 requests
        repeat(20) {
            rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        }
        
        // The 21st would normally block.
        // But let's say we receive a header saying we have 500 req / 10s (50 req/s)
        // And count is 20 / 10s.
        
        rateLimiter.updateLimits(
            region, 
            "500:10,30000:600", 
            "20:10,20:600"
        )

        // 2. Now we should be able to burst more immediately
        val start = currentTime
        repeat(20) {
            rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        }
        val duration = currentTime - start

        // With 500/10s, we have ~50 req/s. 
        // Spacing might be enforced: 1000 / (50 * 1.2) = 16ms.
        // 20 requests * 16ms = 320ms.
        // Definitely shouldn't be 1000ms (which would happen if we were still on 20/1s limit).
        
        assertTrue(duration < 1000, "Should have updated to higher limit and allowed burst. Took: ${duration}ms")
    }
}
