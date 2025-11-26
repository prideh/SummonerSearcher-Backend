package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RiotApiRateLimiterTest {

    // TimeProvider that delegates to the TestScope's virtual time
    class TestTimeProvider(private val testScope: TestScope) : TimeProvider {
        override fun now(): Long = testScope.currentTime
    }

    @Test
    fun `should allow burst and recover after window`() = runTest {
        val timeProvider = TestTimeProvider(this)
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        val region = "euw1"

        val startTime = currentTime

        // 1. First Burst: 90 requests
        repeat(90) {
            rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        }

        // 2. Wait for window to pass (2 minutes)
        // We don't manually advance time here because acquirePermit will call delay() if needed.
        // But since we consumed exactly 90 (capacity), the next one SHOULD trigger a delay.
        
        // However, to test "recover after window", we can manually advance time to simulate a gap in traffic.
        testScheduler.advanceTimeBy(130_000)

        // 3. Second Burst: 90 requests
        // Should be immediate (no delay) because we waited 2 minutes.
        val burstStart = currentTime
        repeat(90) {
            rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        }
        val burstDuration = currentTime - burstStart
        
        // Note: runTest's delay() skips time, so burstDuration would be > 0 if delay was called.
        // Ideally 0 or very small.
        // UPDATE: Short Term Pacer (20 req/s) is still active!
        // 90 requests will take 90/20 = 4.5 seconds to clear the short term bucket.
        // So we expect duration to be around 4000-5000ms.
        
        assertTrue(burstDuration < 6000, "Second burst should be paced only by short-term limit (~4-5s). Took: ${burstDuration}ms")
    }

    @Test
    fun `should isolate regions`() = runTest {
        val timeProvider = TestTimeProvider(this)
        val rateLimiter = RiotApiRateLimiter(timeProvider)

        // 1. Max out EUW1
        repeat(90) {
            rateLimiter.acquirePermit("euw1", ApiPriority.HIGH)
        }

        // 2. Try NA1 immediately
        // Should succeed because it's a different bucket
        val start = currentTime
        rateLimiter.acquirePermit("na1", ApiPriority.HIGH)
        val duration = currentTime - start

        assertTrue(duration < 100, "NA1 request should be immediate despite EUW1 being full")
    }

    @Test
    fun `should enforce short term pacing`() = runTest {
        val timeProvider = TestTimeProvider(this)
        val rateLimiter = RiotApiRateLimiter(timeProvider)
        val region = "euw1"

        // 1. Burst 20 requests (Short term limit: 20 req / 1s)
        repeat(20) {
            rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        }

        // 2. The 21st request should trigger a wait
        // The first request was at time 0. The 21st request comes immediately.
        // It must wait until the 1st request expires (at 1000ms).
        
        val start = currentTime
        rateLimiter.acquirePermit(region, ApiPriority.HIGH)
        val end = currentTime
        
        // We expect the time to have advanced to at least 1000ms
        assertTrue(end >= 1000, "Should have waited for short term window. Time is $end")
    }
}
