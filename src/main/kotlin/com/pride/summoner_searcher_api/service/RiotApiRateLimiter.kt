package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import kotlin.math.min

/**
 * A centralized rate limiter for the Riot API that enforces multiple rate limits simultaneously.
 * Uses a "Continuous Refill" Token Bucket algorithm to prevent bursts at window boundaries.
 *
 * Limits:
 * - 20 requests per 1 second (Short term)
 * - 100 requests per 2 minutes (Long term)
 */
@Component
class RiotApiRateLimiter {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Short term: 20 req / 1s = 0.02 req/ms
    private val shortTermBucket = RateLimitBucket(
        maxTokens = 20.0, 
        refillPeriod = Duration.ofSeconds(1), 
        name = "1s"
    )

    // Long term: 100 req / 120s = ~0.000833 req/ms
    private val longTermBucket = RateLimitBucket(
        maxTokens = 100.0, 
        refillPeriod = Duration.ofMinutes(2), 
        name = "2m"
    )

    /**
     * Acquires a permit to make a Riot API request.
     * Suspends until both rate limits can be satisfied.
     */
    suspend fun acquirePermit() {
        // We must satisfy BOTH buckets.
        // Note: This is a simplification. Ideally we'd check both, wait for the max wait time, then consume from both.
        // But acquiring sequentially is safer to ensure we don't over-consume one while waiting for the other.
        // The order matters: satisfy the fast bucket first, then the slow one.
        shortTermBucket.acquire()
        longTermBucket.acquire()
    }

    /**
     * Inner class representing a single rate limit bucket using Continuous Refill.
     */
    inner class RateLimitBucket(
        private val maxTokens: Double,
        private val refillPeriod: Duration,
        private val name: String
    ) {
        private var tokens = maxTokens
        private var lastRefillTime = System.currentTimeMillis()
        private val refillRatePerMs = maxTokens / refillPeriod.toMillis()
        private val mutex = Mutex()

        suspend fun acquire() {
            mutex.withLock {
                refill()

                while (tokens < 1.0) {
                    val tokensNeeded = 1.0 - tokens
                    val waitTimeMs = (tokensNeeded / refillRatePerMs).toLong() + 1 // +1 buffer

                    if (waitTimeMs > 0) {
                        logger.debug("Bucket [{}] depleted ({:.2f}). Waiting {}ms...", name, tokens, waitTimeMs)
                        // Unlock to allow other coroutines (though they'll likely also wait)
                        // In this specific implementation, we hold the lock to ensure strict ordering 
                        // and prevent "thundering herd" where everyone wakes up and races.
                        // However, holding the lock blocks everyone. 
                        // Better pattern: release lock, delay, re-acquire.
                    }
                    
                    // We must release the lock while waiting, otherwise no one else can process
                    mutex.unlock()
                    try {
                        delay(waitTimeMs)
                    } finally {
                        mutex.lock()
                        // Must refill again after waking up
                        refill()
                    }
                }

                tokens -= 1.0
                logger.trace("Token acquired from bucket [{}]. Remaining: {:.2f}", name, tokens)
            }
        }

        private fun refill() {
            val now = System.currentTimeMillis()
            val elapsed = now - lastRefillTime
            if (elapsed > 0) {
                val newTokens = elapsed * refillRatePerMs
                tokens = min(maxTokens, tokens + newTokens)
                lastRefillTime = now
            }
        }
    }
}
