package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * A centralized rate limiter for the Riot API that enforces multiple rate limits simultaneously.
 * Uses the token bucket algorithm to manage:
 * - 20 requests per 1 second
 * - 100 requests per 2 minutes
 *
 * This ensures that all API calls across the application stay within Riot's rate limits.
 */
@Component
class RiotApiRateLimiter {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Two buckets for Riot's two rate limit windows
    private val shortTermBucket = RateLimitBucket(maxTokens = 20, refillPeriod = Duration.ofSeconds(1), name = "1s")
    private val longTermBucket = RateLimitBucket(maxTokens = 100, refillPeriod = Duration.ofMinutes(2), name = "2m")

    /**
     * Acquires a permit to make a Riot API request.
     * Suspends until both rate limits can be satisfied.
     */
    suspend fun acquirePermit() {
        // Must satisfy BOTH rate limits before proceeding
        shortTermBucket.acquire()
        longTermBucket.acquire()
    }

    /**
     * Inner class representing a single rate limit bucket using the token bucket algorithm.
     */
    inner class RateLimitBucket(
        private val maxTokens: Int,
        private val refillPeriod: Duration,
        private val name: String
    ) {
        private val tokens = AtomicInteger(maxTokens)
        private var lastRefill = Instant.now()
        private val mutex = Mutex()

        /**
         * Acquires a single token from the bucket.
         * If no tokens are available, waits until the bucket refills.
         */
        suspend fun acquire() {
            mutex.withLock {
                refillTokens()

                while (tokens.get() <= 0) {
                    // Calculate how long to wait until next refill
                    val now = Instant.now()
                    val elapsed = Duration.between(lastRefill, now)
                    val waitTime = refillPeriod.minus(elapsed).toMillis()

                    if (waitTime > 0) {
                        val tokensAvailable = tokens.get()
                        logger.debug("Rate limit bucket [{}] depleted ({}). Waiting {}ms...", 
                            name, tokensAvailable, waitTime)
                        
                        // Release lock while waiting to allow other operations
                        mutex.unlock()
                        try {
                            kotlinx.coroutines.delay(waitTime)
                        } finally {
                            mutex.lock()
                        }
                        refillTokens()
                    } else {
                        // Time has passed but tokens haven't refilled - force refill
                        refillTokens()
                    }
                }

                // Consume one token
                tokens.decrementAndGet()
                logger.trace("Token acquired from bucket [{}]. Remaining: {}", name, tokens.get())
            }
        }

        /**
         * Refills the bucket if enough time has elapsed.
         */
        private fun refillTokens() {
            val now = Instant.now()
            val elapsed = Duration.between(lastRefill, now)

            if (elapsed >= refillPeriod) {
                val previousTokens = tokens.get()
                tokens.set(maxTokens)
                lastRefill = now
                logger.debug("Refilled bucket [{}]: {} -> {} tokens", name, previousTokens, maxTokens)
            }
        }
    }
}
