package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import kotlin.math.max
import kotlin.math.min

enum class ApiPriority {
    HIGH, // User requests
    LOW   // Cache warmer / background tasks
}

/**
 * A centralized rate limiter for the Riot API.
 * 
 * Strategies:
 * - Short Term (20/1s): Uses "Strict Pacing" (1 req every 50ms) to prevent bursts.
 * - Long Term (100/2m): Uses "Token Bucket" with Priority Reservation.
 */
@Component
class RiotApiRateLimiter {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Enforce 50ms gap between requests (1000ms / 20 reqs)
    private val shortTermPacer = PacedBucket(
        intervalMs = 50, 
        name = "1s-Pacer"
    )

    // 100 requests per 2 minutes
    private val longTermBucket = PriorityTokenBucket(
        maxTokens = 100.0, 
        refillPeriod = Duration.ofMinutes(2), 
        name = "2m-Bucket"
    )

    /**
     * Acquires a permit to make a Riot API request.
     * @param priority The priority of the request. LOW priority requests yield to HIGH.
     */
    suspend fun acquirePermit(priority: ApiPriority) {
        // 1. Pass the long-term limit first (capacity check)
        longTermBucket.acquire(priority)
        
        // 2. Pass the short-term pacer (strict timing)
        shortTermPacer.acquire()
    }

    /**
     * Enforces a strict minimum interval between requests.
     * No bursts allowed.
     */
    inner class PacedBucket(
        private val intervalMs: Long,
        private val name: String
    ) {
        private var nextAllowedTime = System.currentTimeMillis()
        private val mutex = Mutex()

        suspend fun acquire() {
            mutex.withLock {
                val now = System.currentTimeMillis()
                var waitTime = nextAllowedTime - now

                if (waitTime > 0) {
                    // We must wait to maintain pacing
                    logger.debug("[{}] Pacing: Waiting {}ms to maintain 50ms gap", name, waitTime)
                    delay(waitTime)
                }

                // Update next allowed time
                nextAllowedTime = max(now, nextAllowedTime) + intervalMs
                logger.info("[{}] Request permitted at {}", name, System.currentTimeMillis())
            }
        }
    }

    /**
     * Token Bucket with Priority Reservation.
     * LOW priority requests must leave a buffer of tokens for HIGH priority requests.
     */
    inner class PriorityTokenBucket(
        private val maxTokens: Double,
        private val refillPeriod: Duration,
        private val name: String
    ) {
        private var tokens = maxTokens
        private var lastRefillTime = System.currentTimeMillis()
        private val refillRatePerMs = maxTokens / refillPeriod.toMillis()
        private val mutex = Mutex()
        
        // Reserve 10 tokens (10%) for High Priority requests
        private val reservedBuffer = 10.0

        suspend fun acquire(priority: ApiPriority) {
            mutex.withLock {
                refill()

                while (true) {
                    val available = tokens
                    val required = 1.0
                    
                    // Check if we can proceed based on priority
                    val canProceed = if (priority == ApiPriority.HIGH) {
                        available >= required
                    } else {
                        available >= (required + reservedBuffer)
                    }

                    if (canProceed) {
                        tokens -= required
                        logger.trace("Token acquired from [{}] (Priority: {}). Remaining: {:.2f}", name, priority, tokens)
                        return
                    }

                    // Calculate wait time
                    // If LOW priority, we wait until we have (1 + buffer) tokens
                    val targetTokens = if (priority == ApiPriority.HIGH) 1.0 else (1.0 + reservedBuffer)
                    val tokensNeeded = targetTokens - tokens
                    val waitTimeMs = (tokensNeeded / refillRatePerMs).toLong() + 50 // +50ms buffer

                    if (waitTimeMs > 0) {
                        logger.debug("[{}] Waiting {}ms (Priority: {}, Tokens: {:.2f})...", name, waitTimeMs, priority, tokens)
                        
                        // Release lock and wait
                        mutex.unlock()
                        try {
                            delay(waitTimeMs)
                        } finally {
                            mutex.lock()
                            refill()
                        }
                    } else {
                        // Should not happen if logic is correct, but safety refill
                        refill()
                    }
                }
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
