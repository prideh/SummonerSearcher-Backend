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

interface TimeProvider {
    fun now(): Long
}

@Component
class SystemTimeProvider : TimeProvider {
    override fun now() = System.currentTimeMillis()
}

/**
 * A centralized rate limiter for the Riot API.
 * 
 * Strategies:
 * - Short Term (20/1s): Uses "Strict Pacing" (1 req every 55ms) to prevent bursts.
 * - Long Term (90/2m): Uses "Token Bucket" with Priority Reservation.
 */
@Component
class RiotApiRateLimiter(
    private val timeProvider: TimeProvider
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Enforce 55ms gap between requests (approx 18 reqs/s)
    // Conservative pacing to ensure we never exceed 20/1s even with jitter
    private val shortTermPacer = PacedBucket(
        intervalMs = 55, 
        name = "1s-Pacer"
    )

    // 90 requests per 2 minutes (90% of 100 limit)
    // Safety buffer for clock drift and strict enforcement
    private val longTermBucket = PriorityTokenBucket(
        maxTokens = 90.0, 
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
        private var nextAllowedTime = timeProvider.now()
        private val mutex = Mutex()

        suspend fun acquire() {
            mutex.withLock {
                val now = timeProvider.now()
                var waitTime = nextAllowedTime - now

                if (waitTime > 0) {
                    // We must wait to maintain pacing
                    logger.debug("[{}] Pacing: Waiting {}ms to maintain 55ms gap", name, waitTime)
                    delay(waitTime)
                }

                // Update next allowed time
                // We use max(now, nextAllowedTime) to ensure we don't accumulate "credit" for past idle time
                // We must always space out future requests
                nextAllowedTime = max(now, nextAllowedTime) + intervalMs
                logger.info("[{}] Request permitted at {}", name, timeProvider.now())
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
        private var lastRefillTime = timeProvider.now()
        private val refillRatePerMs = maxTokens / refillPeriod.toMillis()
        private val mutex = Mutex()
        
        // Reserve 10 tokens for High Priority requests
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
                    
                    // Time needed = Tokens needed / Refill rate
                    val waitTimeMs = (tokensNeeded / refillRatePerMs).toLong() + 50 // +50ms buffer

                    if (waitTimeMs > 0) {
                        logger.debug("[{}] Waiting {}ms (Priority: {}, Tokens: {:.2f})...", name, waitTimeMs, priority, tokens)
                        
                        // Release lock and wait
                        // We must release the lock so other threads (e.g. High Priority) can acquire tokens
                        // or so that we can re-acquire and check refill again
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
            val now = timeProvider.now()
            val elapsed = now - lastRefillTime
            if (elapsed > 0) {
                val newTokens = elapsed * refillRatePerMs
                tokens = min(maxTokens, tokens + newTokens)
                lastRefillTime = now
            }
        }
    }
}
