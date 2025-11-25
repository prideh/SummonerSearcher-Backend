package com.pride.summoner_searcher_api.service

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max

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
 * - Per Region: Limits are enforced separately for each routing value (e.g., euw1, na1).
 * - Sliding Window: Allows "Burst & Wait" behavior.
 *   - Short Term: 20 requests / 1 second.
 *   - Long Term: 100 requests / 2 minutes.
 */
@Component
class RiotApiRateLimiter(
    private val timeProvider: TimeProvider
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val limiters = ConcurrentHashMap<String, RegionRateLimiter>()

    /**
     * Acquires a permit to make a Riot API request for a specific region.
     * @param region The routing value (e.g., "euw1", "americas").
     * @param priority The priority of the request.
     */
    suspend fun acquirePermit(region: String, priority: ApiPriority) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .acquire(priority)
    }

    inner class RegionRateLimiter(private val regionName: String) {
        // Concurrency limiter: Max 10 concurrent operations per region
        // Prevents exceeding the 20 req/s short-term limit while maintaining fast parallel execution
        private val concurrencySemaphore = Semaphore(10)
        
        // Short Term: 20 requests / 1 second
        // We use 15 capacity to leave a buffer (prevents concurrent requests from hitting exactly 20)
        private val shortTermBucket = SlidingWindowBucket(
            capacity = 15,
            window = Duration.ofSeconds(1),
            name = "$regionName-1s"
        )

        // Long Term: 100 requests / 2 minutes
        // We use 90 capacity to be safe and leave a buffer
        private val longTermBucket = SlidingWindowBucket(
            capacity = 90,
            window = Duration.ofMinutes(2),
            name = "$regionName-2m"
        )

        suspend fun acquire(priority: ApiPriority) {
            // Limit concurrent access to prevent thundering herd at bucket boundary
            concurrencySemaphore.withPermit {
                // Check Long Term first (Capacity)
                longTermBucket.acquire(priority)
                // Check Short Term second (Pacing)
                shortTermBucket.acquire(priority)
            }
        }
    }

    inner class SlidingWindowBucket(
        private val capacity: Int,
        private val window: Duration,
        private val name: String
    ) {
        private val timestamps = ArrayDeque<Long>()
        private val mutex = Mutex()
        private val windowMs = window.toMillis()
        
        // Reserve buffer for HIGH priority requests (only for Long Term bucket really, but applied generally)
        // For 1s bucket, capacity is small (20), so buffer should be small (e.g. 1)
        // For 2m bucket, capacity is 90, so buffer is implicitly the remaining 10 to reach 100.
        // We will strictly enforce 'capacity' here. The "buffer" is the difference between Riot's limit (100) and our capacity (90).
        
        suspend fun acquire(priority: ApiPriority) {
            mutex.withLock {
                while (true) {
                    val now = timeProvider.now()
                    cleanUp(now)

                    if (timestamps.size < capacity) {
                        // Add 10ms minimum spacing to prevent cold-start burst
                        // Without this, 10+ coroutines can all check an empty bucket and burst past the limit
                        if (timestamps.isNotEmpty()) {
                            val lastRequest = timestamps.last()
                            val timeSinceLast = now - lastRequest
                            val minSpacing = 10L // 10ms minimum between requests
                            
                            if (timeSinceLast < minSpacing) {
                                val waitTime = minSpacing - timeSinceLast
                                logger.trace("[{}] Spacing delay: {}ms", name, waitTime)
                                mutex.unlock() // Release lock while waiting
                                try {
                                    delay(waitTime)
                                } finally {
                                    mutex.lock() // Re-acquire lock
                                }
                                continue // Re-check after delay
                            }
                        }
                        
                        // We have space and spacing is satisfied
                        timestamps.addLast(timeProvider.now())
                        logger.trace("[{}] Permit acquired. Count: {}/{}", name, timestamps.size, capacity)
                        return
                    }

                    // Window is full. Must wait until the oldest request expires.
                    val oldest = timestamps.first()
                    val expiryTime = oldest + windowMs
                    val waitTime = expiryTime - now

                    if (waitTime > 0) {
                        logger.debug("[{}] Limit reached ({}/{}). Waiting {}ms...", name, timestamps.size, capacity, waitTime)
                        mutex.unlock() // Release lock while waiting
                        try {
                            delay(waitTime + 10) // +10ms buffer
                        } finally {
                            mutex.lock() // Re-acquire lock
                        }
                    } else {
                        // Should have been cleaned up, but retry loop will handle it
                    }
                }
            }
        }

        private fun cleanUp(now: Long) {
            while (timestamps.isNotEmpty()) {
                val oldest = timestamps.first()
                if (now - oldest >= windowMs) {
                    timestamps.removeFirst()
                } else {
                    break
                }
            }
        }
    }
}
