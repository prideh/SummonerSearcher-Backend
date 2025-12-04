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

    /**
     * Updates the rate limits for a region based on Riot API response headers.
     * @param region The routing value.
     * @param limitHeader The X-App-Rate-Limit header value (e.g., "20:1,100:120").
     * @param countHeader The X-App-Rate-Limit-Count header value (e.g., "1:1,5:120").
     */
    suspend fun updateLimits(region: String, limitHeader: String?, countHeader: String?) {
        if (limitHeader == null || countHeader == null) return
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .updateBuckets(limitHeader, countHeader)
    }

    inner class RegionRateLimiter(private val regionName: String) {
        // Dynamic list of buckets. Copy-on-write or similar thread-safe approach needed if modifying.
        // Using a Mutex to protect the list modification.
        private val bucketsMutex = Mutex()
        @Volatile
        private var buckets = listOf<SlidingWindowBucket>()
        
        // Initial conservative defaults (Dev Key) until we hear otherwise
        init {
            buckets = listOf(
                SlidingWindowBucket(20, Duration.ofSeconds(1), "$regionName-default-short"),
                SlidingWindowBucket(100, Duration.ofMinutes(2), "$regionName-default-long")
            )
        }

        suspend fun acquire(priority: ApiPriority) {
            // Snapshot the current buckets to iterate safely
            val currentBuckets = buckets
            currentBuckets.forEach { bucket ->
                bucket.acquire(priority)
            }
        }

        suspend fun updateBuckets(limitHeader: String, countHeader: String) {
            bucketsMutex.withLock {
                try {
                    val limits = parseHeader(limitHeader) // List<Pair<Capacity, Seconds>>
                    val counts = parseHeader(countHeader) // List<Pair<Count, Seconds>>

                    if (limits.size != counts.size) {
                        logger.warn("[{}] Mismatch in limit/count headers: {} vs {}", regionName, limitHeader, countHeader)
                        return
                    }

                    // Check if we need to re-configure buckets
                    // We match buckets by their window duration (seconds)
                    val newBuckets = mutableListOf<SlidingWindowBucket>()
                    var changed = false

                    limits.forEachIndexed { index, (capacity, seconds) ->
                        val count = counts[index].first
                        val window = Duration.ofSeconds(seconds.toLong())
                        
                        // Find existing bucket for this window
                        val existingBucket = buckets.find { it.windowSeconds == seconds }
                        
                        if (existingBucket != null) {
                            // Update existing bucket
                            if (existingBucket.updateConfig(capacity, count)) {
                                changed = true
                            }
                            newBuckets.add(existingBucket)
                        } else {
                            // Create new bucket
                            logger.info("[{}] Discovered new rate limit: {} req / {}s", regionName, capacity, seconds)
                            val newBucket = SlidingWindowBucket(capacity, window, "$regionName-${seconds}s")
                            newBucket.syncCount(count)
                            newBuckets.add(newBucket)
                            changed = true
                        }
                    }

                    if (changed || newBuckets.size != buckets.size) {
                        buckets = newBuckets
                        logger.debug("[{}] Buckets updated. Active limits: {}", regionName, buckets.map { "${it.capacity}/${it.windowSeconds}s" })
                    }
                } catch (e: Exception) {
                    logger.error("[{}] Failed to parse rate limit headers: {} / {}", regionName, limitHeader, countHeader, e)
                }
            }
        }

        private fun parseHeader(header: String): List<Pair<Int, Int>> {
            return header.split(",").mapNotNull { part ->
                val parts = part.split(":")
                if (parts.size == 2) {
                    val val1 = parts[0].toIntOrNull()
                    val val2 = parts[1].toIntOrNull()
                    if (val1 != null && val2 != null) val1 to val2 else null
                } else null
            }
        }
    }

    inner class SlidingWindowBucket(
        @Volatile var capacity: Int,
        val window: Duration,
        private val name: String
    ) {
        private val timestamps = ArrayDeque<Long>()
        private val mutex = Mutex()
        val windowSeconds = window.seconds.toInt()
        private val windowMs = window.toMillis()

        /**
         * Updates capacity and syncs count if needed.
         * Returns true if configuration changed.
         */
        suspend fun updateConfig(newCapacity: Int, remoteCount: Int): Boolean {
            var changed = false
            if (capacity != newCapacity) {
                logger.info("[{}] Capacity changed: {} -> {}", name, capacity, newCapacity)
                capacity = newCapacity
                changed = true
            }
            
            syncCount(remoteCount)
            return changed
        }

        suspend fun syncCount(remoteCount: Int) {
            mutex.withLock {
                val now = timeProvider.now()
                cleanUp(now)
                
                val localCount = timestamps.size
                if (remoteCount > localCount) {
                    // We are out of sync (e.g. other instance, or restart). 
                    // Conservatively add "fake" timestamps to match the remote count.
                    // We add them as "now" to be safe (worst case), effectively reducing immediate burst.
                    val diff = remoteCount - localCount
                    logger.trace("[{}] Syncing count: Local {} < Remote {}. Adding {} fake entries.", name, localCount, remoteCount, diff)
                    repeat(diff) {
                        timestamps.addLast(now)
                    }
                }
            }
        }

        suspend fun acquire(priority: ApiPriority) {
            mutex.withLock {
                while (true) {
                    val now = timeProvider.now()
                    cleanUp(now)

                    if (timestamps.size < capacity) {
                        // Dynamic Spacing: 
                        // If we have high capacity (e.g. 500/10s = 50 req/s), we want spacing ~ 20ms.
                        // If we have low capacity (e.g. 20/1s = 20 req/s), spacing ~ 50ms.
                        // Formula: 1000ms / (RequestsPerSecond * Factor)
                        // Factor > 1.0 means we allow some burst. Factor 1.0 is strict pacing.
                        // Let's use a factor of 1.2 to allow slight bursting but keep it smooth.
                        
                        if (timestamps.isNotEmpty()) {
                            val reqPerSec = capacity.toDouble() / windowSeconds
                            // Only enforce spacing for short windows (e.g. < 60s) to allow bursting on long windows
                            if (windowSeconds < 60) {
                                val minSpacing = (1000.0 / (reqPerSec * 1.2)).toLong().coerceAtLeast(5L) // Min 5ms
                                
                                val lastRequest = timestamps.last()
                                val timeSinceLast = now - lastRequest
                                
                                if (timeSinceLast < minSpacing) {
                                    val waitTime = minSpacing - timeSinceLast
                                    mutex.unlock()
                                    try {
                                        delay(waitTime)
                                    } finally {
                                        mutex.lock()
                                    }
                                    continue
                                }
                            }
                        }
                        
                        timestamps.addLast(timeProvider.now())
                        return
                    }

                    // Window is full
                    val oldest = timestamps.first()
                    val expiryTime = oldest + windowMs
                    val waitTime = expiryTime - now

                    if (waitTime > 0) {
                        logger.debug("[{}] Limit reached ({}/{}). Waiting {}ms...", name, timestamps.size, capacity, waitTime)
                        mutex.unlock()
                        try {
                            delay(waitTime + 10)
                        } finally {
                            mutex.lock()
                        }
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
