package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.dto.MethodRateLimitSnapshotDto
import com.pride.summoner_searcher_api.dto.RateLimitVerificationSnapshotDto
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

enum class ApiPriority {
    HIGH, // User requests
    LOW   // Cache warmer / background cohort tasks
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
 * - Sliding Window: Allows "Burst & Wait" behavior for both App-level (`X-App-Rate-Limit`)
 *   and Method-level (`X-Method-Rate-Limit`) headers.
 * - Priority Headroom: `LOW` priority requests are capped at 80% capacity (`LOW_PRIORITY_CAP_RATIO`),
 *   reserving 20% headroom for `HIGH` priority user requests.
 * - User-Request Yield & 429 Retry-After: Background `LOW` priority workers can query/await
 *   clearance so they automatically pause when user requests are active or during a `429 Retry-After` window.
 */
@Component
class RiotApiRateLimiter(
    private val timeProvider: TimeProvider
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val limiters = ConcurrentHashMap<String, RegionRateLimiter>()

    companion object {
        const val LOW_PRIORITY_CAP_RATIO = 0.8
        const val USER_REQUEST_YIELD_WINDOW_MS = 2_000L
        const val SERVICE_429_MIN_BACKOFF_MS = 10_000L
    }

    /**
     * Acquires a permit to make a Riot API request for a specific region (and optional endpoint method key).
     */
    suspend fun acquirePermit(region: String, priority: ApiPriority, endpointKey: String? = null) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .acquire(priority, endpointKey)
    }

    /**
     * Updates the rate limits for a region based on Riot API response headers.
     */
    suspend fun updateLimits(
        region: String,
        limitHeader: String?,
        countHeader: String?,
        endpointKey: String? = null,
        methodLimitHeader: String? = null,
        methodCountHeader: String? = null
    ) {
        val normalizedRegion = region.lowercase()
        val regionLimiter = limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
        if (limitHeader != null && countHeader != null) {
            regionLimiter.updateBuckets(limitHeader, countHeader)
        }
        if (!endpointKey.isNullOrBlank() && methodLimitHeader != null && methodCountHeader != null) {
            regionLimiter.updateMethodBuckets(endpointKey, methodLimitHeader, methodCountHeader)
        }
    }

    /**
     * Records a regional `429 Retry-After` window (in milliseconds) and optional `X-Rate-Limit-Type`
     * (`application`, `method`, or `service`) so subsequent permits wait and background workers pause.
     */
    fun recordRetryAfter(region: String, waitMs: Long, rateLimitType: String? = null) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .recordRetryAfter(waitMs, rateLimitType)
    }

    /**
     * Marks the start of a user-initiated (`HIGH` priority) operation so `LOW` priority background workers pause.
     */
    fun beginUserRequest(region: String) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .beginUserRequest()
    }

    /**
     * Marks the completion of a user-initiated (`HIGH` priority) operation.
     */
    fun endUserRequest(region: String) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .endUserRequest()
    }

    /**
     * Suspends a `LOW` priority background worker while any `HIGH` priority user request is active,
     * within [USER_REQUEST_YIELD_WINDOW_MS] of the last user request, or inside a `429 Retry-After` / `service` window.
     */
    suspend fun awaitLowPriorityClearance(region: String) {
        val normalizedRegion = region.lowercase()
        limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
            .awaitLowPriorityClearance()
    }

    /**
     * Returns the remaining active regional `429 Retry-After` or `service` backoff wait time in milliseconds,
     * or `0L` if no cooldown is currently active.
     */
    fun getActiveRetryAfterWaitMs(region: String): Long {
        val normalizedRegion = region.lowercase()
        return limiters[normalizedRegion]?.getActiveRetryAfterWaitMs() ?: 0L
    }

    /**
     * Returns a read-only verification snapshot of observed App, Method, and Service rate limits and headroom.
     */
    fun getVerificationSnapshot(
        region: String,
        networkCollectionEnabled: Boolean = false,
        requiredEndpoints: Set<String> = emptySet()
    ): RateLimitVerificationSnapshotDto {
        val normalizedRegion = region.lowercase()
        val regionLimiter = limiters.computeIfAbsent(normalizedRegion) { RegionRateLimiter(it) }
        return regionLimiter.toVerificationSnapshot(networkCollectionEnabled, requiredEndpoints)
    }

    inner class MethodEndpointState(val endpointKey: String) {
        val mutex = Mutex()
        @Volatile
        var lastLimitHeader: String? = null
        @Volatile
        var lastCountHeader: String? = null
        @Volatile
        var lastUpdatedAtMs: Long? = null
        @Volatile
        var buckets: List<SlidingWindowBucket> = emptyList()
    }

    inner class RegionRateLimiter(private val regionName: String) {
        private val bucketsMutex = Mutex()
        @Volatile
        private var buckets = listOf<SlidingWindowBucket>()
        @Volatile
        private var appLimitsVerifiedFromHeaders: Boolean = false
        @Volatile
        private var lastAppLimitHeader: String? = null
        @Volatile
        private var lastAppCountHeader: String? = null
        @Volatile
        private var retryAfterUntilMs: Long = 0L
        @Volatile
        private var serviceRateLimitUntilMs: Long = 0L
        @Volatile
        private var lastRateLimitType: String? = null
        @Volatile
        private var lastHighPriorityAtMs: Long? = null
        private val activeUserRequests = AtomicInteger(0)
        private val methodEndpoints = ConcurrentHashMap<String, MethodEndpointState>()

        // Initial conservative defaults (Dev Key) until response headers arrive
        init {
            buckets = listOf(
                SlidingWindowBucket(20, Duration.ofSeconds(1), "$regionName-default-short")
            )
        }

        fun beginUserRequest() {
            activeUserRequests.incrementAndGet()
            lastHighPriorityAtMs = timeProvider.now()
        }

        fun endUserRequest() {
            activeUserRequests.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
            lastHighPriorityAtMs = timeProvider.now()
        }

        fun recordRetryAfter(waitMs: Long, rateLimitType: String? = null) {
            val now = timeProvider.now()
            val normalizedType = rateLimitType?.trim()?.lowercase()?.takeIf { it.isNotBlank() }
            if (normalizedType != null) {
                lastRateLimitType = normalizedType
            }
            val effectiveWaitMs = when {
                normalizedType == "service" -> maxOf(waitMs, SERVICE_429_MIN_BACKOFF_MS)
                else -> waitMs
            }
            if (effectiveWaitMs <= 0L) return
            val target = now + effectiveWaitMs
            if (target > retryAfterUntilMs) {
                retryAfterUntilMs = target
            }
            if (normalizedType == "service" && target > serviceRateLimitUntilMs) {
                serviceRateLimitUntilMs = target
            }
        }

        fun getActiveRetryAfterWaitMs(): Long {
            val now = timeProvider.now()
            return (maxOf(retryAfterUntilMs, serviceRateLimitUntilMs) - now).coerceAtLeast(0L)
        }

        private fun isForegroundYieldActive(now: Long): Boolean {
            if (activeUserRequests.get() > 0) return true
            val lastHigh = lastHighPriorityAtMs ?: return false
            return (now - lastHigh) in 0 until USER_REQUEST_YIELD_WINDOW_MS
        }

        suspend fun awaitLowPriorityClearance() {
            while (true) {
                val now = timeProvider.now()
                val retryWait = maxOf(retryAfterUntilMs, serviceRateLimitUntilMs) - now
                if (retryWait > 0) {
                    delay(retryWait + 10)
                    continue
                }
                if (activeUserRequests.get() > 0) {
                    delay(100)
                    continue
                }
                val lastHigh = lastHighPriorityAtMs
                if (lastHigh != null && (now - lastHigh) in 0 until USER_REQUEST_YIELD_WINDOW_MS) {
                    delay((USER_REQUEST_YIELD_WINDOW_MS - (now - lastHigh)).coerceAtLeast(50))
                    continue
                }
                return
            }
        }

        suspend fun acquire(priority: ApiPriority, endpointKey: String? = null) {
            val now = timeProvider.now()
            if (priority == ApiPriority.HIGH) {
                lastHighPriorityAtMs = now
            }
            val retryWait = maxOf(retryAfterUntilMs, serviceRateLimitUntilMs) - now
            if (retryWait > 0) {
                delay(retryWait + 10)
            }

            // Snapshot the current app buckets to iterate safely
            val currentBuckets = buckets
            currentBuckets.forEach { bucket ->
                bucket.acquire(priority)
            }

            if (!endpointKey.isNullOrBlank()) {
                val methodBuckets = methodEndpoints[endpointKey]?.buckets ?: emptyList()
                methodBuckets.forEach { bucket ->
                    bucket.acquire(priority)
                }
            }
        }

        suspend fun updateBuckets(limitHeader: String, countHeader: String) {
            bucketsMutex.withLock {
                try {
                    val limits = parseHeader(limitHeader)
                    val counts = parseHeader(countHeader)

                    if (limits.size != counts.size || limits.isEmpty()) {
                        logger.warn("[{}] Mismatch in limit/count headers: {} vs {}", regionName, limitHeader, countHeader)
                        return
                    }

                    lastAppLimitHeader = limitHeader
                    lastAppCountHeader = countHeader
                    appLimitsVerifiedFromHeaders = true

                    val newBuckets = mutableListOf<SlidingWindowBucket>()
                    var changed = false

                    limits.forEachIndexed { index, (capacity, seconds) ->
                        val count = counts[index].first
                        val window = Duration.ofSeconds(seconds.toLong())

                        val existingBucket = buckets.find { it.windowSeconds == seconds }

                        if (existingBucket != null) {
                            if (existingBucket.updateConfig(capacity, count)) {
                                changed = true
                            }
                            newBuckets.add(existingBucket)
                        } else {
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

        suspend fun updateMethodBuckets(endpointKey: String, methodLimitHeader: String, methodCountHeader: String) {
            val state = methodEndpoints.computeIfAbsent(endpointKey) { MethodEndpointState(it) }
            state.mutex.withLock {
                try {
                    val limits = parseHeader(methodLimitHeader)
                    val counts = parseHeader(methodCountHeader)
                    if (limits.size != counts.size || limits.isEmpty()) return

                    state.lastLimitHeader = methodLimitHeader
                    state.lastCountHeader = methodCountHeader
                    state.lastUpdatedAtMs = timeProvider.now()

                    val newBuckets = mutableListOf<SlidingWindowBucket>()
                    limits.forEachIndexed { index, (capacity, seconds) ->
                        val count = counts[index].first
                        val window = Duration.ofSeconds(seconds.toLong())
                        val existing = state.buckets.find { it.windowSeconds == seconds }
                        if (existing != null) {
                            existing.updateConfig(capacity, count)
                            newBuckets.add(existing)
                        } else {
                            val b = SlidingWindowBucket(capacity, window, "$regionName-$endpointKey-${seconds}s")
                            b.syncCount(count)
                            newBuckets.add(b)
                        }
                    }
                    state.buckets = newBuckets
                } catch (e: Exception) {
                    logger.debug("[{}:{}] Failed to parse method rate limit headers: {} / {}", regionName, endpointKey, methodLimitHeader, methodCountHeader)
                }
            }
        }

        fun toVerificationSnapshot(
            networkCollectionEnabled: Boolean,
            requiredEndpoints: Set<String> = emptySet()
        ): RateLimitVerificationSnapshotDto {
            val now = timeProvider.now()
            val currentAppBuckets = buckets
            val appHasHeadroom = currentAppBuckets.all { it.hasHeadroomForLowPriority(now) }
            val methodMap = methodEndpoints.entries
                .sortedBy { it.key }
                .associate { (key, state) ->
                    val mBuckets = state.buckets
                    key to MethodRateLimitSnapshotDto(
                        endpointKey = key,
                        methodRateLimitHeader = state.lastLimitHeader,
                        methodRateLimitCountHeader = state.lastCountHeader,
                        activeBuckets = mBuckets.map { "${it.currentUsage(now)}/${it.capacity} per ${it.windowSeconds}s" },
                        hasHeadroomForLowPriority = mBuckets.all { it.hasHeadroomForLowPriority(now) },
                        lastUpdatedAt = state.lastUpdatedAtMs?.let { Instant.ofEpochMilli(it) }
                    )
                }
            val verifiedMethodKeys = methodMap.entries
                .filter { (_, v) -> !v.methodRateLimitHeader.isNullOrBlank() && !v.methodRateLimitCountHeader.isNullOrBlank() }
                .map { it.key }
                .toSet()
            val missingRequired = requiredEndpoints - verifiedMethodKeys
            val effectiveRetryUntilMs = maxOf(retryAfterUntilMs, serviceRateLimitUntilMs)
            val retryUntil = effectiveRetryUntilMs.takeIf { it > now }?.let { Instant.ofEpochMilli(it) }
            val serviceActive = serviceRateLimitUntilMs > now ||
                (retryUntil != null && lastRateLimitType == "service")
            val fgYieldActive = isForegroundYieldActive(now)
            val methodsVerified = verifiedMethodKeys.isNotEmpty() &&
                missingRequired.isEmpty() &&
                methodMap.values.all {
                    !it.methodRateLimitHeader.isNullOrBlank() &&
                        !it.methodRateLimitCountHeader.isNullOrBlank() &&
                        it.hasHeadroomForLowPriority
                }
            val ready = networkCollectionEnabled &&
                appLimitsVerifiedFromHeaders &&
                appHasHeadroom &&
                methodsVerified &&
                !fgYieldActive &&
                retryUntil == null &&
                !serviceActive

            return RateLimitVerificationSnapshotDto(
                region = regionName,
                appLimitsVerifiedFromHeaders = appLimitsVerifiedFromHeaders,
                appRateLimitHeader = lastAppLimitHeader,
                appRateLimitCountHeader = lastAppCountHeader,
                activeAppBuckets = currentAppBuckets.map { "${it.currentUsage(now)}/${it.capacity} per ${it.windowSeconds}s" },
                appHasHeadroomForLowPriority = appHasHeadroom,
                methodLimitsByEndpoint = methodMap,
                verifiedMethodEndpoints = verifiedMethodKeys,
                missingRequiredMethodEndpoints = missingRequired,
                lastRateLimitType = lastRateLimitType,
                serviceRateLimitActive = serviceActive,
                foregroundYieldActive = fgYieldActive,
                lowPriorityCapRatio = LOW_PRIORITY_CAP_RATIO,
                userRequestYieldWindowMs = USER_REQUEST_YIELD_WINDOW_MS,
                activeUserRequests = activeUserRequests.get(),
                lastHighPriorityRequestAt = lastHighPriorityAtMs?.let { Instant.ofEpochMilli(it) },
                retryAfterUntil = retryUntil,
                networkCollectionEnabled = networkCollectionEnabled,
                readyForLowPriorityNetworkCollection = ready
            )
        }

        private fun parseHeader(header: String): List<Pair<Int, Int>> {
            return header.split(",").mapNotNull { part ->
                val parts = part.trim().split(":")
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

        fun currentUsage(now: Long): Int {
            return timestamps.count { now - it < windowMs }
        }

        fun hasHeadroomForLowPriority(now: Long): Boolean {
            val lowCap = (capacity * LOW_PRIORITY_CAP_RATIO).toInt().coerceAtLeast(1)
            return currentUsage(now) < lowCap
        }

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

                    // Reserve 20% headroom for HIGH priority requests by capping LOW priority at 80% capacity
                    val effectiveCapacity = when (priority) {
                        ApiPriority.HIGH -> capacity
                        ApiPriority.LOW -> (capacity * LOW_PRIORITY_CAP_RATIO).toInt().coerceAtLeast(1)
                    }

                    if (timestamps.size < effectiveCapacity) {
                        timestamps.addLast(timeProvider.now())
                        return
                    }

                    val targetIndex = (timestamps.size - effectiveCapacity).coerceIn(0, timestamps.size - 1)
                    val targetTimestamp = timestamps[targetIndex]
                    val expiryTime = targetTimestamp + windowMs
                    val waitTime = expiryTime - now

                    if (waitTime > 0) {
                        logger.debug("[{}] Limit reached for {} ({}/{}). Waiting {}ms...", name, priority, timestamps.size, effectiveCapacity, waitTime)
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
