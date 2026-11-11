package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * A generic service for interacting with the Redis cache.
 * This service abstracts the details of serialization (to JSON) and deserialization (from JSON).
 */
@Service
class RedisCacheService(
    // StringRedisTemplate is used because we are storing our objects as JSON strings.
    private val redisTemplate: StringRedisTemplate,
    // ObjectMapper is used for converting Kotlin objects to and from JSON.
    private val objectMapper: ObjectMapper
) {

    /**
     * Retrieves and deserializes an object from the Redis cache.
     * @param key The key of the object to retrieve.
     * @param type The class type of the object to deserialize into.
     * @return The deserialized object of type [T], or null if the key does not exist.
     */
    /**
     * Retrieves and deserializes an object from the Redis cache.
     * Handles both legacy plain JSON and new compressed JSON.
     */
    fun <T> get(key: String, type: Class<T>): T? {
        val rawValue = redisTemplate.opsForValue().get(key) ?: return null
        return deserializeValue(rawValue, type)
    }

    /**
     * Retrieves and deserializes multiple objects from the Redis cache in a single round-trip.
     * Use this for batch fetching to reduce network latency.
     * @param keys The list of keys to retrieve.
     * @param type The class type of the objects.
     * @return A map of Key -> Object? for found items. Keys with no value in Redis will have null.
     */
    fun <T> multiGet(keys: List<String>, type: Class<T>): Map<String, T?> {
        if (keys.isEmpty()) return emptyMap()

        val rawValues = redisTemplate.opsForValue().multiGet(keys) ?: return keys.associateWith { null }

        // rawValues matches the order of keys
        val result = LinkedHashMap<String, T?>(keys.size)
        for (i in keys.indices) {
            val key = keys[i]
            val rawValue = rawValues.getOrNull(i)
            result[key] = if (rawValue != null) deserializeValue(rawValue, type) else null
        }
        return result
    }

    /**
     * Serializes, compresses, and stores an object in the Redis cache using streaming GZIP + Base64.
     */
    fun <T> set(key: String, value: T, expiration: Duration) {
        val compressed = serializeAndCompress(value)
        if (expiration.isZero || expiration.isNegative) {
            redisTemplate.opsForValue().set(key, compressed)
        } else {
            redisTemplate.opsForValue().set(key, compressed, expiration)
        }
    }

    /**
     * Tries to set a key if it does not already exist. Useful for locking.
     * @return true if the key was set, false if it already existed.
     */
    fun setIfAbsent(key: String, value: String, expiration: Duration): Boolean {
        return redisTemplate.opsForValue().setIfAbsent(key, value, expiration) == true
    }

    /**
     * Deletes a key from Redis.
     */
    fun delete(key: String) {
        redisTemplate.delete(key)
    }

    /**
     * Bounded cursor-based key scan matching [pattern] (e.g., `match:euw1:*` or `player:profile:euw1:*`).
     * Enforces [maxKeys] (capped at [HARD_MAX_SCAN_KEYS]) so Redis scans are never unbounded.
     * Falls back to `redisTemplate.keys(pattern)` only when `scan` is unstubbed (returns null/throws) in unit tests.
     */
    fun scanKeys(
        pattern: String,
        countHint: Long = 200,
        maxKeys: Int = DEFAULT_MAX_SCAN_KEYS
    ): List<String> {
        val effectiveLimit = maxKeys.coerceIn(1, HARD_MAX_SCAN_KEYS)
        return try {
            val options = org.springframework.data.redis.core.ScanOptions.scanOptions()
                .match(pattern)
                .count(countHint)
                .build()
            val found = mutableSetOf<String>()
            var scanCursorOpened = false
            redisTemplate.scan(options)?.use { cursor ->
                scanCursorOpened = true
                while (cursor.hasNext() && found.size < effectiveLimit) {
                    found.add(cursor.next())
                }
            }
            if (scanCursorOpened) {
                found.sorted().take(effectiveLimit)
            } else {
                redisTemplate.keys(pattern)?.sorted()?.take(effectiveLimit) ?: emptyList()
            }
        } catch (e: Exception) {
            try {
                redisTemplate.keys(pattern)?.sorted()?.take(effectiveLimit) ?: emptyList()
            } catch (_: Exception) {
                emptyList()
            }
        }
    }

    private fun <T> serializeAndCompress(value: T): String {
        val bos = java.io.ByteArrayOutputStream(1024)
        bos.write(COMPRESSION_PREFIX.toByteArray(java.nio.charset.StandardCharsets.ISO_8859_1))
        java.util.Base64.getEncoder().wrap(bos).use { b64Out ->
            java.util.zip.GZIPOutputStream(b64Out, GZIP_BUFFER_SIZE).use { gzipOut ->
                objectMapper.writeValue(gzipOut, value)
            }
        }
        return bos.toString(java.nio.charset.StandardCharsets.ISO_8859_1)
    }

    private fun <T> deserializeValue(rawValue: String, type: Class<T>): T? {
        val ctx = currentThreadInflateContext.get()
        val effectiveMaxBytes = (ctx?.maxInflatedBytes ?: DEFAULT_MAX_INFLATED_CACHE_BYTES)
            .coerceIn(1_024, HARD_MAX_INFLATED_CACHE_BYTES)
        return try {
            if (rawValue.startsWith(COMPRESSION_PREFIX)) {
                val b64Len = rawValue.length - COMPRESSION_PREFIX.length
                if (b64Len > HARD_MAX_COMPRESSED_BASE64_CHARS) {
                    ctx?.failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                    return null
                }
                val b64Bytes = rawValue.substring(COMPRESSION_PREFIX.length)
                    .toByteArray(java.nio.charset.StandardCharsets.ISO_8859_1)
                java.io.ByteArrayInputStream(b64Bytes).use { rawIn ->
                    java.util.Base64.getDecoder().wrap(rawIn).use { b64In ->
                        java.util.zip.GZIPInputStream(b64In, GZIP_BUFFER_SIZE).use { gzipIn ->
                            BoundedInflatingInputStream(
                                delegate = gzipIn,
                                maxInflatedBytes = effectiveMaxBytes,
                                deadlineAtMs = ctx?.deadlineAtMs,
                                timeProvider = ctx?.timeProvider,
                                headroomGuard = ctx?.headroomGuard,
                                failureReasonHolder = ctx?.failureReasonHolder
                            ).use { boundedIn ->
                                objectMapper.readValue(boundedIn, type)
                            }
                        }
                    }
                }
            } else {
                val utf8Bytes = rawValue.toByteArray(java.nio.charset.StandardCharsets.UTF_8)
                java.io.ByteArrayInputStream(utf8Bytes).use { rawIn ->
                    BoundedInflatingInputStream(
                        delegate = rawIn,
                        maxInflatedBytes = effectiveMaxBytes,
                        deadlineAtMs = ctx?.deadlineAtMs,
                        timeProvider = ctx?.timeProvider,
                        headroomGuard = ctx?.headroomGuard,
                        failureReasonHolder = ctx?.failureReasonHolder
                    ).use { boundedIn ->
                        objectMapper.readValue(boundedIn, type)
                    }
                }
            }
        } catch (e: BoundedCacheReadAbortedException) {
            ctx?.failureReasonHolder?.set(e.reason)
            null
        } catch (e: Exception) {
            val aborted = generateSequence<Throwable>(e) { it.cause }
                .filterIsInstance<BoundedCacheReadAbortedException>()
                .firstOrNull()
            if (aborted != null) {
                ctx?.failureReasonHolder?.set(aborted.reason)
            } else if (ctx != null && (Thread.currentThread().isInterrupted ||
                    (ctx.deadlineAtMs != null && (ctx.timeProvider?.now() ?: System.currentTimeMillis()) >= ctx.deadlineAtMs))
            ) {
                if (ctx.failureReasonHolder?.get() == null) {
                    ctx.failureReasonHolder?.set("DEADLINE_EXCEEDED")
                }
            }
            null
        }
    }

    internal class BoundedCacheReadAbortedException(val reason: String) : java.io.IOException(reason)

    internal data class BoundedInflateContext(
        val maxInflatedBytes: Int,
        val deadlineAtMs: Long? = null,
        val timeProvider: TimeProvider? = null,
        val headroomGuard: (() -> Boolean)? = null,
        val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    )

    internal class BoundedInflatingInputStream(
        private val delegate: java.io.InputStream,
        private val maxInflatedBytes: Int,
        private val deadlineAtMs: Long? = null,
        private val timeProvider: TimeProvider? = null,
        private val headroomGuard: (() -> Boolean)? = null,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ) : java.io.InputStream() {
        private var totalBytesRead = 0

        private fun checkGuardsOrThrow() {
            val nowMs = timeProvider?.now() ?: System.currentTimeMillis()
            if (Thread.currentThread().isInterrupted || (deadlineAtMs != null && nowMs >= deadlineAtMs)) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                throw BoundedCacheReadAbortedException("DEADLINE_EXCEEDED")
            }
            if (headroomGuard != null && !headroomGuard.invoke()) {
                failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                throw BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
            }
            if (totalBytesRead > maxInflatedBytes) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        override fun read(): Int {
            checkGuardsOrThrow()
            val b = delegate.read()
            if (b != -1) {
                totalBytesRead++
            }
            checkGuardsOrThrow()
            return b
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (len <= 0) return 0
            checkGuardsOrThrow()
            val boundedLen = len.coerceAtMost(1_024)
            val n = delegate.read(b, off, boundedLen)
            if (n > 0) {
                totalBytesRead += n
            }
            checkGuardsOrThrow()
            return n
        }

        override fun close() {
            delegate.close()
        }
    }

    companion object {
        private const val COMPRESSION_PREFIX = "GZIP_B64:"
        private const val GZIP_BUFFER_SIZE = 2048
        const val DEFAULT_MAX_SCAN_KEYS = 2_000
        const val HARD_MAX_SCAN_KEYS = 5_000
        const val DEFAULT_MAX_INFLATED_CACHE_BYTES = 16 * 1024 * 1024 // 16 MiB default bomb guard
        const val HARD_MAX_INFLATED_CACHE_BYTES = 32 * 1024 * 1024
        private const val HARD_MAX_COMPRESSED_BASE64_CHARS = 12 * 1024 * 1024

        private val currentThreadInflateContext = ThreadLocal<BoundedInflateContext?>()

        internal fun <R> withBoundedInflateGuards(
            context: BoundedInflateContext,
            block: () -> R
        ): R {
            val previous = currentThreadInflateContext.get()
            currentThreadInflateContext.set(context)
            return try {
                block()
            } finally {
                if (previous != null) {
                    currentThreadInflateContext.set(previous)
                } else {
                    currentThreadInflateContext.remove()
                }
            }
        }
    }
}

