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

    /**
     * Deserializes [rawValue] without allocating whole-payload UTF-8, substring, or ISO-8859-1 copies.
     *
     * Note on boundaries & intentional fail-closed tradeoff:
     * - [redisTemplate] (`StringRedisTemplate`) already materializes [rawValue] as a JVM `String` before
     *   entering [deserializeValue]; these bounds prevent additional whole-payload conversions/copies here.
     * - Compressed caps ([maxCompressedBytesForInflatedCap] / [maxBase64CharsForInflatedCap]) cover single-member
     *   `GZIPOutputStream` payloads written by [serializeAndCompress] (including incompressible stored blocks),
     *   and intentionally fail closed (`RESPONSE_TOO_LARGE` -> `null`) on pathological GZIP shapes (such as
     *   concatenated multi-member streams or oversized `FEXTRA`/`FNAME`/`FCOMMENT` headers) that exceed the
     *   compressed/Base64 cap even if inflated output would remain within `effectiveMaxBytes`.
     */
    private fun <T> deserializeValue(rawValue: String, type: Class<T>): T? {
        val ctx = currentThreadInflateContext.get()
        val effectiveMaxBytes = (ctx?.maxInflatedBytes ?: DEFAULT_MAX_INFLATED_CACHE_BYTES)
            .coerceIn(1_024, HARD_MAX_INFLATED_CACHE_BYTES)
        return try {
            checkContextGuardsOrThrow(ctx)
            if (rawValue.startsWith(COMPRESSION_PREFIX)) {
                val b64Len = rawValue.length - COMPRESSION_PREFIX.length
                val maxCompressedBytes = maxCompressedBytesForInflatedCap(effectiveMaxBytes)
                val maxBase64Chars = maxBase64CharsForInflatedCap(effectiveMaxBytes)
                if (b64Len > maxBase64Chars) {
                    ctx?.failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                    return null
                }
                BoundedBase64SliceInputStream(
                    source = rawValue,
                    startIndex = COMPRESSION_PREFIX.length,
                    maxBase64Chars = maxBase64Chars,
                    deadlineAtMs = ctx?.deadlineAtMs,
                    timeProvider = ctx?.timeProvider,
                    headroomGuard = ctx?.headroomGuard,
                    failureReasonHolder = ctx?.failureReasonHolder
                ).use { rawSliceIn ->
                    java.util.Base64.getDecoder().wrap(rawSliceIn).use { b64In ->
                        BoundedInflatingInputStream(
                            delegate = b64In,
                            maxInflatedBytes = maxCompressedBytes,
                            deadlineAtMs = ctx?.deadlineAtMs,
                            timeProvider = ctx?.timeProvider,
                            headroomGuard = ctx?.headroomGuard,
                            failureReasonHolder = ctx?.failureReasonHolder
                        ).use { boundedCompressedIn ->
                            java.util.zip.GZIPInputStream(boundedCompressedIn, GZIP_BUFFER_SIZE).use { gzipIn ->
                                val boundedIn = BoundedInflatingInputStream(
                                    delegate = gzipIn,
                                    maxInflatedBytes = effectiveMaxBytes,
                                    deadlineAtMs = ctx?.deadlineAtMs,
                                    timeProvider = ctx?.timeProvider,
                                    headroomGuard = ctx?.headroomGuard,
                                    failureReasonHolder = ctx?.failureReasonHolder,
                                    ignoreClose = true
                                )
                                val parsed = objectMapper.readValue(boundedIn, type)
                                boundedIn.drainRemainingWithinBound()
                                parsed
                            }
                        }
                    }
                }
            } else {
                // Enforce exact UTF-8 byte bound in-place before constructing Jackson's parser/buffers (zero heap allocations).
                ensureUtf8BytesWithinBound(rawValue, effectiveMaxBytes, ctx)
                BoundedUtf8CountingReader(
                    source = rawValue,
                    maxUtf8Bytes = effectiveMaxBytes,
                    deadlineAtMs = ctx?.deadlineAtMs,
                    timeProvider = ctx?.timeProvider,
                    headroomGuard = ctx?.headroomGuard,
                    failureReasonHolder = ctx?.failureReasonHolder
                ).use { boundedReader ->
                    val parsed = objectMapper.readValue(boundedReader, type)
                    boundedReader.ensureRemainingWithinBound()
                    parsed
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

    private fun ensureUtf8BytesWithinBound(
        rawValue: String,
        maxUtf8Bytes: Int,
        ctx: BoundedInflateContext?
    ) {
        val len = rawValue.length
        if (len > maxUtf8Bytes) {
            ctx?.failureReasonHolder?.set("RESPONSE_TOO_LARGE")
            throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
        }
        // Every UTF-16 code unit encodes to <= 3 UTF-8 bytes (a 2-char surrogate pair is 4 bytes = 2 bytes/char).
        if (len.toLong() * 3L <= maxUtf8Bytes.toLong()) {
            return
        }
        var totalUtf8Bytes = 0L
        var pos = 0
        var pairedLowPending = false
        while (pos < len) {
            checkContextGuardsOrThrow(ctx)
            val chunkEnd = minOf(pos + 1_024, len)
            while (pos < chunkEnd) {
                val c = rawValue[pos]
                if (pairedLowPending) {
                    pairedLowPending = false
                    totalUtf8Bytes += 2L
                } else {
                    val code = c.code
                    when {
                        code <= 0x7F -> totalUtf8Bytes += 1L
                        code <= 0x7FF -> totalUtf8Bytes += 2L
                        c.isHighSurrogate() -> {
                            if (pos + 1 < len && rawValue[pos + 1].isLowSurrogate()) {
                                totalUtf8Bytes += 2L
                                pairedLowPending = true
                            } else {
                                totalUtf8Bytes += 1L
                            }
                        }
                        c.isLowSurrogate() -> totalUtf8Bytes += 1L
                        else -> totalUtf8Bytes += 3L
                    }
                }
                pos++
            }
            if (totalUtf8Bytes > maxUtf8Bytes.toLong()) {
                ctx?.failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }
    }

    private fun checkContextGuardsOrThrow(ctx: BoundedInflateContext?) {
        if (ctx == null) return
        val nowMs = ctx.timeProvider?.now() ?: System.currentTimeMillis()
        if (Thread.currentThread().isInterrupted || (ctx.deadlineAtMs != null && nowMs >= ctx.deadlineAtMs)) {
            ctx.failureReasonHolder?.set("DEADLINE_EXCEEDED")
            throw BoundedCacheReadAbortedException("DEADLINE_EXCEEDED")
        }
        if (ctx.headroomGuard != null && !ctx.headroomGuard.invoke()) {
            ctx.failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
            throw BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
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

    internal class BoundedBase64SliceInputStream(
        private val source: String,
        startIndex: Int,
        private val maxBase64Chars: Int,
        private val deadlineAtMs: Long? = null,
        private val timeProvider: TimeProvider? = null,
        private val headroomGuard: (() -> Boolean)? = null,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ) : java.io.InputStream() {
        private var index = startIndex.coerceIn(0, source.length)
        private val endExclusive = source.length
        private var totalCharsRead = 0

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
            if (totalCharsRead > maxBase64Chars) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        override fun read(): Int {
            checkGuardsOrThrow()
            if (index >= endExclusive) return -1
            val ch = source[index].code
            if (ch > 0x7F) {
                throw java.io.IOException("Non-ASCII character in compressed Base64 cache payload")
            }
            index++
            totalCharsRead++
            checkGuardsOrThrow()
            return ch
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (off < 0 || len < 0 || len > b.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkGuardsOrThrow()
            if (index >= endExclusive) return -1
            val toRead = minOf(len, 1_024, endExclusive - index)
            for (i in 0 until toRead) {
                val ch = source[index + i].code
                if (ch > 0x7F) {
                    throw java.io.IOException("Non-ASCII character in compressed Base64 cache payload")
                }
                b[off + i] = ch.toByte()
            }
            index += toRead
            totalCharsRead += toRead
            checkGuardsOrThrow()
            return toRead
        }
    }

    internal class BoundedUtf8CountingReader(
        private val source: String,
        private val maxUtf8Bytes: Int,
        private val deadlineAtMs: Long? = null,
        private val timeProvider: TimeProvider? = null,
        private val headroomGuard: (() -> Boolean)? = null,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ) : java.io.Reader() {
        private var index = 0
        private val endExclusive = source.length
        private var totalUtf8BytesRead = 0L
        private var pairedLowPending = false

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
            if (totalUtf8BytesRead > maxUtf8Bytes.toLong()) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        private fun accountUtf8Bytes(start: Int, count: Int) {
            val end = start + count
            var pos = start
            while (pos < end) {
                val c = source[pos]
                if (pairedLowPending) {
                    pairedLowPending = false
                    totalUtf8BytesRead += 2L
                } else {
                    val code = c.code
                    when {
                        code <= 0x7F -> totalUtf8BytesRead += 1L
                        code <= 0x7FF -> totalUtf8BytesRead += 2L
                        c.isHighSurrogate() -> {
                            if (pos + 1 < endExclusive && source[pos + 1].isLowSurrogate()) {
                                totalUtf8BytesRead += 2L
                                pairedLowPending = true
                            } else {
                                totalUtf8BytesRead += 1L
                            }
                        }
                        c.isLowSurrogate() -> totalUtf8BytesRead += 1L
                        else -> totalUtf8BytesRead += 3L
                    }
                }
                pos++
            }
        }

        override fun read(): Int {
            checkGuardsOrThrow()
            if (index >= endExclusive) return -1
            accountUtf8Bytes(index, 1)
            checkGuardsOrThrow()
            val ch = source[index].code
            index++
            return ch
        }

        override fun read(cbuf: CharArray, off: Int, len: Int): Int {
            if (off < 0 || len < 0 || len > cbuf.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkGuardsOrThrow()
            if (index >= endExclusive) return -1
            val toRead = minOf(len, 1_024, endExclusive - index)
            accountUtf8Bytes(index, toRead)
            checkGuardsOrThrow()
            source.toCharArray(cbuf, off, index, index + toRead)
            index += toRead
            return toRead
        }

        fun ensureRemainingWithinBound() {
            checkGuardsOrThrow()
            val remainingChars = endExclusive - index
            if (remainingChars <= 0) return
            if (totalUtf8BytesRead + remainingChars.toLong() * 3L <= maxUtf8Bytes.toLong()) {
                return
            }
            while (index < endExclusive) {
                checkGuardsOrThrow()
                val step = minOf(1_024, endExclusive - index)
                accountUtf8Bytes(index, step)
                index += step
                checkGuardsOrThrow()
            }
        }

        override fun close() {
            // Nothing to release; source String is read in-place without buffer copies.
        }
    }

    internal class BoundedInflatingInputStream(
        private val delegate: java.io.InputStream,
        private val maxInflatedBytes: Int,
        private val deadlineAtMs: Long? = null,
        private val timeProvider: TimeProvider? = null,
        private val headroomGuard: (() -> Boolean)? = null,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null,
        private val ignoreClose: Boolean = false
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
            if (off < 0 || len < 0 || len > b.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkGuardsOrThrow()
            val boundedLen = len.coerceAtMost(1_024)
            val n = delegate.read(b, off, boundedLen)
            if (n > 0) {
                totalBytesRead += n
            }
            checkGuardsOrThrow()
            return n
        }

        fun drainRemainingWithinBound() {
            val first = read()
            if (first == -1) return
            val scratch = ByteArray(256)
            while (read(scratch, 0, scratch.size) != -1) {
                // checkGuardsOrThrow() inside read(...) enforces deadline, headroom, and maxInflatedBytes
            }
        }

        override fun close() {
            if (!ignoreClose) {
                delegate.close()
            }
        }
    }

    companion object {
        private const val COMPRESSION_PREFIX = "GZIP_B64:"
        private const val GZIP_BUFFER_SIZE = 2048
        const val DEFAULT_MAX_SCAN_KEYS = 2_000
        const val HARD_MAX_SCAN_KEYS = 5_000
        const val DEFAULT_MAX_INFLATED_CACHE_BYTES = 16 * 1024 * 1024 // 16 MiB default bomb guard
        const val HARD_MAX_INFLATED_CACHE_BYTES = 32 * 1024 * 1024
        private const val HARD_MAX_COMPRESSED_BYTES = 9 * 1024 * 1024 // 9 MiB compressed -> 12 MiB Base64 ceiling
        private const val HARD_MAX_COMPRESSED_BASE64_CHARS = 12 * 1024 * 1024

        private val currentThreadInflateContext = ThreadLocal<BoundedInflateContext?>()

        internal fun maxCompressedBytesForInflatedCap(maxInflatedBytes: Int): Int {
            val safeInflated = maxInflatedBytes.coerceIn(1_024, HARD_MAX_INFLATED_CACHE_BYTES).toLong()
            val bound = safeInflated + (safeInflated shr 11) + 512L
            return bound.coerceAtMost(HARD_MAX_COMPRESSED_BYTES.toLong()).toInt()
        }

        internal fun maxBase64CharsForInflatedCap(maxInflatedBytes: Int): Int {
            val maxCompressed = maxCompressedBytesForInflatedCap(maxInflatedBytes).toLong()
            val b64Bound = ((maxCompressed + 2L) / 3L) * 4L
            return b64Bound.coerceAtMost(HARD_MAX_COMPRESSED_BASE64_CHARS.toLong()).toInt()
        }

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

