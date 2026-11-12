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

    private val lastSetAllPeakSubBatchBytes = java.util.concurrent.atomic.AtomicLong(0L)
    private val maxObservedSetAllSubBatchBytes = java.util.concurrent.atomic.AtomicLong(0L)
    private val lastSetAllPeakWhilePriorBatchHeldBytes = java.util.concurrent.atomic.AtomicLong(0L)
    private val lastSetAllPeakTransientBufferedBytes = java.util.concurrent.atomic.AtomicLong(0L)
    private val maxObservedOversizedEntryBytes = java.util.concurrent.atomic.AtomicLong(0L)
    private val pipelineSubBatchFlushes = java.util.concurrent.atomic.AtomicLong(0L)
    private val oversizedDirectWrites = java.util.concurrent.atomic.AtomicLong(0L)
    private val pipelineFallbackWrites = java.util.concurrent.atomic.AtomicLong(0L)

    internal data class PipelineWriteTelemetry(
        val lastCallPeakSubBatchBytes: Long,
        val maxObservedSubBatchBytes: Long,
        val lastCallPeakWhilePriorBatchHeldBytes: Long = 0L,
        val lastCallPeakTransientBufferedBytes: Long = 0L,
        val maxObservedOversizedEntryBytes: Long = 0L,
        val pipelineFlushes: Long,
        val oversizedDirectWrites: Long = 0L,
        val fallbackWrites: Long
    )

    internal fun getPipelineWriteTelemetry(): PipelineWriteTelemetry = PipelineWriteTelemetry(
        lastCallPeakSubBatchBytes = lastSetAllPeakSubBatchBytes.get(),
        maxObservedSubBatchBytes = maxObservedSetAllSubBatchBytes.get(),
        lastCallPeakWhilePriorBatchHeldBytes = lastSetAllPeakWhilePriorBatchHeldBytes.get(),
        lastCallPeakTransientBufferedBytes = lastSetAllPeakTransientBufferedBytes.get(),
        maxObservedOversizedEntryBytes = maxObservedOversizedEntryBytes.get(),
        pipelineFlushes = pipelineSubBatchFlushes.get(),
        oversizedDirectWrites = oversizedDirectWrites.get(),
        fallbackWrites = pipelineFallbackWrites.get()
    )

    /**
     * Returns true on real [RedisCacheService] instances (and false on unstubbed relaxed mocks so unit tests
     * spying on per-key [set] continue to observe individual writes without stub changes).
     */
    fun canPipelineBatchWrites(): Boolean = true

    private class PreparedPipelineEntry(
        val key: String,
        val keyBytes: ByteArray,
        val valueBytes: ByteArray
    ) {
        val totalBufferedBytes: Long
            get() = keyBytes.size.toLong() + valueBytes.size.toLong()
    }

    /**
     * Slabbed (`2 KiB` initial, `8 KiB` max slab) ASCII output buffer for `setAll` compression that:
     * 1. Eliminates `ByteArrayOutputStream` power-of-two doubling (`256 KiB -> 512 KiB`) and intermediate `String` copies.
     * 2. Flushes and clears any non-empty `priorBatch` **before** allocating a new slab or contiguous `ByteArray`
     *    whenever `priorSubBatchBytes + keyBytesLength + allocatedSlabBytes + extraBytes > 256 KiB`, guaranteeing
     *    that `priorBatch` is never held concurrently once combined buffered bytes would cross `256 KiB`.
     */
    private class BudgetAwareSlabOutputStream(
        private val keyBytesLength: Int,
        private val currentSubBatchBytes: () -> Long,
        private val flushPriorSubBatchIfNonEmpty: () -> Unit
    ) : java.io.OutputStream() {
        private val slabs = ArrayList<ByteArray>(8)
        private var completedSlabsBytes = 0L
        private var activeSlab = ByteArray(INITIAL_SLAB_BYTES)
        private var activeOffset = 0
        private var totalSize = 0
        var peakWhilePriorHeldBytes: Long = 0L
            private set
        var peakTransientBytes: Long = 0L
            private set

        init {
            recordPeak(0L)
        }

        private fun recordPeak(extraContiguousBytes: Long) {
            val subBytes = currentSubBatchBytes()
            val slabBytes = completedSlabsBytes + activeSlab.size.toLong()
            val combined = subBytes + keyBytesLength.toLong() + slabBytes + extraContiguousBytes
            if (subBytes > 0L && combined > peakWhilePriorHeldBytes) {
                peakWhilePriorHeldBytes = combined
            }
            if (combined > peakTransientBytes) {
                peakTransientBytes = combined
            }
        }

        private fun rollSlabWithBudgetGuard() {
            val subBytes = currentSubBatchBytes()
            val nextSlabBytes = completedSlabsBytes + activeSlab.size.toLong() + MAX_SLAB_BYTES.toLong()
            if (subBytes > 0L &&
                subBytes + keyBytesLength.toLong() + nextSlabBytes > MAX_PIPELINE_SUBBATCH_UTF8_BYTES
            ) {
                flushPriorSubBatchIfNonEmpty()
            }
            slabs.add(activeSlab)
            completedSlabsBytes += activeSlab.size.toLong()
            activeSlab = ByteArray(MAX_SLAB_BYTES)
            activeOffset = 0
            recordPeak(0L)
        }

        override fun write(b: Int) {
            if (activeOffset >= activeSlab.size) {
                rollSlabWithBudgetGuard()
            }
            activeSlab[activeOffset++] = b.toByte()
            totalSize++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            if (off < 0 || len < 0 || len > b.size - off) {
                throw IndexOutOfBoundsException()
            }
            var remaining = len
            var srcPos = off
            while (remaining > 0) {
                if (activeOffset >= activeSlab.size) {
                    rollSlabWithBudgetGuard()
                }
                val toCopy = minOf(remaining, activeSlab.size - activeOffset)
                System.arraycopy(b, srcPos, activeSlab, activeOffset, toCopy)
                activeOffset += toCopy
                srcPos += toCopy
                remaining -= toCopy
                totalSize += toCopy
            }
        }

        fun toByteArrayAndReleaseSlabs(): ByteArray {
            val subBytes = currentSubBatchBytes()
            val slabBytes = completedSlabsBytes + activeSlab.size.toLong()
            if (subBytes > 0L &&
                subBytes + keyBytesLength.toLong() + slabBytes + totalSize.toLong() > MAX_PIPELINE_SUBBATCH_UTF8_BYTES
            ) {
                flushPriorSubBatchIfNonEmpty()
            }
            val out = ByteArray(totalSize)
            recordPeak(totalSize.toLong())
            var dstPos = 0
            for (i in slabs.indices) {
                val slab = slabs[i]
                System.arraycopy(slab, 0, out, dstPos, slab.size)
                dstPos += slab.size
                slabs[i] = EMPTY_BYTES
            }
            slabs.clear()
            completedSlabsBytes = 0L
            if (activeOffset > 0) {
                System.arraycopy(activeSlab, 0, out, dstPos, activeOffset)
            }
            activeSlab = EMPTY_BYTES
            activeOffset = 0
            return out
        }
    }

    /**
     * Streams [entries] with strict sub-batching and bounded transient compression buffers:
     * - **Queued `subBatch` invariant**: `subBatch.size <= 25` and `subBatchBytes <= 256 KiB` (`262,144` bytes) strictly.
     * - **Prior-batch non-overlap**: while `subBatch` is non-empty, `subBatchBytes + keyBytes + slabs + copy <= 256 KiB`;
     *   before any slab roll or contiguous `ByteArray` materialization would cross `256 KiB`, `subBatch` is flushed and cleared first.
     * - **Oversized single entries (`entryBytes > 256 KiB`)**: never added to `subBatch` and never batched via `executePipelined`;
     *   written immediately as an unbatched single-key write preserving `"GZIP_B64:"` encoding, [expiration] TTL, and
     *   [ValueOperations.set] fallback, then released (`2 * entryValueBytes + <= 8 KiB` slab transient peak during assembly).
     */
    fun <T> setAll(entries: Map<String, T>, expiration: Duration) {
        if (entries.isEmpty()) return
        val hasPositiveTtl = !expiration.isZero && !expiration.isNegative
        val redisExpiration = if (hasPositiveTtl) {
            org.springframework.data.redis.core.types.Expiration.from(expiration)
        } else {
            org.springframework.data.redis.core.types.Expiration.persistent()
        }

        val subBatch = ArrayList<PreparedPipelineEntry>(minOf(entries.size, MAX_PIPELINE_SUBBATCH_ENTRIES))
        var subBatchBytes = 0L
        var callPeakSubBatchBytes = 0L
        var callPeakWhilePriorHeldBytes = 0L
        var callPeakTransientBytes = 0L

        fun flushSubBatch() {
            if (subBatch.isEmpty()) return
            if (subBatchBytes > callPeakSubBatchBytes) {
                callPeakSubBatchBytes = subBatchBytes
            }
            if (subBatchBytes > callPeakTransientBytes) {
                callPeakTransientBytes = subBatchBytes
            }
            maxObservedSetAllSubBatchBytes.accumulateAndGet(subBatchBytes, ::maxOf)
            val pipelinedSucceeded = try {
                if (redisTemplate.connectionFactory == null) {
                    false
                } else {
                    redisTemplate.executePipelined { connection ->
                        val stringCommands = connection.stringCommands()
                        for (item in subBatch) {
                            stringCommands.set(
                                item.keyBytes,
                                item.valueBytes,
                                redisExpiration,
                                org.springframework.data.redis.connection.RedisStringCommands.SetOption.UPSERT
                            )
                        }
                        null
                    }
                    pipelineSubBatchFlushes.incrementAndGet()
                    true
                }
            } catch (_: Exception) {
                false
            }

            if (!pipelinedSucceeded) {
                val valueOps = redisTemplate.opsForValue()
                for (item in subBatch) {
                    val compressed = String(item.valueBytes, java.nio.charset.StandardCharsets.ISO_8859_1)
                    if (hasPositiveTtl) {
                        valueOps.set(item.key, compressed, expiration)
                    } else {
                        valueOps.set(item.key, compressed)
                    }
                    pipelineFallbackWrites.incrementAndGet()
                }
            }
            subBatch.clear()
            subBatchBytes = 0L
        }

        fun writeDirectOversizedEntry(item: PreparedPipelineEntry) {
            val directRawSucceeded = try {
                if (redisTemplate.connectionFactory == null) {
                    false
                } else {
                    redisTemplate.execute(
                        org.springframework.data.redis.core.RedisCallback<Boolean> { connection ->
                            connection.stringCommands().set(
                                item.keyBytes,
                                item.valueBytes,
                                redisExpiration,
                                org.springframework.data.redis.connection.RedisStringCommands.SetOption.UPSERT
                            )
                        }
                    ) == true
                }
            } catch (_: Exception) {
                false
            }
            oversizedDirectWrites.incrementAndGet()
            if (!directRawSucceeded) {
                val valueOps = redisTemplate.opsForValue()
                val compressed = String(item.valueBytes, java.nio.charset.StandardCharsets.ISO_8859_1)
                if (hasPositiveTtl) {
                    valueOps.set(item.key, compressed, expiration)
                } else {
                    valueOps.set(item.key, compressed)
                }
                pipelineFallbackWrites.incrementAndGet()
            }
        }

        for ((key, value) in entries) {
            val keyBytes = key.toByteArray(java.nio.charset.StandardCharsets.UTF_8)
            if (subBatch.isNotEmpty() &&
                (subBatch.size >= MAX_PIPELINE_SUBBATCH_ENTRIES ||
                    subBatchBytes + keyBytes.size.toLong() + INITIAL_SLAB_BYTES.toLong() > MAX_PIPELINE_SUBBATCH_UTF8_BYTES)
            ) {
                flushSubBatch()
            }

            val slabOut = BudgetAwareSlabOutputStream(
                keyBytesLength = keyBytes.size,
                currentSubBatchBytes = { subBatchBytes },
                flushPriorSubBatchIfNonEmpty = ::flushSubBatch
            )
            slabOut.write(COMPRESSION_PREFIX_ASCII_BYTES, 0, COMPRESSION_PREFIX_ASCII_BYTES.size)
            java.util.Base64.getEncoder().wrap(slabOut).use { b64Out ->
                java.util.zip.GZIPOutputStream(b64Out, GZIP_BUFFER_SIZE).use { gzipOut ->
                    objectMapper.writeValue(gzipOut, value)
                }
            }
            val valueBytes = slabOut.toByteArrayAndReleaseSlabs()
            if (slabOut.peakWhilePriorHeldBytes > callPeakWhilePriorHeldBytes) {
                callPeakWhilePriorHeldBytes = slabOut.peakWhilePriorHeldBytes
            }
            if (slabOut.peakTransientBytes > callPeakTransientBytes) {
                callPeakTransientBytes = slabOut.peakTransientBytes
            }

            val prepared = PreparedPipelineEntry(key, keyBytes, valueBytes)
            val entryBytes = prepared.totalBufferedBytes
            if (entryBytes > MAX_PIPELINE_SUBBATCH_UTF8_BYTES) {
                // Never admit an oversized (> 256 KiB) entry into `subBatch` or `executePipelined`.
                flushSubBatch()
                maxObservedOversizedEntryBytes.accumulateAndGet(entryBytes, ::maxOf)
                if (entryBytes > callPeakTransientBytes) {
                    callPeakTransientBytes = entryBytes
                }
                writeDirectOversizedEntry(prepared)
                continue
            }

            if (subBatch.isNotEmpty() &&
                (subBatch.size >= MAX_PIPELINE_SUBBATCH_ENTRIES ||
                    subBatchBytes + entryBytes > MAX_PIPELINE_SUBBATCH_UTF8_BYTES)
            ) {
                flushSubBatch()
            }
            subBatch.add(prepared)
            subBatchBytes += entryBytes
            if (subBatchBytes > callPeakTransientBytes) {
                callPeakTransientBytes = subBatchBytes
            }
            if (subBatch.size >= MAX_PIPELINE_SUBBATCH_ENTRIES ||
                subBatchBytes >= MAX_PIPELINE_SUBBATCH_UTF8_BYTES
            ) {
                flushSubBatch()
            }
        }
        flushSubBatch()
        lastSetAllPeakSubBatchBytes.set(callPeakSubBatchBytes)
        lastSetAllPeakWhilePriorBatchHeldBytes.set(callPeakWhilePriorHeldBytes)
        lastSetAllPeakTransientBufferedBytes.set(callPeakTransientBytes)
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
                BoundedBase64DecodingInputStream(
                    source = rawValue,
                    startIndex = COMPRESSION_PREFIX.length,
                    maxBase64Chars = maxBase64Chars,
                    maxCompressedBytes = maxCompressedBytes,
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

    /**
     * Streaming, zero-substring RFC 4648 Base64 decoder that decodes up to 1,024 Base64 characters
     * (768 compressed bytes) per refill into a single reusable buffer, enforcing exact [maxBase64Chars]
     * and [maxCompressedBytes] bounds plus interrupt/deadline checks on every read while sampling
     * [headroomGuard] at initial entry and every 1 KiB boundary (avoiding `Base64.getDecoder().wrap`
     * single-byte `read()` amplification).
     */
    internal class BoundedBase64DecodingInputStream(
        private val source: String,
        startIndex: Int,
        private val maxBase64Chars: Int,
        private val maxCompressedBytes: Int,
        private val deadlineAtMs: Long? = null,
        private val timeProvider: TimeProvider? = null,
        private val headroomGuard: (() -> Boolean)? = null,
        private val failureReasonHolder: java.util.concurrent.atomic.AtomicReference<String?>? = null
    ) : java.io.InputStream() {
        private var index = startIndex.coerceIn(0, source.length)
        private val endExclusive = source.length
        private var totalCharsRead = 0
        private var totalDecodedBytes = 0
        private var paddingSeen = false
        private val decodedChunk = ByteArray(768)
        private var decodedOffset = 0
        private var decodedLimit = 0
        private var initialHeadroomChecked = false
        private var nextHeadroomCheckAtChars = HEADROOM_CHECK_STRIDE_BYTES

        private fun checkFastGuardsOrThrow() {
            val nowMs = timeProvider?.now() ?: System.currentTimeMillis()
            if (Thread.currentThread().isInterrupted || (deadlineAtMs != null && nowMs >= deadlineAtMs)) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                throw BoundedCacheReadAbortedException("DEADLINE_EXCEEDED")
            }
            if (totalCharsRead > maxBase64Chars || totalDecodedBytes > maxCompressedBytes) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        private fun checkHeadroomIfDueOrThrow(force: Boolean = false) {
            if (headroomGuard != null && (force || !initialHeadroomChecked || totalCharsRead >= nextHeadroomCheckAtChars)) {
                initialHeadroomChecked = true
                while (nextHeadroomCheckAtChars <= totalCharsRead) {
                    nextHeadroomCheckAtChars += HEADROOM_CHECK_STRIDE_BYTES
                }
                if (!headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    throw BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
                }
            }
        }

        private fun refillDecodedChunk(): Boolean {
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            if (index >= endExclusive) {
                return false
            }
            if (paddingSeen) {
                throw java.io.IOException("Trailing characters after Base64 padding")
            }
            var outPos = 0
            var quanta = 0
            while (quanta < 256 && index < endExclusive) {
                if (paddingSeen) {
                    throw java.io.IOException("Trailing characters after Base64 padding")
                }
                val remaining = endExclusive - index
                if (remaining < 4) {
                    throw java.io.IOException("Truncated Base64 quantum in compressed cache payload")
                }
                val c0 = source[index].code
                val c1 = source[index + 1].code
                val c2 = source[index + 2].code
                val c3 = source[index + 3].code
                if ((c0 or c1 or c2 or c3) > 0x7F) {
                    throw java.io.IOException("Non-ASCII character in compressed Base64 cache payload")
                }
                val v0 = BASE64_DECODE_TABLE[c0]
                val v1 = BASE64_DECODE_TABLE[c1]
                val v2 = BASE64_DECODE_TABLE[c2]
                val v3 = BASE64_DECODE_TABLE[c3]
                if (v0 < 0 || v1 < 0) {
                    throw java.io.IOException("Invalid Base64 character in compressed cache payload")
                }
                index += 4
                totalCharsRead += 4
                if (totalCharsRead > maxBase64Chars) {
                    failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                    throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
                }
                when {
                    v2 >= 0 && v3 >= 0 -> {
                        val bits = (v0 shl 18) or (v1 shl 12) or (v2 shl 6) or v3
                        decodedChunk[outPos++] = (bits shr 16).toByte()
                        decodedChunk[outPos++] = (bits shr 8).toByte()
                        decodedChunk[outPos++] = bits.toByte()
                        totalDecodedBytes += 3
                    }
                    v2 >= 0 && v3 == BASE64_PAD -> {
                        val bits = (v0 shl 18) or (v1 shl 12) or (v2 shl 6)
                        decodedChunk[outPos++] = (bits shr 16).toByte()
                        decodedChunk[outPos++] = (bits shr 8).toByte()
                        totalDecodedBytes += 2
                        paddingSeen = true
                    }
                    v2 == BASE64_PAD && v3 == BASE64_PAD -> {
                        val bits = (v0 shl 18) or (v1 shl 12)
                        decodedChunk[outPos++] = (bits shr 16).toByte()
                        totalDecodedBytes += 1
                        paddingSeen = true
                    }
                    else -> throw java.io.IOException("Malformed Base64 padding in compressed cache payload")
                }
                if (totalDecodedBytes > maxCompressedBytes) {
                    failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                    throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
                }
                if (paddingSeen && index < endExclusive) {
                    throw java.io.IOException("Trailing characters after Base64 padding")
                }
                quanta++
            }
            decodedOffset = 0
            decodedLimit = outPos
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            return outPos > 0
        }

        override fun read(): Int {
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            if (decodedOffset >= decodedLimit && !refillDecodedChunk()) {
                return -1
            }
            return decodedChunk[decodedOffset++].toInt() and 0xFF
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (off < 0 || len < 0 || len > b.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            if (decodedOffset >= decodedLimit && !refillDecodedChunk()) {
                return -1
            }
            val available = decodedLimit - decodedOffset
            val toCopy = minOf(len, available)
            System.arraycopy(decodedChunk, decodedOffset, b, off, toCopy)
            decodedOffset += toCopy
            return toCopy
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
        private var initialHeadroomChecked = false
        private var nextHeadroomCheckAtBytes = HEADROOM_CHECK_STRIDE_BYTES.toLong()

        private fun checkFastGuardsOrThrow() {
            val nowMs = timeProvider?.now() ?: System.currentTimeMillis()
            if (Thread.currentThread().isInterrupted || (deadlineAtMs != null && nowMs >= deadlineAtMs)) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                throw BoundedCacheReadAbortedException("DEADLINE_EXCEEDED")
            }
            if (totalUtf8BytesRead > maxUtf8Bytes.toLong()) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        private fun checkHeadroomIfDueOrThrow(force: Boolean = false) {
            if (headroomGuard != null && (force || !initialHeadroomChecked || totalUtf8BytesRead >= nextHeadroomCheckAtBytes)) {
                initialHeadroomChecked = true
                while (nextHeadroomCheckAtBytes <= totalUtf8BytesRead) {
                    nextHeadroomCheckAtBytes += HEADROOM_CHECK_STRIDE_BYTES.toLong()
                }
                if (!headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    throw BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
                }
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
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            if (index >= endExclusive) return -1
            accountUtf8Bytes(index, 1)
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val ch = source[index].code
            index++
            return ch
        }

        override fun read(cbuf: CharArray, off: Int, len: Int): Int {
            if (off < 0 || len < 0 || len > cbuf.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            if (index >= endExclusive) return -1
            val toRead = minOf(len, 1_024, endExclusive - index)
            accountUtf8Bytes(index, toRead)
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            source.toCharArray(cbuf, off, index, index + toRead)
            index += toRead
            return toRead
        }

        fun ensureRemainingWithinBound() {
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val remainingChars = endExclusive - index
            if (remainingChars <= 0) return
            if (totalUtf8BytesRead + remainingChars.toLong() * 3L <= maxUtf8Bytes.toLong()) {
                return
            }
            while (index < endExclusive) {
                checkFastGuardsOrThrow()
                checkHeadroomIfDueOrThrow()
                val step = minOf(1_024, endExclusive - index)
                accountUtf8Bytes(index, step)
                index += step
                checkFastGuardsOrThrow()
                checkHeadroomIfDueOrThrow()
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
        private var initialHeadroomChecked = false
        private var nextHeadroomCheckAtBytes = HEADROOM_CHECK_STRIDE_BYTES

        private fun checkFastGuardsOrThrow() {
            val nowMs = timeProvider?.now() ?: System.currentTimeMillis()
            if (Thread.currentThread().isInterrupted || (deadlineAtMs != null && nowMs >= deadlineAtMs)) {
                failureReasonHolder?.set("DEADLINE_EXCEEDED")
                throw BoundedCacheReadAbortedException("DEADLINE_EXCEEDED")
            }
            if (totalBytesRead > maxInflatedBytes) {
                failureReasonHolder?.set("RESPONSE_TOO_LARGE")
                throw BoundedCacheReadAbortedException("RESPONSE_TOO_LARGE")
            }
        }

        private fun checkHeadroomIfDueOrThrow(force: Boolean = false) {
            if (headroomGuard != null && (force || !initialHeadroomChecked || totalBytesRead >= nextHeadroomCheckAtBytes)) {
                initialHeadroomChecked = true
                while (nextHeadroomCheckAtBytes <= totalBytesRead) {
                    nextHeadroomCheckAtBytes += HEADROOM_CHECK_STRIDE_BYTES
                }
                if (!headroomGuard.invoke()) {
                    failureReasonHolder?.set("MEMORY_HEADROOM_STOP")
                    throw BoundedCacheReadAbortedException("MEMORY_HEADROOM_STOP")
                }
            }
        }

        override fun read(): Int {
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val b = delegate.read()
            if (b != -1) {
                totalBytesRead++
            }
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            return b
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (off < 0 || len < 0 || len > b.size - off) {
                throw IndexOutOfBoundsException()
            }
            if (len == 0) return 0
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow()
            val boundedLen = len.coerceAtMost(1_024)
            val n = delegate.read(b, off, boundedLen)
            if (n > 0) {
                totalBytesRead += n
            }
            checkFastGuardsOrThrow()
            checkHeadroomIfDueOrThrow(force = n in 1 until boundedLen)
            return n
        }

        fun drainRemainingWithinBound() {
            val first = read()
            if (first == -1) return
            val scratch = ByteArray(256)
            while (read(scratch, 0, scratch.size) != -1) {
                // Fast size/deadline guards + 1 KiB headroom cadence enforced inside read(...)
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
        private val COMPRESSION_PREFIX_ASCII_BYTES =
            COMPRESSION_PREFIX.toByteArray(java.nio.charset.StandardCharsets.ISO_8859_1)
        private val EMPTY_BYTES = ByteArray(0)
        private const val INITIAL_SLAB_BYTES = 2_048
        private const val MAX_SLAB_BYTES = 8_192
        private const val GZIP_BUFFER_SIZE = 2048
        internal const val HEADROOM_CHECK_STRIDE_BYTES = 1_024
        private const val BASE64_PAD = -2
        private val BASE64_DECODE_TABLE = IntArray(128) { -1 }.apply {
            for (i in 'A'.code..'Z'.code) this[i] = i - 'A'.code
            for (i in 'a'.code..'z'.code) this[i] = 26 + (i - 'a'.code)
            for (i in '0'.code..'9'.code) this[i] = 52 + (i - '0'.code)
            this['+'.code] = 62
            this['/'.code] = 63
            this['='.code] = BASE64_PAD
        }
        const val MAX_PIPELINE_SUBBATCH_ENTRIES = 25
        const val MAX_PIPELINE_SUBBATCH_UTF8_BYTES = 256L * 1024L // 256 KiB strict pre-compressed sub-batch ceiling
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

