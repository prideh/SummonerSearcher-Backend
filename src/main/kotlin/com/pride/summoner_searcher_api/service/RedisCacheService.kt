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

        val json = if (rawValue.startsWith(COMPRESSION_PREFIX)) {
            try {
                decompress(rawValue.removePrefix(COMPRESSION_PREFIX))
            } catch (e: Exception) {
                // If decompression fails, log it and treat as cache miss or return null
                return null
            }
        } else {
            // Legacy: It's just plain JSON
            rawValue
        }

        return try {
            objectMapper.readValue(json, type)
        } catch (e: Exception) {
            null
        }
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
        val result = mutableMapOf<String, T?>()
        
        for (i in keys.indices) {
            val key = keys[i]
            val rawValue = rawValues.getOrNull(i)
            
            if (rawValue == null) {
                result[key] = null
                continue
            }

            val json = if (rawValue.startsWith(COMPRESSION_PREFIX)) {
                try {
                    decompress(rawValue.removePrefix(COMPRESSION_PREFIX))
                } catch (e: Exception) {
                    null
                }
            } else {
                rawValue
            }

            val obj = if (json != null) {
                try {
                    objectMapper.readValue(json, type)
                } catch (e: Exception) {
                    null
                }
            } else null
            
            result[key] = obj
        }
        
        return result
    }

    /**
     * Serializes, compresses, and stores an object in the Redis cache.
     */
    fun <T> set(key: String, value: T, expiration: Duration) {
        val json = objectMapper.writeValueAsString(value)
        val compressed = COMPRESSION_PREFIX + compress(json)
        
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
        // We calculate value separately only if needed, but for locks usually value doesn't matter much.
        // For simplicity, we just store the raw string provided (e.g., "LOCKED").
        return redisTemplate.opsForValue().setIfAbsent(key, value, expiration) == true
    }

    /**
     * Deletes a key from Redis.
     */
    fun delete(key: String) {
        redisTemplate.delete(key)
    }

    private fun compress(data: String): String {
        val bos = java.io.ByteArrayOutputStream()
        java.util.zip.GZIPOutputStream(bos).bufferedWriter(java.nio.charset.StandardCharsets.UTF_8).use { it.write(data) }
        return java.util.Base64.getEncoder().encodeToString(bos.toByteArray())
    }

    private fun decompress(compressedData: String): String {
        val bytes = java.util.Base64.getDecoder().decode(compressedData)
        val bis = java.io.ByteArrayInputStream(bytes)
        return java.util.zip.GZIPInputStream(bis).bufferedReader(java.nio.charset.StandardCharsets.UTF_8).use { it.readText() }
    }

    companion object {
        private const val COMPRESSION_PREFIX = "GZIP_B64:"
    }
}
