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
