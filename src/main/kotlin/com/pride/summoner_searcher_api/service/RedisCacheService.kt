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
    fun <T> get(key: String, type: Class<T>): T? {
        val json = redisTemplate.opsForValue().get(key)
        return if (json != null) {
            objectMapper.readValue(json, type)
        } else {
            null
        }
    }

    /**
     * Serializes and stores an object in the Redis cache with a specified expiration.
     * @param key The key to store the object under.
     * @param value The object to store.
     * @param expiration The duration until the cache entry expires. If zero or negative, it persists forever.
     */
    fun <T> set(key: String, value: T, expiration: Duration) {
        val json = objectMapper.writeValueAsString(value)
        // A zero or negative duration is treated as a command to persist the key indefinitely.
        if (expiration.isZero || expiration.isNegative) {
            redisTemplate.opsForValue().set(key, json)
        } else {
            redisTemplate.opsForValue().set(key, json, expiration)
        }
    }
}
