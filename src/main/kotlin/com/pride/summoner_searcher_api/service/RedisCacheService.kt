package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class RedisCacheService(
    private val redisTemplate: StringRedisTemplate,
    private val objectMapper: ObjectMapper
) {

    fun <T> get(key: String, type: Class<T>): T? {
        val json = redisTemplate.opsForValue().get(key)
        return if (json != null) {
            objectMapper.readValue(json, type)
        } else {
            null
        }
    }

    fun <T> set(key: String, value: T, expiration: Duration) {
        val json = objectMapper.writeValueAsString(value)
        // Check if the duration is zero or negative, which means persist forever.
        if (expiration.isZero || expiration.isNegative) {
            redisTemplate.opsForValue().set(key, json)
        } else {
            redisTemplate.opsForValue().set(key, json, expiration)
        }
    }
}
