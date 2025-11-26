package com.pride.summoner_searcher_api.config

import com.pride.summoner_searcher_api.service.EmailSender
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.core.StringRedisTemplate
import org.mockito.Mockito
import org.springframework.data.redis.core.ValueOperations

@TestConfiguration
class TestRedisConfig {

    @Bean
    @Primary
    fun stringRedisTemplate(): StringRedisTemplate {
        val mockTemplate = Mockito.mock(StringRedisTemplate::class.java)
        val mockValueOps = Mockito.mock(ValueOperations::class.java) as ValueOperations<String, String>
        Mockito.`when`(mockTemplate.opsForValue()).thenReturn(mockValueOps)
        return mockTemplate
    }

    @Bean
    @Primary
    fun emailSender(): EmailSender {
        return Mockito.mock(EmailSender::class.java)
    }
}
