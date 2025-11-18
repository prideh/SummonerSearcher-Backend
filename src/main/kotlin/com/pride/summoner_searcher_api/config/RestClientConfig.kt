package com.pride.summoner_searcher_api.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.client.RestClient

@Configuration
class RestClientConfig {

    @Value("\${riotApiKey}")
    private lateinit var riotApiKey: String

    @Bean
    fun riotRestClient(): RestClient {
        return RestClient.builder()
            .defaultHeader("X-Riot-Token", riotApiKey)
            .build()
    }
}
