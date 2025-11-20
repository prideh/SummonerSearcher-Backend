package com.pride.summoner_searcher_api.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.client.RestClient

/**
 * Configuration class for setting up external API clients.
 */
@Configuration
class RestClientConfig {

    // Injects the Riot API key from the application properties.
    @Value("\${riotApiKey}")
    private lateinit var riotApiKey: String

    /**
     * Creates a singleton RestClient bean specifically for interacting with the Riot Games API.
     *
     * This client is pre-configured with a default header `X-Riot-Token`, which automatically
     * adds the Riot API key to every outgoing request. This simplifies the API service layer,
     * as the key does not need to be added manually to each call.
     *
     * @return A configured RestClient instance for the Riot API.
     */
    @Bean
    fun riotRestClient(): RestClient {
        return RestClient.builder()
            .defaultHeader("X-Riot-Token", riotApiKey)
            .build()
    }
}
