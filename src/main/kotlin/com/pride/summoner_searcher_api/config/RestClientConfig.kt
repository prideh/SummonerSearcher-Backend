package com.pride.summoner_searcher_api.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
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
    companion object {
        fun createRiotRequestFactory(
            responseTimeout: java.time.Duration = java.time.Duration.ofSeconds(30),
            connectTimeoutMillis: Int = 30000
        ): org.springframework.http.client.ReactorNettyClientRequestFactory {
            val provider = reactor.netty.resources.ConnectionProvider.builder("riot-api-pool")
                .maxConnections(24)
                .pendingAcquireMaxCount(64)
                .maxIdleTime(java.time.Duration.ofSeconds(10))
                .maxLifeTime(java.time.Duration.ofSeconds(50))
                .pendingAcquireTimeout(java.time.Duration.ofSeconds(60))
                .evictInBackground(java.time.Duration.ofSeconds(30))
                .build()

            val httpClient = reactor.netty.http.client.HttpClient.create(provider)
                .responseTimeout(responseTimeout)
                .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .protocol(reactor.netty.http.HttpProtocol.HTTP11)
                .headers { it.set(org.springframework.http.HttpHeaders.USER_AGENT, "SummonerSearcher/1.0") }

            return org.springframework.http.client.ReactorNettyClientRequestFactory(httpClient)
        }

        fun buildRiotRestClient(
            riotApiKey: String = "test-riot-key",
            responseTimeout: java.time.Duration = java.time.Duration.ofSeconds(30),
            connectTimeoutMillis: Int = 30000
        ): RestClient {
            return RestClient.builder()
                .requestFactory(createRiotRequestFactory(responseTimeout, connectTimeoutMillis))
                .defaultHeader("X-Riot-Token", riotApiKey)
                .build()
        }
    }

    @Bean("riotRestClient")
    @Primary
    fun riotRestClient(): RestClient {
        return buildRiotRestClient(riotApiKey)
    }

    /**
     * Separate unauthenticated RestClient for public Riot Data Dragon static endpoints
     * (`ddragon.leagueoflegends.com`). Never attaches `X-Riot-Token` so the Riot API key
     * is never sent to Data Dragon CDNs.
     */
    @Bean("dataDragonRestClient")
    fun dataDragonRestClient(): RestClient {
        val provider = reactor.netty.resources.ConnectionProvider.builder("ddragon-pool")
            .maxConnections(4)
            .pendingAcquireMaxCount(8)
            .maxIdleTime(java.time.Duration.ofSeconds(10))
            .maxLifeTime(java.time.Duration.ofSeconds(50))
            .pendingAcquireTimeout(java.time.Duration.ofSeconds(15))
            .evictInBackground(java.time.Duration.ofSeconds(30))
            .build()

        val httpClient = reactor.netty.http.client.HttpClient.create(provider)
            .responseTimeout(java.time.Duration.ofSeconds(10))
            .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
            .protocol(reactor.netty.http.HttpProtocol.HTTP11)
            .headers { it.set(org.springframework.http.HttpHeaders.USER_AGENT, "SummonerSearcher/1.0") }

        val requestFactory = org.springframework.http.client.ReactorNettyClientRequestFactory(httpClient)

        return RestClient.builder()
            .requestFactory(requestFactory)
            .requestInterceptor { request, body, execution ->
                request.headers.remove("X-Riot-Token")
                execution.execute(request, body)
            }
            .build()
    }
}

