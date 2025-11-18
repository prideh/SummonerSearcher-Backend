package com.pride.summoner_searcher_api.config

import com.warrenstrange.googleauth.GoogleAuthenticator
import com.warrenstrange.googleauth.GoogleAuthenticatorConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class TwoFactorAuthConfig {

    @Bean
    fun googleAuthenticator(): GoogleAuthenticator {
        val config = GoogleAuthenticatorConfig.GoogleAuthenticatorConfigBuilder()
            .setWindowSize(5) // Allow for a 2.5-minute clock drift
            .build()
        return GoogleAuthenticator(config)
    }
}
