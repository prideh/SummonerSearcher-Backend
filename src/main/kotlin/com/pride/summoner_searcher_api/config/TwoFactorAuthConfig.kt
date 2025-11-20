package com.pride.summoner_searcher_api.config

import com.warrenstrange.googleauth.GoogleAuthenticator
import com.warrenstrange.googleauth.GoogleAuthenticatorConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Configures the beans required for Two-Factor Authentication (2FA).
 */
@Configuration
class TwoFactorAuthConfig {

    /**
     * Creates a singleton GoogleAuthenticator bean for generating and verifying 2FA codes.
     *
     * This bean is configured with a window size of 5. This allows for more flexibility with
     * clock drift between the server and the user's device. Each window represents 30 seconds,
     * so a size of 5 allows for a code to be valid for 2.5 minutes (the current code, plus
     * two codes before and two codes after).
     *
     * @return A configured GoogleAuthenticator instance.
     */
    @Bean
    fun googleAuthenticator(): GoogleAuthenticator {
        val config = GoogleAuthenticatorConfig.GoogleAuthenticatorConfigBuilder()
            .setWindowSize(5)
            .build()
        return GoogleAuthenticator(config)
    }
}
