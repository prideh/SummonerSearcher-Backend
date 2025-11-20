package com.pride.summoner_searcher_api.config

import com.pride.summoner_searcher_api.filter.JwtAuthenticationFilter
import com.pride.summoner_searcher_api.handler.CustomAccessDeniedHandler
import com.pride.summoner_searcher_api.handler.CustomAuthenticationEntryPoint
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

/**
 * Configures the application's security settings, including authentication, authorization, and CORS.
 */
@Configuration
@EnableWebSecurity
class SecurityConfig(
    private val jwtAuthenticationFilter: JwtAuthenticationFilter,
    private val customAuthenticationEntryPoint: CustomAuthenticationEntryPoint,
    private val customAccessDeniedHandler: CustomAccessDeniedHandler
) {

    // Injects the allowed CORS origins from application properties, with a default for local development.
    @Value("\${cors.allowed-origins:http://localhost:5173}")
    private lateinit var allowedOrigins: String

    /**
     * Defines the main security filter chain that protects the application's endpoints.
     * This is the heart of the web security configuration.
     */
    @Bean
    fun securityFilterChain(http: HttpSecurity): SecurityFilterChain {
        http
            // Disable CSRF protection, as it's not needed for a stateless REST API that uses JWTs.
            .csrf { it.disable() }
            // Enable and configure CORS using the bean defined below.
            .cors { cors -> cors.configurationSource(corsConfigurationSource()) }
            // Configure custom entry points for authentication errors.
            .exceptionHandling {
                it.authenticationEntryPoint(customAuthenticationEntryPoint) // Handles errors for unauthenticated users.
                it.accessDeniedHandler(customAccessDeniedHandler)       // Handles errors for authenticated users with insufficient permissions.
            }
            // Define authorization rules for all HTTP requests.
            .authorizeHttpRequests { auth ->
                auth
                    // Public endpoints: Allow all requests to authentication-related paths.
                    .requestMatchers("/api/auth/**").permitAll()
                    // Protected endpoints: Require authentication for all other API paths.
                    .requestMatchers("/api/user/**").authenticated()
                    .requestMatchers("/api/riot/**").authenticated()
                    // Default security rule: Deny any request that doesn't match the rules above.
                    .anyRequest().denyAll()
            }
            // Configure session management to be stateless, as we are using JWTs for authentication.
            .sessionManagement { session ->
                session.sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            }
            // Add the custom JWT filter to the chain before the standard username/password filter.
            .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter::class.java)

        return http.build()
    }

    /**
     * Provides a PasswordEncoder bean to the Spring context for hashing passwords.
     * BCrypt is the industry standard for password hashing.
     */
    @Bean
    fun passwordEncoder(): PasswordEncoder {
        return BCryptPasswordEncoder()
    }

    /**
     * Exposes the default AuthenticationManager as a bean for use in the AuthController.
     */
    @Bean
    fun authenticationManager(authenticationConfiguration: AuthenticationConfiguration): AuthenticationManager {
        return authenticationConfiguration.authenticationManager
    }

    /**
     * Configures the Cross-Origin Resource Sharing (CORS) policy for the application.
     * This is crucial for allowing the frontend (running on a different domain) to communicate with the backend.
     */
    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource {
        val configuration = CorsConfiguration()
        // The list of frontend domains that are allowed to make requests.
        configuration.allowedOrigins = allowedOrigins.split(",").map { it.trim() }
        // The HTTP methods that are allowed (e.g., GET, POST).
        configuration.allowedMethods = listOf("GET", "POST", "PUT", "DELETE", "OPTIONS")
        // The HTTP headers that the frontend is allowed to send.
        configuration.allowedHeaders = listOf("Authorization", "Content-Type")
        // Allows the browser to send credentials (like cookies or JWTs in headers).
        configuration.allowCredentials = true
        
        val source = UrlBasedCorsConfigurationSource()
        // Apply this CORS configuration to all paths in the application.
        source.registerCorsConfiguration("/**", configuration)
        return source
    }
}
