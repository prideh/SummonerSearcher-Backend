package com.pride.summoner_searcher_api.handler

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.MediaType
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.core.AuthenticationException
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class CustomAuthenticationEntryPoint : AuthenticationEntryPoint {

    override fun commence(
        request: HttpServletRequest,
        response: HttpServletResponse,
        authException: AuthenticationException
    ) {
        response.contentType = MediaType.APPLICATION_JSON_VALUE
        response.status = HttpServletResponse.SC_UNAUTHORIZED

        val errorMessage = when (authException) {
            is BadCredentialsException, is UsernameNotFoundException -> "Invalid email or password"
            else -> authException.message ?: "Authentication failed"
        }

        val errorDetails = mutableMapOf<String, Any>()
        errorDetails["timestamp"] = LocalDateTime.now().toString()
        errorDetails["status"] = HttpServletResponse.SC_UNAUTHORIZED
        errorDetails["error"] = "Unauthorized"
        errorDetails["message"] = errorMessage
        errorDetails["path"] = request.servletPath

        val objectMapper = ObjectMapper()
        response.writer.write(objectMapper.writeValueAsString(errorDetails))
    }
}
