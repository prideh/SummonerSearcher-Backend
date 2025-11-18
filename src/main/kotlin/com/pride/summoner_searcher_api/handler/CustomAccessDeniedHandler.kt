package com.pride.summoner_searcher_api.handler

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.MediaType
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.web.access.AccessDeniedHandler
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class CustomAccessDeniedHandler : AccessDeniedHandler {

    override fun handle(
        request: HttpServletRequest,
        response: HttpServletResponse,
        accessDeniedException: AccessDeniedException
    ) {
        response.contentType = MediaType.APPLICATION_JSON_VALUE
        response.status = HttpServletResponse.SC_FORBIDDEN

        val errorDetails = mutableMapOf<String, Any>()
        errorDetails["timestamp"] = LocalDateTime.now().toString()
        errorDetails["status"] = HttpServletResponse.SC_FORBIDDEN
        errorDetails["error"] = "Forbidden"
        errorDetails["message"] = "Access Denied: ${accessDeniedException.message}"
        errorDetails["path"] = request.servletPath

        val objectMapper = ObjectMapper()
        response.writer.write(objectMapper.writeValueAsString(errorDetails))
    }
}
