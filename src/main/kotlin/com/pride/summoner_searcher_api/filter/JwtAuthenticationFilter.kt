package com.pride.summoner_searcher_api.filter

import com.pride.summoner_searcher_api.util.JwtUtil
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class JwtAuthenticationFilter(
    private val userDetailsService: UserDetailsService,
    private val jwtUtil: JwtUtil
) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        // Bypass filter for public auth endpoints
        if (request.servletPath.startsWith("/api/auth")) {
            filterChain.doFilter(request, response)
            return
        }

        val header = request.getHeader("Authorization")

        if (header == null || !header.startsWith("Bearer ")) {
            filterChain.doFilter(request, response)
            return
        }

        val token = header.substring(7)
        try {
            val email = jwtUtil.getEmailFromToken(token)

            if (SecurityContextHolder.getContext().authentication == null) {
                val userDetails = this.userDetailsService.loadUserByUsername(email)

                if (jwtUtil.validateToken(token)) {
                    val authentication = UsernamePasswordAuthenticationToken(
                        userDetails, null, userDetails.authorities
                    )
                    authentication.details = WebAuthenticationDetailsSource().buildDetails(request)
                    SecurityContextHolder.getContext().authentication = authentication
                }
            }
        } catch (e: Exception) {
            // In case of invalid token, just clear the context and continue the chain
            // The endpoint-level security will handle the unauthorized access
            SecurityContextHolder.clearContext()
        }

        filterChain.doFilter(request, response)
    }
}
