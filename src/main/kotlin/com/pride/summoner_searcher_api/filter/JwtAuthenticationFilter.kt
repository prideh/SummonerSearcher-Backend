package com.pride.summoner_searcher_api.filter

import com.pride.summoner_searcher_api.util.JwtUtil
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

/**
 * A custom Spring Security filter that intercepts every incoming request once.
 * Its primary purpose is to validate the JWT (JSON Web Token) from the Authorization header
 * and set the user's authentication context if the token is valid.
 */
@Component
class JwtAuthenticationFilter(
    private val userDetailsService: UserDetailsService,
    private val jwtUtil: JwtUtil
) : OncePerRequestFilter() {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * The core logic of the filter. It is executed for each incoming request.
     */
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val requestUri = request.requestURI
        
        // Log all /api/ai requests for debugging
        if (requestUri.startsWith("/api/ai")) {
            log.info("JWT Filter processing /api/ai request: $requestUri")
        }
        
        // 1. Extract the "Authorization" header, which should contain the JWT.
        val authHeader: String? = request.getHeader("Authorization")

        // 2. If the header is missing or doesn't start with "Bearer ", it's not a JWT request.
        //    In this case, we simply continue the filter chain without setting any authentication.
        //    If the endpoint is protected, a later filter will block the request.
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            if (requestUri.startsWith("/api/ai")) {
                log.warn("AI endpoint request missing or invalid Authorization header")
            }
            filterChain.doFilter(request, response)
            return
        }

        // 3. Extract the token string by removing the "Bearer " prefix.
        val jwt: String = authHeader.substring(7)

        try {
            // 4. Extract the user's email from the token's claims.
            val userEmail: String = jwtUtil.getEmailFromToken(jwt)

            // 5. Check if there is already an authentication object in the security context.
            //    If it's null, it means this is the first time we are authenticating this request.
            if (SecurityContextHolder.getContext().authentication == null) {
                // 6. Load the user's details from the database using the email from the token.
                //    This also implicitly checks if the user exists and is verified.
                val userDetails = this.userDetailsService.loadUserByUsername(userEmail)

                // 7. Validate the token's signature and expiration date.
                if (jwtUtil.validateToken(jwt)) {
                    // 8. If the token is valid, create an authentication object.
                    val authToken = UsernamePasswordAuthenticationToken(
                        userDetails,
                        null, // Credentials are not needed as we are using a pre-authenticated JWT.
                        userDetails.authorities
                    )
                    // 9. Set additional details about the request on the authentication object.
                    authToken.details = WebAuthenticationDetailsSource().buildDetails(request)
                    
                    // 10. Set the authentication object in the SecurityContextHolder.
                    //     Spring Security now considers this request to be authenticated.
                    SecurityContextHolder.getContext().authentication = authToken
                    
                    if (request.requestURI.startsWith("/api/ai")) {
                        log.info("AI endpoint authentication successful for user: $userEmail")
                    }
                }
            }
        } catch (e: Exception) {
            // If any exception occurs during token parsing or validation (e.g., expired token, invalid signature),
            // we log the error as debug to prevent spamming the console on public endpoints,
            // and ensure the security context is cleared to prevent any partial authentication state.
            log.debug("Cannot set user authentication: {}", e.message)
            SecurityContextHolder.clearContext()
        }

        // 11. Continue the filter chain to the next filter or the controller.
        filterChain.doFilter(request, response)
    }
}
