package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import com.pride.summoner_searcher_api.service.EmailSender
import com.pride.summoner_searcher_api.service.EncryptionService
import com.pride.summoner_searcher_api.service.RefreshTokenService
import com.pride.summoner_searcher_api.service.TwoFactorAuthService
import com.pride.summoner_searcher_api.util.JwtUtil
import org.springframework.beans.factory.annotation.Value
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseCookie
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.util.UUID

// --- DTOs for Authentication ---
data class AuthRequest(val email: String, val password: String)
data class TwoFactorLoginRequest(val tempToken: String, val code: Int)
data class ForgotPasswordRequest(val email: String)
data class ResetPasswordRequest(val token: String, val newPassword: String)

// --- DTOs for Authentication Responses ---
data class AuthResponse(
    val jwt: String,
    val twoFactorEnabled: Boolean,
    val darkmodePreference: Boolean,
    val recentSearches: List<com.pride.summoner_searcher_api.model.RecentSearch>
)
data class TwoFactorRequiredResponse(val twoFactorRequired: Boolean, val tempToken: String)

/**
 * Controller for handling all public authentication endpoints, such as login, registration, and password reset.
 */
@RestController
@RequestMapping("/api/auth")
class AuthController(
    private val authenticationManager: AuthenticationManager,
    private val jwtUtil: JwtUtil,
    private val userRepository: UserRepository,
    private val passwordEncoder: PasswordEncoder,
    private val emailSender: EmailSender,
    private val twoFactorAuthService: TwoFactorAuthService,
    private val encryptionService: EncryptionService,
    private val refreshTokenService: RefreshTokenService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Value("\${jwt.cookie.secure:false}")
    private var isCookieSecure: Boolean = false

    @Value("\${jwt.cookie.same-site:Strict}")
    private var cookieSameSite: String = "Strict"

    @Value("\${jwt.cookie.domain:}")
    private var cookieDomain: String = ""

    /**
     * Authenticates a user with their email and password.
     * If 2FA is enabled, it returns a temporary token. Otherwise, it returns the final authentication response.
     */
    @PostMapping("/login")
    fun createAuthenticationToken(@RequestBody authRequest: AuthRequest): ResponseEntity<Any> {
        try {
            authenticationManager.authenticate(
                UsernamePasswordAuthenticationToken(authRequest.email, authRequest.password)
            )
        } catch (e: BadCredentialsException) {
            logger.warn("Failed login attempt for email: {}", authRequest.email)
            throw e
        }

        val user = userRepository.findByEmail(authRequest.email)
            ?: throw RuntimeException("User not found after authentication")

        if (!user.verified) {
            logger.warn("Login attempt for unverified user: {}", user.email)
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Account not verified. Please check your email to verify your account.")
        }

        if (user.twoFactorEnabled) {
            logger.info("Successful primary authentication for user: {}. 2FA required.", user.email)
            val tempToken = jwtUtil.generateToken(user.email)
            return ResponseEntity.ok(TwoFactorRequiredResponse(twoFactorRequired = true, tempToken = tempToken))
        } else {
            logger.info("Successful login for user: {}", user.email)
            val jwt = jwtUtil.generateToken(user.email)
            val refreshToken = refreshTokenService.createRefreshToken(user.email)
            
            val cookieBuilder = ResponseCookie.from("refreshToken", refreshToken.token)
                .httpOnly(true)
                .secure(isCookieSecure)
                .path("/")
                .maxAge(2592000) // 30 days
                .sameSite(cookieSameSite)

            if (cookieDomain.isNotEmpty()) {
                cookieBuilder.domain(cookieDomain)
            }

            val jwtCookie = cookieBuilder.build()

            return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, jwtCookie.toString())
                .body(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
        }
    }

    /**
     * Completes the login process for a user with 2FA enabled.
     * @param request The request containing the temporary token and the 2FA code.
     * @return The final [AuthResponse] upon successful verification.
     */
    @PostMapping("/2fa-login")
    fun twoFactorLogin(@RequestBody request: TwoFactorLoginRequest): ResponseEntity<Any> {
        val email = jwtUtil.getEmailFromToken(request.tempToken)
        val user = userRepository.findByEmail(email)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (!user.twoFactorEnabled || user.twoFactorSecret == null) {
            logger.warn("2FA login attempt for user {} where 2FA is not enabled.", email)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("2FA is not enabled for this user.")
        }

        val decryptedSecret = encryptionService.decrypt(user.twoFactorSecret!!)
        if (!twoFactorAuthService.isCodeValid(decryptedSecret, request.code)) {
            logger.warn("Invalid 2FA code attempt for user: {}", email)
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid 2FA code.")
        }

        logger.info("Successful 2FA login for user: {}", email)
        val jwt = jwtUtil.generateToken(user.email)
        val refreshToken = refreshTokenService.createRefreshToken(user.email)

        val cookieBuilder = ResponseCookie.from("refreshToken", refreshToken.token)
            .httpOnly(true)
            .secure(isCookieSecure)
            .path("/")
            .maxAge(2592000) // 30 days
            .sameSite(cookieSameSite)

        if (cookieDomain.isNotEmpty()) {
            cookieBuilder.domain(cookieDomain)
        }

        val jwtCookie = cookieBuilder.build()

        return ResponseEntity.ok()
            .header(HttpHeaders.SET_COOKIE, jwtCookie.toString())
            .body(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
    }

    @PostMapping("/refresh-token")
    fun refreshToken(@CookieValue(name = "refreshToken") requestRefreshToken: String?): ResponseEntity<Any> {
        if (requestRefreshToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Refresh Token is missing or expired.")
        }

        return try {
            val refreshToken = refreshTokenService.findByToken(requestRefreshToken)
                ?: throw RuntimeException("Refresh token is not in database!")
            
            refreshTokenService.verifyExpiration(refreshToken)
            val user = refreshToken.user
            val token = jwtUtil.generateToken(user.email)
            
            ResponseEntity.ok(mapOf("jwt" to token))
        } catch (e: Exception) {
            logger.error("Cannot refresh token: {}", e.message)
            ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.message)
        }
    }

    @PostMapping("/logout")
    fun logoutUser(): ResponseEntity<Any> {
        val cookieBuilder = ResponseCookie.from("refreshToken", "")
            .httpOnly(true)
            .secure(isCookieSecure)
            .path("/")
            .maxAge(0)
            .sameSite(cookieSameSite)

        if (cookieDomain.isNotEmpty()) {
            cookieBuilder.domain(cookieDomain)
        }

        val jwtCookie = cookieBuilder.build()
        
        return ResponseEntity.ok()
            .header(HttpHeaders.SET_COOKIE, jwtCookie.toString())
            .body("You've been signed out!")
    }

    /**
     * Registers a new user account.
     * If a user with the same email exists but is not verified, it updates their record and resends the verification email.
     */
    @PostMapping("/register")
    @Transactional
    fun registerUser(@RequestBody authRequest: AuthRequest): ResponseEntity<String> {
        val existingUser = userRepository.findByEmail(authRequest.email)

        val userToSave = if (existingUser != null && !existingUser.verified) {
            logger.info("Updating existing unverified user: {}", authRequest.email)
            existingUser.apply {
                hashedPassword = passwordEncoder.encode(authRequest.password)
                verificationToken = UUID.randomUUID().toString()
                verificationTokenExpiry = LocalDateTime.now().plusHours(24)
            }
        } else if (existingUser != null && existingUser.verified) {
            logger.warn("Registration attempt with existing, verified email: {}", authRequest.email)
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Email already in use")
        } else {
            logger.info("Registering new user: {}", authRequest.email)
            User(
                email = authRequest.email,
                hashedPassword = passwordEncoder.encode(authRequest.password),
                darkmodePreference = true, // Default to true for new users
                verified = false,
                verificationToken = UUID.randomUUID().toString(),
                verificationTokenExpiry = LocalDateTime.now().plusHours(24),
                recentSearches = mutableListOf()
            )
        }

        val savedUser = userRepository.save(userToSave)
        emailSender.sendVerificationEmail(savedUser.email, savedUser.verificationToken!!)
        logger.info("Verification email sent for user: {}", savedUser.email)

        return ResponseEntity.status(HttpStatus.CREATED).body("Registration successful. Please check your email to verify your account.")
    }

    /**
     * Verifies a user's account using the token sent to their email.
     */
    @GetMapping("/verify")
    @Transactional
    fun verifyAccount(@RequestParam("token") token: String): ResponseEntity<String> {
        val user = userRepository.findByVerificationToken(token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid verification token.")

        if (user.verificationTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            logger.warn("Expired verification token used for email: {}", user.email)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Verification token has expired. Please register again.")
        }

        user.verified = true
        user.verificationToken = null
        user.verificationTokenExpiry = null
        userRepository.save(user)
        logger.info("Account verified for user: {}", user.email)

        return ResponseEntity.ok("Your account has been verified. You can now log in.")
    }

    /**
     * Initiates the password reset process for a user.
     */
    @PostMapping("/forgot-password")
    @Transactional
    fun forgotPassword(@RequestBody request: ForgotPasswordRequest): ResponseEntity<String> {
        val user = userRepository.findByEmail(request.email)
        if (user != null) {
            if (!user.verified) {
                logger.warn("Password reset attempt for unverified user: {}", user.email)
                return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Please verify your email address before resetting your password.")
            }

            val token = UUID.randomUUID().toString()
            user.passwordResetToken = token
            user.passwordResetTokenExpiry = LocalDateTime.now().plusHours(1)
            userRepository.save(user)
            emailSender.sendPasswordResetEmail(user.email, token)
            logger.info("Password reset token generated for user: {}", user.email)
        } else {
            logger.warn("Password reset requested for non-existent email: {}", request.email)
        }
        // Always return a generic success message to prevent email enumeration attacks.
        return ResponseEntity.ok("If an account with this email exists, a password reset link has been sent.")
    }

    /**
     * Resets the user's password using a valid reset token.
     */
    @PostMapping("/reset-password")
    @Transactional
    fun resetPassword(@RequestBody request: ResetPasswordRequest): ResponseEntity<String> {
        val user = userRepository.findByPasswordResetToken(request.token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid or expired password reset token.")

        if (user.passwordResetTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            logger.warn("Expired password reset token used for user: {}", user.email)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid or expired password reset token.")
        }

        user.hashedPassword = passwordEncoder.encode(request.newPassword)
        user.passwordResetToken = null
        user.passwordResetTokenExpiry = null
        userRepository.save(user)
        logger.info("Password successfully reset for user: {}", user.email)

        return ResponseEntity.ok("Your password has been reset successfully. You can now log in.")
    }

    /**
     * Validates a password reset token to check if it's still valid for use.
     */
    @GetMapping("/validate-reset-token")
    fun validateResetToken(@RequestParam("token") token: String): ResponseEntity<String> {
        val user = userRepository.findByPasswordResetToken(token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid password reset token.")

        if (user.passwordResetTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Password reset token has expired.")
        }

        return ResponseEntity.ok("Token is valid.")
    }
}
