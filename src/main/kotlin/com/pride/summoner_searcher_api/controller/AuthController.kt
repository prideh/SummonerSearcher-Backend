package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import com.pride.summoner_searcher_api.service.EmailService
import com.pride.summoner_searcher_api.service.EncryptionService
import com.pride.summoner_searcher_api.service.TwoFactorAuthService
import com.pride.summoner_searcher_api.util.JwtUtil
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.util.UUID

data class AuthRequest(val email: String, val password: String)
data class TwoFactorLoginRequest(val tempToken: String, val code: Int)
data class ForgotPasswordRequest(val email: String)
data class ResetPasswordRequest(val token: String, val newPassword: String)

data class AuthResponse(
    val jwt: String,
    val twoFactorEnabled: Boolean,
    val darkmodePreference: Boolean,
    val recentSearches: List<String>
)

data class TwoFactorRequiredResponse(val twoFactorRequired: Boolean, val tempToken: String)

@RestController
@RequestMapping("/api/auth")
class AuthController(
    private val authenticationManager: AuthenticationManager,
    private val userDetailsService: UserDetailsService,
    private val jwtUtil: JwtUtil,
    private val userRepository: UserRepository,
    private val passwordEncoder: PasswordEncoder,
    private val emailService: EmailService,
    private val twoFactorAuthService: TwoFactorAuthService,
    private val encryptionService: EncryptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

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

        if (user.twoFactorEnabled) {
            logger.info("Successful primary authentication for user: {}. 2FA required.", user.email)
            val tempToken = jwtUtil.generateToken(user.email)
            return ResponseEntity.ok(TwoFactorRequiredResponse(twoFactorRequired = true, tempToken = tempToken))
        } else {
            logger.info("Successful login for user: {}", user.email)
            val jwt = jwtUtil.generateToken(user.email)
            return ResponseEntity.ok(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
        }
    }

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
        return ResponseEntity.ok(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
    }

    @PostMapping("/register")
    @Transactional
    fun registerUser(@RequestBody authRequest: AuthRequest): ResponseEntity<String> {
        val existingUser = userRepository.findByEmail(authRequest.email)

        val userToSave = if (existingUser != null && !existingUser.verified) {
            // User exists but is not verified, update their record
            logger.info("Updating existing unverified user: {}", authRequest.email)
            existingUser.apply {
                password = passwordEncoder.encode(authRequest.password)
                verificationToken = UUID.randomUUID().toString()
                verificationTokenExpiry = LocalDateTime.now().plusHours(24)
            }
        } else if (existingUser != null && existingUser.verified) {
            // User exists and is verified, deny registration
            logger.warn("Registration attempt with existing, verified email: {}", authRequest.email)
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Email already in use")
        } else {
            // No user exists, create a new one
            logger.info("Registering new user: {}", authRequest.email)
            User(
                email = authRequest.email,
                password = passwordEncoder.encode(authRequest.password),
                darkmodePreference = false,
                verified = false,
                verificationToken = UUID.randomUUID().toString(),
                verificationTokenExpiry = LocalDateTime.now().plusHours(24),
                recentSearches = mutableListOf()
            )
        }

        val savedUser = userRepository.save(userToSave)
        emailService.sendVerificationEmail(savedUser.email, savedUser.verificationToken!!)
        logger.info("Verification email sent for user: {}", savedUser.email)

        return ResponseEntity.status(HttpStatus.CREATED).body("Registration successful. Please check your email to verify your account.")
    }

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
            emailService.sendPasswordResetEmail(user.email, token)
            logger.info("Password reset token generated for user: {}", user.email)
            return ResponseEntity.ok("A password reset link has been sent to your email.")
        } else {
            logger.warn("Password reset requested for non-existent email: {}", request.email)
            return ResponseEntity.ok("If an account with this email exists, a password reset link has been sent.")
        }
    }

    @PostMapping("/reset-password")
    @Transactional
    fun resetPassword(@RequestBody request: ResetPasswordRequest): ResponseEntity<String> {
        val user = userRepository.findByPasswordResetToken(request.token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid or expired password reset token.")

        if (user.passwordResetTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            logger.warn("Expired password reset token used for user: {}", user.email)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid or expired password reset token.")
        }

        user.password = passwordEncoder.encode(request.newPassword)
        user.passwordResetToken = null
        user.passwordResetTokenExpiry = null
        userRepository.save(user)
        logger.info("Password successfully reset for user: {}", user.email)

        return ResponseEntity.ok("Your password has been reset successfully. You can now log in.")
    }

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
