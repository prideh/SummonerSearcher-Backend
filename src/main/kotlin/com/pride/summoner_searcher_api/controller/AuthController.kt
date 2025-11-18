package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import com.pride.summoner_searcher_api.service.EmailService
import com.pride.summoner_searcher_api.service.EncryptionService
import com.pride.summoner_searcher_api.service.TwoFactorAuthService
import com.pride.summoner_searcher_api.util.JwtUtil
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.userdetails.UserDetails
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

// The definitive successful login response
data class AuthResponse(
    val jwt: String,
    val twoFactorEnabled: Boolean,
    val darkmodePreference: Boolean,
    val recentSearches: List<String>
)

// A response used only to signal that 2FA is required
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

    @PostMapping("/login")
    fun createAuthenticationToken(@RequestBody authRequest: AuthRequest): ResponseEntity<Any> {
        authenticationManager.authenticate(
            UsernamePasswordAuthenticationToken(authRequest.email, authRequest.password)
        )
        val user = userRepository.findByEmail(authRequest.email)
            ?: throw RuntimeException("User not found after authentication")

        return if (user.twoFactorEnabled) {
            // User has 2FA enabled, return a temporary token
            val tempToken = jwtUtil.generateToken(user.email)
            ResponseEntity.ok(TwoFactorRequiredResponse(twoFactorRequired = true, tempToken = tempToken))
        } else {
            // User does not have 2FA, return the full AuthResponse
            val jwt = jwtUtil.generateToken(user.email)
            ResponseEntity.ok(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
        }
    }

    @PostMapping("/2fa-login")
    fun twoFactorLogin(@RequestBody request: TwoFactorLoginRequest): ResponseEntity<Any> {
        val email = jwtUtil.getEmailFromToken(request.tempToken)
        val user = userRepository.findByEmail(email)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (!user.twoFactorEnabled || user.twoFactorSecret == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("2FA is not enabled for this user.")
        }

        val decryptedSecret = encryptionService.decrypt(user.twoFactorSecret!!)
        if (!twoFactorAuthService.isCodeValid(decryptedSecret, request.code)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid 2FA code.")
        }

        // 2FA code is valid, generate the full AuthResponse
        val jwt = jwtUtil.generateToken(user.email)
        return ResponseEntity.ok(AuthResponse(jwt, user.twoFactorEnabled, user.darkmodePreference, user.recentSearches))
    }

    @PostMapping("/register")
    @Transactional
    fun registerUser(@RequestBody authRequest: AuthRequest): ResponseEntity<String> {
        if (userRepository.findByEmail(authRequest.email) != null) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Email already in use")
        }

        val encodedPassword = passwordEncoder.encode(authRequest.password)
        val verificationToken = UUID.randomUUID().toString()
        val newUser = User(
            email = authRequest.email,
            password = encodedPassword,
            darkmodePreference = false,
            verified = false,
            verificationToken = verificationToken,
            verificationTokenExpiry = LocalDateTime.now().plusHours(24),
            recentSearches = mutableListOf()
        )
        userRepository.save(newUser)

        emailService.sendVerificationEmail(newUser.email, verificationToken)

        return ResponseEntity.status(HttpStatus.CREATED).body("Registration successful. Please check your email to verify your account.")
    }

    @GetMapping("/verify")
    @Transactional
    fun verifyAccount(@RequestParam("token") token: String): ResponseEntity<String> {
        val user = userRepository.findByVerificationToken(token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid verification token.")

        if (user.verificationTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Verification token has expired. Please register again.")
        }

        user.verified = true
        user.verificationToken = null
        user.verificationTokenExpiry = null
        userRepository.save(user)

        return ResponseEntity.ok("Your account has been verified. You can now log in.")
    }

    @PostMapping("/forgot-password")
    @Transactional
    fun forgotPassword(@RequestBody request: ForgotPasswordRequest): ResponseEntity<String> {
        val user = userRepository.findByEmail(request.email)
        if (user != null) {
            val token = UUID.randomUUID().toString()
            user.passwordResetToken = token
            user.passwordResetTokenExpiry = LocalDateTime.now().plusHours(1) // 1-hour expiry
            userRepository.save(user)
            emailService.sendPasswordResetEmail(user.email, token)
            return ResponseEntity.ok("A password reset link has been sent to your email.") // Specific success message
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No account found with this email address.") // Specific error message
        }
    }

    @PostMapping("/reset-password")
    @Transactional
    fun resetPassword(@RequestBody request: ResetPasswordRequest): ResponseEntity<String> {
        val user = userRepository.findByPasswordResetToken(request.token)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Invalid or expired password reset token.")

        if (user.passwordResetTokenExpiry?.isBefore(LocalDateTime.now()) == true) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid or expired password reset token.")
        }

        user.password = passwordEncoder.encode(request.newPassword)
        user.passwordResetToken = null
        user.passwordResetTokenExpiry = null
        userRepository.save(user)

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
