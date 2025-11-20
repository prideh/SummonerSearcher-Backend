package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.repository.UserRepository
import com.pride.summoner_searcher_api.service.EncryptionService
import com.pride.summoner_searcher_api.service.TwoFactorAuthService
import com.pride.summoner_searcher_api.service.UserService
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*
import java.security.Principal

data class ChangePasswordRequest(val oldPassword: String, val newPassword: String)
data class DeleteAccountRequest(val password: String)
data class TwoFactorEnableRequest(val secret: String, val code: Int)
data class TwoFactorDisableRequest(val code: Int)
data class TwoFactorEnableResponse(val secret: String, val qrCodeDataUri: String)

@RestController
@RequestMapping("/api/user")
class UserController(
    private val userRepository: UserRepository,
    private val passwordEncoder: PasswordEncoder,
    private val twoFactorAuthService: TwoFactorAuthService,
    private val encryptionService: EncryptionService,
    private val userService: UserService,
    @Value("\${dummy.user.email}") private val dummyUserEmail: String
) {

    private fun isDummyUser(principal: Principal): Boolean {
        return principal.name.equals(dummyUserEmail, ignoreCase = true)
    }

    @GetMapping("/recent-searches")
    fun getRecentSearches(): ResponseEntity<List<String>> {
        val authentication = SecurityContextHolder.getContext().authentication
        val userEmail = authentication.name
        val user = userService.findByEmail(userEmail)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)

        return ResponseEntity.ok(user.recentSearches)
    }

    @PostMapping("/recent-searches/clear")
    @Transactional
    fun clearRecentSearches(): ResponseEntity<String> {
        val authentication = SecurityContextHolder.getContext().authentication
        val userEmail = authentication.name
        val user = userService.findByEmail(userEmail)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        userService.clearRecentSearches(user)
        return ResponseEntity.ok("Recent searches cleared successfully.")
    }

    @PostMapping("/change-password")
    @Transactional
    fun changePassword(principal: Principal, @RequestBody request: ChangePasswordRequest): ResponseEntity<String> {
        if (isDummyUser(principal)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("The dummy account's password cannot be changed.")
        }

        val user = userRepository.findByEmail(principal.name)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (!passwordEncoder.matches(request.oldPassword, user.password)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Incorrect old password.")
        }

        user.password = passwordEncoder.encode(request.newPassword)
        userRepository.save(user)

        return ResponseEntity.ok("Password changed successfully.")
    }

    @PostMapping("/delete-account")
    @Transactional
    fun deleteAccount(principal: Principal, @RequestBody request: DeleteAccountRequest): ResponseEntity<String> {
        if (isDummyUser(principal)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("The dummy account cannot be deleted.")
        }

        val user = userRepository.findByEmail(principal.name)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (user.twoFactorEnabled) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Please disable Two-Factor Authentication before deleting your account.")
        }

        if (!passwordEncoder.matches(request.password, user.password)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Incorrect password.")
        }

        userRepository.delete(user)

        return ResponseEntity.ok("Account deleted successfully.")
    }

    @GetMapping("/2fa/enable")
    fun enableTwoFactorAuth(principal: Principal): ResponseEntity<Any> {
        if (isDummyUser(principal)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("2FA cannot be enabled for the dummy account.")
        }

        val user = userRepository.findByEmail(principal.name)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).build()

        val secret = twoFactorAuthService.generateNewSecret()
        val qrCodeDataUri = twoFactorAuthService.createQrCodeDataUri(secret, user.email, "SummonerSearcher")

        return ResponseEntity.ok(TwoFactorEnableResponse(secret, qrCodeDataUri))
    }

    @PostMapping("/2fa/verify-enable")
    @Transactional
    fun verifyAndEnableTwoFactorAuth(@RequestBody request: TwoFactorEnableRequest, principal: Principal): ResponseEntity<String> {
        if (isDummyUser(principal)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("2FA cannot be enabled for the dummy account.")
        }

        val user = userRepository.findByEmail(principal.name)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (!twoFactorAuthService.isCodeValid(request.secret, request.code)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid 2FA code.")
        }

        user.twoFactorSecret = encryptionService.encrypt(request.secret)
        user.twoFactorEnabled = true
        userRepository.save(user)

        return ResponseEntity.ok("2FA has been enabled successfully.")
    }

    @PostMapping("/2fa/disable")
    @Transactional
    fun disableTwoFactorAuth(@RequestBody request: TwoFactorDisableRequest, principal: Principal): ResponseEntity<String> {
        val user = userRepository.findByEmail(principal.name)
            ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.")

        if (!user.twoFactorEnabled || user.twoFactorSecret == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("2FA is not enabled.")
        }

        val decryptedSecret = encryptionService.decrypt(user.twoFactorSecret!!)
        if (!twoFactorAuthService.isCodeValid(decryptedSecret, request.code)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid 2FA code.")
        }

        user.twoFactorEnabled = false
        user.twoFactorSecret = null
        userRepository.save(user)

        return ResponseEntity.ok("2FA has been disabled successfully.")
    }
}
