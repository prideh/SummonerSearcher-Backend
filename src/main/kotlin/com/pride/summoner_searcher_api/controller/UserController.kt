package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import com.pride.summoner_searcher_api.service.EncryptionService
import com.pride.summoner_searcher_api.service.TwoFactorAuthService
import com.pride.summoner_searcher_api.service.UserService
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*

data class ChangePasswordRequest(val oldPassword: String, val newPassword: String)
data class DeleteAccountRequest(val password: String)
data class TwoFactorEnableRequest(val secret: String, val code: Int)
data class TwoFactorDisableRequest(val code: Int)
data class TwoFactorEnableResponse(val secret: String, val qrCodeDataUri: String)
data class DarkModeRequest(val enabled: Boolean)

/**
 * Controller for handling all user-specific actions, such as managing profile settings and account details.
 * All endpoints in this controller require the user to be authenticated.
 */
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

    /**
     * A private helper to check if the authenticated user is the protected dummy user.
     */
    private fun isDummyUser(user: User): Boolean {
        return user.email.equals(dummyUserEmail, ignoreCase = true)
    }

    /**
     * Retrieves the recent search history for the currently authenticated user.
     */
    @GetMapping("/recent-searches")
    fun getRecentSearches(@CurrentUser user: User): ResponseEntity<List<com.pride.summoner_searcher_api.model.RecentSearch>> {
        return ResponseEntity.ok(user.recentSearches)
    }

    /**
     * Clears the recent search history for the currently authenticated user.
     */
    @PostMapping("/recent-searches/clear")
    @Transactional
    fun clearRecentSearches(@CurrentUser user: User): ResponseEntity<String> {
        userService.clearRecentSearches(user)
        return ResponseEntity.ok("Recent searches cleared successfully.")
    }

    /**
     * Updates the dark mode preference for the currently authenticated user.
     */
    @PostMapping("/settings/darkmode")
    @Transactional
    fun updateDarkMode(@CurrentUser user: User, @RequestBody request: DarkModeRequest): ResponseEntity<String> {
        user.darkmodePreference = request.enabled
        userRepository.save(user)
        return ResponseEntity.ok("Dark mode preference updated successfully.")
    }

    /**
     * Changes the password for the currently authenticated user.
     */
    @PostMapping("/change-password")
    @Transactional
    fun changePassword(@CurrentUser user: User, @RequestBody request: ChangePasswordRequest): ResponseEntity<String> {
        if (isDummyUser(user)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("The dummy account's password cannot be changed.")
        }

        if (!passwordEncoder.matches(request.oldPassword, user.hashedPassword)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Incorrect old password.")
        }

        user.hashedPassword = passwordEncoder.encode(request.newPassword)
        userRepository.save(user)

        return ResponseEntity.ok("Password changed successfully.")
    }

    /**
     * Deletes the account of the currently authenticated user.
     */
    @PostMapping("/delete-account")
    @Transactional
    fun deleteAccount(@CurrentUser user: User, @RequestBody request: DeleteAccountRequest): ResponseEntity<String> {
        if (isDummyUser(user)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("The dummy account cannot be deleted.")
        }

        if (user.twoFactorEnabled) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Please disable Two-Factor Authentication before deleting your account.")
        }

        if (!passwordEncoder.matches(request.password, user.hashedPassword)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Incorrect password.")
        }

        userRepository.delete(user)

        return ResponseEntity.ok("Account deleted successfully.")
    }

    /**
     * Starts the 2FA setup process by generating a new secret and a corresponding QR code.
     * @return A response containing the secret and the QR code data URI for the frontend to display.
     */
    @GetMapping("/2fa/enable")
    fun enableTwoFactorAuth(@CurrentUser user: User): ResponseEntity<Any> {
        if (isDummyUser(user)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("2FA cannot be enabled for the dummy account.")
        }

        val secret = twoFactorAuthService.generateNewSecret()
        val qrCodeDataUri = twoFactorAuthService.createQrCodeDataUri(secret, user.email, "SummonerSearcher")

        return ResponseEntity.ok(TwoFactorEnableResponse(secret, qrCodeDataUri))
    }

    /**
     * Verifies the 2FA code provided by the user and permanently enables 2FA for their account.
     */
    @PostMapping("/2fa/verify-enable")
    @Transactional
    fun verifyAndEnableTwoFactorAuth(@CurrentUser user: User, @RequestBody request: TwoFactorEnableRequest): ResponseEntity<String> {
        if (isDummyUser(user)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("2FA cannot be enabled for the dummy account.")
        }

        if (!twoFactorAuthService.isCodeValid(request.secret, request.code)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid 2FA code.")
        }

        user.twoFactorSecret = encryptionService.encrypt(request.secret)
        user.twoFactorEnabled = true
        userRepository.save(user)

        return ResponseEntity.ok("2FA has been enabled successfully.")
    }

    /**
     * Disables 2FA for the user after verifying a final 2FA code.
     */
    @PostMapping("/2fa/disable")
    @Transactional
    fun disableTwoFactorAuth(@CurrentUser user: User, @RequestBody request: TwoFactorDisableRequest): ResponseEntity<String> {
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
