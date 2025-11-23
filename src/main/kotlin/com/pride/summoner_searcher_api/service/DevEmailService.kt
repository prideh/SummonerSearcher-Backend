package com.pride.summoner_searcher_api.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("dev")
class DevEmailService(
    @Value("\${app.frontend.url}") private val frontendUrl: String
) : EmailSender {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun sendVerificationEmail(to: String, token: String) {
        val verificationLink = "$frontendUrl/verify-email?token=$token"
        logger.info("--- DEV EMAIL ---")
        logger.info("To: {}", to)
        logger.info("Subject: Verify Your Account")
        logger.info("Body: Please click the following link to verify your email: {}", verificationLink)
        logger.info("--- END DEV EMAIL ---")
        
        // Write token to file for AI verification
        java.io.File("token.txt").writeText(token)
    }

    override fun sendPasswordResetEmail(to: String, token: String) {
        val resetLink = "$frontendUrl/reset-password?token=$token"
        logger.info("--- DEV EMAIL ---")
        logger.info("To: {}", to)
        logger.info("Subject: Password Reset Request")
        logger.info("Body: Please click the following link to reset your password: {}", resetLink)
        logger.info("--- END DEV EMAIL ---")
    }
}
