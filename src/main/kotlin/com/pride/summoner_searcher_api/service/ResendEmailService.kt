package com.pride.summoner_searcher_api.service

import com.resend.Resend
import com.resend.services.emails.model.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service

@Service
@Profile("prod")
class ResendEmailService(
    @Value("\${RESEND_API_KEY:}") private val resendApiKey: String,
    @Value("\${resend.from.email}") private val fromEmail: String,
    @Value("\${app.frontend.url}") private val frontendUrl: String
) : EmailSender {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Async
    override fun sendVerificationEmail(to: String, token: String) {
        val subject = "Verify Your Account"
        val verificationLink = "$frontendUrl/verify-email?token=$token"

        val htmlBody = """
            <html>
                <body>
                    <h2>Welcome to Summoner Searcher!</h2>
                    <p>Please click the button below to verify your email address.</p>
                    <a href="$verificationLink" style="background-color:#007bff;color:white;padding:10px 15px;text-decoration:none;border-radius:5px;">Verify Email</a>
                    <p>If you cannot click the button, copy and paste this link into your browser:</p>
                    <p>$verificationLink</p>
                </body>
            </html>
        """.trimIndent()

        sendEmail(to, subject, htmlBody)
    }

    @Async
    override fun sendPasswordResetEmail(to: String, token: String) {
        val subject = "Password Reset Request"
        val resetLink = "$frontendUrl/reset-password?token=$token"

        val htmlBody = """
            <html>
                <body>
                    <h2>Password Reset Request</h2>
                    <p>You requested a password reset. Please click the button below to set a new password.</p>
                    <a href="$resetLink" style="background-color:#007bff;color:white;padding:10px 15px;text-decoration:none;border-radius:5px;">Reset Password</a>
                    <p>If you did not request a password reset, please ignore this email.</p>
                    <p>If you cannot click the button, copy and paste this link into your browser:</p>
                    <p>$resetLink</p>
                </body>
            </html>
        """.trimIndent()

        sendEmail(to, subject, htmlBody)
    }

    private fun sendEmail(to: String, subject: String, htmlBody: String) {
        if (resendApiKey.isBlank()) {
            logger.warn("Resend API Key is not configured. Email not sent.")
            return
        }

        try {
            val resend = Resend(resendApiKey)

            val params = CreateEmailOptions.builder()
                .from(fromEmail)
                .to(to)
                .subject(subject)
                .html(htmlBody)
                .build()

            val data = resend.emails().send(params)
            logger.info("Email with subject '{}' sent to {}. Id: {}", subject, to, data.id)
        } catch (ex: Exception) {
            logger.error("Error sending email to {}: {}", to, ex.message)
        }
    }
}
