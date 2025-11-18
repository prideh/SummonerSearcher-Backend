package com.pride.summoner_searcher_api.service

import com.sendgrid.Method
import com.sendgrid.Request
import com.sendgrid.SendGrid
import com.sendgrid.helpers.mail.Mail
import com.sendgrid.helpers.mail.objects.Content
import com.sendgrid.helpers.mail.objects.Email
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.io.IOException

@Service
@Profile("prod")
class SendGridEmailService(
    // By adding a default value of "", the app won't crash if the env var is missing in dev
    @Value("\${SENDGRID_API_KEY:}") private val sendGridApiKey: String,
    @Value("\${sendgrid.from.email}") private val fromEmail: String,
    @Value("\${app.frontend.url}") private val frontendUrl: String
) : EmailSender {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Async
    override fun sendVerificationEmail(to: String, token: String) {
        val subject = "Verify Your Account"
        val verificationLink = "$frontendUrl/verify-email?token=$token"
        
        val htmlBody = """
            <html>...</html>
        """.trimIndent() // Your HTML body

        val textBody = "Please click the following link to verify your email: $verificationLink"
        
        sendEmail(to, subject, textBody, htmlBody)
    }

    @Async
    override fun sendPasswordResetEmail(to: String, token: String) {
        val subject = "Password Reset Request"
        val resetLink = "$frontendUrl/reset-password?token=$token"

        val htmlBody = """
            <html>...</html>
        """.trimIndent() // Your HTML body

        val textBody = "Please click the following link to reset your password: $resetLink"

        sendEmail(to, subject, textBody, htmlBody)
    }

    private fun sendEmail(to: String, subject: String, textBody: String, htmlBody: String) {
        if (sendGridApiKey.isBlank()) {
            logger.warn("SendGrid API Key is not configured. Email not sent.")
            return
        }

        val from = Email(fromEmail)
        val toEmail = Email(to)
        
        val textContent = Content("text/plain", textBody)
        val htmlContent = Content("text/html", htmlBody)

        val mail = Mail(from, subject, toEmail, textContent)
        mail.addContent(htmlContent)

        val sg = SendGrid(sendGridApiKey)
        val request = Request()
        try {
            request.method = Method.POST
            request.endpoint = "mail/send"
            request.body = mail.build()
            val response = sg.api(request)
            if (response.statusCode >= 400) {
                logger.error("SendGrid error sending email to {}: Status Code: {}, Body: {}", to, response.statusCode, response.body)
            } else {
                logger.info("Email with subject '{}' sent to {}", subject, to)
            }
        } catch (ex: IOException) {
            logger.error("IO Exception sending email to {}: {}", to, ex.message)
        }
    }
}
