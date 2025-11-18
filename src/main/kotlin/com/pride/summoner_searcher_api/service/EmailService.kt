package com.pride.summoner_searcher_api.service

import com.sendgrid.Method
import com.sendgrid.Request
import com.sendgrid.SendGrid
import com.sendgrid.helpers.mail.Mail
import com.sendgrid.helpers.mail.objects.Content
import com.sendgrid.helpers.mail.objects.Email
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.io.IOException

@Service
class EmailService(
    @Value("\${SENDGRID_API_KEY}") private val sendGridApiKey: String,
    @Value("\${sendgrid.from.email}") private val fromEmail: String,
    @Value("\${app.frontend.url}") private val frontendUrl: String
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Async
    fun sendVerificationEmail(to: String, token: String) {
        val subject = "Verify Your Account"
        val verificationLink = "$frontendUrl/verify-email?token=$token"
        val body = """
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
        
        sendEmail(to, subject, body)
    }

    @Async
    fun sendPasswordResetEmail(to: String, token: String) {
        val subject = "Password Reset Request"
        val resetLink = "$frontendUrl/reset-password?token=$token"
        val body = """
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

        sendEmail(to, subject, body)
    }

    private fun sendEmail(to: String, subject: String, body: String) {
        val from = Email(fromEmail)
        val toEmail = Email(to)
        val content = Content("text/html", body)
        val mail = Mail(from, subject, toEmail, content)

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
