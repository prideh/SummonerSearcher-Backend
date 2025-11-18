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
    @Value("\${sendgrid.from.email}") private val fromEmail: String
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Async
    fun sendVerificationEmail(to: String, token: String) {
        val from = Email(fromEmail)
        val subject = "Verify Your Account"
        val toEmail = Email(to)
        val content = Content("text/plain", "Please click the following link to verify your email: http://localhost:5173/verify-email?token=$token")
        val mail = Mail(from, subject, toEmail, content)

        val sg = SendGrid(sendGridApiKey)
        val request = Request()
        try {
            request.method = Method.POST
            request.endpoint = "mail/send"
            request.body = mail.build()
            val response = sg.api(request)
            if (response.statusCode >= 400) {
                logger.error("SendGrid error sending verification email to {}: Status Code: {}, Body: {}", to, response.statusCode, response.body)
            } else {
                logger.info("Verification email sent to {}", to)
            }
        } catch (ex: IOException) {
            logger.error("IO Exception sending verification email to {}", to, ex)
        }
    }

    @Async
    fun sendPasswordResetEmail(to: String, token: String) {
        val from = Email(fromEmail)
        val subject = "Password Reset Request"
        val toEmail = Email(to)
        val content = Content("text/plain", "Please click the following link to reset your password: http://localhost:5173/reset-password?token=$token")
        val mail = Mail(from, subject, toEmail, content)

        val sg = SendGrid(sendGridApiKey)
        val request = Request()
        try {
            request.method = Method.POST
            request.endpoint = "mail/send"
            request.body = mail.build()
            val response = sg.api(request)
            if (response.statusCode >= 400) {
                logger.error("SendGrid error sending password reset to {}: Status Code: {}, Body: {}", to, response.statusCode, response.body)
            } else {
                logger.info("Password reset email sent to {}", to)
            }
        } catch (ex: IOException) {
            logger.error("IO Exception sending password reset email to {}", to, ex)
        }
    }
}
