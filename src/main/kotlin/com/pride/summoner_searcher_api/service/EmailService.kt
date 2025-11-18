package com.pride.summoner_searcher_api.service

import org.springframework.mail.SimpleMailMessage
import org.springframework.mail.javamail.JavaMailSender
import org.springframework.stereotype.Service

@Service
class EmailService(
    private val mailSender: JavaMailSender
) {

    fun sendVerificationEmail(to: String, token: String) {
        val message = SimpleMailMessage()
        message.setTo(to)
        message.setSubject("Verify Your Account")
        // This URL now points to your frontend, which will then call the backend API
        message.setText("Please click the following link to verify your email: http://localhost:5173/verify-email?token=$token")
        mailSender.send(message)
    }

    fun sendPasswordResetEmail(to: String, token: String) {
        val message = SimpleMailMessage()
        message.setTo(to)
        message.setSubject("Password Reset Request")
        // This URL should point to your frontend's password reset page
        message.setText("Please click the following link to reset your password: http://localhost:5173/reset-password?token=$token")
        mailSender.send(message)
    }
}
