package com.pride.summoner_searcher_api.service

interface EmailSender {
    fun sendVerificationEmail(to: String, token: String)
    fun sendPasswordResetEmail(to: String, token: String)
}
