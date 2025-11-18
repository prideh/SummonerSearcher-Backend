package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.User
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface UserRepository : JpaRepository<User, Long> {
    fun findByEmail(email: String): User?
    fun findByVerificationToken(token: String): User?
    fun findByPasswordResetToken(token: String): User?
}
