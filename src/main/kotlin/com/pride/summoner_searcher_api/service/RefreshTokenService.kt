package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.RefreshToken
import com.pride.summoner_searcher_api.repository.RefreshTokenRepository
import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

@Service
class RefreshTokenService(
    private val refreshTokenRepository: RefreshTokenRepository,
    private val userRepository: UserRepository
) {
    @Value("\${jwt.refreshExpiration:2592000000}") // 30 days
    private var refreshExpiration: Long = 2592000000

    fun createRefreshToken(email: String): RefreshToken {
        val user = userRepository.findByEmail(email) ?: throw RuntimeException("User not found")
        
        val refreshToken = RefreshToken(
            user = user,
            expiryDate = Instant.now().plusMillis(refreshExpiration),
            token = UUID.randomUUID().toString()
        )

        return refreshTokenRepository.save(refreshToken)
    }

    fun findByToken(token: String): RefreshToken? {
        return refreshTokenRepository.findByToken(token).orElse(null)
    }

    fun verifyExpiration(token: RefreshToken): RefreshToken {
        if (token.expiryDate.compareTo(Instant.now()) < 0) {
            refreshTokenRepository.delete(token)
            throw RuntimeException("Refresh token was expired. Please make a new signin request")
        }
        return token
    }

    @Transactional
    fun deleteByUserId(userId: Long): Int {
        val user = userRepository.findById(userId).orElseThrow { RuntimeException("User not found") }
        return refreshTokenRepository.deleteByUser(user)
    }
}
