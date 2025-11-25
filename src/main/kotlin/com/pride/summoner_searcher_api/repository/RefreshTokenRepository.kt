package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.RefreshToken
import com.pride.summoner_searcher_api.model.User
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.Optional

@Repository
interface RefreshTokenRepository : JpaRepository<RefreshToken, Long> {
    fun findByToken(token: String): Optional<RefreshToken>
    fun deleteByUser(user: User): Int
}
