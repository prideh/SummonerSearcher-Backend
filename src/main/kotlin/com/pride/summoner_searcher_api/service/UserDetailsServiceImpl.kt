package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.security.authentication.DisabledException
import org.springframework.security.core.userdetails.User
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.stereotype.Service

@Service
class UserDetailsServiceImpl(
    private val userRepository: UserRepository
) : UserDetailsService {

    override fun loadUserByUsername(email: String): UserDetails {
        val user = userRepository.findByEmail(email)
            ?: throw UsernameNotFoundException("User not found with email: $email")

        if (!user.verified) {
            throw DisabledException("User account is not verified. Please check your email.")
        }

        return org.springframework.security.core.userdetails.User(
            user.email, // Use email as the principal name
            user.password,
            emptyList() // You can add roles/authorities here if your User entity has them
        )
    }
}
