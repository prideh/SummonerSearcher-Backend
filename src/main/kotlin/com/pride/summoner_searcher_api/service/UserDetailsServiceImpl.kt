package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.security.authentication.DisabledException
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.stereotype.Service

/**
 * This service is a core part of the Spring Security authentication process.
 * Its primary responsibility is to load a user's details from the database given their username (in this case, their email).
 */
@Service
class UserDetailsServiceImpl(
    private val userRepository: UserRepository
) : UserDetailsService {

    /**
     * Locates the user based on the email. In the actual authentication process,
     * the AuthenticationManager will compare the password provided by the user with the one stored in the returned UserDetails.
     *
     * @param email The email identifying the user whose data is required.
     * @return A UserDetails object containing the user's credentials.
     * @throws UsernameNotFoundException if the user could not be found.
     * @throws DisabledException if the user is found but their account has not been verified.
     */
    override fun loadUserByUsername(email: String): UserDetails {
        // 1. Fetch the user from the database.
        val user = userRepository.findByEmail(email)
            ?: throw UsernameNotFoundException("User not found with email: $email")

        // 2. Check if the user's account is verified. If not, block the login.
        if (!user.verified) {
            throw DisabledException("User account is not verified. Please check your email.")
        }

        // 3. Create and return a Spring Security UserDetails object.
        // This object is what Spring Security uses internally to perform the password check.
        return org.springframework.security.core.userdetails.User(
            user.email,
            user.password,
            emptyList() // This list is for user roles/authorities (e.g., "ROLE_ADMIN"). Empty for now.
        )
    }
}
