package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.repository.UserRepository
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
     * Locates the user based on the email.
     * Since our [com.pride.summoner_searcher_api.model.User] class now implements [UserDetails],
     * we can return it directly. Spring Security will handle the password check and account status checks
     * (e.g., isEnabled(), which we've mapped to the 'verified' property).
     *
     * @param email The email identifying the user whose data is required.
     * @return The [com.pride.summoner_searcher_api.model.User] object, which also serves as the [UserDetails].
     * @throws UsernameNotFoundException if the user could not be found.
     */
    override fun loadUserByUsername(email: String): UserDetails {
        return userRepository.findByEmail(email)
            ?: throw UsernameNotFoundException("User not found with email: $email")
    }
}
