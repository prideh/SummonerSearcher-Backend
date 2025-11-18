package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.stereotype.Service

@Service
class UserService(private val userRepository: UserRepository) {

    fun findByEmail(email: String): User? {
        return userRepository.findByEmail(email)
    }

    fun addRecentSearch(user: User, searchQuery: String) {
        // Remove if exists to move it to the top
        user.recentSearches.remove(searchQuery)

        // Add to the top of the list
        user.recentSearches.add(0, searchQuery)

        // Use take() to safely get the first 5 elements and replace the list.
        user.recentSearches = user.recentSearches.take(5).toMutableList()

        userRepository.save(user)
    }

    fun clearRecentSearches(user: User) {
        user.recentSearches.clear()
        userRepository.save(user)
    }
}
