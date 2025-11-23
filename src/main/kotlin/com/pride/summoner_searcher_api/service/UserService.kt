package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

/**
 * Service class for handling business logic related to user accounts.
 */
@Service
class UserService(private val userRepository: UserRepository) {

    /**
     * Finds a user by their email address.
     * @param email The email of the user to find.
     * @return The [User] object if found, otherwise null.
     */
    fun findByEmail(email: String): User? {
        return userRepository.findByEmail(email)
    }

    /**
     * Adds a new search query to a user's list of recent searches.
     * This method ensures that the list does not exceed 5 entries and that the most recent search is at the top.
     * @param user The user to update.
     * @param searchQuery The new search query to add.
     * @param server The server region where the search was performed.
     */
    @Transactional
    fun addRecentSearch(user: User, searchQuery: String, server: String) {
        val searches = user.recentSearches
        val newSearch = com.pride.summoner_searcher_api.model.RecentSearch(searchQuery, server)
        
        // Remove the query if it already exists (same query AND same server) to avoid duplicates and to move it to the top.
        searches.removeIf { it.query == searchQuery && it.server == server }
        
        // Add the new query to the beginning of the list.
        searches.add(0, newSearch)
        
        // Trim the list to the 5 most recent searches.
        if (searches.size > 5) {
            user.recentSearches = searches.take(5).toMutableList()
        } else {
            user.recentSearches = searches
        }
        userRepository.save(user)
    }

    /**
     * Clears all recent search queries for a given user.
     * @param user The user whose recent searches will be cleared.
     */
    @Transactional
    fun clearRecentSearches(user: User) {
        user.recentSearches.clear()
        userRepository.save(user)
    }
}
