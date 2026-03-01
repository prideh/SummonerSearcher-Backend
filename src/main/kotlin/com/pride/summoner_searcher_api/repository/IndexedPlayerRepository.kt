package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.IndexedPlayer
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface IndexedPlayerRepository : JpaRepository<IndexedPlayer, String> {
    
    /**
     * Finds players globally (across all regions) whose game name starts with the given prefix
     * (case-insensitive), ordered by the most recently seen players first.
     * Capped at 8 to keep the autocomplete dropdown concise with cross-region results.
     */
    fun findTop8ByGameNameStartingWithIgnoreCaseOrderByLastSeenAtDesc(
        gameNamePrefix: String
    ): List<IndexedPlayer>
}
