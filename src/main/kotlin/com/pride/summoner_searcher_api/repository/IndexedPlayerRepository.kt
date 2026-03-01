package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.IndexedPlayer
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface IndexedPlayerRepository : JpaRepository<IndexedPlayer, String> {
    
    /**
     * Finds players by region whose game name starts with the given prefix (case-insensitive),
     * ordered by the most recently seen players first.
     */
    fun findTop5ByRegionAndGameNameStartingWithIgnoreCaseOrderByLastSeenAtDesc(
        region: String,
        gameNamePrefix: String
    ): List<IndexedPlayer>
}
