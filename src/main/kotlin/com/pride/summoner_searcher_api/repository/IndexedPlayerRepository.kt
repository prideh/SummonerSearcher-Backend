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

    /**
     * Retrieves the top 5,000 most recently seen indexed players for dynamic XML sitemap generation.
     */
    fun findTop5000ByOrderByLastSeenAtDesc(): List<IndexedPlayer>

    /**
     * Read-only O(rows) aggregate of searchable identity rows grouped by normalized region.
     * Because `puuid` is the `@Id` primary key of `indexed_players`, `COUNT(p)` equals distinct PUUIDs.
     * Each row contains: `[normalizedRegion, playerCount, maxLastSeenAt]`.
     */
    @org.springframework.data.jpa.repository.Query(
        "SELECT LOWER(TRIM(p.region)), COUNT(p), MAX(p.lastSeenAt) FROM IndexedPlayer p GROUP BY LOWER(TRIM(p.region))"
    )
    fun countPlayersGroupedByRegion(): List<Array<Any?>>
}

