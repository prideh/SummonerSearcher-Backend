package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface PlayerSeasonStatsRepository : JpaRepository<PlayerSeasonStatsEntity, UUID> {

    fun findByRegionAndPuuidAndSeasonYearAndQueueId(
        region: String,
        puuid: String,
        seasonYear: Int,
        queueId: Int = 420
    ): PlayerSeasonStatsEntity?

    /**
     * Read-only scalar aggregate summary of viewed-player seasonal history completeness grouped by region.
     * Each row contains:
     * `[region, totalPlayers, completePlayers, partialPlayers, cappedPlayers, staleSchemaPlayers, totalFoldedGames, totalPendingGames, latestUpdatedAt]`.
     */
    @Query(
        """
        SELECT LOWER(TRIM(s.region)),
               COUNT(s),
               COALESCE(SUM(CASE WHEN s.historyComplete = true AND s.historyCapped = false AND s.pendingGamesCount = 0 AND s.schemaVersion = :currentSchemaVersion THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN (s.historyComplete = false OR s.historyCapped = true OR s.pendingGamesCount > 0) AND s.schemaVersion = :currentSchemaVersion THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN s.historyCapped = true AND s.schemaVersion = :currentSchemaVersion THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN s.schemaVersion <> :currentSchemaVersion THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(s.foldedGamesCount), 0),
               COALESCE(SUM(s.pendingGamesCount), 0),
               MAX(s.updatedAt)
        FROM PlayerSeasonStatsEntity s
        WHERE s.seasonYear = :seasonYear AND s.queueId = :queueId
        GROUP BY LOWER(TRIM(s.region))
        """
    )
    fun summarizeByRegionForSeasonYear(
        @Param("seasonYear") seasonYear: Int,
        @Param("queueId") queueId: Int = 420,
        @Param("currentSchemaVersion") currentSchemaVersion: Int = 1
    ): List<Array<Any?>>
}
