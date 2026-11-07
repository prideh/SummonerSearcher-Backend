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

    /**
     * Atomically inserts a new searchable player identity if no row exists for [puuid].
     * Uses `REQUIRES_NEW` so a concurrent unique-constraint race never aborts an outer transaction.
     * Returns `1` when inserted, or `0` if a row for [puuid] already exists.
     */
    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW
    )
    @org.springframework.data.jpa.repository.Query(
        value = """
            INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
            SELECT :puuid, :gameName, :tagLine, :region, :profileIconId, :summonerLevel, :lastSeenAt
            WHERE NOT EXISTS (
                SELECT 1 FROM indexed_players existing WHERE existing.puuid = :puuid
            )
        """,
        nativeQuery = true
    )
    fun insertPlayerIfAbsent(
        @org.springframework.data.repository.query.Param("puuid") puuid: String,
        @org.springframework.data.repository.query.Param("gameName") gameName: String,
        @org.springframework.data.repository.query.Param("tagLine") tagLine: String,
        @org.springframework.data.repository.query.Param("region") region: String,
        @org.springframework.data.repository.query.Param("profileIconId") profileIconId: Int,
        @org.springframework.data.repository.query.Param("summonerLevel") summonerLevel: Long,
        @org.springframework.data.repository.query.Param("lastSeenAt") lastSeenAt: java.time.Instant
    ): Int

    /**
     * Atomically updates an existing player identity row ONLY when the incoming match's [lastSeenAt]
     * is newer than or equal to the stored `last_seen_at`.
     * Returns `1` when updated, or `0` when skipped because the stored row is newer.
     */
    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW
    )
    @org.springframework.data.jpa.repository.Query(
        value = """
            UPDATE indexed_players
            SET game_name = :gameName,
                tag_line = :tagLine,
                region = :region,
                profile_icon_id = :profileIconId,
                summoner_level = :summonerLevel,
                last_seen_at = :lastSeenAt
            WHERE puuid = :puuid
              AND last_seen_at <= :lastSeenAt
        """,
        nativeQuery = true
    )
    fun updatePlayerIfNewer(
        @org.springframework.data.repository.query.Param("puuid") puuid: String,
        @org.springframework.data.repository.query.Param("gameName") gameName: String,
        @org.springframework.data.repository.query.Param("tagLine") tagLine: String,
        @org.springframework.data.repository.query.Param("region") region: String,
        @org.springframework.data.repository.query.Param("profileIconId") profileIconId: Int,
        @org.springframework.data.repository.query.Param("summonerLevel") summonerLevel: Long,
        @org.springframework.data.repository.query.Param("lastSeenAt") lastSeenAt: java.time.Instant
    ): Int
}

/**
 * Atomically inserts a new searchable player identity or updates an existing row ONLY when
 * the incoming match's [lastSeenAt] is newer than or equal to the stored `last_seen_at`.
 * Returns `1` when inserted or updated, or `0` when skipped because the stored row is newer.
 */
fun IndexedPlayerRepository.upsertPlayerIfNewer(
    puuid: String,
    gameName: String,
    tagLine: String,
    region: String,
    profileIconId: Int,
    summonerLevel: Long,
    lastSeenAt: java.time.Instant
): Int {
    val updatedFirst = updatePlayerIfNewer(
        puuid = puuid,
        gameName = gameName,
        tagLine = tagLine,
        region = region,
        profileIconId = profileIconId,
        summonerLevel = summonerLevel,
        lastSeenAt = lastSeenAt
    )
    if (updatedFirst > 0) return updatedFirst
    val inserted = try {
        insertPlayerIfAbsent(
            puuid = puuid,
            gameName = gameName,
            tagLine = tagLine,
            region = region,
            profileIconId = profileIconId,
            summonerLevel = summonerLevel,
            lastSeenAt = lastSeenAt
        )
    } catch (_: org.springframework.dao.DataIntegrityViolationException) {
        0
    }
    if (inserted > 0) return inserted
    return updatePlayerIfNewer(
        puuid = puuid,
        gameName = gameName,
        tagLine = tagLine,
        region = region,
        profileIconId = profileIconId,
        summonerLevel = summonerLevel,
        lastSeenAt = lastSeenAt
    )
}

