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

const val MAX_MATCHES_FOR_PARTICIPANT_INDEXING = 20
const val MAX_INDEXED_PLAYERS_PER_BATCH = 200

data class ParticipantBatchIndexingOutcome(
    val upsertedCount: Long = 0L,
    val staleSkippedCount: Long = 0L,
    val inspectedMatchesCount: Int = 0,
    val uniqueCandidatesCount: Int = 0
)

/**
 * Shared bounded participant identity batch indexing for already-resolved [com.pride.summoner_searcher_api.dto.MatchDto] payloads,
 * reused identically by profile refresh (`PlayerCacheService`) and low-priority cohort collection (`CohortBenchmarkService`).
 *
 * Strictly enforces:
 * - Inspects at most [maxMatches] (`20`) matches per batch.
 * - Deduplicates participants across all inspected matches by `puuid`, keeping only the newest
 *   `lastSeenAt` (`match.info.gameCreation`, falling back to [fallbackNowEpochMilli] only if `<= 0L`).
 * - Sorts deduplicated candidates by `lastSeenAt` descending and caps upserts at [maxPlayersPerBatch] (`200`).
 * - Preserves exact profile transaction semantics: when [transactionTemplate] is non-null, executes `upsertBlock()`
 *   via `transactionTemplate.executeWithoutResult` (with zero catch/untransactional-replay fallback); when null,
 *   executes `upsertBlock()` directly. Each underlying [updatePlayerIfNewer] / [insertPlayerIfAbsent] statement runs in
 *   its own `@Transactional(propagation = Propagation.REQUIRES_NEW)` scope.
 */
fun IndexedPlayerRepository.indexParticipantsFromMatchesBounded(
    matches: List<com.pride.summoner_searcher_api.dto.MatchDto>,
    region: String,
    fallbackNowEpochMilli: Long,
    transactionTemplate: org.springframework.transaction.support.TransactionTemplate? = null,
    maxMatches: Int = MAX_MATCHES_FOR_PARTICIPANT_INDEXING,
    maxPlayersPerBatch: Int = MAX_INDEXED_PLAYERS_PER_BATCH
): ParticipantBatchIndexingOutcome {
    if (matches.isEmpty() || maxMatches <= 0 || maxPlayersPerBatch <= 0) {
        return ParticipantBatchIndexingOutcome()
    }
    val normalizedRegion = region.lowercase().trim()
    val boundedMatches = matches.take(maxMatches)
    val latestByPuuid = LinkedHashMap<String, IndexedPlayer>()

    for (match in boundedMatches) {
        val info = match.info ?: continue
        val matchPlayedAtMs = (info.gameCreation ?: 0L).takeIf { it > 0L } ?: fallbackNowEpochMilli
        val matchPlayedAt = java.time.Instant.ofEpochMilli(matchPlayedAtMs)

        for (p in info.participants) {
            val cleanPuuid = p.puuid?.trim()?.takeIf { it.isNotBlank() } ?: continue
            val cleanGameName = p.riotIdGameName?.trim()?.takeIf { it.isNotBlank() } ?: continue
            val cleanTagLine = p.riotIdTagline?.trim()?.takeIf { it.isNotBlank() } ?: continue

            val existingInBatch = latestByPuuid[cleanPuuid]
            if (existingInBatch == null || !matchPlayedAt.isBefore(existingInBatch.lastSeenAt)) {
                latestByPuuid[cleanPuuid] = IndexedPlayer(
                    puuid = cleanPuuid,
                    gameName = cleanGameName,
                    tagLine = cleanTagLine,
                    region = normalizedRegion,
                    profileIconId = p.profileIcon ?: 0,
                    summonerLevel = p.summonerLevel?.toLong() ?: 0L,
                    lastSeenAt = matchPlayedAt
                )
            }
        }
    }

    if (latestByPuuid.isEmpty()) {
        return ParticipantBatchIndexingOutcome(
            inspectedMatchesCount = boundedMatches.size,
            uniqueCandidatesCount = 0
        )
    }

    val boundedCandidates = latestByPuuid.values
        .sortedByDescending { it.lastSeenAt }
        .take(maxPlayersPerBatch)

    var upsertedInBatch = 0L
    var staleSkippedInBatch = 0L
    val upsertBlock = {
        for (candidate in boundedCandidates) {
            val updatedRows = upsertPlayerIfNewer(
                puuid = candidate.puuid,
                gameName = candidate.gameName,
                tagLine = candidate.tagLine,
                region = candidate.region,
                profileIconId = candidate.profileIconId,
                summonerLevel = candidate.summonerLevel,
                lastSeenAt = candidate.lastSeenAt
            )
            if (updatedRows > 0) {
                upsertedInBatch++
            } else {
                staleSkippedInBatch++
            }
        }
    }

    val tx = transactionTemplate
    if (tx != null) {
        tx.executeWithoutResult { upsertBlock() }
    } else {
        upsertBlock()
    }

    return ParticipantBatchIndexingOutcome(
        upsertedCount = upsertedInBatch,
        staleSkippedCount = staleSkippedInBatch,
        inspectedMatchesCount = boundedMatches.size,
        uniqueCandidatesCount = boundedCandidates.size
    )
}



