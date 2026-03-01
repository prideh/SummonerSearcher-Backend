package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant

/**
 * Represents a player cached in the local database for autocomplete search functionality.
 */
@Entity
@Table(
    name = "indexed_players",
    indexes = [
        Index(name = "idx_game_name", columnList = "game_name"),
        Index(name = "idx_region", columnList = "region")
    ]
)
class IndexedPlayer(
    /** The player's unique Riot PUUID. Used as the primary key. */
    @Id
    @Column(nullable = false, unique = true)
    val puuid: String,

    /** The player's current game name (Riot ID left side). */
    @Column(name = "game_name", nullable = false)
    var gameName: String,

    /** The player's current tagline (Riot ID right side). */
    @Column(name = "tag_line", nullable = false)
    var tagLine: String,

    /** The region the player belongs to (e.g., 'na', 'euw', 'kr'). */
    @Column(nullable = false)
    var region: String,

    /** The ID of the player's current profile icon. */
    @Column(name = "profile_icon_id", nullable = false)
    var profileIconId: Int,

    /** The player's current summoner level. */
    @Column(name = "summoner_level", nullable = false)
    var summonerLevel: Long,

    /** Timestamp of when this player was last seen/updated in the system. */
    @Column(name = "last_seen_at", nullable = false)
    var lastSeenAt: Instant = Instant.now()
)
