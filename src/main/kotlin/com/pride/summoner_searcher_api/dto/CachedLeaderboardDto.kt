package com.pride.summoner_searcher_api.dto

import java.time.Instant

/**
 * A wrapper object for storing the challenger leaderboard in the cache.
 * This allows us to store a timestamp along with the data, so our application
 * can manage its own refresh logic instead of relying on Redis's TTL expiration.
 *
 * @property lastRefreshed The timestamp of when the data was last fetched from the Riot API.
 * @property leaderboard The actual leaderboard data.
 */
data class CachedLeaderboardDto(
    val lastRefreshed: Instant,
    val leaderboard: LeagueListDTO
)
