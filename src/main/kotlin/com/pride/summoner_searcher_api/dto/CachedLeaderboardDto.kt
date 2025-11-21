package com.pride.summoner_searcher_api.dto

import java.time.Instant

/**
 * A wrapper object for storing the challenger leaderboard in the cache.
 * This allows us to store a timestamp along with the data, so our application
 * can manage its own refresh logic instead of relying on Redis's TTL expiration.
 *
 * The fields are nullable to prevent deserialization errors if the cache ever contains
 * old or malformed data from a previous deployment.
 */
data class CachedLeaderboardDto(
    val lastRefreshed: Instant? = null,
    val leaderboard: LeagueListDTO? = null
)
