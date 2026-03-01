package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.core.type.TypeReference
import com.pride.summoner_searcher_api.dto.AutocompletePlayerDto
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class SearchService(
    private val indexedPlayerRepository: IndexedPlayerRepository,
    private val redisCacheService: RedisCacheService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Searches for players whose game name starts with the specified query, across all regions.
     * Caches the result in Redis for fast retrieval in subsequent identical searches.
     */
    fun autocompleteSearch(query: String): List<AutocompletePlayerDto> {
        if (query.isBlank()) return emptyList()

        val normalizedQuery = query.trim()
        val cacheKey = "autocomplete:${normalizedQuery.lowercase()}"

        try {
            val cachedResult = redisCacheService.get(cacheKey, Array<AutocompletePlayerDto>::class.java)
            if (cachedResult != null) {
                return cachedResult.toList()
            }
        } catch (e: Exception) {
            logger.warn("Redis read error for autocomplete cache", e)
        }

        // Cache Miss: Query the database globally (no region filter)
        val dbResults = indexedPlayerRepository.findTop8ByGameNameStartingWithIgnoreCaseOrderByLastSeenAtDesc(
            gameNamePrefix = normalizedQuery
        )

        val results = dbResults.map {
            AutocompletePlayerDto(
                puuid = it.puuid,
                gameName = it.gameName,
                tagLine = it.tagLine,
                region = it.region,
                profileIconId = it.profileIconId,
                summonerLevel = it.summonerLevel
            )
        }

        // Cache the outcome (even if empty, to prevent DB spam for bad queries)
        try {
            redisCacheService.set(cacheKey, results, Duration.ofMinutes(15))
        } catch (e: Exception) {
            logger.warn("Redis write error for autocomplete cache", e)
        }

        return results
    }
}
