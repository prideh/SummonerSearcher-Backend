package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.dto.AutocompletePlayerDto
import com.pride.summoner_searcher_api.service.SearchService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * Controller for handling search autocomplete queries.
 */
@RestController
@RequestMapping("/api/search")
class SearchController(
    private val searchService: SearchService
) {

    /**
     * Endpoint for fetching autocomplete suggestions based on the provided region and query prefix.
     * @param region The server region to search on.
     * @param query The partial or full game name of the player.
     * @return A list of up to 5 [AutocompletePlayerDto] matching the query.
     */
    @GetMapping("/autocomplete/{region}")
    fun autocomplete(
        @PathVariable region: String,
        @RequestParam query: String
    ): ResponseEntity<List<AutocompletePlayerDto>> {
        if (query.length < 2) {
            return ResponseEntity.ok(emptyList()) // Require at least 2 characters string to search
        }
        
        val results = searchService.autocompleteSearch(region, query)
        return ResponseEntity.ok(results)
    }
}
