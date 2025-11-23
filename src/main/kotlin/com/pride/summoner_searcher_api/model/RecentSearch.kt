package com.pride.summoner_searcher_api.model

import jakarta.persistence.Embeddable

@Embeddable
data class RecentSearch(
    var query: String,
    var server: String
)
