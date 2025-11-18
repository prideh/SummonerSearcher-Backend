package com.pride.summoner_searcher_api.exception

import org.springframework.http.HttpStatusCode

class RiotApiStatusException(
    val statusCode: HttpStatusCode,
    val reason: String?,
    val url: String
) : RuntimeException("Riot API call to '$url' failed with status $statusCode. Reason: $reason")
