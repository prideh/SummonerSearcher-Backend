package com.pride.summoner_searcher_api.util

fun mapToRegionRouting(region: String): String {
    return when (region.uppercase()) {
        "EUN1", "EUW1", "TR1", "RU" -> "EUROPE"
        "NA1", "BR1", "LA1", "LA2" -> "AMERICAS"
        "KR", "JP1" -> "ASIA"
        "OC1", "SG2", "TW2", "VN2", "PH2", "TH2" -> "SEA"
        else -> throw IllegalArgumentException("Invalid region: $region")
    }
}
