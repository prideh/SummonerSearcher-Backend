package com.pride.summoner_searcher_api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.EnableCaching
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableCaching
@EnableScheduling
class SummonerSearcherApiApplication

fun main(args: Array<String>) {
	runApplication<SummonerSearcherApiApplication>(*args)
}
