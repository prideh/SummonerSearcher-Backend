package com.pride.summoner_searcher_api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
@EnableAsync
class SummonerSearcherApiApplication

fun main(args: Array<String>) {
	runApplication<SummonerSearcherApiApplication>(*args)
}
