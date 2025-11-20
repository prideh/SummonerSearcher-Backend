package com.pride.summoner_searcher_api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * The main entry point for the Summoner Searcher Spring Boot application.
 *
 * This class initializes the Spring application context and enables key features.
 *
 * @see SpringBootApplication Enables auto-configuration, component scanning, and property support.
 * @see EnableScheduling Activates Spring's scheduled task execution capabilities (for background jobs like cache warming).
 * @see EnableAsync Enables Spring's ability to run methods asynchronously in the background (used for email sending).
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
class SummonerSearcherApiApplication

fun main(args: Array<String>) {
	runApplication<SummonerSearcherApiApplication>(*args)
}
