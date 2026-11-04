package com.pride.summoner_searcher_api.prototype.probenchmark

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import java.io.File
import java.time.Instant

/**
 * Opt-in CLI runner for the Issue #15 private prototype.
 *
 * Completely inert during normal application startup unless `--run-pro-benchmark-prototype`
 * is explicitly passed in [ApplicationArguments].
 */
@Component
class ProBenchmarkPrototypeRunner(
    private val prototypeService: ProBenchmarkPrototypeService,
    private val objectMapper: ObjectMapper
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        val DEFAULT_SAMPLE_GAME_IDS = listOf(
            "115660540726095110", // IG vs TES G1
            "115660540726095111", // IG vs TES G2
            "115660540726095112", // IG vs TES G3
            "115660540726095113", // IG vs TES G4
            "115660540726095094", // BLG vs HLE G1
            "115660540726095095", // BLG vs HLE G2
            "115660540726095082", // G2 vs HLE G1
            "115660540726095083"  // G2 vs HLE G2
        )
    }

    override fun run(args: ApplicationArguments?) {
        if (args == null || !args.containsOption("run-pro-benchmark-prototype")) {
            return
        }

        val gameIds = args.getOptionValues("pro-game-ids")
            ?.flatMap { it.split(",") }
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.ifEmpty { DEFAULT_SAMPLE_GAME_IDS }
            ?: DEFAULT_SAMPLE_GAME_IDS

        val outputPath = args.getOptionValues("pro-benchmark-output")
            ?.firstOrNull()
            ?: "prototype/pro-benchmark/pro_benchmark_runner_output.json"

        logger.info("Starting private Pro-Benchmark Prototype for {} games -> {}", gameIds.size, outputPath)

        val client = RestClient.builder()
            .baseUrl("https://feed.lolesports.com/livestats/v1")
            .defaultHeader("User-Agent", "SummonerSearcher-Prototype/1.0")
            .build()

        val alignmentResults = mutableMapOf<String, ClockAlignmentResultDto>()

        for (gameId in gameIds) {
            try {
                val initWindow = prototypeService.executeWithPoliteBackoff {
                    client.get()
                        .uri("/window/{gameId}", gameId)
                        .retrieve()
                        .body(LivestatsWindowResponseDto::class.java)
                } ?: continue

                val firstFrameIso = initWindow.frames.firstOrNull()?.rfc460Timestamp ?: continue
                val firstInstant = Instant.parse(firstFrameIso)
                val roleMap = buildMap {
                    initWindow.gameMetadata?.blueTeamMetadata?.participantMetadata?.forEach {
                        put(it.participantId, it.role)
                    }
                    initWindow.gameMetadata?.redTeamMetadata?.participantMetadata?.forEach {
                        put(it.participantId, it.role)
                    }
                }

                val collectedFrames = mutableMapOf<String, LivestatsWindowFrameDto>()
                for (step in 0..11) {
                    val st = prototypeService.floorToTenSecondWindow(firstInstant.plusSeconds(step * 10L))
                    val win = prototypeService.executeWithPoliteBackoff {
                        client.get()
                            .uri("/window/{gameId}?startingTime={st}", gameId, st)
                            .retrieve()
                            .body(LivestatsWindowResponseDto::class.java)
                    }
                    win?.frames?.forEach { collectedFrames[it.rfc460Timestamp] = it }
                }

                val endQuery = prototypeService.floorToTenSecondWindow(firstInstant.plusSeconds(3 * 3600L))
                val endWin = prototypeService.executeWithPoliteBackoff {
                    client.get()
                        .uri("/window/{gameId}?startingTime={st}", gameId, endQuery)
                        .retrieve()
                        .body(LivestatsWindowResponseDto::class.java)
                }
                endWin?.frames?.forEach { collectedFrames[it.rfc460Timestamp] = it }

                val result = prototypeService.analyzeClockAlignment(
                    frames = collectedFrames.values.toList(),
                    roleByParticipantId = roleMap
                )
                alignmentResults[gameId] = result
                logger.info(
                    "Game {}: preSpawn={}s, passiveFromSpawn={}s (err={}s), totalPause={}s",
                    gameId,
                    result.preSpawnOffsetSeconds,
                    result.passiveGoldFromSpawnSeconds,
                    result.passiveGoldErrorVsTheorySeconds,
                    result.totalPauseSeconds
                )
            } catch (e: Exception) {
                logger.error("Failed to analyze gameId={}", gameId, e)
            }
        }

        val outFile = File(outputPath)
        outFile.parentFile?.mkdirs()
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(outFile, alignmentResults)
        logger.info("Private prototype output written to {}", outFile.absolutePath)
    }
}
