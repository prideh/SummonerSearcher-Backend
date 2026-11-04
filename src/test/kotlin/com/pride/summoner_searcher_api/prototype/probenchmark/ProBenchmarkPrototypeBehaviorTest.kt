package com.pride.summoner_searcher_api.prototype.probenchmark

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.shouldBe
import java.time.Instant

class ProBenchmarkPrototypeBehaviorTest : BehaviorSpec({

    val service = ProBenchmarkPrototypeService()

    fun makeWindowFrame(
        ts: String,
        state: String = "in_game",
        golds: List<Int> = List(10) { 500 },
        csList: List<Int> = List(10) { 0 },
        maxHp: Int = 650
    ): LivestatsWindowFrameDto {
        val blueP = (1..5).map { pid ->
            LivestatsWindowParticipantFrameDto(
                participantId = pid,
                totalGold = golds[pid - 1],
                creepScore = csList[pid - 1],
                currentHealth = maxHp,
                maxHealth = maxHp
            )
        }
        val redP = (6..10).map { pid ->
            LivestatsWindowParticipantFrameDto(
                participantId = pid,
                totalGold = golds[pid - 1],
                creepScore = csList[pid - 1],
                currentHealth = maxHp,
                maxHealth = maxHp
            )
        }
        return LivestatsWindowFrameDto(
            rfc460Timestamp = ts,
            gameState = state,
            blueTeam = LivestatsWindowTeamFrameDto(
                totalGold = blueP.sumOf { it.totalGold },
                participants = blueP
            ),
            redTeam = LivestatsWindowTeamFrameDto(
                totalGold = redP.sumOf { it.totalGold },
                participants = redP
            )
        )
    }

    val standardRoles = mapOf(
        1 to "top", 2 to "jungle", 3 to "mid", 4 to "bottom", 5 to "support",
        6 to "top", 7 to "jungle", 8 to "mid", 9 to "bottom", 10 to "support"
    )

    Given("ProBenchmarkPrototypeService clock-alignment and pause analyzer") {

        When("flooring timestamps for camelCase startingTime pagination") {
            Then("it rounds down to the nearest 10-second UTC boundary") {
                val instant = Instant.parse("2026-11-03T22:14:56.128Z")
                service.floorToTenSecondWindow(instant) shouldBe "2026-11-03T22:14:50Z"

                val exact = Instant.parse("2026-11-03T22:15:00.000Z")
                service.floorToTenSecondWindow(exact) shouldBe "2026-11-03T22:15:00Z"
            }
        }

        When("analyzing a game timeline modeled after IG vs TES Game 1 (115660540726095110)") {
            val frames = listOf(
                // 1. Pre-spawn loading frame (totalGold = 0, maxHp = 0)
                makeWindowFrame(
                    ts = "2026-11-03T22:14:56.128Z",
                    golds = List(10) { 0 },
                    maxHp = 0
                ),
                // 2. Fountain spawn frame (totalGold = 500, maxHp > 0 -> 0:00.0 in-game anchor)
                makeWindowFrame(
                    ts = "2026-11-03T22:15:00.645Z",
                    golds = List(10) { 500 },
                    maxHp = 600
                ),
                // 3. Level-1 ward clear at 32.9s by P8 (+15g, cs=1) -> must be ignored as lane minion CS
                makeWindowFrame(
                    ts = "2026-11-03T22:15:33.576Z",
                    golds = listOf(500, 500, 500, 500, 500, 500, 500, 515, 500, 500),
                    csList = listOf(0, 0, 0, 0, 0, 0, 0, 1, 0, 0)
                ),
                // 4. First mid lane melee minion CS at 61.936s by P8 (+21g, cs 1 -> 2)
                makeWindowFrame(
                    ts = "2026-11-03T22:16:02.581Z",
                    golds = listOf(500, 500, 500, 500, 500, 500, 500, 536, 500, 500),
                    csList = listOf(0, 0, 0, 0, 0, 0, 0, 2, 0, 0)
                ),
                // 5. Simultaneous passive gold tick (500 -> 502) across untouched players at 65.944s from spawn
                makeWindowFrame(
                    ts = "2026-11-03T22:16:06.589Z",
                    golds = listOf(502, 515, 502, 502, 512, 502, 507, 557, 502, 517),
                    csList = listOf(0, 0, 0, 0, 0, 0, 0, 3, 0, 0)
                ),
                // 6. First jungle CS at 67.914s (P2) and side-lane melee CS at 69.918s (P1)
                makeWindowFrame(
                    ts = "2026-11-03T22:16:08.559Z",
                    golds = listOf(506, 535, 526, 506, 517, 506, 511, 561, 506, 522),
                    csList = listOf(0, 1, 1, 0, 0, 0, 0, 3, 0, 0)
                ),
                makeWindowFrame(
                    ts = "2026-11-03T22:16:10.563Z",
                    golds = listOf(530, 555, 530, 510, 521, 530, 515, 579, 530, 544),
                    csList = listOf(1, 2, 1, 0, 0, 1, 0, 4, 1, 1)
                ),
                // 7. Mid-game pause at 22:21:24.286Z .. 22:23:09.306Z (105.020s freeze)
                makeWindowFrame(
                    ts = "2026-11-03T22:21:24.286Z",
                    state = "paused",
                    golds = List(10) { 2654 }
                ),
                makeWindowFrame(
                    ts = "2026-11-03T22:23:09.306Z",
                    state = "in_game",
                    golds = List(10) { 2654 }
                ),
                // 8. End of game at 22:47:47.646Z
                makeWindowFrame(
                    ts = "2026-11-03T22:47:47.646Z",
                    state = "finished",
                    golds = List(10) { 14500 }
                )
            )

            val result = service.analyzeClockAlignment(frames, standardRoles)

            Then("it isolates the pre-spawn delay and locks onto the passive gold tick within 50ms of theory") {
                result.tFirstFrameIso shouldBe "2026-11-03T22:14:56.128Z"
                result.tSpawnIso shouldBe "2026-11-03T22:15:00.645Z"
                result.preSpawnOffsetSeconds shouldBe 4.517
                result.passiveGoldFromSpawnSeconds shouldBe 65.944
                result.passiveGoldErrorVsTheorySeconds!!.shouldBeBetween(-0.05, 0.05, 0.001)
            }

            Then("it filters out level-1 ward kills and captures true first Mid, Side, and Jungle CS") {
                result.firstMidCsFromSpawnSeconds shouldBe 61.936
                result.firstJungleCsFromSpawnSeconds shouldBe 67.914
                result.firstSideCsFromSpawnSeconds shouldBe 69.918
            }

            Then("it measures the 105.020s pause and subtracts it from the true in-game duration") {
                result.pauses shouldHaveSize 1
                result.totalPauseSeconds shouldBe 105.02
                result.trueInGameDurationSeconds shouldBe 1861.981
            }
        }

        When("comparing 10-minute checkpoints across Naive, Spawn-Unadjusted, and True In-Game Clock") {
            val tFirstFrame = Instant.parse("2026-11-03T22:14:56.128Z")
            val tSpawn = Instant.parse("2026-11-03T22:15:00.645Z")
            val pauses = listOf(
                PauseIntervalDto(
                    pauseStartIso = "2026-11-03T22:21:24.286Z",
                    pauseEndIso = "2026-11-03T22:23:09.306Z",
                    durationSeconds = 105.02,
                    elapsedFromSpawnAtStartSeconds = 383.641,
                    detectedBy = "gameState==paused"
                )
            )
            val detailsFrames = listOf(
                LivestatsDetailsFrameDto(
                    rfc460Timestamp = "2026-11-03T22:24:56.208Z", // Naive 10m (actually 8:10 in-game)
                    participants = listOf(
                        LivestatsDetailsParticipantFrameDto(
                            participantId = 3,
                            creepScore = 67,
                            totalGoldEarned = 2841,
                            level = 7
                        )
                    )
                ),
                LivestatsDetailsFrameDto(
                    rfc460Timestamp = "2026-11-03T22:25:00.538Z", // Spawn + 10m unadjusted (actually 8:15 in-game)
                    participants = listOf(
                        LivestatsDetailsParticipantFrameDto(
                            participantId = 3,
                            creepScore = 70,
                            totalGoldEarned = 2931,
                            level = 7
                        )
                    )
                ),
                LivestatsDetailsFrameDto(
                    rfc460Timestamp = "2026-11-03T22:26:45.558Z", // True in-game 10:00 (Spawn + 600s + 105.02s)
                    participants = listOf(
                        LivestatsDetailsParticipantFrameDto(
                            participantId = 3,
                            creepScore = 92,
                            totalGoldEarned = 3567,
                            level = 8
                        )
                    )
                )
            )

            val comparison = service.extractCheckpointComparison(
                detailsFrames = detailsFrames,
                tFirstFrame = tFirstFrame,
                tSpawn = tSpawn,
                pauses = pauses,
                markMinutes = 10
            )

            Then("it quantifies the -109.537s naive bias and selects the true 10:00 frame (92 CS / 3567g / L8)") {
                comparison.naiveFirstFrame.clockBiasVsTrueInGameSeconds shouldBe -109.537
                comparison.spawnUnadjusted.clockBiasVsTrueInGameSeconds shouldBe -105.02
                comparison.trueInGameClock.clockBiasVsTrueInGameSeconds shouldBe 0.0

                val rookieNaive = comparison.naiveFirstFrame.participants[3]!!
                val rookieTrue = comparison.trueInGameClock.participants[3]!!

                rookieNaive.creepScore shouldBe 67
                rookieNaive.totalGold shouldBe 2841
                rookieTrue.creepScore shouldBe 92
                rookieTrue.totalGold shouldBe 3567
                rookieTrue.level shouldBe 8
            }
        }

        When("checking coarse intervals for passive-gold lower bound violations and polite retry backoff") {
            Then("it flags intervals where gold delta falls below the 2.04g/s passive floor") {
                // 60s normal interval where everyone gained at least 122g
                service.isSubPassiveGoldInterval(60.0, List(10) { 125 }) shouldBe false

                // 60s interval with a 30s pause where a non-farming player only gained 61g
                service.isSubPassiveGoldInterval(60.0, listOf(61, 140, 180, 120, 90, 61, 150, 160, 130, 80)) shouldBe true

                // Complete freeze (max delta == 0)
                service.isSubPassiveGoldInterval(10.0, List(10) { 0 }) shouldBe true
            }

            Then("executeWithPoliteBackoff paces requests and retries transient failures with exponential backoff") {
                val recordedSleeps = mutableListOf<Long>()
                var attempts = 0
                val out = service.executeWithPoliteBackoff(
                    minDelayMs = 150L,
                    maxRetries = 3,
                    baseBackoffMs = 300L,
                    sleepFn = { recordedSleeps.add(it) }
                ) {
                    attempts++
                    if (attempts < 3) throw RuntimeException("Transient 503")
                    "ok"
                }

                out shouldBe "ok"
                attempts shouldBe 3
                recordedSleeps shouldBe listOf(150L, 300L, 600L)
            }
        }

        When("aggregating per-(role, championId) baselines across multiple games") {
            val observations = listOf(
                ProPlayerObservationDto(
                    gameId = "115660540726095110",
                    summonerName = "IG TheShy",
                    role = "top",
                    championId = "Gnar",
                    csAt10 = 88,
                    goldAt10 = 3965,
                    levelAt10 = 8,
                    naiveCsAt10 = 70,
                    naiveGoldAt10 = 3252,
                    csAt15 = 132,
                    goldAt15 = 5610,
                    levelAt15 = 11,
                    bootsInGameClockSeconds = 780.0,
                    firstItemInGameClockSeconds = 810.0,
                    secondItemInGameClockSeconds = 1390.0,
                    skillOrderFirst6 = listOf("Q", "W", "E", "Q", "Q", "R")
                ),
                ProPlayerObservationDto(
                    gameId = "115660540726095094",
                    summonerName = "BLG Bin",
                    role = "top",
                    championId = "Gnar",
                    csAt10 = 90,
                    goldAt10 = 3515,
                    levelAt10 = 9,
                    naiveCsAt10 = 88,
                    naiveGoldAt10 = 3455,
                    csAt15 = 140,
                    goldAt15 = 5490,
                    levelAt15 = 12,
                    bootsInGameClockSeconds = 720.0,
                    firstItemInGameClockSeconds = 850.0,
                    secondItemInGameClockSeconds = 1410.0,
                    skillOrderFirst6 = listOf("Q", "E", "W", "Q", "Q", "R")
                )
            )

            val baselines = service.aggregateRoleChampionBaselines(observations)

            Then("it computes accurate averages and preserves both true and naive comparisons") {
                baselines shouldHaveSize 1
                val gnarTop = baselines.first()
                gnarTop.role shouldBe "top"
                gnarTop.championId shouldBe "Gnar"
                gnarTop.sampleCount shouldBe 2
                gnarTop.players shouldBe listOf("BLG Bin", "IG TheShy")
                gnarTop.avgCsAt10 shouldBe 89.0
                gnarTop.avgGoldAt10 shouldBe 3740.0
                gnarTop.avgLevelAt10 shouldBe 8.5
                gnarTop.avgNaiveCsAt10 shouldBe 79.0
                gnarTop.avgCsAt15 shouldBe 136.0
                gnarTop.avgBootsCompletionSeconds shouldBe 750.0
                gnarTop.avgFirstItemCompletionSeconds shouldBe 830.0
                gnarTop.avgSecondItemCompletionSeconds shouldBe 1400.0
            }
        }
    }
})
