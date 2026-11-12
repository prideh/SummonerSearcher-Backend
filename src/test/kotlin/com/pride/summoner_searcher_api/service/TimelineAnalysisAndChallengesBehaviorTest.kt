package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchEventEntry
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
import com.pride.summoner_searcher_api.dto.TimelineAggregateSummary
import com.pride.summoner_searcher_api.dto.WardTimeBucketsDto
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe

class TimelineAnalysisAndChallengesBehaviorTest : BehaviorSpec({
    val mapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    val timelineService = TimelineAnalysisService()

    Given("A Riot Match-V5 JSON payload containing early-CS and advanced challenge fields (#7)") {
        val rawJson = """
            {
              "metadata": {
                "matchId": "EUW1_7000000001",
                "participants": ["puuid-1", "puuid-2", "puuid-3"]
              },
              "info": {
                "gameCreation": 1730000000000,
                "gameDuration": 1800,
                "gameId": 7000000001,
                "gameMode": "CLASSIC",
                "gameVersion": "16.1.648.1234",
                "queueId": 420,
                "participants": [
                  {
                    "puuid": "puuid-1",
                    "participantId": 1,
                    "championName": "Ahri",
                    "teamPosition": "MIDDLE",
                    "teamId": 100,
                    "win": true,
                    "kills": 8,
                    "deaths": 2,
                    "assists": 11,
                    "challenges": {
                      "kda": 9.5,
                      "goldPerMinute": 445.2,
                      "damagePerMinute": 812.4,
                      "visionScorePerMinute": 1.45,
                      "laneMinionsFirst10Minutes": 84,
                      "jungleCsBefore10Minutes": 4.0,
                      "killParticipation": 0.68,
                      "turretPlatesTaken": 3,
                      "controlWardsPlaced": 5,
                      "visionScoreAdvantageLaneOpponent": 0.35,
                      "maxCsAdvantageOnLaneOpponent": 24.0,
                      "maxLevelLeadLaneOpponent": 2,
                      "soloKills": 3,
                      "takedownsFirst25Minutes": 4,
                      "effectiveHealAndShielding": 120.5,
                      "scuttleCrabKills": 2,
                      "wardTakedowns": 6
                    }
                  },
                  {
                    "puuid": "puuid-2",
                    "participantId": 6,
                    "championName": "Syndra",
                    "teamPosition": "MIDDLE",
                    "teamId": 200,
                    "win": false,
                    "kills": 2,
                    "deaths": 8,
                    "assists": 3
                  },
                  {
                    "puuid": "puuid-3",
                    "participantId": 7,
                    "championName": "LeeSin",
                    "teamPosition": "JUNGLE",
                    "teamId": 200,
                    "win": false,
                    "kills": 3,
                    "deaths": 4,
                    "assists": 5
                  }
                ]
              }
            }
        """.trimIndent()

        When("Jackson deserializes the payload into MatchDto") {
            Then("info.gameVersion and all newly mapped early-CS and challenge fields are preserved without being stripped") {
                val matchDto = mapper.readValue(rawJson, MatchDto::class.java)
                matchDto.info!!.gameVersion shouldBe "16.1.648.1234"
                val serialized = mapper.writeValueAsString(matchDto)
                serialized.contains("\"gameVersion\":\"16.1.648.1234\"") shouldBe true

                val challenges = matchDto.info!!.participants.first().challenges!!

                challenges.laneMinionsFirst10Minutes shouldBe 84
                challenges.jungleCsBefore10Minutes shouldBe 4.0f
                challenges.killParticipation shouldBe 0.68f
                challenges.goldPerMinute shouldBe 445.2f
                challenges.damagePerMinute shouldBe 812.4f
                challenges.visionScorePerMinute shouldBe 1.45f
                challenges.turretPlatesTaken shouldBe 3
                challenges.controlWardsPlaced shouldBe 5
                challenges.visionScoreAdvantageLaneOpponent shouldBe 0.35f
                challenges.maxCsAdvantageOnLaneOpponent shouldBe 24.0f
                challenges.maxLevelLeadLaneOpponent shouldBe 2
                challenges.soloKills shouldBe 3
                challenges.takedownsFirst25Minutes shouldBe 4
                challenges.effectiveHealAndShielding shouldBe 120.5f
                challenges.scuttleCrabKills shouldBe 2
                challenges.wardTakedowns shouldBe 6
            }
        }

        When("TimelineAnalysisService analyzes a timeline containing KILL, DEATH, ASSIST, PLATE, OBJECTIVE, and WARD events (#17)") {
            Then("gold chart points keep combat/objective events without ward clutter, while eventTimelines and aggregateSummary record assistants and ward time buckets") {
                val baseMatchDto = mapper.readValue(rawJson, MatchDto::class.java)
                val matchDto = baseMatchDto.copy(
                    info = baseMatchDto.info!!.copy(
                        gameDuration = 780L,
                        participants = baseMatchDto.info!!.participants.map { p ->
                            if (p.puuid == "puuid-1") p.copy(kills = 2, deaths = 1, assists = 0, wardsPlaced = 2) else p
                        }
                    )
                )
                val timelineJson = """
                    {
                      "metadata": {
                        "matchId": "EUW1_7000000001",
                        "participants": ["puuid-1", "puuid-2", "puuid-3"]
                      },
                      "info": {
                        "frameInterval": 60000,
                        "participants": [
                          { "participantId": 1, "puuid": "puuid-1" },
                          { "participantId": 6, "puuid": "puuid-2" },
                          { "participantId": 7, "puuid": "puuid-3" }
                        ],
                        "frames": [
                          {
                            "timestamp": 300000,
                            "participantFrames": {
                              "1": {
                                "participantId": 1,
                                "totalGold": 2100,
                                "level": 6,
                                "xp": 2000,
                                "currentGold": 800,
                                "minionsKilled": 42,
                                "jungleMinionsKilled": 0,
                                "position": { "x": 7500, "y": 7500 }
                              },
                              "6": {
                                "participantId": 6,
                                "totalGold": 1800,
                                "level": 5,
                                "xp": 1850,
                                "currentGold": 500,
                                "minionsKilled": 36,
                                "jungleMinionsKilled": 0,
                                "position": { "x": 7700, "y": 7700 }
                              }
                            },
                            "events": [
                              {
                                "type": "CHAMPION_KILL",
                                "timestamp": 305000,
                                "killerId": 1,
                                "victimId": 6,
                                "assistingParticipantIds": [],
                                "position": { "x": 7600, "y": 7600 }
                              },
                              {
                                "type": "CHAMPION_KILL",
                                "timestamp": 315000,
                                "killerId": 6,
                                "victimId": 1,
                                "assistingParticipantIds": [7],
                                "position": { "x": 7400, "y": 7400 }
                              },
                              {
                                "type": "CHAMPION_KILL",
                                "timestamp": 320000,
                                "killerId": 1,
                                "victimId": 7,
                                "assistingParticipantIds": [],
                                "position": { "x": 7450, "y": 7450 }
                              },
                              {
                                "type": "TURRET_PLATE_DESTROYED",
                                "timestamp": 325000,
                                "killerId": 1
                              },
                              {
                                "type": "ELITE_MONSTER_KILL",
                                "timestamp": 340000,
                                "killerId": 1,
                                "monsterType": "DRAGON"
                              },
                              {
                                "type": "WARD_PLACED",
                                "timestamp": 240000,
                                "creatorId": 1,
                                "wardType": "YELLOW_TRINKET"
                              },
                              {
                                "type": "WARD_PLACED",
                                "timestamp": 720000,
                                "creatorId": 1,
                                "wardType": "CONTROL_WARD"
                              },
                              {
                                "type": "WARD_KILL",
                                "timestamp": 750000,
                                "killerId": 1,
                                "wardType": "YELLOW_TRINKET"
                              }
                            ]
                          }
                        ]
                      }
                    }
                """.trimIndent()
                val parsedSingleFrameTl = mapper.readValue(timelineJson, MatchTimelineDto::class.java)
                val minute5Frame = parsedSingleFrameTl.info!!.frames!!.single()
                val fullFrames = (0..13).map { minute ->
                    if (minute == 5) {
                        minute5Frame
                    } else {
                        com.pride.summoner_searcher_api.dto.TimelineFrameDto(
                            timestamp = minute * 60_000L,
                            participantFrames = emptyMap(),
                            events = emptyList()
                        )
                    }
                }
                val timelineDto = parsedSingleFrameTl.copy(
                    info = parsedSingleFrameTl.info!!.copy(frames = fullFrames)
                )

                val analysis = timelineService.analyze(
                    listOf(Triple("EUW1_7000000001", timelineDto, matchDto)),
                    "puuid-1"
                )
                val pointEvents = analysis.powerSpikeTimelines.first().points.first().events.map { it.type }

                pointEvents.containsAll(listOf("KILL", "DEATH", "PLATE", "OBJECTIVE")) shouldBe true
                pointEvents.contains("WARD_PLACED") shouldBe false
                pointEvents.contains("WARD_KILLED") shouldBe false

                val matchEvents = analysis.eventTimelines.first().events
                val soloKill = matchEvents.first { it.type == "KILL" && it.target == "Syndra" }
                soloKill.killer shouldBe "Ahri"
                soloKill.assistants shouldBe emptyList()

                val assistedDeath = matchEvents.first { it.type == "DEATH" && it.target == "Ahri" }
                assistedDeath.killer shouldBe "Syndra"
                assistedDeath.assistants shouldBe listOf("LeeSin")

                val wardPlacedEvents = matchEvents.filter { it.type == "WARD_PLACED" && it.isPlayer }
                wardPlacedEvents.size shouldBe 2
                wardPlacedEvents[0].wardType shouldBe "YELLOW_TRINKET"
                wardPlacedEvents[0].position shouldBe null
                wardPlacedEvents[1].wardType shouldBe "CONTROL_WARD"

                val wardKilledEvents = matchEvents.filter { it.type == "WARD_KILLED" && it.isPlayer }
                wardKilledEvents.size shouldBe 1
                wardKilledEvents[0].wardType shouldBe "YELLOW_TRINKET"

                val summary = analysis.aggregateSummary
                summary.wardsPlacedTotal shouldBe 2
                summary.wardsByTimeBucket shouldBe WardTimeBucketsDto(pre10 = 1, min10To20 = 1, post20 = 0)
                summary.deathsBefore10 shouldBe 1
                summary.assistedDeaths shouldBe 1
                summary.unassistedDeaths shouldBe 0
                summary.deathsNearObjectiveWindows shouldBe 1
            }
        }

        When("Jackson deserializes legacy cached MatchEventEntry and TimelineAggregateSummary payloads missing #17 fields") {
            Then("new fields deserialize as null (incomplete) rather than 0 or emptyList") {
                val legacyEventJson = """
                    {
                      "type": "DEATH",
                      "minuteMark": 5,
                      "secondMark": 15,
                      "actor": "Syndra",
                      "target": "Ahri",
                      "isPlayer": true,
                      "isOpponent": true,
                      "laneType": null,
                      "monsterType": null,
                      "position": { "x": 7400, "y": 7400 }
                    }
                """.trimIndent()
                val legacySummaryJson = """
                    {
                      "avgFirstDeathMinute": 6.5,
                      "mostDangerousZone": "Mid Lane",
                      "avgGoldLeadAt10": 150.0,
                      "avgGoldLeadAt15": 320.0,
                      "wardsPlacedTotal": 0,
                      "deathsTotal": 6,
                      "killsTotal": 12,
                      "assistsTotal": 18,
                      "gamesAnalyzed": 3
                    }
                """.trimIndent()

                val legacyEvent = mapper.readValue(legacyEventJson, MatchEventEntry::class.java)
                legacyEvent.killer shouldBe null
                legacyEvent.assistants shouldBe null
                legacyEvent.wardType shouldBe null

                val legacySummary = mapper.readValue(legacySummaryJson, TimelineAggregateSummary::class.java)
                legacySummary.deathsBefore10 shouldBe null
                legacySummary.assistedDeaths shouldBe null
                legacySummary.unassistedDeaths shouldBe null
                legacySummary.deathsNearObjectiveWindows shouldBe null
                legacySummary.wardsByTimeBucket shouldBe null
            }
        }
    }
})
