package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchTimelineDto
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
                "participants": ["puuid-1", "puuid-2"]
              },
              "info": {
                "gameCreation": 1730000000000,
                "gameDuration": 1800,
                "gameId": 7000000001,
                "gameMode": "CLASSIC",
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
                  }
                ]
              }
            }
        """.trimIndent()

        When("Jackson deserializes the payload into MatchDto") {
            Then("all newly mapped early-CS and challenge fields are preserved without being stripped") {
                val matchDto = mapper.readValue(rawJson, MatchDto::class.java)
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

        When("TimelineAnalysisService analyzes a timeline containing KILL, ASSIST, PLATE, and OBJECTIVE events") {
            Then("the gold chart timeline point retains ASSIST and OBJECTIVE events alongside KILL and PLATE") {
                val matchDto = mapper.readValue(rawJson, MatchDto::class.java)
                val timelineJson = """
                    {
                      "metadata": {
                        "matchId": "EUW1_7000000001",
                        "participants": ["puuid-1", "puuid-2"]
                      },
                      "info": {
                        "frameInterval": 60000,
                        "participants": [
                          { "participantId": 1, "puuid": "puuid-1" },
                          { "participantId": 6, "puuid": "puuid-2" }
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
                                "killerId": 2,
                                "victimId": 7,
                                "assistingParticipantIds": [1],
                                "position": { "x": 7400, "y": 7400 }
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
                              }
                            ]
                          }
                        ]
                      }
                    }
                """.trimIndent()
                val timelineDto = mapper.readValue(timelineJson, MatchTimelineDto::class.java)

                val analysis = timelineService.analyze(
                    listOf(Triple("EUW1_7000000001", timelineDto, matchDto)),
                    "puuid-1"
                )
                val pointEvents = analysis.powerSpikeTimelines.first().points.first().events.map { it.type }

                pointEvents.containsAll(listOf("KILL", "ASSIST", "PLATE", "OBJECTIVE")) shouldBe true
            }
        }
    }
})
