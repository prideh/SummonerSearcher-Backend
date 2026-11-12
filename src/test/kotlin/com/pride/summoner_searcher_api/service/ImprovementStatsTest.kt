package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.*
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe

class ImprovementStatsTest : DescribeSpec({

    val service = TimelineAnalysisService()
    val mapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    val playerPuuid = "player-puuid-1"

    fun buildParticipant(
        puuid: String,
        participantId: Int,
        teamId: Int,
        championName: String,
        teamPosition: String,
        win: Boolean,
        deaths: Int? = 2,
        totalTimeSpentDead: Int? = 60,
        turretKills: Int? = 1,
        turretTakedowns: Int? = 3,
        dragonKills: Int? = 0,
        dragonTakedowns: Int? = 2,
        baronKills: Int? = 0,
        voidMonsterKill: Int? = 3
    ): ParticipantDto {
        val raw = mapOf(
            "puuid" to puuid,
            "participantId" to participantId,
            "teamId" to teamId,
            "championName" to championName,
            "teamPosition" to teamPosition,
            "individualPosition" to teamPosition,
            "win" to win,
            "kills" to 5,
            "deaths" to deaths,
            "assists" to 7,
            "totalTimeSpentDead" to totalTimeSpentDead,
            "turretKills" to turretKills,
            "turretTakedowns" to turretTakedowns,
            "dragonKills" to dragonKills,
            "baronKills" to baronKills,
            "challenges" to mapOf(
                "dragonTakedowns" to dragonTakedowns,
                "voidMonsterKill" to voidMonsterKill
            )
        )
        return mapper.convertValue(raw, ParticipantDto::class.java)
    }

    fun buildFrame(
        minute: Int,
        playerFrame: ParticipantFrameDto,
        opponentFrame: ParticipantFrameDto? = null,
        events: List<TimelineEventDto> = emptyList()
    ): TimelineFrameDto {
        val pMap = mutableMapOf("1" to playerFrame)
        if (opponentFrame != null) {
            pMap["6"] = opponentFrame
        }
        return TimelineFrameDto(
            timestamp = minute * 60_000L,
            participantFrames = pMap,
            events = events
        )
    }

    fun pFrame(
        pid: Int,
        totalGold: Int?,
        xp: Int?,
        currentGold: Int?,
        laneCs: Int?,
        jungleCs: Int?
    ) = ParticipantFrameDto(
        participantId = pid,
        totalGold = totalGold,
        level = 10,
        xp = xp,
        currentGold = currentGold,
        minionsKilled = laneCs,
        jungleMinionsKilled = jungleCs,
        position = TimelinePositionDto(7500, 7500)
    )

    fun event(
        type: String,
        timestampMs: Long,
        participantId: Int? = null,
        killerId: Int? = null,
        victimId: Int? = null,
        assists: List<Int>? = null,
        itemId: Int? = null,
        buildingType: String? = null,
        laneType: String? = null,
        towerType: String? = null,
        teamId: Int? = null,
        killerTeamId: Int? = null,
        monsterType: String? = null,
        monsterSubType: String? = null,
        bounty: Int? = null,
        shutdownBounty: Int? = null
    ) = TimelineEventDto(
        type = type,
        timestamp = timestampMs,
        participantId = participantId,
        killerId = killerId,
        victimId = victimId,
        assistingParticipantIds = assists,
        itemId = itemId,
        afterId = null,
        beforeId = null,
        skillSlot = null,
        levelUpType = null,
        wardType = null,
        creatorId = null,
        level = null,
        buildingType = buildingType,
        laneType = laneType,
        towerType = towerType,
        teamId = teamId,
        killerTeamId = killerTeamId,
        monsterType = monsterType,
        monsterSubType = monsterSubType,
        position = TimelinePositionDto(7000, 7000),
        bounty = bounty,
        shutdownBounty = shutdownBounty,
        goldGain = null
    )

    describe("Issue #18 ImprovementStatsDto contract") {

        it("should deserialize provider-shaped MatchDto.info.teams objectives and preserve null vs 0") {
            val json = """
                {
                  "metadata": { "matchId": "EUW1_100", "participants": ["$playerPuuid"] },
                  "info": {
                    "gameId": 100,
                    "gameCreation": 1710000000000,
                    "gameDuration": 1800,
                    "gameMode": "CLASSIC",
                    "gameVersion": "14.20.1",
                    "queueId": 420,
                    "participants": [],
                    "teams": [
                      {
                        "teamId": 100,
                        "win": true,
                        "objectives": {
                          "tower": { "first": true, "kills": 9 },
                          "dragon": { "first": false, "kills": 3 },
                          "baron": { "first": true, "kills": 1 },
                          "riftHerald": { "first": false, "kills": 0 }
                        }
                      },
                      {
                        "teamId": 200,
                        "win": false,
                        "objectives": {
                          "tower": { "first": false, "kills": 2 },
                          "dragon": { "first": true, "kills": 1 },
                          "baron": { "first": false, "kills": 0 },
                          "riftHerald": { "first": true, "kills": 1 },
                          "horde": { "first": true, "kills": 6 }
                        }
                      }
                    ]
                  }
                }
            """.trimIndent()

            val match = mapper.readValue(json, MatchDto::class.java)
            val teams = match.info?.teams.shouldNotBeNull()
            teams shouldHaveSize 2
            val blueObj = teams[0].objectives.shouldNotBeNull()
            blueObj.tower?.first shouldBe true
            blueObj.tower?.kills shouldBe 9
            blueObj.riftHerald?.first shouldBe false
            blueObj.riftHerald?.kills shouldBe 0
            // horde was omitted on team 100 -> must stay null, never 0
            blueObj.horde.shouldBeNull()
            teams[1].objectives?.horde?.kills shouldBe 6
        }

        it("should compute phaseFarming, leadRetention, deathCost, unspentGold, teamObjectives, and champion/role breakdowns accurately across full, short, and partial games") {
            // ── Game 1: Full 30:00 (1800s) Ahri MIDDLE win ──
            // Phase farming:
            //   10m: lane=80,jg=4 | 20m: lane=170,jg=12 (+90 lane = 9.0/m, +8 jg = 0.8/m, total = 9.8/m)
            //   30m: lane=250,jg=24 (+80 lane = 8.0/m, +12 jg = 1.2/m, total = 9.2/m over 10m)
            // Lead retention:
            //   10m: playerGold=3800 vs oppGold=3350 (+450), playerXp=4600 vs oppXp=4280 (+320) -> ahead
            //   20m: playerGold=8200 vs oppGold=7300 (+900), playerXp=9800 vs oppXp=9200 (+600) -> retained!
            //   10-20m events:
            //     - 14:30 (870,000ms) Ally jungler (pid=2) takes DRAGON without player assist -> still included as ALLY_TEAM!
            //     - 16:00 (960,000ms) Enemy mid (pid=6) destroys Blue outer mid tower (teamId=100) -> ENEMY_TEAM!
            // Death cost:
            //   totalTimeSpentDead = 54s / 1800s = 0.03 (3.0%)
            //   2 deaths in timeline:
            //     - 08:00: bounty=300, shutdownBounty=150 -> combined=450
            //     - 22:00: bounty=300, shutdownBounty=0   -> combined=300
            // Unspent gold:
            //   currentGold at 0m=500, 10m=1600 (followed by purchase at 10:30=630,000ms itemId=3157),
            //   20m=900, 30m=1500 (no purchase after 30m)
            val m1Participants = listOf(
                buildParticipant(playerPuuid, 1, 100, "Ahri", "MIDDLE", win = true, deaths = 2, totalTimeSpentDead = 54, turretKills = 2, turretTakedowns = 4, dragonKills = 0, dragonTakedowns = 2, baronKills = 0, voidMonsterKill = 4),
                buildParticipant("ally-jg", 2, 100, "Vi", "JUNGLE", win = true),
                buildParticipant("opp-mid-1", 6, 200, "Syndra", "MIDDLE", win = false)
            )
            val match1 = MatchDto(
                metadata = MatchMetadata("M1", listOf(playerPuuid, "ally-jg", "opp-mid-1")),
                info = MatchInfo(
                    gameId = 1L,
                    gameCreation = 1_710_000_000_000L,
                    gameDuration = 1800L,
                    gameMode = "CLASSIC",
                    gameVersion = "14.20.1",
                    queueId = 420,
                    participants = m1Participants,
                    teams = listOf(
                        TeamDto(
                            teamId = 100,
                            win = true,
                            objectives = TeamObjectivesDto(
                                tower = ObjectiveStatsDto(first = true, kills = 9),
                                dragon = ObjectiveStatsDto(first = true, kills = 3),
                                baron = ObjectiveStatsDto(first = true, kills = 1),
                                riftHerald = ObjectiveStatsDto(first = false, kills = 0),
                                horde = ObjectiveStatsDto(first = true, kills = 5)
                            )
                        ),
                        TeamDto(
                            teamId = 200,
                            win = false,
                            objectives = TeamObjectivesDto(
                                tower = ObjectiveStatsDto(first = false, kills = 3),
                                dragon = ObjectiveStatsDto(first = false, kills = 1),
                                baron = ObjectiveStatsDto(first = false, kills = 0),
                                riftHerald = ObjectiveStatsDto(first = true, kills = 1),
                                horde = ObjectiveStatsDto(first = false, kills = 1)
                            )
                        )
                    )
                )
            )
            fun withContiguousEmptyMinuteFrames(
                durationMinutes: Int,
                explicitFrames: List<TimelineFrameDto>
            ): List<TimelineFrameDto> {
                val explicitByMinute = explicitFrames.associateBy { ((it.timestamp ?: 0L) / 60_000L).toInt() }
                return (0..durationMinutes).map { minute ->
                    explicitByMinute[minute] ?: TimelineFrameDto(
                        timestamp = minute * 60_000L,
                        participantFrames = emptyMap(),
                        events = emptyList()
                    )
                }
            }

            val timeline1 = MatchTimelineDto(
                metadata = MatchTimelineMetadata("M1", listOf(playerPuuid, "ally-jg", "opp-mid-1")),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    participants = listOf(
                        TimelineParticipantDto(1, playerPuuid),
                        TimelineParticipantDto(2, "ally-jg"),
                        TimelineParticipantDto(6, "opp-mid-1")
                    ),
                    frames = withContiguousEmptyMinuteFrames(
                        durationMinutes = 30,
                        explicitFrames = listOf(
                            buildFrame(
                                minute = 0,
                                playerFrame = pFrame(1, totalGold = 500, xp = 0, currentGold = 500, laneCs = 0, jungleCs = 0),
                                opponentFrame = pFrame(6, totalGold = 500, xp = 0, currentGold = 500, laneCs = 0, jungleCs = 0)
                            ),
                            buildFrame(
                                minute = 8,
                                playerFrame = pFrame(1, totalGold = 2800, xp = 3400, currentGold = 700, laneCs = 62, jungleCs = 0),
                                opponentFrame = pFrame(6, totalGold = 2700, xp = 3300, currentGold = 600, laneCs = 60, jungleCs = 0),
                                events = listOf(
                                    event("CHAMPION_KILL", 480_000L, killerId = 6, victimId = 1, bounty = 300, shutdownBounty = 150)
                                )
                            ),
                            buildFrame(
                                minute = 10,
                                playerFrame = pFrame(1, totalGold = 3800, xp = 4600, currentGold = 1600, laneCs = 80, jungleCs = 4),
                                opponentFrame = pFrame(6, totalGold = 3350, xp = 4280, currentGold = 400, laneCs = 72, jungleCs = 0),
                                events = listOf(
                                    event("ITEM_PURCHASED", 630_000L, participantId = 1, itemId = 3157)
                                )
                            ),
                            buildFrame(
                                minute = 15,
                                playerFrame = pFrame(1, totalGold = 5800, xp = 7000, currentGold = 800, laneCs = 125, jungleCs = 8),
                                opponentFrame = pFrame(6, totalGold = 5200, xp = 6600, currentGold = 500, laneCs = 115, jungleCs = 4),
                                events = listOf(
                                    // Ally jungler (pid=2) solos dragon at 14:30 (870,000ms) - player NOT in assists
                                    event("ELITE_MONSTER_KILL", 870_000L, killerId = 2, assists = emptyList(), killerTeamId = 100, monsterType = "DRAGON", monsterSubType = "FIRE_DRAGON"),
                                    // Enemy mid (pid=6) destroys Blue mid outer tower (teamId=100) at 16:00 (960,000ms)
                                    event("BUILDING_KILL", 960_000L, killerId = 6, teamId = 100, buildingType = "TOWER_BUILDING", laneType = "MID_LANE", towerType = "OUTER_TURRET")
                                )
                            ),
                            buildFrame(
                                minute = 20,
                                playerFrame = pFrame(1, totalGold = 8200, xp = 9800, currentGold = 900, laneCs = 170, jungleCs = 12),
                                opponentFrame = pFrame(6, totalGold = 7300, xp = 9200, currentGold = 600, laneCs = 155, jungleCs = 4)
                            ),
                            buildFrame(
                                minute = 22,
                                playerFrame = pFrame(1, totalGold = 9000, xp = 11000, currentGold = 1100, laneCs = 185, jungleCs = 16),
                                opponentFrame = pFrame(6, totalGold = 8400, xp = 10600, currentGold = 700, laneCs = 170, jungleCs = 8),
                                events = listOf(
                                    event("CHAMPION_KILL", 1_320_000L, killerId = 6, victimId = 1, bounty = 300, shutdownBounty = 0)
                                )
                            ),
                            buildFrame(
                                minute = 30,
                                playerFrame = pFrame(1, totalGold = 13500, xp = 15500, currentGold = 1500, laneCs = 250, jungleCs = 24),
                                opponentFrame = pFrame(6, totalGold = 11800, xp = 14200, currentGold = 400, laneCs = 220, jungleCs = 12)
                            )
                        )
                    )
                )
            )

            // ── Game 2: Short 15:00 (900s) Ahri MIDDLE early surrender ──
            // Reaches 10m (behind in gold/xp: -300g, -200xp), never reaches 20m -> excluded from 10-20m and 20m-end phase farming!
            // Death has null bounty/shutdownBounty (legacy/missing fields) -> must stay null, not 0!
            // horde is null on team objectives -> excluded from horde denominator!
            val m2Participants = listOf(
                buildParticipant(playerPuuid, 1, 100, "Ahri", "MIDDLE", win = false, deaths = 1, totalTimeSpentDead = 18, turretKills = 0, turretTakedowns = 0, dragonKills = 0, dragonTakedowns = 0, baronKills = 0, voidMonsterKill = null),
                buildParticipant("opp-mid-2", 6, 200, "Zed", "MIDDLE", win = true)
            )
            val match2 = MatchDto(
                metadata = MatchMetadata("M2", listOf(playerPuuid, "opp-mid-2")),
                info = MatchInfo(
                    gameId = 2L,
                    gameCreation = 1_710_000_100_000L,
                    gameDuration = 900L,
                    gameMode = "CLASSIC",
                    gameVersion = "14.20.1",
                    queueId = 420,
                    participants = m2Participants,
                    teams = listOf(
                        TeamDto(
                            teamId = 100,
                            win = false,
                            objectives = TeamObjectivesDto(
                                tower = ObjectiveStatsDto(first = false, kills = 1),
                                dragon = ObjectiveStatsDto(first = false, kills = 0),
                                baron = ObjectiveStatsDto(first = false, kills = 0),
                                riftHerald = ObjectiveStatsDto(first = false, kills = 0),
                                horde = null // Absent on this payload -> must not be counted as 0
                            )
                        ),
                        TeamDto(
                            teamId = 200,
                            win = true,
                            objectives = TeamObjectivesDto(
                                tower = ObjectiveStatsDto(first = true, kills = 4),
                                dragon = ObjectiveStatsDto(first = true, kills = 2),
                                baron = ObjectiveStatsDto(first = false, kills = 0),
                                riftHerald = ObjectiveStatsDto(first = true, kills = 1),
                                horde = null
                            )
                        )
                    )
                )
            )
            val timeline2 = MatchTimelineDto(
                metadata = MatchTimelineMetadata("M2", listOf(playerPuuid, "opp-mid-2")),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    participants = listOf(
                        TimelineParticipantDto(1, playerPuuid),
                        TimelineParticipantDto(6, "opp-mid-2")
                    ),
                    frames = withContiguousEmptyMinuteFrames(
                        durationMinutes = 15,
                        explicitFrames = listOf(
                            buildFrame(
                                minute = 10,
                                playerFrame = pFrame(1, totalGold = 3100, xp = 4100, currentGold = 600, laneCs = 70, jungleCs = 0),
                                opponentFrame = pFrame(6, totalGold = 3400, xp = 4300, currentGold = 800, laneCs = 82, jungleCs = 0),
                                events = listOf(
                                    // Death with null bounty & null shutdownBounty
                                    event("CHAMPION_KILL", 540_000L, killerId = 6, victimId = 1, bounty = null, shutdownBounty = null)
                                )
                            ),
                            buildFrame(
                                minute = 15,
                                playerFrame = pFrame(1, totalGold = 4500, xp = 6000, currentGold = 400, laneCs = 105, jungleCs = 0),
                                opponentFrame = pFrame(6, totalGold = 5300, xp = 6700, currentGold = 900, laneCs = 128, jungleCs = 4)
                            )
                        )
                    )
                )
            )

            // ── Game 3: Exactly 20:00 (1200s) LeeSin JUNGLE loss ──
            // Reaches 10-20m (lane 10->25 = +15 = 1.5/m, jg 60->130 = +70 = 7.0/m),
            // ends at 20:00 so excluded from 20m-end!
            // Ahead at 10m (+500g, +250xp), behind at 20m (-600g, -300xp) -> lead NOT retained!
            // totalTimeSpentDead is null (incomplete legacy participant field) -> excluded from timeDead denominator!
            // info.teams is null (legacy match without teams) -> excluded from teamObjectives denominator!
            val m3Participants = listOf(
                buildParticipant(playerPuuid, 1, 100, "LeeSin", "JUNGLE", win = false, deaths = 3, totalTimeSpentDead = null, turretKills = null, turretTakedowns = null, dragonKills = null, dragonTakedowns = null, baronKills = null, voidMonsterKill = null),
                buildParticipant("opp-jg-3", 6, 200, "Viego", "JUNGLE", win = true)
            )
            val match3 = MatchDto(
                metadata = MatchMetadata("M3", listOf(playerPuuid, "opp-jg-3")),
                info = MatchInfo(
                    gameId = 3L,
                    gameCreation = 1_710_000_200_000L,
                    gameDuration = 1200L,
                    gameMode = "CLASSIC",
                    gameVersion = "14.20.1",
                    queueId = 420,
                    participants = m3Participants,
                    teams = null
                )
            )
            val timeline3 = MatchTimelineDto(
                metadata = MatchTimelineMetadata("M3", listOf(playerPuuid, "opp-jg-3")),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    participants = listOf(
                        TimelineParticipantDto(1, playerPuuid),
                        TimelineParticipantDto(6, "opp-jg-3")
                    ),
                    frames = withContiguousEmptyMinuteFrames(
                        durationMinutes = 20,
                        explicitFrames = listOf(
                            buildFrame(
                                minute = 10,
                                playerFrame = pFrame(1, totalGold = 3900, xp = 4200, currentGold = 1000, laneCs = 10, jungleCs = 60),
                                opponentFrame = pFrame(6, totalGold = 3400, xp = 3950, currentGold = 500, laneCs = 8, jungleCs = 52)
                            ),
                            buildFrame(
                                minute = 20,
                                playerFrame = pFrame(1, totalGold = 6800, xp = 8100, currentGold = 300, laneCs = 25, jungleCs = 130),
                                opponentFrame = pFrame(6, totalGold = 7400, xp = 8400, currentGold = 900, laneCs = 30, jungleCs = 148)
                            )
                        )
                    )
                )
            )

            // ── Game 4: 25:00 (1500s) Orianna MIDDLE win, but timeline is missing (passed only in allMatches) ──
            val m4Participants = listOf(
                buildParticipant(playerPuuid, 1, 100, "Orianna", "MIDDLE", win = true, deaths = 1, totalTimeSpentDead = 30, turretKills = 1, turretTakedowns = 2, dragonKills = 1, dragonTakedowns = 3, baronKills = 1, voidMonsterKill = 2),
                buildParticipant("opp-mid-4", 6, 200, "Viktor", "MIDDLE", win = false)
            )
            val match4 = MatchDto(
                metadata = MatchMetadata("M4", listOf(playerPuuid, "opp-mid-4")),
                info = MatchInfo(
                    gameId = 4L,
                    gameCreation = 1_710_000_300_000L,
                    gameDuration = 1500L,
                    gameMode = "CLASSIC",
                    gameVersion = "14.20.1",
                    queueId = 420,
                    participants = m4Participants,
                    teams = listOf(
                        TeamDto(
                            teamId = 100,
                            win = true,
                            objectives = TeamObjectivesDto(
                                tower = ObjectiveStatsDto(first = true, kills = 7),
                                dragon = ObjectiveStatsDto(first = false, kills = 3),
                                baron = ObjectiveStatsDto(first = true, kills = 1),
                                riftHerald = ObjectiveStatsDto(first = true, kills = 1),
                                horde = ObjectiveStatsDto(first = false, kills = 3)
                            )
                        )
                    )
                )
            )

            val analysis = service.analyze(
                timelines = listOf(
                    Triple("M1", timeline1, match1),
                    Triple("M2", timeline2, match2),
                    Triple("M3", timeline3, match3)
                ),
                puuid = playerPuuid,
                allMatches = listOf(match1, match2, match3, match4)
            )

            val stats = analysis.improvementStats.shouldNotBeNull()
            stats.totalGamesConsidered shouldBe 4
            stats.excludedInvalidDurationGames shouldBe 0
            stats.excludedMissingTimelineGames shouldBe 1

            // ── 1. Phase Farming assertions ──
            val pf = stats.phaseFarming
            pf.totalGamesConsidered shouldBe 4
            pf.excludedMissingTimelineGames shouldBe 1
            // 10-20m: M1 (+90 lane, +8 jg) and M3 (+15 lane, +70 jg) are eligible (2 games); M2 (15m) is excludedShortGames (1)
            pf.min10To20.eligibleGames shouldBe 2
            pf.min10To20.excludedShortGames shouldBe 1
            pf.min10To20.avgLaneCsGained shouldBe 52.5      // (90 + 15) / 2
            pf.min10To20.avgJungleCsGained shouldBe 39.0    // (8 + 70) / 2
            pf.min10To20.avgLaneCsPerMin shouldBe 5.25      // (9.0 + 1.5) / 2
            pf.min10To20.avgJungleCsPerMin shouldBe 3.9     // (0.8 + 7.0) / 2
            pf.min10To20.avgTotalCsPerMin shouldBe 9.15     // (9.8 + 8.5) / 2

            // 20m-end: Only M1 (30m) reached >20m! M2 (15m) and M3 (20m exact) are excludedShortGames (2)
            pf.min20ToEnd.eligibleGames shouldBe 1
            pf.min20ToEnd.excludedShortGames shouldBe 2
            pf.min20ToEnd.avgLaneCsGained shouldBe 80.0
            pf.min20ToEnd.avgJungleCsGained shouldBe 12.0
            pf.min20ToEnd.avgLaneCsPerMin shouldBe 8.0
            pf.min20ToEnd.avgJungleCsPerMin shouldBe 1.2
            pf.min20ToEnd.avgTotalCsPerMin shouldBe 9.2

            // ── 2. Lead Retention assertions ──
            val lr = stats.leadRetention
            lr.totalGamesConsidered shouldBe 4
            lr.excludedMissingTimelineGames shouldBe 1
            lr.eligibleGamesAt10 shouldBe 3                 // M1 (+450g), M2 (-300g), M3 (+500g)
            lr.eligibleGamesAt20 shouldBe 2                 // M1 (+900g), M3 (-600g); M2 excludedShortGamesBefore20 = 1
            lr.excludedShortGamesBefore20 shouldBe 1
            lr.avgGoldLeadAt10 shouldBe 216.7               // (450 - 300 + 500) / 3 = 216.666...
            lr.avgGoldLeadAt20 shouldBe 150.0               // (900 - 600) / 2 = 150.0
            lr.gamesAheadInGoldAt10 shouldBe 2              // M1 and M3
            lr.gamesAheadInGoldAt10Reaching20 shouldBe 2    // M1 and M3
            lr.gamesRetainedGoldLeadAt20 shouldBe 1         // M1 retained, M3 lost lead
            lr.goldLeadRetentionRate shouldBe 0.5
            lr.gamesAheadInXpAt10 shouldBe 2
            lr.gamesAheadInXpAt10Reaching20 shouldBe 2
            lr.gamesRetainedXpLeadAt20 shouldBe 1
            lr.xpLeadRetentionRate shouldBe 0.5

            // Non-personal objective/tower events in 10-20m are included for temporal association
            lr.allyObjectiveEvents10To20 shouldBe 1
            lr.enemyTowerEvents10To20 shouldBe 1
            val m1Lr = lr.perMatch.first { it.matchId == "M1" }
            m1Lr.temporalEvents10To20 shouldHaveSize 2
            m1Lr.temporalEvents10To20[0].objectiveType shouldBe "DRAGON"
            m1Lr.temporalEvents10To20[0].securingTeam shouldBe "ALLY_TEAM"
            m1Lr.temporalEvents10To20[0].playerPersonallyInvolved shouldBe false
            m1Lr.temporalEvents10To20[1].objectiveType shouldBe "TOWER_BUILDING"
            m1Lr.temporalEvents10To20[1].securingTeam shouldBe "ENEMY_TEAM"

            // ── 3. Death Cost assertions ──
            val dc = stats.deathCost
            dc.totalGamesConsidered shouldBe 4
            dc.eligibleTimeDeadGames shouldBe 3             // M1 (54s), M2 (18s), M4 (30s); M3 has totalTimeSpentDead=null
            dc.excludedMissingTimeSpentDeadGames shouldBe 1
            dc.totalTimeSpentDeadSeconds shouldBe 102       // 54 + 18 + 30
            dc.avgTimeSpentDeadSeconds shouldBe 34.0        // 102 / 3
            // Per-game shares: M1 = 54/1800 = 0.03, M2 = 18/900 = 0.02, M4 = 30/1500 = 0.02 -> avg = 0.0233 (2.33%)
            dc.avgDeadTimeGameShare shouldBe 0.0233
            dc.avgDeadTimeGameSharePct shouldBe 2.33
            // M3 has null totalTimeSpentDead -> stays null, not 0
            dc.perMatch.first { it.matchId == "M3" }.totalTimeSpentDeadSeconds.shouldBeNull()

            // Bounties on player deaths (M1 has 2 deaths with bounty data, M2 has 1 death with null bounty, M3 has 0 deaths)
            val bounty = dc.bountyOnPlayerDeaths
            bounty.eligibleTimelineGames shouldBe 3
            bounty.excludedMissingTimelineGames shouldBe 1
            bounty.totalRecordedDeaths shouldBe 3           // 2 in M1 + 1 in M2 + 0 in M3
            bounty.deathsWithBaseBountyData shouldBe 2
            bounty.deathsWithShutdownBountyData shouldBe 2
            bounty.deathsWithAnyBountyData shouldBe 2
            bounty.deathsMissingBountyData shouldBe 1       // M2's death had null bounty/shutdownBounty
            bounty.totalBaseKillBountyGold shouldBe 600     // 300 + 300
            bounty.totalShutdownBountyGold shouldBe 150     // 150 + 0
            bounty.totalCombinedBountyGold shouldBe 750     // 450 + 300 (never double-counted)
            // M2's death had null bounty -> per-match bounty sums on M2 must be null, NOT 0!
            val m2Dc = dc.perMatch.first { it.matchId == "M2" }
            m2Dc.recordedDeathsCount shouldBe 1
            m2Dc.totalBaseKillBountyGold.shouldBeNull()
            m2Dc.totalShutdownBountyGold.shouldBeNull()
            m2Dc.totalCombinedBountyGold.shouldBeNull()

            // ── 4. Unspent Gold assertions ──
            val ug = stats.unspentGold
            ug.eligibleGames shouldBe 3                     // M1, M2, M3; M4 missing timeline
            ug.excludedMissingTimelineGames shouldBe 1
            // Samples: M1=[500, 700, 1600, 800, 900, 1100, 1500] (7), M2=[600, 400] (2), M3=[1000, 300] (2) -> total 11 samples
            // Sorted: [300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1500, 1600] -> median (6th) = 800.0, max = 1600
            ug.sampleCount shouldBe 11
            ug.medianCurrentGold shouldBe 800.0
            ug.maxCurrentGold shouldBe 1600
            ug.highBalanceSampleCount shouldBe 2            // 1600 at 10m and 1500 at 30m in M1
            // Only the 10m sample (1600g) was followed by an ITEM_PURCHASED event (at 10:30, itemId=3157)
            ug.highBalanceBeforePurchaseSampleCount shouldBe 1
            ug.highBalanceBeforePurchaseSamples shouldHaveSize 1
            ug.highBalanceBeforePurchaseSamples[0].sampledMinute shouldBe 10
            ug.highBalanceBeforePurchaseSamples[0].currentGoldAtSample shouldBe 1600
            ug.highBalanceBeforePurchaseSamples[0].nextPurchaseItemId shouldBe 3157

            // ── 5. Team Objectives assertions ──
            val to = stats.teamObjectives
            to.totalGamesConsidered shouldBe 4
            to.eligibleTeamObjectiveGames shouldBe 3        // M1, M2, M4 have info.teams; M3 has teams=null
            to.excludedMissingTeamsDataGames shouldBe 1
            // Tower: M1 (first=true, kills=9), M2 (first=false, kills=1), M4 (first=true, kills=7) -> 3 games, 2 firsts, 17 kills
            to.tower.eligibleGamesWithKills shouldBe 3
            to.tower.teamFirstCount shouldBe 2
            to.tower.teamFirstRate shouldBe 0.6667
            to.tower.teamTotalKills shouldBe 17
            to.tower.teamAvgKills shouldBe 5.67
            // Personal turret kills vs takedowns (M1: 2/4, M2: 0/0, M4: 1/2; M3 null)
            to.tower.personalLastHitEligibleGames shouldBe 3
            to.tower.personalTotalLastHits shouldBe 3
            to.tower.personalTotalTakedowns shouldBe 6

            // Rift Herald: M1 (0), M2 (0), M4 (1) -> 0 kills in M1/M2 is preserved as 0, so 3 eligible games, total=1
            to.riftHerald.eligibleGamesWithKills shouldBe 3
            to.riftHerald.teamTotalKills shouldBe 1
            to.riftHerald.teamFirstCount shouldBe 1

            // Horde (Voidgrubs): M1 (first=true, kills=5), M2 (null!), M3 (teams=null!), M4 (first=false, kills=3)
            // Only 2 eligible games (M1 and M4), 2 excluded!
            to.horde.eligibleGamesWithKills shouldBe 2
            to.horde.excludedMissingObjectiveGames shouldBe 2
            to.horde.teamTotalKills shouldBe 8
            to.horde.teamAvgKills shouldBe 4.0
            to.horde.teamFirstCount shouldBe 1
            to.horde.teamFirstRate shouldBe 0.5

            // ── 6. Champion & Role Breakdowns ──
            stats.byChampion.keys shouldBe setOf("Ahri", "LeeSin", "Orianna")
            val ahriSlice = stats.byChampion["Ahri"].shouldNotBeNull()
            ahriSlice.totalGamesConsidered shouldBe 2
            ahriSlice.phaseFarming.min10To20.eligibleGames shouldBe 1
            ahriSlice.phaseFarming.min10To20.excludedShortGames shouldBe 1
            ahriSlice.phaseFarming.min10To20.avgLaneCsPerMin shouldBe 9.0

            val leeSlice = stats.byChampion["LeeSin"].shouldNotBeNull()
            leeSlice.totalGamesConsidered shouldBe 1
            leeSlice.deathCost.eligibleTimeDeadGames shouldBe 0
            leeSlice.deathCost.totalTimeSpentDeadSeconds.shouldBeNull()
            leeSlice.teamObjectives.eligibleTeamObjectiveGames shouldBe 0
            leeSlice.teamObjectives.tower.teamTotalKills.shouldBeNull()

            stats.byRole.keys shouldBe setOf("MIDDLE", "JUNGLE")
            val midSlice = stats.byRole["MIDDLE"].shouldNotBeNull()
            midSlice.totalGamesConsidered shouldBe 3       // M1 (Ahri), M2 (Ahri), M4 (Orianna)
            midSlice.excludedMissingTimelineGames shouldBe 1 // M4
            val jgSlice = stats.byRole["JUNGLE"].shouldNotBeNull()
            jgSlice.totalGamesConsidered shouldBe 1        // M3 (LeeSin)
            jgSlice.phaseFarming.min10To20.avgJungleCsPerMin shouldBe 7.0
            jgSlice.phaseFarming.min20ToEnd.eligibleGames shouldBe 0
            jgSlice.phaseFarming.min20ToEnd.avgJungleCsPerMin.shouldBeNull()

            // ── 7. Real Serialized DTO Contract Fixture Verification (for Frontend #15 integration) ──
            val prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(stats)
            val fixtureFile = java.io.File("src/test/resources/fixtures/improvement-stats-contract.json")
            fixtureFile.exists() shouldBe true
            val expectedJson = fixtureFile.readText()
            mapper.readTree(prettyJson) shouldBe mapper.readTree(expectedJson)
            mapper.readValue(expectedJson, ImprovementStatsDto::class.java) shouldBe stats
        }

        it("should lazily backfill cached timelines in TimelineService when CHAMPION_KILL events predate bounty/shutdownBounty") {
            val riotApiService = io.mockk.mockk<RiotApiService>()
            val redisCacheService = io.mockk.mockk<RedisCacheService>(relaxed = true)
            val timelineService = TimelineService(riotApiService, redisCacheService)

            val legacyTimeline = MatchTimelineDto(
                metadata = MatchTimelineMetadata("EUW1_999", listOf(playerPuuid)),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    participants = listOf(TimelineParticipantDto(1, playerPuuid)),
                    frames = listOf(
                        TimelineFrameDto(
                            timestamp = 300_000L,
                            participantFrames = emptyMap(),
                            events = listOf(
                                event("CHAMPION_KILL", 290_000L, killerId = 6, victimId = 1, bounty = null, shutdownBounty = null)
                            )
                        )
                    )
                )
            )
            val freshTimeline = MatchTimelineDto(
                metadata = MatchTimelineMetadata("EUW1_999", listOf(playerPuuid)),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    participants = listOf(TimelineParticipantDto(1, playerPuuid)),
                    frames = listOf(
                        TimelineFrameDto(
                            timestamp = 300_000L,
                            participantFrames = emptyMap(),
                            events = listOf(
                                event("CHAMPION_KILL", 290_000L, killerId = 6, victimId = 1, bounty = 300, shutdownBounty = 150)
                            )
                        )
                    )
                )
            )

            io.mockk.every { redisCacheService.get("timeline:euw1:EUW1_999", MatchTimelineDto::class.java) } returns legacyTimeline
            io.mockk.coEvery { riotApiService.getMatchTimeline("EUW1_999", "euw1") } returns freshTimeline

            val result = kotlinx.coroutines.runBlocking {
                timelineService.getTimeline("EUW1_999", "euw1")
            }

            val killEvent = result?.info?.frames?.first()?.events?.first().shouldNotBeNull()
            killEvent.bounty shouldBe 300
            killEvent.shutdownBounty shouldBe 150
            io.mockk.verify(exactly = 1) {
                redisCacheService.set("timeline:euw1:EUW1_999", freshTimeline, java.time.Duration.ofHours(24))
            }
        }
    }
})
