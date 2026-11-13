package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.config.RestClientConfig
import com.pride.summoner_searcher_api.dto.*
import com.sun.net.httpserver.HttpServer
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPOutputStream

class CoachPrepRealProfileBaselineReproTest {

    private val objectMapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    private val targetPuuid = "spear-shot-1111-real-puuid-0123456789abcdef0123456789abcdef0123456789ab"
    private val oppPuuid = "opp-top-euw1-puuid-fedcba9876543210fedcba9876543210fedcba9876543210fedc"

    private fun buildRichMatchDto(matchId: String, gameIndex: Int, extraChallengesPadding: Int = 0): MatchDto {
        val participants = (1..10).map { pid ->
            val isPlayer = pid == 1
            val isOpp = pid == 6
            val teamId = if (pid <= 5) 100 else 200
            val pos = when ((pid - 1) % 5) {
                0 -> "TOP"
                1 -> "JUNGLE"
                2 -> "MIDDLE"
                3 -> "BOTTOM"
                else -> "UTILITY"
            }
            val raw = mutableMapOf<String, Any?>(
                "puuid" to when {
                    isPlayer -> targetPuuid
                    isOpp -> oppPuuid
                    else -> "puuid-m${gameIndex}-p${pid}-${(gameIndex * 31 + pid * 17).toString(16).padStart(56, 'a')}"
                },
                "participantId" to pid,
                "teamId" to teamId,
                "teamPosition" to pos,
                "individualPosition" to pos,
                "lane" to pos,
                "role" to if (pos == "UTILITY") "SUPPORT" else "SOLO",
                "championId" to (80 + (pid + gameIndex) % 60),
                "championName" to if (isPlayer) "Pantheon" else if (isOpp) "Aatrox" else "Champion${(pid + gameIndex) % 45}",
                "riotIdGameName" to if (isPlayer) "Spear Shot" else "ChallengerEUW_${gameIndex}_$pid",
                "riotIdTagline" to if (isPlayer) "1111" else "EUW$pid",
                "summonerName" to if (isPlayer) "Spear Shot" else "ChallengerEUW_${gameIndex}_$pid",
                "summonerId" to "enc-sum-${gameIndex}-${pid}-${"x".repeat(32 + extraChallengesPadding)}",
                "profileIcon" to (4200 + pid),
                "summonerLevel" to (650 + gameIndex + pid),
                "win" to (if (gameIndex % 2 == 0) teamId == 100 else teamId == 200),
                "kills" to (5 + (pid + gameIndex) % 9),
                "deaths" to (3 + (pid * 3 + gameIndex) % 7),
                "assists" to (6 + (pid * 2 + gameIndex) % 11),
                "champLevel" to (15 + pid % 4),
                "champExperience" to (15840 + pid * 237 + gameIndex * 89),
                "totalMinionsKilled" to (185 + pid * 5 + gameIndex % 23),
                "neutralMinionsKilled" to (18 + pid * 3),
                "totalAllyJungleMinionsKilled" to (8 + pid),
                "totalEnemyJungleMinionsKilled" to (10 + pid),
                "visionScore" to (26 + pid * 2),
                "wardsPlaced" to (12 + pid % 6),
                "wardsKilled" to (5 + pid % 4),
                "detectorWardsPlaced" to (3 + pid % 3),
                "goldEarned" to (13450 + pid * 345 + gameIndex * 119),
                "goldSpent" to (12700 + pid * 310 + gameIndex * 97),
                "bountyLevel" to (pid % 4),
                "totalDamageDealt" to (165000 + pid * 5321 + gameIndex * 913),
                "physicalDamageDealt" to (112000 + pid * 3400),
                "magicDamageDealt" to (38000 + pid * 1200),
                "trueDamageDealt" to (15000 + pid * 721),
                "totalDamageDealtToChampions" to (26500 + pid * 1323 + gameIndex * 331),
                "physicalDamageDealtToChampions" to (18200 + pid * 731),
                "magicDamageDealtToChampions" to (5900 + pid * 289),
                "trueDamageDealtToChampions" to (2400 + pid * 103),
                "totalDamageTaken" to (29400 + pid * 947 + gameIndex * 211),
                "physicalDamageTaken" to (17200 + pid * 512),
                "magicDamageTaken" to (9800 + pid * 319),
                "trueDamageTaken" to (2400 + pid * 116),
                "damageSelfMitigated" to (24300 + pid * 812),
                "damageDealtToObjectives" to (11800 + pid * 650),
                "damageDealtToBuildings" to (4600 + pid * 310),
                "damageDealtToTurrets" to (4600 + pid * 310),
                "totalHeal" to (6400 + pid * 290),
                "totalHealsOnTeammates" to (420 + pid * 50),
                "totalDamageShieldedOnTeammates" to (310 + pid * 45),
                "timeCCingOthers" to (28 + pid * 2),
                "totalTimeCCDealt" to (195 + pid * 11),
                "totalTimeSpentDead" to (42 + pid * 4 + gameIndex % 17),
                "longestTimeSpentLiving" to (590 + pid * 19),
                "timePlayed" to 1860,
                "item0" to 6692, "item1" to 3071, "item2" to 3111,
                "item3" to 6333, "item4" to 3156, "item5" to 3026, "item6" to 3364,
                "itemsPurchased" to (24 + pid),
                "consumablesPurchased" to (5 + pid % 4),
                "summoner1Id" to 4, "summoner1Casts" to (4 + pid % 3),
                "summoner2Id" to 14, "summoner2Casts" to (6 + pid % 4),
                "spell1Casts" to (118 + pid * 7),
                "spell2Casts" to (49 + pid * 3),
                "spell3Casts" to (38 + pid * 2),
                "spell4Casts" to (8 + pid % 4),
                "turretKills" to (1 + pid % 3),
                "turretTakedowns" to (3 + pid % 4),
                "turretsLost" to 5,
                "inhibitorKills" to (pid % 2),
                "inhibitorTakedowns" to (1 + pid % 2),
                "inhibitorsLost" to 1,
                "dragonKills" to (if (pid == 2) 3 else 0),
                "baronKills" to (if (pid == 2) 1 else 0),
                "firstBloodKill" to (pid == 1 && gameIndex % 3 == 0),
                "firstBloodAssist" to (pid == 2 && gameIndex % 3 == 0),
                "firstTowerKill" to (pid == 1 && gameIndex % 4 == 0),
                "firstTowerAssist" to false,
                "doubleKills" to (pid % 2),
                "tripleKills" to 0, "quadraKills" to 0, "pentaKills" to 0,
                "killingSprees" to (1 + pid % 2),
                "largestKillingSpree" to (3 + pid % 4),
                "largestMultiKill" to (1 + pid % 2),
                "largestCriticalStrike" to 0,
                "allInPings" to (3 + pid % 4),
                "assistMePings" to (4 + pid % 5),
                "dangerPings" to (2 + pid % 3),
                "enemyMissingPings" to (7 + pid % 6),
                "enemyVisionPings" to (5 + pid % 4),
                "getBackPings" to (4 + pid % 4),
                "onMyWayPings" to (9 + pid % 5),
                "pushPings" to (3 + pid % 3),
                "needVisionPings" to (2 + pid % 3),
                "challenges" to mapOf(
                    "kda" to 3.75 + pid * 0.11,
                    "killParticipation" to 0.58,
                    "damagePerMinute" to 842.4 + pid * 13.7 + gameIndex * 4.1,
                    "goldPerMinute" to 438.9 + pid * 9.3 + gameIndex * 2.7,
                    "teamDamagePercentage" to 0.24,
                    "damageTakenOnTeamPercentage" to 0.23,
                    "visionScorePerMinute" to 1.04 + pid * 0.03,
                    "visionScoreAdvantageLaneOpponent" to 0.22,
                    "laneMinionsFirst10Minutes" to (76 + pid % 9),
                    "jungleCsBefore10Minutes" to (if (pid in setOf(2, 7)) 62.0 else 0.0),
                    "maxCsAdvantageOnLaneOpponent" to 18.0 + pid,
                    "maxLevelLeadLaneOpponent" to 2,
                    "turretPlatesTaken" to (3 + pid % 3),
                    "soloKills" to (1 + pid % 3),
                    "outnumberedKills" to (pid % 2),
                    "pickKillWithAlly" to (4 + pid),
                    "saveAllyFromDeath" to (pid % 2),
                    "immobilizeAndKillWithAlly" to (5 + pid),
                    "knockEnemyIntoTeamAndKill" to 0,
                    "skillshotsDodged" to (38 + pid * 4 + gameIndex),
                    "skillshotsHit" to (44 + pid * 3 + gameIndex),
                    "dodgeSkillShotsSmallWindow" to (7 + pid),
                    "landSkillShotsEarlyGame" to (11 + pid),
                    "effectiveHealAndShielding" to (420.5 + pid * 35.0),
                    "bountyGold" to (450.0 + pid * 50.0),
                    "controlWardsPlaced" to (3 + pid % 3),
                    "stealthWardsPlaced" to (9 + pid % 4),
                    "wardTakedowns" to (5 + pid % 3),
                    "wardTakedownsBefore20M" to (2 + pid % 2),
                    "controlWardTimeCoverageInRiverOrEnemyHalf" to 0.44,
                    "dragonTakedowns" to 2,
                    "riftHeraldTakedowns" to 1,
                    "baronTakedowns" to 1,
                    "voidMonsterKill" to 3,
                    "scuttleCrabKills" to (if (pid in setOf(2, 7)) 3 else 1),
                    "enemyJungleMonsterKills" to (4 + pid),
                    "alliedJungleMonsterKills" to (6 + pid),
                    "takedownsFirstXMinutes" to (3 + pid % 4),
                    "killsNearEnemyTurret" to (1 + pid % 2),
                    "killsUnderOwnTurret" to (1 + pid % 2),
                    "multikills" to (pid % 2),
                    "quickSoloKills" to (pid % 2),
                    "earlyLaningPhaseGoldExpAdvantage" to 1,
                    "laningPhaseGoldExpAdvantage" to 1
                ),
                "perks" to mapOf(
                    "statPerks" to mapOf("defense" to 5001, "flex" to 5008, "offense" to 5005),
                    "styles" to listOf(
                        mapOf(
                            "description" to "primaryStyle",
                            "style" to 8000,
                            "selections" to listOf(
                                mapOf("perk" to 8005, "var1" to 1420 + gameIndex, "var2" to 980, "var3" to 0),
                                mapOf("perk" to 9111, "var1" to 612 + pid, "var2" to 240, "var3" to 0),
                                mapOf("perk" to 9104, "var1" to 13, "var2" to 45, "var3" to 0),
                                mapOf("perk" to 8299, "var1" to 831 + pid, "var2" to 0, "var3" to 0)
                            )
                        ),
                        mapOf(
                            "description" to "subStyle",
                            "style" to 8300,
                            "selections" to listOf(
                                mapOf("perk" to 8345, "var1" to 3, "var2" to 0, "var3" to 0),
                                mapOf("perk" to 8347, "var1" to 0, "var2" to 0, "var3" to 0)
                            )
                        )
                    )
                )
            )
            objectMapper.convertValue(raw, ParticipantDto::class.java)
        }
        return MatchDto(
            metadata = MatchMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
            info = MatchInfo(
                gameId = 7_710_000_000L + gameIndex,
                gameCreation = 1_762_000_000_000L + gameIndex * 3_600_000L,
                gameDuration = 1_860L + (gameIndex % 7) * 45L,
                gameMode = "CLASSIC",
                gameVersion = "16.9.642.1928",
                queueId = 420,
                participants = participants,
                teams = listOf(
                    TeamDto(
                        teamId = 100,
                        win = gameIndex % 2 == 0,
                        objectives = TeamObjectivesDto(
                            tower = ObjectiveStatsDto(true, 9),
                            dragon = ObjectiveStatsDto(true, 3),
                            baron = ObjectiveStatsDto(true, 1),
                            riftHerald = ObjectiveStatsDto(true, 1),
                            horde = ObjectiveStatsDto(true, 4)
                        )
                    ),
                    TeamDto(
                        teamId = 200,
                        win = gameIndex % 2 != 0,
                        objectives = TeamObjectivesDto(
                            tower = ObjectiveStatsDto(false, 4),
                            dragon = ObjectiveStatsDto(false, 1),
                            baron = ObjectiveStatsDto(false, 0),
                            riftHerald = ObjectiveStatsDto(false, 0),
                            horde = ObjectiveStatsDto(false, 2)
                        )
                    )
                )
            )
        )
    }

    private fun buildRepresentative108MbProfile(matchIds20: List<String>, targetJsonBytes: Int = 1_076_586): SummonerProfileDto {
        val allMatchIds = (1..883).map { idx ->
            if (idx <= 20) matchIds20[idx - 1] else "EUW1_${7_709_999_999L - idx}"
        }
        val champStats = (1..75).map { idx ->
            val games = (900 / (idx + 1)).coerceAtLeast(1)
            val wins = (games * 53) / 100
            ChampionStatsDto(
                championName = if (idx == 1) "Pantheon" else "ChampPoolEntry_$idx",
                games = games,
                wins = wins,
                losses = games - wins,
                kills = games * 6,
                deaths = games * 5,
                assists = games * 7,
                soloKills = games * 2,
                winRate = 53.2,
                kda = 2.61,
                averageKills = 6.1,
                averageDeaths = 4.9,
                averageAssists = 6.8,
                averageCsPerMinute = 7.2,
                averageSoloKills = 1.9,
                averageTurretPlates = 2.4
            )
        }
        var pad = 0
        var profile = SummonerProfileDto(
            puuid = targetPuuid,
            gameName = "Spear Shot",
            tagLine = "1111",
            summonerLevel = 984L,
            profileIconId = 5891,
            soloQueueRank = LeagueEntryDto(
                puuid = targetPuuid,
                leaguePoints = 1142,
                rank = "I",
                wins = 472,
                losses = 411,
                veteran = true,
                inactive = false,
                freshBlood = false,
                hotStreak = false,
                leagueId = "challenger-euw1-league-id-0001",
                queueType = "RANKED_SOLO_5x5",
                tier = "CHALLENGER",
                summonerName = "Spear Shot",
                gameName = "Spear Shot",
                tagLine = "1111"
            ),
            recentMatches = matchIds20.mapIndexed { idx, id -> buildRichMatchDto(id, idx, 0) },
            allMatchIds = allMatchIds,
            championStats = champStats,
            totalMatches = 883,
            lastRefreshed = Instant.parse("2026-11-12T21:30:00Z")
        )
        val initialBytes = objectMapper.writeValueAsBytes(profile).size
        if (initialBytes < targetJsonBytes) {
            val deltaPerParticipant = ((targetJsonBytes - initialBytes) / 200).coerceAtLeast(0)
            pad = deltaPerParticipant
            profile = profile.copy(
                recentMatches = matchIds20.mapIndexed { idx, id -> buildRichMatchDto(id, idx, pad) }
            )
        }
        return profile
    }

    private fun buildRealisticTimelineBytes(matchId: String, seed: Int): ByteArray {
        val participants = (1..10).map { pid ->
            TimelineParticipantDto(
                participantId = pid,
                puuid = when (pid) {
                    1 -> targetPuuid
                    6 -> oppPuuid
                    else -> "puuid-tl-pid-$pid-seed-$seed-0123456789abcdef0123456789abcdef"
                }
            )
        }
        // ~34-minute Challenger EUW match (~1.35 MB uncompressed JSON, ~45 KB gzipped)
        val frames = (0..34).map { minute ->
            val pFrames = (1..10).associate { pid ->
                pid.toString() to ParticipantFrameDto(
                    participantId = pid,
                    totalGold = 500 + minute * (405 + pid * 7 + seed),
                    level = (1 + minute / 2).coerceAtMost(18),
                    xp = minute * (465 + pid * 9),
                    currentGold = 320 + (minute * 73 + pid * 41) % 1400,
                    minionsKilled = minute * 7 + pid % 4,
                    jungleMinionsKilled = if (pid in setOf(2, 7)) minute * 5 else minute / 3,
                    position = TimelinePositionDto(x = 1800 + minute * 260 + pid * 37, y = 12200 - minute * 215 - pid * 19)
                )
            }
            val events = ArrayList<TimelineEventDto>(110)
            for (pid in 1..10) {
                for (sub in 0..3) {
                    events.add(
                        TimelineEventDto(
                            type = if (sub % 2 == 0) "ITEM_PURCHASED" else "SKILL_LEVEL_UP",
                            timestamp = minute * 60_000L + pid * 1_100L + sub * 230L,
                            participantId = pid,
                            killerId = null, victimId = null, assistingParticipantIds = null,
                            itemId = if (sub % 2 == 0) 1001 + (minute * 13 + pid + sub) % 2200 else null,
                            afterId = null, beforeId = null,
                            skillSlot = if (sub % 2 == 1) 1 + (minute + pid) % 4 else null,
                            levelUpType = if (sub % 2 == 1) "NORMAL" else null,
                            wardType = null, creatorId = null, level = null, buildingType = null,
                            laneType = null, towerType = null, teamId = null, monsterType = null,
                            monsterSubType = null, position = null, bounty = null, shutdownBounty = null, goldGain = null
                        )
                    )
                }
                events.add(
                    TimelineEventDto(
                        type = "WARD_PLACED",
                        timestamp = minute * 60_000L + pid * 1_700L,
                        participantId = null,
                        killerId = null, victimId = null, assistingParticipantIds = null,
                        itemId = null, afterId = null, beforeId = null, skillSlot = null, levelUpType = null,
                        wardType = "YELLOW_TRINKET", creatorId = pid, level = null, buildingType = null,
                        laneType = null, towerType = null, teamId = null, monsterType = null,
                        monsterSubType = null, position = TimelinePositionDto(4100 + pid * 90, 9200 - minute * 80),
                        bounty = null, shutdownBounty = null, goldGain = null
                    )
                )
            }
            if (minute in listOf(3, 5, 8, 11, 14, 17, 20, 24, 28, 31)) {
                events.add(
                    TimelineEventDto(
                        type = "CHAMPION_KILL",
                        timestamp = minute * 60_000L + 24_000L,
                        participantId = null,
                        killerId = if (minute % 2 == 0) 1 else 6,
                        victimId = if (minute % 2 == 0) 6 else 1,
                        assistingParticipantIds = listOf(2, 3),
                        itemId = null, afterId = null, beforeId = null, skillSlot = null, levelUpType = null,
                        wardType = null, creatorId = null, level = null, buildingType = null,
                        laneType = null, towerType = null, teamId = null, monsterType = null,
                        monsterSubType = null, position = TimelinePositionDto(3400 + minute * 120, 10800 - minute * 110),
                        bounty = 300, shutdownBounty = 150, goldGain = null
                    )
                )
            }
            TimelineFrameDto(timestamp = minute * 60_000L, participantFrames = pFrames, events = events)
        }
        val dto = MatchTimelineDto(
            metadata = MatchTimelineMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
            info = MatchTimelineInfo(frameInterval = 60_000L, frames = frames, participants = participants)
        )
        return objectMapper.writeValueAsBytes(dto)
    }

    @Test
    fun measureRepresentative108MbProfileAndTwoTimeoutsReproduction() = runBlocking {
        val redisMap = ConcurrentHashMap<String, String>()
        val valueOps = mockk<ValueOperations<String, String>>(relaxed = true)
        every { valueOps.set(any(), any(), any<Duration>()) } answers { redisMap[firstArg()] = secondArg() }
        every { valueOps.get(any()) } answers { redisMap[firstArg()] }
        val stringRedisTemplate = mockk<StringRedisTemplate>(relaxed = true)
        every { stringRedisTemplate.opsForValue() } returns valueOps
        every { stringRedisTemplate.delete(any<String>()) } answers { redisMap.remove(firstArg()) != null }

        val redisCacheService = RedisCacheService(stringRedisTemplate, objectMapper)
        val realSystemMemory = SystemMemoryStatsProvider()
        val sampleCalls = AtomicInteger(0)
        val sampleNanos = AtomicLong(0L)
        val countingMemory = object : MemoryStatsProvider {
            override fun sample(): RawMemorySample {
                val t0 = System.nanoTime()
                val real = realSystemMemory.sample()
                sampleNanos.addAndGet(System.nanoTime() - t0)
                sampleCalls.incrementAndGet()
                return real.copy(
                    jvmHeapUsedBytes = 92L * 1024L * 1024L,
                    jvmHeapCommittedBytes = 205L * 1024L * 1024L,
                    jvmHeapMaxBytes = 205L * 1024L * 1024L,
                    cgroupMemoryCurrentBytes = 344_985_600L,
                    cgroupMemoryMaxBytes = 536_870_912L
                )
            }
        }

        val matchIds20 = (1..20).map { "EUW1_77100000${it.toString().padStart(2, '0')}" }
        val profile108Mb = buildRepresentative108MbProfile(matchIds20, 1_076_586)
        val profileJsonBytes = objectMapper.writeValueAsBytes(profile108Mb).size
        val profileKey = "summoner:profile:v11:euw1:$targetPuuid"
        redisCacheService.set(profileKey, profile108Mb, Duration.ofHours(1))
        val storedProfileB64Len = redisMap[profileKey]!!.length

        // Also build a ~4.5 MB JSON profile whose GZIP_B64 string itself is ~1,080,000 chars
        val matchIdsHeavy = (1..20).map { "EUW1_77200000${it.toString().padStart(2, '0')}" }
        val heavyProfile = buildRepresentative108MbProfile(matchIdsHeavy, 4_600_000)
        val heavyJsonBytes = objectMapper.writeValueAsBytes(heavyProfile).size
        val heavyProfileKey = "summoner:profile:v11:euw1:heavy-$targetPuuid"
        redisCacheService.set(heavyProfileKey, heavyProfile, Duration.ofHours(1))
        val heavyStoredB64Len = redisMap[heavyProfileKey]!!.length

        val rawTimelines = HashMap<String, ByteArray>(40)
        val gzipTimelines = HashMap<String, ByteArray>(40)
        for ((idx, id) in (matchIds20 + matchIdsHeavy).withIndex()) {
            val raw = buildRealisticTimelineBytes(id, idx + 1)
            rawTimelines[id] = raw
            gzipTimelines[id] = ByteArrayOutputStream().use { bos ->
                GZIPOutputStream(bos).use { gz -> gz.write(raw) }
                bos.toByteArray()
            }
        }
        val sampleRawTlSize = rawTimelines[matchIds20.first()]!!.size
        val sampleGzipTlSize = gzipTimelines[matchIds20.first()]!!.size

        println(
            "REPRO_FIXTURE_SIZES profileJsonBytes=$profileJsonBytes storedGzipB64Chars=$storedProfileB64Len " +
                "heavyJsonBytes=$heavyJsonBytes heavyStoredGzipB64Chars=$heavyStoredB64Len " +
                "rawTimelineBytes=$sampleRawTlSize gzipTimelineBytes=$sampleGzipTlSize"
        )

        val acceptEncodingSeen = CopyOnWriteArrayList<String>()
        val simulatedUncompressedLatencyMs = AtomicLong(0L)
        val simulatedGzipLatencyMs = AtomicLong(0L)
        val stallMatchId = java.util.concurrent.atomic.AtomicReference<String?>(null)
        val uninterruptibleStallMs = AtomicLong(0L)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool { r -> Thread(r, "repro-euw-riot").apply { isDaemon = true } }
        server.createContext("/lol/match/v5/matches/") { exchange ->
            try {
                val matchId = exchange.requestURI.path.substringAfter("/matches/").substringBefore("/timeline")
                val acceptEnc = exchange.requestHeaders.getFirst("Accept-Encoding") ?: "<none>"
                if (acceptEncodingSeen.size < 5) acceptEncodingSeen.add(acceptEnc)
                val clientSupportsGzip = acceptEnc.contains("gzip", ignoreCase = true)

                val payload = if (clientSupportsGzip) gzipTimelines[matchId] else rawTimelines[matchId]
                if (payload == null) {
                    exchange.sendResponseHeaders(404, -1L)
                } else {
                    if (stallMatchId.get() == matchId && uninterruptibleStallMs.get() > 0L) {
                        Thread.sleep(uninterruptibleStallMs.get())
                    }
                    val delayMs = if (clientSupportsGzip) simulatedGzipLatencyMs.get() else simulatedUncompressedLatencyMs.get()
                    if (delayMs > 0L) {
                        Thread.sleep(delayMs)
                    }
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.responseHeaders.add("X-App-Rate-Limit", "100:1,500:120")
                    exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
                    exchange.responseHeaders.add("X-Method-Rate-Limit", "2000:10")
                    exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "1:10")
                    if (clientSupportsGzip) {
                        exchange.responseHeaders.add("Content-Encoding", "gzip")
                    }
                    exchange.sendResponseHeaders(200, payload.size.toLong())
                    exchange.responseBody.use { out ->
                        var offset = 0
                        while (offset < payload.size) {
                            val chunk = minOf(1_448, payload.size - offset)
                            out.write(payload, offset, chunk)
                            offset += chunk
                        }
                    }
                }
            } catch (_: Exception) {
                // Client aborted mid-stream on deadline
            } finally {
                runCatching { exchange.close() }
            }
        }
        server.start()
        try {
            val port = server.address.port
            val rateLimiter = RiotApiRateLimiter(SystemTimeProvider())
            val riotRestClient = RestClientConfig.buildRiotRestClient("test-key")
            val riotApiService = RiotApiService(
                riotRestClient = riotRestClient,
                rateLimiter = rateLimiter
            ).apply {
                matchBaseUrlResolver = { "http://127.0.0.1:$port" }
            }
            val summaries = CopyOnWriteArrayList<String>()
            val timelineService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingMemory
            ).apply {
                stageSummaryObserver = { msg -> summaries.add(msg) }
            }

            // Guard rail invariant check: zero relaxation of memory or deadline defaults
            val preCheck = timelineService.evaluatePreAdmissionHeadroom()
            assertTrue(preCheck.admitted, "344,985,600B < 350 MiB must still be admitted")
            val overAdmissionMemory = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = RawMemorySample(
                    jvmHeapUsedBytes = 92L * 1024L * 1024L,
                    jvmHeapCommittedBytes = 205L * 1024L * 1024L,
                    jvmHeapMaxBytes = 205L * 1024L * 1024L,
                    cgroupMemoryCurrentBytes = 351L * 1024L * 1024L, // > 350 MiB admission cap
                    cgroupMemoryMaxBytes = 512L * 1024L * 1024L
                )
            }
            val guardCheckService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = overAdmissionMemory
            )
            assertTrue(!guardCheckService.evaluatePreAdmissionHeadroom().admitted, "350 MiB admission guard must remain intact")

            val peakRealJvmHeapBytes = AtomicLong(0L)
            fun trackRealHeapPeak(): Long {
                val cur = realSystemMemory.sample().jvmHeapUsedBytes
                peakRealJvmHeapBytes.accumulateAndGet(cur, ::maxOf)
                return cur
            }

            // Scenario 1: Local 0ms RTT with 1,076,551-byte JSON profile + 20 content-negotiated timelines
            sampleCalls.set(0); sampleNanos.set(0L); summaries.clear(); peakRealJvmHeapBytes.set(0L)
            trackRealHeapPeak()
            val t0Local = System.currentTimeMillis()
            val outLocal = timelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = {
                    trackRealHeapPeak()
                    redisCacheService.get(profileKey, SummonerProfileDto::class.java).also { trackRealHeapPeak() }
                }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val elapsedLocal = System.currentTimeMillis() - t0Local
            trackRealHeapPeak()
            val localSamples = sampleCalls.get()
            println(
                "FIXED_SCENARIO1_LOCAL_108MB_JSON elapsedMs=$elapsedLocal status=${outLocal.result.status} " +
                    "covered=${outLocal.result.coveredMatchIds.size}/${outLocal.result.requestedMatchIds.size} " +
                    "acceptEncoding=${acceptEncodingSeen.firstOrNull()} sampleCalls=$localSamples " +
                    "sampleCpuMs=${sampleNanos.get() / 1_000_000} realHeapPeakBytes=${peakRealJvmHeapBytes.get()} " +
                    "summary=${summaries.lastOrNull()}"
            )
            assertTrue(acceptEncodingSeen.firstOrNull()?.contains("gzip", ignoreCase = true) == true, "Must negotiate gzip on bounded timeline fetch")
            assertTrue(outLocal.result.status == "COMPLETE", "Expected COMPLETE on 1.08MB profile, got ${outLocal.result.status}")
            assertTrue(outLocal.result.coveredMatchIds.size == 20, "Expected 20/20 covered")
            assertTrue(outLocal.result.analysis != null && outLocal.result.analysis?.powerSpikeTimelines?.size == 20)
            assertTrue(outLocal.result.peakRetainedRawTimelines == 1, "Must retain at most 1 raw timeline in flight")
            assertTrue(localSamples < 3_500, "Expected bounded memory sample calls (<3,500 vs 18,203 baseline), got $localSamples")

            // Scenario 1B: Local 0ms RTT with heavy (~4.60MB JSON / 128,085-char GZIP_B64) profile + 20 content-negotiated timelines
            val heavyTimelineService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = redisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingMemory
            ).apply {
                stageSummaryObserver = { msg -> summaries.add(msg) }
            }
            sampleCalls.set(0); sampleNanos.set(0L); summaries.clear()
            val t0Heavy = System.currentTimeMillis()
            val outHeavy = heavyTimelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { redisCacheService.get(heavyProfileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val elapsedHeavy = System.currentTimeMillis() - t0Heavy
            println(
                "FIXED_SCENARIO1B_LOCAL_HEAVY_PROFILE elapsedMs=$elapsedHeavy status=${outHeavy.result.status} " +
                    "covered=${outHeavy.result.coveredMatchIds.size}/${outHeavy.result.requestedMatchIds.size} " +
                    "sampleCalls=${sampleCalls.get()} sampleCpuMs=${sampleNanos.get() / 1_000_000} summary=${summaries.lastOrNull()}"
            )
            assertTrue(outHeavy.result.status == "COMPLETE" && outHeavy.result.coveredMatchIds.size == 20)

            // Scenario 2: Cold Transatlantic EUW latency (~850ms uncompressed vs ~110ms gzipped) — Open #1 + Retry #2
            val coldRedisMap = ConcurrentHashMap<String, String>()
            coldRedisMap[profileKey] = redisMap[profileKey]!!
            val coldValueOps = mockk<ValueOperations<String, String>>(relaxed = true)
            every { coldValueOps.set(any(), any(), any<Duration>()) } answers { coldRedisMap[firstArg()] = secondArg() }
            every { coldValueOps.get(any()) } answers { coldRedisMap[firstArg()] }
            val coldRedisTemplate = mockk<StringRedisTemplate>(relaxed = true)
            every { coldRedisTemplate.opsForValue() } returns coldValueOps
            val coldRedisCacheService = RedisCacheService(coldRedisTemplate, objectMapper)
            val coldTimelineService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = coldRedisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingMemory
            ).apply {
                stageSummaryObserver = { msg -> summaries.add(msg) }
            }

            simulatedUncompressedLatencyMs.set(850L)
            simulatedGzipLatencyMs.set(110L)
            sampleCalls.set(0); sampleNanos.set(0L); summaries.clear(); peakRealJvmHeapBytes.set(0L)
            trackRealHeapPeak()

            val tOpen1 = System.currentTimeMillis()
            val open1 = coldTimelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = {
                    trackRealHeapPeak()
                    coldRedisCacheService.get(profileKey, SummonerProfileDto::class.java).also { trackRealHeapPeak() }
                }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val elapsedOpen1 = System.currentTimeMillis() - tOpen1
            trackRealHeapPeak()
            val cachedTimelinesAfterOpen1 = coldRedisMap.keys.count { it != profileKey && it.contains("EUW1_77100000") }
            println(
                "FIXED_SCENARIO2_EUW_OPEN1 elapsedMs=$elapsedOpen1 status=${open1.result.status} stopReason=${open1.result.stopReason} " +
                    "covered=${open1.result.coveredMatchIds.size}/${open1.result.requestedMatchIds.size} " +
                    "analysisNonNull=${open1.result.analysis != null} cachedAfterOpen1=$cachedTimelinesAfterOpen1 " +
                    "sampleCalls=${sampleCalls.get()} sampleCpuMs=${sampleNanos.get() / 1_000_000} " +
                    "realHeapPeakBytes=${peakRealJvmHeapBytes.get()} summary=${summaries.lastOrNull()}"
            )
            assertTrue(open1.result.status == "COMPLETE", "Expected COMPLETE on EUW Open #1 with gzip, got ${open1.result.status}")
            assertTrue(open1.result.coveredMatchIds.size == 20 && cachedTimelinesAfterOpen1 == 20)
            assertTrue(elapsedOpen1 < 10_000L, "Expected well under 12,000ms budget, took ${elapsedOpen1}ms")

            summaries.clear(); sampleCalls.set(0); sampleNanos.set(0L)
            val tRetry2 = System.currentTimeMillis()
            val retry2 = coldTimelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { coldRedisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val elapsedRetry2 = System.currentTimeMillis() - tRetry2
            println(
                "FIXED_SCENARIO2_EUW_RETRY2 elapsedMs=$elapsedRetry2 status=${retry2.result.status} cacheHit=${retry2.result.cacheHit} " +
                    "covered=${retry2.result.coveredMatchIds.size}/${retry2.result.requestedMatchIds.size} " +
                    "sampleCalls=${sampleCalls.get()} sampleCpuMs=${sampleNanos.get() / 1_000_000} summary=${summaries.lastOrNull()}"
            )
            assertTrue(retry2.result.status == "COMPLETE" && retry2.result.cacheHit)

            // Scenario 3A: Bounded worker completion reserve under slow per-match network (420ms/match within 2,500ms budget)
            // Must stop I/O cleanly BEFORE caller isolation cutoff and return PARTIAL (>0/20) with analysis, then progress on retry!
            val slowRedisMap = ConcurrentHashMap<String, String>()
            slowRedisMap[profileKey] = redisMap[profileKey]!!
            val slowValueOps = mockk<ValueOperations<String, String>>(relaxed = true)
            every { slowValueOps.set(any(), any(), any<Duration>()) } answers { slowRedisMap[firstArg()] = secondArg() }
            every { slowValueOps.get(any()) } answers { slowRedisMap[firstArg()] }
            val slowRedisTemplate = mockk<StringRedisTemplate>(relaxed = true)
            every { slowRedisTemplate.opsForValue() } returns slowValueOps
            val slowRedisCacheService = RedisCacheService(slowRedisTemplate, objectMapper)
            val reserveTimelineService = TimelineService(
                riotApiService = riotApiService,
                redisCacheService = slowRedisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingMemory,
                prepDeadlineMs = 2_500L
            ).apply {
                stageSummaryObserver = { msg -> summaries.add(msg) }
            }
            simulatedGzipLatencyMs.set(420L)
            summaries.clear()
            val partialOpen = reserveTimelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { slowRedisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val partialLog = summaries.last()
            println("FIXED_SCENARIO3A_RESERVE_OPEN1 status=${partialOpen.result.status} covered=${partialOpen.result.coveredMatchIds.size}/20 summary=$partialLog")
            assertTrue(partialOpen.result.status == "PARTIAL", "Expected PARTIAL instead of self-cancelled BLOCKED 0/20, got ${partialOpen.result.status}")
            assertTrue(partialOpen.result.coveredMatchIds.isNotEmpty() && partialOpen.result.analysis != null)
            assertTrue(partialLog.contains("stage=timeline_loop"), "Worker must finish inside completion reserve without caller self-cancel: $partialLog")

            summaries.clear()
            val partialRetry2 = reserveTimelineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { slowRedisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val partialRetry2Log = summaries.last()
            println("FIXED_SCENARIO3A_RESERVE_RETRY2 status=${partialRetry2.result.status} covered=${partialRetry2.result.coveredMatchIds.size}/20 summary=$partialRetry2Log")
            assertTrue(partialRetry2.result.coveredMatchIds.size > partialOpen.result.coveredMatchIds.size, "Retry #2 must advance coverage using Redis-cached timelines")

            // Scenario 3B: Uninterruptible worker stall exceeding callerDeadlineMs + 75ms -> singleflight_caller_deadline
            // (On unmodified 9598428/aae2a392 this exact path returned BLOCKED 0/20 with 0/0/0/0 counters.)
            // Must salvage PARTIAL 4/20 from PartialCompactionSnapshot, log live subStage + non-zero counters, and complete on Retry #2!
            val stallRedisMap = ConcurrentHashMap<String, String>()
            stallRedisMap[profileKey] = redisMap[profileKey]!!
            val stallValueOps = mockk<ValueOperations<String, String>>(relaxed = true)
            every { stallValueOps.set(any(), any(), any<Duration>()) } answers { stallRedisMap[firstArg()] = secondArg() }
            every { stallValueOps.get(any()) } answers { stallRedisMap[firstArg()] }
            val stallRedisTemplate = mockk<StringRedisTemplate>(relaxed = true)
            every { stallRedisTemplate.opsForValue() } returns stallValueOps
            val stallRedisCacheService = RedisCacheService(stallRedisTemplate, objectMapper)

            val stallingRiotApi = mockk<RiotApiService>()
            val match5Id = matchIds20[4]
            val shouldStallMatch5 = java.util.concurrent.atomic.AtomicBoolean(true)
            coEvery {
                stallingRiotApi.getMatchTimelineBounded(
                    matchId = any(),
                    region = any(),
                    priority = any(),
                    maxRetries = any(),
                    httpAttemptCounter = any(),
                    failureReasonHolder = any(),
                    maxTotalDurationMs = any(),
                    headroomGuard = any()
                )
            } answers {
                val mid = firstArg<String>()
                val counter = arg<AtomicInteger?>(4)
                counter?.incrementAndGet()
                if (mid == match5Id && shouldStallMatch5.get()) {
                    val deadlineEnd = System.currentTimeMillis() + 900L
                    while (System.currentTimeMillis() < deadlineEnd) {
                        try {
                            Thread.sleep(25L)
                        } catch (_: InterruptedException) {
                            // Ignore interrupt to force singleflight_caller_deadline path
                        }
                    }
                    null
                } else {
                    objectMapper.readValue(rawTimelines[mid]!!, MatchTimelineDto::class.java)
                }
            }

            val callerDeadlineService = TimelineService(
                riotApiService = stallingRiotApi,
                redisCacheService = stallRedisCacheService,
                timelineAnalysisService = TimelineAnalysisService(),
                rateLimiter = rateLimiter,
                timeProvider = SystemTimeProvider(),
                memoryStatsProvider = countingMemory,
                prepDeadlineMs = 600L
            ).apply {
                stageSummaryObserver = { msg -> summaries.add(msg) }
            }
            summaries.clear()
            val callerDeadlineOut = callerDeadlineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { stallRedisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val callerDeadlineLog = summaries.first { it.contains("stage=singleflight_caller_deadline") }
            println("FIXED_SCENARIO3B_CALLER_DEADLINE_OPEN1 status=${callerDeadlineOut.result.status} covered=${callerDeadlineOut.result.coveredMatchIds.size}/20 summary=$callerDeadlineLog")
            assertTrue(callerDeadlineOut.result.status == "PARTIAL", "Expected salvaged PARTIAL 4/20 on caller deadline, got ${callerDeadlineOut.result.status}")
            assertTrue(callerDeadlineOut.result.coveredMatchIds.size == 4, "Expected 4 covered matches, got ${callerDeadlineOut.result.coveredMatchIds.size}")
            assertTrue(callerDeadlineOut.result.stopReason == "DEADLINE_EXCEEDED")
            assertTrue(callerDeadlineOut.result.analysis != null && callerDeadlineOut.result.analysis?.powerSpikeTimelines?.size == 4)
            assertTrue(callerDeadlineLog.contains("subStage=riot_http_fetch"), "Must report active subStage: $callerDeadlineLog")
            assertTrue(callerDeadlineLog.contains("httpAttempts=5"), "Must report live httpAttempts instead of hardcoded 0: $callerDeadlineLog")
            assertTrue(!callerDeadlineLog.contains("riotLoopMs=0 "), "Must report live non-zero riotLoopMs: $callerDeadlineLog")

            // Wait briefly for detached worker stall to exit, clear stall, and verify Retry #2 reuses the 4 Redis-cached timelines -> COMPLETE 20/20
            Thread.sleep(450L)
            shouldStallMatch5.set(false)
            summaries.clear()
            val callerDeadlineRetry2 = callerDeadlineService.prepareCoachTimelinesForPlayer(
                region = "euw1",
                puuid = targetPuuid,
                requestedMatchIds = null,
                includeEmptyFallbackAnalysisForLegacyRoute = false,
                profileLoader = { stallRedisCacheService.get(profileKey, SummonerProfileDto::class.java) }
            ) as TimelineService.PlayerPreparationOutcome.Ready
            val callerDeadlineRetry2Log = summaries.last()
            println("FIXED_SCENARIO3B_CALLER_DEADLINE_RETRY2 status=${callerDeadlineRetry2.result.status} covered=${callerDeadlineRetry2.result.coveredMatchIds.size}/20 summary=$callerDeadlineRetry2Log")
            assertTrue(callerDeadlineRetry2.result.status == "COMPLETE" && callerDeadlineRetry2.result.coveredMatchIds.size == 20)
            assertTrue(callerDeadlineRetry2Log.contains("httpAttempts=16"), "Retry #2 must fetch only remaining 16 timelines: $callerDeadlineRetry2Log")
        } finally {
            server.stop(0)
        }
    }
}
