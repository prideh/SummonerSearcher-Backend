package com.pride.summoner_searcher_api.integration

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.config.SecurityConfig
import com.pride.summoner_searcher_api.controller.AiController
import com.pride.summoner_searcher_api.controller.TimelineController
import com.pride.summoner_searcher_api.dto.*
import com.pride.summoner_searcher_api.filter.JwtAuthenticationFilter
import com.pride.summoner_searcher_api.handler.CustomAccessDeniedHandler
import com.pride.summoner_searcher_api.handler.CustomAuthenticationEntryPoint
import com.pride.summoner_searcher_api.handler.GlobalExceptionHandler
import com.pride.summoner_searcher_api.model.AiInteraction
import com.pride.summoner_searcher_api.model.PromptVersion
import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.CurationRunRepository
import com.pride.summoner_searcher_api.repository.DiscoveredPatternRepository
import com.pride.summoner_searcher_api.repository.PromptVersionRepository
import com.pride.summoner_searcher_api.service.*
import com.pride.summoner_searcher_api.util.JwtUtil
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper
import reactor.core.publisher.Mono
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Bounded offline real-HTTP contract & cross-stack integration server test for BE55 (`6e817d7`).
 *
 * Boots an actual embedded Tomcat + Spring MVC + Jackson + `SecurityConfig` (`CORS` + `JwtAuthenticationFilter`)
 * slice with real `TimelineController`, `TimelineService`, `TimelineAnalysisService`, `RedisCacheService`
 * (streaming `GZIP_B64:` compression/inflation over an in-memory map), and `AiController` + real
 * `AiAnalysisService.constructPrompt(...)` — with ZERO external Riot API, Gemini AI, Redis, or PostgreSQL network calls.
 *
 * Supports two execution modes:
 *  1. Standalone JUnit verification (`./gradlew test --tests ...CoachTimelineHttpContractServerTest --offline`)
 *  2. Cross-stack HTTP bridge mode when `COACH_HTTP_HOLD_PORT_FILE` is set (`scripts/run-coach-http-integration.sh`),
 *     exposing the live local Tomcat server to FE18 (`51c4edf`) Vitest + Axios over real HTTP.
 */
@SpringBootTest(
    classes = [CoachTimelineHttpContractServerTest.CoachTimelineHttpTestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "server.address=127.0.0.1",
        "spring.main.banner-mode=off",
        "cors.allowed-origins=http://localhost:3000,http://localhost:5173,http://127.0.0.1:3000"
    ]
)
class CoachTimelineHttpContractServerTest {

    class ControllableTimeProvider(initialMs: Long = 1_700_000_000_000L) : TimeProvider {
        private val current = AtomicLong(initialMs)
        override fun now(): Long = current.get()
        fun advanceBy(deltaMs: Long) {
            current.addAndGet(deltaMs)
        }
        fun reset(ms: Long = 1_700_000_000_000L) {
            current.set(ms)
        }
    }

    class HealthyMemoryStatsProvider(
        @Volatile var cgroupUsedBytes: Long? = 300L * 1024L * 1024L,
        @Volatile var cgroupMaxBytes: Long? = 512L * 1024L * 1024L,
        @Volatile var heapUsedBytes: Long = 110L * 1024L * 1024L,
        @Volatile var heapMaxBytes: Long = 256L * 1024L * 1024L
    ) : MemoryStatsProvider {
        override fun sample(): RawMemorySample = RawMemorySample(
            jvmHeapUsedBytes = heapUsedBytes,
            jvmHeapCommittedBytes = heapMaxBytes,
            jvmHeapMaxBytes = heapMaxBytes,
            cgroupMemoryCurrentBytes = cgroupUsedBytes,
            cgroupMemoryMaxBytes = cgroupMaxBytes
        )
    }

    data class RecordedPrepareHttpCall(
        val method: String,
        val uri: String,
        val region: String,
        val puuid: String,
        val requestedMatchIds: List<String>,
        val status: Int,
        val origin: String?
    )

    data class RecordedRiotTimelineFetch(
        val matchId: String,
        val region: String
    )

    data class RecordedChatCapture(
        val userMessage: String,
        val sessionId: String?,
        val summonerName: String?,
        val timelineStatus: String?,
        val timelineCoverage: Any?,
        val gamesAnalyzed: Any?,
        val hasTimelineAnalysis: Any?,
        val context: Map<String, Any>,
        val constructedPrompt: String
    )

    class TestHarnessState {
        val mapper: ObjectMapper = ObjectMapper()
            .findAndRegisterModules()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        val timeProvider = ControllableTimeProvider()
        val memoryProvider = HealthyMemoryStatsProvider()
        val redisStore = ConcurrentHashMap<String, String>()
        val prepareHttpCalls = CopyOnWriteArrayList<RecordedPrepareHttpCall>()
        val riotTimelineFetches = CopyOnWriteArrayList<RecordedRiotTimelineFetch>()
        val chatCaptures = CopyOnWriteArrayList<RecordedChatCapture>()
        val refreshProfileAdvanced = AtomicBoolean(false)

        fun reset(timelineService: TimelineService) {
            timeProvider.reset()
            redisStore.clear()
            prepareHttpCalls.clear()
            riotTimelineFetches.clear()
            chatCaptures.clear()
            refreshProfileAdvanced.set(false)
            clearTimelineWarmCache(timelineService)
        }

        private fun clearTimelineWarmCache(timelineService: TimelineService) {
            val lockField = TimelineService::class.java.getDeclaredField("preparedCacheLock").apply { isAccessible = true }
            val cacheField = TimelineService::class.java.getDeclaredField("preparedCache").apply { isAccessible = true }
            val bytesField = TimelineService::class.java.getDeclaredField("preparedCacheTotalBytes").apply { isAccessible = true }
            val lock = lockField.get(timelineService)
            synchronized(lock) {
                @Suppress("UNCHECKED_CAST")
                val map = cacheField.get(timelineService) as MutableMap<String, Any?>
                map.clear()
                bytesField.setInt(timelineService, 0)
            }
        }

        fun buildMatchDto(
            matchId: String,
            targetPuuid: String,
            win: Boolean = true,
            durationSec: Long = 1800L
        ): MatchDto {
            val oppPuuid = "puuid-opp-top"
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
                val raw = mapOf(
                    "puuid" to when {
                        isPlayer -> targetPuuid
                        isOpp -> oppPuuid
                        else -> "puuid-$pid"
                    },
                    "participantId" to pid,
                    "teamId" to teamId,
                    "teamPosition" to pos,
                    "individualPosition" to pos,
                    "championName" to when {
                        isPlayer -> "Pantheon"
                        isOpp -> "Aatrox"
                        else -> "Champ$pid"
                    },
                    "win" to if (teamId == 100) win else !win,
                    "kills" to if (isPlayer) 8 else 3,
                    "deaths" to if (isPlayer) 2 else 5,
                    "assists" to if (isPlayer) 7 else 4,
                    "totalMinionsKilled" to 190,
                    "neutralMinionsKilled" to 12,
                    "visionScore" to 28,
                    "goldEarned" to 13500,
                    "totalDamageDealtToChampions" to 26000,
                    "totalTimeSpentDead" to 42,
                    "turretKills" to 2,
                    "turretTakedowns" to 3,
                    "dragonKills" to 0,
                    "baronKills" to 0,
                    "challenges" to mapOf(
                        "turretPlatesTaken" to 6,
                        "dragonTakedowns" to 2,
                        "voidMonsterKill" to 3,
                        "laneMinionsFirst10Minutes" to 78
                    )
                )
                mapper.convertValue(raw, ParticipantDto::class.java)
            }
            val teams = listOf(
                TeamDto(
                    teamId = 100,
                    win = win,
                    objectives = TeamObjectivesDto(
                        tower = ObjectiveStatsDto(first = true, kills = 8),
                        dragon = ObjectiveStatsDto(first = true, kills = 3),
                        baron = ObjectiveStatsDto(first = false, kills = 1),
                        riftHerald = ObjectiveStatsDto(first = true, kills = 1),
                        horde = ObjectiveStatsDto(first = true, kills = 4)
                    )
                ),
                TeamDto(
                    teamId = 200,
                    win = !win,
                    objectives = TeamObjectivesDto(
                        tower = ObjectiveStatsDto(first = false, kills = 3),
                        dragon = ObjectiveStatsDto(first = false, kills = 1),
                        baron = ObjectiveStatsDto(first = false, kills = 0),
                        riftHerald = ObjectiveStatsDto(first = false, kills = 0),
                        horde = ObjectiveStatsDto(first = false, kills = 2)
                    )
                )
            )
            return MatchDto(
                metadata = MatchMetadata(matchId = matchId, participants = participants.mapNotNull { it.puuid }),
                info = MatchInfo(
                    gameId = 1000L + matchId.hashCode().toLong(),
                    gameCreation = 1_760_000_000_000L,
                    gameDuration = durationSec,
                    gameMode = "CLASSIC",
                    gameVersion = "16.1.1",
                    queueId = 420,
                    participants = participants,
                    teams = teams
                )
            )
        }

        fun buildRichRawTimeline(
            matchId: String,
            targetPuuid: String,
            seed: Int = 1
        ): MatchTimelineDto {
            val oppPuuid = "puuid-opp-top"
            val timelineParticipants = (1..10).map { pid ->
                TimelineParticipantDto(
                    participantId = pid,
                    puuid = when (pid) {
                        1 -> targetPuuid
                        6 -> oppPuuid
                        else -> "puuid-$pid"
                    }
                )
            }
            val frames = (0..25).map { minute ->
                val pFrames = (1..10).associate { pid ->
                    val goldPerMin = if (pid == 1) 430 + seed * 5 else 380 + pid * 3
                    val xpPerMin = if (pid == 1) 480 + seed * 4 else 435 + pid * 3
                    pid.toString() to ParticipantFrameDto(
                        participantId = pid,
                        totalGold = 500 + minute * goldPerMin,
                        level = (1 + minute / 2).coerceAtMost(18),
                        xp = minute * xpPerMin,
                        currentGold = if (minute == 11 && pid == 1) 1850 else 350 + (minute * 25) % 900,
                        minionsKilled = minute * 7 + (if (pid == 1) 4 + seed else 0),
                        jungleMinionsKilled = if (minute >= 12) (minute - 10) * 2 else 0,
                        position = TimelinePositionDto(x = 2000 + minute * 250 + pid * 30, y = 12000 - minute * 200)
                    )
                }
                val events = mutableListOf<TimelineEventDto>()
                if (minute in listOf(1, 3, 5, 7, 9, 12)) {
                    events.add(
                        TimelineEventDto(
                            type = "SKILL_LEVEL_UP",
                            timestamp = minute * 60_000L + 1_000L,
                            participantId = 1,
                            killerId = null,
                            victimId = null,
                            assistingParticipantIds = null,
                            itemId = null,
                            afterId = null,
                            beforeId = null,
                            skillSlot = 1,
                            levelUpType = "NORMAL",
                            wardType = null,
                            creatorId = null,
                            level = null,
                            buildingType = null,
                            laneType = null,
                            towerType = null,
                            teamId = null,
                            monsterType = null,
                            monsterSubType = null,
                            position = null,
                            bounty = null,
                            shutdownBounty = null,
                            goldGain = null
                        )
                    )
                }
                if (minute == 6) {
                    events.add(
                        TimelineEventDto(
                            type = "CHAMPION_KILL",
                            timestamp = 6 * 60_000L + 15_000L,
                            participantId = null,
                            killerId = 1,
                            victimId = 6,
                            assistingParticipantIds = emptyList(),
                            itemId = null,
                            afterId = null,
                            beforeId = null,
                            skillSlot = null,
                            levelUpType = null,
                            wardType = null,
                            creatorId = null,
                            level = null,
                            buildingType = null,
                            laneType = null,
                            towerType = null,
                            teamId = null,
                            monsterType = null,
                            monsterSubType = null,
                            position = TimelinePositionDto(x = 2900, y = 11800),
                            bounty = 300,
                            shutdownBounty = 0,
                            goldGain = null
                        )
                    )
                }
                if (minute == 8) {
                    events.add(
                        TimelineEventDto(
                            type = "CHAMPION_KILL",
                            timestamp = 8 * 60_000L + 18_000L,
                            participantId = null,
                            killerId = 6,
                            victimId = 1,
                            assistingParticipantIds = listOf(7),
                            itemId = null,
                            afterId = null,
                            beforeId = null,
                            skillSlot = null,
                            levelUpType = null,
                            wardType = null,
                            creatorId = null,
                            level = null,
                            buildingType = null,
                            laneType = null,
                            towerType = null,
                            teamId = null,
                            monsterType = null,
                            monsterSubType = null,
                            position = TimelinePositionDto(x = 3200, y = 11500),
                            bounty = 300,
                            shutdownBounty = 150,
                            goldGain = null
                        )
                    )
                }
                if (minute == 12) {
                    events.add(
                        TimelineEventDto(
                            type = "ITEM_PURCHASED",
                            timestamp = 12 * 60_000L + 15_000L,
                            participantId = 1,
                            killerId = null,
                            victimId = null,
                            assistingParticipantIds = null,
                            itemId = 3071,
                            afterId = null,
                            beforeId = null,
                            skillSlot = null,
                            levelUpType = null,
                            wardType = null,
                            creatorId = null,
                            level = null,
                            buildingType = null,
                            laneType = null,
                            towerType = null,
                            teamId = null,
                            monsterType = null,
                            monsterSubType = null,
                            position = null,
                            bounty = null,
                            shutdownBounty = null,
                            goldGain = null
                        )
                    )
                }
                if (minute == 14) {
                    events.add(
                        TimelineEventDto(
                            type = "ELITE_MONSTER_KILL",
                            timestamp = 14 * 60_000L + 10_000L,
                            participantId = null,
                            killerId = 1,
                            victimId = null,
                            assistingParticipantIds = listOf(2),
                            itemId = null,
                            afterId = null,
                            beforeId = null,
                            skillSlot = null,
                            levelUpType = null,
                            wardType = null,
                            creatorId = null,
                            level = null,
                            buildingType = null,
                            laneType = null,
                            towerType = null,
                            teamId = 100,
                            killerTeamId = 100,
                            monsterType = "RIFTHERALD",
                            monsterSubType = null,
                            position = TimelinePositionDto(x = 4500, y = 10200),
                            bounty = null,
                            shutdownBounty = null,
                            goldGain = null
                        )
                    )
                }
                TimelineFrameDto(
                    timestamp = minute * 60_000L,
                    participantFrames = pFrames,
                    events = events
                )
            }
            return MatchTimelineDto(
                metadata = MatchTimelineMetadata(
                    matchId = matchId,
                    participants = timelineParticipants.mapNotNull { it.puuid }
                ),
                info = MatchTimelineInfo(
                    frameInterval = 60_000L,
                    frames = frames,
                    participants = timelineParticipants
                )
            )
        }

        fun resolveProfileForPlayer(puuid: String, region: String): SummonerProfileDto? {
            val normalizedRegion = region.trim().lowercase()
            val matchIds: List<String> = when {
                puuid == "puuid-complete" && normalizedRegion == "euw1" ->
                    listOf("EUW1_C1", "EUW1_C2", "EUW1_C3")
                puuid == "puuid-partial" && normalizedRegion == "euw1" ->
                    listOf("EUW1_P1", "EUW1_P2", "EUW1_P3")
                puuid == "puuid-blocked" && normalizedRegion == "euw1" ->
                    listOf("EUW1_B1", "EUW1_B2")
                puuid == "puuid-stale-a" && normalizedRegion == "euw1" ->
                    listOf("EUW1_SA1", "EUW1_SA2")
                puuid == "puuid-fresh-b" && normalizedRegion == "na1" ->
                    listOf("NA1_FB1", "NA1_FB2")
                puuid == "puuid-refresh" && normalizedRegion == "euw1" ->
                    if (refreshProfileAdvanced.get()) listOf("EUW1_R2", "EUW1_R1") else listOf("EUW1_R1")
                else -> return null
            }
            val recentDtos = matchIds.mapIndexed { idx, id ->
                buildMatchDto(matchId = id, targetPuuid = puuid, win = idx % 2 == 0)
            }
            return SummonerProfileDto(
                puuid = puuid,
                gameName = "Player-$puuid",
                tagLine = normalizedRegion.uppercase(),
                summonerLevel = 420,
                profileIconId = 29,
                soloQueueRank = null,
                recentMatches = recentDtos,
                allMatchIds = matchIds
            )
        }

        fun resolveTargetPuuidForMatch(matchId: String): String = when {
            matchId.startsWith("EUW1_C") -> "puuid-complete"
            matchId.startsWith("EUW1_P") -> "puuid-partial"
            matchId.startsWith("EUW1_B") -> "puuid-blocked"
            matchId.startsWith("EUW1_SA") -> "puuid-stale-a"
            matchId.startsWith("NA1_FB") -> "puuid-fresh-b"
            matchId.startsWith("EUW1_R") -> "puuid-refresh"
            else -> "puuid-complete"
        }
    }

    /**
     * Subclass of BE55's real [AiAnalysisService] that invokes BE55's real `detectIntent` and
     * `constructPrompt` on incoming `AiController` `/api/ai/chat` payloads (enforcing byte budgets
     * and generating the exact prompt string), records the payload + prompt in [TestHarnessState],
     * and returns a deterministic offline response with zero external Gemini network calls.
     */
    class OfflinePromptVerifyingAiAnalysisService(
        private val harnessState: TestHarnessState,
        exampleRepository: AiLearnedExampleRepository,
        interactionRepository: AiInteractionRepository,
        patternRepository: DiscoveredPatternRepository,
        promptVersionRepository: PromptVersionRepository
    ) : AiAnalysisService(
        apiKey = "offline-test-key-never-called",
        geminiModel = "gemini-3.5-flash-lite",
        geminiThinkingLevel = "low",
        exampleRepository = exampleRepository,
        interactionRepository = interactionRepository,
        patternRepository = patternRepository,
        promptVersionRepository = promptVersionRepository
    ) {
        private val constructPromptMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "constructPrompt",
            Map::class.java,
            List::class.java,
            String::class.java,
            IntentResult::class.java,
            PromptVersion::class.java
        ).apply { isAccessible = true }

        override fun analyze(
            context: Map<String, Any>,
            messages: List<Map<String, String>>,
            userMessage: String,
            sessionId: String?
        ): Mono<AiAnalysisResult> {
            val intent = detectIntent(userMessage, messages)
            val constructedPrompt = constructPromptMethod.invoke(
                this,
                context,
                messages,
                userMessage,
                intent,
                null
            ) as String

            val insightsMap = context["timelineInsights"] as? Map<*, *>
            val improvementStatsMap = context["improvementStats"] as? Map<*, *>
            harnessState.chatCaptures.add(
                RecordedChatCapture(
                    userMessage = userMessage,
                    sessionId = sessionId,
                    summonerName = context["summonerName"]?.toString(),
                    timelineStatus = context["timelineStatus"]?.toString(),
                    timelineCoverage = context["timelineCoverage"],
                    gamesAnalyzed = insightsMap?.get("gamesAnalyzed"),
                    hasTimelineAnalysis = improvementStatsMap?.get("hasTimelineAnalysis"),
                    context = LinkedHashMap(context),
                    constructedPrompt = constructedPrompt
                )
            )

            val statusLabel = context["timelineStatus"]?.toString() ?: "NONE"
            val reply = buildString {
                append("Verified offline coach reply [timelineStatus=$statusLabel] for ${context["summonerName"] ?: "Unknown"}.")
                append("\n---SUGGESTIONS---\n")
                append("[\"Check 10m lane lead\", \"Review early deaths\", \"Compare CS phases\"]")
            }
            return Mono.just(AiAnalysisResult(reply, 0.7, intent.category, "v1.0"))
        }
    }

    @RestController
    @RequestMapping("/api/ai/__test_harness")
    class TestHarnessController(
        private val state: TestHarnessState,
        private val timelineService: TimelineService
    ) {
        @GetMapping("/state")
        fun getState(): Map<String, Any> = mapOf(
            "prepareHttpCalls" to state.prepareHttpCalls.toList(),
            "riotTimelineFetches" to state.riotTimelineFetches.toList(),
            "chatCaptures" to state.chatCaptures.toList(),
            "redisKeysCount" to state.redisStore.size,
            "refreshProfileAdvanced" to state.refreshProfileAdvanced.get()
        )

        @PostMapping("/reset")
        fun reset(): Map<String, Any> {
            state.reset(timelineService)
            return mapOf("ok" to true)
        }

        @PostMapping("/advance-refresh-profile")
        fun advanceRefreshProfile(): Map<String, Any> {
            state.refreshProfileAdvanced.set(true)
            return mapOf(
                "ok" to true,
                "matchIds" to listOf("EUW1_R2", "EUW1_R1")
            )
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(
        exclude = [
            DataSourceAutoConfiguration::class,
            HibernateJpaAutoConfiguration::class,
            JpaRepositoriesAutoConfiguration::class,
            RedisAutoConfiguration::class,
            RedisRepositoriesAutoConfiguration::class
        ]
    )
    @Import(
        SecurityConfig::class,
        CustomAuthenticationEntryPoint::class,
        CustomAccessDeniedHandler::class,
        GlobalExceptionHandler::class,
        TimelineController::class,
        AiController::class,
        TestHarnessController::class
    )
    class CoachTimelineHttpTestApp {

        @Bean
        fun testHarnessState(): TestHarnessState = TestHarnessState()

        @Bean
        fun prepareHttpRecordingFilter(state: TestHarnessState): OncePerRequestFilter =
            object : OncePerRequestFilter() {
                override fun doFilterInternal(
                    request: HttpServletRequest,
                    response: HttpServletResponse,
                    filterChain: FilterChain
                ) {
                    val uri = request.requestURI
                    if (request.method != "OPTIONS" && uri.startsWith("/api/riot/timeline/prepare-coach/")) {
                        val wrapped = ContentCachingRequestWrapper(request)
                        filterChain.doFilter(wrapped, response)
                        val bodyBytes = wrapped.contentAsByteArray
                        val requestedIds = if (bodyBytes.isNotEmpty()) {
                            try {
                                val dto = state.mapper.readValue(
                                    String(bodyBytes, StandardCharsets.UTF_8),
                                    CoachTimelinePreparationRequestDto::class.java
                                )
                                dto.matchIds?.filterNotNull() ?: emptyList()
                            } catch (_: Exception) {
                                emptyList()
                            }
                        } else {
                            emptyList()
                        }
                        val segments = uri.removePrefix("/api/riot/timeline/prepare-coach/").split("/")
                        state.prepareHttpCalls.add(
                            RecordedPrepareHttpCall(
                                method = request.method,
                                uri = uri,
                                region = segments.getOrNull(0) ?: "",
                                puuid = segments.getOrNull(1) ?: "",
                                requestedMatchIds = requestedIds,
                                status = response.status,
                                origin = request.getHeader("Origin")
                            )
                        )
                    } else {
                        filterChain.doFilter(request, response)
                    }
                }
            }

        @Bean
        fun userDetailsService(): UserDetailsService = mockk(relaxed = true)

        @Bean
        fun jwtUtil(): JwtUtil = mockk(relaxed = true)

        @Bean
        fun jwtAuthenticationFilter(
            userDetailsService: UserDetailsService,
            jwtUtil: JwtUtil
        ): JwtAuthenticationFilter = JwtAuthenticationFilter(userDetailsService, jwtUtil)

        @Bean
        fun stringRedisTemplate(state: TestHarnessState): StringRedisTemplate {
            val valueOps = mockk<ValueOperations<String, String>>(relaxed = true)
            every { valueOps.get(any<String>()) } answers { state.redisStore[firstArg()] }
            every { valueOps.set(any<String>(), any<String>()) } answers {
                state.redisStore[firstArg()] = secondArg()
            }
            every { valueOps.set(any<String>(), any<String>(), any<Duration>()) } answers {
                val key = firstArg<String>()
                state.redisStore[key] = secondArg()
                if (key.endsWith("EUW1_P2")) {
                    // Simulate Redis write completing at 12.1s so TimelineService's own pre-match
                    // deadline guard halts naturally before EUW1_P3 (2/3 PARTIAL + DEADLINE_EXCEEDED).
                    state.timeProvider.advanceBy(300L)
                }
            }
            every { valueOps.setIfAbsent(any<String>(), any<String>(), any<Duration>()) } answers {
                state.redisStore.putIfAbsent(firstArg(), secondArg()) == null
            }
            every { valueOps.multiGet(any<Collection<String>>()) } answers {
                firstArg<Collection<String>>().map { state.redisStore[it] }
            }

            val template = mockk<StringRedisTemplate>(relaxed = true)
            every { template.opsForValue() } returns valueOps
            every { template.delete(any<String>()) } answers {
                state.redisStore.remove(firstArg()) != null
            }
            return template
        }

        @Bean
        fun redisCacheService(
            stringRedisTemplate: StringRedisTemplate,
            objectMapper: ObjectMapper
        ): RedisCacheService = RedisCacheService(stringRedisTemplate, objectMapper)

        @Bean
        fun timelineAnalysisService(): TimelineAnalysisService = TimelineAnalysisService()

        @Bean
        fun riotApiService(state: TestHarnessState): RiotApiService {
            val mock = mockk<RiotApiService>()
            coEvery {
                mock.getMatchTimelineBounded(any(), any(), any(), any(), any(), any(), any(), any())
            } coAnswers {
                val matchId = arg<String>(0)
                val region = arg<String>(1)
                val failureHolder = arg<AtomicReference<String?>?>(5)
                state.riotTimelineFetches.add(RecordedRiotTimelineFetch(matchId = matchId, region = region))

                when {
                    matchId.startsWith("EUW1_B") -> {
                        delay(30)
                        failureHolder?.set("HTTP_429_TOO_MANY_REQUESTS")
                        null
                    }
                    matchId == "EUW1_C1" -> {
                        delay(80)
                        state.buildRichRawTimeline(matchId, "puuid-complete", seed = 1)
                    }
                    matchId == "EUW1_C2" -> state.buildRichRawTimeline(matchId, "puuid-complete", seed = 2)
                    matchId == "EUW1_C3" -> state.buildRichRawTimeline(matchId, "puuid-complete", seed = 3)
                    matchId == "EUW1_P1" -> {
                        delay(40)
                        state.timeProvider.advanceBy(6_000L)
                        state.buildRichRawTimeline(matchId, "puuid-partial", seed = 1)
                    }
                    matchId == "EUW1_P2" -> {
                        delay(20)
                        state.timeProvider.advanceBy(5_800L)
                        state.buildRichRawTimeline(matchId, "puuid-partial", seed = 2)
                    }
                    matchId == "EUW1_SA1" -> {
                        delay(260)
                        state.buildRichRawTimeline(matchId, "puuid-stale-a", seed = 1)
                    }
                    matchId.startsWith("NA1_FB") -> {
                        delay(25)
                        val seed = matchId.takeLast(1).toIntOrNull() ?: 1
                        state.buildRichRawTimeline(matchId, "puuid-fresh-b", seed = seed)
                    }
                    matchId == "EUW1_R2" -> {
                        delay(90)
                        state.buildRichRawTimeline(matchId, "puuid-refresh", seed = 2)
                    }
                    matchId == "EUW1_R1" -> {
                        delay(20)
                        state.buildRichRawTimeline(matchId, "puuid-refresh", seed = 1)
                    }
                    else -> {
                        val target = state.resolveTargetPuuidForMatch(matchId)
                        state.buildRichRawTimeline(matchId, target, seed = 1)
                    }
                }
            }
            return mock
        }

        @Bean
        fun playerCacheService(state: TestHarnessState): PlayerCacheService {
            val mock = mockk<PlayerCacheService>(relaxed = true)
            every { mock.getCachedProfile(any(), any()) } answers {
                val puuid = arg<String>(0)
                val region = arg<String>(1)
                state.resolveProfileForPlayer(puuid, region)
            }
            return mock
        }

        @Bean
        fun timelineService(
            riotApiService: RiotApiService,
            redisCacheService: RedisCacheService,
            timelineAnalysisService: TimelineAnalysisService,
            state: TestHarnessState
        ): TimelineService = TimelineService(
            riotApiService = riotApiService,
            redisCacheService = redisCacheService,
            timelineAnalysisService = timelineAnalysisService,
            rateLimiter = RiotApiRateLimiter(state.timeProvider),
            timeProvider = state.timeProvider,
            memoryStatsProvider = state.memoryProvider
        )

        @Bean
        fun aiInteractionRepository(): AiInteractionRepository {
            val repo = mockk<AiInteractionRepository>(relaxed = true)
            every { repo.findAvgQualityByTemperatureAndCategory(any()) } returns emptyList()
            every { repo.save(any<AiInteraction>()) } answers {
                val orig = firstArg<AiInteraction>()
                AiInteraction(
                    id = orig.id ?: UUID.randomUUID(),
                    user = orig.user,
                    summonerName = orig.summonerName,
                    summonerRank = orig.summonerRank,
                    summonerRole = orig.summonerRole,
                    userQuestion = orig.userQuestion,
                    aiResponse = orig.aiResponse,
                    contextFingerprint = orig.contextFingerprint,
                    promptVersion = orig.promptVersion,
                    responseTimeMs = orig.responseTimeMs,
                    temperature = orig.temperature,
                    createdAt = orig.createdAt
                )
            }
            return repo
        }

        @Bean
        fun aiFeedbackRepository(): AiFeedbackRepository = mockk(relaxed = true)

        @Bean
        fun aiLearnedExampleRepository(): AiLearnedExampleRepository = mockk(relaxed = true)

        @Bean
        fun curationRunRepository(): CurationRunRepository = mockk(relaxed = true)

        @Bean
        fun feedbackProtectionService(): FeedbackProtectionService = mockk(relaxed = true)

        @Bean
        fun discoveredPatternRepository(): DiscoveredPatternRepository = mockk(relaxed = true)

        @Bean
        fun promptVersionRepository(): PromptVersionRepository {
            val repo = mockk<PromptVersionRepository>(relaxed = true)
            every { repo.findByIsActiveTrueAndCategoryScope(any()) } returns emptyList()
            every { repo.findByIsActiveTrueAndCategoryScopeIsNull() } returns emptyList()
            return repo
        }

        @Bean
        @Primary
        fun aiAnalysisService(
            state: TestHarnessState,
            exampleRepository: AiLearnedExampleRepository,
            interactionRepository: AiInteractionRepository,
            patternRepository: DiscoveredPatternRepository,
            promptVersionRepository: PromptVersionRepository
        ): AiAnalysisService = OfflinePromptVerifyingAiAnalysisService(
            harnessState = state,
            exampleRepository = exampleRepository,
            interactionRepository = interactionRepository,
            patternRepository = patternRepository,
            promptVersionRepository = promptVersionRepository
        )
    }

    @LocalServerPort
    private var port: Int = 0

    @Autowired
    private lateinit var harnessState: TestHarnessState

    @Autowired
    private lateinit var timelineService: TimelineService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build()

    @BeforeEach
    fun setUp() {
        harnessState.reset(timelineService)
    }

    @Test
    fun `real HTTP contract serves CORS preflight, Jackson MatchEventEntry wire flags, COMPLETE, natural PARTIAL, BLOCKED, and offline AiController prompt construction`() {
        val baseUrl = "http://127.0.0.1:$port"

        // 1. Verify credentialed CORS preflight from FE origin http://localhost:3000
        val preflightReq = HttpRequest.newBuilder(
            URI.create("$baseUrl/api/riot/timeline/prepare-coach/euw1/puuid-complete")
        )
            .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
            .header("Origin", "http://localhost:3000")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "Content-Type")
            .build()
        val preflightRes = httpClient.send(preflightReq, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, preflightRes.statusCode())
        assertEquals(
            "http://localhost:3000",
            preflightRes.headers().firstValue("Access-Control-Allow-Origin").orElse(null)
        )
        assertEquals(
            "true",
            preflightRes.headers().firstValue("Access-Control-Allow-Credentials").orElse(null)
        )

        // 2. COMPLETE over real HTTP + verify Spring Jackson serializes MatchEventEntry.isPlayer as `"player":true`
        val completeRes = postJson(
            "$baseUrl/api/riot/timeline/prepare-coach/euw1/puuid-complete",
            """{"matchIds":["EUW1_C1","EUW1_C2","EUW1_C3"]}"""
        )
        assertEquals(200, completeRes.statusCode())
        val completeJson = completeRes.body()
        assertTrue(
            completeJson.contains("\"player\":true"),
            "Spring Jackson wire JSON must serialize Kotlin isPlayer as \"player\":true"
        )
        val completeTree = objectMapper.readTree(completeJson)
        assertEquals("COMPLETE", completeTree["status"].asText())
        assertEquals(3, completeTree["coveredMatchIds"].size())
        assertEquals(3, completeTree["analysis"]["aggregateSummary"]["gamesAnalyzed"].asInt())
        assertTrue(completeTree["analysis"]["improvementStats"].isObject)
        assertTrue(
            harnessState.redisStore.values.all { it.startsWith("GZIP_B64:") },
            "Redis backing store must hold streaming GZIP_B64 compressed timeline entries"
        )

        // 3. Natural PARTIAL over real HTTP (`DEADLINE_EXCEEDED` after 2/3 matches; EUW1_P3 never fetched)
        val partialRes = postJson(
            "$baseUrl/api/riot/timeline/prepare-coach/euw1/puuid-partial",
            """{"matchIds":["EUW1_P1","EUW1_P2","EUW1_P3"]}"""
        )
        assertEquals(200, partialRes.statusCode())
        val partialTree = objectMapper.readTree(partialRes.body())
        assertEquals("PARTIAL", partialTree["status"].asText())
        assertEquals("DEADLINE_EXCEEDED", partialTree["stopReason"].asText())
        assertEquals(listOf("EUW1_P1", "EUW1_P2"), partialTree["coveredMatchIds"].map { it.asText() })
        assertEquals("EUW1_P3", partialTree["missingMatches"][0]["matchId"].asText())
        assertEquals("DEADLINE_EXCEEDED", partialTree["missingMatches"][0]["reason"].asText())
        assertEquals(2, partialTree["analysis"]["aggregateSummary"]["gamesAnalyzed"].asInt())
        assertFalse(
            harnessState.riotTimelineFetches.any { it.matchId == "EUW1_P3" },
            "TimelineService must stop naturally at deadline before fetching EUW1_P3"
        )

        // 4. BLOCKED over real HTTP (`RATE_LIMIT_429` on EUW1_B1 stops immediately without hammering EUW1_B2)
        val blockedRes = postJson(
            "$baseUrl/api/riot/timeline/prepare-coach/euw1/puuid-blocked",
            """{"matchIds":["EUW1_B1","EUW1_B2"]}"""
        )
        assertEquals(200, blockedRes.statusCode())
        val blockedTree = objectMapper.readTree(blockedRes.body())
        assertEquals("BLOCKED", blockedTree["status"].asText())
        assertEquals("RATE_LIMIT_429", blockedTree["stopReason"].asText())
        assertEquals(0, blockedTree["coveredMatchIds"].size())
        assertTrue(blockedTree["analysis"].isNull)
        assertFalse(
            harnessState.riotTimelineFetches.any { it.matchId == "EUW1_B2" },
            "TimelineService must stop immediately on RATE_LIMIT_429 without hammering EUW1_B2"
        )

        // 5. Real HTTP POST /api/ai/chat invokes BE55's real AiAnalysisService.constructPrompt(...) offline
        val chatRes = postJson(
            "$baseUrl/api/ai/chat",
            objectMapper.writeValueAsString(
                mapOf(
                    "context" to mapOf(
                        "summonerName" to "Player-puuid-partial#EUW1",
                        "primaryRole" to "TOP",
                        "totalGamesAnalyzed" to 3,
                        "timelineStatus" to "PARTIAL",
                        "timelineCoverage" to mapOf(
                            "status" to "PARTIAL",
                            "requested" to 3,
                            "covered" to 2,
                            "missing" to 1,
                            "stopReason" to "DEADLINE_EXCEEDED"
                        ),
                        "timelineInsights" to mapOf(
                            "gamesAnalyzed" to 2,
                            "avgGoldLeadAt10" to 250.0
                        )
                    ),
                    "messages" to emptyList<Map<String, String>>(),
                    "userMessage" to "How was my early laning?",
                    "sessionId" to "session_contract_check"
                )
            )
        )
        assertEquals(200, chatRes.statusCode())
        assertEquals(1, harnessState.chatCaptures.size)
        val capturedPrompt = harnessState.chatCaptures.single().constructedPrompt
        assertTrue(
            capturedPrompt.contains("Timeline coverage: status=partial, covered=2/3 games, stop reason=deadline exceeded"),
            "AiAnalysisService.constructPrompt must embed truthful natural-language 2/3 partial coverage header"
        )

        // 6. Optional cross-stack bridge mode for FE18 Vitest integration runner
        val holdPortFile = System.getenv("COACH_HTTP_HOLD_PORT_FILE")?.trim()
        if (!holdPortFile.isNullOrEmpty()) {
            harnessState.reset(timelineService)
            val doneFile = System.getenv("COACH_HTTP_DONE_FILE")?.trim()
            File(holdPortFile).writeText(baseUrl, StandardCharsets.UTF_8)
            val maxHoldDeadlineMs = System.currentTimeMillis() + 90_000L
            while (System.currentTimeMillis() < maxHoldDeadlineMs) {
                if (!doneFile.isNullOrEmpty() && File(doneFile).exists()) {
                    break
                }
                Thread.sleep(100L)
            }
        }
    }

    private fun postJson(url: String, body: String): HttpResponse<String> {
        val req = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Origin", "http://localhost:3000")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build()
        return httpClient.send(req, HttpResponse.BodyHandlers.ofString())
    }
}
