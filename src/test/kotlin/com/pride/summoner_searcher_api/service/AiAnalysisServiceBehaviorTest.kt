package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.AiLearnedExample
import com.pride.summoner_searcher_api.model.DiscoveredPattern
import com.pride.summoner_searcher_api.model.PromptVersion
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.DiscoveredPatternRepository
import com.pride.summoner_searcher_api.repository.PromptVersionRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.math.BigDecimal
import java.util.UUID

class AiAnalysisServiceBehaviorTest : BehaviorSpec({
    val exampleRepository = mockk<AiLearnedExampleRepository>(relaxed = true)
    val interactionRepository = mockk<AiInteractionRepository>(relaxed = true)
    val patternRepository = mockk<DiscoveredPatternRepository>(relaxed = true)
    val promptVersionRepository = mockk<PromptVersionRepository>(relaxed = true)

    fun createService(
        model: String = "gemini-3.5-flash-lite",
        thinkingLevel: String = "low"
    ) = AiAnalysisService(
        apiKey = "test-api-key",
        geminiModel = model,
        geminiThinkingLevel = thinkingLevel,
        exampleRepository = exampleRepository,
        interactionRepository = interactionRepository,
        patternRepository = patternRepository,
        promptVersionRepository = promptVersionRepository
    )

    Given("A multi-turn coaching conversation where the player switches topics (#6)") {
        val service = createService()
        val history = listOf(
            mapOf("role" to "user", "content" to "How can I improve my vision score and control ward placement?"),
            mapOf("role" to "assistant", "content" to "Place control wards before dragon spawns.")
        )

        When("the player asks a specific question about an item build or item passive") {
            Then("it should prioritize the current message and classify as itemization_builds rather than vision_warding") {
                val intent = service.detectIntent(
                    "What does Zhonya's Hourglass do and when should I buy it over Banshee's Veil?",
                    history
                )
                intent.category shouldBe "itemization_builds"
            }
        }

        When("the player asks a short follow-up with no new topic keywords") {
            Then("it should inherit the vision_warding intent from conversation history") {
                val intent = service.detectIntent("Can you give me an example?", history)
                intent.category shouldBe "vision_warding"
            }
        }
    }

    Given("Prompt construction with self-learning examples, patterns, early-CS stats, and PromptVersion (#1, #6, #7, #11)") {
        val service = createService()
        val constructPromptMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "constructPrompt",
            Map::class.java,
            List::class.java,
            String::class.java,
            AiAnalysisService.IntentResult::class.java,
            PromptVersion::class.java
        ).apply { isAccessible = true }

        val context = mapOf(
            "summonerName" to "TestPlayer",
            "totalGamesAnalyzed" to 12,
            "recentMatches" to listOf(
                mapOf(
                    "championName" to "Orianna",
                    "teamPosition" to "MIDDLE",
                    "win" to true,
                    "kills" to 6,
                    "deaths" to 2,
                    "assists" to 9,
                    "csPerMin" to 8.4,
                    "laneMinionsFirst10Minutes" to 82,
                    "jungleCsBefore10Minutes" to 0.0
                ),
                mapOf(
                    "championName" to "Viego",
                    "teamPosition" to "JUNGLE",
                    "win" to false,
                    "kills" to 4,
                    "deaths" to 5,
                    "assists" to 6,
                    "csPerMin" to 6.9,
                    "laneMinionsFirst10Minutes" to 4,
                    "jungleCsBefore10Minutes" to 64.0
                )
            )
        )

        val activePromptVersion = PromptVersion(
            id = UUID.randomUUID(),
            versionName = "v1.1-cs_farming-challenger",
            promptTemplate = "Connect early CS@10 numbers to wave state before first dragon.",
            isActive = true,
            isChallenger = true,
            categoryScope = "cs_farming"
        )

        When("building the coaching prompt for a cs_farming query") {
            Then("it should inject learned examples, discovered patterns, PromptVersion directive, domain guardrails, and early-CS stats") {
                clearMocks(exampleRepository, patternRepository)
                every {
                    exampleRepository.findByIsActiveTrueAndQuestionCategoryOrderByQualityScoreDesc("cs_farming", any())
                } returns listOf(
                    AiLearnedExample(
                        questionCategory = "cs_farming",
                        userQuestion = "How do I stop missing CS at 10 minutes?",
                        aiResponse = "Crash the third wave before roaming so minions don't die to your tower.",
                        qualityScore = BigDecimal("92.5")
                    )
                )

                every {
                    patternRepository.findByTriggerCategoryAndIsActiveTrue("cs_farming")
                } returns listOf(
                    DiscoveredPattern(
                        triggerCategory = "cs_farming",
                        followUpCategory = "wave_management",
                        occurrenceCount = 15,
                        confidenceScore = 0.72
                    )
                )

                val intent = service.detectIntent("Why is my CS low in the first 10 minutes?")
                val prompt = constructPromptMethod.invoke(
                    service,
                    context,
                    emptyList<Map<String, String>>(),
                    "Why is my CS low in the first 10 minutes?",
                    intent,
                    activePromptVersion
                ) as String

                prompt shouldContain "PROVEN EXAMPLES"
                prompt shouldContain "Crash the third wave before roaming"
                prompt shouldContain "wave_management (72% of users ask this next)"
                prompt shouldContain "CATEGORY COACHING DIRECTIVE (v1.1-cs_farming-challenger)"
                prompt shouldContain "Connect early CS@10 numbers to wave state before first dragon."
                prompt shouldContain "Direct Answer Priority (#1 Rule)"
                prompt shouldContain "For general League of Legends domain questions (such as item descriptions"
                prompt shouldContain "laneCs@10=82"
                prompt shouldContain "jungleCs@10=64.0"
            }
        }
    }

    Given("Champion and Challenger PromptVersion A/B selection (#11)") {
        val service = createService()

        When("selecting a prompt version across 40 requests") {
            Then("both champion and challenger receive traffic and totalUses is incremented") {
                clearMocks(promptVersionRepository)
                val champion = PromptVersion(
                    id = UUID.randomUUID(),
                    versionName = "v1.0-builds",
                    promptTemplate = "Baseline builds prompt",
                    isActive = true,
                    isChallenger = false,
                    categoryScope = "itemization_builds",
                    totalUses = 10
                )
                val challenger = PromptVersion(
                    id = UUID.randomUUID(),
                    versionName = "v1.1-builds-challenger",
                    promptTemplate = "Challenger builds prompt",
                    isActive = true,
                    isChallenger = true,
                    categoryScope = "itemization_builds",
                    totalUses = 5
                )

                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("itemization_builds") } returns listOf(champion, challenger)
                every { promptVersionRepository.save(any<PromptVersion>()) } answers { firstArg<PromptVersion>() }

                val chosenNames = (1..40).mapNotNull { service.selectPromptVersion("itemization_builds")?.versionName }.toSet()

                chosenNames shouldContain "v1.0-builds"
                chosenNames shouldContain "v1.1-builds-challenger"
                verify(exactly = 40) { promptVersionRepository.save(any<PromptVersion>()) }
            }
        }
    }

    Given("Gemini 3.5 Flash-Lite generationConfig and SSE thought-part filtering (#10, #12)") {
        val service35 = createService(model = "gemini-3.5-flash-lite", thinkingLevel = "low")
        val service25 = createService(model = "gemini-2.5-flash", thinkingLevel = "low")

        val buildConfigMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "buildGenerationConfig",
            Double::class.javaPrimitiveType
        ).apply { isAccessible = true }

        val extractContentMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "extractContent",
            String::class.java
        ).apply { isAccessible = true }

        val extractStreamDeltaMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "extractStreamDelta",
            String::class.java
        ).apply { isAccessible = true }

        When("building generationConfig for gemini-3.5-flash-lite vs legacy gemini-2.5-flash") {
            Then("gemini-3.5-flash-lite includes thinkingConfig.thinkingLevel=low while gemini-2.5-flash omits it") {
                @Suppress("UNCHECKED_CAST")
                val config35 = buildConfigMethod.invoke(service35, 0.7) as Map<String, Any>
                @Suppress("UNCHECKED_CAST")
                val config25 = buildConfigMethod.invoke(service25, 0.7) as Map<String, Any>

                config35["thinkingConfig"] shouldBe mapOf("thinkingLevel" to "low")
                config25.containsKey("thinkingConfig") shouldBe false
            }
        }

        When("extracting standard and SSE responses that contain internal thought parts") {
            Then("internal thought parts are stripped and only the coaching answer is returned") {
                val samplePayload = """
                    {
                      "candidates": [
                        {
                          "finishReason": "STOP",
                          "content": {
                            "parts": [
                              { "thought": true, "text": "Internal reasoning step that should not leak to user. " },
                              { "text": "Buy **Zhonya's Hourglass** second against Zed to negate Death Mark." }
                            ]
                          }
                        }
                      ]
                    }
                """.trimIndent()

                val standardText = extractContentMethod.invoke(service35, samplePayload) as String
                val streamDelta = extractStreamDeltaMethod.invoke(service35, samplePayload) as String

                standardText shouldBe "Buy **Zhonya's Hourglass** second against Zed to negate Death Mark."
                standardText shouldNotContain "Internal reasoning"
                streamDelta shouldBe "Buy **Zhonya's Hourglass** second against Zed to negate Death Mark."
            }
        }
    }

    Given("Issue #17 AI coach prompt: softened length rule, rank-appropriate persona, and explicit data-boundary declarations") {
        val service = createService()
        val constructPromptMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "constructPrompt",
            Map::class.java,
            List::class.java,
            String::class.java,
            AiAnalysisService.IntentResult::class.java,
            PromptVersion::class.java
        ).apply { isAccessible = true }

        When("building a prompt for a Gold player when timelineStatus is NOT_ANALYZED") {
            Then("it uses rank-appropriate Gold–Platinum guidance, replaces the hard 150-word rule, and includes all explicit data boundaries and guardrails") {
                val goldContext = mapOf(
                    "summonerName" to "MidLearner#EUW",
                    "rank" to "GOLD II (54 LP)",
                    "primaryRole" to "MIDDLE",
                    "totalGamesAnalyzed" to 10,
                    "timelineStatus" to "NOT_ANALYZED",
                    "recentMatches" to listOf(
                        mapOf(
                            "champion" to "Ahri",
                            "role" to "MIDDLE",
                            "win" to true,
                            "kda" to "4.50",
                            "csPerMin" to 7.8,
                            "laneCsAt10" to mapOf("actual" to 81, "spawnedSupply" to 120)
                        )
                    )
                )

                val intent = service.detectIntent("Review my early laning and vision habits")
                val prompt = constructPromptMethod.invoke(
                    service,
                    goldContext,
                    emptyList<Map<String, String>>(),
                    "Review my early laning and vision habits",
                    intent,
                    null
                ) as String

                // 1. Softened 150-word rule -> concise-but-natural depth guidance
                prompt shouldNotContain "MUST be under 150 words"
                prompt shouldNotContain "brutally efficient elevator pitch"
                prompt shouldContain "Concise & Natural Depth"
                prompt shouldContain "allow longer, fuller explanations when the question needs depth"

                // 2. Rank-appropriate persona instead of hardcoded Challenger persona
                prompt shouldNotContain "Treat the player as a Challenger tier player"
                prompt shouldContain "Rank-Appropriate Persona (GOLD II (54 LP))"
                prompt shouldContain "Gold–Platinum progression level"

                // 3. Explicit data-boundary phrasing rules
                prompt shouldContain "timelineStatus: NOT_ANALYZED"
                prompt shouldContain "When `timelineStatus` is `NOT_ANALYZED`, state clearly that only end-of-game summary and challenge stats are available"
                prompt shouldContain "WARD_PLACED"
                prompt shouldContain "never claim ward placement in specific bushes"
                prompt shouldContain "assisted deaths are not necessarily jungle ganks"
                prompt shouldContain "cs10.spawnedSupply"
                prompt shouldContain "120 on verified patch 26.1+ games only"
                prompt shouldContain "laneCs@10=81"
                prompt shouldContain "cs10.spawnedSupply=120"

                // 4. Existing guardrails and ---SUGGESTIONS--- contract preserved
                prompt shouldContain "---SUGGESTIONS---"
                prompt shouldContain "NO SOURCE CODE"
                prompt shouldContain "NO OFF-TOPIC"
            }
        }

        When("building a prompt for a Silver player vs a Master player") {
            Then("it adapts the rank persona to Iron–Silver fundamentals vs Master+ apex play") {
                val silverPrompt = constructPromptMethod.invoke(
                    service,
                    mapOf("rank" to "SILVER IV (20 LP)", "totalGamesAnalyzed" to 8),
                    emptyList<Map<String, String>>(),
                    "How do I die less?",
                    null,
                    null
                ) as String

                val masterPrompt = constructPromptMethod.invoke(
                    service,
                    mapOf("rank" to "MASTER I (240 LP)", "totalGamesAnalyzed" to 30),
                    emptyList<Map<String, String>>(),
                    "How do I punish wave bounce timings?",
                    null,
                    null
                ) as String

                silverPrompt shouldContain "Iron–Silver fundamental level"
                masterPrompt shouldContain "Master+ apex level"
            }
        }

        When("building a prompt with full enriched match context and incomplete legacy cache timeline fields (#17 contract repair)") {
            Then("it serializes draft, opponent items/runes/stats, damage split, objective/turret damage, and marks null legacy cache fields as incomplete rather than zero") {
                val enrichedContext = mapOf(
                    "summonerName" to "MidLearner#EUW",
                    "rank" to "PLATINUM III (40 LP)",
                    "winRate" to "56.0%",
                    "totalWins" to 28,
                    "totalLosses" to 22,
                    "primaryRole" to "MIDDLE",
                    "totalGamesAnalyzed" to 10,
                    "timelineStatus" to "ANALYZED",
                    "topChampions" to listOf(
                        mapOf("name" to "Ahri", "games" to 6, "winRate" to 67, "avgKda" to "4.20", "avgCsPerMin" to "7.9")
                    ),
                    "recentMatches" to listOf(
                        mapOf(
                            "matchId" to "EUW1_7000000001",
                            "gameVersion" to "16.1.648.1234",
                            "gameDuration" to "30:00",
                            "champion" to "Ahri",
                            "role" to "MIDDLE",
                            "win" to true,
                            "kda" to "8/2/11 (9.50)",
                            "killParticipation" to "68%",
                            "csPerMin" to 8.1,
                            "goldPerMin" to 445,
                            "damagePerMin" to 812,
                            "damageShare" to "29%",
                            "damageSplitPct" to mapOf("physical" to 8, "magic" to 79, "true" to 13),
                            "damageDealtToObjectives" to 14200,
                            "damageDealtToTurrets" to 6400,
                            "turretPlates" to 3,
                            "visionScore" to 38,
                            "visionScorePerMin" to "1.27",
                            "controlWardsPlaced" to 5,
                            "wardsPlaced" to 14,
                            "wardsKilled" to 6,
                            "keystone" to 8112,
                            "subStyle" to 8200,
                            "items" to listOf(6655, 3020, 3157),
                            "laneCsAt10" to mapOf(
                                "actual" to 84,
                                "spawnedSupply" to 120,
                                "supplyLabel" to "S16 patch 26.1+ verified spawned wave supply"
                            ),
                            "draft" to mapOf(
                                "allies" to listOf("Gnar (TOP)", "Viego (JUNGLE)", "Ahri (MIDDLE)", "Jinx (BOTTOM)", "Lulu (UTILITY)"),
                                "enemies" to listOf("Renekton (TOP)", "LeeSin (JUNGLE)", "Syndra (MIDDLE)", "KaiSa (BOTTOM)", "Nautilus (UTILITY)")
                            ),
                            "opponent" to mapOf(
                                "champion" to "Syndra",
                                "kda" to "2/8/3 (0.63)",
                                "csPerMin" to 6.8,
                                "killParticipation" to "38%",
                                "visionScore" to 21,
                                "turretPlates" to 1,
                                "keystone" to 8229,
                                "subStyle" to 8300,
                                "items" to listOf(6655, 3020),
                                "maxCsAdvantageOnLaneOpponent" to 24.0,
                                "maxLevelLeadLaneOpponent" to 2,
                                "visionScoreAdvantageLaneOpponent" to "35%"
                            ),
                            "deathContext" to mapOf(
                                "totalDeaths" to 2,
                                "deathsBefore10" to 1,
                                "assistedDeaths" to null,
                                "unassistedDeaths" to null,
                                "deathsNearObjectiveWindows" to 1,
                                "events" to listOf(
                                    mapOf("minute" to 5, "killer" to "Syndra", "assistants" to null, "nearObjectiveWindow" to true)
                                )
                            )
                        )
                    ),
                    "timelineInsights" to mapOf(
                        "matchesAnalyzed" to 1,
                        "avgGoldDiffAt10" to 300,
                        "avgGoldDiffAt15" to 650,
                        "avgCsAt10" to 84.0,
                        "avgCsAt15" to 132.0,
                        "avgLevelDiffAt10" to 0.8,
                        "earlyLeadConversionRate" to 100.0,
                        "throwRate" to 0.0,
                        "comebackRate" to 0.0,
                        "totalDeaths" to 2,
                        "deathsBefore10" to 1,
                        "assistedDeaths" to null,
                        "unassistedDeaths" to null,
                        "deathsNearObjectiveWindows" to 1,
                        "avgDeathMinute" to 11.5,
                        "wardsPlacedTotal" to 0,
                        "wardsByTimeBucket" to null,
                        "dominantPlaystyle" to "Early Game Aggressor",
                        "keyStrength" to "Strong early laning",
                        "keyWeakness" to "Mid-game positioning"
                    )
                )

                val prompt = constructPromptMethod.invoke(
                    service,
                    enrichedContext,
                    emptyList<Map<String, String>>(),
                    "Review my matchup, build, damage split, and deaths",
                    null,
                    null
                ) as String

                // Draft, opponent items/runes/stats, damage split, and objective/turret damage must be serialized
                prompt shouldContain "draft={allies=[Gnar (TOP), Viego (JUNGLE), Ahri (MIDDLE), Jinx (BOTTOM), Lulu (UTILITY)]"
                prompt shouldContain "enemies=[Renekton (TOP), LeeSin (JUNGLE), Syndra (MIDDLE), KaiSa (BOTTOM), Nautilus (UTILITY)]}"
                prompt shouldContain "damageSplitPct={physical=8%, magic=79%, true=13%}"
                prompt shouldContain "damageDealtToObjectives=14200"
                prompt shouldContain "damageDealtToTurrets=6400"
                prompt shouldContain "turretPlates=3"
                prompt shouldContain "subStyle=8200"
                prompt shouldContain "opponentDetails={champion=Syndra, kda=2/8/3 (0.63), cs/min=6.8, kp=38%, vision=21, plates=1, keystone=8229, subStyle=8300, items=[6655, 3020]}"
                prompt shouldContain "topChampions: Ahri (6g, 67% WR, 4.20 KDA, 7.9 CS/m)"

                // Legacy cache null fields must render as incomplete, never coerced to 0, and never label unassisted as 1v1
                prompt shouldContain "assistedDeaths=incomplete, unassistedDeaths=incomplete"
                prompt shouldContain "assistants=incomplete"
                prompt shouldContain "Assisted deaths (multi-enemy involvement, not necessarily ganks): incomplete (legacy cache)"
                prompt shouldContain "Unassisted deaths (single-enemy kill credit, no assist credit): incomplete (legacy cache)"
                prompt shouldNotContain "Unassisted (1v1) deaths"
                prompt shouldContain "never equate unassisted deaths with 1v1 duels"
                prompt shouldContain "Wards placed by time bucket (no coordinates): incomplete (legacy cache)"
                prompt shouldContain "Incomplete Legacy Cache Fields"
            }
        }

        When("building a prompt from a full 20-match enriched payload with multi-turn history and few-shot examples (#17 budget & question selection)") {
            Then("it selects relevant bounded matches for the question, summarizes omitted matches, deduplicates redundant fields, and stays strictly within the hard byte/token budget") {
                clearMocks(exampleRepository, patternRepository)
                every {
                    exampleRepository.findByIsActiveTrueAndQuestionCategoryOrderByQualityScoreDesc(any(), any())
                } returns listOf(
                    AiLearnedExample(
                        questionCategory = "matchups",
                        userQuestion = "How do I play melee vs ranged mid matchups without dying to early poke and ganks?",
                        aiResponse = "Concede the first three ranged minions if needed to preserve HP for the level 3 bounce, and ward raptors at 2:30.",
                        qualityScore = BigDecimal("94.0")
                    ),
                    AiLearnedExample(
                        questionCategory = "matchups",
                        userQuestion = "When should Sylas trade into control mages?",
                        aiResponse = "Trade when their primary waveclear or stun cooldown is used on the wave, and avoid extending past river without jungle vision.",
                        qualityScore = BigDecimal("91.0")
                    )
                )

                // Build 20 full matches (recentMatches[0] = newest Game 20, recentMatches[19] = oldest Game 1)
                // Chronological Game 3 (index 17 in recentMatches) and Game 11 (index 9 in recentMatches) are Sylas vs Viktor losses.
                val fullTwentyMatches = (0 until 20).map { reverseIdx ->
                    val chronoGameNum = 20 - reverseIdx // 20 down to 1
                    val isSylasVsViktor = chronoGameNum == 3 || chronoGameNum == 11
                    val myChamp = if (isSylasVsViktor) "Sylas" else if (chronoGameNum % 2 == 0) "Ahri" else "Orianna"
                    val oppChamp = if (isSylasVsViktor) "Viktor" else if (chronoGameNum % 2 == 0) "Syndra" else "Zed"
                    val win = !isSylasVsViktor && (chronoGameNum % 3 != 0)

                    mapOf(
                        "matchId" to "EUW1_70000000${chronoGameNum.toString().padStart(2, '0')}",
                        "patch" to "16.1",
                        "role" to "MIDDLE",
                        "side" to if (chronoGameNum % 2 == 0) "Blue" else "Red",
                        "champion" to myChamp,
                        "win" to win,
                        "kda" to if (isSylasVsViktor) "2/7/4 (0.86)" else "7/2/9 (8.00)",
                        "csPerMin" to if (isSylasVsViktor) 6.4 else 8.1,
                        "killParticipation" to 62.5,
                        "keystone" to "Electrocute",
                        "subStyle" to "Inspiration",
                        "summonerSpells" to listOf("Flash", "Teleport"),
                        "items" to listOf(
                            "Luden's Companion", "Sorcerer's Shoes", "Zhonya's Hourglass",
                            "Rabadon's Deathcap", "Void Staff", "Cryptbloom"
                        ),
                        "draft" to mapOf(
                            "allies" to listOf("Gnar (TOP)", "Viego (JUNGLE)", "$myChamp (MIDDLE)", "Jinx (BOTTOM)", "Lulu (UTILITY)"),
                            "enemies" to listOf("Renekton (TOP)", "LeeSin (JUNGLE)", "$oppChamp (MIDDLE)", "KaiSa (BOTTOM)", "Nautilus (UTILITY)")
                        ),
                        "damageSplitPct" to mapOf("physical" to 9, "magic" to 82, "true" to 9),
                        "opponent" to mapOf(
                            "name" to "EnemyMid$chronoGameNum",
                            "champion" to oppChamp,
                            "kda" to if (isSylasVsViktor) "9/1/7 (16.00)" else "3/6/4 (1.17)",
                            "csPerMin" to 8.3,
                            "killParticipation" to 68.0,
                            "visionScore" to 29,
                            "turretPlates" to 2,
                            "keystone" to "Summon Aery",
                            "subStyle" to "Precision",
                            "items" to listOf(
                                "Blackfire Torch", "Sorcerer's Shoes", "Lich Bane",
                                "Zhonya's Hourglass", "Rabadon's Deathcap", "Void Staff"
                            )
                        ),
                        "gameDuration" to 31.4,
                        "visionScore" to 26,
                        "turretPlates" to 2,
                        "turretPlatesTaken" to 2,
                        "controlWardsPlaced" to 3,
                        "wardsPlaced" to 13,
                        "wardsKilled" to 5,
                        "damageDealtToObjectives" to 12450,
                        "damageDealtToTurrets" to 5800,
                        "damagePerMinute" to 780,
                        "goldPerMinute" to 430,
                        "soloKills" to 1,
                        "earlyLaningPhaseGoldExpAdvantage" to -0.15,
                        "maxCsAdvantageOnLaneOpponent" to 12.0,
                        "visionScoreAdvantageLaneOpponent" to 0.1,
                        "timeCCingOthers" to 22,
                        "totalDamageShieldedOnTeammates" to 0,
                        "totalHealsOnTeammates" to 0,
                        "laneCsAt10" to mapOf(
                            "actual" to 76,
                            "spawnedSupply" to 120,
                            "supplyLabel" to "S16 patch 26.1+ (Faster Minions: 120 spawned by 10:00)"
                        ),
                        "timelineAt10" to mapOf("goldDiff" to -350, "xpDiff" to -220, "csDiff" to -9),
                        "timelineAt15" to mapOf("goldDiff" to -780, "xpDiff" to -490, "csDiff" to -16),
                        "deathContext" to mapOf(
                            "totalDeaths" to if (isSylasVsViktor) 7 else 2,
                            "firstDeathMinute" to 3.5,
                            "deathsBefore10" to if (isSylasVsViktor) 2 else 1,
                            "assistedDeaths" to if (isSylasVsViktor) 5 else 1,
                            "unassistedDeaths" to if (isSylasVsViktor) 2 else 1,
                            "unknownAssistDeaths" to 0,
                            "missingPositionDeaths" to 0,
                            "deathsNearObjectiveWindows" to 2,
                            "earlyDeaths" to listOf(
                                mapOf(
                                    "time" to "03:30",
                                    "minute" to 3.5,
                                    "approxZone" to "Mid Lane",
                                    "killer" to oppChamp,
                                    "participants" to listOf(myChamp, oppChamp, "LeeSin"),
                                    "assistants" to listOf("LeeSin"),
                                    "assisted" to true,
                                    "assistantsCount" to 1
                                ),
                                mapOf(
                                    "time" to "07:45",
                                    "minute" to 7.75,
                                    "approxZone" to "Top Side River",
                                    "killer" to "LeeSin",
                                    "participants" to listOf(myChamp, "LeeSin", oppChamp),
                                    "assistants" to listOf(oppChamp),
                                    "assisted" to true,
                                    "assistantsCount" to 1
                                ),
                                mapOf(
                                    "time" to "09:20",
                                    "minute" to 9.33,
                                    "approxZone" to "Mid Lane",
                                    "killer" to oppChamp,
                                    "participants" to listOf(myChamp, oppChamp),
                                    "assistants" to emptyList<String>(),
                                    "assisted" to false,
                                    "assistantsCount" to 0
                                )
                            )
                        ),
                        "itemSpikes" to listOf(
                            mapOf("item" to "Luden's Companion", "minute" to 11),
                            mapOf("item" to "Zhonya's Hourglass", "minute" to 19),
                            mapOf("item" to "Rabadon's Deathcap", "minute" to 26)
                        ),
                        "skillMaxOrder" to "Q > W > E"
                    )
                }

                val largeContext = mapOf(
                    "summonerName" to "MidLearner#EUW",
                    "rank" to "EMERALD II (65 LP)",
                    "totalWins" to 58,
                    "totalLosses" to 49,
                    "primaryRole" to "MIDDLE",
                    "totalGamesAnalyzed" to 20,
                    "timelineStatus" to "ANALYZED",
                    "visionDataPrecision" to "Ward counts, types, and time-buckets only — Riot WARD_PLACED events carry no coordinates; never present ward locations.",
                    "winRate" to "54.2%",
                    "kda" to "3.45",
                    "avgKills" to "6.2",
                    "avgDeaths" to "3.8",
                    "avgAssists" to "7.1",
                    "avgCsPerMin" to "7.8",
                    "avgKillParticipation" to "61.0%",
                    "avgSoloKills" to "1.2",
                    "avgTurretPlates" to "1.9",
                    "avgVisionScore" to "25.4",
                    "blueSideWinRate" to "58.0%",
                    "redSideWinRate" to "50.0%",
                    "topChampions" to listOf(
                        mapOf("name" to "Ahri", "games" to 9, "winRate" to "66.7%", "kda" to "4.20"),
                        mapOf("name" to "Orianna", "games" to 9, "winRate" to "55.6%", "kda" to "3.60"),
                        mapOf("name" to "Sylas", "games" to 2, "winRate" to "0.0%", "kda" to "0.86")
                    ),
                    "opponentStats" to mapOf(
                        "avgKda" to "2.90",
                        "avgCsPerMin" to "7.5",
                        "avgKillParticipation" to "55.0%",
                        "avgSoloKills" to "0.9",
                        "avgTurretPlates" to "1.4",
                        "avgVisionScore" to "23.0"
                    ),
                    "topStrengths" to listOf(
                        mapOf("name" to "Damage Per Minute", "consistency" to "82%"),
                        mapOf("name" to "Kill Participation", "consistency" to "78%")
                    ),
                    "topWeaknesses" to listOf(
                        mapOf("name" to "Early Deaths", "consistency" to "38%"),
                        mapOf("name" to "Control Wards", "consistency" to "41%")
                    ),
                    "championMatchups" to mapOf(
                        "Syndra" to mapOf("wins" to 6, "losses" to 3, "total" to 9, "winRate" to "66.7%"),
                        "Zed" to mapOf("wins" to 5, "losses" to 4, "total" to 9, "winRate" to "55.6%"),
                        "Viktor" to mapOf("wins" to 0, "losses" to 2, "total" to 2, "winRate" to "0.0%")
                    ),
                    "recentMatches" to fullTwentyMatches,
                    "timelineInsights" to mapOf(
                        "gamesAnalyzed" to 20,
                        "avgGoldLeadAt10" to -120.0,
                        "avgGoldLeadAt15" to -240.0,
                        "avgFirstDeathMinute" to 5.4,
                        "mostDangerousZone" to "Mid Lane",
                        "wardsPlacedTotal" to 240,
                        "wardsByTimeBucket" to mapOf("pre10" to 65, "min10To20" to 98, "post20" to 77),
                        "killsTotal" to 124,
                        "deathsTotal" to 76,
                        "assistsTotal" to 142,
                        "deathsBefore10" to 22,
                        "assistedDeaths" to 51,
                        "unassistedDeaths" to 25,
                        "deathsNearObjectiveWindows" to 29
                    )
                )

                val history = listOf(
                    mapOf("role" to "user", "content" to "How has my mid lane pool looked over my last 20 games?"),
                    mapOf("role" to "assistant", "content" to "Your Ahri and Orianna are solid, but your two Sylas games were tough losses."),
                    mapOf("role" to "user", "content" to "What should I focus on fixing first?"),
                    mapOf("role" to "assistant", "content" to "Early deaths before 10 minutes in ranged matchups are costing you lane tempo.")
                )

                val userQuestion = "Why did I struggle on Sylas against Viktor and die early?"
                val intent = service.detectIntent(userQuestion, history)
                val prompt = constructPromptMethod.invoke(
                    service,
                    largeContext,
                    history,
                    userQuestion,
                    intent,
                    null
                ) as String

                val buildGroundTruthMethod = AiAnalysisService::class.java.getDeclaredMethod(
                    "buildGroundTruthSection",
                    Map::class.java,
                    String::class.java,
                    String::class.java,
                    Int::class.javaPrimitiveType
                )
                buildGroundTruthMethod.isAccessible = true

                val rawContextJsonBytes = com.fasterxml.jackson.databind.ObjectMapper()
                    .findAndRegisterModules()
                    .writeValueAsString(largeContext)
                    .toByteArray(Charsets.UTF_8)
                    .size
                val groundTruthSection = buildGroundTruthMethod.invoke(
                    service,
                    largeContext,
                    userQuestion,
                    intent.category,
                    AiAnalysisService.MAX_GROUND_TRUTH_BYTES
                ) as String
                val groundTruthBytes = AiAnalysisService.utf8ByteSize(groundTruthSection)

                val promptBytes = AiAnalysisService.utf8ByteSize(prompt)
                val heuristicTokens = AiAnalysisService.heuristicTokenEstimate(prompt)
                println("MEASURED_20_MATCH_FIXTURE_BYTES: rawContextJsonBytes=$rawContextJsonBytes, groundTruthBytes=$groundTruthBytes, finalPromptBytes=$promptBytes, heuristicTokens=$heuristicTokens")

                // 1. Hard size budget: must stay strictly under MAX_PROMPT_BYTES (16,000 bytes / 4,000 heuristic est tokens), vs old 46.7k bytes
                (rawContextJsonBytes > 30_000) shouldBe true
                (groundTruthBytes in 4_000..9_500) shouldBe true
                (promptBytes in 10_000..AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
                (heuristicTokens <= 4_000) shouldBe true
                // User question is preserved verbatim (never silently truncated)
                prompt shouldContain "USER'S QUESTION: \"$userQuestion\""

                // 2. Bounded match count + omitted rollup
                val expandedGameLines = Regex("""^\s{4}Game \d+""", RegexOption.MULTILINE).findAll(prompt).count()
                (expandedGameLines <= AiAnalysisService.MAX_SELECTED_MATCHES) shouldBe true
                (expandedGameLines >= AiAnalysisService.MIN_SELECTED_MATCHES) shouldBe true
                prompt shouldContain "omittedMatchesRollup:"
                prompt shouldContain "recentMatchSummary: 13 wins / 7 losses in last 20 games"

                // 3. Question-selected match relevance: older Sylas vs Viktor games (Game 3 and Game 11) + Most Recent Match (Game 20) are selected with full detail
                prompt shouldContain "Game 3: L on Sylas vs EnemyMid3 (Viktor)"
                prompt shouldContain "Game 11: L on Sylas vs EnemyMid11 (Viktor)"
                prompt shouldContain "Game 20 (Most Recent Match):"
                prompt shouldContain "opponentDetails={champion=Viktor"

                // 4. Redundant earlyDeaths fields stripped & supplyLabel emitted at most once
                prompt shouldNotContain "participants="
                prompt shouldNotContain "assistantsCount="
                Regex("cs10\\.supplyLabel=").findAll(prompt).count() shouldBe 1

                // 5. Observed facts vs hypotheses, conditional recommendations, and no stale Mythic/unsupported build-effect claims (including suggestions)
                prompt shouldContain "Observed Facts vs. Hypotheses & Conditional Recommendations"
                prompt shouldContain "Death events do **not** prove bad trades, overextension"
                prompt shouldContain "frame all coaching recommendations **conditionally**"
                prompt shouldContain "No Stale Mythic-Era or Unsupported Build-Effect Claims (Including `---SUGGESTIONS---`)"
                prompt shouldContain "The Mythic item system was removed in Season 14"
                prompt shouldContain "NEVER propose or mention \"Mythic items\", \"Mythic passives\", \"Mythic paths/rushes\""
                prompt shouldContain "Do not claim specific patch-dependent item stats, gold costs, or build-effect numbers unless the supplied patch/context data explicitly supports them"
                prompt shouldContain "never mention Mythic items/paths or unsupported build-effect claims in suggestions"

                // 6. Extreme dynamic blocks (huge conversation history + long PromptVersion + 1,800-byte user question)
                // still enforce finalPrompt <= MAX_PROMPT_BYTES without truncating the user's question
                val hugeHistory = (1..20).flatMap { turn ->
                    listOf(
                        mapOf("role" to "user", "content" to "Turn $turn question: " + "wave state and roam timer details ".repeat(25)),
                        mapOf("role" to "assistant", "content" to "Turn $turn answer: " + "focus on crash timing before river rotations ".repeat(25))
                    )
                }
                val longPromptVersion = PromptVersion(
                    versionName = "v1.1-stress-test",
                    promptTemplate = "Extra coaching directive: " + "verify ground truth numbers carefully ".repeat(60)
                )
                val nearLimitQuestion = "Why did I lose on Sylas vs Viktor? " + "Please check my early lane state and item timings. ".repeat(32)
                (AiAnalysisService.utf8ByteSize(nearLimitQuestion) in 1_500..AiAnalysisService.MAX_USER_MESSAGE_BYTES) shouldBe true

                val stressedPrompt = constructPromptMethod.invoke(
                    service,
                    largeContext,
                    hugeHistory,
                    nearLimitQuestion,
                    intent,
                    longPromptVersion
                ) as String
                val stressedBytes = AiAnalysisService.utf8ByteSize(stressedPrompt)
                println("MEASURED_STRESSED_PROMPT_BYTES: stressedBytes=$stressedBytes, maxAllowed=${AiAnalysisService.MAX_PROMPT_BYTES}")
                (stressedBytes <= AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
                stressedPrompt shouldContain "USER'S QUESTION: \"$nearLimitQuestion\""
            }
        }

        When("the user question exceeds MAX_USER_MESSAGE_BYTES (> 2,000 UTF-8 bytes)") {
            Then("it should explicitly refuse without silent truncation or paid Gemini calls") {
                val oversizedQuestion = "Can you review every wave and trade in detail? ".repeat(60)
                val questionBytes = AiAnalysisService.utf8ByteSize(oversizedQuestion)
                (questionBytes > AiAnalysisService.MAX_USER_MESSAGE_BYTES) shouldBe true

                val intent = service.detectIntent(oversizedQuestion, emptyList())

                // Direct constructPrompt call throws PromptBudgetExceededException rather than silently truncating userMessage
                val invocationEx = io.kotest.assertions.throwables.shouldThrow<java.lang.reflect.InvocationTargetException> {
                    constructPromptMethod.invoke(
                        service,
                        mapOf("summonerName" to "TestPlayer#NA1"),
                        emptyList<Map<String, String>>(),
                        oversizedQuestion,
                        intent,
                        null
                    )
                }
                (invocationEx.targetException is AiAnalysisService.PromptBudgetExceededException) shouldBe true

                // Public analyze() returns an explicit safe refusal without calling Gemini
                val syncResult = service.analyze(
                    context = mapOf("summonerName" to "TestPlayer#NA1"),
                    messages = emptyList(),
                    userMessage = oversizedQuestion
                ).block()!!
                syncResult.response shouldContain "exceeds the safe coaching context budget"
                syncResult.response shouldContain "${AiAnalysisService.MAX_USER_MESSAGE_BYTES} UTF-8 bytes"
                syncResult.response shouldContain "---SUGGESTIONS---"

                // Public analyzeStream() also emits the explicit safe refusal without calling Gemini
                val streamResult = service.analyzeStream(
                    context = mapOf("summonerName" to "TestPlayer#NA1"),
                    messages = emptyList(),
                    userMessage = oversizedQuestion
                ).chunks.collectList().block()!!.joinToString("")
                streamResult shouldContain "exceeds the safe coaching context budget"
                streamResult shouldContain "${AiAnalysisService.MAX_USER_MESSAGE_BYTES} UTF-8 bytes"
            }
        }
    }
})

