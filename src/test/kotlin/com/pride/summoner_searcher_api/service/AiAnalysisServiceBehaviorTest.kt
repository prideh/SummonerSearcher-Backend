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
                    "avgLaneCsAt10" to mapOf(
                        "avgActual" to 81.0,
                        "matchCount" to 1,
                        "avgTarget" to 120.0,
                        "targetMatchCount" to 1,
                        "avgSupportedActual" to 81.0,
                        "targetLabel" to "Patch 26.1+ passive wave spawn total by 10:00 (114 Top/Bot, 120 Mid; excludes pets/wards/objectives)",
                        "roleBreakdown" to listOf(
                            mapOf(
                                "role" to "MIDDLE",
                                "matchCount" to 1,
                                "avgActual" to 81.0,
                                "avgTarget" to 120.0,
                                "targetMatchCount" to 1,
                                "avgSupportedActual" to 81.0
                            )
                        )
                    ),
                    "missingLaneCsAt10Matches" to 0,
                    "recentMatches" to listOf(
                        mapOf(
                            "champion" to "Ahri",
                            "role" to "MIDDLE",
                            "win" to true,
                            "kda" to "4.50",
                            "csPerMin" to 7.8,
                            "laneCsAt10" to mapOf(
                                "actual" to 81,
                                "target" to 120,
                                "targetLabel" to "Patch 26.1+ passive wave spawn total by 10:00 (114 Top/Bot, 120 Mid; excludes pets/wards/objectives)"
                            )
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
                prompt shouldContain "cs10.target"
                prompt shouldContain "114 for Top/Bot, 120 for Mid on verified patch 26.1+ games only"
                prompt shouldContain "NEVER subtract `actual` from `target` to claim exact \"missed CS\""
                prompt shouldContain "laneCs@10=81"
                prompt shouldContain "cs10.target=120"
                prompt shouldContain "avgLaneCsAt10: {avgActual=81.0 (matchCount=1), avgTarget=120.0 (targetMatchCount=1, avgSupportedActual=81.0)"
                prompt shouldContain "missingLaneCsAt10Matches: 0"
                prompt shouldNotContain "cs10.spawnedSupply"
                prompt shouldNotContain "cs10.supplyLabel"

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
                                "target" to 120,
                                "targetLabel" to "Patch 26.1+ passive wave spawn total by 10:00 (114 Top/Bot, 120 Mid; excludes pets/wards/objectives)"
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
                            "target" to 120,
                            "targetLabel" to "S16 patch 26.1+ (Faster Minions: 120 spawned by 10:00)"
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

                // 1. Hard size budget (strictly under MAX_PROMPT_BYTES = 16,000 bytes, vs old ~46.7k bytes)
                (groundTruthBytes <= AiAnalysisService.MAX_GROUND_TRUTH_BYTES) shouldBe true
                (promptBytes <= AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
                // User question is preserved verbatim (never silently truncated)
                prompt shouldContain "User: $userQuestion"

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

                // 4. Redundant earlyDeaths fields stripped & targetLabel emitted at most once
                prompt shouldNotContain "participants="
                prompt shouldNotContain "assistantsCount="
                Regex("cs10\\.targetLabel=").findAll(prompt).count() shouldBe 1
                prompt shouldNotContain "cs10.spawnedSupply"
                prompt shouldNotContain "cs10.supplyLabel"

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
                val stressedHeuristicTokens = AiAnalysisService.heuristicTokenEstimate(stressedPrompt)
                println("MEASURED_STRESSED_PROMPT_BYTES: stressedBytes=$stressedBytes, stressedHeuristicTokens=$stressedHeuristicTokens, maxAllowed=${AiAnalysisService.MAX_PROMPT_BYTES}")
                (stressedBytes <= AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
                stressedPrompt shouldContain "User: $nearLimitQuestion"
            }
        }

        When("building a prompt with all 5 structured improvementStats evidence groups and mixed role/patch/short-game denominators") {
            Then("it renders phaseFarming, leadRetention, deathCost, unspentGold, and teamObjectives with exact denominators, missingness counts, and interpretation guardrails within budget") {
                val contextWithImprovementStats = mapOf(
                    "summonerName" to "EvidenceTester#NA1",
                    "rank" to "DIAMOND IV (22 LP)",
                    "primaryRole" to "MIDDLE",
                    "totalGamesAnalyzed" to 10,
                    "timelineStatus" to "ANALYZED",
                    "avgLaneCsAt10" to mapOf(
                        "avgActual" to 80.5,
                        "matchCount" to 6,
                        "avgTarget" to 118.0,
                        "targetMatchCount" to 4,
                        "avgSupportedActual" to 82.0,
                        "targetLabel" to "Patch 26.1+ passive wave spawn total by 10:00 (114 Top/Bot, 120 Mid; excludes pets/wards/objectives)",
                        "roleBreakdown" to listOf(
                            mapOf("role" to "MIDDLE", "matchCount" to 4, "avgActual" to 82.0, "avgTarget" to 120.0, "targetMatchCount" to 3, "avgSupportedActual" to 83.5),
                            mapOf("role" to "TOP", "matchCount" to 2, "avgActual" to 77.5, "avgTarget" to 114.0, "targetMatchCount" to 1, "avgSupportedActual" to 79.0)
                        )
                    ),
                    "missingLaneCsAt10Matches" to 2,
                    "recentMatches" to listOf(
                        mapOf(
                            "matchId" to "NA1_5001",
                            "patch" to "16.2",
                            "role" to "MIDDLE",
                            "champion" to "Orianna",
                            "win" to true,
                            "kda" to "7/1/8 (15.00)",
                            "csPerMin" to 8.6,
                            "laneCsAt10" to mapOf(
                                "actual" to 88,
                                "target" to 120,
                                "targetLabel" to "Patch 26.1+ passive wave spawn total by 10:00 (114 Top/Bot, 120 Mid; excludes pets/wards/objectives)"
                            )
                        ),
                        mapOf(
                            "matchId" to "NA1_5002",
                            "patch" to "15.24",
                            "role" to "TOP",
                            "champion" to "Gnar",
                            "win" to false,
                            "kda" to "3/4/5 (2.00)",
                            "csPerMin" to 7.4,
                            "laneCsAt10" to mapOf(
                                "actual" to 75,
                                "target" to null,
                                "targetLabel" to null
                            )
                        )
                    ),
                    "improvementStats" to mapOf(
                        "totalGamesConsidered" to 10,
                        "excludedInvalidDurationGames" to 1,
                        "excludedMissingTimelineGames" to 2,
                        "hasTimelineAnalysis" to true,
                        "phaseFarming" to mapOf(
                            "min10To20" to mapOf(
                                "eligibleGames" to 6,
                                "shortGames" to 1,
                                "missingFrameGames" to 0,
                                "avgLaneCsGained" to 68.5,
                                "avgJungleCsGained" to 8.2,
                                "avgLaneCsPerMin" to 6.85,
                                "avgJungleCsPerMin" to 0.82,
                                "avgTotalCsPerMin" to 7.67
                            ),
                            "min20ToEnd" to mapOf(
                                "eligibleGames" to 5,
                                "shortGames" to 2,
                                "missingFrameGames" to 0,
                                "avgLaneCsGained" to 52.0,
                                "avgJungleCsGained" to 12.4,
                                "avgLaneCsPerMin" to 5.4,
                                "avgJungleCsPerMin" to 1.3,
                                "avgTotalCsPerMin" to 6.7
                            )
                        ),
                        "leadRetention" to mapOf(
                            "missingOpponentGames" to 1,
                            "shortBefore10" to 1,
                            "shortBefore20" to 2,
                            "eligibleGamesAt10" to 6,
                            "eligibleGamesAt20" to 5,
                            "avgGoldLeadAt10" to 285.0,
                            "avgXpLeadAt10" to 190.0,
                            "avgGoldLeadAt20" to 410.0,
                            "avgXpLeadAt20" to 310.0,
                            "aheadGold10" to 4,
                            "aheadGold10Reach20" to 3,
                            "retainedGold20" to 2,
                            "goldLeadRetentionRate" to 0.667,
                            "aheadXp10" to 4,
                            "aheadXp10Reach20" to 3,
                            "retainedXp20" to 3,
                            "xpLeadRetentionRate" to 1.0,
                            "allyObj10To20" to 5,
                            "enemyObj10To20" to 2,
                            "allyTowers10To20" to 4,
                            "enemyTowers10To20" to 1
                        ),
                        "deathCost" to mapOf(
                            "eligibleTimeDeadGames" to 9,
                            "missingTimeDeadGames" to 0,
                            "totalTimeSpentDeadSeconds" to 810,
                            "avgTimeSpentDeadSeconds" to 90.0,
                            "avgDeadTimeGameSharePct" to 5.2,
                            "overallDeadTimeGameSharePct" to 5.1,
                            "bounty" to mapOf(
                                "eligibleTimelineGames" to 7,
                                "totalRecordedDeaths" to 21,
                                "deathsWithAnyBountyData" to 19,
                                "deathsMissingBountyData" to 2,
                                "totalBaseKillBountyGold" to 5400,
                                "totalShutdownBountyGold" to 1350,
                                "totalCombinedBountyGold" to 6750,
                                "avgCombinedBountyPerDeath" to 355.3
                            )
                        ),
                        "unspentGold" to mapOf(
                            "eligibleGames" to 7,
                            "missingSampleGames" to 0,
                            "sampleCount" to 182,
                            "medianCurrentGold" to 620.0,
                            "maxCurrentGold" to 2450,
                            "avgCurrentGold" to 740.5,
                            "highBalanceThresholdGold" to 1600,
                            "highBalanceSampleCount" to 9,
                            "highBalancePrePurchaseCount" to 6,
                            "prePurchaseSamples" to listOf(
                                mapOf(
                                    "matchId" to "NA1_5001",
                                    "championName" to "Orianna",
                                    "minute" to 14,
                                    "currentGold" to 2150,
                                    "nextPurchaseMinute" to 15,
                                    "nextPurchaseItemId" to 3157,
                                    "nextPurchaseItemName" to "Zhonya's Hourglass"
                                )
                            )
                        ),
                        "teamObjectives" to mapOf(
                            "eligibleTeamObjectiveGames" to 9,
                            "missingTeamsGames" to 0,
                            "timelineEligibleGames" to 7,
                            "tower" to mapOf("eligibleGames" to 9, "missingGames" to 0, "firstCount" to 5, "firstRate" to 0.556, "teamKills" to 54, "teamAvg" to 6.0, "enemyAvg" to 4.8, "personalLh" to 11, "personalTd" to 24, "timelineInvolved" to 19),
                            "dragon" to mapOf("eligibleGames" to 9, "missingGames" to 0, "firstCount" to 4, "firstRate" to 0.444, "teamKills" to 21, "teamAvg" to 2.33, "enemyAvg" to 1.89, "personalLh" to 2, "personalTd" to null, "timelineInvolved" to 14),
                            "baron" to mapOf("eligibleGames" to 9, "missingGames" to 0, "firstCount" to 6, "firstRate" to 0.667, "teamKills" to 7, "teamAvg" to 0.78, "enemyAvg" to 0.44, "personalLh" to 1, "personalTd" to 5, "timelineInvolved" to 6),
                            "riftHerald" to mapOf("eligibleGames" to 9, "missingGames" to 0, "firstCount" to 5, "firstRate" to 0.556, "teamKills" to 5, "teamAvg" to 0.56, "enemyAvg" to 0.44, "personalLh" to 1, "personalTd" to 4, "timelineInvolved" to 4),
                            "horde" to mapOf("eligibleGames" to 9, "missingGames" to 0, "firstCount" to 6, "firstRate" to 0.667, "teamKills" to 28, "teamAvg" to 3.11, "enemyAvg" to 2.22, "personalLh" to null, "personalTd" to null, "timelineInvolved" to 12)
                        )
                    )
                )

                val prompt = constructPromptMethod.invoke(
                    service,
                    contextWithImprovementStats,
                    emptyList<Map<String, String>>(),
                    "Review my mid-game farming, lead retention, death cost, unspent gold, and objective participation",
                    null,
                    null
                ) as String

                // Reconciled laneCsAt10 contract
                prompt shouldContain "avgLaneCsAt10: {avgActual=80.5 (matchCount=6), avgTarget=118.0 (targetMatchCount=4, avgSupportedActual=82.0)"
                prompt shouldContain "roleBreakdown=[MIDDLE(actual=82.0 in 4g, target=120.0 in 3g, supportedActual=83.5); TOP(actual=77.5 in 2g, target=114.0 in 1g, supportedActual=79.0)]"
                prompt shouldContain "missingLaneCsAt10Matches: 2"
                prompt shouldContain "cs10.target=120"
                prompt shouldContain "cs10.target=null"
                prompt shouldContain "cs10.targetLabel=Patch 26.1+ passive wave spawn total by 10:00"
                prompt shouldNotContain "cs10.spawnedSupply"

                // All 5 improvementStats groups + denominators + missingness
                prompt shouldContain "5-STAT IMPROVEMENT EVIDENCE (considered=10g, excludedInvalidDuration=1g, excludedMissingTimeline=2g, hasTimelineAnalysis=true)"
                prompt shouldContain "Phase Farming (Post-Lane CS): 10-20m=[eligible=6g (short=1g, missingFrame=0g), avgTotalCsPerMin=7.67"
                prompt shouldContain "20m-End=[eligible=5g (short=2g, missingFrame=0g), avgTotalCsPerMin=6.7"
                prompt shouldContain "Lead Retention (10m->20m vs role opponent): eligible@10m=6g, eligible@20m=5g (missingOpponent=1g, short<10m=1g, short<20m=2g)"
                prompt shouldContain "goldRetention=2/3g reaching 20m (ahead@10m=4g, rate=0.667)"
                prompt shouldContain "Death Cost (Time Dead & Direct Kill Bounty Only): eligibleTimeDead=9g (missingTimeDead=0g), totalDead=810s, avgDeadPerGame=90.0s"
                prompt shouldContain "bounty(eligibleTimeline=7g, recordedDeaths=21, withBounty=19, missingBounty=2, baseGold=5400, shutdownGold=1350, combinedGold=6750"
                prompt shouldContain "Unspent Gold (1-min checkpoint currentGold, NOT recall events): eligible=7g (missingSamples=0g, checkpoints=182)"
                prompt shouldContain "prePurchaseSamples=[Orianna @14m (2150g -> bought Zhonya's Hourglass @15m)]"
                prompt shouldContain "Team Objectives (team totals vs personal credits): eligibleTeam=9g (missingTeams=0g, timelineEligible=7g)"
                prompt shouldContain "tower(elig=9g, miss=0g, first=5/9 [0.556]"
                prompt shouldContain "voidgrubs(elig=9g, miss=0g, first=6/9 [0.667]"
                (AiAnalysisService.utf8ByteSize(prompt) <= AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
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
                syncResult.response shouldContain "${AiAnalysisService.MAX_USER_MESSAGE_BYTES} bytes"
                syncResult.response shouldContain "---SUGGESTIONS---"

                // Public analyzeStream() also emits the explicit safe refusal without calling Gemini
                val streamResult = service.analyzeStream(
                    context = mapOf("summonerName" to "TestPlayer#NA1"),
                    messages = emptyList(),
                    userMessage = oversizedQuestion
                ).chunks.collectList().block()!!.joinToString("")
                streamResult shouldContain "exceeds the safe coaching context budget"
                streamResult.shouldContain("${AiAnalysisService.MAX_USER_MESSAGE_BYTES} bytes")
            }
        }

        When("building a prompt for Spear Shot cached profile where rankedRecord (1020W 884L = 1904G, 53.6%), savedHistory (462W 421L = 883G, 52.3%), and recentMatchSample (11W 9L = 20G, 55.0%) have different denominators (#34)") {
            Then("it renders each sample with its own explicit scope and denominator, rejects unscoped scalar conflation on both new and legacy payloads, handles absent/mismatched/zero samples, and preserves atomic sample labels under budget truncation") {
                val buildGroundTruthMethod = AiAnalysisService::class.java.getDeclaredMethod(
                    "buildGroundTruthSection",
                    Map::class.java,
                    String::class.java,
                    String::class.java,
                    Int::class.javaPrimitiveType
                ).apply { isAccessible = true }

                val buildConfigMethod = AiAnalysisService::class.java.getDeclaredMethod(
                    "buildGenerationConfig",
                    Double::class.javaPrimitiveType
                ).apply { isAccessible = true }

                val spearShotMatches = (0 until 20).map { idx ->
                    mapOf(
                        "matchId" to "EUW1_${8000 + idx}",
                        "patch" to "26.1",
                        "role" to "TOP",
                        "side" to if (idx % 2 == 0) "Blue" else "Red",
                        "champion" to "Pantheon",
                        "win" to (idx < 11),
                        "kda" to "6/4/7 (3.25)",
                        "csPerMin" to 7.1
                    )
                }

                val spearShotScopedContext = mapOf<String, Any>(
                    "summonerName" to "Spear Shot#EUW",
                    "rank" to "CHALLENGER I (1120 LP)",
                    "rankedRecord" to mapOf(
                        "wins" to 1020,
                        "losses" to 884,
                        "games" to 1904,
                        "winRate" to "53.6"
                    ),
                    "savedHistory" to mapOf(
                        "wins" to 462,
                        "losses" to 421,
                        "games" to 883,
                        "winRate" to "52.3"
                    ),
                    "recentMatchSample" to mapOf(
                        "games" to 20,
                        "wins" to 11,
                        "losses" to 9,
                        "winRate" to "55.0"
                    ),
                    "primaryRole" to "TOP",
                    "totalGamesAnalyzed" to 20,
                    "timelineStatus" to "NOT_ANALYZED",
                    "kda" to "2.21",
                    "avgKills" to "5.7",
                    "avgDeaths" to "4.9",
                    "avgAssists" to "5.2",
                    "avgCsPerMin" to "7.1",
                    "avgKillParticipation" to "54.0",
                    "avgSoloKills" to "1.2",
                    "avgTurretPlates" to "2.1",
                    "avgVisionScore" to "21.0",
                    "blueSide" to mapOf("games" to 440, "wins" to 240, "losses" to 200, "winRate" to "54.5"),
                    "redSide" to mapOf("games" to 443, "wins" to 222, "losses" to 221, "winRate" to "50.1"),
                    "topChampions" to listOf(
                        mapOf("name" to "Pantheon", "games" to 790, "wins" to 418, "losses" to 372, "winRate" to "53", "kda" to "2.21")
                    ),
                    "opponentStats" to mapOf("avgKda" to "2.10", "avgCsPerMin" to "6.9"),
                    "recentMatches" to spearShotMatches
                )

                val prompt = constructPromptMethod.invoke(
                    service,
                    spearShotScopedContext,
                    emptyList<Map<String, String>>(),
                    "What is my overall win rate and how am I performing?",
                    null,
                    null
                ) as String

                // 1. Explicit scoped sample records with distinct denominators
                prompt shouldContain "rankedRecord (Solo/Duo Queue Counter): 1020W 884L across 1904 games (53.6% WR)"
                prompt shouldContain "savedHistory (Saved Match History): 462W 421L across 883 games (52.3% WR)"
                prompt shouldContain "recentMatchSample (Detailed Match List): 11W 9L across 20 games (55.0% WR)"
                prompt shouldContain "savedHistoryAverages [sample: savedHistory (883g)]: kda=2.21"
                prompt shouldContain "savedHistorySideSplits [sample: savedHistory (883g)]: blueSide=240W 200L (440g, 54.5% WR), redSide=222W 221L (443g, 50.1% WR)"
                prompt shouldContain "topChampions: Pantheon (790g, 53% WR, 2.21 KDA, 418W 372L) [sample: savedHistory (883g)]"
                prompt shouldContain "opponentStats: {avgKda=2.10, avgCsPerMin=6.9} [sample: savedHistory (883g)]"
                prompt shouldContain "Strict Sample Scope & Denominator Separation (`rankedRecord` vs. `savedHistory` vs. `recentMatchSample`)"

                // 2. Never emit unscoped scalar lines or pair 52.3% with 1904 games on the same line
                prompt shouldNotContain "\n  totalWins:"
                prompt shouldNotContain "\n  totalLosses:"
                prompt shouldNotContain "\n  totalGamesAnalyzed:"
                prompt shouldNotContain "\n  winRate:"
                prompt shouldNotContain "\n  blueSideWinRate:"
                prompt shouldNotContain "\n  redSideWinRate:"
                prompt.lines().any { line ->
                    line.contains("52.3") && (line.contains("1904") || line.contains("1,904"))
                } shouldBe false

                // 3. Legacy unscoped Spear Shot payload (totalWins=1020, totalLosses=884, winRate="52.3", totalGamesAnalyzed=20)
                // must compute 53.6% for rankedRecord (1020/1904) and mark savedHistory as unavailable (never pairing 52.3% with 1904)
                val legacyUnscopedSpearShot = mapOf<String, Any>(
                    "summonerName" to "Spear Shot#EUW",
                    "rank" to "CHALLENGER I (1120 LP)",
                    "totalWins" to 1020,
                    "totalLosses" to 884,
                    "winRate" to "52.3",
                    "totalGamesAnalyzed" to 20,
                    "timelineStatus" to "NOT_ANALYZED",
                    "recentMatches" to spearShotMatches
                )
                val legacyPrompt = constructPromptMethod.invoke(
                    service,
                    legacyUnscopedSpearShot,
                    emptyList<Map<String, String>>(),
                    "What is my overall win rate?",
                    null,
                    null
                ) as String
                legacyPrompt shouldContain "rankedRecord (Solo/Duo Queue Counter): 1020W 884L across 1904 games (53.6% WR)"
                legacyPrompt shouldContain "savedHistory (Saved Match History): unavailable (missing W/L counter; do NOT use rankedRecord)"
                legacyPrompt shouldContain "recentMatchSample (Detailed Match List): 11W 9L across 20 games (55.0% WR)"
                legacyPrompt shouldNotContain "52.3"

                // 4. Absent, zero-game, or counter-mismatched sample handling (never derives savedHistory denominator from rankedRecord or recentMatches)
                val mismatchedAndAbsentContext = mapOf<String, Any?>(
                    "summonerName" to "EdgeCase#EUW",
                    "rank" to "Unranked",
                    "rankedRecord" to mapOf("wins" to 0, "losses" to 0, "games" to 0, "winRate" to "0.0"),
                    "savedHistory" to mapOf("wins" to 462, "losses" to 421, "games" to 1904, "winRate" to "52.3"),
                    "recentMatchSample" to mapOf("games" to 0, "wins" to 0, "losses" to 0, "winRate" to null),
                    "kda" to "3.50",
                    "topChampions" to listOf(mapOf("name" to "Pantheon", "games" to 10, "winRate" to "60", "kda" to "3.50"))
                )
                val edgePrompt = constructPromptMethod.invoke(
                    service,
                    mismatchedAndAbsentContext,
                    emptyList<Map<String, String>>(),
                    "Check my stats",
                    null,
                    null
                ) as String
                edgePrompt shouldContain "rankedRecord (Solo/Duo Queue Counter): unavailable (unranked or no valid ranked W/L counter)"
                edgePrompt shouldContain "savedHistory (Saved Match History): unavailable (missing W/L counter; do NOT use rankedRecord)"
                edgePrompt shouldContain "recentMatchSample (Detailed Match List): unavailable (0 detailed matches loaded)"
                edgePrompt shouldNotContain "savedHistoryAverages ["
                edgePrompt shouldNotContain "topChampions: Pantheon"

                // 5. Truncation safety: at minimum cascade budget (256B) and tighter (160B), no line ever has an orphaned WR without its sample label and denominator
                val gtAt256 = buildGroundTruthMethod.invoke(
                    service,
                    spearShotScopedContext,
                    "What is my overall win rate?",
                    "general",
                    256
                ) as String
                (AiAnalysisService.utf8ByteSize(gtAt256) <= 256) shouldBe true
                gtAt256 shouldContain "rankedRecord (Solo/Duo Queue Counter): 1020W 884L across 1904 games (53.6% WR)"
                gtAt256 shouldContain "savedHistory (Saved Match History): 462W 421L across 883 games (52.3% WR)"
                gtAt256 shouldContain "recentMatchSample (Detailed Match List): 11W 9L across 20 games (55.0% WR)"

                val gtAt160 = buildGroundTruthMethod.invoke(
                    service,
                    spearShotScopedContext,
                    "What is my overall win rate?",
                    "general",
                    160
                ) as String
                (AiAnalysisService.utf8ByteSize(gtAt160) <= 160) shouldBe true
                gtAt160.lines().forEach { line ->
                    if (line.contains("% WR")) {
                        (line.contains("rankedRecord") || line.contains("savedHistory") || line.contains("recentMatchSample")) shouldBe true
                        line shouldContain "games"
                    }
                }

                // 6. Verify MAX_PROMPT_BYTES = 16_000 and maxOutputTokens = 4096 remain unchanged
                AiAnalysisService.MAX_PROMPT_BYTES shouldBe 16_000
                @Suppress("UNCHECKED_CAST")
                val genConfig = buildConfigMethod.invoke(service, 0.7) as Map<String, Any>
                genConfig["maxOutputTokens"] shouldBe 4096
            }
        }
    }

    Given("Issue #38 natural player-language sample naming without losing separate game counts/win rates, causality limits, or invited-roast behavior") {
        val service = createService()
        val constructPromptMethod = AiAnalysisService::class.java.getDeclaredMethod(
            "constructPrompt",
            Map::class.java,
            List::class.java,
            String::class.java,
            AiAnalysisService.IntentResult::class.java,
            PromptVersion::class.java
        ).apply { isAccessible = true }

        val spearShotMatches = (0 until 20).map { idx ->
            mapOf(
                "matchId" to "EUW1_${9000 + idx}",
                "patch" to "26.1",
                "role" to "TOP",
                "side" to if (idx % 2 == 0) "Blue" else "Red",
                "champion" to "Pantheon",
                "win" to (idx < 11),
                "kda" to "6/4/7 (3.25)",
                "csPerMin" to 7.1
            )
        }

        val spearShotContext = mapOf<String, Any>(
            "summonerName" to "Spear Shot#EUW",
            "rank" to "CHALLENGER I (1120 LP)",
            "rankedRecord" to mapOf("wins" to 1020, "losses" to 884, "games" to 1904, "winRate" to "53.6"),
            "savedHistory" to mapOf("wins" to 462, "losses" to 421, "games" to 883, "winRate" to "52.3"),
            "recentMatchSample" to mapOf("games" to 20, "wins" to 11, "losses" to 9, "winRate" to "55.0"),
            "primaryRole" to "TOP",
            "totalGamesAnalyzed" to 20,
            "timelineStatus" to "NOT_ANALYZED",
            "kda" to "2.21",
            "avgCsPerMin" to "7.1",
            "avgTurretPlates" to "2.1",
            "topChampions" to listOf(
                mapOf("name" to "Pantheon", "games" to 790, "wins" to 418, "losses" to 372, "winRate" to "53", "kda" to "2.21")
            ),
            "recentMatches" to spearShotMatches
        )

        When("building prompts for normal coaching, win-rate comparisons, missing timelines, suggested follow-ups, and invited roasts") {
            Then("it enforces natural sample phrasing, preserves separate denominators/rates (#34), explains missing timelines in plain words, filters schema-leaking few-shot examples, and preserves causality and invited-roast rules") {
                clearMocks(exampleRepository, patternRepository)
                every {
                    exampleRepository.findByIsActiveTrueAndQuestionCategoryOrderByQualityScoreDesc(any(), any())
                } returns listOf(
                    AiLearnedExample(
                        questionCategory = "general",
                        userQuestion = "How has my Pantheon looked lately?",
                        aiResponse = "In `recentMatchSample` you went 11W 9L, while `savedHistory` shows 52.3% WR (`timelineStatus` is `NOT_ANALYZED`).",
                        qualityScore = BigDecimal("95.0")
                    ),
                    AiLearnedExample(
                        questionCategory = "general",
                        userQuestion = "What habit should I track next on Pantheon?",
                        aiResponse = "Across your last 20 detailed matches (55.0% win rate), check whether early river deaths happen before your reset timer.",
                        qualityScore = BigDecimal("91.0")
                    )
                )
                every {
                    patternRepository.findByTriggerCategoryAndIsActiveTrue(any())
                } returns listOf(
                    DiscoveredPattern(
                        triggerCategory = "general",
                        followUpCategory = "wave_management",
                        occurrenceCount = 12,
                        confidenceScore = 0.68
                    )
                )

                val normalCoachingPrompt = constructPromptMethod.invoke(
                    service,
                    spearShotContext,
                    emptyList<Map<String, String>>(),
                    "Give me two practical Pantheon improvement habits for my next five games.",
                    null,
                    null
                ) as String

                val winRateComparisonPrompt = constructPromptMethod.invoke(
                    service,
                    spearShotContext,
                    emptyList<Map<String, String>>(),
                    "Why does my ranked win rate differ from my saved matches and my last 20 games?",
                    null,
                    null
                ) as String

                val missingTimelinePrompt = constructPromptMethod.invoke(
                    service,
                    spearShotContext,
                    emptyList<Map<String, String>>(),
                    "What were my exact 10-minute gold diff and first-item completion minute?",
                    null,
                    null
                ) as String

                val invitedRoastPrompt = constructPromptMethod.invoke(
                    service,
                    spearShotContext,
                    emptyList<Map<String, String>>(),
                    "Roast my Pantheon stats playfully, then give me one controllable habit to review next game.",
                    null,
                    null
                ) as String

                for (p in listOf(normalCoachingPrompt, winRateComparisonPrompt, missingTimelinePrompt, invitedRoastPrompt)) {
                    // 1. Old #34 literal-key citation instruction is removed; natural 3-sample mapping is present
                    p shouldNotContain "Always cite the exact sample name and denominator"
                    p shouldContain "Natural Player Language (No Internal Keys or Backticks)"
                    p shouldContain "(1) `rankedRecord` (\"your ranked record\": Solo/Duo W/L, games, and win rate)"
                    p shouldContain "(2) `savedHistory` (\"your saved matches\", with the saved-match count when relevant and always paired with saved-match W/L or win rate"
                    p shouldContain "(3) `recentMatchSample` (\"your last 20 detailed matches\" / \"your last N detailed matches\", with its own games and win rate"

                    // 2. Separate denominators & win rates from #34 remain completely intact
                    p shouldContain "rankedRecord (Solo/Duo Queue Counter): 1020W 884L across 1904 games (53.6% WR)"
                    p shouldContain "savedHistory (Saved Match History): 462W 421L across 883 games (52.3% WR)"
                    p shouldContain "recentMatchSample (Detailed Match List): 11W 9L across 20 games (55.0% WR)"
                    p shouldContain "NEVER combine samples, remove counts from win-rate/record claims, or pair one sample's win rate with another sample's game count"

                    // 3. Missing timeline & follow-up suggestions forbid internal keys, schema markers, NOT_ANALYZED, and backticks
                    p shouldContain "Timeline (timeline not analyzed yet)"
                    p shouldNotContain "Timeline Status (NOT_ANALYZED)"
                    p shouldContain "in plain language (never `timelineStatus` or `NOT_ANALYZED`)"
                    p shouldContain "plain player language—no raw keys, `[sample: ...]`, `NOT_ANALYZED`, or backticks; never mention Mythic items/paths or unsupported build-effect claims in suggestions"

                    // 4. Evidence/causality rules & invited-roast openness remain intact (no roast-suppression added)
                    p shouldContain "Observed Facts vs. Hypotheses & Conditional Recommendations"
                    p shouldContain "Deaths & Assist Attribution"
                    p shouldNotContain "never roast"
                    p shouldNotContain "refuse to roast"

                    // 5. Hard prompt budget invariant preserved
                    (AiAnalysisService.utf8ByteSize(p) <= AiAnalysisService.MAX_PROMPT_BYTES) shouldBe true
                }

                // 6. Pre-#38 few-shot example exposing `recentMatchSample` / `savedHistory` / `NOT_ANALYZED` is filtered out,
                // while the clean player-language example and natural suggestion pattern guidance remain included
                normalCoachingPrompt shouldNotContain "In `recentMatchSample` you went 11W 9L"
                normalCoachingPrompt shouldContain "Across your last 20 detailed matches (55.0% win rate), check whether early river deaths happen before your reset timer."
                normalCoachingPrompt shouldContain "do not echo raw category keys"

                // 7. Zero-match fallback uses natural plural phrasing rather than "your last 0 detailed matches"
                val zeroMatchesPrompt = constructPromptMethod.invoke(
                    service,
                    mapOf(
                        "summonerName" to "NewPlayer#EUW",
                        "rank" to "Unranked",
                        "recentMatchSample" to mapOf("games" to 0, "wins" to 0, "losses" to 0)
                    ),
                    emptyList<Map<String, String>>(),
                    "Analyze my profile",
                    null,
                    null
                ) as String
                zeroMatchesPrompt shouldContain "\"your recent detailed matches\""
                zeroMatchesPrompt shouldNotContain "your last 0 detailed matches"

                clearMocks(exampleRepository, patternRepository)
            }
        }

        When("checking player-facing coaching prose and follow-up suggestions with containsInternalSchemaLeak") {
            Then("it flags internal sample keys, schema markers, header labels, and NOT_ANALYZED while passing natural player-facing coaching and suggestions") {
                val leakyOutputs = listOf(
                    "Across your `recentMatchSample` of 20 games you have a 55.0% win rate.",
                    "Your savedHistory sits at 52.3% across 883 matches, while rankedRecord is 53.6%.",
                    "Your Pantheon averages 2.21 KDA [sample: savedHistory (883g)].",
                    "Because timelineStatus is NOT_ANALYZED, item spike minutes are unavailable.",
                    "---SUGGESTIONS--- [\"Compare recentMatchSample to savedHistory\", \"Check `topChampions`\", \"Why is timeline NOT_ANALYZED?\"]"
                )
                leakyOutputs.forEach { sample ->
                    AiAnalysisService.containsInternalSchemaLeak(sample) shouldBe true
                }

                val cleanOutputs = listOf(
                    "In **your ranked record**, you sit at **53.6% win rate** across **1,904 games** (1,020W–884L), compared with **52.3%** across **883 saved matches** and **55.0%** across **your last 20 detailed matches** (11W–9L).\n---SUGGESTIONS--- [\"How do my Blue and Red side win rates compare?\", \"What two Pantheon habits should I track next?\", \"How should I play early river skirmishes?\"]",
                    "Match timeline data hasn't been analyzed yet, so only end-of-game summary and challenge stats are available—exact 10-minute gold diffs and item completion minutes aren't loaded."
                )
                cleanOutputs.forEach { sample ->
                    AiAnalysisService.containsInternalSchemaLeak(sample) shouldBe false
                }
            }
        }
    }
})


