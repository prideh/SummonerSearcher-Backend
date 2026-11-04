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
    }
})
