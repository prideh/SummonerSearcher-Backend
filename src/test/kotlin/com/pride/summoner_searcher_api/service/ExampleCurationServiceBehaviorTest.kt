package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.AiFeedback
import com.pride.summoner_searcher_api.model.AiInteraction
import com.pride.summoner_searcher_api.model.AiLearnedExample
import com.pride.summoner_searcher_api.model.CurationRun
import com.pride.summoner_searcher_api.model.PromptVersion
import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.CurationRunRepository
import com.pride.summoner_searcher_api.repository.PromptVersionRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import java.math.BigDecimal
import java.util.Optional
import java.util.UUID

class ExampleCurationServiceBehaviorTest : BehaviorSpec({
    val interactionRepository = mockk<AiInteractionRepository>(relaxed = true)
    val feedbackRepository = mockk<AiFeedbackRepository>(relaxed = true)
    val exampleRepository = mockk<AiLearnedExampleRepository>(relaxed = true)
    val curationRunRepository = mockk<CurationRunRepository>(relaxed = true)
    val promptVersionRepository = mockk<PromptVersionRepository>(relaxed = true)

    val curationService = ExampleCurationService(
        interactionRepository = interactionRepository,
        feedbackRepository = feedbackRepository,
        exampleRepository = exampleRepository,
        curationRunRepository = curationRunRepository,
        promptVersionRepository = promptVersionRepository
    )

    Given("An active challenger PromptVersion competing against a champion (#11)") {
        When("the challenger has >=20 uses and higher satisfaction than the champion") {
            Then("the challenger is promoted to champion and the previous champion is deactivated") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )
                val champion = PromptVersion(
                    id = UUID.randomUUID(),
                    versionName = "v1.0-itemization",
                    promptTemplate = "Old champion directive",
                    isActive = true,
                    isChallenger = false,
                    categoryScope = "itemization_builds",
                    totalUses = 50,
                    positiveFeedbackCount = 6,
                    negativeFeedbackCount = 6 // 50% satisfaction
                )
                val challenger = PromptVersion(
                    id = UUID.randomUUID(),
                    versionName = "v1.1-itemization-challenger",
                    promptTemplate = "New challenger directive",
                    isActive = true,
                    isChallenger = true,
                    categoryScope = "itemization_builds",
                    totalUses = 25,
                    positiveFeedbackCount = 8,
                    negativeFeedbackCount = 2 // 80% satisfaction
                )

                every { interactionRepository.findCandidatesForLearning(any()) } returns emptyList()
                every { curationRunRepository.save(any<CurationRun>()) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns listOf(challenger)
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("itemization_builds") } returns listOf(champion, challenger)
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()
                every { promptVersionRepository.save(any<PromptVersion>()) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                challenger.isChallenger shouldBe false
                challenger.isActive shouldBe true
                champion.isActive shouldBe false
            }
        }

        When("an underperforming category exceeds 60% negative feedback with no active challenger") {
            Then("it automatically creates and activates a boundary-safe, concise challenger PromptVersion for that category") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )
                every { interactionRepository.findCandidatesForLearning(any()) } returns emptyList()
                every { curationRunRepository.save(any<CurationRun>()) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns listOf(
                    arrayOf<Any>("matchups", 16L, 22L), // 72.7% negative rate
                    arrayOf<Any>("vision_warding", 15L, 20L) // 75.0% negative rate
                )
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope(any()) } returns emptyList()

                val savedVersions = mutableListOf<PromptVersion>()
                every { promptVersionRepository.save(capture(savedVersions)) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                savedVersions.size shouldBe 2
                val matchupsChallenger = savedVersions.first { it.categoryScope == "matchups" }
                matchupsChallenger.isActive shouldBe true
                matchupsChallenger.isChallenger shouldBe true
                matchupsChallenger.promptTemplate shouldContain "Q`/`W`/`E`/`R"
                matchupsChallenger.promptTemplate shouldContain "hypotheses"

                val visionChallenger = savedVersions.first { it.categoryScope == "vision_warding" }
                visionChallenger.promptTemplate shouldContain "timing buckets only"
                visionChallenger.promptTemplate shouldContain "never claim specific bush or map coordinates"
                visionChallenger.promptTemplate shouldNotContain "specify exact warding timings and locations"
            }
        }
    }

    Given("Issue #36 & Nov 10 example curation gates and concise quality scoring") {
        When("curateNewExamples evaluates candidates containing Nov 10 boundary violations, unrequested essays, concise grounded answers, and requested deep dives") {
            Then("it rejects boundary-violating and overly verbose default candidates, scores concise ~65-word grounded answers at 100, accepts expanded answers when requested, and evicts legacy invalid active examples at capacity") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )

                fun makeCandidate(question: String, response: String, category: String = "general"): Pair<AiInteraction, AiFeedback> {
                    val id = UUID.randomUUID()
                    val interaction = AiInteraction(
                        id = id,
                        summonerName = "Nov10Player#EUW",
                        summonerRank = "EMERALD III",
                        summonerRole = category,
                        userQuestion = question,
                        aiResponse = response
                    )
                    val feedback = AiFeedback(
                        id = UUID.randomUUID(),
                        interaction = interaction,
                        feedbackType = "positive",
                        isValidated = true,
                        engagementTimeMs = 20_000
                    )
                    return interaction to feedback
                }

                val badComparison = makeCandidate(
                    "Compare my win rates.",
                    "Your 49% lifetime ranked record across 384 games suggests your overall matchmaking rating has stabilized, while 55% in your last 20 games reflects better synergy and tighter macro. Next game focus on keeping tempo."
                )
                val badRoast = makeCandidate(
                    "Roast me.",
                    "Securing first Voidgrubs in 6 of 20 games and first tower in 8 of 20 is a personal weakness, and 147.05s dead proves you take low-percentage fights. Next game try playing safer."
                )
                val badUnrequestedEssay = makeCandidate(
                    "Quick Pantheon tip?",
                    (1..240).joinToString(" ") { "essayWord$it" } + " 55.0% next game focus on spacing."
                )
                val conciseGrounded = makeCandidate(
                    "Roast my recent Pantheon stats and give me two fixes.",
                    "Across your last 20 detailed matches (11W–9L, 55.0% win rate), you averaged 147.05 seconds on a grey screen per game (7.84% of game time)—nearly two and a half minutes off the map. Your 30% first-Voidgrub and 40% first-tower rates are 5-player team stats rather than a personal weakness, and match timelines aren't analyzed yet. Next game: (1) check wave state before committing W, and (2) spend gold immediately on reset.\n---SUGGESTIONS--- [\"Compare my three win-rate windows\", \"Explain my Pantheon matchups in detail\", \"How do I lower mid-game deaths?\"]",
                    category = "full_analysis"
                )
                val requestedExpanded = makeCandidate(
                    "Explain my Pantheon vs ranged top matchups in detail.",
                    (1..150).joinToString(" ") { "groundedDetail$it" } + " across 20 games (55.0% win rate), hold W and E cooldowns and next game track wave bounce timers.",
                    category = "matchups"
                )

                val allPairs = listOf(badComparison, badRoast, badUnrequestedEssay, conciseGrounded, requestedExpanded)
                every { interactionRepository.findCandidatesForLearning(any()) } returns allPairs.map { it.first }
                allPairs.forEach { (interaction, feedback) ->
                    every { feedbackRepository.findByInteractionId(interaction.id!!) } returns Optional.of(feedback)
                }

                // `full_analysis` has 0 active examples; `matchups` is at capacity (10) with a legacy boundary-violating active example at score 99.0
                every { exampleRepository.countByQuestionCategoryAndIsActive("full_analysis", true) } returns 0L
                every { exampleRepository.countByQuestionCategoryAndIsActive("matchups", true) } returns 10L

                val legacyBadExample = AiLearnedExample(
                    id = UUID.randomUUID(),
                    questionCategory = "matchups",
                    userQuestion = "How do I trade on Pantheon?",
                    aiResponse = "Open with Pantheon's Aegis of Zealous stun and rush Divine Sunderer.",
                    qualityScore = BigDecimal("99.0"),
                    isActive = true
                )
                every {
                    exampleRepository.findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc("matchups")
                } returns listOf(legacyBadExample)

                val savedExamples = mutableListOf<AiLearnedExample>()
                every { exampleRepository.save(capture(savedExamples)) } answers { firstArg<AiLearnedExample>() }
                val savedRun = slot<CurationRun>()
                every { curationRunRepository.save(capture(savedRun)) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()

                curationService.curateNewExamples()

                savedRun.captured.examplesAdded shouldBe 1
                savedRun.captured.examplesReplaced shouldBe 1
                legacyBadExample.isActive shouldBe false

                val activeSaved = savedExamples.filter { it.isActive }
                activeSaved.size shouldBe 2
                val savedConcise = activeSaved.first { it.questionCategory == "full_analysis" }
                savedConcise.qualityScore shouldBe BigDecimal("100")
                val savedExpanded = activeSaved.first { it.questionCategory == "matchups" }
                savedExpanded.qualityScore shouldBe BigDecimal("100")
            }
        }
    }
})
