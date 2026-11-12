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
            Then("it automatically creates and activates a boundary-clean challenger PromptVersion for that category") {
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
                    arrayOf<Any>("cs_farming", 16L, 22L) // 72.7% negative rate
                )
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("cs_farming") } returns emptyList()

                val savedVersion = slot<PromptVersion>()
                every { promptVersionRepository.save(capture(savedVersion)) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                verify(atLeast = 1) { promptVersionRepository.save(any<PromptVersion>()) }
                savedVersion.captured.categoryScope shouldBe "cs_farming"
                savedVersion.captured.isActive shouldBe true
                savedVersion.captured.isChallenger shouldBe true
                AiAnalysisService.isEligiblePromptVersionDirective(savedVersion.captured.promptTemplate) shouldBe true
            }
        }
    }

    Given("Nov 11 Coach Quality example curation gates and legacy eviction") {
        When("curating candidates containing schema leaks, CS@10 mislabeling, boundary violations, bloated answers, and a concise clean answer at category cap 10") {
            Then("it rejects every poisoned/bloated candidate, scores the concise clean answer >80, and evicts a legacy boundary-violating active example first") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )

                fun mkCandidate(question: String, answer: String): AiInteraction {
                    val id = UUID.randomUUID()
                    val interaction = AiInteraction(
                        id = id,
                        summonerName = "Spear Shot#EUW",
                        summonerRank = "CHALLENGER I",
                        summonerRole = "cs_farming",
                        userQuestion = question,
                        aiResponse = answer
                    )
                    every { feedbackRepository.findByInteractionId(id) } returns Optional.of(
                        AiFeedback(
                            id = UUID.randomUUID(),
                            interaction = interaction,
                            feedbackType = "positive",
                            engagementTimeMs = 18_000L,
                            isValidated = true
                        )
                    )
                    return interaction
                }

                val badCs10Target = mkCandidate(
                    "How is my Pantheon 10-minute CS?",
                    "Across your last 20 games you averaged 76.2 CS at 10:00 against the 114 Top lane target, meaning you missed 37.8 CS before 10 minutes. Practice last-hitting drills.\n---SUGGESTIONS--- [\"Check wave states\", \"Review resets\", \"Compare matchups\"]"
                )
                val badVoidgrubAndAbility = mkCandidate(
                    "What are my Pantheon weaknesses?",
                    "Your 35.0% first-Voidgrub rate is a glaring weakness and shows you take low-percentage fights with Aegis of Zealous. Try resetting earlier next game.\n---SUGGESTIONS--- [\"Review river timers\", \"Check runes\", \"Compare top matchups\"]"
                )
                val bloatedNonExpanded = mkCandidate(
                    "Quick check on my CS?",
                    ("Across your last 20 detailed matches (55.0% win rate), you averaged 7.1 CS/min and 76.2 lane CS at 10:00. Focus on crashing the wave before resetting so you do not lose farm during river rotations. ").repeat(6)
                )
                val badReversedPassivePolarity = mkCandidate(
                    "Roast my Pantheon laning!",
                    "Sequentially verify that your empowered passive and enemy core mobility cooldowns are down before stepping across the wave threshold.\n---SUGGESTIONS--- [\"Check trades\", \"Review resets\", \"Compare matchups\"]"
                )
                val badAggregateCausalDiagnosis = mkCandidate(
                    "Roast my Pantheon stats!",
                    "Across your 883 saved matches, your 2.21 KDA and 4.9 deaths point to a persistent issue with over-forcing trades when your spacing and cooldown windows don't support the engage. Next game, eliminate unassisted single-enemy kill credit deaths.\n---SUGGESTIONS--- [\"Check deaths\", \"Review trades\", \"Compare side splits\"]"
                )
                val badRawSchemaIdentifierLeak = mkCandidate(
                    "How is my win rate looking?",
                    "Across recentMatchSample you went 11W-9L (55.0%), compared with savedHistory (52.3%) and rankedRecord (53.6%). Next game, track your pre-6 wave crash.\n---SUGGESTIONS--- [\"Check builds\", \"Review matchups\", \"Compare sides\"]"
                )
                val bad145WordSpatialAssociations = mkCandidate(
                    "How am I doing overall?",
                    "In your ranked record this split you sit at a 53.6% win rate across 1,904 games (1,020W–884L), compared with 52.3% across your 883 saved matches (462W–421L) and 55.0% across your last 20 detailed matches (11W–9L). Across 14 eligible Top lane games you average 76.2 lane CS at 10:00, where 114 is the Patch 26.1+ theoretical wave ceiling rather than a target, while your team's 36.8% first-Voidgrub rate across 19 games reflects five-player map states rather than solo performance. Across 20 analyzed timelines, your 18 pre-10 deaths and 14 objective-window deaths in Top Side River are spatial timeline associations only and cannot prove what started each skirmish. Next game, focus on two observable habits: first, check the minimap for the enemy jungler before walking into river while keeping E ready; second, crash your top wave fully before channeling R.\n---SUGGESTIONS--- [\"Compare sides\", \"Check Aatrox\", \"Review resets\"]"
                )
                val badRoastBleedingTempo = mkCandidate(
                    "Roast my Pantheon stats brutally!",
                    "Across your last 5 detailed matches (1W-4L, 20.0% win rate), you spent 390 seconds on grey screen. You are currently bleeding tempo entirely through repeated deaths rather than clean execution. Next game, crash top wave before channeling R.\n---SUGGESTIONS--- [\"Check deaths\", \"Review R roams\", \"Compare sides\"]"
                )
                val badAngryVarianceExecution = mkCandidate(
                    "Matchmaking is rigged!! Losers queue is ruining my games!",
                    "A 1W-4L stretch across your last 5 detailed matches (20.0% win rate) is frustrating, though high match variance and individual execution drive these outcomes rather than hidden matchmaking systems. Next game, take a short reset break after two losses.\n---SUGGESTIONS--- [\"Compare Darius and Garen\", \"Check wave prio\", \"Review builds\"]"
                )
                val badMissingPlayMoreGames = mkCandidate(
                    "What were my exact grey-screen seconds and high-gold checkpoints?",
                    "Your grey-screen time and high-gold checkpoints are currently unavailable across your last 4 detailed matches, though playing more games may clarify those numbers soon.\n---SUGGESTIONS--- [\"Review 10-minute CS\", \"Check Top builds\", \"Analyze last 4 games\"]"
                )
                val badBroadWaveManipulation = mkCandidate(
                    "How do I stop losing top lane?",
                    "Across your last 5 detailed matches (1W-4L, 20.0% win rate), focus on wave manipulation and stay calm when behind.\n---SUGGESTIONS--- [\"Compare Darius and Garen\", \"Check builds\", \"Review resets\"]"
                )
                val cleanShortDefault = mkCandidate(
                    "How is my early Pantheon farming?",
                    "Across **your last 20 detailed matches** (11W–9L, **55.0%** win rate), you average **76.2 lane CS at 10:00** across **14 Top games** (with **114** as the Patch 26.1+ theoretical maximum passive wave spawn—a ceiling, not a target) and **7.1 CS/min** across **your 883 saved matches** (**52.3%** win rate). Summary stats only support hypotheses about recall states, so next game: (1) **check whether early R roams happen before crashing the wave**, and (2) **track one pre-6 reset on a cannon wave**.\n---SUGGESTIONS--- [\"How do my mid-game CS numbers hold up?\", \"When should Pantheon Ult from top?\", \"Compare my Blue and Red side stats\"]"
                )

                val legacyPoisonedHighScore = AiLearnedExample(
                    id = UUID.randomUUID(),
                    questionCategory = "cs_farming",
                    userQuestion = "Old CS question",
                    aiResponse = "You missed 38 CS against the 114 lane benchmark at 10 minutes.",
                    qualityScore = BigDecimal("99.0"),
                    isActive = true
                )
                val existingCleanLowerScore = AiLearnedExample(
                    id = UUID.randomUUID(),
                    questionCategory = "cs_farming",
                    userQuestion = "Clean existing CS question",
                    aiResponse = "Across your last 20 detailed matches (55.0% win rate), crash the cannon wave before resetting so your next habit stays consistent.",
                    qualityScore = BigDecimal("85.0"),
                    isActive = true
                )

                every { interactionRepository.findCandidatesForLearning(any()) } returns listOf(
                    badCs10Target,
                    badVoidgrubAndAbility,
                    bloatedNonExpanded,
                    badReversedPassivePolarity,
                    badAggregateCausalDiagnosis,
                    badRawSchemaIdentifierLeak,
                    bad145WordSpatialAssociations,
                    badRoastBleedingTempo,
                    badAngryVarianceExecution,
                    badMissingPlayMoreGames,
                    badBroadWaveManipulation,
                    cleanShortDefault
                )
                every { exampleRepository.countByQuestionCategoryAndIsActive("cs_farming", true) } returns 10L
                every {
                    exampleRepository.findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc("cs_farming")
                } returns listOf(existingCleanLowerScore, legacyPoisonedHighScore)

                val savedExamples = mutableListOf<AiLearnedExample>()
                every { exampleRepository.save(capture(savedExamples)) } answers { firstArg<AiLearnedExample>() }
                val savedRun = slot<CurationRun>()
                every { curationRunRepository.save(capture(savedRun)) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns listOf(
                    arrayOf<Any>("trading_laning", 15L, 21L)
                )
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("trading_laning") } returns emptyList()
                val savedTradingChallenger = slot<PromptVersion>()
                every { promptVersionRepository.save(capture(savedTradingChallenger)) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                savedRun.captured.examplesAdded shouldBe 0
                savedRun.captured.examplesReplaced shouldBe 1
                // Legacy poisoned example (even with 99.0 score) is evicted before the clean 85.0 example
                legacyPoisonedHighScore.isActive shouldBe false
                existingCleanLowerScore.isActive shouldBe true
                savedExamples.any { it.userQuestion == "How is my early Pantheon farming?" && it.isActive } shouldBe true
                savedTradingChallenger.captured.categoryScope shouldBe "trading_laning"
                AiAnalysisService.isEligiblePromptVersionDirective(savedTradingChallenger.captured.promptTemplate) shouldBe true
            }
        }
    }
})
