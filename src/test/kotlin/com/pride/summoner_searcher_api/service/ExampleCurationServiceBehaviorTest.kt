package com.pride.summoner_searcher_api.service

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

        When("underperforming categories exceed 60% negative feedback with no active challenger") {
            Then("it creates boundary-safe, concise challenger PromptVersions whose directives (including cs_farming #39 theoretical-maximum wording) pass all 3 safety filters") {
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
                    arrayOf<Any>("itemization_builds", 15L, 21L),
                    arrayOf<Any>("matchups", 16L, 22L),
                    arrayOf<Any>("cs_farming", 17L, 24L),
                    arrayOf<Any>("vision_warding", 15L, 20L),
                    arrayOf<Any>("macro_rotations", 14L, 20L)
                )
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope(any()) } returns emptyList()

                val savedVersions = mutableListOf<PromptVersion>()
                every { promptVersionRepository.save(capture(savedVersions)) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                savedVersions.size shouldBe 5
                savedVersions.forEach { challenger ->
                    challenger.isActive shouldBe true
                    challenger.isChallenger shouldBe true
                    AiAnalysisService.containsInternalSchemaLeak(challenger.promptTemplate) shouldBe false
                    AiAnalysisService.containsCs10TheoreticalMaxMislabeling(challenger.promptTemplate) shouldBe false
                    AiAnalysisService.containsEvidenceBoundaryViolation(challenger.promptTemplate) shouldBe false
                }

                val csChallenger = savedVersions.first { it.categoryScope == "cs_farming" }
                csChallenger.promptTemplate.contains("theoretical maximum passive wave spawn by 10:00") shouldBe true
                csChallenger.promptTemplate.contains("not a target or benchmark, and never a missed-CS baseline") shouldBe true
                csChallenger.promptTemplate.contains("10:00 benchmark") shouldBe false
                csChallenger.promptTemplate.contains("cs10.target") shouldBe false

                val matchupsChallenger = savedVersions.first { it.categoryScope == "matchups" }
                matchupsChallenger.promptTemplate.contains("Q/W/E/R") shouldBe true
                matchupsChallenger.promptTemplate.contains("hypotheses") shouldBe true

                val visionChallenger = savedVersions.first { it.categoryScope == "vision_warding" }
                visionChallenger.promptTemplate.contains("timing buckets only") shouldBe true
                visionChallenger.promptTemplate.contains("never claim specific bush or map coordinates") shouldBe true
                visionChallenger.promptTemplate.contains("specify exact warding timings and locations") shouldBe false
            }
        }
    }

    Given("Candidate interactions during RLHF curation (#39)") {
        When("candidates include responses that mislabel 114/120 as CS@10 targets, claim missed CS from 114, or substitute 107/70-85 heuristics alongside a clean theoretical-maximum response") {
            Then("it skips every CS@10-mislabeling interaction and curates only the valid coaching example") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )

                val mislabeledTargetCandidate = com.pride.summoner_searcher_api.model.AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "Spear Shot#EUW",
                    summonerRank = "CHALLENGER I",
                    summonerRole = "cs_farming",
                    userQuestion = "Analyze my Pantheon profile",
                    aiResponse = "Your 72.9 CS@10 is well below the 114 Top lane target, making early farming your #1 focus area in your next game. Focus on hitting every wave.\n---SUGGESTIONS--- [\"How do I hit 114 CS@10?\"]"
                )
                val mislabeled107Candidate = com.pride.summoner_searcher_api.model.AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "MidTester#NA1",
                    summonerRank = "DIAMOND II",
                    summonerRole = "cs_farming",
                    userQuestion = "How is my early CS?",
                    aiResponse = "Only 107 lane minions are farmable by 10:00, and a realistic target is 70-85 CS @ 10m. Focus on wave crashes in your next game.\n---SUGGESTIONS--- [\"Review wave crashes\"]"
                )
                val validCandidate = com.pride.summoner_searcher_api.model.AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "Spear Shot#EUW",
                    summonerRank = "CHALLENGER I",
                    summonerRole = "cs_farming",
                    userQuestion = "How should I interpret my 72.9 lane CS at 10 minutes on Pantheon?",
                    aiResponse = "Across **14 Top games** on Patch 26.1, you averaged **72.9 lane CS at 10:00** while winning **57.1%** of those matches; **114** is strictly the theoretical maximum passive wave spawn by 10:00 (not a target or benchmark), so evaluate early farm alongside Pantheon roam timers and trade states. In your next game, focus on crashing cannon waves cleanly before river rotations.\n---SUGGESTIONS--- [\"How do my early Pantheon roams trade off against wave crashes?\", \"Compare my Blue and Red side win rates\", \"Review my recent Pantheon matchups\"]"
                )

                every { interactionRepository.findCandidatesForLearning(any()) } returns listOf(
                    mislabeledTargetCandidate,
                    mislabeled107Candidate,
                    validCandidate
                )
                every { feedbackRepository.findByInteractionId(any()) } answers {
                    val interactionId = firstArg<UUID>()
                    val interaction = listOf(mislabeledTargetCandidate, mislabeled107Candidate, validCandidate)
                        .first { it.id == interactionId }
                    java.util.Optional.of(
                        com.pride.summoner_searcher_api.model.AiFeedback(
                            interaction = interaction,
                            feedbackType = "positive",
                            engagementTimeMs = 20_000L,
                            isValidated = true
                        )
                    )
                }
                every { exampleRepository.countByQuestionCategoryAndIsActive("cs_farming", true) } returns 0L
                val savedExamples = mutableListOf<com.pride.summoner_searcher_api.model.AiLearnedExample>()
                every { exampleRepository.save(capture(savedExamples)) } answers { firstArg() }
                val savedRun = slot<CurationRun>()
                every { curationRunRepository.save(capture(savedRun)) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()

                curationService.curateNewExamples()

                savedExamples.size shouldBe 1
                savedExamples.single().sourceInteraction shouldBe validCandidate
                savedRun.captured.candidatesFound shouldBe 3
                savedRun.captured.examplesAdded shouldBe 1
            }
        }
    }

    Given("Combined Issue #36, #38, #39 & Nov 10 example curation gates and concise quality scoring") {
        When("curateNewExamples evaluates exact recorded Nov 10 roast strings, unrelated-negation MMR claims, schema leaks, unrequested essays, concise grounded answers, and requested deep dives") {
            Then("it rejects every violating or verbose default candidate, scores concise ~65-word grounded answers at 100, accepts expanded answers when requested, and evicts legacy invalid active examples at capacity") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )

                fun makeCandidate(
                    question: String,
                    response: String,
                    category: String = "general"
                ): Pair<com.pride.summoner_searcher_api.model.AiInteraction, com.pride.summoner_searcher_api.model.AiFeedback> {
                    val id = UUID.randomUUID()
                    val interaction = com.pride.summoner_searcher_api.model.AiInteraction(
                        id = id,
                        summonerName = "Nov10Player#EUW",
                        summonerRank = "EMERALD III",
                        summonerRole = category,
                        userQuestion = question,
                        aiResponse = response
                    )
                    val feedback = com.pride.summoner_searcher_api.model.AiFeedback(
                        id = UUID.randomUUID(),
                        interaction = interaction,
                        feedbackType = "positive",
                        isValidated = true,
                        engagementTimeMs = 20_000L
                    )
                    return interaction to feedback
                }

                val badUnrelatedNegationComparison = makeCandidate(
                    "Compare my win rates.",
                    "Even though MMR is hidden and timelines are not analyzed, your 49% ranked record across 384 games suggests your overall matchmaking rating has stabilized, while 55% in 20 games reflects better synergy and tighter macro. Next game focus on tempo."
                )
                val badRecordedNov10Roast = makeCandidate(
                    "Roast me.",
                    "Your kill participation is languishing at 38.5%, paired with a poor 30% first-secure rate on Voidgrubs. Your teammates aren't forcing you to take low-percentage fights. Next game try playing safer."
                )
                val badSchemaLeak = makeCandidate(
                    "How is my recent form?",
                    "In recentMatchSample across 20 games (55.0% win rate), focus on wave crashes next game."
                )
                val badUnrequestedEssay = makeCandidate(
                    "Quick Pantheon tip?",
                    (1..240).joinToString(" ") { "essayWord$it" } + " 55.0% next game focus on spacing."
                )
                val conciseGrounded = makeCandidate(
                    "Roast my recent Pantheon stats and give me two fixes.",
                    "Across your last 20 detailed matches (11W–9L, 55.0% win rate), you averaged 147.05 seconds on a grey screen per game (7.84% of game time)—nearly two and a half minutes off the map. Your team's 30% first-Voidgrub and 40% first-tower rates are 5-player team stats rather than a personal weakness, and match timelines aren't analyzed yet. Next game: (1) check wave state before committing W, and (2) spend gold immediately on reset.\n---SUGGESTIONS--- [\"Compare my three win-rate windows\", \"Explain my Pantheon matchups in detail\", \"How do I lower mid-game deaths?\"]",
                    category = "full_analysis"
                )
                val requestedExpanded = makeCandidate(
                    "Explain my Pantheon vs ranged top matchups in detail.",
                    (1..150).joinToString(" ") { "groundedDetail$it" } + " across 20 games (55.0% win rate), hold W and E cooldowns and next game track wave bounce timers.",
                    category = "matchups"
                )

                val allPairs = listOf(
                    badUnrelatedNegationComparison,
                    badRecordedNov10Roast,
                    badSchemaLeak,
                    badUnrequestedEssay,
                    conciseGrounded,
                    requestedExpanded
                )
                every { interactionRepository.findCandidatesForLearning(any()) } returns allPairs.map { it.first }
                allPairs.forEach { (interaction, feedback) ->
                    every { feedbackRepository.findByInteractionId(interaction.id!!) } returns java.util.Optional.of(feedback)
                }

                every { exampleRepository.countByQuestionCategoryAndIsActive("full_analysis", true) } returns 0L
                every { exampleRepository.countByQuestionCategoryAndIsActive("matchups", true) } returns 10L

                val legacyBadExample = com.pride.summoner_searcher_api.model.AiLearnedExample(
                    id = UUID.randomUUID(),
                    questionCategory = "matchups",
                    userQuestion = "How do I trade on Pantheon?",
                    aiResponse = "Don't waste Flash before casting Pantheon's Aegis of Zealous stun and rushing Divine Sunderer.",
                    qualityScore = java.math.BigDecimal("99.0"),
                    isActive = true
                )
                every {
                    exampleRepository.findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc("matchups")
                } returns listOf(legacyBadExample)

                val savedExamples = mutableListOf<com.pride.summoner_searcher_api.model.AiLearnedExample>()
                every { exampleRepository.save(capture(savedExamples)) } answers { firstArg() }
                val savedRun = slot<CurationRun>()
                every { curationRunRepository.save(capture(savedRun)) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()

                curationService.curateNewExamples()

                savedRun.captured.candidatesFound shouldBe 6
                savedRun.captured.examplesAdded shouldBe 1
                savedRun.captured.examplesReplaced shouldBe 1
                legacyBadExample.isActive shouldBe false

                val activeSaved = savedExamples.filter { it.isActive }
                activeSaved.size shouldBe 2
                activeSaved.first { it.questionCategory == "full_analysis" }.qualityScore shouldBe java.math.BigDecimal("100")
                activeSaved.first { it.questionCategory == "matchups" }.qualityScore shouldBe java.math.BigDecimal("100")
            }
        }
    }
})
