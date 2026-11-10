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

        When("an underperforming category exceeds 60% negative feedback with no active challenger") {
            Then("it automatically creates and activates a new challenger PromptVersion for that category") {
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
                    arrayOf<Any>("matchups", 16L, 22L) // 72.7% negative rate
                )
                every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("matchups") } returns emptyList()

                val savedVersion = slot<PromptVersion>()
                every { promptVersionRepository.save(capture(savedVersion)) } answers { firstArg<PromptVersion>() }

                curationService.curateNewExamples()

                verify(atLeast = 1) { promptVersionRepository.save(any<PromptVersion>()) }
                savedVersion.captured.categoryScope shouldBe "matchups"
                savedVersion.captured.isActive shouldBe true
                savedVersion.captured.isChallenger shouldBe true
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
})
