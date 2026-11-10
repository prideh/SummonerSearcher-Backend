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

    Given("RLHF candidate curation with CS@10 theoretical-maximum mislabeling guard (#39)") {
        When("recent positively rated interactions include mislabeled CS@10 target/missed-CS/107 responses alongside a compliant theoretical-maximum response") {
            Then("it skips the mislabeled interactions and saves only the compliant example") {
                clearMocks(
                    interactionRepository,
                    feedbackRepository,
                    exampleRepository,
                    curationRunRepository,
                    promptVersionRepository
                )

                val fillerWords = "Review your wave crash timing before recall windows so enemy minions push back into you cleanly. ".repeat(8)
                val poisonedTargetCandidate = AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "Spear Shot#EUW",
                    summonerRank = "CHALLENGER I",
                    summonerRole = "cs_farming",
                    userQuestion = "How is my early lane CS at 10?",
                    aiResponse = "You averaged 72.9 CS at 10 across 14 games against the Top lane target of 114, so focus on fixing the missed 41.1 CS in lane. $fillerWords"
                )
                val poisoned107Candidate = AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "Spear Shot#EUW",
                    summonerRank = "CHALLENGER I",
                    summonerRole = "cs_farming",
                    userQuestion = "How many minions spawn by 10 minutes?",
                    aiResponse = "Only 107 lane minions spawn by 10:00 (~101-107 reachable), and 70-85 CS @ 10m is strong pacing, so try to focus on recall waves. $fillerWords"
                )
                val cleanCandidate = AiInteraction(
                    id = UUID.randomUUID(),
                    summonerName = "Spear Shot#EUW",
                    summonerRank = "CHALLENGER I",
                    summonerRole = "cs_farming",
                    userQuestion = "How should I interpret 72.9 lane CS at 10 across 14 Top games?",
                    aiResponse = "You averaged 72.9 lane CS at 10 across 14 Top games; 114 is the Patch 26.1+ theoretical maximum passive wave spawn ceiling, not a target or benchmark. Next game, focus on crashing cannon waves cleanly before roaming. $fillerWords"
                )

                listOf(poisonedTargetCandidate, poisoned107Candidate, cleanCandidate).forEach { candidate ->
                    every { feedbackRepository.findByInteractionId(candidate.id!!) } returns Optional.of(
                        AiFeedback(
                            id = UUID.randomUUID(),
                            interaction = candidate,
                            feedbackType = "positive",
                            engagementTimeMs = 20_000L,
                            isValidated = true
                        )
                    )
                }

                every { interactionRepository.findCandidatesForLearning(any()) } returns listOf(
                    poisonedTargetCandidate,
                    poisoned107Candidate,
                    cleanCandidate
                )
                every { exampleRepository.countByQuestionCategoryAndIsActive("cs_farming", true) } returns 2L
                val savedExamples = mutableListOf<AiLearnedExample>()
                every { exampleRepository.save(capture(savedExamples)) } answers { firstArg<AiLearnedExample>() }
                every { curationRunRepository.save(any<CurationRun>()) } answers { firstArg<CurationRun>() }
                every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
                every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()

                curationService.curateNewExamples()

                savedExamples.size shouldBe 1
                savedExamples.single().sourceInteraction?.id shouldBe cleanCandidate.id
            }
        }
    }
})
