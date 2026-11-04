package com.pride.summoner_searcher_api.service

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

    beforeTest {
        clearMocks(
            interactionRepository,
            feedbackRepository,
            exampleRepository,
            curationRunRepository,
            promptVersionRepository,
            answers = false
        )
    }

    Given("An active challenger PromptVersion competing against a champion (#11)") {
        When("the challenger has >=20 uses and higher satisfaction than the champion") {
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
            every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns listOf(challenger)
            every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("itemization_builds") } returns listOf(champion, challenger)
            every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns emptyList()
            every { promptVersionRepository.save(any()) } answers { firstArg() }

            curationService.curateNewExamples()

            Then("the challenger is promoted to champion and the previous champion is deactivated") {
                challenger.isChallenger shouldBe false
                challenger.isActive shouldBe true
                champion.isActive shouldBe false
            }
        }

        When("an underperforming category exceeds 60% negative feedback with no active challenger") {
            every { interactionRepository.findCandidatesForLearning(any()) } returns emptyList()
            every { promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue() } returns emptyList()
            every { feedbackRepository.findNegativeRateByCategory(any(), any()) } returns listOf(
                arrayOf<Any>("matchups", 16L, 22L) // 72.7% negative rate
            )
            every { promptVersionRepository.findByIsActiveTrueAndCategoryScope("matchups") } returns emptyList()

            val savedVersion = slot<PromptVersion>()
            every { promptVersionRepository.save(capture(savedVersion)) } answers { firstArg() }

            curationService.curateNewExamples()

            Then("it automatically creates and activates a new challenger PromptVersion for that category") {
                verify(atLeast = 1) { promptVersionRepository.save(any()) }
                savedVersion.captured.categoryScope shouldBe "matchups"
                savedVersion.captured.isActive shouldBe true
                savedVersion.captured.isChallenger shouldBe true
            }
        }
    }
})
