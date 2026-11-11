package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.AiInteraction
import com.pride.summoner_searcher_api.model.AiLearnedExample
import com.pride.summoner_searcher_api.model.CurationRun
import com.pride.summoner_searcher_api.model.PromptVersion
import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.CurationRunRepository
import com.pride.summoner_searcher_api.repository.PromptVersionRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Service for automatically curating high-quality examples for few-shot learning
 * Runs as a scheduled job to continuously improve the AI
 */
@Service
class ExampleCurationService(
    private val interactionRepository: AiInteractionRepository,
    private val feedbackRepository: AiFeedbackRepository,
    private val exampleRepository: AiLearnedExampleRepository,
    private val curationRunRepository: CurationRunRepository,
    private val promptVersionRepository: PromptVersionRepository
) {
    
    private val logger = LoggerFactory.getLogger(ExampleCurationService::class.java)
    
    /**
     * Scheduled job: runs daily at 2 AM to curate new examples from recent interactions
     * This is the heart of the RLHF learning loop
     */
    @Scheduled(cron = "0 0 2 * * *")
    fun curateNewExamples() {
        val startTime = Instant.now()
        logger.info("Starting automatic example curation...")
        
        // Find interactions from last 7 days with positive validated feedback
        val candidates = interactionRepository.findCandidatesForLearning(
            since = Instant.now().minus(7, ChronoUnit.DAYS)
        )
        
        logger.info("Found ${candidates.size} candidate interactions for learning")
        
        var addedCount = 0
        var replacedCount = 0
        val qualityScores = mutableListOf<Double>()
        
        candidates.forEach { interaction ->
            if (!AiAnalysisService.isEligibleCuratedExample(interaction.userQuestion, interaction.aiResponse)) {
                return@forEach
            }

            val qualityScore = calculateQualityScore(interaction)
            
            // Only add high-quality examples (score > 80)
            if (qualityScore > BigDecimal("80.0")) {
                val category = interaction.summonerRole?.takeIf { it in AiAnalysisService.VALID_CATEGORIES }
                    ?: categorizeQuestion(interaction.userQuestion)
                
                // Check if we already have enough examples for this category
                val existingCount = exampleRepository.countByQuestionCategoryAndIsActive(category, true)
                
                if (existingCount < 10) {
                    // Add new example
                    val example = AiLearnedExample(
                        sourceInteraction = interaction,
                        questionCategory = category,
                        userQuestion = interaction.userQuestion,
                        aiResponse = interaction.aiResponse,
                        qualityScore = qualityScore,
                        rankContext = interaction.summonerRank
                    )
                    exampleRepository.save(example)
                    addedCount++
                    qualityScores.add(qualityScore.toDouble())
                    logger.info("Added new example - Category: $category, Score: $qualityScore")
                } else {
                    // Evict any legacy active example that fails current schema/CS10/boundary/brevity rules first;
                    // otherwise replace the lowest-scoring active example if this candidate scores higher.
                    val activeExamples = exampleRepository
                        .findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc(category)
                    val legacyIneligible = activeExamples.firstOrNull {
                        !AiAnalysisService.isEligibleCuratedExample(it.userQuestion, it.aiResponse)
                    }
                    val targetToReplace = legacyIneligible ?: activeExamples.firstOrNull()?.takeIf {
                        qualityScore > (it.qualityScore ?: BigDecimal.ZERO)
                    }
                    
                    if (targetToReplace != null) {
                        targetToReplace.isActive = false
                        exampleRepository.save(targetToReplace)
                        
                        val newExample = AiLearnedExample(
                            sourceInteraction = interaction,
                            questionCategory = category,
                            userQuestion = interaction.userQuestion,
                            aiResponse = interaction.aiResponse,
                            qualityScore = qualityScore,
                            rankContext = interaction.summonerRank
                        )
                        exampleRepository.save(newExample)
                        replacedCount++
                        qualityScores.add(qualityScore.toDouble())
                        logger.info("Replaced example - Category: $category, Old: ${targetToReplace.qualityScore}, New: $qualityScore")
                    }
                }
            }
        }
        
        // Calculate duration and average quality
        val endTime = Instant.now()
        val durationMs = java.time.Duration.between(startTime, endTime).toMillis()
        val avgQuality = if (qualityScores.isNotEmpty()) qualityScores.average() else null
        
        // Save curation run history
        val curationRun = CurationRun(
            runDate = startTime,
            candidatesFound = candidates.size,
            examplesAdded = addedCount,
            examplesReplaced = replacedCount,
            avgQualityScore = avgQuality,
            durationMs = durationMs
        )
        curationRunRepository.save(curationRun)
        
        logger.info("Example curation complete - Added: $addedCount, Replaced: $replacedCount, Duration: ${durationMs}ms")
        
        // 🔄 PROMPT EVOLUTION: detect underperforming categories and evaluate A/B challengers
        evaluateActiveChallengers()
        detectUnderperformingCategories()
    }
    
    /**
     * Evaluate active A/B challenger PromptVersions that have accumulated sufficient feedback.
     * Promotes winning challengers to champion and retires underperforming challengers.
     */
    private fun evaluateActiveChallengers() {
        try {
            val activeChallengers = promptVersionRepository.findByIsActiveTrueAndIsChallengerTrue()
            activeChallengers.forEach { challenger ->
                val totalFeedback = challenger.positiveFeedbackCount + challenger.negativeFeedbackCount
                if (challenger.totalUses >= 20 && totalFeedback >= 5) {
                    val satisfactionRate = challenger.positiveFeedbackCount.toDouble() / totalFeedback
                    val scope = challenger.categoryScope
                    val scopedVersions = if (scope != null) {
                        promptVersionRepository.findByIsActiveTrueAndCategoryScope(scope)
                    } else {
                        promptVersionRepository.findByIsActiveTrueAndCategoryScopeIsNull()
                    }
                    val currentChampion = scopedVersions.firstOrNull { !it.isChallenger && it.id != challenger.id }
                    val championFeedback = (currentChampion?.positiveFeedbackCount ?: 0) + (currentChampion?.negativeFeedbackCount ?: 0)
                    val championRate = if (championFeedback > 0) {
                        currentChampion!!.positiveFeedbackCount.toDouble() / championFeedback
                    } else 0.50

                    if (satisfactionRate >= 0.55 && satisfactionRate >= championRate) {
                        currentChampion?.let {
                            it.isActive = false
                            promptVersionRepository.save(it)
                        }
                        challenger.isChallenger = false
                        promptVersionRepository.save(challenger)
                        logger.info("Promoted challenger PromptVersion '{}' to champion (satisfaction={}%) for scope={}",
                            challenger.versionName, (satisfactionRate * 100).toInt(), scope ?: "global")
                    } else if (satisfactionRate < 0.40) {
                        challenger.isActive = false
                        promptVersionRepository.save(challenger)
                        logger.info("Retired underperforming challenger PromptVersion '{}' (satisfaction={}%) for scope={}",
                            challenger.versionName, (satisfactionRate * 100).toInt(), scope ?: "global")
                    }
                }
            }
        } catch (e: Exception) {
            logger.warn("Could not evaluate active challenger PromptVersions: ${e.message}")
        }
    }

    /**
     * Detect question categories with high negative feedback rates.
     * Categories with >60% negative rate and ≥20 interactions trigger creation of a scoped
     * challenger PromptVersion for A/B testing against the current baseline.
     */
    private fun detectUnderperformingCategories() {
        val since = Instant.now().minus(30, ChronoUnit.DAYS)
        
        try {
            val categoryStats = feedbackRepository.findNegativeRateByCategory(since, minInteractions = 20)
            
            categoryStats.forEach { row ->
                val category = row[0] as? String ?: return@forEach
                val negativeCount = (row[1] as? Number)?.toDouble() ?: 0.0
                val totalCount = (row[2] as? Number)?.toDouble() ?: 1.0
                val negativeRate = negativeCount / totalCount
                
                if (negativeRate > 0.60) {
                    logger.warn(
                        "PROMPT EVOLUTION TRIGGER: Category '$category' has ${(negativeRate * 100).toInt()}% " +
                        "negative rate over last 30 days ($totalCount interactions)."
                    )
                    val existingScoped = promptVersionRepository.findByIsActiveTrueAndCategoryScope(category)
                    if (existingScoped.none { it.isChallenger }) {
                        val safeCategorySlug = category.take(20).replace(Regex("[^a-zA-Z0-9_-]"), "")
                        val versionName = "v1.1-$safeCategorySlug-${Instant.now().epochSecond % 100000}"
                        val challengerTemplate = buildCategoryChallengerDirective(category)
                        val challenger = PromptVersion(
                            versionName = versionName.take(50),
                            promptTemplate = challengerTemplate,
                            isActive = true,
                            isChallenger = true,
                            categoryScope = category
                        )
                        promptVersionRepository.save(challenger)
                        logger.info("Created A/B challenger PromptVersion '{}' for underperforming category '{}'", challenger.versionName, category)
                    }
                }
            }
        } catch (e: Exception) {
            logger.warn("Could not run underperforming category detection: ${e.message}")
        }
    }

    private fun buildCategoryChallengerDirective(category: String): String =
        when (category) {
            "itemization_builds" -> "Answer concisely using items grounded in context and their functional purpose against the enemy composition (never cite Mythic-era items or unverified patch numbers), then give at most two practical build or reset checks."
            "matchups" -> "Answer concisely by citing the player's scoped matchup record with game counts and Q/W/E/R cooldown or spacing windows (use official ability names only when certain; frame unrecorded execution as hypotheses), plus at most two controllable next-game habits."
            "cs_farming" -> "Answer concisely with sample-scoped CS/min and 10:00 lane CS numbers (frame 114 Top/Bot and 120 Mid on Patch 26.1+ strictly as the theoretical maximum passive wave spawn by 10:00—not a target or benchmark, and never a missed-CS baseline), plus at most two controllable wave-reset or recall checks."
            "vision_warding" -> "Answer concisely using ward counts, ward types, and timing buckets only (never claim specific bush or map coordinates), and give at most two practical objective-timer warding habits."
            else -> "Answer the user's question concisely with key sample-scoped evidence, honest uncertainty, and at most two controllable next-game actions (expand only when explicitly asked)."
        }
    
    /**
     * Calculate quality score (0-100) based on multiple factors
     * This weighting system ensures only concise, evidence-grounded responses become training examples
     */
    private fun calculateQualityScore(interaction: AiInteraction): BigDecimal {
        var score = 0.0
        
        // Factor 1: User feedback (40 points) - Most important signal
        val feedback = feedbackRepository.findByInteractionId(interaction.id!!).orElse(null)
        if (feedback?.feedbackType == "positive" && feedback.isValidated) {
            score += 40.0
        } else {
            // No positive validated feedback = low score
            return BigDecimal.ZERO
        }
        
        // Factor 2: Engagement time (20 points) - Users read quality responses thoroughly
        val engagementTime = feedback.engagementTimeMs ?: 0
        when {
            engagementTime > 15000 -> score += 20.0 // Read for 15+ seconds
            engagementTime > 8000 -> score += 10.0  // Read for 8+ seconds
            else -> score += 5.0
        }
        
        // Factor 3: Coaching body length (15 points) - Short by default; fuller when expansion was requested
        val coachingBody = AiAnalysisService.stripSuggestionsBlock(interaction.aiResponse)
        val wordCount = coachingBody.split(Regex("\\s+")).count { it.isNotBlank() }
        val expandedRequested = AiAnalysisService.requestsExpandedAnswer(interaction.userQuestion)
        if (expandedRequested) {
            when {
                wordCount in 120..420 -> score += 15.0
                wordCount in 80..500 -> score += 10.0
                else -> score += 5.0
            }
        } else {
            when {
                wordCount in 35..180 -> score += 15.0 // Ideal short default answer
                wordCount in 20..220 -> score += 10.0
                else -> score += 5.0
            }
        }
        
        // Factor 4: Contains numerical evidence in coaching body (10 points) - Good coaching references stats
        if (coachingBody.contains(Regex("\\d+\\.\\d+|\\d+%"))) {
            score += 10.0
        }
        
        // Factor 5: Contains actionable advice keywords in coaching body (15 points)
        val actionKeywords = listOf("practice", "focus on", "aim for", "try", "drill", "next game", "improve", "check", "track", "habit", "review")
        val lowerBody = coachingBody.lowercase()
        val hasAction = actionKeywords.any { lowerBody.contains(it) }
        if (hasAction) {
            score += 15.0
        }
        
        return BigDecimal(score)
    }
    
    /**
     * Categorize question for targeted few-shot learning
     * Same logic as AI service for consistency
     */
    private fun categorizeQuestion(question: String): String {
        return AiAnalysisService.categorizeQuestion(question)
    }
}
