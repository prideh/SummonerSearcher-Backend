package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.AiInteraction
import com.pride.summoner_searcher_api.model.AiLearnedExample
import com.pride.summoner_searcher_api.model.CurationRun
import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.CurationRunRepository
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
    private val curationRunRepository: CurationRunRepository
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
            val qualityScore = calculateQualityScore(interaction)
            
            // Only add high-quality examples (score > 80)
            if (qualityScore > BigDecimal("80.0")) {
                val category = categorizeQuestion(interaction.userQuestion)
                
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
                    // Replace lowest-scoring example if this one is better
                    val lowestExample = exampleRepository
                        .findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc(category)
                        .firstOrNull()
                    
                    if (lowestExample != null && qualityScore > lowestExample.qualityScore!!) {
                        lowestExample.isActive = false
                        exampleRepository.save(lowestExample)
                        
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
                        logger.info("Replaced example - Category: $category, Old: ${lowestExample.qualityScore}, New: $qualityScore")
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
        
        // 🔄 PROMPT EVOLUTION: detect underperforming categories
        detectUnderperformingCategories()
    }
    
    /**
     * Detect question categories with high negative feedback rates.
     * Categories with >60% negative rate and ≥20 interactions trigger a prompt evolution flag.
     * Currently logs the finding; future: auto-generate new PromptVersion for that category.
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
                        "negative rate over last 30 days ($totalCount interactions). " +
                        "Consider rewriting the $category section of the prompt."
                    )
                    // TODO Phase 2: auto-generate new PromptVersion for this category
                    // and A/B test it against the current champion
                }
            }
        } catch (e: Exception) {
            logger.warn("Could not run underperforming category detection: ${e.message}")
        }
    }
    
    /**
     * Calculate quality score (0-100) based on multiple factors
     * This weighting system ensures only the best responses become training examples
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
        
        // Factor 3: Response length (15 points) - Not too short, not too long
        val wordCount = interaction.aiResponse.split("\\s+".toRegex()).size
        when {
            wordCount in 100..500 -> score += 15.0 // Ideal length
            wordCount in 50..100 || wordCount in 500..700 -> score += 10.0
            else -> score += 5.0
        }
        
        // Factor 4: Contains numerical evidence (10 points) - Good coaching references stats
        if (interaction.aiResponse.contains(Regex("\\d+\\.\\d+|\\d+%"))) {
            score += 10.0
        }
        
        // Factor 5: Contains actionable advice keywords (15 points)
        val actionKeywords = listOf("practice", "focus on", "aim for", "try", "drill", "next game", "improve")
        val hasAction = actionKeywords.any { interaction.aiResponse.lowercase().contains(it) }
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
        val lowerQ = question.lowercase()
        return when {
            lowerQ.contains("cs") || lowerQ.contains("farm") || lowerQ.contains("minion") -> "cs_improvement"
            lowerQ.contains("ward") || lowerQ.contains("vision") -> "vision_improvement"
            lowerQ.contains("champion") || lowerQ.contains("pool") -> "champion_pool"
            lowerQ.contains("analyze") || lowerQ.contains("profile") -> "full_analysis"
            else -> "general"
        }
    }
}
