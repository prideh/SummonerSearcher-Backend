package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.DiscoveredPattern
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.DiscoveredPatternRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Discovers behavioral patterns from multi-turn user conversations.
 * Example: "After cs_farming answers, 68% of users ask about wave_management next"
 * These patterns are injected into the AI prompt to pre-emptively address follow-ups.
 * Runs weekly on Sunday at 3AM to avoid interfering with nightly curation.
 */
@Service
class PatternDiscoveryService(
    private val interactionRepository: AiInteractionRepository,
    private val patternRepository: DiscoveredPatternRepository
) {
    private val logger = LoggerFactory.getLogger(PatternDiscoveryService::class.java)
    
    // Minimum confidence to activate a pattern (45% of users follow up with this)
    private val MIN_CONFIDENCE = 0.45
    
    // Minimum occurrences before we trust the pattern
    private val MIN_OCCURRENCES = 5
    
    @Scheduled(cron = "0 0 3 * * SUN")
    fun discoverPatterns() {
        logger.info("Starting weekly pattern discovery...")
        val since = Instant.now().minus(90, ChronoUnit.DAYS)
        
        // Find all multi-turn sessions from the last 90 days
        val sessionIds = interactionRepository.findMultiTurnSessionIds(since)
        logger.info("Analyzing ${sessionIds.size} multi-turn sessions")
        
        // Map: triggerCategory -> list of followUpCategories seen after it
        val sequenceMap = mutableMapOf<String, MutableList<String>>()
        
        sessionIds.forEach { sessionId ->
            val interactions = interactionRepository.findBySessionId(sessionId)
            
            // Analyze consecutive pairs: what did user ask AFTER each category?
            for (i in 0 until interactions.size - 1) {
                val current = interactions[i]
                val next = interactions[i + 1]
                
                val triggerCategory = categorizeQuestion(current.userQuestion)
                val followUpCategory = categorizeQuestion(next.userQuestion)
                
                // Only record if they're different categories (not repeating same question)
                if (triggerCategory != followUpCategory) {
                    sequenceMap.getOrPut(triggerCategory) { mutableListOf() }.add(followUpCategory)
                }
            }
        }
        
        // Calculate confidence scores and save patterns
        var savedCount = 0
        sequenceMap.forEach { (triggerCategory, followUps) ->
            val totalFollowUps = followUps.size
            val followUpCounts = followUps.groupingBy { it }.eachCount()
            
            followUpCounts.forEach { (followUpCategory, count) ->
                val confidence = count.toDouble() / totalFollowUps
                
                if (count >= MIN_OCCURRENCES && confidence >= MIN_CONFIDENCE) {
                    // Upsert pattern
                    val existing = patternRepository.findByTriggerCategoryAndFollowUpCategory(
                        triggerCategory, followUpCategory
                    )
                    
                    if (existing != null) {
                        existing.occurrenceCount = count
                        existing.confidenceScore = confidence
                        existing.lastUpdated = Instant.now()
                        existing.isActive = true
                        patternRepository.save(existing)
                    } else {
                        patternRepository.save(DiscoveredPattern(
                            triggerCategory = triggerCategory,
                            followUpCategory = followUpCategory,
                            occurrenceCount = count,
                            confidenceScore = confidence
                        ))
                        savedCount++
                    }
                    
                    logger.info("Pattern: $triggerCategory → $followUpCategory (${(confidence * 100).toInt()}% confidence, $count occurrences)")
                }
            }
        }
        
        logger.info("Pattern discovery complete - $savedCount new patterns found")
    }
    
    /**
     * Categorize question - mirrors AiAnalysisService logic for consistency.
     * Kept simple here since we only need broad categories for pattern matching.
     */
    private fun categorizeQuestion(question: String): String {
        val lowerQ = question.lowercase()
        return when {
            lowerQ.contains("cs") || lowerQ.contains("farm") || lowerQ.contains("minion") || lowerQ.contains("last hit") -> "cs_farming"
            lowerQ.contains("ward") || lowerQ.contains("vision") || lowerQ.contains("pink ward") || lowerQ.contains("control ward") -> "vision_warding"
            lowerQ.contains("wave") || lowerQ.contains("freeze") || lowerQ.contains("push") || lowerQ.contains("slow push") -> "wave_management"
            lowerQ.contains("trade") || lowerQ.contains("all-in") || lowerQ.contains("poke") || lowerQ.contains("harass") -> "trading_laning"
            lowerQ.contains("item") || lowerQ.contains("build") || lowerQ.contains("buy") -> "itemization_builds"
            lowerQ.contains("roam") || lowerQ.contains("rotate") || lowerQ.contains("macro") -> "roaming"
            lowerQ.contains("jungle") || lowerQ.contains("gank") || lowerQ.contains("camp") -> "jungle_pathing"
            lowerQ.contains("teamfight") || lowerQ.contains("team fight") || lowerQ.contains("engage") -> "teamfighting"
            lowerQ.contains("objective") || lowerQ.contains("dragon") || lowerQ.contains("baron") -> "objective_control"
            lowerQ.contains("champion") || lowerQ.contains("pool") || lowerQ.contains("pick") -> "champion_pool"
            lowerQ.contains("analyze") || lowerQ.contains("profile") || lowerQ.contains("overall") -> "full_analysis"
            else -> "general"
        }
    }
}
