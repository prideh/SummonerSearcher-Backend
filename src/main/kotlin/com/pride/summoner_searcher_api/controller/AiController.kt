package com.pride.summoner_searcher_api.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.model.AiFeedback
import com.pride.summoner_searcher_api.model.AiInteraction
import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.AiFeedbackRepository
import com.pride.summoner_searcher_api.repository.AiInteractionRepository
import com.pride.summoner_searcher_api.repository.AiLearnedExampleRepository
import com.pride.summoner_searcher_api.repository.CurationRunRepository
import com.pride.summoner_searcher_api.repository.DiscoveredPatternRepository
import com.pride.summoner_searcher_api.service.AiAnalysisResult
import com.pride.summoner_searcher_api.service.AiAnalysisService
import com.pride.summoner_searcher_api.service.FeedbackProtectionService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

data class AiChatRequest(
    val context: Map<String, Any>,
    val messages: List<Map<String, String>>,
    val userMessage: String,
    val sessionId: String? = null
)

@RestController
@RequestMapping("/api/ai")
class AiController(
    private val aiAnalysisService: AiAnalysisService,
    private val interactionRepository: AiInteractionRepository,
    private val feedbackRepository: AiFeedbackRepository,
    private val exampleRepository: AiLearnedExampleRepository,
    private val curationRunRepository: CurationRunRepository,
    private val feedbackProtectionService: FeedbackProtectionService,
    private val patternRepository: DiscoveredPatternRepository
) {

    private val logger = LoggerFactory.getLogger(AiController::class.java)
    private val objectMapper = ObjectMapper()

    @PostMapping("/chat")
    fun chat(@CurrentUser user: User?, @RequestBody request: AiChatRequest): AiChatResponse {
        val userEmail = user?.email ?: "Anonymous"
        logger.info("AI chat request received from user: $userEmail")
        logger.debug("Request context keys: ${request.context.keys}")
        logger.debug("User message: ${request.userMessage}")
        
        val startTime = System.currentTimeMillis()
        
        // Block on the reactive Mono to make this a synchronous endpoint
        val result = aiAnalysisService.analyze(request.context, request.messages, request.userMessage, request.sessionId).block()
            ?: AiAnalysisResult("Error: No response from AI service", 0.7)
        
        val responseTime = System.currentTimeMillis() - startTime
        
        // Save interaction to database for learning
        val interaction = AiInteraction(
            user = user,
            summonerName = request.context["summonerName"]?.toString() ?: "Unknown",
            summonerRank = request.context["rank"]?.toString(),
            summonerRole = result.category.take(20),
            userQuestion = request.userMessage,
            aiResponse = result.response,
            contextFingerprint = request.sessionId ?: generateContextFingerprint(request.context),
            promptVersion = "v1.0", // Will be dynamic later
            responseTimeMs = responseTime,
            temperature = result.temperature
        )
        val savedInteraction = interactionRepository.save(interaction)
        
        logger.info("AI chat response generated (temp=${result.temperature}) for user: $userEmail (${responseTime}ms)")
        
        return AiChatResponse(
            response = result.response,
            interactionId = savedInteraction.id.toString()
        )
    }

    @PostMapping(
        path = ["/chat"],
        produces = [MediaType.TEXT_EVENT_STREAM_VALUE],
        headers = ["Accept=text/event-stream"]
    )
    fun chatStreamByHeader(@CurrentUser user: User?, @RequestBody request: AiChatRequest): SseEmitter =
        streamChatInternal(user, request)

    @PostMapping("/chat/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun chatStream(@CurrentUser user: User?, @RequestBody request: AiChatRequest): SseEmitter =
        streamChatInternal(user, request)

    private fun streamChatInternal(user: User?, request: AiChatRequest): SseEmitter {
        val userEmail = user?.email ?: "Anonymous"
        logger.info("AI streaming chat request received from user: $userEmail")
        val startTime = System.currentTimeMillis()
        val emitter = SseEmitter(120_000L)
        val fullResponse = StringBuilder()

        val streamContext = aiAnalysisService.analyzeStream(
            request.context,
            request.messages,
            request.userMessage,
            request.sessionId
        )

        val disposable = streamContext.chunks.subscribe(
            { chunk ->
                fullResponse.append(chunk)
                try {
                    val payload = objectMapper.writeValueAsString(
                        mapOf(
                            "type" to "chunk",
                            "chunk" to chunk,
                            "delta" to chunk,
                            "text" to chunk
                        )
                    )
                    emitter.send(payload)
                } catch (e: Exception) {
                    logger.debug("Client disconnected during SSE chunk send: {}", e.message)
                }
            },
            { error ->
                logger.error("Error streaming AI response for user $userEmail", error)
                try {
                    val errorPayload = objectMapper.writeValueAsString(
                        mapOf("type" to "error", "error" to "Error generating AI response")
                    )
                    emitter.send(errorPayload)
                    emitter.complete()
                } catch (_: Exception) {
                    emitter.completeWithError(error)
                }
            },
            {
                val responseTime = System.currentTimeMillis() - startTime
                val finalResponseText = fullResponse.toString().ifBlank { "No content generated." }
                try {
                    val interaction = AiInteraction(
                        user = user,
                        summonerName = request.context["summonerName"]?.toString() ?: "Unknown",
                        summonerRank = request.context["rank"]?.toString(),
                        summonerRole = streamContext.category.take(20),
                        userQuestion = request.userMessage,
                        aiResponse = finalResponseText,
                        contextFingerprint = request.sessionId ?: generateContextFingerprint(request.context),
                        promptVersion = "v1.0",
                        responseTimeMs = responseTime,
                        temperature = streamContext.temperature
                    )
                    val savedInteraction = interactionRepository.save(interaction)
                    val donePayload = objectMapper.writeValueAsString(
                        mapOf(
                            "type" to "done",
                            "done" to true,
                            "interactionId" to savedInteraction.id.toString(),
                            "response" to finalResponseText
                        )
                    )
                    emitter.send(donePayload)
                    emitter.complete()
                } catch (e: Exception) {
                    logger.warn("Failed finalizing streamed AI interaction: {}", e.message)
                    emitter.complete()
                }
            }
        )

        emitter.onCompletion { disposable.dispose() }
        emitter.onTimeout {
            disposable.dispose()
            emitter.complete()
        }
        emitter.onError { disposable.dispose() }

        return emitter
    }
    
    @PostMapping("/feedback")
    fun submitFeedback(
        @CurrentUser user: User?,
        @RequestBody request: FeedbackRequest
    ): ResponseEntity<FeedbackResponse> {
        logger.info("Feedback received - Type: ${request.feedbackType}, Engagement: ${request.engagementTimeMs}ms")
        
        // Find the interaction
        val interaction = interactionRepository.findById(UUID.fromString(request.interactionId))
            .orElseThrow { IllegalArgumentException("Interaction not found") }
        
        // Calculate abuse score
        val abuseScore = feedbackProtectionService.calculateAbuseScore(
            user?.id,
            request.feedbackType,
            request.engagementTimeMs,
            request.sessionId
        )
        
        val isValidated = feedbackProtectionService.isValidForTraining(abuseScore)
        
        // Save feedback
        val feedback = AiFeedback(
            interaction = interaction,
            user = user,
            feedbackType = request.feedbackType,
            engagementTimeMs = request.engagementTimeMs,
            isValidated = isValidated,
            abuseScore = abuseScore
        )
        feedbackRepository.save(feedback)
        
        logger.info("Feedback saved - Validated: $isValidated, AbuseScore: $abuseScore")
        
        return ResponseEntity.ok(FeedbackResponse(accepted = isValidated))
    }
    
    /**
     * Generate a fingerprint of the context for caching
     * Hash key stats to identify similar game situations
     */
    private fun generateContextFingerprint(context: Map<String, Any>): String {
        val rank = context["rank"]?.toString() ?: "unknown"
        val role = context["primaryRole"]?.toString() ?: "unknown"
        val kda = context["kda"]?.toString() ?: "0"
        val winRate = context["winRate"]?.toString() ?: "0"
        return "$rank-$role-$kda-$winRate".hashCode().toString()
    }
    
    /**
     * Admin endpoint: Get AI metrics for dashboard
     * Only accessible by users with ADMIN role
     */
    @GetMapping("/metrics")
    fun getAiMetrics(@CurrentUser user: User?): AiMetricsResponse {
        // Security: Only admins can view metrics
        if (user == null) {
            throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required")
        }
        if (user.role != "ADMIN") {
            throw ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required")
        }
        
        val totalInteractions = interactionRepository.count()
        val validatedFeedbackCount = feedbackRepository.countByIsValidatedTrue()
        val learnedExamplesCount = exampleRepository.countByIsActiveTrue()
        
        // Calculate satisfaction rate (positive feedback / total validated feedback)
        val allValidatedFeedback = feedbackRepository.findByIsValidatedTrueAndFeedbackType("positive").size +
                                   feedbackRepository.findByIsValidatedTrueAndFeedbackType("negative").size
        val positiveFeedback = feedbackRepository.findByIsValidatedTrueAndFeedbackType("positive").size
        
        val satisfactionRate = if (allValidatedFeedback > 0) {
            (positiveFeedback.toDouble() / allValidatedFeedback * 100).toBigDecimal()
                .setScale(1, RoundingMode.HALF_UP)
        } else BigDecimal.ZERO
        
        // Weekly trend: compare this week vs last week
        val thisWeekStart = Instant.now().minus(7, ChronoUnit.DAYS)
        val lastWeekStart = Instant.now().minus(14, ChronoUnit.DAYS)
        
        val thisWeekInteractions = interactionRepository.findCandidatesForLearning(thisWeekStart).size
        val lastWeekInteractions = interactionRepository.findCandidatesForLearning(lastWeekStart).size - thisWeekInteractions
        
        return AiMetricsResponse(
            totalInteractions = totalInteractions,
            validatedFeedbackCount = validatedFeedbackCount,
            learnedExamplesCount = learnedExamplesCount,
            satisfactionRate = satisfactionRate,
            thisWeekInteractions = thisWeekInteractions,
            lastWeekInteractions = lastWeekInteractions
        )
    }
    
    /**
     * Admin endpoint: Get curation run history (last 30 days)
     */
    @GetMapping("/curation-history")
    fun getCurationHistory(@CurrentUser user: User?): List<CurationRunResponse> {
        // Security: Only admins can view history
        if (user == null) {
            throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required")
        }
        if (user.role != "ADMIN") {
            throw ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required")
        }
        
        val thirtyDaysAgo = Instant.now().minus(30, ChronoUnit.DAYS)
        val runs = curationRunRepository.findByRunDateBetweenOrderByRunDateDesc(
            thirtyDaysAgo,
            Instant.now()
        )
        
        return runs.map { run ->
            CurationRunResponse(
                runDate = run.runDate.toString(),
                candidatesFound = run.candidatesFound,
                examplesAdded = run.examplesAdded,
                examplesReplaced = run.examplesReplaced,
                avgQualityScore = run.avgQualityScore,
                durationMs = run.durationMs
            )
        }
    }
    
    /**
     * Admin endpoint: Get temperature performance stats per category
     */
    @GetMapping("/temperature-stats")
    fun getTemperatureStats(@CurrentUser user: User?): List<TemperatureStatResponse> {
        if (user == null) throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required")
        if (user.role != "ADMIN") throw ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required")
        
        return interactionRepository.findTemperatureStats().mapNotNull { row ->
            val temp = (row[0] as? Number)?.toDouble() ?: return@mapNotNull null
            val totalUses = (row[1] as? Number)?.toInt() ?: 0
            val positiveCount = (row[2] as? Number)?.toInt() ?: 0
            val totalWithFeedback = (row[3] as? Number)?.toInt() ?: 0
            TemperatureStatResponse(
                temperature = temp,
                totalUses = totalUses,
                positiveCount = positiveCount,
                satisfactionRate = if (totalWithFeedback > 0) 
                    (positiveCount.toDouble() / totalWithFeedback * 100).toBigDecimal().setScale(1, RoundingMode.HALF_UP).toDouble()
                    else null
            )
        }
    }
    
    /**
     * Admin endpoint: Get discovered behavioral patterns
     */
    @GetMapping("/discovered-patterns")
    fun getDiscoveredPatterns(@CurrentUser user: User?): List<DiscoveredPatternResponse> {
        if (user == null) throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required")
        if (user.role != "ADMIN") throw ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required")
        
        return patternRepository.findAll()
            .sortedByDescending { it.confidenceScore }
            .map { pattern ->
                DiscoveredPatternResponse(
                    triggerCategory = pattern.triggerCategory,
                    followUpCategory = pattern.followUpCategory,
                    occurrenceCount = pattern.occurrenceCount,
                    confidenceScore = (pattern.confidenceScore * 100).toBigDecimal().setScale(1, RoundingMode.HALF_UP).toDouble(),
                    isActive = pattern.isActive,
                    lastUpdated = pattern.lastUpdated.toString()
                )
            }
    }
}

data class AiChatResponse(
    val response: String,
    val interactionId: String
)

data class FeedbackRequest(
    val interactionId: String,
    val feedbackType: String,
    val engagementTimeMs: Long,
    val sessionId: String? = null
)

data class FeedbackResponse(
    val accepted: Boolean
)

data class AiMetricsResponse(
    val totalInteractions: Long,
    val validatedFeedbackCount: Long,
    val learnedExamplesCount: Long,
    val satisfactionRate: BigDecimal,
    val thisWeekInteractions: Int,
    val lastWeekInteractions: Int
)

data class CurationRunResponse(
    val runDate: String,
    val candidatesFound: Int,
    val examplesAdded: Int,
    val examplesReplaced: Int,
    val avgQualityScore: Double?,
    val durationMs: Long?
)

data class TemperatureStatResponse(
    val temperature: Double,
    val totalUses: Int,
    val positiveCount: Int,
    val satisfactionRate: Double?
)

data class DiscoveredPatternResponse(
    val triggerCategory: String,
    val followUpCategory: String,
    val occurrenceCount: Int,
    val confidenceScore: Double,
    val isActive: Boolean,
    val lastUpdated: String
)
