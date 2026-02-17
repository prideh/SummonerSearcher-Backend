package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.annotation.CurrentUser
import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.service.AiAnalysisService
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class AiChatRequest(
    val context: Map<String, Any>,
    val messages: List<Map<String, String>>,
    val userMessage: String
)

@RestController
@RequestMapping("/api/ai")
class AiController(private val aiAnalysisService: AiAnalysisService) {

    private val logger = LoggerFactory.getLogger(AiController::class.java)

    @PostMapping("/chat")
    fun chat(@CurrentUser user: User?, @RequestBody request: AiChatRequest): String {
        val userEmail = user?.email ?: "Anonymous"
        logger.info("AI chat request received from user: $userEmail")
        logger.debug("Request context keys: ${request.context.keys}")
        logger.debug("User message: ${request.userMessage}")
        
        // Block on the reactive Mono to make this a synchronous endpoint
        // This ensures Spring Security authentication works properly
        val result = aiAnalysisService.analyze(request.context, request.messages, request.userMessage).block()
            ?: "Error: No response from AI service"
        
        logger.info("AI chat response generated successfully for user: $userEmail")
        return result
    }
}
