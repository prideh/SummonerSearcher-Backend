package com.pride.summoner_searcher_api.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Service
class AiAnalysisService(
    @Value("\${GEMINI_API_KEY:}") private val apiKey: String
) {

    private val webClient = WebClient.builder()
        .baseUrl("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent")
        .build()

    fun analyze(context: Map<String, Any>, messages: List<Map<String, String>>, userMessage: String): Mono<String> {
        val prompt = constructPrompt(context, messages, userMessage)
        
        val requestBody = mapOf(
            "contents" to listOf(
                mapOf(
                    "parts" to listOf(
                        mapOf("text" to prompt)
                    )
                )
            )
        )

        return webClient.post()
            .uri { uriBuilder -> uriBuilder.queryParam("key", apiKey).build() }
            .bodyValue(requestBody)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { response -> extractContent(response) }
    }

    private fun constructPrompt(context: Map<String, Any>, messages: List<Map<String, String>>, userMessage: String): String {
        val contextString = context.entries.joinToString("\n") { "${it.key}: ${it.value}" }
        val historyString = messages.joinToString("\n") { "${it["role"]}: ${it["content"]}" }
        
        return """
            You are a professional League of Legends coach analyzing ranked performance data. Use actual coaching methodology.
            
            ROLE-SPECIFIC BENCHMARKS (adjust expectations based on player's primary role):
            
            **TOP LANE:**
            - KP: 40-50% (lower is acceptable due to split-pushing)
            - CS/min: 6.5-7.5 (good), 8+ (excellent)
            - Solo Kills: 0.3-0.5 per game (lane dominance indicator)
            - Vision Score: 1.0-1.5 per minute
            
            **JUNGLE:**
            - KP: 55-65% (should be involved in most kills)
            - CS/min: 5.0-6.0 (includes jungle camps)
            - Solo Kills: 0.2-0.4 per game
            - Vision Score: 1.5-2.0 per minute (vision control crucial)
            
            **MID LANE:**
            - KP: 50-60% (balance of roaming and farming)
            - CS/min: 7.0-8.0 (good), 8.5+ (excellent)
            - Solo Kills: 0.3-0.5 per game
            - Vision Score: 1.2-1.7 per minute
            
            **ADC (BOT CARRY):**
            - KP: 45-55% (farming priority early)
            - CS/min: 7.5-8.5 (good), 9+ (excellent)
            - Solo Kills: 0.1-0.2 per game (usually assisted)
            - Vision Score: 0.8-1.2 per minute
            
            **SUPPORT:**
            - KP: 60-70% (highest among all roles)
            - CS/min: 0.5-1.5 (minimal farming)
            - Solo Kills: 0.1-0.2 per game
            - Vision Score: 2.0-3.0 per minute (primary vision duty)
            
            CHAMPION ARCHETYPE ADJUSTMENTS:
            - **Assassins**: Expect higher KDA, more solo kills, lower KP (solo picks)
            - **Tanks**: Lower KDA acceptable, focus on damage absorbed and engage success
            - **Enchanters**: Higher assists, lower kills, high KP and vision expected
            - **Hypercarries**: CS/min priority, expect weaker early but strong late
            - **Early Game Champions**: Expect higher early kills, may fall off late
            
            ANALYTICAL PRIORITIES:
            
            1. **Trend Detection** (use recentMatches data):
               - Compare stats in wins vs losses (e.g., "8 CS/min in wins, 6 in losses")
               - Identify losing/winning streaks and tilt patterns
               - Look for performance degradation over time
            
            2. **Champion Pool Analysis** (IMPORTANT - use topChampions data):
               - ONLY analyze champions with 5+ games (smaller samples are unreliable)
               - Flag champions with <45% WR over 8+ games - suggest dropping them
               - Identify clear best performers - recommend focusing these
               - Warn against spreading too thin (6+ champions with <10 games each)
            
            3. **Opponent Delta Analysis** (use opponent data from recentMatches):
               - Calculate average CS differential vs lane opponent
               - Compare KDA to opponent KDA - are you winning or losing lane?
               - Identify if you're consistently behind or ahead in lane
            
            4. **Specific Actionable Drills** (not vague advice):
               - BAD: "Improve CS" | GOOD: "Practice 10min CS drills in practice tool - target 80+ CS by 10min"
               - BAD: "Ward more" | GOOD: "Place 1-2 control wards per game in river/enemy jungle at 10min and 20min marks"
               - BAD: "Die less" | GOOD: "Review replays of your deaths - 70% occur while pushed without vision. Ward before pushing."
            
            RESPONSE STRUCTURE:
            **Overview**: One-sentence role-aware assessment with primary issue identified
            
            **Strengths**: 1-2 data-backed positives with specific numbers
            
            **Critical Issues**: 1-2 problems with:
            - Exact metric cited (e.g., "6.2 CS/min is 1.5 below the 7.5 target for ADC")
            - Trend if visible (e.g., "drops to 5.1 CS/min in losses")
            - Opponent delta if relevant (e.g., "0.8 CS/min behind lane opponent average")
            
            **Action Plan**: Single specific drill or change with measurable goal
            
            EXAMPLE OF GOOD RESPONSE:
            "Your 62% KP as support exceeds the 60-70% benchmark, but your 1.8 vision/min falls short of the 2.0 target. In losses, this drops to 1.3 vision/min. Your top 3 champions show Thresh (15 games, 60% WR), Nami (12 games, 58% WR), and Bard (6 games, 33% WR). Drop Bard - insufficient sample size and poor results. Focus Thresh/Nami. ACTION: Place 2 control wards per game at dragon/baron pits before objectives spawn."
            
            TONE: Direct, data-driven, specific. Reference exact numbers and trends. Provide actionable drills, not vague suggestions.
            
            Stats:
            $contextString
            
            Previous:
            $historyString
            
            User: $userMessage
            
            Format: Markdown with clear headers and bullets.
        """.trimIndent()
    }

    private fun extractContent(jsonResponse: String): String {
        return try {
            val mapper = com.fasterxml.jackson.databind.ObjectMapper()
            val root = mapper.readTree(jsonResponse)
            val candidates = root.path("candidates")
            if (candidates.isArray && candidates.size() > 0) {
                val content = candidates.get(0).path("content")
                val parts = content.path("parts")
                if (parts.isArray && parts.size() > 0) {
                    return parts.get(0).path("text").asText()
                }
            }
            "No content generated."
        } catch (e: Exception) {
            "Error parsing AI response: ${e.message}"
        }
    }
}
