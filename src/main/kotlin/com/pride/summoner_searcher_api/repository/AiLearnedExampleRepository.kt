package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.AiLearnedExample
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface AiLearnedExampleRepository : JpaRepository<AiLearnedExample, UUID> {
    
    /** Find top examples for a category (for few-shot learning) */
    fun findByIsActiveTrueAndQuestionCategoryOrderByQualityScoreDesc(
        category: String, 
        pageable: Pageable
    ): List<AiLearnedExample>
    
    /** Find lowest-scoring example in category (for replacement) */
    fun findByQuestionCategoryAndIsActiveTrueOrderByQualityScoreAsc(
        category: String
    ): List<AiLearnedExample>
    
    /** Count active examples in category */
    fun countByQuestionCategoryAndIsActive(category: String, isActive: Boolean): Long
    
    /** Count all active examples */
    fun countByIsActiveTrue(): Long
}
