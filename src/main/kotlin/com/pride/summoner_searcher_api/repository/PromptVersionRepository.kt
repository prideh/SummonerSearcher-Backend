package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.PromptVersion
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.Optional
import java.util.UUID

@Repository
interface PromptVersionRepository : JpaRepository<PromptVersion, UUID> {
    
    /** Find prompt version by name */
    fun findByVersionName(versionName: String): Optional<PromptVersion>
    
    /** Find the currently active prompt version */
    fun findByIsActiveTrue(): Optional<PromptVersion>

    /** Find all active prompt versions scoped to a specific question category */
    fun findByIsActiveTrueAndCategoryScope(categoryScope: String): List<PromptVersion>

    /** Find all active global prompt versions (categoryScope is null) */
    fun findByIsActiveTrueAndCategoryScopeIsNull(): List<PromptVersion>

    /** Find all active challenger prompt versions for A/B evaluation */
    fun findByIsActiveTrueAndIsChallengerTrue(): List<PromptVersion>
}
