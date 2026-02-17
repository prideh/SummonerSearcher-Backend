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
}
