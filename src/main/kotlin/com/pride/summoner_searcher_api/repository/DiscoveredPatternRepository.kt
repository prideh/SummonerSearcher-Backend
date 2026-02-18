package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.DiscoveredPattern
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface DiscoveredPatternRepository : JpaRepository<DiscoveredPattern, UUID> {
    
    /** Find all active patterns for a given trigger category */
    fun findByTriggerCategoryAndIsActiveTrue(triggerCategory: String): List<DiscoveredPattern>
    
    /** Find existing pattern to update (avoid duplicates) */
    fun findByTriggerCategoryAndFollowUpCategory(
        triggerCategory: String, 
        followUpCategory: String
    ): DiscoveredPattern?
}
