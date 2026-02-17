package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.CurationRun
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

@Repository
interface CurationRunRepository : JpaRepository<CurationRun, UUID> {
    
    /**
     * Find the most recent curation runs, ordered by date descending
     */
    fun findAllByOrderByRunDateDesc(pageable: Pageable): List<CurationRun>
    
    /**
     * Find curation runs within a date range
     */
    fun findByRunDateBetweenOrderByRunDateDesc(start: Instant, end: Instant): List<CurationRun>
}
