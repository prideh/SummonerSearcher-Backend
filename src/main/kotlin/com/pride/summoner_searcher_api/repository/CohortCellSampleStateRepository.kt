package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface CohortCellSampleStateRepository : JpaRepository<CohortCellSampleStateEntity, UUID> {
    fun findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(
        region: String,
        patch: String,
        queueId: Int = 420
    ): List<CohortCellSampleStateEntity>

    fun findByRegionOrderByPatchDescTierAscRoleAsc(region: String): List<CohortCellSampleStateEntity>

    fun findByRegionAndPatchAndQueueIdAndTierAndRole(
        region: String,
        patch: String,
        queueId: Int,
        tier: String,
        role: String
    ): CohortCellSampleStateEntity?

    @org.springframework.data.jpa.repository.Query(
        "SELECT DISTINCT e.patch FROM CohortCellSampleStateEntity e WHERE e.region = :region AND e.eligibleGames > 0"
    )
    fun findDistinctPatchesByRegionWithEligibleGames(
        @org.springframework.data.repository.query.Param("region") region: String
    ): List<String>
}
