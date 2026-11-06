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
}
