package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface CohortBenchmarkCellRepository : JpaRepository<CohortBenchmarkCellEntity, UUID> {
    fun findByRegionAndPatchAndQueueIdOrderByTierAscRoleAsc(
        region: String,
        patch: String,
        queueId: Int = 420
    ): List<CohortBenchmarkCellEntity>

    fun findByRegionOrderByPatchDescTierAscRoleAsc(region: String): List<CohortBenchmarkCellEntity>

    fun findByRegionAndPatchAndQueueIdAndTierAndRole(
        region: String,
        patch: String,
        queueId: Int,
        tier: String,
        role: String
    ): CohortBenchmarkCellEntity?

    @org.springframework.data.jpa.repository.Query(
        "SELECT DISTINCT e.patch FROM CohortBenchmarkCellEntity e WHERE e.region = :region AND e.eligibleGames > 0"
    )
    fun findDistinctPatchesByRegionWithEligibleGames(
        @org.springframework.data.repository.query.Param("region") region: String
    ): List<String>

    @org.springframework.data.jpa.repository.Query(
        """
        SELECT
            e.patch,
            COUNT(e),
            SUM(CASE WHEN e.passesQualityGate = true THEN 1 ELSE 0 END)
        FROM CohortBenchmarkCellEntity e
        WHERE LOWER(TRIM(e.region)) = :region
          AND e.queueId = :queueId
        GROUP BY e.patch
        """
    )
    fun summarizeCellCountsByPatchForRegion(
        @org.springframework.data.repository.query.Param("region") region: String,
        @org.springframework.data.repository.query.Param("queueId") queueId: Int = 420
    ): List<Array<Any?>>
}
