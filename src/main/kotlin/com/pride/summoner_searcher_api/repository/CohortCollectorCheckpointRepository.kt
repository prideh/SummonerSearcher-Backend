package com.pride.summoner_searcher_api.repository

import com.pride.summoner_searcher_api.model.CohortCollectorCheckpointEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface CohortCollectorCheckpointRepository : JpaRepository<CohortCollectorCheckpointEntity, UUID> {
    fun findByRegionAndTargetPatchAndQueueId(
        region: String,
        targetPatch: String,
        queueId: Int = 420
    ): CohortCollectorCheckpointEntity?

    fun findFirstByRegionAndQueueIdOrderByUpdatedAtDesc(
        region: String,
        queueId: Int = 420
    ): CohortCollectorCheckpointEntity?
}
