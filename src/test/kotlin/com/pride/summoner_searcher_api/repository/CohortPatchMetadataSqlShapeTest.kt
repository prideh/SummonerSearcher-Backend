package com.pride.summoner_searcher_api.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.CohortCollectorCheckpointDto
import com.pride.summoner_searcher_api.model.CohortBenchmarkCellEntity
import com.pride.summoner_searcher_api.model.CohortCellSampleStateEntity
import com.pride.summoner_searcher_api.model.CohortCollectorCheckpointEntity
import com.pride.summoner_searcher_api.service.CohortBenchmarkService
import com.pride.summoner_searcher_api.service.RedisCacheService
import com.pride.summoner_searcher_api.service.RiotApiRateLimiter
import com.pride.summoner_searcher_api.service.RiotApiService
import com.pride.summoner_searcher_api.service.SystemTimeProvider
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContainIgnoringCase
import io.kotest.matchers.string.shouldNotContainIgnoringCase
import io.mockk.every
import io.mockk.mockk
import jakarta.persistence.EntityManager
import org.hibernate.resource.jdbc.spi.StatementInspector
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

class CapturingStatementInspector : StatementInspector {
    companion object {
        val executedSql = CopyOnWriteArrayList<String>()

        fun clear() {
            executedSql.clear()
        }
    }

    override fun inspect(sql: String): String {
        executedSql.add(sql)
        return sql
    }
}

@DataJpaTest
@TestPropertySource(
    properties = [
        "spring.jpa.properties.hibernate.session_factory.statement_inspector=com.pride.summoner_searcher_api.repository.CapturingStatementInspector"
    ]
)
class CohortPatchMetadataSqlShapeTest {

    @Autowired
    private lateinit var sampleStateRepository: CohortCellSampleStateRepository

    @Autowired
    private lateinit var benchmarkCellRepository: CohortBenchmarkCellRepository

    @Autowired
    private lateinit var checkpointRepository: CohortCollectorCheckpointRepository

    @Autowired
    private lateinit var entityManager: EntityManager

    @Test
    fun `resolveGroundedCurrentPatch executes scalar patch SQL without selecting samples_json or metrics_json and preserves exact region-only equivalence`() {
        val objectMapper = ObjectMapper().findAndRegisterModules()
        val now = Instant.parse("2026-11-06T12:00:00Z")
        val largeSamplesPayload = """{"samplesByPlayerHash":{"p1":[{"matchIdHash":"m1","laneCsAt10":85.0}]},"padding":"${"X".repeat(64_000)}"}"""
        val largeMetricsPayload = """{"laneCsAt10":{"median":82.0},"padding":"${"Y".repeat(8_000)}"}"""

        // 1. Seed cohort_cell_sample_states across patches, eligibleGames (0 vs >0), queues (420 and non-420), and regions
        sampleStateRepository.saveAll(
            listOf(
                CohortCellSampleStateEntity(
                    region = "euw1",
                    patch = "16.21",
                    queueId = 420,
                    tier = "DIAMOND",
                    role = "TOP",
                    distinctPlayers = 25,
                    eligibleGames = 110,
                    samplesJson = largeSamplesPayload,
                    updatedAt = now
                ),
                CohortCellSampleStateEntity(
                    region = "euw1",
                    patch = "16.21",
                    queueId = 420,
                    tier = "DIAMOND",
                    role = "MIDDLE",
                    distinctPlayers = 30,
                    eligibleGames = 140,
                    samplesJson = largeSamplesPayload,
                    updatedAt = now
                ),
                // Non-420 queue row on euw1 with eligibleGames > 0 to verify region-only semantics are preserved identically
                CohortCellSampleStateEntity(
                    region = "euw1",
                    patch = "16.20",
                    queueId = 440,
                    tier = "GOLD",
                    role = "TOP",
                    distinctPlayers = 10,
                    eligibleGames = 20,
                    samplesJson = largeSamplesPayload,
                    updatedAt = now
                ),
                // eligibleGames == 0 row on patch 16.23 MUST be excluded
                CohortCellSampleStateEntity(
                    region = "euw1",
                    patch = "16.23",
                    queueId = 420,
                    tier = "MASTER",
                    role = "TOP",
                    distinctPlayers = 0,
                    eligibleGames = 0,
                    samplesJson = largeSamplesPayload,
                    updatedAt = now
                ),
                // Other region row ("na1") on patch 16.24 MUST be excluded
                CohortCellSampleStateEntity(
                    region = "na1",
                    patch = "16.24",
                    queueId = 420,
                    tier = "CHALLENGER",
                    role = "MIDDLE",
                    distinctPlayers = 15,
                    eligibleGames = 60,
                    samplesJson = largeSamplesPayload,
                    updatedAt = now
                )
            )
        )

        // 2. Seed cohort_benchmark_cells across patches, eligibleGames (0 vs >0), and regions
        benchmarkCellRepository.saveAll(
            listOf(
                CohortBenchmarkCellEntity(
                    region = "euw1",
                    patch = "16.22",
                    queueId = 420,
                    tier = "EMERALD",
                    role = "BOTTOM",
                    distinctPlayers = 40,
                    eligibleGames = 180,
                    metricsJson = largeMetricsPayload,
                    computedAt = now
                ),
                // eligibleGames == 0 row on patch 16.25 MUST be excluded
                CohortBenchmarkCellEntity(
                    region = "euw1",
                    patch = "16.25",
                    queueId = 420,
                    tier = "SILVER",
                    role = "TOP",
                    distinctPlayers = 0,
                    eligibleGames = 0,
                    metricsJson = largeMetricsPayload,
                    computedAt = now
                ),
                // Other region row ("kr") on patch 16.26 MUST be excluded
                CohortBenchmarkCellEntity(
                    region = "kr",
                    patch = "16.26",
                    queueId = 420,
                    tier = "DIAMOND",
                    role = "TOP",
                    distinctPlayers = 50,
                    eligibleGames = 210,
                    metricsJson = largeMetricsPayload,
                    computedAt = now
                )
            )
        )

        // 3. Seed latest checkpoint row with targetPatch="latest" and checkpointJson.targetPatch="16.21"
        val cpDto = CohortCollectorCheckpointDto(
            region = "euw1",
            targetPatch = "16.21",
            startedAt = now,
            updatedAt = now
        )
        checkpointRepository.save(
            CohortCollectorCheckpointEntity(
                region = "euw1",
                targetPatch = "latest",
                queueId = 420,
                checkpointJson = objectMapper.writeValueAsString(cpDto),
                startedAt = now,
                updatedAt = now
            )
        )

        entityManager.flush()
        entityManager.clear()

        // Compute legacy full-entity reference patch sets before clearing SQL inspector
        val legacySamplePatches = sampleStateRepository.findByRegionOrderByPatchDescTierAscRoleAsc("euw1")
            .filter { it.eligibleGames > 0 }
            .map { it.patch }
            .toSet()
        val legacyBenchmarkPatches = benchmarkCellRepository.findByRegionOrderByPatchDescTierAscRoleAsc("euw1")
            .filter { it.eligibleGames > 0 }
            .map { it.patch }
            .toSet()

        entityManager.clear()
        CapturingStatementInspector.clear()

        // 4. Execute scalar repository queries and resolveGroundedCurrentPatch
        val scalarSamplePatches = sampleStateRepository.findDistinctPatchesByRegionWithEligibleGames("euw1")
        val scalarBenchmarkPatches = benchmarkCellRepository.findDistinctPatchesByRegionWithEligibleGames("euw1")

        CapturingStatementInspector.clear()

        val redisCacheService = mockk<RedisCacheService>(relaxed = true)
        every { redisCacheService.get(any(), any<Class<Any>>()) } returns null
        every { redisCacheService.scanKeys(any(), any()) } returns emptyList()
        val riotApiService = mockk<RiotApiService>(relaxed = true)
        every { riotApiService.fetchDataDragonVersions(any()) } returns listOf("16.22.1", "16.21.1")

        val service = CohortBenchmarkService(
            redisCacheService = redisCacheService,
            cohortBenchmarkCellRepository = benchmarkCellRepository,
            riotApiRateLimiter = RiotApiRateLimiter(SystemTimeProvider()),
            riotApiService = riotApiService,
            objectMapper = objectMapper,
            networkCollectionEnabled = false,
            cohortCollectorCheckpointRepository = checkpointRepository,
            cohortCellSampleStateRepository = sampleStateRepository
        )

        val groundedPatch = service.resolveGroundedCurrentPatch("euw1")
        val sqlStatementsDuringResolve = CapturingStatementInspector.executedSql.toList()

        // 5. Verify exact set equivalence with legacy full-entity filtering
        scalarSamplePatches shouldContainExactlyInAnyOrder legacySamplePatches
        scalarSamplePatches shouldContainExactlyInAnyOrder listOf("16.20", "16.21")
        scalarBenchmarkPatches shouldContainExactlyInAnyOrder legacyBenchmarkPatches
        scalarBenchmarkPatches shouldContainExactlyInAnyOrder listOf("16.22")
        groundedPatch shouldBe "16.22"

        // 6. Verify actual SQL shape executed during resolveGroundedCurrentPatch:
        //    - Exactly 1 SELECT on cohort_cell_sample_states, 1 SELECT on cohort_benchmark_cells, 1 SELECT on cohort_collector_checkpoints
        //    - Neither cohort_cell_sample_states nor cohort_benchmark_cells query selects samples_json or metrics_json
        val sampleSql = sqlStatementsDuringResolve.single { it.contains("cohort_cell_sample_states", ignoreCase = true) }
        val benchmarkSql = sqlStatementsDuringResolve.single { it.contains("cohort_benchmark_cells", ignoreCase = true) }

        sampleSql shouldContainIgnoringCase "select distinct"
        sampleSql shouldContainIgnoringCase ".patch"
        sampleSql shouldContainIgnoringCase "eligible_games>0"
        sampleSql shouldNotContainIgnoringCase "samples_json"
        sampleSql shouldNotContainIgnoringCase "distinct_players"
        sampleSql shouldNotContainIgnoringCase "passes_quality_gate"
        sampleSql shouldNotContainIgnoringCase "queue_id"

        benchmarkSql shouldContainIgnoringCase "select distinct"
        benchmarkSql shouldContainIgnoringCase ".patch"
        benchmarkSql shouldContainIgnoringCase "eligible_games>0"
        benchmarkSql shouldNotContainIgnoringCase "metrics_json"
        benchmarkSql shouldNotContainIgnoringCase "distinct_players"
        benchmarkSql shouldNotContainIgnoringCase "passes_quality_gate"
        benchmarkSql shouldNotContainIgnoringCase "queue_id"
    }
}
