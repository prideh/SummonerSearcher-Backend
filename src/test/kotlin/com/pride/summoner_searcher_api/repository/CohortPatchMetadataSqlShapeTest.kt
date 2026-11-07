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
import io.mockk.Called
import io.mockk.clearMocks
import io.mockk.verify
import org.hibernate.resource.jdbc.spi.StatementInspector
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
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
@Import(
    CohortBenchmarkService::class,
    com.pride.summoner_searcher_api.service.SystemMemoryStatsProvider::class,
    RiotApiRateLimiter::class,
    CohortPatchMetadataSqlShapeTest.SpringWiringTestConfig::class
)
@TestPropertySource(
    properties = [
        "spring.jpa.properties.hibernate.session_factory.statement_inspector=com.pride.summoner_searcher_api.repository.CapturingStatementInspector"
    ]
)
class CohortPatchMetadataSqlShapeTest {

    @TestConfiguration
    class SpringWiringTestConfig {
        @Bean
        fun timeProvider(): com.pride.summoner_searcher_api.service.TimeProvider =
            object : com.pride.summoner_searcher_api.service.TimeProvider {
                override fun now(): Long = Instant.parse("2026-11-06T12:00:00Z").toEpochMilli()
            }

        @Bean
        fun redisCacheService(): RedisCacheService = mockk(relaxed = true)

        @Bean
        fun riotApiService(): RiotApiService = mockk(relaxed = true)

        @Bean
        fun objectMapper(): ObjectMapper = ObjectMapper().findAndRegisterModules()
    }

    @Autowired
    private lateinit var sampleStateRepository: CohortCellSampleStateRepository

    @Autowired
    private lateinit var benchmarkCellRepository: CohortBenchmarkCellRepository

    @Autowired
    private lateinit var checkpointRepository: CohortCollectorCheckpointRepository

    @Autowired
    private lateinit var entityManager: EntityManager

    @Autowired
    private lateinit var transactionTemplate: org.springframework.transaction.support.TransactionTemplate

    @Autowired
    private lateinit var springWiredCohortBenchmarkService: CohortBenchmarkService

    @Autowired
    private lateinit var springWiredRedisCacheService: RedisCacheService

    @Autowired
    private lateinit var springWiredRiotApiService: RiotApiService

    @Autowired
    private lateinit var indexedPlayerRepository: IndexedPlayerRepository

    @Autowired
    private lateinit var playerSeasonStatsRepository: PlayerSeasonStatsRepository

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
            patchSelectionSource = "AUTO_LATEST_EXACT_PATCH",
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

    @Test
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED
    )
    fun `ingestCachedPlayerByExactIds rolls back first table in real H2 when second table save fails and commits both atomically on success`() {
        sampleStateRepository.deleteAll()
        benchmarkCellRepository.deleteAll()
        checkpointRepository.deleteAll()

        try {
            val objectMapper = ObjectMapper().findAndRegisterModules()
            val fixedNow = Instant.parse("2026-11-06T12:00:00Z")
            val timeProvider = object : com.pride.summoner_searcher_api.service.TimeProvider {
                override fun now(): Long = fixedNow.toEpochMilli()
            }

            val redisCacheService = mockk<RedisCacheService>(relaxed = true)
            every { redisCacheService.get(any(), any<Class<Any>>()) } returns null
            every {
                redisCacheService.get("cohort:patch:external:ddragon", String::class.java)
            } returns "16.22"
            every { redisCacheService.setIfAbsent("lock:cohort:collector:active:euw1", "ACTIVE", any()) } returns true
            every {
                redisCacheService.get(
                    "cohort:rank:snapshot:euw1:test-h2-puuid",
                    com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto::class.java
                )
            } returns com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto(
                puuid = "test-h2-puuid",
                region = "euw1",
                tier = "CHALLENGER",
                rank = "I",
                leaguePoints = 1100,
                queueType = "RANKED_SOLO_5x5",
                snapshotAt = fixedNow.minusSeconds(600)
            )

            val matchJson = """
                {
                  "metadata": {
                    "matchId": "EUW1_H2_TX_1",
                    "participants": ["test-h2-puuid"]
                  },
                  "info": {
                    "gameCreation": ${fixedNow.minusSeconds(1800).toEpochMilli()},
                    "gameDuration": 1800,
                    "gameId": 777001,
                    "queueId": 420,
                    "gameMode": "CLASSIC",
                    "gameVersion": "16.22.615.1111",
                    "participants": [
                      {
                        "puuid": "test-h2-puuid",
                        "teamId": 100,
                        "teamPosition": "TOP",
                        "individualPosition": "TOP",
                        "kills": 8,
                        "deaths": 2,
                        "assists": 5,
                        "totalMinionsKilled": 240,
                        "neutralMinionsKilled": 12,
                        "goldEarned": 14500,
                        "totalDamageDealtToChampions": 28000,
                        "visionScore": 32,
                        "totalTimeSpentDead": 45,
                        "win": true,
                        "challenges": {
                          "laneMinionsFirst10Minutes": 88,
                          "jungleCsBefore10Minutes": 4.0,
                          "goldPerMinute": 483.3,
                          "damagePerMinute": 933.3,
                          "visionScorePerMinute": 1.06,
                          "killParticipation": 0.65
                        }
                      }
                    ]
                  }
                }
            """.trimIndent()
            val matchDto = objectMapper.readValue(matchJson, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            every {
                redisCacheService.get("match:euw1:EUW1_H2_TX_1", com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            } returns matchDto

            val riotApiService = mockk<RiotApiService>(relaxed = true)

            val failBenchmarkSave = java.util.concurrent.atomic.AtomicBoolean(true)
            val faultInjectingBenchmarkRepo = java.lang.reflect.Proxy.newProxyInstance(
                CohortBenchmarkCellRepository::class.java.classLoader,
                arrayOf(CohortBenchmarkCellRepository::class.java)
            ) { _, method, args ->
                if (method.name == "save" && failBenchmarkSave.get()) {
                    sampleStateRepository.flush()
                    throw org.springframework.dao.DataIntegrityViolationException("Simulated second-table failure in cohort_benchmark_cells")
                }
                if (args == null) {
                    method.invoke(benchmarkCellRepository)
                } else {
                    method.invoke(benchmarkCellRepository, *args)
                }
            } as CohortBenchmarkCellRepository

            val service = CohortBenchmarkService(
                redisCacheService = redisCacheService,
                cohortBenchmarkCellRepository = faultInjectingBenchmarkRepo,
                riotApiRateLimiter = RiotApiRateLimiter(timeProvider),
                riotApiService = riotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = false,
                timeProvider = timeProvider,
                cohortCollectorCheckpointRepository = checkpointRepository,
                cohortCellSampleStateRepository = sampleStateRepository,
                transactionTemplate = transactionTemplate
            )

            CapturingStatementInspector.clear()

            // 1. First attempt: sampleStateRepository.save executes INSERT into H2, then benchmarkCellRepository.save throws.
            //    Real H2 TransactionTemplate MUST roll back the first-table INSERT!
            val failedResult = service.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = "test-h2-puuid",
                targetPatch = "16.22",
                explicitMatchIds = listOf("EUW1_H2_TX_1")
            )
            val sqlDuringFailedTx = CapturingStatementInspector.executedSql.toList()

            failedResult.status shouldBe "PERSISTENCE_ERROR"
            failedResult.netNewSamplesPersisted shouldBe 0
            // Confirm INSERT into cohort_cell_sample_states actually ran inside the transaction before rollback
            sqlDuringFailedTx.any {
                it.contains("insert", ignoreCase = true) && it.contains("cohort_cell_sample_states", ignoreCase = true)
            } shouldBe true

            // Verify in H2 after transaction rollback: BOTH tables have 0 rows!
            sampleStateRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            ) shouldBe null
            benchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            ) shouldBe null
            service.getInMemoryDurableCellStateForTest("euw1", "16.22", "CHALLENGER", "TOP") shouldBe null
            service.getDurableDbWriteCounts()["cohortCellSampleStatesWrites"] shouldBe 0L
            service.getDurableDbWriteCounts()["cohortBenchmarkCellsWrites"] shouldBe 0L

            // 2. Second attempt with second-table failure cleared: both tables commit atomically in H2!
            failBenchmarkSave.set(false)
            val successResult = service.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = "test-h2-puuid",
                targetPatch = "16.22",
                explicitMatchIds = listOf("EUW1_H2_TX_1")
            )

            successResult.status shouldBe "COMPLETED"
            successResult.netNewSamplesPersisted shouldBe 1
            successResult.updatedCells.size shouldBe 1
            verify { riotApiService wasNot Called }

            val committedSampleRow = sampleStateRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            )
            val committedBenchmarkRow = benchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            )
            (committedSampleRow != null) shouldBe true
            committedSampleRow!!.eligibleGames shouldBe 1
            committedSampleRow.distinctPlayers shouldBe 1
            (committedBenchmarkRow != null) shouldBe true
            committedBenchmarkRow!!.eligibleGames shouldBe 1
            committedBenchmarkRow.distinctPlayers shouldBe 1
            service.getDurableDbWriteCounts()["cohortCellSampleStatesWrites"] shouldBe 1L
            service.getDurableDbWriteCounts()["cohortBenchmarkCellsWrites"] shouldBe 1L
        } finally {
            sampleStateRepository.deleteAll()
            benchmarkCellRepository.deleteAll()
            checkpointRepository.deleteAll()
        }
    }

    @Test
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED
    )
    fun `production Spring DI wires CohortBenchmarkService with usable TransactionTemplate and JPA repositories and commits exact cached player ingestion atomically`() {
        sampleStateRepository.deleteAll()
        benchmarkCellRepository.deleteAll()
        checkpointRepository.deleteAll()
        clearMocks(springWiredRedisCacheService, springWiredRiotApiService)

        try {
            val objectMapper = ObjectMapper().findAndRegisterModules()
            val fixedNow = Instant.parse("2026-11-06T12:00:00Z")

            every { springWiredRedisCacheService.get(any(), any<Class<Any>>()) } returns null
            every {
                springWiredRedisCacheService.get("cohort:patch:external:ddragon", String::class.java)
            } returns "16.22"
            every {
                springWiredRedisCacheService.setIfAbsent("lock:cohort:collector:active:euw1", "ACTIVE", any())
            } returns true
            every {
                springWiredRedisCacheService.get(
                    "cohort:rank:snapshot:euw1:spring-di-puuid",
                    com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto::class.java
                )
            } returns com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto(
                puuid = "spring-di-puuid",
                region = "euw1",
                tier = "CHALLENGER",
                rank = "I",
                leaguePoints = 1200,
                queueType = "RANKED_SOLO_5x5",
                snapshotAt = fixedNow.minusSeconds(300)
            )

            val matchJson = """
                {
                  "metadata": {
                    "matchId": "EUW1_SPRING_DI_1",
                    "participants": ["spring-di-puuid"]
                  },
                  "info": {
                    "gameCreation": ${fixedNow.minusSeconds(1200).toEpochMilli()},
                    "gameDuration": 1800,
                    "gameId": 888001,
                    "queueId": 420,
                    "gameMode": "CLASSIC",
                    "gameVersion": "16.22.615.1111",
                    "participants": [
                      {
                        "puuid": "spring-di-puuid",
                        "teamId": 100,
                        "teamPosition": "TOP",
                        "individualPosition": "TOP",
                        "kills": 10,
                        "deaths": 1,
                        "assists": 6,
                        "totalMinionsKilled": 255,
                        "neutralMinionsKilled": 10,
                        "goldEarned": 15200,
                        "totalDamageDealtToChampions": 31000,
                        "visionScore": 35,
                        "totalTimeSpentDead": 20,
                        "win": true,
                        "challenges": {
                          "laneMinionsFirst10Minutes": 92,
                          "jungleCsBefore10Minutes": 4.0,
                          "goldPerMinute": 506.6,
                          "damagePerMinute": 1033.3,
                          "visionScorePerMinute": 1.16,
                          "killParticipation": 0.70
                        }
                      }
                    ]
                  }
                }
            """.trimIndent()
            val matchDto = objectMapper.readValue(matchJson, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            every {
                springWiredRedisCacheService.get("match:euw1:EUW1_SPRING_DI_1", com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            } returns matchDto

            val result = springWiredCohortBenchmarkService.ingestCachedPlayerByExactIds(
                region = "euw1",
                puuid = "spring-di-puuid",
                targetPatch = "16.22",
                explicitMatchIds = listOf("EUW1_SPRING_DI_1")
            )

            // Proves Spring's constructor injection wired both CohortCellSampleStateRepository and a usable
            // TransactionTemplate (with non-null PlatformTransactionManager) into CohortBenchmarkService
            result.status shouldBe "COMPLETED"
            result.netNewSamplesPersisted shouldBe 1
            result.updatedCells.size shouldBe 1
            verify { springWiredRiotApiService wasNot Called }

            val sampleRow = sampleStateRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            )
            val benchmarkRow = benchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "CHALLENGER",
                role = "TOP"
            )
            (sampleRow != null) shouldBe true
            sampleRow!!.eligibleGames shouldBe 1
            (benchmarkRow != null) shouldBe true
            benchmarkRow!!.eligibleGames shouldBe 1
        } finally {
            sampleStateRepository.deleteAll()
            benchmarkCellRepository.deleteAll()
            checkpointRepository.deleteAll()
        }
    }

    @Test
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED
    )
    fun `real H2 PlayerSeasonStatsRepository and IndexedPlayerRepository enforce monotonic lastSeenAt identity indexing, optimistic locking, and scalar coverage summaries`() {
        playerSeasonStatsRepository.deleteAll()
        indexedPlayerRepository.deleteAll()
        clearMocks(springWiredRedisCacheService, springWiredRiotApiService)

        try {
            val objectMapper = ObjectMapper().findAndRegisterModules()
            val fixedNow = Instant.parse("2026-11-06T12:00:00Z")
            val clock = object : com.pride.summoner_searcher_api.service.TimeProvider {
                override fun now(): Long = fixedNow.toEpochMilli()
            }

            // 1. Verify fail-closed when TransactionTemplate is missing: no rows written to player_season_stats
            val unwiredTxCacheService = com.pride.summoner_searcher_api.service.PlayerCacheService(
                redisCacheService = springWiredRedisCacheService,
                riotApiService = springWiredRiotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = playerSeasonStatsRepository,
                transactionTemplate = null,
                objectMapper = objectMapper
            )
            every { springWiredRedisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { springWiredRedisCacheService.get(any(), com.pride.summoner_searcher_api.dto.SummonerProfileDto::class.java) } returns null
            io.mockk.coEvery { springWiredRiotApiService.getSummonerByPuuid("h2-season-puuid", "euw1") } returns
                com.pride.summoner_searcher_api.dto.SummonerDto("id", "h2-season-puuid", 400L, 77)
            io.mockk.coEvery { springWiredRiotApiService.fetchLeagueRank("h2-season-puuid", "euw1") } returns null
            io.mockk.coEvery {
                springWiredRiotApiService.fetchMatchIdsWithOutcomeSince(any(), any(), any())
            } coAnswers {
                val ids = springWiredRiotApiService.fetchMatchIdsSince(firstArg(), secondArg(), thirdArg())
                com.pride.summoner_searcher_api.service.RiotApiService.MatchIdsPaginationOutcome(
                    matchIds = ids,
                    pagesFetched = if (ids.isEmpty()) 1 else (ids.size + 99) / 100,
                    exhaustedCleanly = ids.size <= 2000,
                    hitSafetyCap = ids.size > 2000,
                    stoppedOnPageFailure = false
                )
            }
            io.mockk.coEvery { springWiredRiotApiService.fetchMatchIdsSince("h2-season-puuid", "euw1", any()) } returns listOf("EUW1_NEW_2000", "EUW1_OLD_1000")
            every { springWiredRedisCacheService.multiGet(any(), com.pride.summoner_searcher_api.dto.MatchDto::class.java) } answers {
                firstArg<List<String>>().associateWith { null }
            }

            fun makeMatchWithParticipant(
                matchId: String,
                creationMs: Long,
                coPlayerGameName: String,
                coPlayerTag: String,
                coPlayerLevel: Int,
                coPlayerIcon: Int
            ): com.pride.summoner_searcher_api.dto.MatchDto {
                val json = """
                    {
                      "metadata": {
                        "matchId": "$matchId",
                        "participants": ["h2-season-puuid", "co-player-puuid"]
                      },
                      "info": {
                        "gameCreation": $creationMs,
                        "gameDuration": 1800,
                        "gameId": ${matchId.hashCode().toLong()},
                        "queueId": 420,
                        "gameMode": "CLASSIC",
                        "gameVersion": "16.22.1",
                        "teams": [],
                        "participants": [
                          {
                            "puuid": "h2-season-puuid",
                            "riotIdGameName": "MainPlayer",
                            "riotIdTagline": "EUW",
                            "summonerLevel": 400,
                            "profileIcon": 77,
                            "championName": "Orianna",
                            "teamId": 100,
                            "teamPosition": "MIDDLE",
                            "win": true,
                            "kills": 9,
                            "deaths": 2,
                            "assists": 8,
                            "totalMinionsKilled": 225,
                            "neutralMinionsKilled": 10,
                            "visionScore": 28
                          },
                          {
                            "puuid": "co-player-puuid",
                            "riotIdGameName": "$coPlayerGameName",
                            "riotIdTagline": "$coPlayerTag",
                            "summonerLevel": $coPlayerLevel,
                            "profileIcon": $coPlayerIcon,
                            "championName": "LeeSin",
                            "teamId": 100,
                            "teamPosition": "JUNGLE",
                            "win": true,
                            "kills": 6,
                            "deaths": 3,
                            "assists": 12,
                            "totalMinionsKilled": 40,
                            "neutralMinionsKilled": 165,
                            "visionScore": 31
                          }
                        ]
                      }
                    }
                """.trimIndent()
                return objectMapper.readValue(json, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            }

            val newerCreationMs = fixedNow.minusSeconds(1_000).toEpochMilli()
            val olderCreationMs = fixedNow.minusSeconds(50_000).toEpochMilli()
            val ancientCreationMs = fixedNow.minusSeconds(200_000).toEpochMilli()

            io.mockk.coEvery { springWiredRiotApiService.getMatchById("EUW1_NEW_2000", "euw1") } returns
                makeMatchWithParticipant("EUW1_NEW_2000", newerCreationMs, "NewRiotName", "NEW1", 350, 99)
            io.mockk.coEvery { springWiredRiotApiService.getMatchById("EUW1_OLD_1000", "euw1") } returns
                makeMatchWithParticipant("EUW1_OLD_1000", olderCreationMs, "OldRiotName", "OLD1", 340, 11)
            io.mockk.coEvery { springWiredRiotApiService.getMatchById("EUW1_ANCIENT_500", "euw1") } returns
                makeMatchWithParticipant("EUW1_ANCIENT_500", ancientCreationMs, "AncientRiotName", "ANC1", 300, 5)

            val unwiredError = runCatching {
                kotlinx.coroutines.runBlocking {
                    unwiredTxCacheService.getPlayerProfile("h2-season-puuid", "euw1", "MainPlayer", "EUW", explicitRefresh = true)
                }
            }.exceptionOrNull()
            // Missing TransactionTemplate MUST fail closed (never hide durability failure or write uncommitted Redis profile)
            (unwiredError is IllegalStateException) shouldBe true
            playerSeasonStatsRepository.findByRegionAndPuuidAndSeasonYearAndQueueId("euw1", "h2-season-puuid", 2026, 420) shouldBe null
            verify(exactly = 0) {
                springWiredRedisCacheService.set(
                    "player:profile:euw1:h2-season-puuid",
                    any<com.pride.summoner_searcher_api.dto.SummonerProfileDto>(),
                    any()
                )
            }

            // 2. Seed an actual stale-schema row (schemaVersion = 0) for h2-season-puuid in H2 before refreshing,
            //    proving that a stale-schema row is atomically rebuilt and replaced in-place without merge rejection!
            val seededStaleRow = playerSeasonStatsRepository.saveAndFlush(
                com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                    region = "euw1",
                    puuid = "h2-season-puuid",
                    seasonYear = 2026,
                    queueId = 420,
                    schemaVersion = 0,
                    historyCapped = false,
                    historyComplete = false,
                    foldedGamesCount = 99,
                    pendingGamesCount = 4,
                    stateJson = """{"schemaVersion":0,"corrupt":true}""",
                    updatedAt = fixedNow.minusSeconds(3600)
                )
            )
            val staleRowId = seededStaleRow.id
            val staleRowInitialVersion = seededStaleRow.version

            val wiredCacheService = com.pride.summoner_searcher_api.service.PlayerCacheService(
                redisCacheService = springWiredRedisCacheService,
                riotApiService = springWiredRiotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = playerSeasonStatsRepository,
                transactionTemplate = transactionTemplate,
                objectMapper = objectMapper
            )
            io.mockk.coEvery { springWiredRiotApiService.fetchMatchIdsSince("h2-season-puuid", "euw1", any()) } returns
                listOf("EUW1_NEW_2000", "EUW1_OLD_1000")

            val firstWiredProfile = kotlinx.coroutines.runBlocking {
                wiredCacheService.getPlayerProfile("h2-season-puuid", "euw1", "MainPlayer", "EUW", explicitRefresh = true)
            }
            firstWiredProfile?.totalMatches shouldBe 2

            // Verify within-batch monotonic lastSeenAt participant indexing in H2:
            // Both EUW1_NEW_2000 and EUW1_OLD_1000 were indexed in the same batch; H2 MUST hold "NewRiotName#NEW1" (newerCreationMs)
            val indexedCoPlayerAfterBatch1 = indexedPlayerRepository.findById("co-player-puuid").orElse(null)
            (indexedCoPlayerAfterBatch1 != null) shouldBe true
            indexedCoPlayerAfterBatch1!!.gameName shouldBe "NewRiotName"
            indexedCoPlayerAfterBatch1.tagLine shouldBe "NEW1"
            indexedCoPlayerAfterBatch1.summonerLevel shouldBe 350L
            indexedCoPlayerAfterBatch1.profileIconId shouldBe 99
            indexedCoPlayerAfterBatch1.lastSeenAt shouldBe Instant.ofEpochMilli(newerCreationMs)

            // Verify the stale-schema row was atomically replaced in-place (same row ID, incremented @Version, schemaVersion = 1)
            val replacedStaleRow = playerSeasonStatsRepository.findByRegionAndPuuidAndSeasonYearAndQueueId(
                "euw1",
                "h2-season-puuid",
                2026,
                420
            )
            (replacedStaleRow != null) shouldBe true
            replacedStaleRow!!.id shouldBe staleRowId
            replacedStaleRow.schemaVersion shouldBe com.pride.summoner_searcher_api.service.DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION
            replacedStaleRow.foldedGamesCount shouldBe 2
            replacedStaleRow.pendingGamesCount shouldBe 0
            replacedStaleRow.historyComplete shouldBe true
            (replacedStaleRow.version > staleRowInitialVersion) shouldBe true

            // 3. Incrementally add an even older backfilled match ("EUW1_ANCIENT_500")
            io.mockk.coEvery { springWiredRiotApiService.fetchMatchIdsSince("h2-season-puuid", "euw1", any()) } returns
                listOf("EUW1_NEW_2000", "EUW1_OLD_1000", "EUW1_ANCIENT_500")

            val wiredProfile = kotlinx.coroutines.runBlocking {
                wiredCacheService.getPlayerProfile("h2-season-puuid", "euw1", "MainPlayer", "EUW", explicitRefresh = true)
            }
            wiredProfile?.totalMatches shouldBe 3

            // Verify H2 indexed_players row was NOT overwritten by EUW1_ANCIENT_500 ("AncientRiotName#ANC1")
            val indexedCoPlayerAfterAncientMatch = indexedPlayerRepository.findById("co-player-puuid").orElse(null)
            indexedCoPlayerAfterAncientMatch!!.gameName shouldBe "NewRiotName"
            indexedCoPlayerAfterAncientMatch.tagLine shouldBe "NEW1"
            indexedCoPlayerAfterAncientMatch.lastSeenAt shouldBe Instant.ofEpochMilli(newerCreationMs)

            // 4. Test concurrent transactions racing newer vs older lastSeenAt upserts for the same PUUID in H2
            val racePuuid = "concurrent-race-puuid"
            val threadCount = 6
            val startBarrier = java.util.concurrent.CyclicBarrier(threadCount)
            val executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount)
            val newestRaceTs = Instant.ofEpochMilli(newerCreationMs + 60_000L)
            try {
                val futures = (0 until threadCount).map { workerIdx ->
                    executor.submit<Int> {
                        val isNewestWorker = workerIdx == 2
                        val candidateTs = if (isNewestWorker) {
                            newestRaceTs
                        } else {
                            Instant.ofEpochMilli(ancientCreationMs + workerIdx * 1_000L)
                        }
                        val candidateName = if (isNewestWorker) "WinnerNewestName" else "StaleWorker_$workerIdx"
                        val candidateTag = if (isNewestWorker) "WIN1" else "OLD$workerIdx"
                        startBarrier.await(5, java.util.concurrent.TimeUnit.SECONDS)
                        transactionTemplate.execute {
                            indexedPlayerRepository.upsertPlayerIfNewer(
                                puuid = racePuuid,
                                gameName = candidateName,
                                tagLine = candidateTag,
                                region = "euw1",
                                profileIconId = if (isNewestWorker) 888 else 10 + workerIdx,
                                summonerLevel = if (isNewestWorker) 777L else 100L + workerIdx,
                                lastSeenAt = candidateTs
                            )
                        } ?: 0
                    }
                }
                futures.forEach { it.get(5, java.util.concurrent.TimeUnit.SECONDS) }
            } finally {
                executor.shutdownNow()
            }

            val raceWinner = indexedPlayerRepository.findById(racePuuid).orElse(null)
            (raceWinner != null) shouldBe true
            raceWinner!!.gameName shouldBe "WinnerNewestName"
            raceWinner.tagLine shouldBe "WIN1"
            raceWinner.profileIconId shouldBe 888
            raceWinner.summonerLevel shouldBe 777L
            raceWinner.lastSeenAt shouldBe newestRaceTs

            // Verify H2 player_season_stats row committed atomically with @Version
            val savedSeasonRow = playerSeasonStatsRepository.findByRegionAndPuuidAndSeasonYearAndQueueId(
                "euw1",
                "h2-season-puuid",
                2026,
                420
            )
            (savedSeasonRow != null) shouldBe true
            savedSeasonRow!!.foldedGamesCount shouldBe 3
            savedSeasonRow.historyComplete shouldBe true
            savedSeasonRow.pendingGamesCount shouldBe 0

            // 5. Seed partial-history, capped-history, and stale-schema player rows in H2 and verify scalar SQL shape + operator coverage summary
            playerSeasonStatsRepository.saveAllAndFlush(
                listOf(
                    com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                        region = "euw1",
                        puuid = "h2-partial-puuid",
                        seasonYear = 2026,
                        queueId = 420,
                        schemaVersion = 1,
                        historyCapped = false,
                        historyComplete = false,
                        foldedGamesCount = 40,
                        pendingGamesCount = 5,
                        stateJson = "{}",
                        updatedAt = fixedNow
                    ),
                    com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                        region = "euw1",
                        puuid = "h2-capped-puuid",
                        seasonYear = 2026,
                        queueId = 420,
                        schemaVersion = 1,
                        historyCapped = true,
                        historyComplete = false,
                        foldedGamesCount = 2100,
                        pendingGamesCount = 0,
                        stateJson = "{}",
                        updatedAt = fixedNow
                    ),
                    com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity(
                        region = "euw1",
                        puuid = "h2-stale-schema-puuid",
                        seasonYear = 2026,
                        queueId = 420,
                        schemaVersion = 0,
                        historyCapped = false,
                        historyComplete = true,
                        foldedGamesCount = 12,
                        pendingGamesCount = 0,
                        stateJson = "{}",
                        updatedAt = fixedNow
                    )
                )
            )

            CapturingStatementInspector.clear()
            val coverageSummary = springWiredCohortBenchmarkService.getDiscoveryAndSeasonCoverageSummary("euw1", 2026)
            val coverageSql = CapturingStatementInspector.executedSql.toList()

            // Verify scalar aggregate SQL does NOT select state_json, samples_json, or metrics_json
            val seasonSummarySql = coverageSql.single { it.contains("player_season_stats", ignoreCase = true) }
            seasonSummarySql shouldNotContainIgnoringCase "state_json"
            coverageSql.forEach { sql ->
                sql shouldNotContainIgnoringCase "state_json"
                sql shouldNotContainIgnoringCase "samples_json"
                sql shouldNotContainIgnoringCase "metrics_json"
            }

            coverageSummary.identityIndexCoverage.totalIndexedPlayersGlobal shouldBe 3L
            coverageSummary.identityIndexCoverage.targetRegionIndexedPlayers shouldBe 3L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.trackedViewedPlayers shouldBe 4L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.completeHistoryPlayers shouldBe 1L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.partialHistoryPlayers shouldBe 2L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.cappedHistoryPlayers shouldBe 1L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.staleSchemaVersionPlayers shouldBe 1L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.totalFoldedSeasonGames shouldBe 2155L
            coverageSummary.seasonHistoryCompleteness.targetRegionStats.totalPendingGapGames shouldBe 5L

            // Verify coverage SQL failure or unwired repository NEVER fakes zero counts
            val brokenIdentityRepo = io.mockk.mockk<IndexedPlayerRepository>()
            io.mockk.every {
                brokenIdentityRepo.countPlayersGroupedByRegion()
            } throws org.springframework.dao.DataAccessResourceFailureException("Simulated indexed_players SQL failure")
            val brokenCoverageService = CohortBenchmarkService(
                redisCacheService = springWiredRedisCacheService,
                riotApiService = springWiredRiotApiService,
                riotApiRateLimiter = RiotApiRateLimiter(clock),
                cohortBenchmarkCellRepository = benchmarkCellRepository,
                cohortCellSampleStateRepository = sampleStateRepository,
                cohortCollectorCheckpointRepository = checkpointRepository,
                indexedPlayerRepository = brokenIdentityRepo,
                playerSeasonStatsRepository = playerSeasonStatsRepository,
                transactionTemplate = transactionTemplate,
                objectMapper = objectMapper
            )
            val coverageSqlError = runCatching {
                brokenCoverageService.getDiscoveryAndSeasonCoverageSummary("euw1", 2026)
            }.exceptionOrNull()
            (coverageSqlError is org.springframework.dao.DataAccessResourceFailureException) shouldBe true

            val unwiredCoverageService = CohortBenchmarkService(
                redisCacheService = springWiredRedisCacheService,
                riotApiService = springWiredRiotApiService,
                riotApiRateLimiter = RiotApiRateLimiter(clock),
                cohortBenchmarkCellRepository = benchmarkCellRepository,
                cohortCellSampleStateRepository = sampleStateRepository,
                cohortCollectorCheckpointRepository = checkpointRepository,
                indexedPlayerRepository = null,
                playerSeasonStatsRepository = null,
                transactionTemplate = transactionTemplate,
                objectMapper = objectMapper
            )
            val unwiredCoverageError = runCatching {
                unwiredCoverageService.getDiscoveryAndSeasonCoverageSummary("euw1", 2026)
            }.exceptionOrNull()
            (unwiredCoverageError is IllegalStateException) shouldBe true
        } finally {
            playerSeasonStatsRepository.deleteAll()
            indexedPlayerRepository.deleteAll()
        }
    }

    @Test
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED
    )
    fun `collector resolved matches populate IndexedPlayerRepository with bounds and isolation`() {
        playerSeasonStatsRepository.deleteAll()
        indexedPlayerRepository.deleteAll()
        sampleStateRepository.deleteAll()
        benchmarkCellRepository.deleteAll()
        checkpointRepository.deleteAll()
        clearMocks(springWiredRedisCacheService, springWiredRiotApiService)

        try {
            val objectMapper = ObjectMapper().findAndRegisterModules()
            val fixedNow = Instant.parse("2026-11-06T12:00:00Z")
            val nowMs = fixedNow.toEpochMilli()
            val clock = object : com.pride.summoner_searcher_api.service.TimeProvider {
                override fun now(): Long = nowMs
            }

            fun buildMatchWithParticipants(
                matchId: String,
                gameCreationMs: Long,
                participants: List<Triple<String, String, Pair<String, String>>>
            ): com.pride.summoner_searcher_api.dto.MatchDto {
                val participantsJson = participants.mapIndexed { idx, (puuid, role, riotId) ->
                    val (gameName, tagLine) = riotId
                    """
                    {
                      "puuid": "$puuid",
                      "riotIdGameName": "$gameName",
                      "riotIdTagline": "$tagLine",
                      "profileIcon": ${100 + idx},
                      "summonerLevel": ${200 + idx},
                      "teamId": ${if (idx < 5) 100 else 200},
                      "teamPosition": "$role",
                      "individualPosition": "$role",
                      "kills": 6,
                      "deaths": 2,
                      "assists": 8,
                      "totalMinionsKilled": 210,
                      "neutralMinionsKilled": 12,
                      "goldEarned": 13500,
                      "totalDamageDealtToChampions": 26000,
                      "visionScore": 35,
                      "totalTimeSpentDead": 60,
                      "win": ${idx < 5},
                      "challenges": {
                        "laneMinionsFirst10Minutes": 84,
                        "jungleCsBefore10Minutes": 4.0,
                        "goldPerMinute": 450.0,
                        "damagePerMinute": 866.0,
                        "visionScorePerMinute": 1.16,
                        "killParticipation": 0.70
                      }
                    }
                    """.trimIndent()
                }.joinToString(",\n")

                val pListJson = participants.joinToString(",") { "\"${it.first}\"" }
                val json = """
                {
                  "metadata": {
                    "matchId": "$matchId",
                    "participants": [$pListJson]
                  },
                  "info": {
                    "gameCreation": $gameCreationMs,
                    "gameDuration": 1800,
                    "gameId": ${gameCreationMs % 100000},
                    "queueId": 420,
                    "gameMode": "CLASSIC",
                    "gameVersion": "16.22.615.1000",
                    "participants": [$participantsJson]
                  }
                }
                """.trimIndent()
                return objectMapper.readValue(json, com.pride.summoner_searcher_api.dto.MatchDto::class.java)
            }

            // Prepare rate limiter with verified App + Method headers so collector batch runs cleanly
            val rateLimiter = com.pride.summoner_searcher_api.service.RiotApiRateLimiter(clock)
            kotlinx.coroutines.runBlocking {
                for (ep in listOf("league-v4-entries-by-puuid", "match-v5-ids-by-puuid", "match-v5-match-by-id")) {
                    rateLimiter.updateLimits(
                        region = "euw1",
                        limitHeader = "20:1,100:120",
                        countHeader = "1:1,5:120",
                        endpointKey = ep,
                        methodLimitHeader = "500:10",
                        methodCountHeader = "1:10"
                    )
                }
            }

            val redisStore = java.util.concurrent.ConcurrentHashMap<String, Any>()
            redisStore["cohort:patch:external:ddragon"] = "16.22"
            redisStore["cohort:rank:snapshot:euw1:seed-top-puuid"] = com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto(
                puuid = "seed-top-puuid",
                region = "euw1",
                tier = "DIAMOND",
                rank = "I",
                leaguePoints = 75,
                queueType = "RANKED_SOLO_5x5",
                snapshotAt = fixedNow.minusSeconds(600)
            )

            // Match 1 (older, cached in Redis, t = nowMs - 7_200_000L) and Match 2 (newer, fetched via low-priority Riot API, t = nowMs - 3_600_000L)
            // Both contain all 5 roles (TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY) and share "seed-top-puuid" + "shared-jg-puuid" to test within-batch dedup!
            val olderGameMs = nowMs - 7_200_000L
            val newerGameMs = nowMs - 3_600_000L
            val match1Cached = buildMatchWithParticipants(
                matchId = "EUW1_COLL_1",
                gameCreationMs = olderGameMs,
                participants = listOf(
                    Triple("seed-top-puuid", "TOP", "SeedTopOld" to "OLD"),
                    Triple("shared-jg-puuid", "JUNGLE", "SharedJgOld" to "OLD"),
                    Triple("m1-mid-puuid", "MIDDLE", "M1Mid" to "EUW"),
                    Triple("m1-bot-puuid", "BOTTOM", "M1Bot" to "EUW"),
                    Triple("m1-sup-puuid", "UTILITY", "M1Sup" to "EUW")
                )
            )
            val match2Fetched = buildMatchWithParticipants(
                matchId = "EUW1_COLL_2",
                gameCreationMs = newerGameMs,
                participants = listOf(
                    Triple("seed-top-puuid", "TOP", "SeedTopNew" to "NEW"),
                    Triple("shared-jg-puuid", "JUNGLE", "SharedJgNew" to "NEW"),
                    Triple("m2-mid-puuid", "MIDDLE", "M2Mid" to "EUW"),
                    Triple("m2-bot-puuid", "BOTTOM", "M2Bot" to "EUW"),
                    Triple("m2-sup-puuid", "UTILITY", "M2Sup" to "EUW")
                )
            )
            redisStore["match:euw1:EUW1_COLL_1"] = match1Cached

            every { springWiredRedisCacheService.get(any(), any<Class<Any>>()) } answers {
                redisStore[firstArg<String>()]
            }
            every { springWiredRedisCacheService.set(any(), any<Any>(), any()) } answers {
                redisStore[firstArg<String>()] = secondArg<Any>()
            }
            every { springWiredRedisCacheService.setIfAbsent(any(), any(), any()) } returns true
            every { springWiredRedisCacheService.delete(any()) } answers {
                redisStore.remove(firstArg<String>())
            }

            io.mockk.coEvery {
                springWiredRiotApiService.getMatchIdsByPuuidLowPriority(
                    puuid = "seed-top-puuid",
                    region = "euw1",
                    queueId = 420,
                    count = any(),
                    start = 0,
                    maxRetries = 0,
                    httpAttemptCounter = any(),
                    failureReasonHolder = any()
                )
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                listOf("EUW1_COLL_2", "EUW1_COLL_1")
            }

            io.mockk.coEvery {
                springWiredRiotApiService.getMatchByIdLowPriority(
                    matchId = "EUW1_COLL_2",
                    region = "euw1",
                    maxRetries = 0,
                    httpAttemptCounter = any(),
                    failureReasonHolder = any()
                )
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(3).incrementAndGet()
                match2Fetched
            }

            val collectorService = CohortBenchmarkService(
                redisCacheService = springWiredRedisCacheService,
                cohortBenchmarkCellRepository = benchmarkCellRepository,
                riotApiRateLimiter = rateLimiter,
                riotApiService = springWiredRiotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = clock,
                cohortCollectorCheckpointRepository = checkpointRepository,
                cohortCellSampleStateRepository = sampleStateRepository,
                transactionTemplate = transactionTemplate,
                indexedPlayerRepository = indexedPlayerRepository,
                playerSeasonStatsRepository = playerSeasonStatsRepository
            )

            // 1. Actual identity growth + within-batch deduplication + zero added network calls
            val coverageBefore = collectorService.getDiscoveryAndSeasonCoverageSummary("euw1", 2026)
            coverageBefore.identityIndexCoverage.targetRegionIndexedPlayers shouldBe 0L

            val batchStartNs = System.nanoTime()
            val checkpoint = kotlinx.coroutines.runBlocking {
                collectorService.runResumableLowPriorityCollectorBatch(
                    region = "euw1",
                    targetPatch = "16.22",
                    seedPuuids = listOf("seed-top-puuid"),
                    maxPlayersPerBatch = 1,
                    maxGamesPerPlayer = 10
                )
            }
            val batchElapsedMs = (System.nanoTime() - batchStartNs) / 1_000_000.0

            checkpoint.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
            // Exactly 2 Riot network calls: 1 getMatchIdsByPuuidLowPriority + 1 getMatchByIdLowPriority for EUW1_COLL_2 (EUW1_COLL_1 was cached)
            checkpoint.totalNetworkCallsMade shouldBe 2
            checkpoint.fetchedMatchIdLists shouldBe 1
            checkpoint.fetchedMatches shouldBe 1
            io.mockk.coVerify(exactly = 1) {
                springWiredRiotApiService.getMatchIdsByPuuidLowPriority(any(), any(), any(), any(), any(), any(), any(), any())
            }
            io.mockk.coVerify(exactly = 1) {
                springWiredRiotApiService.getMatchByIdLowPriority(any(), any(), any(), any(), any())
            }
            io.mockk.coVerify(exactly = 0) {
                springWiredRiotApiService.getSummonerByPuuid(any(), any())
            }
            io.mockk.coVerify(exactly = 0) {
                springWiredRiotApiService.fetchLeagueRankLowPriority(any(), any(), any(), any(), any())
            }

            // Verify all 8 distinct participants across both matches (including JUNGLE and UTILITY) are indexed in H2
            val coverageAfterBatch = collectorService.getDiscoveryAndSeasonCoverageSummary("euw1", 2026)
            coverageAfterBatch.identityIndexCoverage.targetRegionIndexedPlayers shouldBe 8L
            coverageAfterBatch.identityIndexCoverage.totalIndexedPlayersGlobal shouldBe 8L
            collectorService.getCollectorParticipantIndexingCounts()["collectorIndexedPlayersUpserted"] shouldBe 8L
            // Viewed-player season stats remain untouched by collector (0 rows)
            playerSeasonStatsRepository.count() shouldBe 0L

            // Verify within-batch deduplication kept the NEWER match's Riot ID and timestamp for shared participants
            val seedIndexed = indexedPlayerRepository.findById("seed-top-puuid").orElseThrow()
            seedIndexed.gameName shouldBe "SeedTopNew"
            seedIndexed.tagLine shouldBe "NEW"
            seedIndexed.lastSeenAt.toEpochMilli() shouldBe newerGameMs

            val sharedJgIndexed = indexedPlayerRepository.findById("shared-jg-puuid").orElseThrow()
            sharedJgIndexed.gameName shouldBe "SharedJgNew"
            sharedJgIndexed.tagLine shouldBe "NEW"
            sharedJgIndexed.lastSeenAt.toEpochMilli() shouldBe newerGameMs

            // Verify collector pendingCandidatePuuids only queued eligible roles (TOP/MIDDLE/BOTTOM), NOT JUNGLE or UTILITY
            checkpoint.pendingCandidatePuuids.contains("shared-jg-puuid") shouldBe false
            checkpoint.pendingCandidatePuuids.contains("m1-sup-puuid") shouldBe false
            checkpoint.pendingCandidatePuuids.contains("m2-sup-puuid") shouldBe false
            checkpoint.pendingCandidatePuuids.containsAll(listOf("m1-mid-puuid", "m1-bot-puuid", "m2-mid-puuid", "m2-bot-puuid")) shouldBe true

            // 2. Newer-only monotonic lastSeenAt: resolving an older match later NEVER overwrites newer identity
            val staleMatch = buildMatchWithParticipants(
                matchId = "EUW1_COLL_STALE",
                gameCreationMs = olderGameMs - 500_000L,
                participants = listOf(
                    Triple("seed-top-puuid", "TOP", "StaleOverwriteAttempt" to "STALE")
                )
            )
            val staleOutcome = collectorService.indexResolvedCollectorMatchesSafely(listOf(staleMatch), "euw1")
            staleOutcome.upsertedCount shouldBe 0L
            staleOutcome.staleSkippedCount shouldBe 1L
            val seedAfterStale = indexedPlayerRepository.findById("seed-top-puuid").orElseThrow()
            seedAfterStale.gameName shouldBe "SeedTopNew"
            seedAfterStale.tagLine shouldBe "NEW"
            seedAfterStale.lastSeenAt.toEpochMilli() shouldBe newerGameMs

            // 3. Bounds enforcement: >20 matches (25 matches) and >200 unique participants (25 * 10 = 250 participants)
            val twentyFiveMatches = (1..25).map { mIdx ->
                val matchTime = newerGameMs + mIdx * 1000L
                val tenParticipants = (1..10).map { pIdx ->
                    val pid = "bounded-m$mIdx-p$pIdx"
                    Triple(pid, "TOP", "BoundedName_${mIdx}_$pIdx" to "EUW")
                }
                buildMatchWithParticipants("EUW1_BOUND_$mIdx", matchTime, tenParticipants)
            }
            val boundStartNs = System.nanoTime()
            val boundedOutcome = collectorService.indexResolvedCollectorMatchesSafely(twentyFiveMatches, "euw1")
            val boundElapsedMs = (System.nanoTime() - boundStartNs) / 1_000_000.0
            boundedOutcome.inspectedMatchesCount shouldBe 20
            boundedOutcome.uniqueCandidatesCount shouldBe 200
            boundedOutcome.upsertedCount shouldBe 200L
            // Matches 21..25 were beyond MAX_MATCHES_FOR_PARTICIPANT_INDEXING (20) and must not be indexed
            indexedPlayerRepository.findById("bounded-m21-p1").isPresent shouldBe false
            indexedPlayerRepository.findById("bounded-m20-p1").isPresent shouldBe true
            println("FINITE_OVERHEAD_METRIC: collector_batch_ms=$batchElapsedMs bounded_20m_200p_upsert_ms=$boundElapsedMs")

            // 4. Concurrency test: concurrent collector + profile refreshes racing older & newer timestamps on the same PUUID
            val cacheServiceForRace = com.pride.summoner_searcher_api.service.PlayerCacheService(
                redisCacheService = springWiredRedisCacheService,
                riotApiService = springWiredRiotApiService,
                indexedPlayerRepository = indexedPlayerRepository,
                timeProvider = clock,
                playerSeasonStatsRepository = playerSeasonStatsRepository,
                transactionTemplate = transactionTemplate,
                objectMapper = objectMapper
            )
            val raceTimestamps = (1..16).map { it * 1_000_000L }.shuffled()
            val racePool = java.util.concurrent.Executors.newFixedThreadPool(8)
            val startLatch = java.util.concurrent.CountDownLatch(1)
            try {
                val futures = raceTimestamps.mapIndexed { idx, ts ->
                    racePool.submit(java.util.concurrent.Callable {
                        startLatch.await()
                        val raceMatch = buildMatchWithParticipants(
                            matchId = "EUW1_RACE_$ts",
                            gameCreationMs = ts,
                            participants = listOf(
                                Triple("concurrent-shared-puuid", "MIDDLE", "RaceName_$ts" to "T$ts")
                            )
                        )
                        if (idx % 2 == 0) {
                            collectorService.indexResolvedCollectorMatchesSafely(listOf(raceMatch), "euw1")
                        } else {
                            cacheServiceForRace.indexPlayersFromMatches(listOf(raceMatch), "euw1")
                        }
                    })
                }
                startLatch.countDown()
                futures.forEach { it.get(15, java.util.concurrent.TimeUnit.SECONDS) }
            } finally {
                racePool.shutdownNow()
            }
            val raceWinner = indexedPlayerRepository.findById("concurrent-shared-puuid").orElseThrow()
            raceWinner.lastSeenAt.toEpochMilli() shouldBe 16_000_000L
            raceWinner.gameName shouldBe "RaceName_16000000"
            raceWinner.tagLine shouldBe "T16000000"

            // 5. Failure isolation & cancellation preservation:
            //    5a. Real SQL failure on indexed_players (where updatePlayerIfNewer/insertPlayerIfAbsent use Propagation.REQUIRES_NEW)
            //        MUST NOT fail or roll back actual cohort_cell_sample_states, cohort_benchmark_cells, or cohort_collector_checkpoints DB rows!
            redisStore["cohort:rank:snapshot:euw1:m1-mid-puuid"] = com.pride.summoner_searcher_api.dto.CohortRankSnapshotDto(
                puuid = "m1-mid-puuid",
                region = "euw1",
                tier = "EMERALD",
                rank = "II",
                leaguePoints = 50,
                queueType = "RANKED_SOLO_5x5",
                snapshotAt = fixedNow.minusSeconds(300)
            )
            io.mockk.coEvery {
                springWiredRiotApiService.getMatchIdsByPuuidLowPriority(
                    puuid = "m1-mid-puuid",
                    region = "euw1",
                    queueId = 420,
                    count = any(),
                    start = 0,
                    maxRetries = 0,
                    httpAttemptCounter = any(),
                    failureReasonHolder = any()
                )
            } answers {
                arg<java.util.concurrent.atomic.AtomicInteger>(6).incrementAndGet()
                listOf("EUW1_COLL_1")
            }

            transactionTemplate.executeWithoutResult {
                entityManager.createNativeQuery("ALTER TABLE indexed_players RENAME TO indexed_players_offline_test").executeUpdate()
            }
            val isolatedCheckpoint = try {
                kotlinx.coroutines.runBlocking {
                    collectorService.runResumableLowPriorityCollectorBatch(
                        region = "euw1",
                        targetPatch = "16.22",
                        seedPuuids = listOf("m1-mid-puuid"),
                        maxPlayersPerBatch = 1,
                        maxGamesPerPlayer = 10
                    )
                }
            } finally {
                transactionTemplate.executeWithoutResult {
                    entityManager.createNativeQuery("ALTER TABLE indexed_players_offline_test RENAME TO indexed_players").executeUpdate()
                }
            }

            isolatedCheckpoint.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
            collectorService.getCollectorParticipantIndexingCounts()["collectorIndexedPlayersFailures"] shouldBe 1L

            // Verify actual H2 rows in cohort_cell_sample_states, cohort_benchmark_cells, AND cohort_collector_checkpoints committed!
            val emeraldMidSample = sampleStateRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "EMERALD",
                role = "MIDDLE"
            )
            (emeraldMidSample != null) shouldBe true
            emeraldMidSample!!.eligibleGames shouldBe 1
            emeraldMidSample.distinctPlayers shouldBe 1

            val emeraldMidCell = benchmarkCellRepository.findByRegionAndPatchAndQueueIdAndTierAndRole(
                region = "euw1",
                patch = "16.22",
                queueId = 420,
                tier = "EMERALD",
                role = "MIDDLE"
            )
            (emeraldMidCell != null) shouldBe true
            emeraldMidCell!!.eligibleGames shouldBe 1
            emeraldMidCell.distinctPlayers shouldBe 1

            val persistedPatchCheckpoint = checkpointRepository.findByRegionAndTargetPatchAndQueueId(
                region = "euw1",
                targetPatch = "16.22",
                queueId = 420
            )
            val persistedLatestCheckpoint = checkpointRepository.findByRegionAndTargetPatchAndQueueId(
                region = "euw1",
                targetPatch = "latest",
                queueId = 420
            )
            (persistedPatchCheckpoint != null) shouldBe true
            (persistedLatestCheckpoint != null) shouldBe true
            persistedPatchCheckpoint!!.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
            persistedPatchCheckpoint.processedPuuidsCount shouldBe 2
            persistedPatchCheckpoint.totalNetworkCallsMade shouldBe 3
            val persistedCheckpointDto = objectMapper.readValue(
                persistedPatchCheckpoint.checkpointJson,
                CohortCollectorCheckpointDto::class.java
            )
            persistedCheckpointDto.lastStopReason shouldBe "BATCH_BUDGET_REACHED_RESUMABLE"
            persistedCheckpointDto.processedPuuids.containsAll(listOf("seed-top-puuid", "m1-mid-puuid")) shouldBe true
            persistedCheckpointDto.totalNetworkCallsMade shouldBe 3

            //    5b. CancellationException MUST be rethrown and never swallowed
            val cancellingIdentityRepo = io.mockk.mockk<IndexedPlayerRepository>()
            io.mockk.every {
                cancellingIdentityRepo.updatePlayerIfNewer(any(), any(), any(), any(), any(), any(), any())
            } throws kotlinx.coroutines.CancellationException("Simulated collector coroutine cancellation")
            val cancellingService = CohortBenchmarkService(
                redisCacheService = springWiredRedisCacheService,
                cohortBenchmarkCellRepository = benchmarkCellRepository,
                riotApiRateLimiter = rateLimiter,
                riotApiService = springWiredRiotApiService,
                objectMapper = objectMapper,
                networkCollectionEnabled = true,
                timeProvider = clock,
                cohortCollectorCheckpointRepository = checkpointRepository,
                cohortCellSampleStateRepository = sampleStateRepository,
                transactionTemplate = transactionTemplate,
                indexedPlayerRepository = cancellingIdentityRepo,
                playerSeasonStatsRepository = playerSeasonStatsRepository
            )
            val cancellationThrown = runCatching {
                cancellingService.indexResolvedCollectorMatchesSafely(listOf(match1Cached), "euw1")
            }.exceptionOrNull()
            (cancellationThrown is kotlinx.coroutines.CancellationException) shouldBe true
        } finally {
            playerSeasonStatsRepository.deleteAll()
            indexedPlayerRepository.deleteAll()
            sampleStateRepository.deleteAll()
            benchmarkCellRepository.deleteAll()
            checkpointRepository.deleteAll()
        }
    }
}



