package com.pride.summoner_searcher_api.tools

import com.pride.summoner_searcher_api.dto.CachedLeaderboardDto
import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.LeagueListDTO
import com.pride.summoner_searcher_api.service.ChallengerLeagueService
import com.pride.summoner_searcher_api.service.MemoryStatsProvider
import com.pride.summoner_searcher_api.service.RawMemorySample
import com.pride.summoner_searcher_api.service.RedisCacheService
import com.pride.summoner_searcher_api.service.RiotApiService
import com.pride.summoner_searcher_api.service.TimeProvider
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ExecutionOptions
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ItemStatus
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.KeyCoordinationMode
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ManifestEntry
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.RunOutcome
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.SnapshotManifest
import com.sun.net.httpserver.HttpServer
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import javax.sql.DataSource

/**
 * End-to-end qualification suite running against a real isolated local PostgreSQL cluster
 * (`initdb` + `pg_ctl` on an ephemeral localhost port) and local mock Riot Account-V1 HTTP server:
 * - Zero live Riot API calls, zero live DB connections, zero existing credentials used.
 * - Verifies pre-migration missing-table compatibility (`ChallengerLeagueService` + `OffProdChallengerIdentityCompletionCli`).
 * - Verifies default dry-run zero effects (`0` HTTP calls, `0` DB writes).
 * - Verifies reviewed additive migration SQL (`V20261109__create_player_riot_identities.sql`) and non-blank `CHECK` constraint.
 * - Verifies partial completion and deterministic resume via `--output-remaining-manifest-file` across:
 *   `404`, omitted fields, `429` rate limit, wall-time budget exhaustion, and `unknown_commit`.
 * - Verifies full `191`-candidate (`31` EUW + `160` KR) backfill, `600/600` (`300` EUW + `300` KR)
 *   `ChallengerLeagueService` read consumer coverage (`indexed-first`, `0` Account-V1 fanout),
 *   and `139a7de48e67af9d664c08fd61ce131b6e63bd74` read/autocomplete compatibility.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OffProdChallengerIdentityPostgresQualificationTest {

    private var pgBinDir: File? = null
    private var pgTempDir: Path? = null
    private var pgPort: Int = 0
    private lateinit var jdbcUrl: String
    private lateinit var pgDataSource: PGSimpleDataSource

    private fun deterministicPuuid(region: String, index: Int): String {
        val raw = "realpg-${region.lowercase()}-challenger-${index.toString().padStart(4, '0')}-qualification-"
        val repeated = raw.repeat((78 / raw.length) + 2)
        return repeated.substring(0, 78)
    }

    private fun findPostgresBinDir(): File? {
        val root = File("/usr/lib/postgresql")
        if (!root.isDirectory) return null
        return root.listFiles()
            ?.filter { it.isDirectory && File(it, "bin/initdb").canExecute() && File(it, "bin/pg_ctl").canExecute() }
            ?.sortedByDescending { it.name }
            ?.firstOrNull()
            ?.let { File(it, "bin") }
    }

    @BeforeAll
    fun startIsolatedPostgres() {
        val binDir = findPostgresBinDir()
        check(binDir != null) {
            "Local PostgreSQL binaries (/usr/lib/postgresql/*/bin/initdb) required for real PG qualification; test skips are forbidden"
        }
        pgBinDir = binDir

        val tempRoot = Files.createTempDirectory("offprod-pg-qual-")
        pgTempDir = tempRoot
        val dataDir = tempRoot.resolve("data").toFile()
        val socketDir = tempRoot.resolve("sock").toFile().apply { mkdirs() }
        val logFile = tempRoot.resolve("postgres.log").toFile()

        pgPort = ServerSocket(0).use { it.localPort }

        val initProc = ProcessBuilder(
            File(binDir, "initdb").absolutePath,
            "-D", dataDir.absolutePath,
            "-U", "postgres",
            "-A", "trust",
            "--encoding=UTF8",
            "--no-locale"
        ).redirectErrorStream(true).start()
        val initOut = initProc.inputStream.bufferedReader().readText()
        check(initProc.waitFor() == 0) { "initdb failed: $initOut" }

        val startProc = ProcessBuilder(
            File(binDir, "pg_ctl").absolutePath,
            "-D", dataDir.absolutePath,
            "-l", logFile.absolutePath,
            "-w",
            "-o", "-F -p $pgPort -h 127.0.0.1 -k ${socketDir.absolutePath}",
            "start"
        ).redirectErrorStream(true).start()
        val startOut = startProc.inputStream.bufferedReader().readText()
        check(startProc.waitFor() == 0) { "pg_ctl start failed: $startOut\nLog:\n${logFile.takeIf { it.exists() }?.readText()}" }

        jdbcUrl = "jdbc:postgresql://127.0.0.1:$pgPort/postgres"
        pgDataSource = PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = "postgres"
            password = ""
            loginTimeout = 2
            connectTimeout = 2
            socketTimeout = 2
        }
    }

    @AfterAll
    fun stopIsolatedPostgres() {
        val binDir = pgBinDir
        val tempRoot = pgTempDir
        if (binDir != null && tempRoot != null) {
            val dataDir = tempRoot.resolve("data").toFile()
            if (dataDir.exists()) {
                runCatching {
                    ProcessBuilder(
                        File(binDir, "pg_ctl").absolutePath,
                        "-D", dataDir.absolutePath,
                        "-m", "immediate",
                        "-w",
                        "stop"
                    ).start().waitFor()
                }
            }
            runCatching {
                tempRoot.toFile().deleteRecursively()
            }
        }
    }

    private fun resetBaseSchemaAndSeed600Fixture() {
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("DROP TABLE IF EXISTS player_riot_identities")
                stmt.execute("DROP TABLE IF EXISTS indexed_players")
                stmt.execute(
                    """
                    CREATE TABLE indexed_players (
                        puuid VARCHAR(255) PRIMARY KEY,
                        game_name VARCHAR(255) NOT NULL,
                        tag_line VARCHAR(255) NOT NULL,
                        region VARCHAR(255) NOT NULL,
                        profile_icon_id INT NOT NULL,
                        summoner_level BIGINT NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL
                    )
                    """.trimIndent()
                )
                stmt.execute(
                    """
                    CREATE INDEX idx_indexed_players_region_gamename_lower
                    ON indexed_players (region, lower(game_name) varchar_pattern_ops)
                    """.trimIndent()
                )
            }
            conn.prepareStatement(
                """
                INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.trimIndent()
            ).use { ps ->
                val baseSeenAt = Instant.parse("2026-11-01T12:00:00Z")
                // 269 complete EUW players (1..269), 31 absent (270..300)
                for (i in 1..269) {
                    ps.setString(1, deterministicPuuid("euw1", i))
                    ps.setString(2, "EuwIndexed$i")
                    ps.setString(3, "EUW")
                    ps.setString(4, "euw1")
                    ps.setInt(5, 100 + (i % 50))
                    ps.setLong(6, 500L + i)
                    ps.setTimestamp(7, Timestamp.from(baseSeenAt.plusSeconds(i.toLong())))
                    ps.addBatch()
                }

                // 140 complete KR players (1..140), 160 absent (141..300)
                for (i in 1..140) {
                    ps.setString(1, deterministicPuuid("kr", i))
                    ps.setString(2, "KrIndexed$i")
                    ps.setString(3, "KR1")
                    ps.setString(4, "kr")
                    ps.setInt(5, 200 + (i % 50))
                    ps.setLong(6, 600L + i)
                    ps.setTimestamp(7, Timestamp.from(baseSeenAt.plusSeconds(i.toLong())))
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    @Test
    fun `real Postgres e2e covers dry-run, migration, 191 backfill, 600 read coverage, and 139a7de compatibility`() = runBlocking {
        resetBaseSchemaAndSeed600Fixture()

        val missingEuwCandidates = (270..300).map { i ->
            ManifestEntry(puuid = deterministicPuuid("euw1", i), platformRegion = "euw1")
        }
        val missingKrCandidates = (141..300).map { i ->
            ManifestEntry(puuid = deterministicPuuid("kr", i), platformRegion = "kr")
        }
        val all191Missing = missingEuwCandidates + missingKrCandidates
        assertEquals(31, missingEuwCandidates.size)
        assertEquals(160, missingKrCandidates.size)
        assertEquals(191, all191Missing.size)

        val workDir = Files.createTempDirectory("offprod-cli-e2e-")
        val manifestFile = workDir.resolve("manifest-191.json").toFile()
        val dryRunReportFile = workDir.resolve("dry-run-report.json").toFile()
        val liveReportFile = workDir.resolve("live-report.json").toFile()
        val remainingManifestFile = workDir.resolve("remaining-manifest.json").toFile()
        val idempotentReportFile = workDir.resolve("idempotent-report.json").toFile()

        OffProdChallengerIdentityCompletionCli.writeSnapshotManifest(
            file = manifestFile,
            snapshotCapturedAtUtc = "2026-11-09T11:34:00Z",
            entries = all191Missing
        )

        val httpCalls = AtomicInteger(0)
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool()
        server.createContext("/riot/account/v1/accounts/by-puuid/") { exchange ->
            val count = httpCalls.incrementAndGet()
            val puuid = exchange.requestURI.path.substringAfterLast("/")
            val euwIdx = (270..300).firstOrNull { deterministicPuuid("euw1", it) == puuid }
            val krIdx = (141..300).firstOrNull { deterministicPuuid("kr", it) == puuid }
            val bodyBytes = when {
                euwIdx != null -> """{"puuid":"$puuid","gameName":"RiotEuw$euwIdx","tagLine":"EUW"}""".toByteArray(StandardCharsets.UTF_8)
                krIdx != null -> """{"puuid":"$puuid","gameName":"RiotKr$krIdx","tagLine":"KR1"}""".toByteArray(StandardCharsets.UTF_8)
                else -> """{"status":{"message":"Not found","status_code":404}}""".toByteArray(StandardCharsets.UTF_8)
            }
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.responseHeaders.add("X-App-Rate-Limit", "500:1,1000:120")
            exchange.responseHeaders.add("X-App-Rate-Limit-Count", "$count:1,$count:120")
            exchange.responseHeaders.add("X-Method-Rate-Limit", "1000:60")
            exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "$count:60")
            exchange.sendResponseHeaders(if (euwIdx != null || krIdx != null) 200 else 404, bodyBytes.size.toLong())
            exchange.responseBody.use { it.write(bodyBytes) }
        }
        server.start()

        try {
            val port = server.address.port
            val redisStore = ConcurrentHashMap<String, CachedLeaderboardDto>()
            val redisCacheService = mockk<RedisCacheService>()
            every { redisCacheService.get(any(), CachedLeaderboardDto::class.java) } answers {
                redisStore[firstArg()]
            }
            every { redisCacheService.set(any(), any<CachedLeaderboardDto>(), any()) } answers {
                redisStore[firstArg()] = secondArg()
            }

            val leagueCalls = AtomicInteger(0)
            val riotApiService = mockk<RiotApiService>()
            coEvery {
                riotApiService.fetchChallengerLeagueBounded(
                    region = any(),
                    queue = any(),
                    priority = any(),
                    maxRetries = any(),
                    deadlineAtMs = any(),
                    maxResponseBytes = any(),
                    maxEntries = any(),
                    headroomGuard = any(),
                    httpAttemptCounter = any(),
                    failureReasonHolder = any()
                )
            } answers {
                leagueCalls.incrementAndGet()
                val region = arg<String>(0)
                val entries = (1..300).map { i ->
                    LeagueEntryDto(
                        puuid = deterministicPuuid(region, i),
                        leaguePoints = 2200 - i,
                        rank = "I",
                        wins = 120,
                        losses = 80,
                        veteran = false,
                        inactive = false,
                        freshBlood = false,
                        hotStreak = false,
                        leagueId = "challenger-$region",
                        queueType = "RANKED_SOLO_5x5",
                        tier = "CHALLENGER",
                        summonerName = "",
                        gameName = null,
                        tagLine = null
                    )
                }
                LeagueListDTO(
                    tier = "CHALLENGER",
                    leagueId = "challenger-$region",
                    queue = "RANKED_SOLO_5x5",
                    name = "Challenger $region",
                    entries = entries
                )
            }

            val clock = object : TimeProvider {
                private val nowRef = AtomicLong(1_760_000_000_000L)
                override fun now(): Long = nowRef.get()
            }
            val memoryProvider = object : MemoryStatsProvider {
                override fun sample(): RawMemorySample = RawMemorySample(
                    jvmHeapUsedBytes = 150L * 1024L * 1024L,
                    jvmHeapCommittedBytes = 256L * 1024L * 1024L,
                    jvmHeapMaxBytes = 256L * 1024L * 1024L,
                    jvmNonHeapUsedBytes = 64L * 1024L * 1024L,
                    cgroupMemoryCurrentBytes = 310L * 1024L * 1024L,
                    cgroupMemoryMaxBytes = 512L * 1024L * 1024L
                )
            }

            val challengerService = ChallengerLeagueService(
                redisCacheService = redisCacheService,
                riotApiService = riotApiService,
                dataSource = pgDataSource,
                timeProvider = clock,
                memoryStatsProvider = memoryProvider
            )

            // 1. Pre-migration missing-table compatibility in ChallengerLeagueService:
            // player_riot_identities does NOT exist yet -> resolves 269/300 EUW and 140/300 KR from indexed_players
            val euwPre = challengerService.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
            val krPre = challengerService.getOrRefreshChallengerLeague("kr", "RANKED_SOLO_5x5")
            assertNotNull(euwPre)
            assertNotNull(krPre)
            assertEquals(269, euwPre!!.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })
            assertEquals(140, krPre!!.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })

            // 2. Standalone CLI default dry-run BEFORE migration exists:
            // Verifies 0 HTTP attempts, 0 DB writes, missing-table compatibility on DB recheck, and all 191 absent from indexed_players
            val dryRunResult = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--output-report-file=${dryRunReportFile.absolutePath}"
                )
            )
            assertEquals(0, dryRunResult.exitCode, "Dry-run CLI should succeed: ${dryRunResult.errorMessage}")
            assertEquals(0, httpCalls.get(), "Dry-run CLI must make 0 HTTP requests")
            val dryRunReport = dryRunResult.report!!
            assertEquals(RunOutcome.DRY_RUN_DB_RECHECK_ONLY_COMPLETED, dryRunReport.outcome)
            assertEquals(191, dryRunReport.manifestTotalCount)
            assertEquals(0, dryRunReport.alreadyResolvedInDbCount)
            assertEquals(0, dryRunReport.presentWithBlankFieldInIndexedPlayersCount)
            assertEquals(191, dryRunReport.absentFromIndexedPlayersCount)
            assertEquals(0, dryRunReport.httpAttemptsMade)
            assertEquals(0, dryRunReport.durableIndexedPlayerUpdatesVerifiedCount)
            assertEquals(0, dryRunReport.durableIdentityTableUpsertsVerifiedCount)
            assertEquals(191, dryRunReport.remainingPendingCount)
            assertTrue(dryRunReportFile.isFile)

            // Verify player_riot_identities still does NOT exist after dry-run
            pgDataSource.connection.use { conn ->
                conn.prepareStatement("SELECT to_regclass('public.player_riot_identities')").use { ps ->
                    ps.executeQuery().use { rs ->
                        assertTrue(rs.next())
                        assertEquals(null, rs.getString(1), "Dry-run must not create player_riot_identities")
                    }
                }
            }

            // 3. Run standalone CLI with --apply-additive-ddl=true and mocked Riot Account-V1 server for all 191 absent candidates (31 EUW + 160 KR)
            // Enforces absent-manifest scope (allowIndexedPlayersBlankUpdate = false by default) -> 0 indexed_players updates and exact 191 identity writes!
            val fixedVerifiedInstant = Instant.parse("2026-11-09T12:30:00Z")
            val liveResult = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--apply-additive-ddl=true",
                    "--key-coordination-mode=EXCLUSIVE_KEY",
                    "--coordination-attestation=offprod-pg-qual-lease-001",
                    "--riot-api-key=${OffProdChallengerIdentityCompletionTool.LOOPBACK_TEST_API_KEY_PREFIX}pg-qual-191",
                    "--allow-loopback-http-for-testing=true",
                    "--cluster-url-override=europe=http://127.0.0.1:$port,asia=http://127.0.0.1:$port",
                    "--max-http-attempts=191",
                    "--max-wall-time-ms=180000",
                    "--output-report-file=${liveReportFile.absolutePath}",
                    "--output-remaining-manifest-file=${remainingManifestFile.absolutePath}"
                ),
                wallClock = WallClock { fixedVerifiedInstant }
            )
            assertEquals(0, liveResult.exitCode, "Live CLI run should succeed: ${liveResult.errorMessage}")
            assertTrue(liveResult.ddlApplied)
            assertEquals(191, httpCalls.get())
            val liveReport = liveResult.report!!
            assertEquals(RunOutcome.STAGED_ALL_CANDIDATES, liveReport.outcome)
            assertEquals(191, liveReport.manifestTotalCount)
            assertEquals(191, liveReport.absentFromIndexedPlayersCount)
            assertEquals(0, liveReport.presentWithBlankFieldInIndexedPlayersCount)
            assertEquals(191, liveReport.httpAttemptsMade)
            assertEquals(0, liveReport.durableIndexedPlayerUpdatesVerifiedCount, "Must execute 0 updates on indexed_players under absent-manifest scope")
            assertEquals(191, liveReport.durableIdentityTableUpsertsVerifiedCount, "Must execute exact 191 verified upserts in player_riot_identities")
            assertEquals(0, liveReport.remainingPendingCount)

            val remainingAfterFull = OffProdChallengerIdentityCompletionCli.readSnapshotManifest(remainingManifestFile)
            assertEquals(0, remainingAfterFull.entries.size)

            // 4. Verify PostgreSQL state:
            // - player_riot_identities has exact 191 rows (31 EUW + 160 KR) with verified_at = 2026-11-09T12:30:00Z
            // - indexed_players still has exact 409 rows (269 complete EUW + 140 complete KR; 0 fabricated rows, 0 updates)
            // - CHECK constraint rejects blank game_name/tag_line in player_riot_identities
            // - Seed 1 conflicting row into player_riot_identities for EUW rank 1 to prove indexed_players precedence in ChallengerLeagueService
            pgDataSource.connection.use { conn ->
                conn.prepareStatement("SELECT COUNT(*) FROM player_riot_identities").use { ps ->
                    ps.executeQuery().use { rs ->
                        assertTrue(rs.next())
                        assertEquals(191, rs.getInt(1))
                    }
                }
                conn.prepareStatement("SELECT platform_region, COUNT(*) FROM player_riot_identities GROUP BY platform_region").use { ps ->
                    ps.executeQuery().use { rs ->
                        val counts = mutableMapOf<String, Int>()
                        while (rs.next()) {
                            counts[rs.getString(1)] = rs.getInt(2)
                        }
                        assertEquals(31, counts["euw1"])
                        assertEquals(160, counts["kr"])
                    }
                }
                conn.prepareStatement("SELECT COUNT(*) FROM indexed_players").use { ps ->
                    ps.executeQuery().use { rs ->
                        assertTrue(rs.next())
                        assertEquals(409, rs.getInt(1), "Must never insert fabricated rows or alter indexed_players count")
                    }
                }
                // Verify non-blank CHECK constraint in real PostgreSQL
                assertThrows(SQLException::class.java) {
                    conn.prepareStatement(
                        "INSERT INTO player_riot_identities (puuid, platform_region, game_name, tag_line, verified_at) VALUES (?, 'euw1', '   ', 'EUW', now())"
                    ).use { ps ->
                        ps.setString(1, "x".repeat(78))
                        ps.executeUpdate()
                    }
                }
                // Insert conflicting identity into player_riot_identities for EUW rank 1 (which is already in indexed_players as EuwIndexed1#EUW)
                conn.prepareStatement(
                    "INSERT INTO player_riot_identities (puuid, platform_region, game_name, tag_line, verified_at) VALUES (?, 'euw1', 'ConflictingOverride1', 'WRONG', now())"
                ).use { ps ->
                    ps.setString(1, deterministicPuuid("euw1", 1))
                    ps.executeUpdate()
                }
            }

            // 5. Verify ChallengerLeagueService read consumer coverage on 600/600 fixture (300/300 EUW + 300/300 KR)
            // with indexed-first precedence (EUW rank 1 resolves to EuwIndexed1#EUW from indexed_players, ignoring ConflictingOverride1#WRONG) and 0 Account-V1 fanout calls
            redisStore.clear()
            val euwPost = challengerService.getOrRefreshChallengerLeague("euw1", "RANKED_SOLO_5x5")
            val krPost = challengerService.getOrRefreshChallengerLeague("kr", "RANKED_SOLO_5x5")
            assertNotNull(euwPost)
            assertNotNull(krPost)
            assertEquals(300, euwPost!!.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })
            assertEquals(300, krPost!!.entries.count { !it.gameName.isNullOrBlank() && !it.tagLine.isNullOrBlank() })
            val entry1Euw = euwPost.entries.first { it.puuid == deterministicPuuid("euw1", 1) }
            assertEquals("EuwIndexed1", entry1Euw.gameName, "indexed_players must take precedence over player_riot_identities")
            assertEquals("EUW", entry1Euw.tagLine)
            val entry270 = euwPost.entries.first { it.puuid == deterministicPuuid("euw1", 270) }
            assertEquals("RiotEuw270", entry270.gameName)
            assertEquals("EUW", entry270.tagLine)
            val entry300Euw = euwPost.entries.first { it.puuid == deterministicPuuid("euw1", 300) }
            assertEquals("RiotEuw300", entry300Euw.gameName)
            assertEquals("EUW", entry300Euw.tagLine)
            val entry300Kr = krPost.entries.first { it.puuid == deterministicPuuid("kr", 300) }
            assertEquals("RiotKr300", entry300Kr.gameName)
            assertEquals("KR1", entry300Kr.tagLine)
            assertEquals(4, leagueCalls.get(), "Exactly 1 League-V4 call per refresh (4 refreshes total) and 0 Account-V1 fanout calls")

            // 6. Idempotent re-run of CLI makes 0 HTTP attempts and 0 DB writes
            val callsBeforeRerun = httpCalls.get()
            val rerunResult = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=EXCLUSIVE_KEY",
                    "--coordination-attestation=offprod-pg-qual-lease-001",
                    "--riot-api-key=${OffProdChallengerIdentityCompletionTool.LOOPBACK_TEST_API_KEY_PREFIX}pg-qual-191",
                    "--allow-loopback-http-for-testing=true",
                    "--cluster-url-override=europe=http://127.0.0.1:$port,asia=http://127.0.0.1:$port",
                    "--output-report-file=${idempotentReportFile.absolutePath}"
                )
            )
            assertEquals(0, rerunResult.exitCode)
            assertEquals(callsBeforeRerun, httpCalls.get(), "Idempotent re-run must make 0 HTTP calls")
            val idempotentReport = rerunResult.report!!
            assertEquals(RunOutcome.STAGED_ALL_CANDIDATES, idempotentReport.outcome)
            assertEquals(191, idempotentReport.alreadyResolvedInDbCount)
            assertEquals(0, idempotentReport.httpAttemptsMade)
            assertEquals(0, idempotentReport.durableIdentityTableUpsertsVerifiedCount)

            // 7. Old candidate 139a7de48e67af9d664c08fd61ce131b6e63bd74 read & autocomplete compatibility:
            // - 139a7de ChallengerLeagueService queries `SELECT * FROM indexed_players WHERE puuid = ANY(?)`
            // - 139a7de Autocomplete queries `SELECT * FROM indexed_players WHERE region = :region AND LOWER(game_name) LIKE LOWER(CONCAT(:prefix, '%')) ORDER BY last_seen_at DESC LIMIT 8`
            // Neither sees fabricated rows nor fails when player_riot_identities is present alongside indexed_players.
            pgDataSource.connection.use { conn ->
                conn.prepareStatement(
                    """
                    SELECT puuid, game_name, tag_line, profile_icon_id, summoner_level
                    FROM indexed_players
                    WHERE region = ?
                      AND LOWER(game_name) LIKE LOWER(? || '%')
                    ORDER BY last_seen_at DESC
                    LIMIT 8
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, "euw1")
                    ps.setString(2, "EuwIndexed26")
                    ps.executeQuery().use { rs ->
                        val matches = mutableListOf<String>()
                        while (rs.next()) {
                            matches += rs.getString("game_name")
                            assertTrue(rs.getInt("profile_icon_id") > 0)
                            assertTrue(rs.getLong("summoner_level") > 0L)
                        }
                        assertEquals(8, matches.size)
                        assertTrue(matches.all { it.startsWith("EuwIndexed26") })
                    }
                }
                // Absent Challenger players (stored only in player_riot_identities) never pollute autocomplete with fake icon/level/last_seen_at
                conn.prepareStatement(
                    """
                    SELECT COUNT(*) FROM indexed_players
                    WHERE region = 'euw1' AND LOWER(game_name) LIKE LOWER('RiotEuw300%')
                    """.trimIndent()
                ).use { ps ->
                    ps.executeQuery().use { rs ->
                        assertTrue(rs.next())
                        assertEquals(0, rs.getInt(1), "Identity-only Challenger rows must not pollute autocomplete index")
                    }
                }
            }

            // 8. Verify default CLI absent-manifest scope blocks any candidate present in indexed_players with blank field (0 HTTP calls, 0 indexed_players updates)
            //    and verify shared global maxWallTimeMs budget across --apply-additive-ddl=true + runBounded
            val pBlankIndexed = deterministicPuuid("euw1", 999)
            pgDataSource.connection.use { conn ->
                conn.prepareStatement(
                    "INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at) VALUES (?, 'ExistingName', '   ', 'euw1', 777, 888, now())"
                ).use { ps ->
                    ps.setString(1, pBlankIndexed)
                    ps.executeUpdate()
                }
            }
            val blankScopeFile = workDir.resolve("blank-scope-manifest.json").toFile()
            OffProdChallengerIdentityCompletionCli.writeSnapshotManifest(
                file = blankScopeFile,
                snapshotCapturedAtUtc = "2026-11-09T11:34:00Z",
                entries = listOf(ManifestEntry(pBlankIndexed, "euw1"))
            )
            val callsBeforeScopeCheck = httpCalls.get()
            val blockedScopeResult = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${blankScopeFile.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=EXCLUSIVE_KEY",
                    "--coordination-attestation=offprod-pg-qual-scope-check",
                    "--riot-api-key=${OffProdChallengerIdentityCompletionTool.LOOPBACK_TEST_API_KEY_PREFIX}pg-qual-scope"
                )
            )
            assertEquals(1, blockedScopeResult.exitCode)
            assertEquals(RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT, blockedScopeResult.report!!.outcome)
            assertEquals(callsBeforeScopeCheck, httpCalls.get(), "Must make 0 HTTP calls when absent-manifest scope is violated")
            assertEquals(0, blockedScopeResult.report!!.durableIndexedPlayerUpdatesVerifiedCount)

            // Verify shared global wall-time budget across DDL + runBounded:
            // (a) If DDL takes 50_000ms of a 180_000ms budget, runBounded receives the remaining 130_000ms budget;
            // (b) If DDL consumes the entire 180_000ms budget, runCli halts with exitCode=10 and 0 HTTP calls.
            val ddlExhaustClock = object : MonotonicClock {
                private val currentMs = AtomicLong(1_000L)
                override fun nowMs(): Long = currentMs.addAndGet(100L)
                override fun sleepMs(durationMs: Long) {}
            }
            val sharedBudgetExhaustedResult = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--apply-additive-ddl=true",
                    "--db-timeout-ms=5000",
                    "--max-wall-time-ms=1000"
                ),
                monotonicClock = ddlExhaustClock
            )
            assertEquals(10, sharedBudgetExhaustedResult.exitCode, "Must exit with 10 when shared global wall-time budget is exhausted by DDL stage")
            assertEquals(callsBeforeScopeCheck, httpCalls.get(), "Must make 0 HTTP calls when shared global budget is exhausted after DDL")
        } finally {
            server.stop(0)
            workDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `partial completion across 404, omitted fields, 429, budget, and unknown_commit resumes on real Postgres`() {
        resetBaseSchemaAndSeed600Fixture()
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(JdbcIdentityCompletionPersistenceAdapter.loadReviewedAdditiveMigrationSql())
            }
        }

        // 6 missing EUW candidates (indices 272..277, absent from indexed_players):
        // p1 (272): 404 Not Found on Pass 1 -> resolved on Pass 2
        // p2 (273): 200 OK with omitted tagLine on Pass 1 -> resolved on Pass 2
        // p3 (274): 200 OK durable write on Pass 1
        // p4 (275): 429 Too Many Requests on Pass 1 (aborts Pass 1, leaving [p1, p2, p4, p5, p6] in remainingEntries)
        // On Pass 2 (resuming from remainingAfterPass1: [p1, p2, p4, p5, p6]):
        //   - p1: 200 OK durable write
        //   - p2: 200 OK durable write, then wall-time clock advances past budget -> stops with PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED (leaving [p4, p5, p6])
        // On Pass 3 (resuming from remainingAfterPass2: [p4, p5, p6]):
        //   - p4: DB commit succeeds on real PostgreSQL, then post-commit verification throws simulated socket drop ->
        //     aborts with ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT and keeps [p4, p5, p6] in remainingEntries
        // On Pass 4 (resuming via CLI from remaining-pass3.json: [p4, p5, p6]):
        //   - p4 is already committed in PostgreSQL -> SKIPPED_ALREADY_RESOLVED_IN_DB (0 HTTP call for p4!)
        //   - p5 & p6: 200 OK durable writes -> STAGED_ALL_CANDIDATES (0 remaining)
        val p1 = deterministicPuuid("euw1", 272)
        val p2 = deterministicPuuid("euw1", 273)
        val p3 = deterministicPuuid("euw1", 274)
        val p4 = deterministicPuuid("euw1", 275)
        val p5 = deterministicPuuid("euw1", 276)
        val p6 = deterministicPuuid("euw1", 277)

        val initialCandidates = listOf(p1, p2, p3, p4, p5, p6).map {
            ManifestEntry(puuid = it, platformRegion = "euw1")
        }

        val passNumber = AtomicInteger(1)
        val httpCallsByPuuid = ConcurrentHashMap<String, AtomicInteger>()
        val fakeClockMs = AtomicLong(10_000L)
        val failPostCommitReadForP4Once = AtomicBoolean(false)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newCachedThreadPool()
        server.createContext("/riot/account/v1/accounts/by-puuid/") { exchange ->
            val puuid = exchange.requestURI.path.substringAfterLast("/")
            httpCallsByPuuid.computeIfAbsent(puuid) { AtomicInteger(0) }.incrementAndGet()
            val pass = passNumber.get()

            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
            exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
            exchange.responseHeaders.add("X-Method-Rate-Limit", "1000:60")
            exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "1:60")

            when {
                pass == 1 && puuid == p1 -> {
                    val body = """{"status":{"message":"Data not found","status_code":404}}""".toByteArray()
                    exchange.sendResponseHeaders(404, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                pass == 1 && puuid == p2 -> {
                    val body = """{"puuid":"$p2","gameName":"OmittedTagOnly"}""".toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                pass == 1 && puuid == p4 -> {
                    exchange.responseHeaders.add("Retry-After", "5")
                    val body = """{"status":{"message":"Rate limit exceeded","status_code":429}}""".toByteArray()
                    exchange.sendResponseHeaders(429, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                else -> {
                    val label = when (puuid) {
                        p1 -> "ResolvedP1"
                        p2 -> "ResolvedP2"
                        p3 -> "ResolvedP3"
                        p4 -> "ResolvedP4"
                        p5 -> "ResolvedP5"
                        p6 -> "ResolvedP6"
                        else -> "Unknown"
                    }
                    val body = """{"puuid":"$puuid","gameName":"$label","tagLine":"EUW"}""".toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
            }
        }
        server.start()

        val workDir = Files.createTempDirectory("offprod-pg-resume-")
        try {
            val port = server.address.port
            val clock = object : MonotonicClock {
                override fun nowMs(): Long = fakeClockMs.get()
                override fun sleepMs(durationMs: Long) {
                    if (durationMs > 0) fakeClockMs.addAndGet(durationMs)
                }
            }
            val testApiKey = "${OffProdChallengerIdentityCompletionTool.LOOPBACK_TEST_API_KEY_PREFIX}resume-test"

            // Pass 1: p1=404, p2=omitted tagLine, p3=200 durable write, p4=429 rate limit abort (p5, p6 untouched)
            val pass1Adapter = JdbcIdentityCompletionPersistenceAdapter(dataSource = pgDataSource)
            val pass1Options = ExecutionOptions(
                dryRun = false,
                networkEnabled = true,
                allowDurableWrites = true,
                keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                coordinationAttestation = "lease-pass-1",
                maxTotalHttpAttempts = 191,
                maxWallTimeMs = 60_000L,
                allowLoopbackHttpForTesting = true,
                baseUrlByCluster = mapOf("europe" to "http://127.0.0.1:$port")
            )
            val pass1Report = OffProdChallengerIdentityCompletionTool(
                persistencePort = pass1Adapter,
                monotonicClock = clock,
                wallClock = WallClock { Instant.parse("2026-11-09T13:00:00Z") }
            ).runBounded(
                manifest = SnapshotManifest("2026-11-09T11:34:00Z", entries = initialCandidates),
                options = pass1Options,
                riotApiKeyProvider = { testApiKey }
            )

            assertEquals(RunOutcome.ABORTED_HTTP_429, pass1Report.outcome)
            assertEquals(1, pass1Report.notFound404Count)
            assertEquals(1, pass1Report.omittedRiotIdFieldsCount)
            assertEquals(1, pass1Report.durableIdentityTableUpsertsVerifiedCount)

            // Build resumable manifest from all candidates that did not complete durable persistence on Pass 1
            val remainingAfterPass1 = pass1Report.itemResults
                .filter {
                    it.status != ItemStatus.COMPLETED_DURABLE_WRITE_VERIFIED &&
                        it.status != ItemStatus.SKIPPED_ALREADY_RESOLVED_IN_DB &&
                        it.status != ItemStatus.CONCURRENT_ALREADY_COMPLETE_VERIFIED
                }
                .map { ManifestEntry(it.puuid, it.platformRegion) }
            assertEquals(listOf(p1, p2, p4, p5, p6), remainingAfterPass1.map { it.normalizedPuuid })

            // Pass 2: p1 succeeds, p2 succeeds, then wall-time clock advances past budget before p4
            passNumber.set(2)
            var pass2Writes = 0
            val budgetExhaustingAdapter = object : IdentityCompletionPersistencePort by pass1Adapter {
                override fun persistAndVerifyIdentityBounded(
                    puuid: String,
                    platformRegion: String,
                    gameName: String,
                    tagLine: String,
                    verifiedAt: Instant,
                    preCallDbState: OffProdChallengerIdentityCompletionTool.PreCallDbState,
                    timeoutMs: Long
                ): DbWriteVerificationResult {
                    val outcome = pass1Adapter.persistAndVerifyIdentityBounded(
                        puuid, platformRegion, gameName, tagLine, verifiedAt, preCallDbState, timeoutMs
                    )
                    pass2Writes++
                    if (pass2Writes == 2) {
                        // Exhaust wall-time budget right after p2 commits
                        fakeClockMs.addAndGet(65_000L)
                    }
                    return outcome
                }
            }
            val pass2Report = OffProdChallengerIdentityCompletionTool(
                persistencePort = budgetExhaustingAdapter,
                monotonicClock = clock,
                wallClock = WallClock { Instant.parse("2026-11-09T13:02:00Z") }
            ).runBounded(
                manifest = SnapshotManifest("2026-11-09T11:34:00Z", entries = remainingAfterPass1),
                options = pass1Options,
                riotApiKeyProvider = { testApiKey }
            )

            assertEquals(RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED, pass2Report.outcome)
            assertEquals(2, pass2Report.durableIdentityTableUpsertsVerifiedCount)
            assertEquals(3, pass2Report.remainingPendingCount)
            assertEquals(listOf(p4, p5, p6), pass2Report.remainingEntries.map { it.normalizedPuuid })

            // Pass 3: p4 commits in PostgreSQL, but post-commit verification connection throws simulated socket drop ->
            // returns FailureOrUnknownCommit -> tool aborts with ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT and keeps [p4, p5, p6] in remainingEntries
            passNumber.set(3)
            failPostCommitReadForP4Once.set(true)
            var p4CommittedInPg = false
            val unknownCommitDataSource = object : DataSource by pgDataSource {
                override fun getConnection(): Connection {
                    val realConn = pgDataSource.connection
                    return object : Connection by realConn {
                        override fun prepareStatement(sql: String): java.sql.PreparedStatement {
                            val realPs = realConn.prepareStatement(sql)
                            if (failPostCommitReadForP4Once.get() && p4CommittedInPg && sql.contains("FROM player_riot_identities")) {
                                failPostCommitReadForP4Once.set(false)
                                realPs.close()
                                throw SQLException("Simulated socket reset after commit before post-commit verification read")
                            }
                            return object : java.sql.PreparedStatement by realPs {
                                override fun executeUpdate(): Int {
                                    val updated = realPs.executeUpdate()
                                    if (sql.contains("INSERT INTO player_riot_identities")) {
                                        p4CommittedInPg = true
                                    }
                                    return updated
                                }
                            }
                        }
                    }
                }
            }
            val pass3Adapter = JdbcIdentityCompletionPersistenceAdapter(dataSource = unknownCommitDataSource)
            val pass3Report = OffProdChallengerIdentityCompletionTool(
                persistencePort = pass3Adapter,
                monotonicClock = clock,
                wallClock = WallClock { Instant.parse("2026-11-09T13:05:00Z") }
            ).runBounded(
                manifest = SnapshotManifest("2026-11-09T11:34:00Z", entries = pass2Report.remainingEntries),
                options = pass1Options,
                riotApiKeyProvider = { testApiKey }
            )

            assertEquals(RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT, pass3Report.outcome)
            assertEquals(0, pass3Report.durableIdentityTableUpsertsVerifiedCount, "Ambiguous commit must not be counted as verified durable write")
            assertEquals(1, pass3Report.dbFailureOrUnknownCommitCount)
            assertEquals(3, pass3Report.remainingPendingCount, "p4 must remain in remainingEntries until verified")
            assertEquals(listOf(p4, p5, p6), pass3Report.remainingEntries.map { it.normalizedPuuid })
            assertEquals(
                ItemStatus.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
                pass3Report.itemResults.first { it.puuid == p4 }.status
            )

            val remainingPass3File = workDir.resolve("remaining-pass3.json").toFile()
            val finalReportFile = workDir.resolve("final-pass4-report.json").toFile()
            val finalRemainingFile = workDir.resolve("final-pass4-remaining.json").toFile()
            OffProdChallengerIdentityCompletionCli.writeSnapshotManifest(
                file = remainingPass3File,
                snapshotCapturedAtUtc = "2026-11-09T11:34:00Z",
                entries = pass3Report.remainingEntries
            )

            // Pass 4 via standalone CLI using remaining-pass3.json:
            // - p4 was already committed in PostgreSQL during Pass 3 -> SKIPPED_ALREADY_RESOLVED_IN_DB (0 additional HTTP calls for p4!)
            // - p5 and p6 are fetched and durably persisted -> STAGED_ALL_CANDIDATES (0 remaining)
            val p4HttpCallsBeforePass4 = httpCallsByPuuid[p4]?.get() ?: 0
            val pass4Result = OffProdChallengerIdentityCompletionCli.runCli(
                args = listOf(
                    "--manifest-file=${remainingPass3File.absolutePath}",
                    "--jdbc-url=$jdbcUrl",
                    "--db-user=postgres",
                    "--db-pass=",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=EXCLUSIVE_KEY",
                    "--coordination-attestation=lease-pass-4",
                    "--riot-api-key=$testApiKey",
                    "--allow-loopback-http-for-testing=true",
                    "--cluster-url-override=europe=http://127.0.0.1:$port",
                    "--output-report-file=${finalReportFile.absolutePath}",
                    "--output-remaining-manifest-file=${finalRemainingFile.absolutePath}"
                )
            )
            assertEquals(0, pass4Result.exitCode, "Pass 4 CLI should succeed: ${pass4Result.errorMessage}")
            val pass4Report = pass4Result.report!!
            assertEquals(RunOutcome.STAGED_ALL_CANDIDATES, pass4Report.outcome)
            assertEquals(1, pass4Report.alreadyResolvedInDbCount, "p4 committed before socket drop in Pass 3 must be skipped on Pass 4 DB recheck")
            assertEquals(2, pass4Report.httpAttemptsMade, "Only p5 and p6 should trigger HTTP requests on Pass 4")
            assertEquals(2, pass4Report.durableIdentityTableUpsertsVerifiedCount)
            assertEquals(0, pass4Report.remainingPendingCount)
            assertEquals(p4HttpCallsBeforePass4, httpCallsByPuuid[p4]?.get(), "p4 must make 0 additional HTTP calls on Pass 4")

            val finalRemaining = OffProdChallengerIdentityCompletionCli.readSnapshotManifest(finalRemainingFile)
            assertEquals(0, finalRemaining.entries.size)

            // Verify all 6 candidates are durably present in PostgreSQL player_riot_identities
            pgDataSource.connection.use { conn ->
                conn.prepareStatement("SELECT COUNT(*) FROM player_riot_identities WHERE puuid = ANY(?)").use { ps ->
                    ps.setArray(1, conn.createArrayOf("varchar", arrayOf(p1, p2, p3, p4, p5, p6)))
                    ps.executeQuery().use { rs ->
                        assertTrue(rs.next())
                        assertEquals(6, rs.getInt(1))
                    }
                }
            }
        } finally {
            server.stop(0)
            workDir.toFile().deleteRecursively()
        }
    }
}
