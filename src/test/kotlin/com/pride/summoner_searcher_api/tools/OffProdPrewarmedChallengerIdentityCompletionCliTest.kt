package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ManifestEntry
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.RunOutcome
import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.io.PrintWriter
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OffProdPrewarmedChallengerIdentityCompletionCliTest {

    private var pgTempDir: Path? = null
    private var pgBinDir: File? = null
    private var pgPort: Int = 0
    private lateinit var jdbcUrl: String
    private lateinit var pgDataSource: PGSimpleDataSource

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    private fun officialPuuid(index: Int): String =
        "puuid_prewarm_test_${index.toString().padStart(4, '0')}_".padEnd(78, 'a')

    @BeforeAll
    fun startIsolatedPostgres() {
        val candidateBins = listOf(
            File("/usr/lib/postgresql/15/bin"),
            File("/usr/lib/postgresql/16/bin"),
            File("/usr/lib/postgresql/14/bin")
        )
        val binDir = candidateBins.firstOrNull { File(it, "initdb").canExecute() && File(it, "pg_ctl").canExecute() }
            ?: throw IllegalStateException("PostgreSQL binaries (initdb/pg_ctl) not found in /usr/lib/postgresql/*")
        pgBinDir = binDir

        val tempRoot = Files.createTempDirectory("offprod-prewarm-cli-pg-")
        pgTempDir = tempRoot
        val dataDir = tempRoot.resolve("data").toFile()
        val socketDir = tempRoot.resolve("socket").toFile().apply { mkdirs() }
        val logFile = tempRoot.resolve("postgres.log").toFile()

        pgPort = ServerSocket(0).use { it.localPort }

        val initProc = ProcessBuilder(
            File(binDir, "initdb").absolutePath,
            "-D", dataDir.absolutePath,
            "-U", "postgres",
            "-A", "trust",
            "--encoding=UTF8",
            "--locale=C"
        ).redirectErrorStream(true).start()
        val initOut = initProc.inputStream.bufferedReader().readText()
        check(initProc.waitFor() == 0) { "initdb failed: $initOut" }

        val startProc = ProcessBuilder(
            File(binDir, "pg_ctl").absolutePath,
            "-D", dataDir.absolutePath,
            "-l", logFile.absolutePath,
            "-w",
            "-o", "-F -p $pgPort -k ${socketDir.absolutePath} -h 127.0.0.1",
            "start"
        ).redirectErrorStream(true).start()
        val startOut = startProc.inputStream.bufferedReader().readText()
        check(startProc.waitFor() == 0) {
            "pg_ctl start failed: $startOut\nLog:\n${logFile.takeIf { it.exists() }?.readText()}"
        }

        jdbcUrl = "jdbc:postgresql://127.0.0.1:$pgPort/postgres"
        pgDataSource = PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = "postgres"
            password = ""
        }

        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.indexed_players (
                        puuid VARCHAR(78) PRIMARY KEY,
                        game_name VARCHAR(255) NOT NULL,
                        tag_line VARCHAR(255) NOT NULL,
                        platform_region VARCHAR(16) NOT NULL,
                        profile_icon_id INT NOT NULL,
                        summoner_level BIGINT NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL
                    )
                    """.trimIndent()
                )
            }
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
                        "stop"
                    ).redirectErrorStream(true).start().waitFor(10, TimeUnit.SECONDS)
                }
            }
            runCatching { tempRoot.toFile().deleteRecursively() }
        }
    }

    private fun writeTempManifest(entries: List<ManifestEntry>): File {
        val file = Files.createTempFile("prewarm-manifest-", ".json").toFile()
        OffProdChallengerIdentityCompletionCli.writeSnapshotManifest(
            file = file,
            snapshotCapturedAtUtc = "2026-11-09T00:00:00Z",
            entries = entries
        )
        return file
    }

    @Test
    fun `pre-validation rejects apply-additive-ddl, invalid timeout bounds, missing attestation, and 256MiB heap cap with 0 physical connections`() {
        val getConnCalls = AtomicInteger(0)
        val nonConnectingDs = object : DataSource {
            override fun getConnection(): Connection {
                getConnCalls.incrementAndGet()
                throw AssertionError("getConnection() must not be invoked when CLI options are invalid")
            }
            override fun getConnection(username: String?, password: String?): Connection = getConnection()
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        val manifestFile = writeTempManifest(
            listOf(ManifestEntry(puuid = officialPuuid(1), platformRegion = "euw1"))
        )
        try {
            val invalidCases = listOf(
                // 1. Completion wrapper must NEVER allow DDL
                listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--apply-additive-ddl=true",
                    "--dry-run=false",
                    "--allow-durable-writes=true"
                ),
                // 2. Invalid readiness timeout bounds
                listOf("--manifest-file=${manifestFile.absolutePath}", "--readiness-timeout-ms=0"),
                listOf("--manifest-file=${manifestFile.absolutePath}", "--readiness-timeout-ms=10001"),
                // 3. Invalid max wall time bounds (> 180,000ms)
                listOf("--manifest-file=${manifestFile.absolutePath}", "--max-wall-time-ms=0"),
                listOf("--manifest-file=${manifestFile.absolutePath}", "--max-wall-time-ms=180001"),
                // 4. Invalid db-timeout-ms (> 2,500ms)
                listOf("--manifest-file=${manifestFile.absolutePath}", "--db-timeout-ms=0"),
                listOf("--manifest-file=${manifestFile.absolutePath}", "--db-timeout-ms=2501"),
                // 5. Invalid cleanup-timeout-ms
                listOf("--manifest-file=${manifestFile.absolutePath}", "--cleanup-timeout-ms=0"),
                listOf("--manifest-file=${manifestFile.absolutePath}", "--cleanup-timeout-ms=5001"),
                // 6. Durable write / network mode without coordination attestation
                listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true"
                )
            )

            for (args in invalidCases) {
                val res = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                    args = args,
                    env = mapOf(
                        "OFFPROD_JDBC_URL" to jdbcUrl,
                        "OFFPROD_DB_PASS" to "NON_CREDENTIAL_COMPLETION_DB_MARKER"
                    ),
                    physicalDataSourceOverride = nonConnectingDs,
                    maxHeapBytesProvider = { 128L * 1024L * 1024L }
                )
                assertEquals(2, res.exitCode, "Expected exitCode=2 for invalid args $args")
                assertEquals(0, res.wrapperReport.physicalConnectionsOpened)
                assertFalse(res.wrapperReport.sanitizedDetail.contains("NON_CREDENTIAL_COMPLETION_DB_MARKER"))
            }

            // 7. Standalone > 256 MiB JVM heap cap rejected before any connection
            val heapRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf("--manifest-file=${manifestFile.absolutePath}", "--dry-run=true"),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl),
                physicalDataSourceOverride = nonConnectingDs,
                maxHeapBytesProvider = { 257L * 1024L * 1024L }
            )
            assertEquals(2, heapRes.exitCode)
            assertEquals(0, heapRes.wrapperReport.physicalConnectionsOpened)
            assertTrue(heapRes.wrapperReport.sanitizedDetail.contains("256 MiB"))

            assertEquals(0, getConnCalls.get(), "Expected 0 physical getConnection() calls on invalid options")
        } finally {
            manifestFile.delete()
        }
    }

    @Test
    fun `deterministic late-binding routes leases by setReadOnly or first SQL regardless of getConnection borrow order and rejects same physical PID`() {
        fun createMockPhysicalConn(backendPid: Int): Connection {
            var currentReadOnly = false
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        Proxy.newProxyInstance(
                            PreparedStatement::class.java.classLoader,
                            arrayOf(PreparedStatement::class.java)
                        ) { _, psMethod, _ ->
                            when (psMethod.name) {
                                "setQueryTimeout", "setString", "setTimestamp", "close", "cancel" -> null
                                "executeUpdate" -> 1
                                "executeQuery" -> {
                                    val served = AtomicBoolean(false)
                                    Proxy.newProxyInstance(
                                        ResultSet::class.java.classLoader,
                                        arrayOf(ResultSet::class.java)
                                    ) { _, rsMethod, _ ->
                                        when (rsMethod.name) {
                                            "next" -> served.compareAndSet(false, true)
                                            "getInt" -> if (sql.contains("pg_backend_pid")) backendPid else 0
                                            "getString" -> "PlayerOne"
                                            "close" -> null
                                            else -> null
                                        }
                                    } as ResultSet
                                }
                                else -> null
                            }
                        } as PreparedStatement
                    }
                    "setReadOnly" -> {
                        currentReadOnly = args?.firstOrNull() as? Boolean ?: false
                        null
                    }
                    "isReadOnly" -> currentReadOnly
                    "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                    "getAutoCommit" -> true
                    "isWrapperFor" -> false
                    "equals" -> proxy === args?.firstOrNull()
                    "hashCode" -> System.identityHashCode(proxy)
                    else -> null
                }
            } as Connection
        }

        val netExec = Executors.newSingleThreadExecutor { r -> Thread(r).apply { isDaemon = true } }
        try {
            val writeConn = createMockPhysicalConn(backendPid = 8101)
            val readVerifyConn = createMockPhysicalConn(backendPid = 8202)
            val pool = OffProdPrewarmedChallengerIdentityCompletionCli.LateBindingTwoSessionDataSource(
                physicalWriteConn = writeConn,
                physicalReadVerifyConn = readVerifyConn,
                writeBackendPid = 8101,
                readVerifyBackendPid = 8202,
                netTimeoutExec = netExec
            )

            // Borrow two leases BEFORE calling setReadOnly on either lease (reverse/interleaved order!)
            val leaseBorrowedFirst = pool.connection
            val leaseBorrowedSecond = pool.connection
            assertNotEquals(leaseBorrowedFirst, leaseBorrowedSecond)

            // Bind the SECOND-borrowed lease to WRITE_SESSION (isReadOnly = false)
            // and the FIRST-borrowed lease to READ_VERIFY_SESSION (isReadOnly = true)!
            leaseBorrowedSecond.isReadOnly = false
            leaseBorrowedFirst.isReadOnly = true

            leaseBorrowedSecond.prepareStatement(
                "INSERT INTO player_riot_identities (puuid, platform_region, game_name, tag_line, verified_at) VALUES (?, ?, ?, ?, ?)"
            ).use { ps ->
                ps.setString(1, officialPuuid(1))
                ps.executeUpdate()
            }
            leaseBorrowedFirst.prepareStatement(
                "SELECT game_name, tag_line, verified_at FROM player_riot_identities WHERE puuid = ?"
            ).use { ps ->
                ps.setString(1, officialPuuid(1))
                ps.executeQuery().close()
            }

            leaseBorrowedSecond.close()
            leaseBorrowedFirst.close()

            val audit = pool.getAuditEntries()
            assertEquals(2, audit.size)
            assertEquals(OffProdPrewarmedChallengerIdentityCompletionCli.SessionRole.WRITE_SESSION, audit[0].sessionRole)
            assertEquals(8101, audit[0].backendPid)
            assertEquals(OffProdPrewarmedChallengerIdentityCompletionCli.SessionRole.READ_VERIFY_SESSION, audit[1].sessionRole)
            assertEquals(8202, audit[1].backendPid)

            // Also verify late-binding by first SQL statement when setReadOnly is NOT called prior to prepareStatement:
            pool.connection.use { sqlBoundReadLease ->
                sqlBoundReadLease.prepareStatement("SELECT puuid, game_name, tag_line FROM indexed_players WHERE puuid IN (?)").use { ps ->
                    ps.executeQuery().close()
                }
            }
            pool.connection.use { sqlBoundWriteLease ->
                sqlBoundWriteLease.prepareStatement(
                    "INSERT INTO player_riot_identities (puuid, platform_region, game_name, tag_line, verified_at) VALUES (?, ?, ?, ?, ?)"
                ).use { ps ->
                    ps.setString(1, officialPuuid(2))
                    ps.executeUpdate()
                }
            }
            val auditAfterSqlBind = pool.getAuditEntries()
            assertEquals(4, auditAfterSqlBind.size)
            assertEquals(8202, auditAfterSqlBind[2].backendPid)
            assertEquals(8101, auditAfterSqlBind[3].backendPid)

            // Switching role on an already bound lease must poison the pool and throw SQLException:
            pool.connection.use { lease ->
                lease.isReadOnly = false
                assertThrows<SQLException> {
                    lease.isReadOnly = true
                }
            }
            assertTrue(pool.isPoisoned())
        } finally {
            netExec.shutdownNow()
        }
    }

    @Test
    fun `readiness counts within 180s budget and abort-poison prevents session reuse or 3rd connection`() {
        val manifestFile = writeTempManifest(
            listOf(
                ManifestEntry(puuid = officialPuuid(10), platformRegion = "euw1"),
                ManifestEntry(puuid = officialPuuid(11), platformRegion = "euw1")
            )
        )
        try {
            // Part A: Readiness elapsed time is counted inside maxWallTimeMs; if readiness exhausts maxWallTimeMs,
            // wrapper exits 10 with 0 HTTP calls and 0 candidate writes.
            var fakeNowMs = 500_000L
            val fakeClock = object : MonotonicClock {
                override fun nowMs(): Long = fakeNowMs
                override fun sleepMs(ms: Long) {
                    fakeNowMs += ms
                }
            }
            val httpCalls = AtomicInteger(0)
            val connCounter = AtomicInteger(0)
            val exhaustingReadinessDs = object : DataSource {
                override fun getConnection(): Connection {
                    val idx = connCounter.incrementAndGet()
                    fakeNowMs += 2_000L // 2 connections x 2,000ms = 4,000ms, exhausting maxWallTimeMs = 4,000ms
                    val pid = 6000 + idx
                    return Proxy.newProxyInstance(
                        Connection::class.java.classLoader,
                        arrayOf(Connection::class.java)
                    ) { proxy, method, args ->
                        when (method.name) {
                            "prepareStatement" -> {
                                val sql = (args?.firstOrNull() as? String).orEmpty()
                                Proxy.newProxyInstance(
                                    PreparedStatement::class.java.classLoader,
                                    arrayOf(PreparedStatement::class.java)
                                ) { _, psMethod, _ ->
                                    when (psMethod.name) {
                                        "setQueryTimeout", "close", "cancel" -> null
                                        "executeUpdate" -> 0
                                        "executeQuery" -> {
                                            val served = AtomicBoolean(false)
                                            Proxy.newProxyInstance(
                                                ResultSet::class.java.classLoader,
                                                arrayOf(ResultSet::class.java)
                                            ) { _, rsMethod, _ ->
                                                when (rsMethod.name) {
                                                    "next" -> served.compareAndSet(false, true)
                                                    "getInt" -> if (sql.contains("pg_backend_pid")) pid else 0
                                                    "getBoolean" -> true
                                                    "getObject" -> true
                                                    "getString" -> "public"
                                                    "close" -> null
                                                    else -> null
                                                }
                                            } as ResultSet
                                        }
                                        else -> null
                                    }
                                } as PreparedStatement
                            }
                            "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                            "getAutoCommit" -> true
                            "isWrapperFor" -> false
                            "equals" -> proxy === args?.firstOrNull()
                            "hashCode" -> System.identityHashCode(proxy)
                            else -> null
                        }
                    } as Connection
                }
                override fun getConnection(username: String?, password: String?): Connection = getConnection()
                override fun getLogWriter(): PrintWriter? = null
                override fun setLogWriter(out: PrintWriter?) {}
                override fun setLoginTimeout(seconds: Int) {}
                override fun getLoginTimeout(): Int = 0
                override fun getParentLogger(): Logger = Logger.getGlobal()
                override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
                override fun isWrapperFor(iface: Class<*>?): Boolean = false
            }

            val budgetExhaustedRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                    "--coordination-attestation=production-suspended-and-quiesced",
                    "--readiness-timeout-ms=5000",
                    "--max-wall-time-ms=4000"
                ),
                env = mapOf("OFFPROD_RIOT_API_KEY" to "test-valid-key"),
                physicalDataSourceOverride = exhaustingReadinessDs,
                httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                    httpCalls.incrementAndGet()
                    throw AssertionError("No HTTP call may occur when readiness exhausts shared maxWallTimeMs")
                },
                monotonicClock = fakeClock,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(10, budgetExhaustedRes.exitCode)
            assertEquals(2, budgetExhaustedRes.wrapperReport.physicalConnectionsOpened)
            assertEquals(0, httpCalls.get())
            assertTrue(budgetExhaustedRes.wrapperReport.connectionsClosedCleanly)

            // Part B: Abort/Poison test — when candidate 1 write stalls past db-timeout-ms (80ms),
            // JdbcIdentityCompletionPersistenceAdapter calls abort()/cancel(), poisoning the 2-session pool,
            // preventing any session reuse, and NEVER opening a 3rd physical connection!
            val releaseStalledWrite = CountDownLatch(1)
            val physicalConnsOpened = AtomicInteger(0)
            val abortSignaledOnWriteConn = CountDownLatch(1)

            fun createAbortTestConn(isWriteSession: Boolean, backendPid: Int): Connection {
                return Proxy.newProxyInstance(
                    Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)
                ) { proxy, method, args ->
                    when (method.name) {
                        "prepareStatement" -> {
                            val sql = (args?.firstOrNull() as? String).orEmpty()
                            val isUpsert = sql.uppercase().contains("INSERT INTO PLAYER_RIOT_IDENTITIES")
                            val isPid = sql.contains("pg_backend_pid")
                            val isContractCheck = sql.contains("table_shape_valid")
                            Proxy.newProxyInstance(
                                PreparedStatement::class.java.classLoader,
                                arrayOf(PreparedStatement::class.java)
                            ) { _, psMethod, _ ->
                                when (psMethod.name) {
                                    "setQueryTimeout", "setString", "setTimestamp", "close" -> null
                                    "cancel" -> {
                                        if (isWriteSession) abortSignaledOnWriteConn.countDown()
                                        null
                                    }
                                    "executeUpdate" -> {
                                        if (isUpsert) {
                                            var interrupted = false
                                            while (true) {
                                                try {
                                                    if (releaseStalledWrite.await(5, TimeUnit.SECONDS)) break
                                                    break
                                                } catch (_: InterruptedException) {
                                                    interrupted = true
                                                }
                                            }
                                            if (interrupted) Thread.currentThread().interrupt()
                                        }
                                        1
                                    }
                                    "executeQuery" -> {
                                        val served = AtomicBoolean(false)
                                        Proxy.newProxyInstance(
                                            ResultSet::class.java.classLoader,
                                            arrayOf(ResultSet::class.java)
                                        ) { _, rsMethod, _ ->
                                            when (rsMethod.name) {
                                                // Return 1 row for pg_backend_pid and contract check, 0 rows for precheck
                                                "next" -> if (isPid || isContractCheck) served.compareAndSet(false, true) else false
                                                "getInt" -> if (isPid) backendPid else 0
                                                "getBoolean" -> true
                                                "getObject" -> true
                                                "getString" -> "public"
                                                "close" -> null
                                                else -> null
                                            }
                                        } as ResultSet
                                    }
                                    else -> null
                                }
                            } as PreparedStatement
                        }
                        "abort" -> {
                            if (isWriteSession) abortSignaledOnWriteConn.countDown()
                            null
                        }
                        "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close" -> null
                        "getAutoCommit" -> true
                        "isWrapperFor" -> false
                        "equals" -> proxy === args?.firstOrNull()
                        "hashCode" -> System.identityHashCode(proxy)
                        else -> null
                    }
                } as Connection
            }

            val abortTestDs = object : DataSource {
                override fun getConnection(): Connection {
                    val count = physicalConnsOpened.incrementAndGet()
                    check(count <= 2) { "Must NEVER open a 3rd physical connection (count=$count)" }
                    return createAbortTestConn(isWriteSession = count == 1, backendPid = 9100 + count)
                }
                override fun getConnection(username: String?, password: String?): Connection = getConnection()
                override fun getLogWriter(): PrintWriter? = null
                override fun setLogWriter(out: PrintWriter?) {}
                override fun setLoginTimeout(seconds: Int) {}
                override fun getLoginTimeout(): Int = 0
                override fun getParentLogger(): Logger = Logger.getGlobal()
                override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
                override fun isWrapperFor(iface: Class<*>?): Boolean = false
            }

            try {
                val abortRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                    args = listOf(
                        "--manifest-file=${manifestFile.absolutePath}",
                        "--dry-run=false",
                        "--network-enabled=true",
                        "--allow-durable-writes=true",
                        "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                        "--coordination-attestation=production-suspended-and-quiesced",
                        "--readiness-timeout-ms=2000",
                        "--db-timeout-ms=80",
                        "--max-wall-time-ms=10000"
                    ),
                    env = mapOf("OFFPROD_RIOT_API_KEY" to "test-valid-key"),
                    physicalDataSourceOverride = abortTestDs,
                    httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = mapOf(
                                "x-app-rate-limit" to "20:1,100:120",
                                "x-app-rate-limit-count" to "1:1,1:120",
                                "x-method-rate-limit" to "1000:60",
                                "x-method-rate-limit-count" to "1:60"
                            ),
                            bodyBytes = """{"puuid":"${officialPuuid(10)}","gameName":"Alpha","tagLine":"EUW"}""".toByteArray(StandardCharsets.UTF_8)
                        )
                    },
                    maxHeapBytesProvider = { 128L * 1024L * 1024L }
                )

                assertTrue(abortSignaledOnWriteConn.await(2, TimeUnit.SECONDS), "Expected abort/cancel on stalled write session")
                assertEquals(1, abortRes.exitCode)
                assertEquals(2, physicalConnsOpened.get(), "Expected at most 2 physical connections opened even after timeout/abort")
                assertEquals(2, abortRes.wrapperReport.physicalConnectionsOpened)
                assertTrue(abortRes.wrapperReport.poolPoisoned, "Expected poolPoisoned=true after timeout/abort")
                assertNotNull(abortRes.wrapperReport.poolPoisonReason)
                assertEquals(
                    RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
                    abortRes.wrapperReport.completionReport?.outcome
                )
            } finally {
                releaseStalledWrite.countDown()
            }
        } finally {
            manifestFile.delete()
        }
    }

    @Test
    fun `real PostgreSQL 15 qualification proves distinct write and readback pg_backend_pid across repeated candidates, max 2 connections, and 256MiB subprocess`() {
        // 1. Verify durable-write mode fails closed during readiness when public.player_riot_identities is absent (0 HTTP calls, 0 DDL)
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
                st.execute("TRUNCATE TABLE public.indexed_players")
            }
        }

        val candidates = (1..12).map { idx ->
            ManifestEntry(
                puuid = officialPuuid(idx),
                platformRegion = if (idx % 2 == 0) "euw1" else "na1"
            )
        }
        val manifestFile = writeTempManifest(candidates)
        val wrapperReportFile = Files.createTempFile("prewarm-wrapper-report-", ".json").toFile()
        val completionReportFile = Files.createTempFile("prewarm-completion-report-", ".json").toFile()

        try {
            val httpCallsWhenTableAbsent = AtomicInteger(0)
            val tableAbsentRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                    "--coordination-attestation=production-suspended-and-quiesced"
                ),
                env = mapOf(
                    "OFFPROD_JDBC_URL" to jdbcUrl,
                    "OFFPROD_DB_USER" to "postgres",
                    "OFFPROD_RIOT_API_KEY" to "test-valid-key"
                ),
                httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                    httpCallsWhenTableAbsent.incrementAndGet()
                    throw AssertionError("No HTTP call may occur when public.player_riot_identities is absent in durable-write mode")
                },
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, tableAbsentRes.exitCode)
            assertEquals(false, tableAbsentRes.wrapperReport.identityTablePresentAtReadiness)
            assertEquals(0, httpCallsWhenTableAbsent.get())

            // 1b. Verify real PostgreSQL readiness rejects same-name permissive OR-true and NOT VALID constraints (exitCode=3, 0 HTTP calls)
            val invalidConstraintDdls = listOf(
                """
                CREATE TABLE public.player_riot_identities (
                    puuid           VARCHAR(78)  PRIMARY KEY,
                    platform_region VARCHAR(16)  NOT NULL,
                    game_name       VARCHAR(255) NOT NULL,
                    tag_line        VARCHAR(255) NOT NULL,
                    verified_at     TIMESTAMPTZ  NOT NULL,
                    CONSTRAINT chk_player_riot_identities_nonblank
                        CHECK ((BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '') OR true)
                )
                """.trimIndent(),
                """
                CREATE TABLE public.player_riot_identities (
                    puuid           VARCHAR(78)  PRIMARY KEY,
                    platform_region VARCHAR(16)  NOT NULL,
                    game_name       VARCHAR(255) NOT NULL,
                    tag_line        VARCHAR(255) NOT NULL,
                    verified_at     TIMESTAMPTZ  NOT NULL
                );
                ALTER TABLE public.player_riot_identities
                    ADD CONSTRAINT chk_player_riot_identities_nonblank
                    CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '') NOT VALID
                """.trimIndent()
            )
            for (badConstraintDdl in invalidConstraintDdls) {
                pgDataSource.connection.use { conn ->
                    conn.createStatement().use { st ->
                        st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
                        st.execute(badConstraintDdl)
                    }
                }
                val badConstraintRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                    args = listOf(
                        "--manifest-file=${manifestFile.absolutePath}",
                        "--dry-run=false",
                        "--network-enabled=true",
                        "--allow-durable-writes=true",
                        "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                        "--coordination-attestation=production-suspended-and-quiesced"
                    ),
                    env = mapOf(
                        "OFFPROD_JDBC_URL" to jdbcUrl,
                        "OFFPROD_DB_USER" to "postgres",
                        "OFFPROD_RIOT_API_KEY" to "test-valid-key"
                    ),
                    httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                        httpCallsWhenTableAbsent.incrementAndGet()
                        throw AssertionError("No HTTP call may occur when public.player_riot_identities has permissive OR-true or NOT VALID constraint")
                    },
                    maxHeapBytesProvider = { 128L * 1024L * 1024L }
                )
                assertEquals(3, badConstraintRes.exitCode)
                assertEquals(true, badConstraintRes.wrapperReport.identityTablePresentAtReadiness)
                assertEquals(false, badConstraintRes.wrapperReport.identityTableShapeValidAtReadiness)
                assertEquals(0, httpCallsWhenTableAbsent.get())
            }
            pgDataSource.connection.use { conn ->
                conn.createStatement().use { st ->
                    st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
                }
            }

            // 2. Stage public.player_riot_identities via OffProdAdditiveIdentitySchemaSetupCli,
            // and seed candidate #1 as ALREADY_COMPLETE in indexed_players so 11 candidates require Riot lookup + DB upsert + post-commit verify:
            val setupRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(0, setupRes.exitCode)

            pgDataSource.connection.use { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO public.indexed_players (puuid, game_name, tag_line, platform_region, profile_icon_id, summoner_level, last_seen_at)
                    VALUES (?, 'ExistingChallenger', 'EUW1', 'na1', 1, 500, NOW())
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, officialPuuid(1))
                    ps.executeUpdate()
                }
            }

            // 3. Wrap real PostgreSQL DataSource with:
            //    - 1,600ms cold-connection latency per physical connection (2 x 1,600ms = 3,200ms during readiness,
            //      which would exceed the 2,500ms per-candidate DB fence if connections were opened cold per candidate!)
            //    - hard assertion that at most 2 physical connections are ever opened across all 12 candidates!
            val realPhysicalConnectionsOpened = AtomicInteger(0)
            val slowColdConnectRealPgDs = object : DataSource by pgDataSource {
                override fun getConnection(): Connection {
                    val n = realPhysicalConnectionsOpened.incrementAndGet()
                    check(n <= 2) { "Expected at most 2 physical PostgreSQL connections across entire run, but opened #$n" }
                    Thread.sleep(1_600L)
                    return pgDataSource.connection
                }
            }

            val multiCandidateRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                    "--coordination-attestation=production-suspended-and-quiesced",
                    "--readiness-timeout-ms=10000",
                    "--db-timeout-ms=2500",
                    "--max-wall-time-ms=180000",
                    "--output-report-file=${completionReportFile.absolutePath}",
                    "--output-wrapper-report-file=${wrapperReportFile.absolutePath}"
                ),
                env = mapOf("OFFPROD_RIOT_API_KEY" to "test-valid-key"),
                physicalDataSourceOverride = slowColdConnectRealPgDs,
                httpTransportOverride = BoundedAccountV1Transport { url, _, _, _, _, _, _ ->
                    val puuid = url.substringAfterLast("/")
                    val idx = candidates.indexOfFirst { it.normalizedPuuid == puuid } + 1
                    TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = mapOf(
                            "x-app-rate-limit" to "20:1,100:120",
                            "x-app-rate-limit-count" to "$idx:1,$idx:120",
                            "x-method-rate-limit" to "1000:60",
                            "x-method-rate-limit-count" to "$idx:60"
                        ),
                        bodyBytes = """{"puuid":"$puuid","gameName":"ChallengerPlayer$idx","tagLine":"TAG$idx"}""".toByteArray(StandardCharsets.UTF_8)
                    )
                },
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )

            assertEquals(0, multiCandidateRes.exitCode, "Expected exitCode=0: ${multiCandidateRes.wrapperReport.sanitizedDetail}")
            assertEquals(2, realPhysicalConnectionsOpened.get(), "Expected exactly 2 physical PostgreSQL connections opened")
            assertEquals(2, multiCandidateRes.wrapperReport.physicalConnectionsOpened)
            assertTrue(multiCandidateRes.wrapperReport.distinctWriteAndVerifyPidsVerified)
            assertFalse(multiCandidateRes.wrapperReport.poolPoisoned)
            assertTrue(multiCandidateRes.wrapperReport.connectionsClosedCleanly)
            assertFalse(multiCandidateRes.wrapperReport.cleanupTimedOut)

            val writePid = requireNotNull(multiCandidateRes.wrapperReport.writeBackendPid)
            val verifyPid = requireNotNull(multiCandidateRes.wrapperReport.readVerifyBackendPid)
            assertTrue(writePid > 0)
            assertTrue(verifyPid > 0)
            assertNotEquals(writePid, verifyPid, "Write and read/verify PostgreSQL backend PIDs must be distinct")

            // Readiness was >= 3,200ms and deducted from the 180,000ms budget:
            assertTrue(multiCandidateRes.wrapperReport.readinessElapsedMs >= 3_100L)
            assertTrue(multiCandidateRes.wrapperReport.remainingRunBudgetMs <= 180_000L - multiCandidateRes.wrapperReport.readinessElapsedMs)

            val completionReport = requireNotNull(multiCandidateRes.wrapperReport.completionReport)
            assertEquals(RunOutcome.STAGED_ALL_CANDIDATES, completionReport.outcome)
            assertEquals(1, completionReport.alreadyResolvedInDbCount)
            assertEquals(11, completionReport.durableIdentityTableUpsertsVerifiedCount)
            assertEquals(11, completionReport.httpAttemptsMade)

            // Verify per-candidate live pg_backend_pid() audit across all 11 written candidates!
            val audit = multiCandidateRes.wrapperReport.sqlSessionAudit
            for (idx in 2..12) {
                val puuid = officialPuuid(idx)
                val upsertEntries = audit.filter {
                    it.operationKind == OffProdPrewarmedChallengerIdentityCompletionCli.SqlOperationKind.CANDIDATE_IDENTITY_UPSERT &&
                        it.candidatePuuid == puuid
                }
                val verifyEntries = audit.filter {
                    it.operationKind == OffProdPrewarmedChallengerIdentityCompletionCli.SqlOperationKind.POST_COMMIT_VERIFY_IDENTITY_SELECT &&
                        it.candidatePuuid == puuid
                }
                assertEquals(1, upsertEntries.size, "Expected exactly 1 upsert audit entry for candidate $idx")
                assertEquals(1, verifyEntries.size, "Expected exactly 1 post-commit verify audit entry for candidate $idx")
                assertEquals(writePid, upsertEntries.single().backendPid, "Candidate $idx upsert must execute on writeBackendPid")
                assertEquals(verifyPid, verifyEntries.single().backendPid, "Candidate $idx verify must execute on readVerifyBackendPid")
                assertNotEquals(
                    upsertEntries.single().backendPid,
                    verifyEntries.single().backendPid,
                    "Candidate $idx write and readback must use distinct PostgreSQL backend PIDs"
                )
                assertNotEquals(
                    upsertEntries.single().leaseIdentityHash,
                    verifyEntries.single().leaseIdentityHash,
                    "Candidate $idx write and readback must use distinct Connection lease instances"
                )
            }

            // Verify all 11 rows are durably committed in public.player_riot_identities:
            pgDataSource.connection.use { conn ->
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT COUNT(*) FROM public.player_riot_identities").use { rs ->
                        assertTrue(rs.next())
                        assertEquals(11, rs.getInt(1))
                    }
                }
            }

            // 4. Standalone OS JVM subprocess qualification (-Xms16m -Xmx128m, <= 256 MiB cap) against real PostgreSQL + loopback HTTP stub:
            pgDataSource.connection.use { conn ->
                conn.createStatement().use { st ->
                    st.execute("TRUNCATE TABLE public.player_riot_identities")
                }
            }

            val httpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            val httpPort = httpServer.address.port
            httpServer.createContext("/riot/account/v1/accounts/by-puuid/") { exchange ->
                val reqPath = exchange.requestURI.path
                val puuid = reqPath.substringAfterLast("/")
                val idx = candidates.indexOfFirst { it.normalizedPuuid == puuid } + 1
                val body = """{"puuid":"$puuid","gameName":"SubprocPlayer$idx","tagLine":"SP$idx"}""".toByteArray(StandardCharsets.UTF_8)
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
                exchange.responseHeaders.add("X-App-Rate-Limit-Count", "$idx:1,$idx:120")
                exchange.responseHeaders.add("X-Method-Rate-Limit", "1000:60")
                exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "$idx:60")
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            httpServer.start()

            val subprocWrapperReportFile = Files.createTempFile("prewarm-subproc-wrapper-", ".json").toFile()
            val subprocCompletionReportFile = Files.createTempFile("prewarm-subproc-completion-", ".json").toFile()
            try {
                val javaBin = File(System.getProperty("java.home"), "bin/java").absolutePath
                val classpath = System.getProperty("java.class.path")
                val baseUrl = "http://127.0.0.1:$httpPort"
                val proc = ProcessBuilder(
                    javaBin,
                    "-Xms16m",
                    "-Xmx128m",
                    "-XX:MaxMetaspaceSize=96m",
                    "-XX:+UseSerialGC",
                    "-cp", classpath,
                    "com.pride.summoner_searcher_api.tools.OffProdPrewarmedChallengerIdentityCompletionCli",
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--allow-loopback-http-for-testing=true",
                    "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                    "--coordination-attestation=production-suspended-and-quiesced",
                    "--cluster-url-override=americas=$baseUrl,europe=$baseUrl,asia=$baseUrl",
                    "--readiness-timeout-ms=10000",
                    "--db-timeout-ms=2500",
                    "--max-wall-time-ms=180000",
                    "--enforce-max-heap-256m=true",
                    "--output-report-file=${subprocCompletionReportFile.absolutePath}",
                    "--output-wrapper-report-file=${subprocWrapperReportFile.absolutePath}"
                ).apply {
                    environment()["OFFPROD_JDBC_URL"] = jdbcUrl
                    environment()["OFFPROD_DB_USER"] = "postgres"
                    environment()["OFFPROD_DB_PASS"] = ""
                    environment()["OFFPROD_RIOT_API_KEY"] = "TEST-LOOPBACK-ONLY-SUBPROC"
                }.redirectErrorStream(true).start()

                val finished = proc.waitFor(20, TimeUnit.SECONDS)
                val out = proc.inputStream.bufferedReader().readText()
                assertTrue(finished, "Subprocess did not finish in 20s: $out")
                assertEquals(0, proc.exitValue(), "Subprocess failed: $out")

                val parsedSubprocWrapper = objectMapper.readValue(
                    subprocWrapperReportFile,
                    OffProdPrewarmedChallengerIdentityCompletionCli.PrewarmedCompletionExecutionReport::class.java
                )
                assertEquals(0, parsedSubprocWrapper.exitCode)
                assertEquals(2, parsedSubprocWrapper.physicalConnectionsOpened)
                assertTrue(parsedSubprocWrapper.distinctWriteAndVerifyPidsVerified)
                assertNotNull(parsedSubprocWrapper.writeBackendPid)
                assertNotNull(parsedSubprocWrapper.readVerifyBackendPid)
                assertNotEquals(parsedSubprocWrapper.writeBackendPid, parsedSubprocWrapper.readVerifyBackendPid)
                assertTrue(parsedSubprocWrapper.connectionsClosedCleanly)
                assertTrue(parsedSubprocWrapper.jvmMaxHeapBytes <= OffProdPrewarmedChallengerIdentityCompletionCli.MAX_STANDALONE_JVM_HEAP_BYTES)
                assertEquals(11, parsedSubprocWrapper.completionReport?.durableIdentityTableUpsertsVerifiedCount)
            } finally {
                httpServer.stop(0)
                subprocWrapperReportFile.delete()
                subprocCompletionReportFile.delete()
            }
        } finally {
            manifestFile.delete()
            wrapperReportFile.delete()
            completionReportFile.delete()
        }
    }

    @Test
    fun `negative tests - fail closed on missing or invalid PID, schema, or table shape evidence, sanitize poison-path exceptions, and include awaited cleanup in total budget`() {
        val manifestFile = writeTempManifest(
            listOf(ManifestEntry(puuid = officialPuuid(99), platformRegion = "euw1"))
        )
        val wrapperReportFile = Files.createTempFile("prewarm-poison-wrapper-", ".json").toFile()
        val completionReportFile = Files.createTempFile("prewarm-poison-completion-", ".json").toFile()

        try {
            val httpCallsOnReadinessFailure = AtomicInteger(0)

            fun createConfigurablePrewarmConn(
                pidPsNull: Boolean = false,
                pidRsNull: Boolean = false,
                pidZeroRows: Boolean = false,
                pidValue: Int = 9501,
                livePidZeroAfterReadiness: Boolean = false,
                searchPathPsNull: Boolean = false,
                contractPsNull: Boolean = false,
                contractRsNull: Boolean = false,
                contractZeroRows: Boolean = false,
                currentSchemaName: String? = "public",
                tablePresent: Boolean? = true,
                tableShapeValid: Boolean? = true,
                upsertErrorMessage: String? = null,
                onClose: () -> Unit = {}
            ): Connection {
                val pidCallCount = AtomicInteger(0)
                return Proxy.newProxyInstance(
                    Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)
                ) { proxy, method, args ->
                    when (method.name) {
                        "prepareStatement" -> {
                            val sql = (args?.firstOrNull() as? String).orEmpty()
                            when {
                                sql.contains("pg_backend_pid") -> {
                                    val callNum = pidCallCount.incrementAndGet()
                                    if (pidPsNull || (livePidZeroAfterReadiness && callNum > 1)) return@newProxyInstance null
                                    Proxy.newProxyInstance(
                                        PreparedStatement::class.java.classLoader,
                                        arrayOf(PreparedStatement::class.java)
                                    ) { _, psMethod, _ ->
                                        when (psMethod.name) {
                                            "setQueryTimeout", "close", "cancel" -> null
                                            "executeQuery" -> {
                                                if (pidRsNull) return@newProxyInstance null
                                                val served = AtomicBoolean(false)
                                                Proxy.newProxyInstance(
                                                    ResultSet::class.java.classLoader,
                                                    arrayOf(ResultSet::class.java)
                                                ) { _, rsMethod, _ ->
                                                    when (rsMethod.name) {
                                                        "next" -> if (pidZeroRows) false else served.compareAndSet(false, true)
                                                        "getInt" -> pidValue
                                                        "close" -> null
                                                        else -> null
                                                    }
                                                } as ResultSet
                                            }
                                            else -> null
                                        }
                                    } as PreparedStatement
                                }
                                sql.uppercase().contains("SET SEARCH_PATH") -> {
                                    if (searchPathPsNull) return@newProxyInstance null
                                    Proxy.newProxyInstance(
                                        PreparedStatement::class.java.classLoader,
                                        arrayOf(PreparedStatement::class.java)
                                    ) { _, psMethod, _ ->
                                        when (psMethod.name) {
                                            "setQueryTimeout", "close", "cancel" -> null
                                            "executeUpdate" -> 0
                                            else -> null
                                        }
                                    } as PreparedStatement
                                }
                                sql.contains("table_shape_valid") -> {
                                    if (contractPsNull) return@newProxyInstance null
                                    Proxy.newProxyInstance(
                                        PreparedStatement::class.java.classLoader,
                                        arrayOf(PreparedStatement::class.java)
                                    ) { _, psMethod, _ ->
                                        when (psMethod.name) {
                                            "setQueryTimeout", "close", "cancel" -> null
                                            "executeQuery" -> {
                                                if (contractRsNull) return@newProxyInstance null
                                                val served = AtomicBoolean(false)
                                                Proxy.newProxyInstance(
                                                    ResultSet::class.java.classLoader,
                                                    arrayOf(ResultSet::class.java)
                                                ) { _, rsMethod, rsArgs ->
                                                    when (rsMethod.name) {
                                                        "next" -> if (contractZeroRows) false else served.compareAndSet(false, true)
                                                        "getString" -> currentSchemaName
                                                        "getObject" -> when ((rsArgs?.firstOrNull() as? String).orEmpty()) {
                                                            "table_present" -> tablePresent
                                                            "table_shape_valid" -> tableShapeValid
                                                            else -> null
                                                        }
                                                        "close" -> null
                                                        else -> null
                                                    }
                                                } as ResultSet
                                            }
                                            else -> null
                                        }
                                    } as PreparedStatement
                                }
                                sql.uppercase().contains("INSERT INTO PLAYER_RIOT_IDENTITIES") -> {
                                    Proxy.newProxyInstance(
                                        PreparedStatement::class.java.classLoader,
                                        arrayOf(PreparedStatement::class.java)
                                    ) { _, psMethod, _ ->
                                        when (psMethod.name) {
                                            "setQueryTimeout", "setString", "setTimestamp", "close", "cancel" -> null
                                            "executeUpdate" -> {
                                                if (upsertErrorMessage != null) {
                                                    throw SQLException(upsertErrorMessage, "08006")
                                                }
                                                1
                                            }
                                            else -> null
                                        }
                                    } as PreparedStatement
                                }
                                else -> {
                                    // Precheck SELECT queries return 0 rows so candidate proceeds to HTTP + upsert
                                    Proxy.newProxyInstance(
                                        PreparedStatement::class.java.classLoader,
                                        arrayOf(PreparedStatement::class.java)
                                    ) { _, psMethod, _ ->
                                        when (psMethod.name) {
                                            "setQueryTimeout", "setString", "close", "cancel" -> null
                                            "executeQuery" -> {
                                                Proxy.newProxyInstance(
                                                    ResultSet::class.java.classLoader,
                                                    arrayOf(ResultSet::class.java)
                                                ) { _, rsMethod, _ ->
                                                    when (rsMethod.name) {
                                                        "next" -> false
                                                        "close" -> null
                                                        else -> null
                                                    }
                                                } as ResultSet
                                            }
                                            else -> null
                                        }
                                    } as PreparedStatement
                                }
                            }
                        }
                        "close" -> {
                            onClose()
                            null
                        }
                        "isWrapperFor" -> false
                        "getAutoCommit" -> true
                        "isReadOnly" -> false
                        "setReadOnly", "setAutoCommit", "setNetworkTimeout", "abort", "commit", "rollback" -> null
                        "equals" -> proxy === args?.firstOrNull()
                        "hashCode" -> System.identityHashCode(proxy)
                        else -> null
                    }
                } as Connection
            }

            fun dsFromFactory(factory: (Int) -> Connection): DataSource {
                val counter = AtomicInteger(0)
                return object : DataSource {
                    override fun getConnection(): Connection = factory(counter.incrementAndGet())
                    override fun getConnection(username: String?, password: String?): Connection = getConnection()
                    override fun getLogWriter(): PrintWriter? = null
                    override fun setLogWriter(out: PrintWriter?) {}
                    override fun setLoginTimeout(seconds: Int) {}
                    override fun getLoginTimeout(): Int = 0
                    override fun getParentLogger(): Logger = Logger.getGlobal()
                    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
                    override fun isWrapperFor(iface: Class<*>?): Boolean = false
                }
            }

            // 1. Missing/invalid PID evidence during readiness must fail closed (exitCode=3, 0 HTTP calls) and never fabricate 1001/1002:
            val invalidPidReadinessCases = listOf(
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidPsNull = true, pidValue = 9000 + idx) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidRsNull = true, pidValue = 9000 + idx) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidZeroRows = true, pidValue = 9000 + idx) },
                dsFromFactory { _ -> createConfigurablePrewarmConn(pidValue = 0) },
                dsFromFactory { _ -> createConfigurablePrewarmConn(pidValue = -99) }
            )

            for (badPidDs in invalidPidReadinessCases) {
                val res = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                    args = listOf(
                        "--manifest-file=${manifestFile.absolutePath}",
                        "--dry-run=false",
                        "--network-enabled=true",
                        "--allow-durable-writes=true",
                        "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                        "--coordination-attestation=production-suspended-and-quiesced"
                    ),
                    env = mapOf("OFFPROD_RIOT_API_KEY" to "test-valid-key"),
                    physicalDataSourceOverride = badPidDs,
                    httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                        httpCallsOnReadinessFailure.incrementAndGet()
                        throw AssertionError("No HTTP call may occur when PID evidence is missing/invalid")
                    },
                    maxHeapBytesProvider = { 128L * 1024L * 1024L }
                )
                assertEquals(3, res.exitCode)
                assertNull(res.wrapperReport.writeBackendPid, "Must not fabricate fallback PID 1001")
                assertNull(res.wrapperReport.readVerifyBackendPid, "Must not fabricate fallback PID 1002")
                assertFalse(res.wrapperReport.distinctWriteAndVerifyPidsVerified)
            }

            // 2. Missing/invalid schema or table shape evidence during readiness must fail closed (exitCode=3, 0 HTTP calls) and never default to Triple(true, true, true):
            val invalidContractReadinessCases = listOf(
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, searchPathPsNull = true) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, contractPsNull = true) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, contractRsNull = true) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, contractZeroRows = true) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, currentSchemaName = null) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, currentSchemaName = "   ") },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, currentSchemaName = "shadow_schema") },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, tablePresent = null) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, tablePresent = true, tableShapeValid = null) },
                dsFromFactory { idx -> createConfigurablePrewarmConn(pidValue = 9100 + idx, tablePresent = true, tableShapeValid = false) }
            )

            for (badContractDs in invalidContractReadinessCases) {
                val res = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                    args = listOf(
                        "--manifest-file=${manifestFile.absolutePath}",
                        "--dry-run=false",
                        "--network-enabled=true",
                        "--allow-durable-writes=true",
                        "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                        "--coordination-attestation=production-suspended-and-quiesced"
                    ),
                    env = mapOf("OFFPROD_RIOT_API_KEY" to "test-valid-key"),
                    physicalDataSourceOverride = badContractDs,
                    httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                        httpCallsOnReadinessFailure.incrementAndGet()
                        throw AssertionError("No HTTP call may occur when schema/shape evidence is missing/invalid")
                    },
                    maxHeapBytesProvider = { 128L * 1024L * 1024L }
                )
                assertEquals(3, res.exitCode)
            }
            assertEquals(0, httpCallsOnReadinessFailure.get(), "Expected 0 HTTP calls across all readiness negative cases")

            // 3. Live verifyLiveSessionBackendPid failure when SELECT pg_backend_pid() returns null during statement execution must poison pool and fail closed (never falling back to expectedPid):
            val livePidNullDs = dsFromFactory { idx ->
                createConfigurablePrewarmConn(pidValue = 9200 + idx, livePidZeroAfterReadiness = true)
            }
            val livePidNullRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=true"
                ),
                env = emptyMap(),
                physicalDataSourceOverride = livePidNullDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(1, livePidNullRes.exitCode)
            assertTrue(livePidNullRes.wrapperReport.poolPoisoned, "Expected poolPoisoned=true when live pg_backend_pid() returns null")
            assertEquals(
                RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
                livePidNullRes.wrapperReport.completionReport?.outcome
            )

            // 4. Poison-path secret redaction & no-transient-raw-report test:
            //    - Verify buildForwardedCliArgs never forwards --output-report-file to underlying fef2ed6 CLI.
            //    - Throw fake-marker SQLException through SQL execution poison path and verify that during close()
            //      (after underlying OffProdChallengerIdentityCompletionCli.runCli has returned, before wrapper writes),
            //      completionReportFile does NOT exist on disk (no transient raw report ever written).
            //    - Verify poolPoisonReason, sanitizedDetail, completionReport, and both JSON files redact every secret marker:
            val forwardedCheck = OffProdPrewarmedChallengerIdentityCompletionCli.buildForwardedCliArgs(
                flags = mapOf(
                    "manifest-file" to manifestFile.absolutePath,
                    "output-report-file" to completionReportFile.absolutePath,
                    "output-wrapper-report-file" to wrapperReportFile.absolutePath,
                    "readiness-timeout-ms" to "2000",
                    "cleanup-timeout-ms" to "1000"
                ),
                remainingRunBudgetMs = 45_000L
            )
            assertFalse(
                forwardedCheck.any { it.startsWith("--output-report-file") },
                "buildForwardedCliArgs must never forward --output-report-file to underlying CLI: $forwardedCheck"
            )
            assertFalse(
                forwardedCheck.any { it.startsWith("--output-wrapper-report-file") },
                "buildForwardedCliArgs must never forward --output-wrapper-report-file to underlying CLI: $forwardedCheck"
            )

            completionReportFile.delete()
            wrapperReportFile.delete()
            assertFalse(completionReportFile.exists())
            assertFalse(wrapperReportFile.exists())

            val explicitDbSecretMarker = "NON_CREDENTIAL_POISON_DB_SECRET_MARKER"
            val explicitApiSecretMarker = "NON_CREDENTIAL_POISON_API_SECRET_MARKER"
            val formatPrefixMarker = "RGAPI-NON_CREDENTIAL_POISON_FORMAT_MARKER"
            val formatKvMarker = "NON_CREDENTIAL_POISON_KV_MARKER"
            val transientReportExistedBeforeWrapperWrite = AtomicBoolean(false)
            val closeHookObservedAfterRunCli = AtomicBoolean(false)

            val poisonSecretDs = dsFromFactory { idx ->
                createConfigurablePrewarmConn(
                    pidValue = 9300 + idx,
                    upsertErrorMessage = "FATAL: connection terminated on jdbc:postgresql://db.internal:5432/prod?password=$explicitDbSecretMarker " +
                        "apiKey=$explicitApiSecretMarker header=$formatPrefixMarker token=$formatKvMarker",
                    onClose = {
                        if (closeHookObservedAfterRunCli.compareAndSet(false, true)) {
                            if (completionReportFile.exists() || wrapperReportFile.exists()) {
                                transientReportExistedBeforeWrapperWrite.set(true)
                            }
                        }
                    }
                )
            }

            val poisonSecretRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=false",
                    "--network-enabled=true",
                    "--allow-durable-writes=true",
                    "--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW",
                    "--coordination-attestation=production-suspended-and-quiesced",
                    "--output-report-file=${completionReportFile.absolutePath}",
                    "--output-wrapper-report-file=${wrapperReportFile.absolutePath}"
                ),
                env = mapOf(
                    "OFFPROD_DB_PASS" to explicitDbSecretMarker,
                    "OFFPROD_RIOT_API_KEY" to explicitApiSecretMarker
                ),
                physicalDataSourceOverride = poisonSecretDs,
                httpTransportOverride = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                    TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = mapOf(
                            "x-app-rate-limit" to "20:1,100:120",
                            "x-app-rate-limit-count" to "1:1,1:120",
                            "x-method-rate-limit" to "1000:60",
                            "x-method-rate-limit-count" to "1:60"
                        ),
                        bodyBytes = """{"puuid":"${officialPuuid(99)}","gameName":"PoisonTest","tagLine":"REDACT"}""".toByteArray(StandardCharsets.UTF_8)
                    )
                },
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )

            assertTrue(closeHookObservedAfterRunCli.get(), "Expected connection close hook to execute after underlying runCli returned")
            assertFalse(
                transientReportExistedBeforeWrapperWrite.get(),
                "Expected NO transient report file on disk after underlying runCli returned and before wrapper wrote sanitized report"
            )
            assertTrue(completionReportFile.exists(), "Expected wrapper to write sanitized completion report")
            assertTrue(wrapperReportFile.exists(), "Expected wrapper to write sanitized wrapper report")
            assertEquals(completionReportFile.absolutePath, poisonSecretRes.cliResult?.outputReportPath)

            assertEquals(1, poisonSecretRes.exitCode)
            assertTrue(poisonSecretRes.wrapperReport.poolPoisoned)
            val poisonReason = requireNotNull(poisonSecretRes.wrapperReport.poolPoisonReason)
            val wrapperJson = wrapperReportFile.readText()
            val completionJson = completionReportFile.readText()

            val forbiddenMarkers = listOf(
                explicitDbSecretMarker,
                explicitApiSecretMarker,
                formatPrefixMarker,
                formatKvMarker,
                "db.internal"
            )
            for (marker in forbiddenMarkers) {
                assertFalse(poisonReason.contains(marker), "poolPoisonReason leaked marker '$marker': $poisonReason")
                assertFalse(poisonSecretRes.wrapperReport.sanitizedDetail.contains(marker), "sanitizedDetail leaked marker '$marker'")
                assertFalse(
                    poisonSecretRes.wrapperReport.completionReport?.abortReason.orEmpty().contains(marker),
                    "completionReport.abortReason leaked marker '$marker'"
                )
                for (item in poisonSecretRes.wrapperReport.completionReport?.itemResults.orEmpty()) {
                    assertFalse(item.detail.orEmpty().contains(marker), "itemResult.detail leaked marker '$marker'")
                }
                assertFalse(wrapperJson.contains(marker), "Serialized wrapper report JSON leaked marker '$marker'")
                assertFalse(completionJson.contains(marker), "Serialized completion report JSON leaked marker '$marker'")
            }

            // 5. Full 180s budget reserves cleanupTimeoutMs and fails closed with exitCode=10 if awaited cleanup exceeds maxWallTimeMs:
            var fakeNowMs = 1_000_000L
            val fakeClock = object : MonotonicClock {
                override fun nowMs(): Long = fakeNowMs
                override fun sleepMs(ms: Long) {
                    fakeNowMs += ms
                }
            }
            val cleanupOverWallTimeDs = dsFromFactory { idx ->
                createConfigurablePrewarmConn(
                    pidValue = 9400 + idx,
                    onClose = {
                        // Advance fake clock during awaited close() so total elapsed exceeds maxWallTimeMs (5,000ms)
                        fakeNowMs += 5_500L
                    }
                )
            }
            val overWallTimeRes = OffProdPrewarmedChallengerIdentityCompletionCli.runPrewarmedCli(
                args = listOf(
                    "--manifest-file=${manifestFile.absolutePath}",
                    "--dry-run=true",
                    "--readiness-timeout-ms=2000",
                    "--cleanup-timeout-ms=1000",
                    "--max-wall-time-ms=5000"
                ),
                env = emptyMap(),
                physicalDataSourceOverride = cleanupOverWallTimeDs,
                monotonicClock = fakeClock,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(10, overWallTimeRes.exitCode)
            assertTrue(
                overWallTimeRes.wrapperReport.remainingRunBudgetMs in 3_800L..4_000L,
                "Expected remainingRunBudgetMs (${overWallTimeRes.wrapperReport.remainingRunBudgetMs}ms) to reserve 1,000ms cleanupTimeoutMs from 5,000ms maxWallTimeMs"
            )
            assertTrue(overWallTimeRes.wrapperReport.totalWallElapsedMs > overWallTimeRes.wrapperReport.totalWallBudgetMs)
        } finally {
            manifestFile.delete()
            wrapperReportFile.delete()
            completionReportFile.delete()
        }
    }
}
