package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.io.PrintWriter
import java.lang.reflect.Proxy
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OffProdAdditiveIdentitySchemaSetupCliTest {

    private var pgBinDir: File? = null
    private var pgTempDir: Path? = null
    private var pgPort: Int = 0
    private lateinit var jdbcUrl: String
    private lateinit var pgDataSource: PGSimpleDataSource

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

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
            "Local PostgreSQL binaries (/usr/lib/postgresql/*/bin/initdb) required for real PG setup qualification; test skips are forbidden"
        }
        pgBinDir = binDir

        val tempRoot = Files.createTempDirectory("offprod-setup-pg-qual-")
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
        check(startProc.waitFor() == 0) {
            "pg_ctl start failed: $startOut\nLog:\n${logFile.takeIf { it.exists() }?.readText()}"
        }

        jdbcUrl = "jdbc:postgresql://127.0.0.1:$pgPort/postgres"
        pgDataSource = PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = "postgres"
            password = ""
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

    private fun CountingNonConnectingDataSource(
        getConnCounter: AtomicInteger
    ): DataSource = object : DataSource {
        override fun getConnection(): Connection {
            getConnCounter.incrementAndGet()
            throw AssertionError("getConnection() must not be called when options are invalid")
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

    @Test
    fun `pre-validation rejects all invalid flags, timeout bounds, mode combinations, and 256MiB heap cap with 0 connections and 0 SQL`() {
        val getConnCalls = AtomicInteger(0)
        val countingDs = CountingNonConnectingDataSource(getConnCalls)

        val invalidArgSets = listOf(
            listOf("--unknown-flag=true"),
            listOf("positional-arg"),
            listOf("--dry-run=notabool"),
            listOf("--readiness-timeout-ms=notalong"),
            listOf("--readiness-timeout-ms=0"),
            listOf("--readiness-timeout-ms=10001"),
            listOf("--sql-timeout-ms=0"),
            listOf("--sql-timeout-ms=2501"),
            listOf("--lock-timeout-ms=0"),
            listOf("--lock-timeout-ms=2001"),
            listOf("--lock-timeout-ms=2000", "--sql-timeout-ms=1500"),
            listOf("--max-total-wall-time-ms=0"),
            listOf("--max-total-wall-time-ms=15001"),
            listOf("--max-total-wall-time-ms=1000", "--sql-timeout-ms=2500"),
            listOf("--apply-ddl=true", "--dry-run=true", "--allow-ddl-writes=true"),
            listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=false"),
            listOf("--dry-run=false", "--apply-ddl=false", "--allow-ddl-writes=true")
        )

        for (invalidArgs in invalidArgSets) {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = invalidArgs,
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_PASS" to "TOP_SECRET_PW_999"),
                dataSourceOverride = countingDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(2, res.exitCode, "Expected exitCode=2 for invalid args $invalidArgs")
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_INVALID_OPTIONS,
                res.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupStage.OPTION_VALIDATION,
                res.report.stage
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                res.report.ddlCommitStatus
            )
            assertFalse(res.report.sanitizedDetail.contains("TOP_SECRET_PW_999"))
        }

        // Also verify standalone > 256 MiB heap cap rejection before any connection
        val heapExceededRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--dry-run=true"),
            env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl),
            dataSourceOverride = countingDs,
            maxHeapBytesProvider = { 257L * 1024L * 1024L }
        )
        assertEquals(2, heapExceededRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_INVALID_OPTIONS,
            heapExceededRes.report.outcome
        )
        assertTrue(heapExceededRes.report.sanitizedDetail.contains("256 MiB"))

        // Also verify non-PostgreSQL JDBC URL rejection before any connection
        val badUrlRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--dry-run=true", "--jdbc-url=jdbc:h2:mem:test"),
            env = emptyMap(),
            dataSourceOverride = null,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(2, badUrlRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_INVALID_OPTIONS,
            badUrlRes.report.outcome
        )

        assertEquals(0, getConnCalls.get(), "Expected 0 DataSource.getConnection() calls across all invalid option cases")
    }

    @Test
    fun `shared 15s total wall-time cap bounds combined readiness and SQL stages and aborts with 0 SQL when exhausted before SQL stage`() {
        val sqlExecutedCount = AtomicInteger(0)
        var fakeNowMs = 1_000_000L
        val fakeClock = object : MonotonicClock {
            override fun nowMs(): Long = fakeNowMs
            override fun sleepMs(ms: Long) {
                fakeNowMs += ms
            }
        }

        val mockConn = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { proxy, method, args ->
            when (method.name) {
                "prepareStatement", "createStatement" -> {
                    sqlExecutedCount.incrementAndGet()
                    throw AssertionError("No SQL should be prepared when shared total wall-time budget is exhausted after readiness")
                }
                "isWrapperFor" -> false
                "getAutoCommit" -> true
                "isReadOnly" -> false
                "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                "equals" -> proxy === args?.firstOrNull()
                "hashCode" -> System.identityHashCode(proxy)
                else -> null
            }
        } as Connection

        val connCalls = AtomicInteger(0)
        val ds = object : DataSource {
            override fun getConnection(): Connection {
                val c = connCalls.incrementAndGet()
                // Advance monotonic clock by 1,500ms on each of the 2 readiness connections (total 3,000ms),
                // exhausting maxTotalWallTimeMs = 3,000ms right as readiness finishes!
                fakeNowMs += 1_500L
                return if (c == 1) mockConn else Proxy.newProxyInstance(
                    Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)
                ) { proxy, method, args ->
                    when (method.name) {
                        "prepareStatement", "createStatement" -> {
                            sqlExecutedCount.incrementAndGet()
                            throw AssertionError("No SQL should be prepared on verifyConn when total budget exhausted")
                        }
                        "isWrapperFor" -> false
                        "getAutoCommit" -> true
                        "isReadOnly" -> true
                        "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
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

        val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf(
                "--apply-ddl=true",
                "--dry-run=false",
                "--allow-ddl-writes=true",
                "--readiness-timeout-ms=5000",
                "--sql-timeout-ms=2500",
                "--lock-timeout-ms=2000",
                "--max-total-wall-time-ms=3000"
            ),
            env = emptyMap(),
            dataSourceOverride = ds,
            monotonicClock = fakeClock,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )

        assertEquals(10, res.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED,
            res.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            res.report.ddlCommitStatus
        )
        assertEquals(0, sqlExecutedCount.get(), "Expected 0 SQL statements when shared total wall-time budget is exhausted")
    }

    @Test
    fun `deterministic latch test - delayed connection acquisition released AFTER readiness timeout return executes 0 SQL statements`() {
        val getConnStarted = CountDownLatch(1)
        val releaseGetConn = CountDownLatch(1)
        val lateConnClosed = CountDownLatch(1)
        val sqlPreparedCount = AtomicInteger(0)
        val sqlExecutedCount = AtomicInteger(0)

        val mockConn = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "prepareStatement", "createStatement" -> {
                    sqlPreparedCount.incrementAndGet()
                    sqlExecutedCount.incrementAndGet()
                    throw AssertionError("No SQL statement may be prepared or executed on a late-acquired connection")
                }
                "close", "abort" -> {
                    lateConnClosed.countDown()
                    null
                }
                "isClosed" -> false
                "getAutoCommit" -> true
                "isReadOnly" -> false
                "setReadOnly", "setAutoCommit", "setNetworkTimeout" -> null
                "isWrapperFor" -> false
                "toString" -> "MockLateConnection"
                "hashCode" -> 101
                "equals" -> false
                else -> null
            }
        } as Connection

        val stalledDs = object : DataSource {
            override fun getConnection(): Connection {
                getConnStarted.countDown()
                // Ignore thread interruption to simulate uninterruptible socket/TLS stall
                var interrupted = false
                while (true) {
                    try {
                        if (releaseGetConn.await(5, TimeUnit.SECONDS)) break
                        break
                    } catch (_: InterruptedException) {
                        interrupted = true
                    }
                }
                if (interrupted) Thread.currentThread().interrupt()
                return mockConn
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

        val result = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf(
                "--apply-ddl=true",
                "--dry-run=false",
                "--allow-ddl-writes=true",
                "--readiness-timeout-ms=80",
                "--sql-timeout-ms=2500",
                "--lock-timeout-ms=2000",
                "--max-total-wall-time-ms=15000"
            ),
            env = emptyMap(),
            dataSourceOverride = stalledDs,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )

        assertTrue(getConnStarted.await(1, TimeUnit.SECONDS))
        assertEquals(3, result.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL,
            result.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupStage.CONNECTION_READINESS,
            result.report.stage
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            result.report.ddlCommitStatus
        )

        // Release connection acquisition AFTER caller has already returned
        releaseGetConn.countDown()
        assertTrue(lateConnClosed.await(2, TimeUnit.SECONDS), "Expected late-acquired connection to be closed/aborted")
        Thread.sleep(50)
        assertEquals(0, sqlPreparedCount.get(), "Expected 0 prepareStatement calls after readiness timeout return")
        assertEquals(0, sqlExecutedCount.get(), "Expected 0 SQL executions after readiness timeout return")
    }

    @Test
    fun `deterministic latch test - delayed DDL setter setQueryTimeout released AFTER 2500ms SQL fence return executes 0 DDL executeUpdate calls`() {
        val ddlSetterStarted = CountDownLatch(1)
        val releaseDdlSetter = CountDownLatch(1)
        val ddlWorkerFinished = CountDownLatch(1)
        val ddlExecuteUpdateCalls = AtomicInteger(0)

        fun createMockConnection(isVerifyConn: Boolean): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        val isDdl = sql.uppercase().contains("CREATE TABLE")
                        Proxy.newProxyInstance(
                            PreparedStatement::class.java.classLoader,
                            arrayOf(PreparedStatement::class.java)
                        ) { _, psMethod, _ ->
                            when (psMethod.name) {
                                "setQueryTimeout" -> {
                                    if (isDdl) {
                                        ddlSetterStarted.countDown()
                                        var interrupted = false
                                        while (true) {
                                            try {
                                                if (releaseDdlSetter.await(5, TimeUnit.SECONDS)) break
                                                break
                                            } catch (_: InterruptedException) {
                                                interrupted = true
                                            }
                                        }
                                        if (interrupted) Thread.currentThread().interrupt()
                                    }
                                    null
                                }
                                "executeUpdate" -> {
                                    ddlExecuteUpdateCalls.incrementAndGet()
                                    1
                                }
                                "executeQuery" -> {
                                    val rowServed = AtomicBoolean(false)
                                    Proxy.newProxyInstance(
                                        ResultSet::class.java.classLoader,
                                        arrayOf(ResultSet::class.java)
                                    ) { _, rsMethod, rsArgs ->
                                        when (rsMethod.name) {
                                            "next" -> rowServed.compareAndSet(false, true)
                                            "getBoolean" -> {
                                                val col = (rsArgs?.firstOrNull() as? String).orEmpty()
                                                col == "can_create"
                                            }
                                            "close" -> null
                                            else -> null
                                        }
                                    } as ResultSet
                                }
                                "close" -> {
                                    if (isDdl) ddlWorkerFinished.countDown()
                                    null
                                }
                                "cancel" -> null
                                else -> null
                            }
                        } as PreparedStatement
                    }
                    "isWrapperFor" -> false
                    "getAutoCommit" -> true
                    "isReadOnly" -> isVerifyConn
                    "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                    "equals" -> proxy === args?.firstOrNull()
                    "hashCode" -> System.identityHashCode(proxy)
                    "toString" -> if (isVerifyConn) "MockVerifyConn" else "MockDdlConn"
                    else -> null
                }
            } as Connection
        }

        val connCounter = AtomicInteger(0)
        val ds = object : DataSource {
            override fun getConnection(): Connection {
                val idx = connCounter.incrementAndGet()
                return createMockConnection(isVerifyConn = idx > 1)
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

        val result = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf(
                "--apply-ddl=true",
                "--dry-run=false",
                "--allow-ddl-writes=true",
                "--readiness-timeout-ms=2000",
                "--sql-timeout-ms=90",
                "--lock-timeout-ms=80",
                "--max-total-wall-time-ms=5000"
            ),
            env = emptyMap(),
            dataSourceOverride = ds,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )

        assertTrue(ddlSetterStarted.await(1, TimeUnit.SECONDS))
        assertEquals(3, result.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
            result.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            result.report.ddlCommitStatus
        )
        assertNull(result.report.recheckCommandHint)

        // Now release stalled setter AFTER caller return and verify 0 executeUpdate calls!
        releaseDdlSetter.countDown()
        assertTrue(ddlWorkerFinished.await(2, TimeUnit.SECONDS))
        Thread.sleep(50)
        assertEquals(0, ddlExecuteUpdateCalls.get(), "Expected 0 DDL executeUpdate() calls after setter timeout return")
    }

    @Test
    fun `in-flight DDL lock timeout or SQL exception reports UNKNOWN_COMMIT_RECHECK_REQUIRED and scrubs secrets`() {
        val secretPassword = "SuperSecretDbPassword!2026"
        val secretRiotKey = "RGAPI-11112222-3333-4444-5555-666677778888"

        fun createConn(isVerifyConn: Boolean): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        val isDdl = sql.uppercase().contains("CREATE TABLE")
                        Proxy.newProxyInstance(
                            PreparedStatement::class.java.classLoader,
                            arrayOf(PreparedStatement::class.java)
                        ) { _, psMethod, _ ->
                            when (psMethod.name) {
                                "setQueryTimeout", "close", "cancel" -> null
                                "executeUpdate" -> {
                                    if (isDdl) {
                                        throw SQLException(
                                            "ERROR: canceling statement due to lock timeout on jdbc:postgresql://db.internal:5432/prod?password=$secretPassword token=$secretRiotKey",
                                            "55P03"
                                        )
                                    }
                                    0
                                }
                                "executeQuery" -> {
                                    val served = AtomicBoolean(false)
                                    Proxy.newProxyInstance(
                                        ResultSet::class.java.classLoader,
                                        arrayOf(ResultSet::class.java)
                                    ) { _, rsMethod, rsArgs ->
                                        when (rsMethod.name) {
                                            "next" -> served.compareAndSet(false, true)
                                            "getBoolean" -> (rsArgs?.firstOrNull() as? String) == "can_create"
                                            "close" -> null
                                            else -> null
                                        }
                                    } as ResultSet
                                }
                                else -> null
                            }
                        } as PreparedStatement
                    }
                    "isWrapperFor" -> false
                    "getAutoCommit" -> true
                    "isReadOnly" -> isVerifyConn
                    "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                    "equals" -> proxy === args?.firstOrNull()
                    "hashCode" -> System.identityHashCode(proxy)
                    else -> null
                }
            } as Connection
        }

        val count = AtomicInteger(0)
        val ds = object : DataSource {
            override fun getConnection(): Connection = createConn(isVerifyConn = count.incrementAndGet() > 1)
            override fun getConnection(username: String?, password: String?): Connection = getConnection()
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        val reportFile = Files.createTempFile("setup-unknown-commit-", ".json").toFile()
        try {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf(
                    "--apply-ddl=true",
                    "--dry-run=false",
                    "--allow-ddl-writes=true",
                    "--output-report-file=${reportFile.absolutePath}"
                ),
                env = mapOf(
                    "OFFPROD_DB_PASS" to secretPassword,
                    "OFFPROD_RIOT_API_KEY" to secretRiotKey
                ),
                dataSourceOverride = ds,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )

            assertEquals(3, res.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT,
                res.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                res.report.ddlCommitStatus
            )
            assertNotNull(res.report.recheckCommandHint)
            assertFalse(res.report.sanitizedDetail.contains(secretPassword))
            assertFalse(res.report.sanitizedDetail.contains(secretRiotKey))
            assertFalse(res.report.sanitizedDetail.contains("db.internal"))

            val rawJson = reportFile.readText()
            assertFalse(rawJson.contains(secretPassword))
            assertFalse(rawJson.contains(secretRiotKey))
            assertFalse(rawJson.contains("db.internal"))
        } finally {
            reportFile.delete()
        }
    }

    @Test
    fun `real PostgreSQL 15 qualification covers default dry-run, 3100ms slow WAN readiness + fast DDL, real lock_timeout contention, and standalone 256MiB JVM cap`() {
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
            }
        }

        // 1. Default dry-run (no mode flags -> dryRun=true, applyDdl=false): 0 DDL executed, table remains absent
        val dryRunReportFile = Files.createTempFile("setup-pg-dryrun-", ".json").toFile()
        val dryRunRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--output-report-file=${dryRunReportFile.absolutePath}"),
            env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(0, dryRunRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
            dryRunRes.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            dryRunRes.report.ddlCommitStatus
        )
        assertEquals(false, dryRunRes.report.tablePresentBefore)
        assertEquals(false, dryRunRes.report.tablePresentAfter)
        assertEquals(true, dryRunRes.report.hasCreatePrivilege)
        dryRunReportFile.delete()

        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT to_regclass('public.player_riot_identities')").use { rs ->
                    assertTrue(rs.next())
                    assertNull(rs.getString(1), "Dry-run must not create player_riot_identities")
                }
            }
        }

        // 2. Real PostgreSQL lock contention test:
        // Hold an uncommitted transaction creating player_riot_identities so catalog lock blocks concurrent CREATE TABLE IF NOT EXISTS,
        // proving PostgreSQL server-side lock_timeout (<= 200ms here) trips within the 2,500ms SQL fence and reports UNKNOWN_COMMIT_RECHECK_REQUIRED!
        pgDataSource.connection.use { lockHolderConn ->
            lockHolderConn.autoCommit = false
            lockHolderConn.createStatement().use { st ->
                st.execute(OffProdAdditiveIdentitySchemaSetupCli.loadAndValidateExactAdditiveMigrationDdl())
            }

            val lockRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf(
                    "--apply-ddl=true",
                    "--dry-run=false",
                    "--allow-ddl-writes=true",
                    "--readiness-timeout-ms=5000",
                    "--sql-timeout-ms=1500",
                    "--lock-timeout-ms=150",
                    "--max-total-wall-time-ms=10000"
                ),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, lockRes.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT,
                lockRes.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                lockRes.report.ddlCommitStatus
            )
            assertTrue(lockRes.report.sqlElapsedMs < 1500L, "Expected lock_timeout to abort well under 1500ms SQL fence")

            lockHolderConn.rollback()
        }

        // Confirm table is still absent after lockHolderConn rolled back
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT to_regclass('public.player_riot_identities')").use { rs ->
                    assertTrue(rs.next())
                    assertNull(rs.getString(1))
                }
            }
        }

        // 3. Slow WAN connection readiness simulation (1,550ms per getConnection x 2 = 3,100ms > 2,500ms SQL cap,
        // but <= 10,000ms readiness cap) against real PostgreSQL 15.19, followed by fast (< 2,500ms) DDL + verification:
        val slowWanPgDataSource = object : DataSource by pgDataSource {
            override fun getConnection(): Connection {
                Thread.sleep(1_550L)
                return pgDataSource.connection
            }
        }

        val slowWanRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf(
                "--apply-ddl=true",
                "--dry-run=false",
                "--allow-ddl-writes=true",
                "--readiness-timeout-ms=10000",
                "--sql-timeout-ms=2500",
                "--lock-timeout-ms=2000",
                "--max-total-wall-time-ms=15000"
            ),
            env = emptyMap(),
            dataSourceOverride = slowWanPgDataSource,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )

        assertEquals(0, slowWanRes.exitCode, "Expected exitCode=0: detail=${slowWanRes.report.sanitizedDetail}")
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.APPLIED_AND_VERIFIED_DDL,
            slowWanRes.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.COMMITTED_AND_VERIFIED,
            slowWanRes.report.ddlCommitStatus
        )
        assertEquals(false, slowWanRes.report.tablePresentBefore)
        assertEquals(true, slowWanRes.report.tablePresentAfter)
        assertTrue(
            slowWanRes.report.readinessElapsedMs >= 3_000L,
            "Expected readinessElapsedMs >= 3000ms (was ${slowWanRes.report.readinessElapsedMs}ms)"
        )
        assertTrue(
            slowWanRes.report.sqlElapsedMs < 2_500L,
            "Expected sqlElapsedMs < 2500ms (was ${slowWanRes.report.sqlElapsedMs}ms)"
        )

        // 4. Standalone OS JVM subprocess qualification with strict -Xmx128m (<= 256 MiB cap) against real PostgreSQL:
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
            }
        }

        val subprocessReportFile = Files.createTempFile("setup-subprocess-256m-", ".json").toFile()
        try {
            val javaBin = File(System.getProperty("java.home"), "bin/java").absolutePath
            val classpath = System.getProperty("java.class.path")
            val proc = ProcessBuilder(
                javaBin,
                "-Xms16m",
                "-Xmx128m",
                "-XX:MaxMetaspaceSize=96m",
                "-XX:+UseSerialGC",
                "-cp", classpath,
                "com.pride.summoner_searcher_api.tools.OffProdAdditiveIdentitySchemaSetupCli",
                "--apply-ddl=true",
                "--dry-run=false",
                "--allow-ddl-writes=true",
                "--readiness-timeout-ms=10000",
                "--sql-timeout-ms=2500",
                "--lock-timeout-ms=2000",
                "--max-total-wall-time-ms=15000",
                "--enforce-max-heap-256m=true",
                "--output-report-file=${subprocessReportFile.absolutePath}"
            ).apply {
                environment()["OFFPROD_JDBC_URL"] = jdbcUrl
                environment()["OFFPROD_DB_USER"] = "postgres"
                environment()["OFFPROD_DB_PASS"] = ""
            }.redirectErrorStream(true).start()

            val finished = proc.waitFor(15, TimeUnit.SECONDS)
            val procOutput = proc.inputStream.bufferedReader().readText()
            assertTrue(finished, "Subprocess did not finish within 15s: $procOutput")
            assertEquals(0, proc.exitValue(), "Subprocess failed: $procOutput")

            val parsedReport = objectMapper.readValue(
                subprocessReportFile,
                OffProdAdditiveIdentitySchemaSetupCli.SetupExecutionReport::class.java
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.APPLIED_AND_VERIFIED_DDL,
                parsedReport.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.COMMITTED_AND_VERIFIED,
                parsedReport.ddlCommitStatus
            )
            assertEquals(false, parsedReport.tablePresentBefore)
            assertEquals(true, parsedReport.tablePresentAfter)
            assertTrue(
                parsedReport.jvmMaxHeapBytes <= OffProdAdditiveIdentitySchemaSetupCli.MAX_STANDALONE_JVM_HEAP_BYTES,
                "Expected jvmMaxHeapBytes <= 256 MiB (was ${parsedReport.jvmMaxHeapBytes})"
            )
            assertTrue(
                parsedReport.jvmUsedHeapBytesPeak <= 128L * 1024L * 1024L,
                "Expected peak used heap <= 128 MiB (was ${parsedReport.jvmUsedHeapBytesPeak})"
            )

            // Idempotent re-run when table is already present:
            val idempotentRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf(
                    "--apply-ddl=true",
                    "--dry-run=false",
                    "--allow-ddl-writes=true"
                ),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(0, idempotentRes.exitCode)
            assertEquals(true, idempotentRes.report.tablePresentBefore)
            assertEquals(true, idempotentRes.report.tablePresentAfter)
        } finally {
            subprocessReportFile.delete()
        }
    }
}
