package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
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

    private var pgTempDir: Path? = null
    private var pgBinDir: File? = null
    private var pgPort: Int = 0
    private lateinit var jdbcUrl: String
    private lateinit var pgDataSource: PGSimpleDataSource

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

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

        val tempRoot = Files.createTempDirectory("offprod-setup-cli-pg-")
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
            listOf("--cleanup-timeout-ms=0"),
            listOf("--cleanup-timeout-ms=5001"),
            listOf("--apply-ddl=true", "--dry-run=true", "--allow-ddl-writes=true"),
            listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=false"),
            listOf("--dry-run=false", "--apply-ddl=false", "--allow-ddl-writes=true")
        )

        for (invalidArgs in invalidArgSets) {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = invalidArgs,
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_PASS" to "NON_CREDENTIAL_TEST_DB_MARKER_ONLY"),
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
            assertFalse(res.report.sanitizedDetail.contains("NON_CREDENTIAL_TEST_DB_MARKER_ONLY"))
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
    fun `shared 15s total wall-time cap bounds combined readiness and SQL stages and aborts with 0 DDL when exhausted before SQL stage`() {
        val ddlOrCheckSqlExecutedCount = AtomicInteger(0)
        var fakeNowMs = 1_000_000L
        val fakeClock = object : MonotonicClock {
            override fun nowMs(): Long = fakeNowMs
            override fun sleepMs(ms: Long) {
                fakeNowMs += ms
            }
        }

        fun createReadinessOnlyConn(backendPid: Int, readOnlyDefault: Boolean): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        if (sql.contains("pg_backend_pid")) {
                            Proxy.newProxyInstance(
                                PreparedStatement::class.java.classLoader,
                                arrayOf(PreparedStatement::class.java)
                            ) { _, psMethod, _ ->
                                when (psMethod.name) {
                                    "setQueryTimeout", "close", "cancel" -> null
                                    "executeQuery" -> {
                                        val served = AtomicBoolean(false)
                                        Proxy.newProxyInstance(
                                            ResultSet::class.java.classLoader,
                                            arrayOf(ResultSet::class.java)
                                        ) { _, rsMethod, _ ->
                                            when (rsMethod.name) {
                                                "next" -> served.compareAndSet(false, true)
                                                "getInt" -> backendPid
                                                "close" -> null
                                                else -> null
                                            }
                                        } as ResultSet
                                    }
                                    else -> null
                                }
                            } as PreparedStatement
                        } else {
                            ddlOrCheckSqlExecutedCount.incrementAndGet()
                            throw AssertionError("No Stage 2 SQL should be prepared when shared total wall-time budget is exhausted after readiness")
                        }
                    }
                    "createStatement" -> {
                        ddlOrCheckSqlExecutedCount.incrementAndGet()
                        throw AssertionError("No Stage 2 Statement should be created when shared total wall-time budget is exhausted after readiness")
                    }
                    "isWrapperFor" -> false
                    "getAutoCommit" -> true
                    "isReadOnly" -> readOnlyDefault
                    "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                    "equals" -> proxy === args?.firstOrNull()
                    "hashCode" -> System.identityHashCode(proxy)
                    else -> null
                }
            } as Connection
        }

        val connCalls = AtomicInteger(0)
        val ds = object : DataSource {
            override fun getConnection(): Connection {
                val c = connCalls.incrementAndGet()
                // Advance monotonic clock by 1,500ms on each of the 2 readiness connections (total 3,000ms),
                // exhausting maxTotalWallTimeMs = 3,000ms right as readiness finishes!
                fakeNowMs += 1_500L
                return if (c == 1) {
                    createReadinessOnlyConn(backendPid = 2001, readOnlyDefault = false)
                } else {
                    createReadinessOnlyConn(backendPid = 2002, readOnlyDefault = true)
                }
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
        assertTrue(res.report.connectionsClosedCleanly)
        assertEquals(0, ddlOrCheckSqlExecutedCount.get(), "Expected 0 Stage 2 SQL statements when shared total wall-time budget is exhausted")
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

        fun createMockConnection(isVerifyConn: Boolean, backendPid: Int): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        val isDdl = sql.uppercase().contains("CREATE TABLE")
                        val isPidQuery = sql.contains("pg_backend_pid")
                        val isSearchPath = sql.uppercase().contains("SET SEARCH_PATH")
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
                                    if (isDdl) {
                                        ddlExecuteUpdateCalls.incrementAndGet()
                                    }
                                    if (isSearchPath) 0 else 1
                                }
                                "executeQuery" -> {
                                    val rowServed = AtomicBoolean(false)
                                    Proxy.newProxyInstance(
                                        ResultSet::class.java.classLoader,
                                        arrayOf(ResultSet::class.java)
                                    ) { _, rsMethod, rsArgs ->
                                        when (rsMethod.name) {
                                            "next" -> rowServed.compareAndSet(false, true)
                                            "getInt" -> if (isPidQuery) backendPid else 0
                                            "getString" -> "public"
                                            "getObject", "getBoolean" -> {
                                                when ((rsArgs?.firstOrNull() as? String).orEmpty()) {
                                                    "table_present" -> false
                                                    "can_create" -> true
                                                    "table_shape_valid" -> true
                                                    else -> false
                                                }
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
                return createMockConnection(isVerifyConn = idx > 1, backendPid = 3000 + idx)
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
        val explicitDbSecretMarker = "NON_CREDENTIAL_EXPLICIT_DB_MARKER_TEST_ONLY"
        val explicitApiSecretMarker = "NON_CREDENTIAL_EXPLICIT_API_MARKER_TEST_ONLY"
        val formatPrefixMarker = "RGAPI-NON_CREDENTIAL_FORMAT_MARKER_TEST_ONLY"
        val formatKeyValueMarker = "NON_CREDENTIAL_KV_FORMAT_MARKER_TEST_ONLY"

        fun createConn(isVerifyConn: Boolean, backendPid: Int): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        val isDdl = sql.uppercase().contains("CREATE TABLE")
                        val isPidQuery = sql.contains("pg_backend_pid")
                        Proxy.newProxyInstance(
                            PreparedStatement::class.java.classLoader,
                            arrayOf(PreparedStatement::class.java)
                        ) { _, psMethod, _ ->
                            when (psMethod.name) {
                                "setQueryTimeout", "close", "cancel" -> null
                                "executeUpdate" -> {
                                    if (isDdl) {
                                        throw SQLException(
                                            "ERROR: canceling statement due to lock timeout on jdbc:postgresql://db.internal:5432/prod?password=$explicitDbSecretMarker " +
                                                "explicit=$explicitApiSecretMarker prefix=$formatPrefixMarker api_key=$formatKeyValueMarker",
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
                                            "getInt" -> if (isPidQuery) backendPid else 0
                                            "getString" -> "public"
                                            "getObject", "getBoolean" -> {
                                                when ((rsArgs?.firstOrNull() as? String).orEmpty()) {
                                                    "table_present" -> false
                                                    "can_create" -> true
                                                    "table_shape_valid" -> true
                                                    else -> false
                                                }
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
            override fun getConnection(): Connection {
                val idx = count.incrementAndGet()
                return createConn(isVerifyConn = idx > 1, backendPid = 4000 + idx)
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
                    "OFFPROD_DB_PASS" to explicitDbSecretMarker,
                    "OFFPROD_RIOT_API_KEY" to explicitApiSecretMarker
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
            assertFalse(res.report.sanitizedDetail.contains(explicitDbSecretMarker))
            assertFalse(res.report.sanitizedDetail.contains(explicitApiSecretMarker))
            assertFalse(res.report.sanitizedDetail.contains(formatPrefixMarker))
            assertFalse(res.report.sanitizedDetail.contains(formatKeyValueMarker))
            assertFalse(res.report.sanitizedDetail.contains("db.internal"))

            val rawJson = reportFile.readText()
            assertFalse(rawJson.contains(explicitDbSecretMarker))
            assertFalse(rawJson.contains(explicitApiSecretMarker))
            assertFalse(rawJson.contains(formatPrefixMarker))
            assertFalse(rawJson.contains(formatKeyValueMarker))
            assertFalse(rawJson.contains("db.internal"))

            // Separate direct format-matching redaction check with empty explicit secret list:
            val formatOnlySanitized = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(
                raw = "jdbc:postgresql://db.internal:5432/app?password=NON_CREDENTIAL_URL_MARKER prefix=$formatPrefixMarker api_key=$formatKeyValueMarker",
                secretsToRedact = emptyList()
            )
            assertFalse(formatOnlySanitized.contains("NON_CREDENTIAL_URL_MARKER"))
            assertFalse(formatOnlySanitized.contains(formatPrefixMarker))
            assertFalse(formatOnlySanitized.contains(formatKeyValueMarker))
            assertTrue(formatOnlySanitized.contains("RGAPI-[REDACTED]"))
            assertTrue(formatOnlySanitized.contains("api_key=[REDACTED]"))
        } finally {
            reportFile.delete()
        }
    }

    @Test
    fun `unconditional physical PID separation rejects same Connection instance and distinct proxies wrapping same physical backend PID before DDL`() {
        val ddlStatementsExecuted = AtomicInteger(0)

        fun createProxyConnWithPid(backendPid: Int): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        if (sql.uppercase().contains("CREATE TABLE")) {
                            ddlStatementsExecuted.incrementAndGet()
                            throw AssertionError("DDL must never be prepared when primaryConn and verifyConn share the same physical PID")
                        }
                        Proxy.newProxyInstance(
                            PreparedStatement::class.java.classLoader,
                            arrayOf(PreparedStatement::class.java)
                        ) { _, psMethod, _ ->
                            when (psMethod.name) {
                                "setQueryTimeout", "close", "cancel" -> null
                                "executeQuery" -> {
                                    val served = AtomicBoolean(false)
                                    Proxy.newProxyInstance(
                                        ResultSet::class.java.classLoader,
                                        arrayOf(ResultSet::class.java)
                                    ) { _, rsMethod, _ ->
                                        when (rsMethod.name) {
                                            "next" -> served.compareAndSet(false, true)
                                            "getInt" -> backendPid
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
                    "getAutoCommit" -> true // autoCommit=true: previously made !c1AutoCommit check dead!
                    "isReadOnly" -> false
                    "setReadOnly", "setAutoCommit", "setNetworkTimeout", "close", "abort" -> null
                    "equals" -> proxy === args?.firstOrNull()
                    "hashCode" -> System.identityHashCode(proxy)
                    else -> null
                }
            } as Connection
        }

        // Case 1: DataSource returns the exact same Connection instance with autoCommit = true
        val singleConn = createProxyConnWithPid(backendPid = 7777)
        val sameInstanceDs = object : DataSource {
            override fun getConnection(): Connection = singleConn
            override fun getConnection(username: String?, password: String?): Connection = singleConn
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        val sameInstanceRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
            env = emptyMap(),
            dataSourceOverride = sameInstanceDs,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(3, sameInstanceRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL,
            sameInstanceRes.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            sameInstanceRes.report.ddlCommitStatus
        )
        assertEquals(0, ddlStatementsExecuted.get())

        // Case 2: DataSource returns two DISTINCT Connection proxy objects (rawC2 !== c1) that share the same physical backend PID (7777)
        val distinctProxiesSamePidDs = object : DataSource {
            override fun getConnection(): Connection = createProxyConnWithPid(backendPid = 7777)
            override fun getConnection(username: String?, password: String?): Connection = getConnection()
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        val samePidRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
            env = emptyMap(),
            dataSourceOverride = distinctProxiesSamePidDs,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(3, samePidRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL,
            samePidRes.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            samePidRes.report.ddlCommitStatus
        )
        assertEquals(7777, samePidRes.report.primaryBackendPid)
        assertEquals(7777, samePidRes.report.verifyBackendPid)
        assertEquals(0, ddlStatementsExecuted.get())

        // Case 3: Real PostgreSQL connection wrapped in two distinct Connection proxies delegating to the SAME physical PGConnection
        pgDataSource.connection.use { sharedRealPgConn ->
            val realSamePhysicalDs = object : DataSource by pgDataSource {
                override fun getConnection(): Connection {
                    return Proxy.newProxyInstance(
                        Connection::class.java.classLoader,
                        arrayOf(Connection::class.java)
                    ) { proxy, method, args ->
                        when (method.name) {
                            "close", "abort" -> null // keep sharedRealPgConn open for outer .use
                            "equals" -> proxy === args?.firstOrNull()
                            "hashCode" -> System.identityHashCode(proxy)
                            else -> if (args == null) {
                                method.invoke(sharedRealPgConn)
                            } else {
                                method.invoke(sharedRealPgConn, *args)
                            }
                        }
                    } as Connection
                }
            }

            val realSamePidRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = emptyMap(),
                dataSourceOverride = realSamePhysicalDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, realSamePidRes.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL,
                realSamePidRes.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                realSamePidRes.report.ddlCommitStatus
            )
            assertNotNull(realSamePidRes.report.primaryBackendPid)
            assertEquals(realSamePidRes.report.primaryBackendPid, realSamePidRes.report.verifyBackendPid)
        }
    }

    @Test
    fun `bounded awaited cleanup waits for delayed close before return and fails closed when close hangs past cleanupTimeoutMs`() {
        // Part 1: Delayed close (120ms) within cleanupTimeoutMs (1,000ms) is awaited before runSetupCli returns
        val closeCompletedBeforeReturn = AtomicBoolean(false)
        val delayedCloseConn = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { proxy, method, args ->
            when (method.name) {
                "prepareStatement" -> {
                    val sql = (args?.firstOrNull() as? String).orEmpty()
                    val isPidQuery = sql.contains("pg_backend_pid")
                    val isSearchPath = sql.uppercase().contains("SET SEARCH_PATH")
                    Proxy.newProxyInstance(
                        PreparedStatement::class.java.classLoader,
                        arrayOf(PreparedStatement::class.java)
                    ) { _, psMethod, _ ->
                        when (psMethod.name) {
                            "setQueryTimeout", "close", "cancel" -> null
                            "executeUpdate" -> if (isSearchPath) 0 else 1
                            "executeQuery" -> {
                                val served = AtomicBoolean(false)
                                Proxy.newProxyInstance(
                                    ResultSet::class.java.classLoader,
                                    arrayOf(ResultSet::class.java)
                                ) { _, rsMethod, rsArgs ->
                                    when (rsMethod.name) {
                                        "next" -> served.compareAndSet(false, true)
                                        "getInt" -> if (isPidQuery) 5001 else 0
                                        "getString" -> "public"
                                        "getObject", "getBoolean" -> {
                                            when ((rsArgs?.firstOrNull() as? String).orEmpty()) {
                                                "table_present" -> false
                                                "can_create" -> true
                                                else -> false
                                            }
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
                "close" -> {
                    Thread.sleep(120L)
                    closeCompletedBeforeReturn.set(true)
                    null
                }
                "isWrapperFor" -> false
                "getAutoCommit" -> true
                "isReadOnly" -> true
                "setReadOnly", "setAutoCommit", "setNetworkTimeout", "abort" -> null
                "equals" -> proxy === args?.firstOrNull()
                "hashCode" -> System.identityHashCode(proxy)
                else -> null
            }
        } as Connection

        val delayedCloseDs = object : DataSource {
            override fun getConnection(): Connection = delayedCloseConn
            override fun getConnection(username: String?, password: String?): Connection = delayedCloseConn
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        val okCleanupRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--dry-run=true", "--cleanup-timeout-ms=1000"),
            env = emptyMap(),
            dataSourceOverride = delayedCloseDs,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertTrue(
            closeCompletedBeforeReturn.get(),
            "Expected runSetupCli to await connection.close() completion before returning"
        )
        assertEquals(0, okCleanupRes.exitCode)
        assertTrue(okCleanupRes.report.connectionsClosedCleanly)
        assertFalse(okCleanupRes.report.cleanupTimedOut)
        assertTrue(okCleanupRes.report.cleanupElapsedMs >= 100L)

        // Part 2: Hung close exceeding cleanupTimeoutMs (80ms) fails closed with exitCode=3 and cleanupTimedOut=true
        val releaseHungClose = CountDownLatch(1)
        val fallbackAbortCalled = CountDownLatch(1)
        val hungCloseConn = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { proxy, method, args ->
            when (method.name) {
                "prepareStatement" -> delayedCloseConn.prepareStatement((args?.firstOrNull() as? String).orEmpty())
                "close" -> {
                    var interrupted = false
                    while (true) {
                        try {
                            if (releaseHungClose.await(5, TimeUnit.SECONDS)) break
                            break
                        } catch (_: InterruptedException) {
                            interrupted = true
                        }
                    }
                    if (interrupted) Thread.currentThread().interrupt()
                    null
                }
                "abort" -> {
                    fallbackAbortCalled.countDown()
                    null
                }
                "isWrapperFor" -> false
                "getAutoCommit" -> true
                "isReadOnly" -> true
                "setReadOnly", "setAutoCommit", "setNetworkTimeout" -> null
                "equals" -> proxy === args?.firstOrNull()
                "hashCode" -> System.identityHashCode(proxy)
                else -> null
            }
        } as Connection

        val hungCloseDs = object : DataSource {
            override fun getConnection(): Connection = hungCloseConn
            override fun getConnection(username: String?, password: String?): Connection = hungCloseConn
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getGlobal()
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        try {
            val hungCleanupRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--dry-run=true", "--cleanup-timeout-ms=80"),
                env = emptyMap(),
                dataSourceOverride = hungCloseDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, hungCleanupRes.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_CLEANUP_FAILED_OR_TIMED_OUT,
                hungCleanupRes.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupStage.CLEANUP,
                hungCleanupRes.report.stage
            )
            assertFalse(hungCleanupRes.report.connectionsClosedCleanly)
            assertTrue(hungCleanupRes.report.cleanupTimedOut)
            assertTrue(fallbackAbortCalled.await(1, TimeUnit.SECONDS), "Expected fallback abort() on hung close()")
        } finally {
            releaseHungClose.countDown()
        }
    }

    @Test
    fun `real PostgreSQL 15 search_path shadow protection and fail-closed rejection of incompatible existing table shapes`() {
        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP SCHEMA IF EXISTS shadow_schema CASCADE")
                st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
                st.execute("CREATE SCHEMA shadow_schema")
                // Create a conflicting/incompatible table in shadow_schema
                st.execute("CREATE TABLE shadow_schema.player_riot_identities (wrong_col INT)")
            }
        }

        // 1. Even when DataSource connections start with search_path = shadow_schema, public,
        // OffProdAdditiveIdentitySchemaSetupCli binds SET search_path = public, pg_catalog before check/DDL/verify
        // and creates/verifies public.player_riot_identities (not shadow_schema)!
        val shadowedSearchPathDs = PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = "postgres"
            password = ""
            this.options = "-c search_path=shadow_schema,public"
        }

        val shadowRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
            env = emptyMap(),
            dataSourceOverride = shadowedSearchPathDs,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(0, shadowRes.exitCode, "Expected exitCode=0: ${shadowRes.report.sanitizedDetail}")
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.APPLIED_AND_VERIFIED_DDL,
            shadowRes.report.outcome
        )
        assertEquals(true, shadowRes.report.searchPathBoundToPublic)
        assertEquals(false, shadowRes.report.tablePresentBefore)
        assertEquals(true, shadowRes.report.tablePresentAfter)
        assertEquals(true, shadowRes.report.tableShapeValid)
        assertNotNull(shadowRes.report.primaryBackendPid)
        assertNotNull(shadowRes.report.verifyBackendPid)
        assertNotEquals(shadowRes.report.primaryBackendPid, shadowRes.report.verifyBackendPid)

        // 2. Incompatible existing public.player_riot_identities shapes must fail closed (both dry-run and apply-ddl)!
        val incompatibleTableDdls = listOf(
            // Wrong VARCHAR length on puuid (64 instead of 78)
            """
            CREATE TABLE public.player_riot_identities (
                puuid           VARCHAR(64)  PRIMARY KEY,
                platform_region VARCHAR(16)  NOT NULL,
                game_name       VARCHAR(255) NOT NULL,
                tag_line        VARCHAR(255) NOT NULL,
                verified_at     TIMESTAMPTZ  NOT NULL,
                CONSTRAINT chk_player_riot_identities_nonblank
                    CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
            )
            """.trimIndent(),
            // Missing PRIMARY KEY on puuid
            """
            CREATE TABLE public.player_riot_identities (
                puuid           VARCHAR(78)  NOT NULL,
                platform_region VARCHAR(16)  NOT NULL,
                game_name       VARCHAR(255) NOT NULL,
                tag_line        VARCHAR(255) NOT NULL,
                verified_at     TIMESTAMPTZ  NOT NULL,
                CONSTRAINT chk_player_riot_identities_nonblank
                    CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
            )
            """.trimIndent(),
            // Missing chk_player_riot_identities_nonblank CHECK constraint
            """
            CREATE TABLE public.player_riot_identities (
                puuid           VARCHAR(78)  PRIMARY KEY,
                platform_region VARCHAR(16)  NOT NULL,
                game_name       VARCHAR(255) NOT NULL,
                tag_line        VARCHAR(255) NOT NULL,
                verified_at     TIMESTAMPTZ  NOT NULL
            )
            """.trimIndent(),
            // Same-name chk_player_riot_identities_nonblank with permissive OR true (would pass substring LIKE check!)
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
            // Same-name chk_player_riot_identities_nonblank added as NOT VALID (convalidated = false)
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

        for (badDdl in incompatibleTableDdls) {
            pgDataSource.connection.use { conn ->
                conn.createStatement().use { st ->
                    st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
                    st.execute(badDdl)
                }
            }

            val dryRunBad = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--dry-run=true"),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, dryRunBad.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA,
                dryRunBad.report.outcome
            )
            assertEquals(true, dryRunBad.report.tablePresentBefore)
            assertEquals(false, dryRunBad.report.tableShapeValid)

            val applyBad = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = mapOf("OFFPROD_JDBC_URL" to jdbcUrl, "OFFPROD_DB_USER" to "postgres"),
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, applyBad.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA,
                applyBad.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                applyBad.report.ddlCommitStatus
            )
            assertEquals(true, applyBad.report.tablePresentBefore)
            assertEquals(false, applyBad.report.tableShapeValid)
        }

        pgDataSource.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP SCHEMA IF EXISTS shadow_schema CASCADE")
                st.execute("DROP TABLE IF EXISTS public.player_riot_identities")
            }
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
        assertTrue(dryRunRes.report.connectionsClosedCleanly)
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
            assertTrue(lockRes.report.connectionsClosedCleanly)

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
        assertEquals(true, slowWanRes.report.tableShapeValid)
        assertNotNull(slowWanRes.report.primaryBackendPid)
        assertNotNull(slowWanRes.report.verifyBackendPid)
        assertNotEquals(slowWanRes.report.primaryBackendPid, slowWanRes.report.verifyBackendPid)
        assertTrue(slowWanRes.report.connectionsClosedCleanly)
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
            assertEquals(true, parsedReport.tableShapeValid)
            assertNotNull(parsedReport.primaryBackendPid)
            assertNotNull(parsedReport.verifyBackendPid)
            assertNotEquals(parsedReport.primaryBackendPid, parsedReport.verifyBackendPid)
            assertTrue(parsedReport.connectionsClosedCleanly)
            assertTrue(
                parsedReport.jvmMaxHeapBytes <= OffProdAdditiveIdentitySchemaSetupCli.MAX_STANDALONE_JVM_HEAP_BYTES,
                "Expected jvmMaxHeapBytes <= 256 MiB (was ${parsedReport.jvmMaxHeapBytes})"
            )
            assertTrue(
                parsedReport.jvmUsedHeapBytesPeak <= 128L * 1024L * 1024L,
                "Expected peak used heap <= 128 MiB (was ${parsedReport.jvmUsedHeapBytesPeak})"
            )

            // Idempotent re-run when valid table is already present:
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
            assertEquals(true, idempotentRes.report.tableShapeValid)
            assertTrue(idempotentRes.report.connectionsClosedCleanly)
        } finally {
            subprocessReportFile.delete()
        }
    }

    @Test
    fun `negative tests - fail closed on missing or invalid PID, schema, or table shape evidence and enforce total budget ceiling across awaited cleanup`() {
        val ddlStatementsPrepared = AtomicInteger(0)

        fun createConfigurableConn(
            pidPsNull: Boolean = false,
            pidRsNull: Boolean = false,
            pidZeroRows: Boolean = false,
            pidValue: Int = 5501,
            searchPathPsNull: Boolean = false,
            contractPsNull: Boolean = false,
            contractRsNull: Boolean = false,
            contractZeroRows: Boolean = false,
            currentSchemaName: String? = "public",
            tablePresent: Boolean? = true,
            canCreate: Boolean? = true,
            tableShapeValid: Boolean? = true,
            allowDdlExecution: Boolean = false,
            postVerifyPsNull: Boolean = false,
            postVerifyRsNull: Boolean = false,
            postVerifyZeroRows: Boolean = false,
            postVerifySchemaName: String? = "public",
            postVerifyTablePresent: Boolean? = true,
            postVerifyTableShapeValid: Boolean? = true,
            onClose: () -> Unit = {}
        ): Connection {
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { proxy, method, args ->
                when (method.name) {
                    "prepareStatement" -> {
                        val sql = (args?.firstOrNull() as? String).orEmpty()
                        when {
                            sql.uppercase().contains("CREATE TABLE") -> {
                                ddlStatementsPrepared.incrementAndGet()
                                if (!allowDdlExecution) {
                                    throw AssertionError("DDL must never be prepared when pre-DDL PID, schema, or table shape evidence is missing/invalid")
                                }
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
                            sql.contains("pg_backend_pid") -> {
                                if (pidPsNull) return@newProxyInstance null
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
                            else -> {
                                val isPreDdlCheck = sql.contains("can_create")
                                if (isPreDdlCheck && contractPsNull) return@newProxyInstance null
                                if (!isPreDdlCheck && postVerifyPsNull) return@newProxyInstance null
                                Proxy.newProxyInstance(
                                    PreparedStatement::class.java.classLoader,
                                    arrayOf(PreparedStatement::class.java)
                                ) { _, psMethod, _ ->
                                    when (psMethod.name) {
                                        "setQueryTimeout", "close", "cancel" -> null
                                        "executeQuery" -> {
                                            if (isPreDdlCheck && contractRsNull) return@newProxyInstance null
                                            if (!isPreDdlCheck && postVerifyRsNull) return@newProxyInstance null
                                            val served = AtomicBoolean(false)
                                            Proxy.newProxyInstance(
                                                ResultSet::class.java.classLoader,
                                                arrayOf(ResultSet::class.java)
                                            ) { _, rsMethod, rsArgs ->
                                                when (rsMethod.name) {
                                                    "next" -> {
                                                        val zeroRows = if (isPreDdlCheck) contractZeroRows else postVerifyZeroRows
                                                        if (zeroRows) false else served.compareAndSet(false, true)
                                                    }
                                                    "getString" -> if (isPreDdlCheck) currentSchemaName else postVerifySchemaName
                                                    "getObject" -> when ((rsArgs?.firstOrNull() as? String).orEmpty()) {
                                                        "table_present" -> if (isPreDdlCheck) tablePresent else postVerifyTablePresent
                                                        "can_create" -> canCreate
                                                        "table_shape_valid" -> if (isPreDdlCheck) tableShapeValid else postVerifyTableShapeValid
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
                        }
                    }
                    "close" -> {
                        onClose()
                        null
                    }
                    "isWrapperFor" -> false
                    "getAutoCommit" -> true
                    "isReadOnly" -> false
                    "setReadOnly", "setAutoCommit", "setNetworkTimeout", "abort" -> null
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

        // 1. Missing/invalid PID evidence cases must NEVER fabricate 1001/1002 and must fail closed during readiness:
        val invalidPidCases = listOf(
            dsFromFactory { idx -> createConfigurableConn(pidPsNull = true, pidValue = 5000 + idx) },
            dsFromFactory { idx -> createConfigurableConn(pidRsNull = true, pidValue = 5000 + idx) },
            dsFromFactory { idx -> createConfigurableConn(pidZeroRows = true, pidValue = 5000 + idx) },
            dsFromFactory { _ -> createConfigurableConn(pidValue = 0) },
            dsFromFactory { _ -> createConfigurableConn(pidValue = -42) }
        )

        for (badPidDs in invalidPidCases) {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = emptyMap(),
                dataSourceOverride = badPidDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, res.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL,
                res.report.outcome
            )
            assertNull(res.report.primaryBackendPid, "Must not fabricate fallback PID 1001 on missing/invalid PID evidence")
            assertNull(res.report.verifyBackendPid, "Must not fabricate fallback PID 1002 on missing/invalid PID evidence")
        }

        // 2. Pre-DDL missing/invalid schema or table shape evidence cases must NEVER default to 'public' or true:
        val invalidContractCases = listOf(
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, searchPathPsNull = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, contractPsNull = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, contractRsNull = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, contractZeroRows = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, currentSchemaName = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, currentSchemaName = "   ") },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, currentSchemaName = "shadow_schema") },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, tablePresent = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, canCreate = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6000 + idx, tablePresent = true, tableShapeValid = null) }
        )

        for (badContractDs in invalidContractCases) {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = emptyMap(),
                dataSourceOverride = badContractDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, res.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                res.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                res.report.ddlCommitStatus
            )
            assertFalse(res.report.ddlExecutedAndVerified)
        }
        assertEquals(0, ddlStatementsPrepared.get(), "Expected 0 DDL statements prepared across all pre-DDL missing/invalid evidence cases")

        // 2b. Post-DDL verification missing/invalid evidence cases must also fail closed (ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT, ddlExecutedAndVerified=false):
        val invalidPostVerifyCases = listOf(
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyPsNull = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyRsNull = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyZeroRows = true) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifySchemaName = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifySchemaName = "   ") },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifySchemaName = "shadow_schema") },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyTablePresent = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyTablePresent = false) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyTablePresent = true, postVerifyTableShapeValid = null) },
            dsFromFactory { idx -> createConfigurableConn(pidValue = 6500 + idx, tablePresent = false, canCreate = true, allowDdlExecution = true, postVerifyTablePresent = true, postVerifyTableShapeValid = false) }
        )

        for (badPostVerifyDs in invalidPostVerifyCases) {
            val res = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
                args = listOf("--apply-ddl=true", "--dry-run=false", "--allow-ddl-writes=true"),
                env = emptyMap(),
                dataSourceOverride = badPostVerifyDs,
                maxHeapBytesProvider = { 128L * 1024L * 1024L }
            )
            assertEquals(3, res.exitCode)
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                res.report.outcome
            )
            assertEquals(
                OffProdAdditiveIdentitySchemaSetupCli.DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                res.report.ddlCommitStatus
            )
            assertFalse(res.report.ddlExecutedAndVerified)
        }

        // 3. Total wall-time budget includes awaited cleanup: when awaited cleanup pushes totalElapsed > maxTotalWallTimeMs,
        // runSetupCli fails closed with exitCode=10 and ABORTED_TOTAL_BUDGET_EXHAUSTED:
        var fakeClockMs = 10_000L
        val fakeClock = object : MonotonicClock {
            override fun nowMs(): Long = fakeClockMs
            override fun sleepMs(ms: Long) {
                fakeClockMs += ms
            }
        }
        val cleanupOverBudgetDs = dsFromFactory { idx ->
            createConfigurableConn(
                pidValue = 7000 + idx,
                tablePresent = false,
                canCreate = true,
                tableShapeValid = null,
                onClose = {
                    // Advance fake clock during awaited close() so total elapsed exceeds maxTotalWallTimeMs (3,000ms)
                    fakeClockMs += 3_200L
                }
            )
        }

        val overBudgetCleanupRes = OffProdAdditiveIdentitySchemaSetupCli.runSetupCli(
            args = listOf(
                "--dry-run=true",
                "--readiness-timeout-ms=2000",
                "--sql-timeout-ms=1500",
                "--lock-timeout-ms=1000",
                "--cleanup-timeout-ms=1000",
                "--max-total-wall-time-ms=3000"
            ),
            env = emptyMap(),
            dataSourceOverride = cleanupOverBudgetDs,
            monotonicClock = fakeClock,
            maxHeapBytesProvider = { 128L * 1024L * 1024L }
        )
        assertEquals(10, overBudgetCleanupRes.exitCode)
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED,
            overBudgetCleanupRes.report.outcome
        )
        assertEquals(
            OffProdAdditiveIdentitySchemaSetupCli.SetupStage.CLEANUP,
            overBudgetCleanupRes.report.stage
        )
        assertTrue(overBudgetCleanupRes.report.totalWallElapsedMs > overBudgetCleanupRes.report.totalWallBudgetMs)
    }
}
