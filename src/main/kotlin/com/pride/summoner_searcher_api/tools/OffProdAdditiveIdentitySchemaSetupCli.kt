package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.postgresql.PGConnection
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.PreparedStatement
import java.time.Instant
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import kotlin.math.ceil
import kotlin.system.exitProcess

/**
 * Isolated off-production CLI wrapper that stages ONLY the reviewed additive migration
 * (`/db/migration/V20261109__create_player_riot_identities.sql`) with:
 *
 * 1. **Pre-Validation First (`0` Connections / `0` SQL on Invalid Options)**:
 *    - Validates all CLI flags, numeric/boolean types, timeout bounds (`readinessTimeoutMs in 1..10_000`,
 *      `sqlTimeoutMs in 1..2_500`, `lockTimeoutMs in 1..2_000` with `lockTimeoutMs <= sqlTimeoutMs`,
 *      `maxTotalWallTimeMs in 1..15_000` with `maxTotalWallTimeMs >= sqlTimeoutMs`), mode safety
 *      (`--apply-ddl=true` requires `--dry-run=false` and `--allow-ddl-writes=true`), standalone
 *      JVM heap ceiling (`<= 256 MiB` when enforced), and PostgreSQL JDBC target **before** constructing
 *      any [DataSource] or calling `getConnection()`.
 * 2. **Default Dry-Run Zero DDL (`--dry-run=true --apply-ddl=false`)**:
 *    - Opens a single read-only connection (`conn.isReadOnly = true`) during Readiness (`<= 10s`) and
 *      executes only a read-only schema/privilege check within the `<= 2,500ms` SQL fence (`0` DDL statements).
 * 3. **Bounded Connection Readiness (`<= 10,000ms`) Separated from Unchanged `<= 2,500ms` SQL/Lock Fence,
 *    Sharing `<= 15,000ms` Total Process Budget**:
 *    - Stage 1 (`CONNECTION_READINESS`, `<= 10,000ms`) pre-warms the JDBC connection(s) (in `--apply-ddl=true`
 *      mode: both `ddlConn` and a separate `verifyConn !== ddlConn` for post-commit verification) so that
 *      WAN TCP + TLS + SCRAM-SHA-256 authentication happens before the `2,500ms` SQL/lock clock starts.
 *    - If Stage 1 times out or is cancelled, any late-acquired connection is closed/aborted immediately
 *      without preparing or executing any SQL (`0` SQL statements executed, `NOT_STARTED_ZERO_SQL`).
 *    - Stage 2 (`SQL_AND_VERIFICATION`, `<= 2,500ms` and `<= remainingTotalWallTimeMs`) enforces a fresh
 *      `2,500ms` SQL/lock deadline (`lock_timeout <= 2,000ms`, `statement_timeout <= 2,500ms`) with strict
 *      cancellation/deadline fences before `prepareStatement`, before/after `setQueryTimeout`, and
 *      **DIRECTLY BEFORE** every `executeQuery()`, `executeUpdate()`, and `commit()` call.
 * 4. **Conservative `unknown_commit` Reporting & Secret Redaction**:
 *    - If a deadline or lock timeout occurs **before** `ddlPs.executeUpdate()` is invoked, reports
 *      `DdlCommitStatus.NOT_STARTED_ZERO_SQL`.
 *    - If a timeout, lock error, or verification failure occurs **after** `ddlPs.executeUpdate()` is
 *      invoked (or before separate-connection post-commit verification `SELECT to_regclass(...)` completes),
 *      reports `DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED` so the operator can recheck with
 *      `--dry-run=true`.
 *    - Scrubs passwords, URL credentials, and tokens from all error messages and JSON reports.
 */
object OffProdAdditiveIdentitySchemaSetupCli {

    const val MAX_READINESS_TIMEOUT_MS: Long = 10_000L
    const val DEFAULT_READINESS_TIMEOUT_MS: Long = 10_000L

    const val MAX_SQL_TIMEOUT_MS: Long = 2_500L
    const val DEFAULT_SQL_TIMEOUT_MS: Long = 2_500L

    const val MAX_LOCK_TIMEOUT_MS: Long = 2_000L
    const val DEFAULT_LOCK_TIMEOUT_MS: Long = 2_000L

    const val MAX_TOTAL_WALL_TIME_MS: Long = 15_000L
    const val DEFAULT_TOTAL_WALL_TIME_MS: Long = 15_000L

    const val MAX_STANDALONE_JVM_HEAP_BYTES: Long = 256L * 1024L * 1024L

    val ALLOWED_FLAGS: Set<String> = setOf(
        "dry-run",
        "apply-ddl",
        "allow-ddl-writes",
        "readiness-timeout-ms",
        "sql-timeout-ms",
        "lock-timeout-ms",
        "max-total-wall-time-ms",
        "enforce-max-heap-256m",
        "jdbc-url",
        "db-host",
        "db-port",
        "db-name",
        "db-user",
        "db-pass",
        "output-report-file"
    )

    const val SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE: String = """
        SELECT to_regclass('public.player_riot_identities') IS NOT NULL AS table_present,
               has_schema_privilege(current_user, 'public', 'CREATE') AS can_create
    """

    const val SQL_VERIFY_TABLE_PRESENT: String = """
        SELECT to_regclass('public.player_riot_identities') IS NOT NULL AS table_present
    """

    enum class SetupStage {
        OPTION_VALIDATION,
        CONNECTION_READINESS,
        SQL_AND_VERIFICATION,
        COMPLETED
    }

    enum class SetupOutcome {
        DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
        APPLIED_AND_VERIFIED_DDL,
        ABORTED_INVALID_OPTIONS,
        ABORTED_READINESS_FAILURE_NO_SQL,
        ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
        ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT,
        ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
        ABORTED_TOTAL_BUDGET_EXHAUSTED
    }

    enum class DdlCommitStatus {
        NOT_STARTED_ZERO_SQL,
        COMMITTED_AND_VERIFIED,
        UNKNOWN_COMMIT_RECHECK_REQUIRED
    }

    data class SetupOptions(
        val dryRun: Boolean = true,
        val applyDdl: Boolean = false,
        val allowDdlWrites: Boolean = false,
        val readinessTimeoutMs: Long = DEFAULT_READINESS_TIMEOUT_MS,
        val sqlTimeoutMs: Long = DEFAULT_SQL_TIMEOUT_MS,
        val lockTimeoutMs: Long = DEFAULT_LOCK_TIMEOUT_MS,
        val maxTotalWallTimeMs: Long = DEFAULT_TOTAL_WALL_TIME_MS,
        val enforceMaxHeap256m: Boolean = true
    ) {
        init {
            require(readinessTimeoutMs in 1L..MAX_READINESS_TIMEOUT_MS) {
                "readinessTimeoutMs ($readinessTimeoutMs) must be in 1..$MAX_READINESS_TIMEOUT_MS"
            }
            require(sqlTimeoutMs in 1L..MAX_SQL_TIMEOUT_MS) {
                "sqlTimeoutMs ($sqlTimeoutMs) must be in 1..$MAX_SQL_TIMEOUT_MS"
            }
            require(lockTimeoutMs in 1L..MAX_LOCK_TIMEOUT_MS) {
                "lockTimeoutMs ($lockTimeoutMs) must be in 1..$MAX_LOCK_TIMEOUT_MS"
            }
            require(lockTimeoutMs <= sqlTimeoutMs) {
                "lockTimeoutMs ($lockTimeoutMs) must be <= sqlTimeoutMs ($sqlTimeoutMs)"
            }
            require(maxTotalWallTimeMs in 1L..MAX_TOTAL_WALL_TIME_MS) {
                "maxTotalWallTimeMs ($maxTotalWallTimeMs) must be in 1..$MAX_TOTAL_WALL_TIME_MS"
            }
            require(maxTotalWallTimeMs >= sqlTimeoutMs) {
                "maxTotalWallTimeMs ($maxTotalWallTimeMs) must be >= sqlTimeoutMs ($sqlTimeoutMs)"
            }
            if (applyDdl) {
                require(!dryRun && allowDdlWrites) {
                    "--apply-ddl=true requires both --dry-run=false and --allow-ddl-writes=true"
                }
            }
            if (!dryRun) {
                require(applyDdl && allowDdlWrites) {
                    "--dry-run=false requires both --apply-ddl=true and --allow-ddl-writes=true"
                }
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class SetupExecutionReport(
        val outcome: SetupOutcome = SetupOutcome.DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
        val stage: SetupStage = SetupStage.OPTION_VALIDATION,
        val ddlCommitStatus: DdlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
        val dryRun: Boolean = true,
        val applyDdlRequested: Boolean = false,
        val ddlExecutedAndVerified: Boolean = false,
        val tablePresentBefore: Boolean? = null,
        val tablePresentAfter: Boolean? = null,
        val hasCreatePrivilege: Boolean? = null,
        val migrationResourcePath: String = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
        val readinessBudgetMs: Long = 0L,
        val readinessElapsedMs: Long = 0L,
        val sqlBudgetMs: Long = 0L,
        val sqlElapsedMs: Long = 0L,
        val lockTimeoutMs: Long = 0L,
        val totalWallBudgetMs: Long = 0L,
        val totalWallElapsedMs: Long = 0L,
        val jvmMaxHeapBytes: Long = 0L,
        val jvmUsedHeapBytesPeak: Long = 0L,
        val recheckCommandHint: String? = null,
        val sanitizedDetail: String = "",
        val completedAtUtc: Instant = Instant.EPOCH
    )

    data class SetupCliResult(
        val exitCode: Int,
        val report: SetupExecutionReport,
        val outputReportPath: String?
    )

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    @JvmStatic
    fun main(args: Array<String>) {
        val result = runSetupCli(args = args.toList())
        if (result.exitCode != 0) {
            System.err.println(
                "[OffProdAdditiveIdentitySchemaSetupCli] ERROR (exit=${result.exitCode}, stage=${result.report.stage}, " +
                    "outcome=${result.report.outcome}, commitStatus=${result.report.ddlCommitStatus}): ${result.report.sanitizedDetail}"
            )
        } else {
            println(
                "[OffProdAdditiveIdentitySchemaSetupCli] OK: outcome=${result.report.outcome}, " +
                    "commitStatus=${result.report.ddlCommitStatus}, tablePresentBefore=${result.report.tablePresentBefore}, " +
                    "tablePresentAfter=${result.report.tablePresentAfter}, readinessElapsedMs=${result.report.readinessElapsedMs}, " +
                    "sqlElapsedMs=${result.report.sqlElapsedMs}, totalElapsedMs=${result.report.totalWallElapsedMs}"
            )
        }
        exitProcess(result.exitCode)
    }

    fun loadAndValidateExactAdditiveMigrationDdl(): String {
        val rawSql = JdbcIdentityCompletionPersistenceAdapter.loadReviewedAdditiveMigrationSql()
        val executableSql = rawSql.lineSequence()
            .map { it.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("--") }
            .joinToString("\n")
            .trim()
            .removeSuffix(";")
            .trim()

        val normalizedUpper = executableSql.uppercase(Locale.ROOT).replace(Regex("\\s+"), " ")
        require(normalizedUpper.startsWith("CREATE TABLE IF NOT EXISTS PLAYER_RIOT_IDENTITIES (")) {
            "Reviewed migration resource must start with CREATE TABLE IF NOT EXISTS player_riot_identities"
        }
        require(!executableSql.contains(';')) {
            "Reviewed migration resource must contain exactly 1 SQL statement without embedded semicolons"
        }
        val forbiddenTokens = listOf("DROP ", "TRUNCATE ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "GRANT ", "REVOKE ")
        for (token in forbiddenTokens) {
            require(!normalizedUpper.contains(token)) {
                "Reviewed migration resource contains forbidden token '$token'"
            }
        }
        return executableSql
    }

    fun sanitizeErrorMessage(raw: String?, secretsToRedact: List<String> = emptyList()): String {
        if (raw.isNullOrBlank()) return "Unspecified error"
        var sanitized: String = raw
        for (secret in secretsToRedact) {
            val trimmed = secret.trim()
            if (trimmed.isNotEmpty()) {
                sanitized = sanitized.replace(trimmed, "[REDACTED]")
                val urlEncoded = runCatching { URLEncoder.encode(trimmed, StandardCharsets.UTF_8.name()) }.getOrNull()
                if (!urlEncoded.isNullOrBlank()) {
                    sanitized = sanitized.replace(urlEncoded, "[REDACTED]")
                }
            }
        }
        sanitized = sanitized.replace(Regex("(?i)(password|pass|pwd|secret|token|api[_-]?key)\\s*[:=]\\s*[^&\\s;,\"')]+"), "$1=[REDACTED]")
        sanitized = sanitized.replace(Regex("(?i)jdbc:postgresql://([^/\\s?]+)(\\?[^\\s\"')]*)?"), "jdbc:postgresql://[REDACTED_HOST]$2")
        sanitized = sanitized.replace(Regex("(?i)postgresql://[^@\\s]+@"), "postgresql://[REDACTED]@")
        sanitized = sanitized.replace(Regex("RGAPI-[A-Za-z0-9_-]+"), "RGAPI-[REDACTED]")
        return sanitized
    }

    fun runSetupCli(
        args: List<String>,
        env: Map<String, String> = System.getenv(),
        dataSourceOverride: DataSource? = null,
        monotonicClock: MonotonicClock = SystemMonotonicClock,
        wallClock: WallClock = SystemUtcWallClock,
        maxHeapBytesProvider: () -> Long = { Runtime.getRuntime().maxMemory() }
    ): SetupCliResult {
        val processStartMonoMs = monotonicClock.nowMs()
        val rt = Runtime.getRuntime()
        var peakUsedHeapBytes = (rt.totalMemory() - rt.freeMemory()).coerceAtLeast(0L)
        fun sampleHeap() {
            val used = (rt.totalMemory() - rt.freeMemory()).coerceAtLeast(0L)
            if (used > peakUsedHeapBytes) peakUsedHeapBytes = used
        }
        val maxHeapBytes = maxHeapBytesProvider()

        val candidateSecrets = ArrayList<String>()
        env["OFFPROD_DB_PASS"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }
        env["DB_PASS"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }
        env["OFFPROD_RIOT_API_KEY"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }
        env["RIOT_API_KEY"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }

        val flags = try {
            parseFlagsStrict(args, candidateSecrets)
        } catch (e: IllegalArgumentException) {
            return buildInvalidOptionsResult(
                errorMessage = sanitizeErrorMessage(e.message, candidateSecrets),
                outputReportFile = null,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes
            )
        }

        flags["db-pass"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }
        val outputReportFile = flags["output-report-file"]?.trim()?.takeIf { it.isNotEmpty() }?.let { File(it) }

        val options = try {
            val dryRun = parseBooleanFlag(flags, "dry-run", true)
            val applyDdl = parseBooleanFlag(flags, "apply-ddl", false)
            val allowDdlWrites = parseBooleanFlag(flags, "allow-ddl-writes", false)
            val readinessTimeoutMs = parseLongFlag(flags, "readiness-timeout-ms", DEFAULT_READINESS_TIMEOUT_MS)
            val sqlTimeoutMs = parseLongFlag(flags, "sql-timeout-ms", DEFAULT_SQL_TIMEOUT_MS)
            val lockTimeoutMs = parseLongFlag(flags, "lock-timeout-ms", DEFAULT_LOCK_TIMEOUT_MS)
            val maxTotalWallTimeMs = parseLongFlag(flags, "max-total-wall-time-ms", DEFAULT_TOTAL_WALL_TIME_MS)
            val enforceMaxHeap256m = parseBooleanFlag(flags, "enforce-max-heap-256m", true)

            if (enforceMaxHeap256m && maxHeapBytes > MAX_STANDALONE_JVM_HEAP_BYTES) {
                throw IllegalArgumentException(
                    "Standalone JVM max heap ($maxHeapBytes bytes) exceeds 256 MiB cap ($MAX_STANDALONE_JVM_HEAP_BYTES bytes); run with -Xmx256m or lower"
                )
            }

            SetupOptions(
                dryRun = dryRun,
                applyDdl = applyDdl,
                allowDdlWrites = allowDdlWrites,
                readinessTimeoutMs = readinessTimeoutMs,
                sqlTimeoutMs = sqlTimeoutMs,
                lockTimeoutMs = lockTimeoutMs,
                maxTotalWallTimeMs = maxTotalWallTimeMs,
                enforceMaxHeap256m = enforceMaxHeap256m
            )
        } catch (e: IllegalArgumentException) {
            return buildInvalidOptionsResult(
                errorMessage = sanitizeErrorMessage("Invalid CLI options: ${e.message}", candidateSecrets),
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes
            )
        }

        // Validate exact reviewed migration SQL resource BEFORE creating any DataSource or opening any connection
        val validatedDdlSql = try {
            loadAndValidateExactAdditiveMigrationDdl()
        } catch (e: Exception) {
            return buildInvalidOptionsResult(
                errorMessage = sanitizeErrorMessage("Invalid migration resource: ${e.message}", candidateSecrets),
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                options = options
            )
        }

        // Validate JDBC configuration BEFORE any connection attempt
        val dataSource = try {
            dataSourceOverride ?: buildValidatedDataSourceFromFlagsOrEnv(flags, env, options, candidateSecrets)
        } catch (e: Exception) {
            return buildInvalidOptionsResult(
                errorMessage = sanitizeErrorMessage("Invalid JDBC configuration: ${e.message}", candidateSecrets),
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                options = options
            )
        }

        fun totalElapsedMs(): Long = (monotonicClock.nowMs() - processStartMonoMs).coerceAtLeast(0L)
        fun remainingTotalBudgetMs(): Long = options.maxTotalWallTimeMs - totalElapsedMs()

        // =========================================================================
        // STAGE 1: Bounded Connection Readiness (<= 10,000ms, separated from SQL)
        // =========================================================================
        val readinessStartMonoMs = monotonicClock.nowMs()
        val effectiveReadinessBudgetMs = minOf(options.readinessTimeoutMs, remainingTotalBudgetMs()).coerceAtLeast(0L)
        if (effectiveReadinessBudgetMs <= 0L) {
            val totalElapsed = totalElapsedMs()
            val report = SetupExecutionReport(
                outcome = SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED,
                stage = SetupStage.CONNECTION_READINESS,
                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                dryRun = options.dryRun,
                applyDdlRequested = options.applyDdl,
                ddlExecutedAndVerified = false,
                tablePresentBefore = null,
                tablePresentAfter = null,
                hasCreatePrivilege = null,
                migrationResourcePath = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
                readinessBudgetMs = 0L,
                readinessElapsedMs = 0L,
                sqlBudgetMs = options.sqlTimeoutMs,
                sqlElapsedMs = 0L,
                lockTimeoutMs = options.lockTimeoutMs,
                totalWallBudgetMs = options.maxTotalWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                recheckCommandHint = null,
                sanitizedDetail = "Total wall-time budget (${options.maxTotalWallTimeMs}ms) exhausted before connection readiness stage (0 SQL executed)",
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputReportFile, report)
            return SetupCliResult(exitCode = 10, report = report, outputReportPath = savedPath)
        }

        val readinessResult = acquireReadyConnectionsBounded(
            dataSource = dataSource,
            needSeparateWriteAndVerify = options.applyDdl,
            readinessTimeoutMs = effectiveReadinessBudgetMs,
            monotonicClock = monotonicClock,
            secretsToRedact = candidateSecrets
        )
        sampleHeap()
        val readinessEndMonoMs = monotonicClock.nowMs()
        val readinessElapsedMs = (readinessEndMonoMs - readinessStartMonoMs).coerceAtLeast(0L)

        if (readinessResult is ReadinessOutcome.Failure) {
            val totalElapsed = totalElapsedMs()
            val totalExhausted = remainingTotalBudgetMs() <= 0L
            val exitCode = if (totalExhausted) 10 else 3
            val report = SetupExecutionReport(
                outcome = if (totalExhausted) {
                    SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED
                } else {
                    SetupOutcome.ABORTED_READINESS_FAILURE_NO_SQL
                },
                stage = SetupStage.CONNECTION_READINESS,
                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                dryRun = options.dryRun,
                applyDdlRequested = options.applyDdl,
                ddlExecutedAndVerified = false,
                tablePresentBefore = null,
                tablePresentAfter = null,
                hasCreatePrivilege = null,
                migrationResourcePath = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
                readinessBudgetMs = effectiveReadinessBudgetMs,
                readinessElapsedMs = readinessElapsedMs,
                sqlBudgetMs = options.sqlTimeoutMs,
                sqlElapsedMs = 0L,
                lockTimeoutMs = options.lockTimeoutMs,
                totalWallBudgetMs = options.maxTotalWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                recheckCommandHint = null,
                sanitizedDetail = readinessResult.sanitizedReason,
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputReportFile, report)
            return SetupCliResult(exitCode = exitCode, report = report, outputReportPath = savedPath)
        }

        val readyConns = readinessResult as ReadinessOutcome.Success

        // Check shared total wall-time budget before entering Stage 2 (SQL & Verification)
        val remainingBeforeSqlMs = remainingTotalBudgetMs()
        if (remainingBeforeSqlMs <= 0L) {
            triggerNonBlockingConnectionClose(readyConns.primaryConn, readyConns.verifyConn, readyConns.netTimeoutExec)
            val totalElapsed = totalElapsedMs()
            val report = SetupExecutionReport(
                outcome = SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED,
                stage = SetupStage.SQL_AND_VERIFICATION,
                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                dryRun = options.dryRun,
                applyDdlRequested = options.applyDdl,
                ddlExecutedAndVerified = false,
                tablePresentBefore = null,
                tablePresentAfter = null,
                hasCreatePrivilege = null,
                migrationResourcePath = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
                readinessBudgetMs = effectiveReadinessBudgetMs,
                readinessElapsedMs = readinessElapsedMs,
                sqlBudgetMs = 0L,
                sqlElapsedMs = 0L,
                lockTimeoutMs = options.lockTimeoutMs,
                totalWallBudgetMs = options.maxTotalWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                recheckCommandHint = null,
                sanitizedDetail = "Shared total wall-time budget (${options.maxTotalWallTimeMs}ms) exhausted after readiness stage before SQL execution (0 SQL executed)",
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputReportFile, report)
            return SetupCliResult(exitCode = 10, report = report, outputReportPath = savedPath)
        }

        // =========================================================================
        // STAGE 2: Unchanged <= 2,500ms SQL / Lock Fence & Verification Stage
        // =========================================================================
        val effectiveSqlBudgetMs = minOf(options.sqlTimeoutMs, remainingBeforeSqlMs).coerceAtLeast(1L)
        val sqlStartMonoMs = monotonicClock.nowMs()
        val sqlStageOutcome = try {
            executeSqlStageBounded(
                readyConns = readyConns,
                options = options,
                effectiveSqlBudgetMs = effectiveSqlBudgetMs,
                validatedDdlSql = validatedDdlSql,
                monotonicClock = monotonicClock,
                secretsToRedact = candidateSecrets
            )
        } finally {
            triggerNonBlockingConnectionClose(readyConns.primaryConn, readyConns.verifyConn, readyConns.netTimeoutExec)
        }
        sampleHeap()
        val sqlEndMonoMs = monotonicClock.nowMs()
        val sqlElapsedMs = (sqlEndMonoMs - sqlStartMonoMs).coerceAtLeast(0L)
        val totalElapsed = totalElapsedMs()

        val recheckHint = if (sqlStageOutcome.ddlCommitStatus == DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED) {
            "Run OffProdAdditiveIdentitySchemaSetupCli with --dry-run=true --apply-ddl=false to recheck whether public.player_riot_identities committed"
        } else {
            null
        }

        val finalStage = if (sqlStageOutcome.exitCode == 0) SetupStage.COMPLETED else SetupStage.SQL_AND_VERIFICATION
        val report = SetupExecutionReport(
            outcome = sqlStageOutcome.outcome,
            stage = finalStage,
            ddlCommitStatus = sqlStageOutcome.ddlCommitStatus,
            dryRun = options.dryRun,
            applyDdlRequested = options.applyDdl,
            ddlExecutedAndVerified = sqlStageOutcome.ddlExecutedAndVerified,
            tablePresentBefore = sqlStageOutcome.tablePresentBefore,
            tablePresentAfter = sqlStageOutcome.tablePresentAfter,
            hasCreatePrivilege = sqlStageOutcome.hasCreatePrivilege,
            migrationResourcePath = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
            readinessBudgetMs = effectiveReadinessBudgetMs,
            readinessElapsedMs = readinessElapsedMs,
            sqlBudgetMs = effectiveSqlBudgetMs,
            sqlElapsedMs = sqlElapsedMs,
            lockTimeoutMs = options.lockTimeoutMs,
            totalWallBudgetMs = options.maxTotalWallTimeMs,
            totalWallElapsedMs = totalElapsed,
            jvmMaxHeapBytes = maxHeapBytes,
            jvmUsedHeapBytesPeak = peakUsedHeapBytes,
            recheckCommandHint = recheckHint,
            sanitizedDetail = sqlStageOutcome.sanitizedDetail,
            completedAtUtc = wallClock.nowInstant()
        )
        val savedPath = writeReportBestEffort(outputReportFile, report)
        return SetupCliResult(
            exitCode = sqlStageOutcome.exitCode,
            report = report,
            outputReportPath = savedPath
        )
    }

    private sealed class ReadinessOutcome {
        data class Success(
            val primaryConn: Connection,
            val verifyConn: Connection?,
            val primaryConnWasAutoCommit: Boolean,
            val netTimeoutExec: java.util.concurrent.ExecutorService
        ) : ReadinessOutcome()

        data class Failure(
            val sanitizedReason: String
        ) : ReadinessOutcome()
    }

    private fun acquireReadyConnectionsBounded(
        dataSource: DataSource,
        needSeparateWriteAndVerify: Boolean,
        readinessTimeoutMs: Long,
        monotonicClock: MonotonicClock,
        secretsToRedact: List<String>
    ): ReadinessOutcome {
        val primaryConnRef = AtomicReference<Connection?>(null)
        val verifyConnRef = AtomicReference<Connection?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val startMonoMs = monotonicClock.nowMs()
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(readinessTimeoutMs)

        fun remainingMs(): Long {
            val remNanoMs = TimeUnit.NANOSECONDS.toMillis(deadlineNano - System.nanoTime())
            val remMonoMs = readinessTimeoutMs - (monotonicClock.nowMs() - startMonoMs)
            return minOf(remNanoMs, remMonoMs)
        }

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || remainingMs() <= 0L

        val netTimeoutExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-setup-net-timeout").apply { isDaemon = true }
        }
        val readinessExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-setup-readiness").apply { isDaemon = true }
        }

        val future = readinessExec.submit(Callable<ReadinessOutcome> {
            try {
                val initialRemMs = remainingMs()
                if (initialRemMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired before getConnection (0 SQL executed)"
                    )
                }
                dataSource.loginTimeout = ceil(initialRemMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before getConnection (0 SQL executed)"
                    )
                }

                val c1 = dataSource.connection
                primaryConnRef.set(c1)
                if (isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    triggerNonBlockingConnectionClose(c1, null, netTimeoutExec)
                    return@Callable ReadinessOutcome.Failure(
                        "Primary connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without executing any SQL"
                    )
                }

                val remAfterC1Ms = remainingMs()
                if (remAfterC1Ms <= 0L || isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    triggerNonBlockingConnectionClose(c1, null, netTimeoutExec)
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired immediately after primary connection acquisition; closed without SQL"
                    )
                }

                c1.isReadOnly = !needSeparateWriteAndVerify
                c1.setNetworkTimeout(netTimeoutExec, remAfterC1Ms.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                c1.autoCommit = true
                val c1AutoCommit = c1.autoCommit

                if (isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    triggerNonBlockingConnectionClose(c1, null, netTimeoutExec)
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired after primary connection setup; closed without SQL"
                    )
                }

                var c2: Connection? = null
                if (needSeparateWriteAndVerify) {
                    val remBeforeC2Ms = remainingMs()
                    if (remBeforeC2Ms <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, null, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Connection readiness deadline (${readinessTimeoutMs}ms) expired before verification connection acquisition; closed without SQL"
                        )
                    }
                    dataSource.loginTimeout = ceil(remBeforeC2Ms.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                    if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, null, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Connection readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before verification connection acquisition; closed without SQL"
                        )
                    }
                    val rawC2 = dataSource.connection
                    verifyConnRef.set(rawC2)
                    if (isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, rawC2, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Verification connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without executing any SQL"
                        )
                    }

                    if (!c1AutoCommit && rawC2 === c1 && !rawC2.autoCommit) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, rawC2, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Readiness durability check failed: DDL connection has autoCommit=false and DataSource returned the same Connection instance for verification (0 SQL executed)"
                        )
                    }

                    val remAfterC2Ms = remainingMs()
                    if (remAfterC2Ms <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, rawC2, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Connection readiness deadline (${readinessTimeoutMs}ms) expired immediately after verification connection acquisition; closed without SQL"
                        )
                    }

                    rawC2.isReadOnly = true
                    rawC2.setNetworkTimeout(netTimeoutExec, remAfterC2Ms.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                    rawC2.autoCommit = true

                    if (isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        triggerNonBlockingConnectionClose(c1, rawC2, netTimeoutExec)
                        return@Callable ReadinessOutcome.Failure(
                            "Connection readiness deadline (${readinessTimeoutMs}ms) expired after verification connection setup; closed without SQL"
                        )
                    }
                    c2 = rawC2
                }

                ReadinessOutcome.Success(
                    primaryConn = c1,
                    verifyConn = c2,
                    primaryConnWasAutoCommit = c1AutoCommit,
                    netTimeoutExec = netTimeoutExec
                )
            } catch (e: Exception) {
                val c1 = primaryConnRef.getAndSet(null)
                val c2 = verifyConnRef.getAndSet(null)
                triggerNonBlockingConnectionClose(c1, c2, netTimeoutExec)
                ReadinessOutcome.Failure(
                    sanitizeErrorMessage(
                        "Connection readiness failed (${e.javaClass.simpleName}): ${e.message} (0 SQL executed)",
                        secretsToRedact
                    )
                )
            }
        })

        return try {
            future.get(readinessTimeoutMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val c1 = primaryConnRef.getAndSet(null)
            val c2 = verifyConnRef.getAndSet(null)
            triggerNonBlockingConnectionClose(c1, c2, netTimeoutExec)
            ReadinessOutcome.Failure(
                "Connection readiness exceeded ${readinessTimeoutMs}ms deadline; late connections fenced and closed without executing any SQL"
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val c1 = primaryConnRef.getAndSet(null)
            val c2 = verifyConnRef.getAndSet(null)
            triggerNonBlockingConnectionClose(c1, c2, netTimeoutExec)
            ReadinessOutcome.Failure(
                sanitizeErrorMessage(
                    "Connection readiness interrupted/failed: ${e.message} (0 SQL executed)",
                    secretsToRedact
                )
            )
        } finally {
            readinessExec.shutdownNow()
        }
    }

    private data class SqlStageOutcome(
        val exitCode: Int,
        val outcome: SetupOutcome,
        val ddlCommitStatus: DdlCommitStatus,
        val ddlExecutedAndVerified: Boolean,
        val tablePresentBefore: Boolean?,
        val tablePresentAfter: Boolean?,
        val hasCreatePrivilege: Boolean?,
        val sanitizedDetail: String
    )

    private fun executeSqlStageBounded(
        readyConns: ReadinessOutcome.Success,
        options: SetupOptions,
        effectiveSqlBudgetMs: Long,
        validatedDdlSql: String,
        monotonicClock: MonotonicClock,
        secretsToRedact: List<String>
    ): SqlStageOutcome {
        val activeStmtRef = AtomicReference<PreparedStatement?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val inFlightDdlStarted = AtomicBoolean(false)
        val tablePresentBeforeRef = AtomicReference<Boolean?>(null)
        val hasCreatePrivilegeRef = AtomicReference<Boolean?>(null)

        val startMonoMs = monotonicClock.nowMs()
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveSqlBudgetMs)

        fun remainingSqlMs(): Long {
            val remNanoMs = TimeUnit.NANOSECONDS.toMillis(deadlineNano - System.nanoTime())
            val remMonoMs = effectiveSqlBudgetMs - (monotonicClock.nowMs() - startMonoMs)
            return minOf(remNanoMs, remMonoMs)
        }

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || remainingSqlMs() <= 0L

        val sqlExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-setup-sql").apply { isDaemon = true }
        }

        val future = sqlExec.submit(Callable<SqlStageOutcome> {
            try {
                val initialRemMs = remainingSqlMs()
                if (initialRemMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = null,
                        tablePresentAfter = null,
                        hasCreatePrivilege = null,
                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before statement preparation (0 SQL executed)"
                    )
                }

                readyConns.primaryConn.setNetworkTimeout(
                    readyConns.netTimeoutExec,
                    initialRemMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1)
                )
                readyConns.verifyConn?.setNetworkTimeout(
                    readyConns.netTimeoutExec,
                    initialRemMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1)
                )

                if (options.dryRun) {
                    // Default Dry-Run Mode: read-only check only, 0 DDL statements executed
                    if (isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            hasCreatePrivilege = null,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before dry-run prepareStatement (0 SQL executed)"
                        )
                    }

                    var present = false
                    var canCreate = false
                    readyConns.primaryConn.prepareStatement(SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE.trimIndent()).use { ps ->
                        activeStmtRef.set(ps)
                        val remBeforeSetupMs = remainingSqlMs()
                        if (remBeforeSetupMs <= 0L || isCancelledOrPastDeadline()) {
                            activeStmtRef.set(null)
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                ddlExecutedAndVerified = false,
                                tablePresentBefore = null,
                                tablePresentAfter = null,
                                hasCreatePrivilege = null,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before dry-run setQueryTimeout (0 SQL executed)"
                            )
                        }
                        ps.queryTimeout = ceil(remBeforeSetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        // Critical fence AFTER all setup DIRECTLY BEFORE executeQuery():
                        if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                            activeStmtRef.set(null)
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                ddlExecutedAndVerified = false,
                                tablePresentBefore = null,
                                tablePresentAfter = null,
                                hasCreatePrivilege = null,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired after statement setup directly before dry-run executeQuery (0 SQL executed)"
                            )
                        }
                        ps.executeQuery().use { rs ->
                            if (rs.next()) {
                                if (isCancelledOrPastDeadline()) {
                                    activeStmtRef.set(null)
                                    return@Callable SqlStageOutcome(
                                        exitCode = 3,
                                        outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                        ddlExecutedAndVerified = false,
                                        tablePresentBefore = null,
                                        tablePresentAfter = null,
                                        hasCreatePrivilege = null,
                                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired while reading dry-run ResultSet (0 DDL executed)"
                                    )
                                }
                                present = rs.getBoolean("table_present")
                                canCreate = rs.getBoolean("can_create")
                            }
                        }
                        activeStmtRef.set(null)
                    }

                    if (isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = present,
                            tablePresentAfter = present,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired upon dry-run completion (0 DDL executed)"
                        )
                    }

                    return@Callable SqlStageOutcome(
                        exitCode = 0,
                        outcome = SetupOutcome.DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = present,
                        tablePresentAfter = present,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Dry-run readiness and schema check completed within ${effectiveSqlBudgetMs}ms SQL fence (0 DDL executed)"
                    )
                }

                // =========================================================================
                // Apply-DDL Mode:
                // 1. Pre-check table_present & can_create on read-only verifyConn
                // 2. Enforce session lock_timeout + statement_timeout on real PGConnection
                // 3. Execute CREATE TABLE IF NOT EXISTS on primaryConn with pre-execute fence
                // 4. Verify table_present == true on separate verifyConn within the 2,500ms fence
                // =========================================================================
                val verifyConn = requireNotNull(readyConns.verifyConn) {
                    "Separate verification connection required for --apply-ddl=true"
                }

                var presentBefore = false
                var canCreate = true
                verifyConn.prepareStatement(SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE.trimIndent()).use { ps ->
                    activeStmtRef.set(ps)
                    val remPreCheckMs = remainingSqlMs()
                    if (remPreCheckMs <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            hasCreatePrivilege = null,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before pre-DDL schema check setup (0 DDL executed)"
                        )
                    }
                    ps.queryTimeout = ceil(remPreCheckMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                    // Critical fence AFTER all setup DIRECTLY BEFORE executeQuery():
                    if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            hasCreatePrivilege = null,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired directly before pre-DDL schema check executeQuery (0 DDL executed)"
                        )
                    }
                    ps.executeQuery().use { rs ->
                        if (rs.next()) {
                            presentBefore = rs.getBoolean("table_present")
                            canCreate = rs.getBoolean("can_create")
                        }
                    }
                    activeStmtRef.set(null)
                }
                tablePresentBeforeRef.set(presentBefore)
                hasCreatePrivilegeRef.set(canCreate)

                // On real PostgreSQL connections, apply server-side lock_timeout and statement_timeout
                val isRealPg = runCatching { readyConns.primaryConn.isWrapperFor(PGConnection::class.java) }.getOrDefault(false)
                if (isRealPg) {
                    val remLockSetupMs = remainingSqlMs()
                    if (remLockSetupMs <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before SET lock_timeout/statement_timeout (0 DDL executed)"
                        )
                    }
                    val stmtTimeoutMs = minOf(options.sqlTimeoutMs, remLockSetupMs).coerceAtLeast(1L)
                    val effectiveLockMs = minOf(options.lockTimeoutMs, stmtTimeoutMs).coerceAtLeast(1L)
                    readyConns.primaryConn.createStatement().use { st ->
                        st.queryTimeout = ceil(remLockSetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                ddlExecutedAndVerified = false,
                                tablePresentBefore = presentBefore,
                                tablePresentAfter = presentBefore,
                                hasCreatePrivilege = canCreate,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired directly before SET lock_timeout/statement_timeout (0 DDL executed)"
                            )
                        }
                        st.execute("SET lock_timeout = '${effectiveLockMs}ms'; SET statement_timeout = '${stmtTimeoutMs}ms'")
                    }
                }

                if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = presentBefore,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before DDL prepareStatement (0 DDL executed)"
                    )
                }

                readyConns.primaryConn.prepareStatement(validatedDdlSql).use { ddlPs ->
                    activeStmtRef.set(ddlPs)
                    val remDdlSetupMs = remainingSqlMs()
                    if (remDdlSetupMs <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before DDL setQueryTimeout (0 DDL executed)"
                        )
                    }
                    ddlPs.queryTimeout = ceil(remDdlSetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)

                    // Critical fence AFTER all statement setup DIRECTLY BEFORE ddlPs.executeUpdate():
                    if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired or cancelled after setter setup directly before DDL executeUpdate (0 DDL executed)"
                        )
                    }

                    inFlightDdlStarted.set(true)
                    ddlPs.executeUpdate()
                    activeStmtRef.set(null)
                }

                if (!readyConns.primaryConnWasAutoCommit) {
                    // Critical fence DIRECTLY BEFORE explicit commit():
                    if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                        runCatching { readyConns.primaryConn.rollback() }
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT,
                            ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = null,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired directly before explicit DDL commit; rollback attempted, recheck required"
                        )
                    }
                    readyConns.primaryConn.commit()
                }

                // Mandatory post-commit verification read on the separate pre-warmed verifyConn:
                val remVerifyMs = remainingSqlMs()
                if (remVerifyMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "DDL executeUpdate completed but SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before separate-connection post-commit verification read"
                    )
                }

                var presentAfter = false
                verifyConn.prepareStatement(SQL_VERIFY_TABLE_PRESENT.trimIndent()).use { verifyPs ->
                    activeStmtRef.set(verifyPs)
                    val remVerifySetupMs = remainingSqlMs()
                    if (remVerifySetupMs <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                            ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = null,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "DDL executeUpdate completed but deadline expired before verification setQueryTimeout"
                        )
                    }
                    verifyPs.queryTimeout = ceil(remVerifySetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                    // Critical fence AFTER all setup DIRECTLY BEFORE verification executeQuery():
                    if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                            ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                            ddlExecutedAndVerified = false,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = null,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "DDL executeUpdate completed but deadline expired directly before verification executeQuery"
                        )
                    }
                    verifyPs.executeQuery().use { rs ->
                        if (rs.next()) {
                            presentAfter = rs.getBoolean("table_present")
                        }
                    }
                    activeStmtRef.set(null)
                }

                if (isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired during post-commit verification read"
                    )
                }

                if (!presentAfter) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = false,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Post-commit verification read on separate connection returned table_present=false after DDL execution"
                    )
                }

                SqlStageOutcome(
                    exitCode = 0,
                    outcome = SetupOutcome.APPLIED_AND_VERIFIED_DDL,
                    ddlCommitStatus = DdlCommitStatus.COMMITTED_AND_VERIFIED,
                    ddlExecutedAndVerified = true,
                    tablePresentBefore = presentBefore,
                    tablePresentAfter = true,
                    hasCreatePrivilege = canCreate,
                    sanitizedDetail = "Applied reviewed additive migration ${JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH} and verified table presence on separate connection within ${effectiveSqlBudgetMs}ms SQL fence"
                )
            } catch (e: Exception) {
                val started = inFlightDdlStarted.get()
                val outcome = if (started) {
                    SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT
                } else {
                    SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL
                }
                val commitStatus = if (started) {
                    DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED
                } else {
                    DdlCommitStatus.NOT_STARTED_ZERO_SQL
                }
                SqlStageOutcome(
                    exitCode = 3,
                    outcome = outcome,
                    ddlCommitStatus = commitStatus,
                    ddlExecutedAndVerified = false,
                    tablePresentBefore = tablePresentBeforeRef.get(),
                    tablePresentAfter = null,
                    hasCreatePrivilege = hasCreatePrivilegeRef.get(),
                    sanitizedDetail = sanitizeErrorMessage(
                        "SQL stage failed (${e.javaClass.simpleName}): ${e.message}",
                        secretsToRedact
                    )
                )
            } finally {
                activeStmtRef.set(null)
            }
        })

        return try {
            future.get(effectiveSqlBudgetMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val s = activeStmtRef.getAndSet(null)
            Thread({
                runCatching { s?.cancel() }
                runCatching { s?.close() }
                runCatching { readyConns.primaryConn.abort(readyConns.netTimeoutExec) }
                runCatching { readyConns.verifyConn?.abort(readyConns.netTimeoutExec) }
            }, "offprod-setup-sql-abort").apply {
                isDaemon = true
                start()
            }
            val started = inFlightDdlStarted.get()
            SqlStageOutcome(
                exitCode = 3,
                outcome = if (started) {
                    SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT
                } else {
                    SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL
                },
                ddlCommitStatus = if (started) {
                    DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED
                } else {
                    DdlCommitStatus.NOT_STARTED_ZERO_SQL
                },
                ddlExecutedAndVerified = false,
                tablePresentBefore = tablePresentBeforeRef.get(),
                tablePresentAfter = null,
                hasCreatePrivilege = hasCreatePrivilegeRef.get(),
                sanitizedDetail = if (started) {
                    "SQL/lock execution exceeded ${effectiveSqlBudgetMs}ms deadline while DDL or post-commit verification was in flight; commit status unknown, run --dry-run=true to recheck"
                } else {
                    "SQL stage exceeded ${effectiveSqlBudgetMs}ms deadline before DDL executeUpdate was invoked (0 DDL executed)"
                }
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val started = inFlightDdlStarted.get()
            SqlStageOutcome(
                exitCode = 3,
                outcome = if (started) {
                    SetupOutcome.ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT
                } else {
                    SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL
                },
                ddlCommitStatus = if (started) {
                    DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED
                } else {
                    DdlCommitStatus.NOT_STARTED_ZERO_SQL
                },
                ddlExecutedAndVerified = false,
                tablePresentBefore = tablePresentBeforeRef.get(),
                tablePresentAfter = null,
                hasCreatePrivilege = hasCreatePrivilegeRef.get(),
                sanitizedDetail = sanitizeErrorMessage(
                    "SQL stage interrupted/failed: ${e.message}",
                    secretsToRedact
                )
            )
        } finally {
            sqlExec.shutdownNow()
        }
    }

    private fun triggerNonBlockingConnectionClose(
        c1: Connection?,
        c2: Connection?,
        netTimeoutExec: java.util.concurrent.ExecutorService?
    ) {
        Thread({
            if (netTimeoutExec != null) {
                runCatching { c1?.abort(netTimeoutExec) }
                runCatching { c2?.abort(netTimeoutExec) }
            }
            runCatching { c1?.close() }
            runCatching { c2?.close() }
            runCatching { netTimeoutExec?.shutdown() }
        }, "offprod-setup-conn-close").apply {
            isDaemon = true
            start()
        }
    }

    private fun buildInvalidOptionsResult(
        errorMessage: String,
        outputReportFile: File?,
        processStartMonoMs: Long,
        monotonicClock: MonotonicClock,
        wallClock: WallClock,
        maxHeapBytes: Long,
        peakUsedHeapBytes: Long,
        options: SetupOptions? = null
    ): SetupCliResult {
        val elapsed = (monotonicClock.nowMs() - processStartMonoMs).coerceAtLeast(0L)
        val report = SetupExecutionReport(
            outcome = SetupOutcome.ABORTED_INVALID_OPTIONS,
            stage = SetupStage.OPTION_VALIDATION,
            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
            dryRun = options?.dryRun ?: true,
            applyDdlRequested = options?.applyDdl ?: false,
            ddlExecutedAndVerified = false,
            tablePresentBefore = null,
            tablePresentAfter = null,
            hasCreatePrivilege = null,
            migrationResourcePath = JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH,
            readinessBudgetMs = options?.readinessTimeoutMs ?: DEFAULT_READINESS_TIMEOUT_MS,
            readinessElapsedMs = 0L,
            sqlBudgetMs = options?.sqlTimeoutMs ?: DEFAULT_SQL_TIMEOUT_MS,
            sqlElapsedMs = 0L,
            lockTimeoutMs = options?.lockTimeoutMs ?: DEFAULT_LOCK_TIMEOUT_MS,
            totalWallBudgetMs = options?.maxTotalWallTimeMs ?: DEFAULT_TOTAL_WALL_TIME_MS,
            totalWallElapsedMs = elapsed,
            jvmMaxHeapBytes = maxHeapBytes,
            jvmUsedHeapBytesPeak = peakUsedHeapBytes,
            recheckCommandHint = null,
            sanitizedDetail = errorMessage,
            completedAtUtc = wallClock.nowInstant()
        )
        val savedPath = writeReportBestEffort(outputReportFile, report)
        return SetupCliResult(
            exitCode = 2,
            report = report,
            outputReportPath = savedPath
        )
    }

    private fun writeReportBestEffort(file: File?, report: SetupExecutionReport): String? {
        if (file == null) return null
        return try {
            file.parentFile?.mkdirs()
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, report)
            file.absolutePath
        } catch (_: Exception) {
            null
        }
    }

    private fun parseFlagsStrict(args: List<String>, secretsToRedact: List<String>): Map<String, String> {
        val result = LinkedHashMap<String, String>()
        for (raw in args) {
            val trimmed = raw.trim()
            if (trimmed.isEmpty()) continue
            require(trimmed.startsWith("--")) {
                "Unsupported positional argument '${sanitizeErrorMessage(trimmed, secretsToRedact)}'; all arguments must be --key=value flags"
            }
            val withoutPrefix = trimmed.removePrefix("--")
            val eqIdx = withoutPrefix.indexOf('=')
            val key: String
            val value: String
            if (eqIdx > 0) {
                key = withoutPrefix.substring(0, eqIdx).trim().lowercase(Locale.ROOT)
                value = withoutPrefix.substring(eqIdx + 1).trim()
            } else {
                key = withoutPrefix.trim().lowercase(Locale.ROOT)
                value = "true"
            }
            require(key in ALLOWED_FLAGS) {
                "Unknown CLI flag '--$key'"
            }
            result[key] = value
        }
        return result
    }

    private fun parseBooleanFlag(flags: Map<String, String>, key: String, defaultValue: Boolean): Boolean {
        val raw = flags[key] ?: return defaultValue
        return raw.lowercase(Locale.ROOT).toBooleanStrictOrNull()
            ?: throw IllegalArgumentException("--$key must be 'true' or 'false' (was '$raw')")
    }

    private fun parseLongFlag(flags: Map<String, String>, key: String, defaultValue: Long): Long {
        val raw = flags[key] ?: return defaultValue
        return raw.toLongOrNull()
            ?: throw IllegalArgumentException("--$key must be a valid 64-bit integer (was '$raw')")
    }

    private fun buildValidatedDataSourceFromFlagsOrEnv(
        flags: Map<String, String>,
        env: Map<String, String>,
        setupOptions: SetupOptions,
        secretsToRedact: List<String>
    ): DataSource {
        val jdbcUrl = flags["jdbc-url"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["OFFPROD_JDBC_URL"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: run {
                val host = (flags["db-host"] ?: env["OFFPROD_DB_HOST"] ?: env["DB_HOST"] ?: "").trim()
                val portRaw = (flags["db-port"] ?: env["OFFPROD_DB_PORT"] ?: env["DB_PORT"] ?: "5432").trim()
                val port = portRaw.toIntOrNull()?.takeIf { it in 1..65535 }
                    ?: throw IllegalArgumentException("Invalid database port '$portRaw'")
                val name = (flags["db-name"] ?: env["OFFPROD_DB_NAME"] ?: env["DB_NAME"] ?: "summoner_searcher").trim()
                require(host.isNotBlank()) {
                    "Missing JDBC target: provide OFFPROD_JDBC_URL (or --jdbc-url / OFFPROD_DB_HOST)"
                }
                require(name.isNotBlank()) {
                    "Database name must not be blank"
                }
                "jdbc:postgresql://$host:$port/$name"
            }

        require(jdbcUrl.startsWith("jdbc:postgresql://")) {
            "Off-production setup CLI requires a jdbc:postgresql:// URL (received '${sanitizeErrorMessage(jdbcUrl, secretsToRedact)}')"
        }

        val dbUser = (
            flags["db-user"]?.trim()?.takeIf { it.isNotEmpty() }
                ?: env["OFFPROD_DB_USER"]?.trim()?.takeIf { it.isNotEmpty() }
                ?: env["DB_USER"]?.trim()?.takeIf { it.isNotEmpty() }
                ?: "postgres"
            )
        require(dbUser.isNotBlank()) { "Database user must not be blank" }

        val dbPass = flags["db-pass"]
            ?: env["OFFPROD_DB_PASS"]
            ?: env["DB_PASS"]
            ?: ""

        val readinessSec = ceil(setupOptions.readinessTimeoutMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
        val sqlSec = ceil(setupOptions.sqlTimeoutMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)

        return PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = dbUser
            password = dbPass
            connectTimeout = readinessSec
            socketTimeout = maxOf(readinessSec, sqlSec)
            this.options = "-c lock_timeout=${setupOptions.lockTimeoutMs}ms -c statement_timeout=${setupOptions.sqlTimeoutMs}ms"
        }
    }
}
