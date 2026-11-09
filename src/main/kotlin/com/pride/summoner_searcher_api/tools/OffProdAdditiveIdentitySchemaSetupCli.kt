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
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
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
 *      `maxTotalWallTimeMs in 1..15_000` with `maxTotalWallTimeMs >= sqlTimeoutMs`,
 *      `cleanupTimeoutMs in 1..5_000`), mode safety (`--apply-ddl=true` requires `--dry-run=false`
 *      and `--allow-ddl-writes=true`), standalone JVM heap ceiling (`<= 256 MiB` when enforced),
 *      and PostgreSQL JDBC target **before** constructing any [DataSource] or calling `getConnection()`.
 * 2. **Default Dry-Run Zero DDL (`--dry-run=true --apply-ddl=false`)**:
 *    - Opens a single read-only connection (`conn.isReadOnly = true`) during Readiness (`<= 10s`),
 *      verifies its physical backend PID (`SELECT pg_backend_pid()`), binds `SET search_path = public, pg_catalog`,
 *      and executes only a read-only schema/contract/privilege check within the `<= 2,500ms` SQL fence (`0` DDL statements).
 * 3. **Bounded Connection Readiness (`<= 10,000ms`) with Unconditional Physical PID Separation**:
 *    - Stage 1 (`CONNECTION_READINESS`, `<= 10,000ms`) pre-warms the JDBC connection(s).
 *    - When `--apply-ddl=true`, unconditionally enforces `verifyConn !== primaryConn`, distinct unwrapped
 *      physical [PGConnection] instances (when supported), AND distinct PostgreSQL backend PIDs
 *      (`primaryBackendPid != verifyBackendPid` via `SELECT pg_backend_pid()`) **before** any DDL is prepared or executed.
 *    - If Stage 1 times out, fails, or returns connections sharing the same physical backend PID,
 *      connections are closed with bounded await and `0` DDL statements are executed (`NOT_STARTED_ZERO_SQL`).
 * 4. **Trusted Fixed `search_path` (`public, pg_catalog`) & Full Table Shape Contract Validation**:
 *    - Explicitly executes `SET search_path = public, pg_catalog` on `primaryConn` and `verifyConn` and
 *      verifies `current_schema() = 'public'` before pre-check, before unqualified `CREATE TABLE IF NOT EXISTS`,
 *      and before post-commit verification, preventing `search_path` shadowing.
 *    - Validates the full `public.player_riot_identities` schema contract (exact 5 columns, exact types/lengths,
 *      `NOT NULL` on all 5 columns, `PRIMARY KEY (puuid)`, and `chk_player_riot_identities_nonblank`
 *      `BTRIM(game_name) <> '' AND BTRIM(tag_line) <> ''`), failing closed (`ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA`
 *      or `ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT`) if an existing table in `public` has an incompatible shape.
 * 5. **Bounded Awaited Connection Cleanup & Truthful Cleanup Status Reporting**:
 *    - Awaits connection `close()`/`abort()` in `finally` up to `cleanupTimeoutMs` so standalone `exitProcess`
 *      never terminates before close completes, and reports `connectionsClosedCleanly`, `cleanupTimedOut`,
 *      and `cleanupElapsedMs` in [SetupExecutionReport].
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

    const val MAX_CLEANUP_TIMEOUT_MS: Long = 5_000L
    const val DEFAULT_CLEANUP_TIMEOUT_MS: Long = 1_000L

    const val MAX_STANDALONE_JVM_HEAP_BYTES: Long = 256L * 1024L * 1024L

    val ALLOWED_FLAGS: Set<String> = setOf(
        "dry-run",
        "apply-ddl",
        "allow-ddl-writes",
        "readiness-timeout-ms",
        "sql-timeout-ms",
        "lock-timeout-ms",
        "max-total-wall-time-ms",
        "cleanup-timeout-ms",
        "enforce-max-heap-256m",
        "jdbc-url",
        "db-host",
        "db-port",
        "db-name",
        "db-user",
        "db-pass",
        "output-report-file"
    )

    const val SQL_SELECT_BACKEND_PID: String = "SELECT pg_backend_pid() AS backend_pid"

    const val SQL_SET_TRUSTED_SEARCH_PATH: String = "SET search_path = public, pg_catalog"

    const val SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE: String = """
        SELECT
            to_regclass('public.player_riot_identities') IS NOT NULL AS table_present,
            has_schema_privilege(current_user, 'public', 'CREATE') AS can_create,
            current_schema() AS current_schema_name,
            CASE
                WHEN to_regclass('public.player_riot_identities') IS NULL THEN false
                ELSE (
                    (SELECT COUNT(*) FROM pg_catalog.pg_attribute
                      WHERE attrelid = to_regclass('public.player_riot_identities')
                        AND attnum > 0
                        AND NOT attisdropped) = 5
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'puuid'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(78)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'platform_region'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(16)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'game_name'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(255)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'tag_line'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(255)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'verified_at'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'timestamp with time zone'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'p'
                           AND c.convalidated = true
                           AND c.conkey = ARRAY[
                               (SELECT a.attnum FROM pg_catalog.pg_attribute a
                                 WHERE a.attrelid = to_regclass('public.player_riot_identities')
                                   AND a.attname = 'puuid'
                                   AND NOT a.attisdropped)
                           ]
                    )
                    AND (
                        SELECT COUNT(*) FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'c'
                    ) = 1
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'c'
                           AND c.conname = 'chk_player_riot_identities_nonblank'
                           AND c.convalidated = true
                           AND regexp_replace(lower(pg_catalog.pg_get_constraintdef(c.oid, true)), '[\s()]', '', 'g') =
                               'checkbtrimgame_name::text<>''''::textandbtrimtag_line::text<>''''::text'
                    )
                )
            END AS table_shape_valid
    """

    const val SQL_VERIFY_TABLE_PRESENT: String = """
        SELECT
            to_regclass('public.player_riot_identities') IS NOT NULL AS table_present,
            current_schema() AS current_schema_name,
            CASE
                WHEN to_regclass('public.player_riot_identities') IS NULL THEN false
                ELSE (
                    (SELECT COUNT(*) FROM pg_catalog.pg_attribute
                      WHERE attrelid = to_regclass('public.player_riot_identities')
                        AND attnum > 0
                        AND NOT attisdropped) = 5
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'puuid'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(78)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'platform_region'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(16)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'game_name'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(255)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'tag_line'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'character varying(255)'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_attribute
                         WHERE attrelid = to_regclass('public.player_riot_identities')
                           AND attname = 'verified_at'
                           AND NOT attisdropped
                           AND attnotnull = true
                           AND pg_catalog.format_type(atttypid, atttypmod) = 'timestamp with time zone'
                    )
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'p'
                           AND c.convalidated = true
                           AND c.conkey = ARRAY[
                               (SELECT a.attnum FROM pg_catalog.pg_attribute a
                                 WHERE a.attrelid = to_regclass('public.player_riot_identities')
                                   AND a.attname = 'puuid'
                                   AND NOT a.attisdropped)
                           ]
                    )
                    AND (
                        SELECT COUNT(*) FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'c'
                    ) = 1
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_constraint c
                         WHERE c.conrelid = to_regclass('public.player_riot_identities')
                           AND c.contype = 'c'
                           AND c.conname = 'chk_player_riot_identities_nonblank'
                           AND c.convalidated = true
                           AND regexp_replace(lower(pg_catalog.pg_get_constraintdef(c.oid, true)), '[\s()]', '', 'g') =
                               'checkbtrimgame_name::text<>''''::textandbtrimtag_line::text<>''''::text'
                    )
                )
            END AS table_shape_valid
    """

    enum class SetupStage {
        OPTION_VALIDATION,
        CONNECTION_READINESS,
        SQL_AND_VERIFICATION,
        CLEANUP,
        COMPLETED
    }

    enum class SetupOutcome {
        DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
        APPLIED_AND_VERIFIED_DDL,
        ABORTED_INVALID_OPTIONS,
        ABORTED_READINESS_FAILURE_NO_SQL,
        ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
        ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA,
        ABORTED_SQL_LOCK_OR_DEADLINE_UNKNOWN_COMMIT,
        ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
        ABORTED_TOTAL_BUDGET_EXHAUSTED,
        ABORTED_CLEANUP_FAILED_OR_TIMED_OUT
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
        val cleanupTimeoutMs: Long = DEFAULT_CLEANUP_TIMEOUT_MS,
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
            require(cleanupTimeoutMs in 1L..MAX_CLEANUP_TIMEOUT_MS) {
                "cleanupTimeoutMs ($cleanupTimeoutMs) must be in 1..$MAX_CLEANUP_TIMEOUT_MS"
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
        val primaryBackendPid: Int? = null,
        val verifyBackendPid: Int? = null,
        val searchPathBoundToPublic: Boolean? = null,
        val tablePresentBefore: Boolean? = null,
        val tablePresentAfter: Boolean? = null,
        val tableShapeValid: Boolean? = null,
        val hasCreatePrivilege: Boolean? = null,
        val connectionsClosedCleanly: Boolean = true,
        val cleanupTimedOut: Boolean = false,
        val cleanupElapsedMs: Long = 0L,
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

    internal data class CleanupStatus(
        val connectionsClosedCleanly: Boolean,
        val cleanupTimedOut: Boolean,
        val cleanupElapsedMs: Long,
        val cleanupError: String? = null
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
                    "outcome=${result.report.outcome}, commitStatus=${result.report.ddlCommitStatus}, " +
                    "closedCleanly=${result.report.connectionsClosedCleanly}): ${result.report.sanitizedDetail}"
            )
        } else {
            println(
                "[OffProdAdditiveIdentitySchemaSetupCli] OK: outcome=${result.report.outcome}, " +
                    "commitStatus=${result.report.ddlCommitStatus}, primaryPid=${result.report.primaryBackendPid}, " +
                    "verifyPid=${result.report.verifyBackendPid}, tablePresentBefore=${result.report.tablePresentBefore}, " +
                    "tablePresentAfter=${result.report.tablePresentAfter}, tableShapeValid=${result.report.tableShapeValid}, " +
                    "closedCleanly=${result.report.connectionsClosedCleanly}, readinessElapsedMs=${result.report.readinessElapsedMs}, " +
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
            val cleanupTimeoutMs = parseLongFlag(flags, "cleanup-timeout-ms", DEFAULT_CLEANUP_TIMEOUT_MS)
            val enforceMaxHeap256m = parseBooleanFlag(flags, "enforce-max-heap-256m", true)

            SetupOptions(
                dryRun = dryRun,
                applyDdl = applyDdl,
                allowDdlWrites = allowDdlWrites,
                readinessTimeoutMs = readinessTimeoutMs,
                sqlTimeoutMs = sqlTimeoutMs,
                lockTimeoutMs = lockTimeoutMs,
                maxTotalWallTimeMs = maxTotalWallTimeMs,
                cleanupTimeoutMs = cleanupTimeoutMs,
                enforceMaxHeap256m = enforceMaxHeap256m
            )
        } catch (e: IllegalArgumentException) {
            return buildInvalidOptionsResult(
                errorMessage = "Invalid CLI options: ${sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes
            )
        }

        if (options.enforceMaxHeap256m && maxHeapBytes > MAX_STANDALONE_JVM_HEAP_BYTES) {
            return buildInvalidOptionsResult(
                errorMessage = "Standalone JVM max heap ($maxHeapBytes bytes) exceeds 256 MiB ($MAX_STANDALONE_JVM_HEAP_BYTES bytes) cap; pass -Xmx128m or -Xmx256m",
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                options = options
            )
        }

        val validatedDdlSql = try {
            loadAndValidateExactAdditiveMigrationDdl()
        } catch (e: Exception) {
            return buildInvalidOptionsResult(
                errorMessage = "Failed to validate reviewed migration SQL resource: ${sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                options = options
            )
        }

        val dataSource = try {
            dataSourceOverride ?: buildValidatedDataSourceFromFlagsOrEnv(flags, env, options, candidateSecrets)
        } catch (e: Exception) {
            return buildInvalidOptionsResult(
                errorMessage = "Failed to configure PostgreSQL DataSource: ${sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputReportFile = outputReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                options = options
            )
        }

        val totalDeadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.maxTotalWallTimeMs)
        fun totalElapsedMs(): Long = (monotonicClock.nowMs() - processStartMonoMs).coerceAtLeast(0L)
        fun remainingTotalBudgetMs(): Long {
            val remMono = options.maxTotalWallTimeMs - totalElapsedMs()
            val remNano = TimeUnit.NANOSECONDS.toMillis(totalDeadlineNano - System.nanoTime())
            return minOf(remMono, remNano)
        }

        // =========================================================================
        // STAGE 1: Bounded Connection Readiness (<= 10,000ms) + Physical PID Check
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
            cleanupTimeoutMs = options.cleanupTimeoutMs,
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
                primaryBackendPid = readinessResult.primaryBackendPid,
                verifyBackendPid = readinessResult.verifyBackendPid,
                connectionsClosedCleanly = readinessResult.cleanupStatus.connectionsClosedCleanly,
                cleanupTimedOut = readinessResult.cleanupStatus.cleanupTimedOut,
                cleanupElapsedMs = readinessResult.cleanupStatus.cleanupElapsedMs,
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
            val cleanupStatus = closeConnectionsBounded(
                c1 = readyConns.primaryConn,
                c2 = readyConns.verifyConn,
                netTimeoutExec = readyConns.netTimeoutExec,
                awaitTimeoutMs = options.cleanupTimeoutMs,
                monotonicClock = monotonicClock,
                forceAbortFirst = false
            )
            val totalElapsed = totalElapsedMs()
            val report = SetupExecutionReport(
                outcome = SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED,
                stage = SetupStage.SQL_AND_VERIFICATION,
                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                dryRun = options.dryRun,
                applyDdlRequested = options.applyDdl,
                ddlExecutedAndVerified = false,
                primaryBackendPid = readyConns.primaryBackendPid,
                verifyBackendPid = readyConns.verifyBackendPid,
                connectionsClosedCleanly = cleanupStatus.connectionsClosedCleanly,
                cleanupTimedOut = cleanupStatus.cleanupTimedOut,
                cleanupElapsedMs = cleanupStatus.cleanupElapsedMs,
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
                sanitizedDetail = "Shared total wall-time budget (${options.maxTotalWallTimeMs}ms) exhausted after readiness stage before SQL execution (0 DDL executed)",
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputReportFile, report)
            return SetupCliResult(exitCode = 10, report = report, outputReportPath = savedPath)
        }

        // =========================================================================
        // STAGE 2: Unchanged <= 2,500ms SQL / Lock Fence & Verification Stage
        // =========================================================================
        val sqlStageAvailableFromTotalMs = (remainingBeforeSqlMs - options.cleanupTimeoutMs).coerceAtLeast(1L)
        val effectiveSqlBudgetMs = minOf(options.sqlTimeoutMs, sqlStageAvailableFromTotalMs).coerceAtLeast(1L)
        val sqlStartMonoMs = monotonicClock.nowMs()
        val sqlEndMonoRef = AtomicReference<Long>(sqlStartMonoMs)
        var cleanupStatus = CleanupStatus(connectionsClosedCleanly = true, cleanupTimedOut = false, cleanupElapsedMs = 0L)
        val sqlStageOutcome = try {
            val outcome = executeSqlStageBounded(
                readyConns = readyConns,
                options = options,
                effectiveSqlBudgetMs = effectiveSqlBudgetMs,
                validatedDdlSql = validatedDdlSql,
                monotonicClock = monotonicClock,
                secretsToRedact = candidateSecrets
            )
            sqlEndMonoRef.set(monotonicClock.nowMs())
            outcome
        } finally {
            if (sqlEndMonoRef.get() == sqlStartMonoMs) {
                sqlEndMonoRef.set(monotonicClock.nowMs())
            }
            cleanupStatus = closeConnectionsBounded(
                c1 = readyConns.primaryConn,
                c2 = readyConns.verifyConn,
                netTimeoutExec = readyConns.netTimeoutExec,
                awaitTimeoutMs = options.cleanupTimeoutMs,
                monotonicClock = monotonicClock,
                forceAbortFirst = false,
                secretsToRedact = candidateSecrets
            )
        }
        sampleHeap()
        val sqlElapsedMs = (sqlEndMonoRef.get() - sqlStartMonoMs).coerceAtLeast(0L)
        val totalElapsed = totalElapsedMs()
        val totalCeilingExceededAfterCleanup = totalElapsed > options.maxTotalWallTimeMs

        val finalExitCode: Int
        val finalOutcome: SetupOutcome
        val finalStage: SetupStage
        val rawFinalDetail: String
        if (sqlStageOutcome.exitCode == 0 && !cleanupStatus.connectionsClosedCleanly) {
            finalExitCode = 3
            finalOutcome = SetupOutcome.ABORTED_CLEANUP_FAILED_OR_TIMED_OUT
            finalStage = SetupStage.CLEANUP
            rawFinalDetail = "${sqlStageOutcome.sanitizedDetail}; however connection cleanup failed or timed out: ${cleanupStatus.cleanupError ?: "cleanup did not complete cleanly"}"
        } else if (sqlStageOutcome.exitCode == 0 && totalCeilingExceededAfterCleanup) {
            finalExitCode = 10
            finalOutcome = SetupOutcome.ABORTED_TOTAL_BUDGET_EXHAUSTED
            finalStage = SetupStage.CLEANUP
            rawFinalDetail = "${sqlStageOutcome.sanitizedDetail}; however total wall-time including awaited cleanup (${totalElapsed}ms) exceeded shared total budget (${options.maxTotalWallTimeMs}ms)"
        } else {
            finalExitCode = sqlStageOutcome.exitCode
            finalOutcome = sqlStageOutcome.outcome
            finalStage = if (sqlStageOutcome.exitCode == 0) SetupStage.COMPLETED else SetupStage.SQL_AND_VERIFICATION
            rawFinalDetail = if (!cleanupStatus.connectionsClosedCleanly && cleanupStatus.cleanupError != null) {
                "${sqlStageOutcome.sanitizedDetail} (cleanup warning: ${cleanupStatus.cleanupError})"
            } else {
                sqlStageOutcome.sanitizedDetail
            }
        }
        val finalDetail = sanitizeErrorMessage(rawFinalDetail, candidateSecrets)

        val recheckHint = if (sqlStageOutcome.ddlCommitStatus == DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED) {
            "Run OffProdAdditiveIdentitySchemaSetupCli with --dry-run=true --apply-ddl=false to recheck whether public.player_riot_identities committed with valid schema"
        } else {
            null
        }

        val report = SetupExecutionReport(
            outcome = finalOutcome,
            stage = finalStage,
            ddlCommitStatus = sqlStageOutcome.ddlCommitStatus,
            dryRun = options.dryRun,
            applyDdlRequested = options.applyDdl,
            ddlExecutedAndVerified = sqlStageOutcome.ddlExecutedAndVerified && cleanupStatus.connectionsClosedCleanly && !totalCeilingExceededAfterCleanup,
            primaryBackendPid = readyConns.primaryBackendPid,
            verifyBackendPid = readyConns.verifyBackendPid,
            searchPathBoundToPublic = sqlStageOutcome.searchPathBoundToPublic,
            tablePresentBefore = sqlStageOutcome.tablePresentBefore,
            tablePresentAfter = sqlStageOutcome.tablePresentAfter,
            tableShapeValid = sqlStageOutcome.tableShapeValid,
            hasCreatePrivilege = sqlStageOutcome.hasCreatePrivilege,
            connectionsClosedCleanly = cleanupStatus.connectionsClosedCleanly,
            cleanupTimedOut = cleanupStatus.cleanupTimedOut,
            cleanupElapsedMs = cleanupStatus.cleanupElapsedMs,
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
            sanitizedDetail = finalDetail,
            completedAtUtc = wallClock.nowInstant()
        )
        val savedPath = writeReportBestEffort(outputReportFile, report)
        return SetupCliResult(
            exitCode = finalExitCode,
            report = report,
            outputReportPath = savedPath
        )
    }

    private sealed class ReadinessOutcome {
        data class Success(
            val primaryConn: Connection,
            val verifyConn: Connection?,
            val primaryBackendPid: Int,
            val verifyBackendPid: Int?,
            val primaryConnWasAutoCommit: Boolean,
            val netTimeoutExec: ExecutorService
        ) : ReadinessOutcome()

        data class Failure(
            val sanitizedReason: String,
            val primaryBackendPid: Int? = null,
            val verifyBackendPid: Int? = null,
            val cleanupStatus: CleanupStatus = CleanupStatus(
                connectionsClosedCleanly = true,
                cleanupTimedOut = false,
                cleanupElapsedMs = 0L
            )
        ) : ReadinessOutcome()
    }

    internal fun queryBackendPidBounded(
        conn: Connection,
        remainingMsProvider: () -> Long,
        isCancelledProvider: () -> Boolean
    ): Int {
        if (isCancelledProvider() || remainingMsProvider() <= 0L) {
            throw TimeoutException("Readiness deadline expired before SELECT pg_backend_pid()")
        }
        val pgWrapperPid = runCatching {
            if (conn.isWrapperFor(PGConnection::class.java)) {
                conn.unwrap(PGConnection::class.java).backendPID
            } else {
                0
            }
        }.getOrDefault(0)

        val stmt = conn.prepareStatement(SQL_SELECT_BACKEND_PID)
            ?: throw SQLException("SELECT pg_backend_pid() failed: prepareStatement returned null (failing closed on unverifiable PID)")
        stmt.use { ps ->
            val remMs = remainingMsProvider()
            if (remMs <= 0L || isCancelledProvider()) {
                throw TimeoutException("Readiness deadline expired before pg_backend_pid() setQueryTimeout")
            }
            ps.queryTimeout = ceil(remMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
            if (remainingMsProvider() <= 0L || isCancelledProvider()) {
                throw TimeoutException("Readiness deadline expired directly before pg_backend_pid() executeQuery")
            }
            val rs = ps.executeQuery()
                ?: throw SQLException("SELECT pg_backend_pid() failed: executeQuery returned null (failing closed on unverifiable PID)")
            rs.use {
                val hasRow = runCatching { rs.next() }.getOrElse { ex ->
                    if (ex is SQLException) throw ex else false
                }
                if (!hasRow) {
                    throw SQLException("SELECT pg_backend_pid() failed: ResultSet returned 0 rows (failing closed on unverifiable PID)")
                }
                val byName = runCatching { rs.getInt("backend_pid") }.getOrElse { ex ->
                    if (ex is SQLException) throw ex else 0
                }
                val queriedPid = if (byName > 0) {
                    byName
                } else {
                    runCatching { rs.getInt(1) }.getOrElse { ex ->
                        if (ex is SQLException) throw ex else 0
                    }
                }
                if (queriedPid <= 0) {
                    throw SQLException("SELECT pg_backend_pid() returned non-positive or null PID ($queriedPid)")
                }
                if (pgWrapperPid > 0 && pgWrapperPid != queriedPid) {
                    throw SQLException(
                        "PGConnection.backendPID ($pgWrapperPid) does not match SELECT pg_backend_pid() ($queriedPid)"
                    )
                }
                return queriedPid
            }
        }
    }

    internal data class TableContractRow(
        val currentSchema: String,
        val tablePresent: Boolean,
        val tableShapeValid: Boolean?,
        val canCreate: Boolean?
    )

    internal fun readTableContractRowStrict(
        rs: java.sql.ResultSet?,
        requireCanCreate: Boolean
    ): TableContractRow {
        if (rs == null) {
            throw SQLException("Schema contract query returned null ResultSet; failing closed on unverifiable schema evidence")
        }
        val hasRow = runCatching { rs.next() }.getOrElse { ex ->
            if (ex is SQLException) throw ex else false
        }
        if (!hasRow) {
            throw SQLException("Schema contract query returned 0 rows; failing closed on unverifiable schema evidence")
        }
        val rawSchema = rs.getString("current_schema_name")?.trim().orEmpty()
        if (rawSchema.isEmpty()) {
            throw SQLException("Schema contract query returned null or blank current_schema_name; failing closed on unverifiable schema evidence")
        }
        val presentObj = rs.getObject("table_present") as? Boolean
            ?: throw SQLException("Schema contract query returned null or non-boolean table_present; failing closed on unverifiable schema evidence")
        val canCreateObj: Boolean? = if (requireCanCreate) {
            rs.getObject("can_create") as? Boolean
                ?: throw SQLException("Schema contract query returned null or non-boolean can_create; failing closed on unverifiable schema evidence")
        } else {
            null
        }
        val shapeValidObj: Boolean? = if (presentObj) {
            rs.getObject("table_shape_valid") as? Boolean
                ?: throw SQLException("Schema contract query returned null or non-boolean table_shape_valid when table_present=true; failing closed on unverifiable schema evidence")
        } else {
            null
        }
        return TableContractRow(
            currentSchema = rawSchema,
            tablePresent = presentObj,
            tableShapeValid = shapeValidObj,
            canCreate = canCreateObj
        )
    }

    private fun acquireReadyConnectionsBounded(
        dataSource: DataSource,
        needSeparateWriteAndVerify: Boolean,
        readinessTimeoutMs: Long,
        cleanupTimeoutMs: Long,
        monotonicClock: MonotonicClock,
        secretsToRedact: List<String>
    ): ReadinessOutcome {
        val primaryConnRef = AtomicReference<Connection?>(null)
        val verifyConnRef = AtomicReference<Connection?>(null)
        val primaryPidRef = AtomicReference<Int?>(null)
        val verifyPidRef = AtomicReference<Int?>(null)
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
                    netTimeoutExec.shutdown()
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired before getConnection (0 SQL executed)"
                    )
                }
                dataSource.loginTimeout = ceil(initialRemMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                    netTimeoutExec.shutdown()
                    return@Callable ReadinessOutcome.Failure(
                        "Connection readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before getConnection (0 SQL executed)"
                    )
                }

                val c1 = dataSource.connection
                primaryConnRef.set(c1)
                if (isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    val cs = closeConnectionsBounded(c1, null, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                    return@Callable ReadinessOutcome.Failure(
                        sanitizedReason = "Primary connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without executing any SQL",
                        cleanupStatus = cs
                    )
                }

                val remAfterC1Ms = remainingMs()
                if (remAfterC1Ms <= 0L || isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    val cs = closeConnectionsBounded(c1, null, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                    return@Callable ReadinessOutcome.Failure(
                        sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired immediately after primary connection acquisition; closed without SQL",
                        cleanupStatus = cs
                    )
                }

                c1.isReadOnly = !needSeparateWriteAndVerify
                c1.setNetworkTimeout(netTimeoutExec, remAfterC1Ms.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                c1.autoCommit = true
                val c1AutoCommit = c1.autoCommit

                if (isCancelledOrPastDeadline()) {
                    primaryConnRef.set(null)
                    val cs = closeConnectionsBounded(c1, null, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                    return@Callable ReadinessOutcome.Failure(
                        sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired after primary connection setup; closed without SQL",
                        cleanupStatus = cs
                    )
                }

                val c1Pid = queryBackendPidBounded(
                    conn = c1,
                    remainingMsProvider = ::remainingMs,
                    isCancelledProvider = ::isCancelledOrPastDeadline
                )
                primaryPidRef.set(c1Pid)

                var c2: Connection? = null
                var c2Pid: Int? = null
                if (needSeparateWriteAndVerify) {
                    val remBeforeC2Ms = remainingMs()
                    if (remBeforeC2Ms <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, null, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired before verification connection acquisition; closed without DDL",
                            primaryBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }
                    dataSource.loginTimeout = ceil(remBeforeC2Ms.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                    if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, null, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before verification connection acquisition; closed without DDL",
                            primaryBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }
                    val rawC2 = dataSource.connection
                    verifyConnRef.set(rawC2)
                    if (isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Verification connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without executing any DDL",
                            primaryBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }

                    // Unconditional object-identity check (never gated on !c1AutoCommit):
                    if (rawC2 === c1) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = false, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Readiness physical session check failed: DataSource returned the exact same Connection instance for primaryConn and verifyConn (0 DDL executed)",
                            primaryBackendPid = c1Pid,
                            verifyBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }

                    // Unconditional unwrapped PGConnection identity check:
                    val sameUnwrappedPg = runCatching {
                        if (c1.isWrapperFor(PGConnection::class.java) && rawC2.isWrapperFor(PGConnection::class.java)) {
                            val pg1 = c1.unwrap(PGConnection::class.java)
                            val pg2 = rawC2.unwrap(PGConnection::class.java)
                            pg1 === pg2 || (pg1.backendPID > 0 && pg1.backendPID == pg2.backendPID)
                        } else {
                            false
                        }
                    }.getOrDefault(false)
                    if (sameUnwrappedPg) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = false, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Readiness physical session check failed: primaryConn and verifyConn unwrap to the same physical PGConnection/PID ($c1Pid) (0 DDL executed)",
                            primaryBackendPid = c1Pid,
                            verifyBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }

                    val remAfterC2Ms = remainingMs()
                    if (remAfterC2Ms <= 0L || isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired immediately after verification connection acquisition; closed without DDL",
                            primaryBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }

                    rawC2.isReadOnly = true
                    rawC2.setNetworkTimeout(netTimeoutExec, remAfterC2Ms.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                    rawC2.autoCommit = true

                    if (isCancelledOrPastDeadline()) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Connection readiness deadline (${readinessTimeoutMs}ms) expired after verification connection setup; closed without DDL",
                            primaryBackendPid = c1Pid,
                            cleanupStatus = cs
                        )
                    }

                    val queriedC2Pid = queryBackendPidBounded(
                        conn = rawC2,
                        remainingMsProvider = ::remainingMs,
                        isCancelledProvider = ::isCancelledOrPastDeadline
                    )
                    verifyPidRef.set(queriedC2Pid)
                    if (queriedC2Pid == c1Pid) {
                        primaryConnRef.set(null)
                        verifyConnRef.set(null)
                        val cs = closeConnectionsBounded(c1, rawC2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = false, secretsToRedact = secretsToRedact)
                        return@Callable ReadinessOutcome.Failure(
                            sanitizedReason = "Readiness physical session check failed: primaryBackendPid ($c1Pid) and verifyBackendPid ($queriedC2Pid) must be distinct positive PostgreSQL backend PIDs before DDL (0 DDL executed)",
                            primaryBackendPid = c1Pid,
                            verifyBackendPid = queriedC2Pid,
                            cleanupStatus = cs
                        )
                    }
                    c2 = rawC2
                    c2Pid = queriedC2Pid
                }

                ReadinessOutcome.Success(
                    primaryConn = c1,
                    verifyConn = c2,
                    primaryBackendPid = c1Pid,
                    verifyBackendPid = c2Pid,
                    primaryConnWasAutoCommit = c1AutoCommit,
                    netTimeoutExec = netTimeoutExec
                )
            } catch (e: Exception) {
                val c1 = primaryConnRef.getAndSet(null)
                val c2 = verifyConnRef.getAndSet(null)
                val cs = closeConnectionsBounded(c1, c2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
                ReadinessOutcome.Failure(
                    sanitizedReason = sanitizeErrorMessage(
                        "Connection readiness failed (${e.javaClass.simpleName}): ${e.message} (0 DDL executed)",
                        secretsToRedact
                    ),
                    primaryBackendPid = primaryPidRef.get(),
                    verifyBackendPid = verifyPidRef.get(),
                    cleanupStatus = cs
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
            val cs = closeConnectionsBounded(c1, c2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
            ReadinessOutcome.Failure(
                sanitizedReason = "Connection readiness exceeded ${readinessTimeoutMs}ms deadline; late connections fenced and closed without executing any SQL",
                primaryBackendPid = primaryPidRef.get(),
                verifyBackendPid = verifyPidRef.get(),
                cleanupStatus = cs
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val c1 = primaryConnRef.getAndSet(null)
            val c2 = verifyConnRef.getAndSet(null)
            val cs = closeConnectionsBounded(c1, c2, netTimeoutExec, cleanupTimeoutMs, monotonicClock, forceAbortFirst = true, secretsToRedact = secretsToRedact)
            ReadinessOutcome.Failure(
                sanitizedReason = sanitizeErrorMessage(
                    "Connection readiness interrupted/failed: ${e.message} (0 SQL executed)",
                    secretsToRedact
                ),
                primaryBackendPid = primaryPidRef.get(),
                verifyBackendPid = verifyPidRef.get(),
                cleanupStatus = cs
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
        val searchPathBoundToPublic: Boolean?,
        val tablePresentBefore: Boolean?,
        val tablePresentAfter: Boolean?,
        val tableShapeValid: Boolean?,
        val hasCreatePrivilege: Boolean?,
        val sanitizedDetail: String
    )

    private fun bindTrustedPublicSearchPathBounded(
        conn: Connection,
        activeStmtRef: AtomicReference<PreparedStatement?>,
        remainingSqlMs: () -> Long,
        isCancelledOrPastDeadline: () -> Boolean
    ) {
        val remMs = remainingSqlMs()
        if (remMs <= 0L || isCancelledOrPastDeadline()) {
            throw TimeoutException("SQL stage deadline expired before SET search_path = public, pg_catalog")
        }
        val ps = conn.prepareStatement(SQL_SET_TRUSTED_SEARCH_PATH)
            ?: throw SQLException("Failed to prepare SET search_path = public, pg_catalog (prepareStatement returned null)")
        ps.use { stmt ->
            activeStmtRef.set(stmt)
            val remSetupMs = remainingSqlMs()
            if (remSetupMs <= 0L || isCancelledOrPastDeadline()) {
                activeStmtRef.set(null)
                throw TimeoutException("SQL stage deadline expired before SET search_path setQueryTimeout")
            }
            stmt.queryTimeout = ceil(remSetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
            if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                activeStmtRef.set(null)
                throw TimeoutException("SQL stage deadline expired directly before SET search_path executeUpdate")
            }
            stmt.executeUpdate()
            activeStmtRef.set(null)
        }
    }

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
        val searchPathBoundRef = AtomicReference<Boolean?>(null)
        val tablePresentBeforeRef = AtomicReference<Boolean?>(null)
        val tableShapeValidRef = AtomicReference<Boolean?>(null)
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
                        searchPathBoundToPublic = null,
                        tablePresentBefore = null,
                        tablePresentAfter = null,
                        tableShapeValid = null,
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

                // Bind exact trusted search_path (public, pg_catalog) on primaryConn and verifyConn BEFORE any check or DDL!
                bindTrustedPublicSearchPathBounded(
                    conn = readyConns.primaryConn,
                    activeStmtRef = activeStmtRef,
                    remainingSqlMs = ::remainingSqlMs,
                    isCancelledOrPastDeadline = ::isCancelledOrPastDeadline
                )
                if (readyConns.verifyConn != null) {
                    bindTrustedPublicSearchPathBounded(
                        conn = readyConns.verifyConn,
                        activeStmtRef = activeStmtRef,
                        remainingSqlMs = ::remainingSqlMs,
                        isCancelledOrPastDeadline = ::isCancelledOrPastDeadline
                    )
                }

                if (options.dryRun) {
                    // Default Dry-Run Mode: read-only check only, 0 DDL statements executed
                    if (isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = null,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            tableShapeValid = null,
                            hasCreatePrivilege = null,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before dry-run prepareStatement (0 DDL executed)"
                        )
                    }

                    val dryRunPs = readyConns.primaryConn.prepareStatement(SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE.trimIndent())
                        ?: throw SQLException("Dry-run schema check failed: prepareStatement returned null")
                    val contractRow = dryRunPs.use { ps ->
                        activeStmtRef.set(ps)
                        val remBeforeSetupMs = remainingSqlMs()
                        if (remBeforeSetupMs <= 0L || isCancelledOrPastDeadline()) {
                            activeStmtRef.set(null)
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                ddlExecutedAndVerified = false,
                                searchPathBoundToPublic = null,
                                tablePresentBefore = null,
                                tablePresentAfter = null,
                                tableShapeValid = null,
                                hasCreatePrivilege = null,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before dry-run setQueryTimeout (0 DDL executed)"
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
                                searchPathBoundToPublic = null,
                                tablePresentBefore = null,
                                tablePresentAfter = null,
                                tableShapeValid = null,
                                hasCreatePrivilege = null,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired after statement setup directly before dry-run executeQuery (0 DDL executed)"
                            )
                        }
                        val row = ps.executeQuery().use { rs ->
                            readTableContractRowStrict(rs, requireCanCreate = true)
                        }
                        activeStmtRef.set(null)
                        row
                    }

                    val present = contractRow.tablePresent
                    val canCreate = requireNotNull(contractRow.canCreate)
                    val currentSchema = contractRow.currentSchema
                    val shapeValid = contractRow.tableShapeValid
                    val schemaIsPublic = currentSchema.equals("public", ignoreCase = true)
                    searchPathBoundRef.set(schemaIsPublic)
                    tablePresentBeforeRef.set(present)
                    tableShapeValidRef.set(shapeValid)
                    hasCreatePrivilegeRef.set(canCreate)

                    if (isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = schemaIsPublic,
                            tablePresentBefore = present,
                            tablePresentAfter = present,
                            tableShapeValid = shapeValid,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired upon dry-run completion (0 DDL executed)"
                        )
                    }

                    if (!schemaIsPublic) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = false,
                            tablePresentBefore = present,
                            tablePresentAfter = present,
                            tableShapeValid = shapeValid,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "Failed to bind search_path to 'public' (current_schema='$currentSchema'); aborting fail-closed"
                        )
                    }

                    if (present && shapeValid != true) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = true,
                            tablePresentBefore = true,
                            tablePresentAfter = true,
                            tableShapeValid = false,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "Existing table public.player_riot_identities does not match expected columns/types/PRIMARY KEY/non-blank CHECK constraint; failing closed"
                        )
                    }

                    return@Callable SqlStageOutcome(
                        exitCode = 0,
                        outcome = SetupOutcome.DRY_RUN_READINESS_AND_SCHEMA_CHECK_OK,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = true,
                        tablePresentBefore = present,
                        tablePresentAfter = present,
                        tableShapeValid = if (present) true else null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Dry-run readiness and schema check completed within ${effectiveSqlBudgetMs}ms SQL fence (0 DDL executed)"
                    )
                }

                // =========================================================================
                // Apply-DDL Mode:
                // 1. Pre-check table_present, table_shape_valid & can_create on read-only verifyConn
                //    (fail closed BEFORE DDL if existing public.player_riot_identities has wrong shape!)
                // 2. Enforce session lock_timeout + statement_timeout + search_path on primaryConn
                // 3. Execute CREATE TABLE IF NOT EXISTS on primaryConn with pre-execute fence
                // 4. Verify table_present == true AND table_shape_valid == true on separate verifyConn
                // =========================================================================
                val verifyConn = requireNotNull(readyConns.verifyConn) {
                    "Separate verification connection required for --apply-ddl=true"
                }

                val preCheckPs = verifyConn.prepareStatement(SQL_CHECK_TABLE_AND_CREATE_PRIVILEGE.trimIndent())
                    ?: throw SQLException("Pre-DDL schema check failed: prepareStatement returned null")
                val preCheckRow = preCheckPs.use { ps ->
                    activeStmtRef.set(ps)
                    val remPreCheckMs = remainingSqlMs()
                    if (remPreCheckMs <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = null,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            tableShapeValid = null,
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
                            searchPathBoundToPublic = null,
                            tablePresentBefore = null,
                            tablePresentAfter = null,
                            tableShapeValid = null,
                            hasCreatePrivilege = null,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired directly before pre-DDL schema check executeQuery (0 DDL executed)"
                        )
                    }
                    val row = ps.executeQuery().use { rs ->
                        readTableContractRowStrict(rs, requireCanCreate = true)
                    }
                    activeStmtRef.set(null)
                    row
                }

                val presentBefore = preCheckRow.tablePresent
                val canCreate = requireNotNull(preCheckRow.canCreate)
                val currentSchemaBefore = preCheckRow.currentSchema
                val shapeValidBefore = preCheckRow.tableShapeValid
                val schemaBoundBefore = currentSchemaBefore.equals("public", ignoreCase = true)
                searchPathBoundRef.set(schemaBoundBefore)
                tablePresentBeforeRef.set(presentBefore)
                tableShapeValidRef.set(shapeValidBefore)
                hasCreatePrivilegeRef.set(canCreate)

                if (!schemaBoundBefore) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = false,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = presentBefore,
                        tableShapeValid = shapeValidBefore,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Pre-DDL check failed: current_schema() is '$currentSchemaBefore' instead of trusted 'public' (0 DDL executed)"
                    )
                }

                // Fail closed BEFORE DDL if public.player_riot_identities already exists with an incompatible shape!
                if (presentBefore && shapeValidBefore != true) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_EXISTING_TABLE_INCOMPATIBLE_SCHEMA,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = true,
                        tablePresentBefore = true,
                        tablePresentAfter = true,
                        tableShapeValid = false,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Existing table public.player_riot_identities has incompatible schema (wrong columns, types, nullability, PRIMARY KEY, or non-blank CHECK constraint); aborting fail-closed with 0 DDL executed"
                    )
                }

                // On real PostgreSQL connections, apply server-side search_path, lock_timeout, and statement_timeout
                val isRealPg = runCatching { readyConns.primaryConn.isWrapperFor(PGConnection::class.java) }.getOrDefault(false)
                if (isRealPg) {
                    val remLockSetupMs = remainingSqlMs()
                    if (remLockSetupMs <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = true,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            tableShapeValid = if (presentBefore) true else null,
                            hasCreatePrivilege = canCreate,
                            sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before SET lock_timeout/statement_timeout (0 DDL executed)"
                        )
                    }
                    val stmtTimeoutMs = minOf(options.sqlTimeoutMs, remLockSetupMs).coerceAtLeast(1L)
                    val effectiveLockMs = minOf(options.lockTimeoutMs, stmtTimeoutMs).coerceAtLeast(1L)
                    val st = readyConns.primaryConn.createStatement()
                        ?: throw SQLException("Failed to createStatement for SET lock_timeout/statement_timeout")
                    st.use { stmt ->
                        stmt.queryTimeout = ceil(remLockSetupMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                                ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                                ddlExecutedAndVerified = false,
                                searchPathBoundToPublic = true,
                                tablePresentBefore = presentBefore,
                                tablePresentAfter = presentBefore,
                                tableShapeValid = if (presentBefore) true else null,
                                hasCreatePrivilege = canCreate,
                                sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired directly before SET lock_timeout/statement_timeout (0 DDL executed)"
                            )
                        }
                        stmt.execute(
                            "SET search_path = public, pg_catalog; SET lock_timeout = '${effectiveLockMs}ms'; SET statement_timeout = '${stmtTimeoutMs}ms'"
                        )
                    }
                }

                if (remainingSqlMs() <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                        ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = true,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = presentBefore,
                        tableShapeValid = if (presentBefore) true else null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before DDL prepareStatement (0 DDL executed)"
                    )
                }

                val rawDdlPs = readyConns.primaryConn.prepareStatement(validatedDdlSql)
                    ?: throw SQLException("DDL prepareStatement returned null (0 DDL executed)")
                rawDdlPs.use { ddlPs ->
                    activeStmtRef.set(ddlPs)
                    val remDdlSetupMs = remainingSqlMs()
                    if (remDdlSetupMs <= 0L || isCancelledOrPastDeadline()) {
                        activeStmtRef.set(null)
                        return@Callable SqlStageOutcome(
                            exitCode = 3,
                            outcome = SetupOutcome.ABORTED_SQL_FENCE_BEFORE_EXECUTE_NO_SQL,
                            ddlCommitStatus = DdlCommitStatus.NOT_STARTED_ZERO_SQL,
                            ddlExecutedAndVerified = false,
                            searchPathBoundToPublic = true,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            tableShapeValid = if (presentBefore) true else null,
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
                            searchPathBoundToPublic = true,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = presentBefore,
                            tableShapeValid = if (presentBefore) true else null,
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
                            searchPathBoundToPublic = true,
                            tablePresentBefore = presentBefore,
                            tablePresentAfter = null,
                            tableShapeValid = null,
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
                        searchPathBoundToPublic = true,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        tableShapeValid = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "DDL executeUpdate completed but SQL stage deadline (${effectiveSqlBudgetMs}ms) expired before separate-connection post-commit verification read"
                    )
                }

                val rawVerifyPs = verifyConn.prepareStatement(SQL_VERIFY_TABLE_PRESENT.trimIndent())
                    ?: return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = true,
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        tableShapeValid = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Post-commit verification failed: prepareStatement returned null on separate connection"
                    )
                val postVerifyRow = try {
                    rawVerifyPs.use { verifyPs ->
                        activeStmtRef.set(verifyPs)
                        val remVerifySetupMs = remainingSqlMs()
                        if (remVerifySetupMs <= 0L || isCancelledOrPastDeadline()) {
                            activeStmtRef.set(null)
                            return@Callable SqlStageOutcome(
                                exitCode = 3,
                                outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                                ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                                ddlExecutedAndVerified = false,
                                searchPathBoundToPublic = true,
                                tablePresentBefore = presentBefore,
                                tablePresentAfter = null,
                                tableShapeValid = null,
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
                                searchPathBoundToPublic = true,
                                tablePresentBefore = presentBefore,
                                tablePresentAfter = null,
                                tableShapeValid = null,
                                hasCreatePrivilege = canCreate,
                                sanitizedDetail = "DDL executeUpdate completed but deadline expired directly before verification executeQuery"
                            )
                        }
                        val row = verifyPs.executeQuery().use { rs ->
                            readTableContractRowStrict(rs, requireCanCreate = false)
                        }
                        activeStmtRef.set(null)
                        row
                    }
                } catch (verifyEx: Exception) {
                    activeStmtRef.set(null)
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = searchPathBoundRef.get(),
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        tableShapeValid = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = sanitizeErrorMessage(
                            "Post-commit verification failed (${verifyEx.javaClass.simpleName}): ${verifyEx.message}",
                            secretsToRedact
                        )
                    )
                }

                val presentAfter = postVerifyRow.tablePresent
                val currentSchemaAfter = postVerifyRow.currentSchema
                val shapeValidAfter = postVerifyRow.tableShapeValid == true
                tableShapeValidRef.set(postVerifyRow.tableShapeValid)

                if (isCancelledOrPastDeadline()) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = currentSchemaAfter.equals("public", ignoreCase = true),
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = null,
                        tableShapeValid = null,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "SQL stage deadline (${effectiveSqlBudgetMs}ms) expired during post-commit verification read"
                    )
                }

                if (!presentAfter || !shapeValidAfter || !currentSchemaAfter.equals("public", ignoreCase = true)) {
                    return@Callable SqlStageOutcome(
                        exitCode = 3,
                        outcome = SetupOutcome.ABORTED_VERIFICATION_FAILED_UNKNOWN_COMMIT,
                        ddlCommitStatus = DdlCommitStatus.UNKNOWN_COMMIT_RECHECK_REQUIRED,
                        ddlExecutedAndVerified = false,
                        searchPathBoundToPublic = currentSchemaAfter.equals("public", ignoreCase = true),
                        tablePresentBefore = presentBefore,
                        tablePresentAfter = presentAfter,
                        tableShapeValid = postVerifyRow.tableShapeValid,
                        hasCreatePrivilege = canCreate,
                        sanitizedDetail = "Post-commit verification on separate connection (PID=${readyConns.verifyBackendPid}) failed: table_present=$presentAfter, table_shape_valid=${postVerifyRow.tableShapeValid}, current_schema='$currentSchemaAfter'"
                    )
                }

                SqlStageOutcome(
                    exitCode = 0,
                    outcome = SetupOutcome.APPLIED_AND_VERIFIED_DDL,
                    ddlCommitStatus = DdlCommitStatus.COMMITTED_AND_VERIFIED,
                    ddlExecutedAndVerified = true,
                    searchPathBoundToPublic = true,
                    tablePresentBefore = presentBefore,
                    tablePresentAfter = true,
                    tableShapeValid = true,
                    hasCreatePrivilege = canCreate,
                    sanitizedDetail = "Applied reviewed additive migration ${JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH} on PID=${readyConns.primaryBackendPid} and verified full public.player_riot_identities schema contract on separate connection PID=${readyConns.verifyBackendPid} within ${effectiveSqlBudgetMs}ms SQL fence"
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
                    searchPathBoundToPublic = searchPathBoundRef.get(),
                    tablePresentBefore = tablePresentBeforeRef.get(),
                    tablePresentAfter = null,
                    tableShapeValid = tableShapeValidRef.get(),
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
                searchPathBoundToPublic = searchPathBoundRef.get(),
                tablePresentBefore = tablePresentBeforeRef.get(),
                tablePresentAfter = null,
                tableShapeValid = tableShapeValidRef.get(),
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
                searchPathBoundToPublic = searchPathBoundRef.get(),
                tablePresentBefore = tablePresentBeforeRef.get(),
                tablePresentAfter = null,
                tableShapeValid = tableShapeValidRef.get(),
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

    internal fun closeConnectionsBounded(
        c1: Connection?,
        c2: Connection?,
        netTimeoutExec: ExecutorService?,
        awaitTimeoutMs: Long,
        monotonicClock: MonotonicClock,
        forceAbortFirst: Boolean = false,
        secretsToRedact: List<String> = emptyList()
    ): CleanupStatus {
        if (c1 == null && c2 == null) {
            runCatching { netTimeoutExec?.shutdown() }
            return CleanupStatus(
                connectionsClosedCleanly = true,
                cleanupTimedOut = false,
                cleanupElapsedMs = 0L
            )
        }

        val startMonoMs = monotonicClock.nowMs()
        val doneLatch = CountDownLatch(1)
        val closeErrorRef = AtomicReference<String?>(null)

        val closeThread = Thread({
            try {
                if (forceAbortFirst && netTimeoutExec != null) {
                    runCatching { c1?.abort(netTimeoutExec) }
                    runCatching { if (c2 != null && c2 !== c1) c2.abort(netTimeoutExec) }
                }
                val err1 = runCatching { c1?.close() }.exceptionOrNull()
                val err2 = runCatching { if (c2 != null && c2 !== c1) c2.close() }.exceptionOrNull()
                val firstErr = err1 ?: err2
                if (firstErr != null) {
                    closeErrorRef.set(
                        sanitizeErrorMessage("${firstErr.javaClass.simpleName}: ${firstErr.message}", secretsToRedact)
                    )
                    if (netTimeoutExec != null) {
                        runCatching { c1?.abort(netTimeoutExec) }
                        runCatching { if (c2 != null && c2 !== c1) c2.abort(netTimeoutExec) }
                    }
                }
            } finally {
                runCatching { netTimeoutExec?.shutdown() }
                doneLatch.countDown()
            }
        }, "offprod-setup-conn-close").apply {
            isDaemon = true
            start()
        }

        val boundedWaitMs = awaitTimeoutMs.coerceIn(1L, MAX_CLEANUP_TIMEOUT_MS)
        val finishedInTime = try {
            doneLatch.await(boundedWaitMs, TimeUnit.MILLISECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
        val elapsedMs = (monotonicClock.nowMs() - startMonoMs).coerceAtLeast(0L)

        if (!finishedInTime) {
            closeThread.interrupt()
            Thread({
                if (netTimeoutExec != null) {
                    runCatching { c1?.abort(netTimeoutExec) }
                    runCatching { if (c2 != null && c2 !== c1) c2.abort(netTimeoutExec) }
                }
                runCatching { netTimeoutExec?.shutdownNow() }
            }, "offprod-setup-conn-close-fallback-abort").apply {
                isDaemon = true
                start()
            }
            return CleanupStatus(
                connectionsClosedCleanly = false,
                cleanupTimedOut = true,
                cleanupElapsedMs = elapsedMs,
                cleanupError = "Connection close did not finish within ${boundedWaitMs}ms bounded cleanup window"
            )
        }

        val err = closeErrorRef.get()
        return CleanupStatus(
            connectionsClosedCleanly = err == null,
            cleanupTimedOut = false,
            cleanupElapsedMs = elapsedMs,
            cleanupError = err
        )
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
            this.options = "-c search_path=public,pg_catalog -c lock_timeout=${setupOptions.lockTimeoutMs}ms -c statement_timeout=${setupOptions.sqlTimeoutMs}ms"
        }
    }
}
