package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.CompletionRunReport
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ExecutionOptions
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.KeyCoordinationMode
import org.postgresql.PGConnection
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.io.PrintWriter
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.Instant
import java.util.Collections
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.math.ceil
import kotlin.system.exitProcess

/**
 * Isolated pre-warmed 2-session completion CLI wrapper around [OffProdChallengerIdentityCompletionCli.runCli]
 * that keeps all `fef2ed6` source files 100% unchanged while eliminating per-candidate cold WAN connection
 * establishment inside the `<= 2,500ms` per-candidate DB deadline fence.
 *
 * Key Architectural & Safety Guarantees:
 * 1. **Zero DDL in Completion Wrapper**:
 *    - Rejects `--apply-additive-ddl=true` during option pre-validation (`exitCode = 2`, `0` connections opened).
 *      DDL is handled exclusively by [OffProdAdditiveIdentitySchemaSetupCli].
 * 2. **Bounded Readiness (`<= 10,000ms`) Counted Inside the `<= 180,000ms` Total Budget**:
 *    - Pre-establishes at most `2` physical PostgreSQL connections (`1` in `--dry-run=true` mode; `2` in
 *      `--dry-run=false --allow-durable-writes=true` mode) within `readinessTimeoutMs` (`<= 10,000ms`),
 *      binds `SET search_path = public, pg_catalog` on each, verifies `writeBackendPid != readVerifyBackendPid`
 *      via `SELECT pg_backend_pid()`, and validates the full `public.player_riot_identities` schema contract.
 *    - Deducts `readinessElapsedMs` from `maxWallTimeMs` (`<= 180,000ms`) before invoking
 *      `OffProdChallengerIdentityCompletionCli.runCli(..., dataSourceOverride = twoSessionDataSource)`.
 * 3. **Deterministic Late-Binding Session Routing (Never Guess Borrow Order at `getConnection()`)**:
 *    - Because `JdbcIdentityCompletionPersistenceAdapter` calls `dataSource.connection` *before* calling
 *      `conn.isReadOnly = readOnly`, `getConnection()` returns an `UNBOUND` [Connection] lease proxy.
 *    - The lease binds deterministically when `setReadOnly(false)` / write SQL (`WRITE_SESSION`, `writeBackendPid`)
 *      or `setReadOnly(true)` / read SQL (`READ_VERIFY_SESSION`, `readVerifyBackendPid`) is invoked.
 *    - Even if two leases are borrowed in arbitrary order before `setReadOnly(...)` is called, each lease
 *      binds to the exact matching physical session.
 * 4. **Abort / Poison Fencing & Hard Cap of `2` Physical Connections**:
 *    - Never opens a 3rd physical connection (`physicalConnectionsOpened <= 2`).
 *    - If `abort(executor)` or `ps.cancel()` is invoked (by any `fef2ed6` timeout/cancellation fence), or if
 *      a non-missing-table `SQLException` occurs on a session, the pool is immediately marked `poisoned = true`,
 *      underlying physical connections are aborted/closed, and any subsequent reuse is rejected fail-closed.
 * 5. **Bounded Awaited Cleanup in `finally`**:
 *    - Awaits physical connection `close()`/`abort()` up to `cleanupTimeoutMs` before returning so standalone
 *      `exitProcess` cannot exit before cleanup finishes, and records truthful cleanup status in the report.
 */
object OffProdPrewarmedChallengerIdentityCompletionCli {

    const val MAX_READINESS_TIMEOUT_MS: Long = 10_000L
    const val DEFAULT_READINESS_TIMEOUT_MS: Long = 10_000L

    const val MAX_CLEANUP_TIMEOUT_MS: Long = 5_000L
    const val DEFAULT_CLEANUP_TIMEOUT_MS: Long = 1_000L

    const val MAX_TOTAL_WALL_TIME_MS: Long = 180_000L
    const val DEFAULT_TOTAL_WALL_TIME_MS: Long = OffProdChallengerIdentityCompletionTool.DEFAULT_MAX_WALL_TIME_MS

    const val MAX_DB_TIMEOUT_MS: Long = 2_500L
    const val DEFAULT_DB_TIMEOUT_MS: Long = OffProdChallengerIdentityCompletionTool.DEFAULT_DB_OPERATION_TIMEOUT_MS

    const val DEFAULT_LOCK_TIMEOUT_MS: Long = 2_000L
    const val MAX_PHYSICAL_CONNECTIONS: Int = 2
    const val MAX_STANDALONE_JVM_HEAP_BYTES: Long = 256L * 1024L * 1024L

    val ALLOWED_FLAGS: Set<String> = setOf(
        "manifest-file",
        "dry-run",
        "network-enabled",
        "allow-durable-writes",
        "allow-indexed-players-blank-update",
        "apply-additive-ddl",
        "allow-loopback-http-for-testing",
        "max-http-attempts",
        "max-wall-time-ms",
        "readiness-timeout-ms",
        "cleanup-timeout-ms",
        "db-timeout-ms",
        "http-connect-timeout-ms",
        "http-read-timeout-ms",
        "http-call-deadline-ms",
        "max-body-bytes",
        "key-coordination-mode",
        "coordination-attestation",
        "cluster-url-override",
        "riot-api-key",
        "enforce-max-heap-256m",
        "jdbc-url",
        "db-host",
        "db-port",
        "db-name",
        "db-user",
        "db-pass",
        "output-report-file",
        "output-remaining-manifest-file",
        "output-wrapper-report-file"
    )

    enum class SessionRole {
        WRITE_SESSION,
        READ_VERIFY_SESSION
    }

    enum class SqlOperationKind {
        PRECHECK_INDEXED_PLAYERS_SELECT,
        PRECHECK_IDENTITY_SELECT,
        CANDIDATE_INDEXED_PLAYERS_UPDATE,
        CANDIDATE_IDENTITY_UPSERT,
        POST_COMMIT_VERIFY_IDENTITY_SELECT,
        POST_COMMIT_VERIFY_INDEXED_SELECT,
        OTHER_SQL
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class SqlSessionAuditEntry(
        val sequenceNumber: Int = 0,
        val sessionRole: SessionRole = SessionRole.READ_VERIFY_SESSION,
        val operationKind: SqlOperationKind = SqlOperationKind.OTHER_SQL,
        val candidatePuuid: String? = null,
        val backendPid: Int = 0,
        val leaseIdentityHash: Int = 0
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class PrewarmedCompletionExecutionReport(
        val exitCode: Int = 0,
        val dryRun: Boolean = true,
        val networkEnabled: Boolean = false,
        val allowDurableWrites: Boolean = false,
        val physicalConnectionsOpened: Int = 0,
        val maxPhysicalConnectionsAllowed: Int = MAX_PHYSICAL_CONNECTIONS,
        val writeBackendPid: Int? = null,
        val readVerifyBackendPid: Int? = null,
        val distinctWriteAndVerifyPidsVerified: Boolean = false,
        val searchPathBoundToPublic: Boolean? = null,
        val identityTablePresentAtReadiness: Boolean? = null,
        val identityTableShapeValidAtReadiness: Boolean? = null,
        val poolPoisoned: Boolean = false,
        val poolPoisonReason: String? = null,
        val connectionsClosedCleanly: Boolean = true,
        val cleanupTimedOut: Boolean = false,
        val cleanupElapsedMs: Long = 0L,
        val readinessBudgetMs: Long = 0L,
        val readinessElapsedMs: Long = 0L,
        val remainingRunBudgetMs: Long = 0L,
        val runStageElapsedMs: Long = 0L,
        val totalWallBudgetMs: Long = 0L,
        val totalWallElapsedMs: Long = 0L,
        val jvmMaxHeapBytes: Long = 0L,
        val jvmUsedHeapBytesPeak: Long = 0L,
        val sqlSessionAudit: List<SqlSessionAuditEntry> = emptyList(),
        val completionReport: CompletionRunReport? = null,
        val sanitizedDetail: String = "",
        val completedAtUtc: Instant = Instant.EPOCH
    )

    data class PrewarmedCliResult(
        val exitCode: Int,
        val wrapperReport: PrewarmedCompletionExecutionReport,
        val cliResult: OffProdChallengerIdentityCompletionCli.CliExecutionResult?,
        val outputWrapperReportPath: String?
    )

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    @JvmStatic
    fun main(args: Array<String>) {
        val result = runPrewarmedCli(args = args.toList())
        if (result.exitCode != 0) {
            System.err.println(
                "[OffProdPrewarmedChallengerIdentityCompletionCli] ERROR (exit=${result.exitCode}, " +
                    "connsOpened=${result.wrapperReport.physicalConnectionsOpened}, " +
                    "writePid=${result.wrapperReport.writeBackendPid}, readVerifyPid=${result.wrapperReport.readVerifyBackendPid}, " +
                    "poisoned=${result.wrapperReport.poolPoisoned}, closedCleanly=${result.wrapperReport.connectionsClosedCleanly}): " +
                    result.wrapperReport.sanitizedDetail
            )
        } else {
            println(
                "[OffProdPrewarmedChallengerIdentityCompletionCli] OK: exit=${result.exitCode}, " +
                    "outcome=${result.wrapperReport.completionReport?.outcome}, " +
                    "connsOpened=${result.wrapperReport.physicalConnectionsOpened}, " +
                    "writePid=${result.wrapperReport.writeBackendPid}, readVerifyPid=${result.wrapperReport.readVerifyBackendPid}, " +
                    "closedCleanly=${result.wrapperReport.connectionsClosedCleanly}, " +
                    "readinessElapsedMs=${result.wrapperReport.readinessElapsedMs}, " +
                    "runStageElapsedMs=${result.wrapperReport.runStageElapsedMs}, " +
                    "totalElapsedMs=${result.wrapperReport.totalWallElapsedMs}"
            )
        }
        exitProcess(result.exitCode)
    }

    fun runPrewarmedCli(
        args: List<String>,
        env: Map<String, String> = System.getenv(),
        physicalDataSourceOverride: DataSource? = null,
        httpTransportOverride: BoundedAccountV1Transport? = null,
        monotonicClock: MonotonicClock = SystemMonotonicClock,
        wallClock: WallClock = SystemUtcWallClock,
        maxHeapBytesProvider: () -> Long = { Runtime.getRuntime().maxMemory() }
    ): PrewarmedCliResult {
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
            return buildInvalidResult(
                errorMessage = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets),
                outputWrapperReportFile = null,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes
            )
        }

        flags["db-pass"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }
        flags["riot-api-key"]?.takeIf { it.isNotBlank() }?.let { candidateSecrets.add(it) }

        val outputWrapperReportFile = flags["output-wrapper-report-file"]?.trim()?.takeIf { it.isNotEmpty() }?.let { File(it) }

        val dryRun: Boolean
        val networkEnabled: Boolean
        val allowDurableWrites: Boolean
        val allowIndexedPlayersBlankUpdate: Boolean
        val applyAdditiveDdl: Boolean
        val allowLoopbackHttpForTesting: Boolean
        val enforceMaxHeap256m: Boolean
        val maxHttpAttempts: Int
        val maxWallTimeMs: Long
        val readinessTimeoutMs: Long
        val cleanupTimeoutMs: Long
        val dbTimeoutMs: Long
        val connectTimeoutMs: Int
        val readTimeoutMs: Int
        val callDeadlineMs: Long
        val maxBodyBytes: Int

        try {
            dryRun = parseBooleanFlag(flags, "dry-run", true)
            networkEnabled = parseBooleanFlag(flags, "network-enabled", false)
            allowDurableWrites = parseBooleanFlag(flags, "allow-durable-writes", false)
            allowIndexedPlayersBlankUpdate = parseBooleanFlag(flags, "allow-indexed-players-blank-update", false)
            applyAdditiveDdl = parseBooleanFlag(flags, "apply-additive-ddl", false)
            allowLoopbackHttpForTesting = parseBooleanFlag(flags, "allow-loopback-http-for-testing", false)
            enforceMaxHeap256m = parseBooleanFlag(flags, "enforce-max-heap-256m", true)

            maxHttpAttempts = parseIntFlag(
                flags,
                "max-http-attempts",
                OffProdChallengerIdentityCompletionTool.MAX_ALLOWED_HTTP_ATTEMPTS
            )
            maxWallTimeMs = parseLongFlag(
                flags,
                "max-wall-time-ms",
                DEFAULT_TOTAL_WALL_TIME_MS
            )
            readinessTimeoutMs = parseLongFlag(
                flags,
                "readiness-timeout-ms",
                DEFAULT_READINESS_TIMEOUT_MS
            )
            cleanupTimeoutMs = parseLongFlag(
                flags,
                "cleanup-timeout-ms",
                DEFAULT_CLEANUP_TIMEOUT_MS
            )
            dbTimeoutMs = parseLongFlag(
                flags,
                "db-timeout-ms",
                DEFAULT_DB_TIMEOUT_MS
            )
            connectTimeoutMs = parseIntFlag(
                flags,
                "http-connect-timeout-ms",
                OffProdChallengerIdentityCompletionTool.DEFAULT_CONNECT_TIMEOUT_MS
            )
            readTimeoutMs = parseIntFlag(
                flags,
                "http-read-timeout-ms",
                OffProdChallengerIdentityCompletionTool.DEFAULT_READ_TIMEOUT_MS
            )
            callDeadlineMs = parseLongFlag(
                flags,
                "http-call-deadline-ms",
                OffProdChallengerIdentityCompletionTool.DEFAULT_CALL_DEADLINE_MS
            )
            maxBodyBytes = parseIntFlag(
                flags,
                "max-body-bytes",
                OffProdChallengerIdentityCompletionTool.MAX_RESPONSE_BODY_BYTES
            )

            require(!applyAdditiveDdl) {
                "--apply-additive-ddl=true is not permitted in OffProdPrewarmedChallengerIdentityCompletionCli; run OffProdAdditiveIdentitySchemaSetupCli separately for DDL"
            }
            require(readinessTimeoutMs in 1L..MAX_READINESS_TIMEOUT_MS) {
                "readinessTimeoutMs ($readinessTimeoutMs) must be in 1..$MAX_READINESS_TIMEOUT_MS"
            }
            require(cleanupTimeoutMs in 1L..MAX_CLEANUP_TIMEOUT_MS) {
                "cleanupTimeoutMs ($cleanupTimeoutMs) must be in 1..$MAX_CLEANUP_TIMEOUT_MS"
            }
            require(maxWallTimeMs in 1L..MAX_TOTAL_WALL_TIME_MS) {
                "maxWallTimeMs ($maxWallTimeMs) must be in 1..$MAX_TOTAL_WALL_TIME_MS"
            }
            require(dbTimeoutMs in 1L..MAX_DB_TIMEOUT_MS) {
                "dbTimeoutMs ($dbTimeoutMs) must be in 1..$MAX_DB_TIMEOUT_MS"
            }
        } catch (e: IllegalArgumentException) {
            return buildInvalidResult(
                errorMessage = "Invalid CLI flag: ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes
            )
        }

        if (enforceMaxHeap256m && maxHeapBytes > MAX_STANDALONE_JVM_HEAP_BYTES) {
            return buildInvalidResult(
                errorMessage = "Standalone JVM max heap ($maxHeapBytes bytes) exceeds 256 MiB ($MAX_STANDALONE_JVM_HEAP_BYTES bytes) cap; pass -Xmx128m or -Xmx256m",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }

        val manifestPath = flags["manifest-file"]?.trim().orEmpty()
        if (manifestPath.isEmpty()) {
            return buildInvalidResult(
                errorMessage = "Missing required argument --manifest-file=<path>",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }
        val manifestFile = File(manifestPath)
        if (!manifestFile.isFile) {
            return buildInvalidResult(
                errorMessage = "Manifest file does not exist: ${manifestFile.name}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }
        val manifest = try {
            OffProdChallengerIdentityCompletionCli.readSnapshotManifest(manifestFile)
        } catch (e: Exception) {
            return buildInvalidResult(
                errorMessage = "Invalid manifest file '${manifestFile.name}': ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }

        val coordinationMode = try {
            flags["key-coordination-mode"]?.trim()?.takeIf { it.isNotEmpty() }
                ?.let { KeyCoordinationMode.valueOf(it.uppercase(Locale.ROOT)) }
                ?: KeyCoordinationMode.UNCOORDINATED
        } catch (e: IllegalArgumentException) {
            return buildInvalidResult(
                errorMessage = "Invalid --key-coordination-mode: ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }
        val coordinationAttestation = flags["coordination-attestation"]?.trim().orEmpty()

        val baseUrlByCluster = try {
            parseClusterUrlOverrides(flags["cluster-url-override"])
        } catch (e: IllegalArgumentException) {
            return buildInvalidResult(
                errorMessage = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets),
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }

        try {
            ExecutionOptions(
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                enforceAbsentManifestScope = !allowIndexedPlayersBlankUpdate,
                keyCoordinationMode = coordinationMode,
                coordinationAttestation = coordinationAttestation,
                maxTotalHttpAttempts = maxHttpAttempts,
                maxWallTimeMs = maxWallTimeMs,
                dbOperationTimeoutMs = dbTimeoutMs,
                connectTimeoutMs = connectTimeoutMs,
                readTimeoutMs = readTimeoutMs,
                callDeadlineMs = callDeadlineMs,
                maxBodyBytes = maxBodyBytes,
                allowLoopbackHttpForTesting = allowLoopbackHttpForTesting,
                baseUrlByCluster = baseUrlByCluster
            )
        } catch (e: IllegalArgumentException) {
            return buildInvalidResult(
                errorMessage = "Invalid execution options: ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }

        val apiKey = flags["riot-api-key"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["OFFPROD_RIOT_API_KEY"]?.trim().orEmpty()

        if (!dryRun || networkEnabled) {
            if (coordinationMode == KeyCoordinationMode.UNCOORDINATED || coordinationAttestation.isBlank()) {
                return buildInvalidResult(
                    errorMessage = "Non-dry-run or network-enabled completion requires valid --key-coordination-mode and non-blank --coordination-attestation before opening DB connections",
                    outputWrapperReportFile = outputWrapperReportFile,
                    processStartMonoMs = processStartMonoMs,
                    monotonicClock = monotonicClock,
                    wallClock = wallClock,
                    maxHeapBytes = maxHeapBytes,
                    peakUsedHeapBytes = peakUsedHeapBytes,
                    dryRun = dryRun,
                    networkEnabled = networkEnabled,
                    allowDurableWrites = allowDurableWrites,
                    readinessBudgetMs = readinessTimeoutMs,
                    totalWallBudgetMs = maxWallTimeMs
                )
            }
            if (apiKey.isBlank()) {
                return buildInvalidResult(
                    errorMessage = "Network-enabled completion requires non-blank OFFPROD_RIOT_API_KEY (or --riot-api-key) before opening DB connections",
                    outputWrapperReportFile = outputWrapperReportFile,
                    processStartMonoMs = processStartMonoMs,
                    monotonicClock = monotonicClock,
                    wallClock = wallClock,
                    maxHeapBytes = maxHeapBytes,
                    peakUsedHeapBytes = peakUsedHeapBytes,
                    dryRun = dryRun,
                    networkEnabled = networkEnabled,
                    allowDurableWrites = allowDurableWrites,
                    readinessBudgetMs = readinessTimeoutMs,
                    totalWallBudgetMs = maxWallTimeMs
                )
            }
            try {
                for (entry in manifest.entries) {
                    val baseUrl = baseUrlByCluster[entry.routingCluster]?.trim()?.trimEnd('/')
                        ?: throw IllegalArgumentException("Missing base URL for routing cluster '${entry.routingCluster}'")
                    val targetUrl = "$baseUrl/riot/account/v1/accounts/by-puuid/${entry.normalizedPuuid}"
                    OffProdChallengerIdentityCompletionTool.validateTargetUrlAndKeySafety(
                        url = targetUrl,
                        apiKey = apiKey,
                        allowLoopbackHttpForTesting = allowLoopbackHttpForTesting
                    )
                }
            } catch (e: IllegalArgumentException) {
                return buildInvalidResult(
                    errorMessage = "Invalid transport target or API key before DB readiness: ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                    outputWrapperReportFile = outputWrapperReportFile,
                    processStartMonoMs = processStartMonoMs,
                    monotonicClock = monotonicClock,
                    wallClock = wallClock,
                    maxHeapBytes = maxHeapBytes,
                    peakUsedHeapBytes = peakUsedHeapBytes,
                    dryRun = dryRun,
                    networkEnabled = networkEnabled,
                    allowDurableWrites = allowDurableWrites,
                    readinessBudgetMs = readinessTimeoutMs,
                    totalWallBudgetMs = maxWallTimeMs
                )
            }
        }

        val physicalDataSource = try {
            physicalDataSourceOverride ?: buildValidatedDataSourceFromFlagsOrEnv(
                flags = flags,
                env = env,
                readinessTimeoutMs = readinessTimeoutMs,
                dbTimeoutMs = dbTimeoutMs,
                secretsToRedact = candidateSecrets
            )
        } catch (e: Exception) {
            return buildInvalidResult(
                errorMessage = "Failed to configure JDBC DataSource: ${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(e.message, candidateSecrets)}",
                outputWrapperReportFile = outputWrapperReportFile,
                processStartMonoMs = processStartMonoMs,
                monotonicClock = monotonicClock,
                wallClock = wallClock,
                maxHeapBytes = maxHeapBytes,
                peakUsedHeapBytes = peakUsedHeapBytes,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                readinessBudgetMs = readinessTimeoutMs,
                totalWallBudgetMs = maxWallTimeMs
            )
        }

        val totalDeadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWallTimeMs)
        fun totalElapsedMs(): Long = (monotonicClock.nowMs() - processStartMonoMs).coerceAtLeast(0L)
        fun remainingTotalBudgetMs(): Long {
            val remMono = maxWallTimeMs - totalElapsedMs()
            val remNano = TimeUnit.NANOSECONDS.toMillis(totalDeadlineNano - System.nanoTime())
            return minOf(remMono, remNano)
        }

        // =========================================================================
        // STAGE 1: Bounded Pre-Establishment (<= 10,000ms) Counted Inside <= 180,000ms
        // =========================================================================
        val readinessStartMonoMs = monotonicClock.nowMs()
        val effectiveReadinessBudgetMs = minOf(readinessTimeoutMs, remainingTotalBudgetMs()).coerceAtLeast(0L)
        if (effectiveReadinessBudgetMs <= 0L) {
            val totalElapsed = totalElapsedMs()
            val report = PrewarmedCompletionExecutionReport(
                exitCode = 10,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                physicalConnectionsOpened = 0,
                readinessBudgetMs = 0L,
                readinessElapsedMs = 0L,
                remainingRunBudgetMs = 0L,
                runStageElapsedMs = 0L,
                totalWallBudgetMs = maxWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                sanitizedDetail = "Shared global wall-time budget (${maxWallTimeMs}ms) exhausted before DB readiness stage (0 connections opened, 0 HTTP calls)",
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputWrapperReportFile, report)
            return PrewarmedCliResult(exitCode = 10, wrapperReport = report, cliResult = null, outputWrapperReportPath = savedPath)
        }

        val needWriteSession = !dryRun && allowDurableWrites
        val readinessOutcome = establishPrewarmedSessionsBounded(
            physicalDataSource = physicalDataSource,
            needWriteSession = needWriteSession,
            readinessTimeoutMs = effectiveReadinessBudgetMs,
            dbTimeoutMs = dbTimeoutMs,
            cleanupTimeoutMs = cleanupTimeoutMs,
            monotonicClock = monotonicClock,
            secretsToRedact = candidateSecrets
        )
        sampleHeap()
        val readinessEndMonoMs = monotonicClock.nowMs()
        val readinessElapsedMs = (readinessEndMonoMs - readinessStartMonoMs).coerceAtLeast(0L)

        if (readinessOutcome is PrewarmReadinessOutcome.Failure) {
            val totalElapsed = totalElapsedMs()
            val totalExhausted = remainingTotalBudgetMs() <= 0L
            val exitCode = if (totalExhausted) 10 else 3
            val report = PrewarmedCompletionExecutionReport(
                exitCode = exitCode,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                physicalConnectionsOpened = readinessOutcome.physicalConnectionsOpened,
                writeBackendPid = readinessOutcome.writeBackendPid,
                readVerifyBackendPid = readinessOutcome.readVerifyBackendPid,
                distinctWriteAndVerifyPidsVerified = false,
                searchPathBoundToPublic = readinessOutcome.searchPathBoundToPublic,
                identityTablePresentAtReadiness = readinessOutcome.identityTablePresent,
                identityTableShapeValidAtReadiness = readinessOutcome.identityTableShapeValid,
                connectionsClosedCleanly = readinessOutcome.cleanupStatus.connectionsClosedCleanly,
                cleanupTimedOut = readinessOutcome.cleanupStatus.cleanupTimedOut,
                cleanupElapsedMs = readinessOutcome.cleanupStatus.cleanupElapsedMs,
                readinessBudgetMs = effectiveReadinessBudgetMs,
                readinessElapsedMs = readinessElapsedMs,
                remainingRunBudgetMs = remainingTotalBudgetMs().coerceAtLeast(0L),
                runStageElapsedMs = 0L,
                totalWallBudgetMs = maxWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                sanitizedDetail = readinessOutcome.sanitizedReason,
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputWrapperReportFile, report)
            return PrewarmedCliResult(exitCode = exitCode, wrapperReport = report, cliResult = null, outputWrapperReportPath = savedPath)
        }

        val readySessions = readinessOutcome as PrewarmReadinessOutcome.Success

        // Deduct readiness elapsed time (and reserve cleanupTimeoutMs) from the shared <= 180,000ms budget!
        val remainingBeforeRunMs = remainingTotalBudgetMs()
        if (remainingBeforeRunMs <= 0L) {
            val cleanupStatus = OffProdAdditiveIdentitySchemaSetupCli.closeConnectionsBounded(
                c1 = readySessions.physicalWriteConn ?: readySessions.physicalReadVerifyConn,
                c2 = if (readySessions.physicalWriteConn != null) readySessions.physicalReadVerifyConn else null,
                netTimeoutExec = readySessions.netTimeoutExec,
                awaitTimeoutMs = cleanupTimeoutMs,
                monotonicClock = monotonicClock,
                forceAbortFirst = false,
                secretsToRedact = candidateSecrets
            )
            val totalElapsed = totalElapsedMs()
            val report = PrewarmedCompletionExecutionReport(
                exitCode = 10,
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                physicalConnectionsOpened = readySessions.physicalConnectionsOpened,
                writeBackendPid = readySessions.writeBackendPid,
                readVerifyBackendPid = readySessions.readVerifyBackendPid,
                distinctWriteAndVerifyPidsVerified = readySessions.distinctWriteAndVerifyPidsVerified,
                searchPathBoundToPublic = readySessions.searchPathBoundToPublic,
                identityTablePresentAtReadiness = readySessions.identityTablePresent,
                identityTableShapeValidAtReadiness = readySessions.identityTableShapeValid,
                connectionsClosedCleanly = cleanupStatus.connectionsClosedCleanly,
                cleanupTimedOut = cleanupStatus.cleanupTimedOut,
                cleanupElapsedMs = cleanupStatus.cleanupElapsedMs,
                readinessBudgetMs = effectiveReadinessBudgetMs,
                readinessElapsedMs = readinessElapsedMs,
                remainingRunBudgetMs = 0L,
                runStageElapsedMs = 0L,
                totalWallBudgetMs = maxWallTimeMs,
                totalWallElapsedMs = totalElapsed,
                jvmMaxHeapBytes = maxHeapBytes,
                jvmUsedHeapBytesPeak = peakUsedHeapBytes,
                sanitizedDetail = "Shared global wall-time budget (${maxWallTimeMs}ms) exhausted during DB readiness (${readinessElapsedMs}ms); 0 HTTP calls and 0 candidate writes executed",
                completedAtUtc = wallClock.nowInstant()
            )
            val savedPath = writeReportBestEffort(outputWrapperReportFile, report)
            return PrewarmedCliResult(exitCode = 10, wrapperReport = report, cliResult = null, outputWrapperReportPath = savedPath)
        }

        val remainingRunBudgetMs = (remainingBeforeRunMs - cleanupTimeoutMs).coerceAtLeast(1L)

        // =========================================================================
        // STAGE 2: Late-Binding 2-Session DataSource Delegation to Unchanged fef2ed6 CLI
        // =========================================================================
        val lateBindingDataSource = LateBindingTwoSessionDataSource(
            physicalWriteConn = readySessions.physicalWriteConn,
            physicalReadVerifyConn = readySessions.physicalReadVerifyConn,
            writeBackendPid = readySessions.writeBackendPid,
            readVerifyBackendPid = readySessions.readVerifyBackendPid,
            netTimeoutExec = readySessions.netTimeoutExec,
            secretsToRedact = candidateSecrets
        )

        val forwardedArgs = buildForwardedCliArgs(flags, remainingRunBudgetMs)
        val runStartMonoMs = monotonicClock.nowMs()
        val runEndMonoRef = AtomicReference<Long>(runStartMonoMs)
        var cleanupStatus = OffProdAdditiveIdentitySchemaSetupCli.CleanupStatus(
            connectionsClosedCleanly = true,
            cleanupTimedOut = false,
            cleanupElapsedMs = 0L
        )

        val rawCliResult = try {
            val res = OffProdChallengerIdentityCompletionCli.runCli(
                args = forwardedArgs,
                env = env,
                dataSourceOverride = lateBindingDataSource,
                httpTransportOverride = httpTransportOverride,
                monotonicClock = monotonicClock,
                wallClock = wallClock
            )
            runEndMonoRef.set(monotonicClock.nowMs())
            if (res.report?.outcome == OffProdChallengerIdentityCompletionTool.RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT &&
                !lateBindingDataSource.isPoisoned()
            ) {
                lateBindingDataSource.poisonPool(
                    res.report.abortReason ?: "Completion run aborted with ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT"
                )
            }
            res
        } finally {
            if (runEndMonoRef.get() == runStartMonoMs) {
                runEndMonoRef.set(monotonicClock.nowMs())
            }
            lateBindingDataSource.fenceInFlightLeasesOnRunExit()
            cleanupStatus = OffProdAdditiveIdentitySchemaSetupCli.closeConnectionsBounded(
                c1 = readySessions.physicalWriteConn ?: readySessions.physicalReadVerifyConn,
                c2 = if (readySessions.physicalWriteConn != null) readySessions.physicalReadVerifyConn else null,
                netTimeoutExec = readySessions.netTimeoutExec,
                awaitTimeoutMs = cleanupTimeoutMs,
                monotonicClock = monotonicClock,
                forceAbortFirst = lateBindingDataSource.isPoisoned(),
                secretsToRedact = candidateSecrets
            )
        }
        sampleHeap()
        val runStageElapsedMs = (runEndMonoRef.get() - runStartMonoMs).coerceAtLeast(0L)
        val totalElapsed = totalElapsedMs()
        val totalCeilingExceededAfterCleanup = totalElapsed > maxWallTimeMs

        val sanitizedCompletionReport = sanitizeCompletionReport(rawCliResult.report, candidateSecrets)
        val outputReportFile = flags["output-report-file"]?.trim()?.takeIf { it.isNotEmpty() }?.let { File(it) }
        val savedCompletionReportPath = if (outputReportFile != null && sanitizedCompletionReport != null) {
            runCatching {
                outputReportFile.parentFile?.mkdirs()
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputReportFile, sanitizedCompletionReport)
                outputReportFile.absolutePath
            }.getOrNull()
        } else {
            null
        }
        val cliResult = rawCliResult.copy(
            report = sanitizedCompletionReport,
            outputReportPath = savedCompletionReportPath,
            errorMessage = rawCliResult.errorMessage?.let {
                OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(it, candidateSecrets)
            }
        )

        val finalExitCode: Int
        val finalDetail: String
        if (cliResult.exitCode == 0 && !cleanupStatus.connectionsClosedCleanly) {
            finalExitCode = 3
            val rawCleanupErr = cleanupStatus.cleanupError ?: "cleanup did not complete cleanly"
            finalDetail = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(
                "Completion finished but physical connection cleanup failed or timed out: $rawCleanupErr",
                candidateSecrets
            )
        } else if (cliResult.exitCode == 0 && totalCeilingExceededAfterCleanup) {
            finalExitCode = 10
            finalDetail = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(
                "Completion finished but total wall-time including awaited cleanup (${totalElapsed}ms) exceeded shared global budget (${maxWallTimeMs}ms)",
                candidateSecrets
            )
        } else {
            finalExitCode = cliResult.exitCode
            val rawMsg = cliResult.errorMessage
                ?: "Pre-warmed completion finished with outcome=${sanitizedCompletionReport?.outcome}, physicalConnectionsOpened=${readySessions.physicalConnectionsOpened}, writePid=${readySessions.writeBackendPid}, readVerifyPid=${readySessions.readVerifyBackendPid}"
            finalDetail = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(rawMsg, candidateSecrets)
        }

        val sanitizedPoisonReason = lateBindingDataSource.getPoisonReason()?.let {
            OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(it, candidateSecrets)
        }

        val wrapperReport = PrewarmedCompletionExecutionReport(
            exitCode = finalExitCode,
            dryRun = dryRun,
            networkEnabled = networkEnabled,
            allowDurableWrites = allowDurableWrites,
            physicalConnectionsOpened = readySessions.physicalConnectionsOpened,
            maxPhysicalConnectionsAllowed = MAX_PHYSICAL_CONNECTIONS,
            writeBackendPid = readySessions.writeBackendPid,
            readVerifyBackendPid = readySessions.readVerifyBackendPid,
            distinctWriteAndVerifyPidsVerified = readySessions.distinctWriteAndVerifyPidsVerified,
            searchPathBoundToPublic = readySessions.searchPathBoundToPublic,
            identityTablePresentAtReadiness = readySessions.identityTablePresent,
            identityTableShapeValidAtReadiness = readySessions.identityTableShapeValid,
            poolPoisoned = lateBindingDataSource.isPoisoned(),
            poolPoisonReason = sanitizedPoisonReason,
            connectionsClosedCleanly = cleanupStatus.connectionsClosedCleanly,
            cleanupTimedOut = cleanupStatus.cleanupTimedOut,
            cleanupElapsedMs = cleanupStatus.cleanupElapsedMs,
            readinessBudgetMs = effectiveReadinessBudgetMs,
            readinessElapsedMs = readinessElapsedMs,
            remainingRunBudgetMs = remainingRunBudgetMs,
            runStageElapsedMs = runStageElapsedMs,
            totalWallBudgetMs = maxWallTimeMs,
            totalWallElapsedMs = totalElapsed,
            jvmMaxHeapBytes = maxHeapBytes,
            jvmUsedHeapBytesPeak = peakUsedHeapBytes,
            sqlSessionAudit = lateBindingDataSource.getAuditEntries(),
            completionReport = sanitizedCompletionReport,
            sanitizedDetail = finalDetail,
            completedAtUtc = wallClock.nowInstant()
        )
        val savedPath = writeReportBestEffort(outputWrapperReportFile, wrapperReport)
        return PrewarmedCliResult(
            exitCode = finalExitCode,
            wrapperReport = wrapperReport,
            cliResult = cliResult,
            outputWrapperReportPath = savedPath
        )
    }

    internal fun sanitizeCompletionReport(
        report: CompletionRunReport?,
        secretsToRedact: List<String>
    ): CompletionRunReport? {
        if (report == null) return null
        return report.copy(
            abortReason = report.abortReason?.let {
                OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(it, secretsToRedact)
            },
            itemResults = report.itemResults.map { item ->
                item.copy(
                    detail = item.detail?.let {
                        OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(it, secretsToRedact)
                    }
                )
            }
        )
    }

    /**
     * Late-binding 2-session [DataSource] that NEVER opens a 3rd physical connection and NEVER guesses
     * session assignment from `getConnection()` borrow order.
     *
     * Instead, each `getConnection()` call returns an `UNBOUND` [Connection] lease proxy that binds
     * deterministically when the caller declares its mode via `setReadOnly(false)` / `setReadOnly(true)`
     * or prepares its first SQL statement (`INSERT`/`UPDATE` vs `SELECT`).
     */
    internal class LateBindingTwoSessionDataSource(
        private val physicalWriteConn: Connection?,
        private val physicalReadVerifyConn: Connection,
        val writeBackendPid: Int?,
        val readVerifyBackendPid: Int,
        private val netTimeoutExec: ExecutorService,
        private val secretsToRedact: List<String> = emptyList()
    ) : DataSource {

        private val poisoned = AtomicBoolean(false)
        private val poisonReasonRef = AtomicReference<String?>(null)
        private val writeSessionLeased = AtomicBoolean(false)
        private val readVerifySessionLeased = AtomicBoolean(false)
        private val inFlightStatementsCount = AtomicInteger(0)
        private val auditSequence = AtomicInteger(0)
        private val auditEntries = Collections.synchronizedList(ArrayList<SqlSessionAuditEntry>())
        @Volatile
        private var currentLoginTimeoutSeconds: Int = 0

        fun isPoisoned(): Boolean = poisoned.get()

        fun getPoisonReason(): String? = poisonReasonRef.get()

        fun sanitizeReason(raw: String?): String =
            OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(raw, secretsToRedact)

        fun getAuditEntries(): List<SqlSessionAuditEntry> = synchronized(auditEntries) {
            ArrayList(auditEntries)
        }

        fun incrementInFlightStatements() {
            inFlightStatementsCount.incrementAndGet()
        }

        fun decrementInFlightStatements() {
            inFlightStatementsCount.decrementAndGet()
        }

        fun fenceInFlightLeasesOnRunExit() {
            if (writeSessionLeased.get() || readVerifySessionLeased.get() || inFlightStatementsCount.get() > 0) {
                poisonPool(
                    "In-flight SQL session lease or statement still active at completion exit after timeout/cancellation fence " +
                        "(writeLeased=${writeSessionLeased.get()}, readVerifyLeased=${readVerifySessionLeased.get()}, inFlightStatements=${inFlightStatementsCount.get()})"
                )
            }
        }

        fun poisonPool(reason: String) {
            val sanitizedReason = sanitizeReason(reason)
            if (poisoned.compareAndSet(false, true)) {
                poisonReasonRef.set(sanitizedReason)
            }
            Thread({
                runCatching { physicalWriteConn?.abort(netTimeoutExec) }
                runCatching { physicalReadVerifyConn.abort(netTimeoutExec) }
                runCatching { physicalWriteConn?.close() }
                runCatching { physicalReadVerifyConn.close() }
            }, "offprod-prewarmed-pool-poison-abort").apply {
                isDaemon = true
                start()
            }
        }

        override fun getConnection(): Connection {
            if (poisoned.get()) {
                throw SQLException(
                    "Prewarmed 2-session pool is poisoned (${poisonReasonRef.get()}); refusing session reuse and refusing to open a 3rd physical connection"
                )
            }
            val handler = LateBindingLeaseInvocationHandler(
                parentPool = this,
                physicalWriteConn = physicalWriteConn,
                physicalReadVerifyConn = physicalReadVerifyConn,
                writeBackendPid = writeBackendPid,
                readVerifyBackendPid = readVerifyBackendPid,
                writeSessionLeased = writeSessionLeased,
                readVerifySessionLeased = readVerifySessionLeased,
                auditSequence = auditSequence,
                auditEntries = auditEntries
            )
            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java),
                handler
            ) as Connection
        }

        override fun getConnection(username: String?, password: String?): Connection = getConnection()
        override fun getLogWriter(): PrintWriter? = null
        override fun setLogWriter(out: PrintWriter?) {}
        override fun setLoginTimeout(seconds: Int) {
            currentLoginTimeoutSeconds = seconds
        }
        override fun getLoginTimeout(): Int = currentLoginTimeoutSeconds
        override fun getParentLogger(): Logger = Logger.getGlobal()
        override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
        override fun isWrapperFor(iface: Class<*>?): Boolean = false
    }

    private class LateBindingLeaseInvocationHandler(
        private val parentPool: LateBindingTwoSessionDataSource,
        private val physicalWriteConn: Connection?,
        private val physicalReadVerifyConn: Connection,
        private val writeBackendPid: Int?,
        private val readVerifyBackendPid: Int,
        private val writeSessionLeased: AtomicBoolean,
        private val readVerifySessionLeased: AtomicBoolean,
        private val auditSequence: AtomicInteger,
        private val auditEntries: MutableList<SqlSessionAuditEntry>
    ) : InvocationHandler {

        private val leaseClosed = AtomicBoolean(false)
        private val boundRoleRef = AtomicReference<SessionRole?>(null)
        private val boundPhysicalConnRef = AtomicReference<Connection?>(null)
        private val activeStatementInFlight = AtomicBoolean(false)
        private val pendingNetworkTimeoutRef = AtomicReference<Pair<Executor, Int>?>(null)
        private val desiredAutoCommit = AtomicBoolean(true)

        private fun sanitizeThrowableAsSqlException(prefix: String, cause: Throwable): SQLException {
            val sanitizedMsg = parentPool.sanitizeReason("$prefix: ${cause.message}")
            return if (cause is SQLException) {
                SQLException(sanitizedMsg, cause.sqlState, cause.errorCode)
            } else {
                SQLException(sanitizedMsg)
            }
        }

        private fun ensureNotClosedOrPoisoned() {
            if (parentPool.isPoisoned()) {
                throw SQLException(
                    "Prewarmed 2-session pool is poisoned (${parentPool.getPoisonReason()}); refusing operation"
                )
            }
            if (leaseClosed.get()) {
                throw SQLException("Connection lease is already closed")
            }
        }

        private fun bindToRoleDeterministically(targetRole: SessionRole): Connection {
            ensureNotClosedOrPoisoned()
            val currentRole = boundRoleRef.get()
            if (currentRole != null) {
                if (currentRole != targetRole) {
                    parentPool.poisonPool("Attempted to switch bound lease role from $currentRole to $targetRole on the same lease")
                    throw SQLException("Cannot switch bound lease role from $currentRole to $targetRole")
                }
                return requireNotNull(boundPhysicalConnRef.get())
            }

            return when (targetRole) {
                SessionRole.WRITE_SESSION -> {
                    val writeConn = physicalWriteConn
                        ?: throw SQLException("Write session requested, but pool was initialized in read-only mode (1 physical connection)")
                    val expectedWritePid = requireNotNull(writeBackendPid) {
                        "writeBackendPid must be present when physicalWriteConn is present"
                    }
                    if (expectedWritePid == readVerifyBackendPid) {
                        parentPool.poisonPool("writeBackendPid ($expectedWritePid) equals readVerifyBackendPid ($readVerifyBackendPid)")
                        throw SQLException("Distinct physical PIDs required for write and read/verify sessions")
                    }
                    if (!writeSessionLeased.compareAndSet(false, true)) {
                        parentPool.poisonPool("Concurrent or unreleased lease contention on WRITE_SESSION")
                        throw SQLException("WRITE_SESSION is already leased; refusing to open a 3rd physical connection")
                    }
                    boundRoleRef.set(SessionRole.WRITE_SESSION)
                    boundPhysicalConnRef.set(writeConn)
                    try {
                        writeConn.isReadOnly = false
                        writeConn.autoCommit = desiredAutoCommit.get()
                        pendingNetworkTimeoutRef.get()?.let { (exec, ms) ->
                            writeConn.setNetworkTimeout(exec, ms)
                        }
                    } catch (e: Exception) {
                        val sanitizedEx = sanitizeThrowableAsSqlException("Failed to configure WRITE_SESSION on bind", e)
                        parentPool.poisonPool(sanitizedEx.message ?: "Failed to configure WRITE_SESSION on bind")
                        throw sanitizedEx
                    }
                    writeConn
                }

                SessionRole.READ_VERIFY_SESSION -> {
                    if (!readVerifySessionLeased.compareAndSet(false, true)) {
                        parentPool.poisonPool("Concurrent or unreleased lease contention on READ_VERIFY_SESSION")
                        throw SQLException("READ_VERIFY_SESSION is already leased; refusing to open a 3rd physical connection")
                    }
                    boundRoleRef.set(SessionRole.READ_VERIFY_SESSION)
                    boundPhysicalConnRef.set(physicalReadVerifyConn)
                    try {
                        physicalReadVerifyConn.isReadOnly = true
                        physicalReadVerifyConn.autoCommit = true
                        pendingNetworkTimeoutRef.get()?.let { (exec, ms) ->
                            physicalReadVerifyConn.setNetworkTimeout(exec, ms)
                        }
                    } catch (e: Exception) {
                        val sanitizedEx = sanitizeThrowableAsSqlException("Failed to configure READ_VERIFY_SESSION on bind", e)
                        parentPool.poisonPool(sanitizedEx.message ?: "Failed to configure READ_VERIFY_SESSION on bind")
                        throw sanitizedEx
                    }
                    physicalReadVerifyConn
                }
            }
        }

        private fun classifySql(sql: String): Pair<SessionRole, SqlOperationKind> {
            val normalized = sql.trim().uppercase(Locale.ROOT).replace(Regex("\\s+"), " ")
            return when {
                normalized.startsWith("INSERT INTO PLAYER_RIOT_IDENTITIES") ->
                    SessionRole.WRITE_SESSION to SqlOperationKind.CANDIDATE_IDENTITY_UPSERT
                normalized.startsWith("UPDATE INDEXED_PLAYERS") ->
                    SessionRole.WRITE_SESSION to SqlOperationKind.CANDIDATE_INDEXED_PLAYERS_UPDATE
                normalized.startsWith("SELECT GAME_NAME, TAG_LINE, VERIFIED_AT FROM PLAYER_RIOT_IDENTITIES WHERE PUUID = ?") ->
                    SessionRole.READ_VERIFY_SESSION to SqlOperationKind.POST_COMMIT_VERIFY_IDENTITY_SELECT
                normalized.startsWith("SELECT GAME_NAME, TAG_LINE FROM INDEXED_PLAYERS WHERE PUUID = ?") ->
                    SessionRole.READ_VERIFY_SESSION to SqlOperationKind.POST_COMMIT_VERIFY_INDEXED_SELECT
                normalized.startsWith("SELECT PUUID, GAME_NAME, TAG_LINE FROM INDEXED_PLAYERS WHERE PUUID IN") ->
                    SessionRole.READ_VERIFY_SESSION to SqlOperationKind.PRECHECK_INDEXED_PLAYERS_SELECT
                normalized.startsWith("SELECT PUUID, GAME_NAME, TAG_LINE FROM PLAYER_RIOT_IDENTITIES WHERE PUUID IN") ->
                    SessionRole.READ_VERIFY_SESSION to SqlOperationKind.PRECHECK_IDENTITY_SELECT
                normalized.startsWith("SELECT ") ->
                    SessionRole.READ_VERIFY_SESSION to SqlOperationKind.OTHER_SQL
                normalized.startsWith("INSERT ") || normalized.startsWith("UPDATE ") ->
                    SessionRole.WRITE_SESSION to SqlOperationKind.OTHER_SQL
                else ->
                    throw SQLException("Unsupported or forbidden SQL in completion wrapper: $normalized")
            }
        }

        private fun verifyLiveSessionBackendPid(physicalConn: Connection, expectedPid: Int): Int {
            val pgPid = runCatching {
                if (physicalConn.isWrapperFor(PGConnection::class.java)) {
                    physicalConn.unwrap(PGConnection::class.java).backendPID
                } else {
                    0
                }
            }.getOrDefault(0)

            val ps = physicalConn.prepareStatement(OffProdAdditiveIdentitySchemaSetupCli.SQL_SELECT_BACKEND_PID)
                ?: run {
                    val msg = "Live SELECT pg_backend_pid() failed: prepareStatement returned null (failing closed on unverifiable PID)"
                    parentPool.poisonPool(msg)
                    throw SQLException(msg)
                }
            ps.use { stmt ->
                val rs = stmt.executeQuery()
                    ?: run {
                        val msg = "Live SELECT pg_backend_pid() failed: executeQuery returned null (failing closed on unverifiable PID)"
                        parentPool.poisonPool(msg)
                        throw SQLException(msg)
                    }
                rs.use {
                    val hasRow = runCatching { rs.next() }.getOrElse { ex ->
                        if (ex is SQLException) throw ex else false
                    }
                    if (!hasRow) {
                        val msg = "Live SELECT pg_backend_pid() failed: ResultSet returned 0 rows (failing closed on unverifiable PID)"
                        parentPool.poisonPool(msg)
                        throw SQLException(msg)
                    }
                    val byName = runCatching { rs.getInt("backend_pid") }.getOrElse { ex ->
                        if (ex is SQLException) throw ex else 0
                    }
                    val livePid = if (byName > 0) {
                        byName
                    } else {
                        runCatching { rs.getInt(1) }.getOrElse { ex ->
                            if (ex is SQLException) throw ex else 0
                        }
                    }
                    if (livePid <= 0) {
                        val msg = "Live SELECT pg_backend_pid() returned non-positive or null PID ($livePid)"
                        parentPool.poisonPool(msg)
                        throw SQLException(msg)
                    }
                    if (pgPid > 0 && pgPid != livePid) {
                        val msg = "PGConnection.backendPID ($pgPid) does not match live SELECT pg_backend_pid() ($livePid)"
                        parentPool.poisonPool(msg)
                        throw SQLException(msg)
                    }
                    if (livePid != expectedPid) {
                        val msg = "Live pg_backend_pid() ($livePid) diverged from expected session PID ($expectedPid)"
                        parentPool.poisonPool(msg)
                        throw SQLException(msg)
                    }
                    return livePid
                }
            }
        }

        private fun releaseLease() {
            if (leaseClosed.compareAndSet(false, true)) {
                val role = boundRoleRef.get()
                if (activeStatementInFlight.get()) {
                    parentPool.poisonPool("Connection lease closed while SQL statement was still in flight on $role")
                }
                when (role) {
                    SessionRole.WRITE_SESSION -> writeSessionLeased.set(false)
                    SessionRole.READ_VERIFY_SESSION -> readVerifySessionLeased.set(false)
                    null -> {}
                }
            }
        }

        override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
            return when (method.name) {
                "equals" -> proxy === args?.firstOrNull()
                "hashCode" -> System.identityHashCode(proxy)
                "toString" -> "LateBindingConnectionLease(role=${boundRoleRef.get()}, closed=${leaseClosed.get()})"
                "isClosed" -> leaseClosed.get() || parentPool.isPoisoned()
                "close" -> {
                    releaseLease()
                    null
                }
                "abort" -> {
                    parentPool.poisonPool("Connection.abort() invoked by timeout/cancellation fence on role=${boundRoleRef.get()}")
                    releaseLease()
                    null
                }
                "setReadOnly" -> {
                    ensureNotClosedOrPoisoned()
                    val readOnly = args?.firstOrNull() as? Boolean
                        ?: throw SQLException("setReadOnly requires a boolean argument")
                    val targetRole = if (readOnly) SessionRole.READ_VERIFY_SESSION else SessionRole.WRITE_SESSION
                    bindToRoleDeterministically(targetRole)
                    null
                }
                "isReadOnly" -> {
                    ensureNotClosedOrPoisoned()
                    when (boundRoleRef.get()) {
                        SessionRole.WRITE_SESSION -> false
                        SessionRole.READ_VERIFY_SESSION -> true
                        null -> true
                    }
                }
                "setAutoCommit" -> {
                    ensureNotClosedOrPoisoned()
                    val autoCommit = args?.firstOrNull() as? Boolean ?: true
                    desiredAutoCommit.set(autoCommit)
                    boundPhysicalConnRef.get()?.let { it.autoCommit = autoCommit }
                    null
                }
                "getAutoCommit" -> {
                    ensureNotClosedOrPoisoned()
                    boundPhysicalConnRef.get()?.autoCommit ?: desiredAutoCommit.get()
                }
                "setNetworkTimeout" -> {
                    ensureNotClosedOrPoisoned()
                    val exec = args?.getOrNull(0) as? Executor
                        ?: throw SQLException("setNetworkTimeout requires non-null Executor")
                    val ms = args.getOrNull(1) as? Int
                        ?: throw SQLException("setNetworkTimeout requires timeout ms")
                    pendingNetworkTimeoutRef.set(exec to ms)
                    boundPhysicalConnRef.get()?.setNetworkTimeout(exec, ms)
                    null
                }
                "prepareStatement" -> {
                    ensureNotClosedOrPoisoned()
                    val sql = args?.firstOrNull() as? String
                        ?: throw SQLException("prepareStatement requires SQL string")
                    val (requiredRole, opKind) = classifySql(sql)
                    val physicalConn = bindToRoleDeterministically(requiredRole)
                    val expectedPid = when (requiredRole) {
                        SessionRole.WRITE_SESSION -> requireNotNull(writeBackendPid)
                        SessionRole.READ_VERIFY_SESSION -> readVerifyBackendPid
                    }
                    if (writeBackendPid != null && writeBackendPid == readVerifyBackendPid) {
                        parentPool.poisonPool("writeBackendPid ($writeBackendPid) equals readVerifyBackendPid ($readVerifyBackendPid)")
                        throw SQLException("Distinct physical PIDs required for write and read/verify sessions")
                    }

                    val rawPs = try {
                        val prepared = if (args.size == 1) {
                            physicalConn.prepareStatement(sql)
                        } else {
                            method.invoke(physicalConn, *args) as? PreparedStatement
                        }
                        prepared ?: throw SQLException("prepareStatement returned null on $requiredRole")
                    } catch (e: InvocationTargetException) {
                        val sanitizedEx = sanitizeThrowableAsSqlException("prepareStatement failed on $requiredRole", e.targetException)
                        parentPool.poisonPool(sanitizedEx.message ?: "prepareStatement failed on $requiredRole")
                        throw sanitizedEx
                    } catch (e: Exception) {
                        val sanitizedEx = sanitizeThrowableAsSqlException("prepareStatement failed on $requiredRole", e)
                        parentPool.poisonPool(sanitizedEx.message ?: "prepareStatement failed on $requiredRole")
                        throw sanitizedEx
                    }

                    wrapPreparedStatement(
                        rawPs = rawPs,
                        physicalConn = physicalConn,
                        sessionRole = requiredRole,
                        operationKind = opKind,
                        expectedBackendPid = expectedPid,
                        leaseIdentityHash = System.identityHashCode(proxy)
                    )
                }
                "commit", "rollback" -> {
                    ensureNotClosedOrPoisoned()
                    val conn = boundPhysicalConnRef.get()
                        ?: throw SQLException("${method.name} called on unbound lease")
                    try {
                        if (args == null) method.invoke(conn) else method.invoke(conn, *args)
                    } catch (e: InvocationTargetException) {
                        val sanitizedEx = sanitizeThrowableAsSqlException("${method.name} failed", e.targetException)
                        parentPool.poisonPool(sanitizedEx.message ?: "${method.name} failed")
                        throw sanitizedEx
                    }
                }
                else -> {
                    ensureNotClosedOrPoisoned()
                    val conn = boundPhysicalConnRef.get() ?: physicalReadVerifyConn
                    try {
                        if (args == null) method.invoke(conn) else method.invoke(conn, *args)
                    } catch (e: InvocationTargetException) {
                        throw sanitizeThrowableAsSqlException("${method.name} failed", e.targetException)
                    }
                }
            }
        }

        private fun wrapPreparedStatement(
            rawPs: PreparedStatement,
            physicalConn: Connection,
            sessionRole: SessionRole,
            operationKind: SqlOperationKind,
            expectedBackendPid: Int,
            leaseIdentityHash: Int
        ): PreparedStatement {
            val firstStringParamRef = AtomicReference<String?>(null)
            return Proxy.newProxyInstance(
                PreparedStatement::class.java.classLoader,
                arrayOf(PreparedStatement::class.java)
            ) { psProxy, psMethod, psArgs ->
                when (psMethod.name) {
                    "equals" -> psProxy === psArgs?.firstOrNull()
                    "hashCode" -> System.identityHashCode(psProxy)
                    "cancel" -> {
                        parentPool.poisonPool("PreparedStatement.cancel() invoked on $sessionRole ($operationKind)")
                        runCatching { rawPs.cancel() }
                        null
                    }
                    "setString" -> {
                        val paramIdx = psArgs?.getOrNull(0) as? Int
                        val paramVal = psArgs?.getOrNull(1) as? String
                        if (paramIdx == 1 && paramVal != null) {
                            firstStringParamRef.set(paramVal)
                        }
                        try {
                            psMethod.invoke(rawPs, *(psArgs ?: emptyArray()))
                        } catch (e: InvocationTargetException) {
                            throw sanitizeThrowableAsSqlException("setString failed", e.targetException)
                        }
                    }
                    "executeQuery", "executeUpdate" -> {
                        ensureNotClosedOrPoisoned()
                        activeStatementInFlight.set(true)
                        parentPool.incrementInFlightStatements()
                        try {
                            val livePid = verifyLiveSessionBackendPid(physicalConn, expectedBackendPid)
                            val candidatePuuid = when (operationKind) {
                                SqlOperationKind.CANDIDATE_IDENTITY_UPSERT,
                                SqlOperationKind.POST_COMMIT_VERIFY_IDENTITY_SELECT,
                                SqlOperationKind.POST_COMMIT_VERIFY_INDEXED_SELECT -> firstStringParamRef.get()
                                else -> null
                            }
                            val result = if (psArgs == null) {
                                psMethod.invoke(rawPs)
                            } else {
                                psMethod.invoke(rawPs, *psArgs)
                            }
                            val seq = auditSequence.incrementAndGet()
                            auditEntries.add(
                                SqlSessionAuditEntry(
                                    sequenceNumber = seq,
                                    sessionRole = sessionRole,
                                    operationKind = operationKind,
                                    candidatePuuid = candidatePuuid,
                                    backendPid = livePid,
                                    leaseIdentityHash = leaseIdentityHash
                                )
                            )
                            result
                        } catch (e: InvocationTargetException) {
                            val cause = e.targetException
                            val isMissingIdentityTableOnPrecheck =
                                operationKind == SqlOperationKind.PRECHECK_IDENTITY_SELECT &&
                                    cause is SQLException &&
                                    (cause.sqlState == "42P01" || cause.message?.lowercase(Locale.ROOT)?.contains("player_riot_identities") == true)
                            val sanitizedEx = sanitizeThrowableAsSqlException("SQL execution failed on $sessionRole ($operationKind)", cause)
                            if (!isMissingIdentityTableOnPrecheck) {
                                parentPool.poisonPool(sanitizedEx.message ?: "SQL execution failed on $sessionRole ($operationKind)")
                            }
                            throw sanitizedEx
                        } catch (e: Exception) {
                            val sanitizedEx = sanitizeThrowableAsSqlException("SQL execution failed on $sessionRole ($operationKind)", e)
                            parentPool.poisonPool(sanitizedEx.message ?: "SQL execution failed on $sessionRole ($operationKind)")
                            throw sanitizedEx
                        } finally {
                            parentPool.decrementInFlightStatements()
                            activeStatementInFlight.set(false)
                        }
                    }
                    else -> {
                        try {
                            if (psArgs == null) psMethod.invoke(rawPs) else psMethod.invoke(rawPs, *psArgs)
                        } catch (e: InvocationTargetException) {
                            throw sanitizeThrowableAsSqlException("${psMethod.name} failed", e.targetException)
                        }
                    }
                }
            } as PreparedStatement
        }
    }

    private sealed class PrewarmReadinessOutcome {
        data class Success(
            val physicalWriteConn: Connection?,
            val physicalReadVerifyConn: Connection,
            val physicalConnectionsOpened: Int,
            val writeBackendPid: Int?,
            val readVerifyBackendPid: Int,
            val distinctWriteAndVerifyPidsVerified: Boolean,
            val searchPathBoundToPublic: Boolean,
            val identityTablePresent: Boolean,
            val identityTableShapeValid: Boolean?,
            val netTimeoutExec: ExecutorService
        ) : PrewarmReadinessOutcome()

        data class Failure(
            val sanitizedReason: String,
            val physicalConnectionsOpened: Int,
            val writeBackendPid: Int? = null,
            val readVerifyBackendPid: Int? = null,
            val searchPathBoundToPublic: Boolean? = null,
            val identityTablePresent: Boolean? = null,
            val identityTableShapeValid: Boolean? = null,
            val cleanupStatus: OffProdAdditiveIdentitySchemaSetupCli.CleanupStatus
        ) : PrewarmReadinessOutcome()
    }

    private fun establishPrewarmedSessionsBounded(
        physicalDataSource: DataSource,
        needWriteSession: Boolean,
        readinessTimeoutMs: Long,
        dbTimeoutMs: Long,
        cleanupTimeoutMs: Long,
        monotonicClock: MonotonicClock,
        secretsToRedact: List<String>
    ): PrewarmReadinessOutcome {
        val writeConnRef = AtomicReference<Connection?>(null)
        val readVerifyConnRef = AtomicReference<Connection?>(null)
        val writePidRef = AtomicReference<Int?>(null)
        val readVerifyPidRef = AtomicReference<Int?>(null)
        val connsOpenedCount = AtomicInteger(0)
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
            Thread(r, "offprod-prewarm-net-timeout").apply { isDaemon = true }
        }
        val readinessExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-prewarm-readiness").apply { isDaemon = true }
        }

        fun closeBothAndFail(
            reason: String,
            searchPathBound: Boolean? = null,
            tablePresent: Boolean? = null,
            tableShapeValid: Boolean? = null,
            forceAbort: Boolean = false
        ): PrewarmReadinessOutcome.Failure {
            val w = writeConnRef.getAndSet(null)
            val r = readVerifyConnRef.getAndSet(null)
            val cs = OffProdAdditiveIdentitySchemaSetupCli.closeConnectionsBounded(
                c1 = w ?: r,
                c2 = if (w != null) r else null,
                netTimeoutExec = netTimeoutExec,
                awaitTimeoutMs = cleanupTimeoutMs,
                monotonicClock = monotonicClock,
                forceAbortFirst = forceAbort,
                secretsToRedact = secretsToRedact
            )
            return PrewarmReadinessOutcome.Failure(
                sanitizedReason = OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(reason, secretsToRedact),
                physicalConnectionsOpened = connsOpenedCount.get(),
                writeBackendPid = writePidRef.get(),
                readVerifyBackendPid = readVerifyPidRef.get(),
                searchPathBoundToPublic = searchPathBound,
                identityTablePresent = tablePresent,
                identityTableShapeValid = tableShapeValid,
                cleanupStatus = cs
            )
        }

        val future = readinessExec.submit(Callable<PrewarmReadinessOutcome> {
            try {
                if (needWriteSession) {
                    val remBeforeWriteMs = remainingMs()
                    if (remBeforeWriteMs <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness deadline (${readinessTimeoutMs}ms) expired before write session getConnection",
                            forceAbort = true
                        )
                    }
                    physicalDataSource.loginTimeout = ceil(remBeforeWriteMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                    if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before write session getConnection",
                            forceAbort = true
                        )
                    }
                    val wConn = physicalDataSource.connection
                    connsOpenedCount.incrementAndGet()
                    writeConnRef.set(wConn)
                    if (isCancelledOrPastDeadline()) {
                        return@Callable closeBothAndFail(
                            reason = "Write session connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without SQL",
                            forceAbort = true
                        )
                    }

                    val remAfterWriteMs = remainingMs()
                    if (remAfterWriteMs <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness deadline (${readinessTimeoutMs}ms) expired immediately after write session acquisition",
                            forceAbort = true
                        )
                    }
                    wConn.isReadOnly = false
                    wConn.setNetworkTimeout(netTimeoutExec, remAfterWriteMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                    wConn.autoCommit = true

                    configureSessionSearchPathAndTimeoutsBounded(
                        conn = wConn,
                        dbTimeoutMs = dbTimeoutMs,
                        remainingMsProvider = ::remainingMs,
                        isCancelledProvider = ::isCancelledOrPastDeadline
                    )

                    val wPid = querySessionPidBounded(
                        conn = wConn,
                        remainingMsProvider = ::remainingMs,
                        isCancelledProvider = ::isCancelledOrPastDeadline
                    )
                    writePidRef.set(wPid)
                }

                val remBeforeReadMs = remainingMs()
                if (remBeforeReadMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable closeBothAndFail(
                        reason = "Readiness deadline (${readinessTimeoutMs}ms) expired before read/verify session getConnection",
                        forceAbort = true
                    )
                }
                physicalDataSource.loginTimeout = ceil(remBeforeReadMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                if (remainingMs() <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable closeBothAndFail(
                        reason = "Readiness deadline (${readinessTimeoutMs}ms) expired after setLoginTimeout before read/verify session getConnection",
                        forceAbort = true
                    )
                }
                val rConn = physicalDataSource.connection
                connsOpenedCount.incrementAndGet()
                readVerifyConnRef.set(rConn)
                if (isCancelledOrPastDeadline()) {
                    return@Callable closeBothAndFail(
                        reason = "Read/verify session connection acquired after readiness deadline (${readinessTimeoutMs}ms); closed immediately without SQL",
                        forceAbort = true
                    )
                }

                val wConn = writeConnRef.get()
                if (wConn != null) {
                    if (rConn === wConn) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness physical session check failed: DataSource returned the exact same Connection instance for write and read/verify sessions"
                        )
                    }
                    val sameUnwrappedPg = runCatching {
                        if (wConn.isWrapperFor(PGConnection::class.java) && rConn.isWrapperFor(PGConnection::class.java)) {
                            val pgW = wConn.unwrap(PGConnection::class.java)
                            val pgR = rConn.unwrap(PGConnection::class.java)
                            pgW === pgR || (pgW.backendPID > 0 && pgW.backendPID == pgR.backendPID)
                        } else {
                            false
                        }
                    }.getOrDefault(false)
                    if (sameUnwrappedPg) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness physical session check failed: write and read/verify sessions unwrap to the same physical PGConnection/PID"
                        )
                    }
                }

                val remAfterReadMs = remainingMs()
                if (remAfterReadMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable closeBothAndFail(
                        reason = "Readiness deadline (${readinessTimeoutMs}ms) expired immediately after read/verify session acquisition",
                        forceAbort = true
                    )
                }
                rConn.isReadOnly = true
                rConn.setNetworkTimeout(netTimeoutExec, remAfterReadMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                rConn.autoCommit = true

                configureSessionSearchPathAndTimeoutsBounded(
                    conn = rConn,
                    dbTimeoutMs = dbTimeoutMs,
                    remainingMsProvider = ::remainingMs,
                    isCancelledProvider = ::isCancelledOrPastDeadline
                )

                val rPid = querySessionPidBounded(
                    conn = rConn,
                    remainingMsProvider = ::remainingMs,
                    isCancelledProvider = ::isCancelledOrPastDeadline
                )
                readVerifyPidRef.set(rPid)

                val wPid = writePidRef.get()
                if (wPid != null && wPid == rPid) {
                    return@Callable closeBothAndFail(
                        reason = "Readiness physical session check failed: writeBackendPid ($wPid) and readVerifyBackendPid ($rPid) must be distinct positive PostgreSQL backend PIDs"
                    )
                }

                if (wConn != null) {
                    val (writeSchemaIsPublic, writeTablePresent, writeTableShapeValid) = checkPublicTableContractBounded(
                        conn = wConn,
                        remainingMsProvider = ::remainingMs,
                        isCancelledProvider = ::isCancelledOrPastDeadline
                    )
                    if (!writeSchemaIsPublic) {
                        return@Callable closeBothAndFail(
                            reason = "Readiness schema check failed on write session: search_path did not bind to trusted 'public' schema",
                            searchPathBound = false,
                            tablePresent = writeTablePresent,
                            tableShapeValid = writeTableShapeValid
                        )
                    }
                    if (writeTablePresent && writeTableShapeValid == false) {
                        return@Callable closeBothAndFail(
                            reason = "Existing table public.player_riot_identities has an incompatible schema on write session; aborting fail-closed before any HTTP or SQL completion",
                            searchPathBound = true,
                            tablePresent = true,
                            tableShapeValid = false
                        )
                    }
                    if (!writeTablePresent) {
                        return@Callable closeBothAndFail(
                            reason = "Table public.player_riot_identities is absent in durable-write mode; run OffProdAdditiveIdentitySchemaSetupCli first (completion wrapper never executes DDL)",
                            searchPathBound = true,
                            tablePresent = false,
                            tableShapeValid = null
                        )
                    }
                }

                // Verify trusted search_path == 'public' and public.player_riot_identities contract on read/verify session:
                val (schemaIsPublic, tablePresent, tableShapeValid) = checkPublicTableContractBounded(
                    conn = rConn,
                    remainingMsProvider = ::remainingMs,
                    isCancelledProvider = ::isCancelledOrPastDeadline
                )
                if (!schemaIsPublic) {
                    return@Callable closeBothAndFail(
                        reason = "Readiness schema check failed: search_path did not bind to trusted 'public' schema",
                        searchPathBound = false,
                        tablePresent = tablePresent,
                        tableShapeValid = tableShapeValid
                    )
                }
                if (tablePresent && tableShapeValid == false) {
                    return@Callable closeBothAndFail(
                        reason = "Existing table public.player_riot_identities has an incompatible schema (wrong columns, types, PRIMARY KEY, or non-blank CHECK constraint); aborting fail-closed before any HTTP or SQL completion",
                        searchPathBound = true,
                        tablePresent = true,
                        tableShapeValid = false
                    )
                }
                if (needWriteSession && !tablePresent) {
                    return@Callable closeBothAndFail(
                        reason = "Table public.player_riot_identities is absent in durable-write mode; run OffProdAdditiveIdentitySchemaSetupCli first (completion wrapper never executes DDL)",
                        searchPathBound = true,
                        tablePresent = false,
                        tableShapeValid = null
                    )
                }

                PrewarmReadinessOutcome.Success(
                    physicalWriteConn = wConn,
                    physicalReadVerifyConn = rConn,
                    physicalConnectionsOpened = connsOpenedCount.get(),
                    writeBackendPid = wPid,
                    readVerifyBackendPid = rPid,
                    distinctWriteAndVerifyPidsVerified = wPid != null && wPid != rPid,
                    searchPathBoundToPublic = true,
                    identityTablePresent = tablePresent,
                    identityTableShapeValid = if (tablePresent) tableShapeValid else null,
                    netTimeoutExec = netTimeoutExec
                )
            } catch (e: Exception) {
                closeBothAndFail(
                    reason = "Pre-warmed connection readiness failed (${e.javaClass.simpleName}): ${e.message}",
                    forceAbort = true
                )
            }
        })

        return try {
            future.get(readinessTimeoutMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            closeBothAndFail(
                reason = "Pre-warmed connection readiness exceeded ${readinessTimeoutMs}ms deadline; late connections fenced and closed with 0 HTTP calls and 0 candidate writes",
                forceAbort = true
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            closeBothAndFail(
                reason = "Pre-warmed connection readiness interrupted/failed: ${e.message}",
                forceAbort = true
            )
        } finally {
            readinessExec.shutdownNow()
        }
    }

    private fun configureSessionSearchPathAndTimeoutsBounded(
        conn: Connection,
        dbTimeoutMs: Long,
        remainingMsProvider: () -> Long,
        isCancelledProvider: () -> Boolean
    ) {
        val remMs = remainingMsProvider()
        if (remMs <= 0L || isCancelledProvider()) {
            throw TimeoutException("Readiness deadline expired before session search_path/timeout configuration")
        }
        val ps = conn.prepareStatement(OffProdAdditiveIdentitySchemaSetupCli.SQL_SET_TRUSTED_SEARCH_PATH)
            ?: throw SQLException("Failed to prepare SET search_path = public, pg_catalog (prepareStatement returned null)")
        ps.use { stmt ->
            stmt.queryTimeout = ceil(remMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
            if (remainingMsProvider() <= 0L || isCancelledProvider()) {
                throw TimeoutException("Readiness deadline expired directly before SET search_path executeUpdate")
            }
            stmt.executeUpdate()
        }
        val isRealPg = runCatching { conn.isWrapperFor(PGConnection::class.java) }.getOrDefault(false)
        if (isRealPg) {
            val remStmtMs = remainingMsProvider()
            if (remStmtMs <= 0L || isCancelledProvider()) {
                throw TimeoutException("Readiness deadline expired before SET lock_timeout/statement_timeout")
            }
            val lockMs = minOf(DEFAULT_LOCK_TIMEOUT_MS, dbTimeoutMs).coerceAtLeast(1L)
            val st = conn.createStatement()
                ?: throw SQLException("Failed to createStatement for SET lock_timeout/statement_timeout")
            st.use { stmt ->
                stmt.queryTimeout = ceil(remStmtMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                stmt.execute(
                    "SET search_path = public, pg_catalog; SET lock_timeout = '${lockMs}ms'; SET statement_timeout = '${dbTimeoutMs}ms'"
                )
            }
        }
    }

    private fun querySessionPidBounded(
        conn: Connection,
        remainingMsProvider: () -> Long,
        isCancelledProvider: () -> Boolean
    ): Int = OffProdAdditiveIdentitySchemaSetupCli.queryBackendPidBounded(
        conn = conn,
        remainingMsProvider = remainingMsProvider,
        isCancelledProvider = isCancelledProvider
    )

    private fun checkPublicTableContractBounded(
        conn: Connection,
        remainingMsProvider: () -> Long,
        isCancelledProvider: () -> Boolean
    ): Triple<Boolean, Boolean, Boolean?> {
        val remMs = remainingMsProvider()
        if (remMs <= 0L || isCancelledProvider()) {
            throw TimeoutException("Readiness deadline expired before public.player_riot_identities contract check")
        }
        val ps = conn.prepareStatement(OffProdAdditiveIdentitySchemaSetupCli.SQL_VERIFY_TABLE_PRESENT.trimIndent())
            ?: throw SQLException("Schema contract check failed: prepareStatement returned null (failing closed on unverifiable schema evidence)")
        ps.use { stmt ->
            stmt.queryTimeout = ceil(remMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
            if (remainingMsProvider() <= 0L || isCancelledProvider()) {
                throw TimeoutException("Readiness deadline expired directly before public.player_riot_identities contract executeQuery")
            }
            val rs = stmt.executeQuery()
                ?: throw SQLException("Schema contract check failed: executeQuery returned null (failing closed on unverifiable schema evidence)")
            val row = rs.use {
                OffProdAdditiveIdentitySchemaSetupCli.readTableContractRowStrict(it, requireCanCreate = false)
            }
            return Triple(
                row.currentSchema.equals("public", ignoreCase = true),
                row.tablePresent,
                row.tableShapeValid
            )
        }
    }

    internal fun buildForwardedCliArgs(flags: Map<String, String>, remainingRunBudgetMs: Long): List<String> {
        val wrapperOnlyKeys = setOf(
            "readiness-timeout-ms",
            "cleanup-timeout-ms",
            "enforce-max-heap-256m",
            "output-wrapper-report-file",
            "output-report-file"
        )
        val forwarded = ArrayList<String>()
        for ((k, v) in flags) {
            if (k in wrapperOnlyKeys || k == "max-wall-time-ms" || k == "apply-additive-ddl") continue
            forwarded.add("--$k=$v")
        }
        forwarded.add("--apply-additive-ddl=false")
        forwarded.add("--max-wall-time-ms=$remainingRunBudgetMs")
        return forwarded
    }

    private fun buildInvalidResult(
        errorMessage: String,
        outputWrapperReportFile: File?,
        processStartMonoMs: Long,
        monotonicClock: MonotonicClock,
        wallClock: WallClock,
        maxHeapBytes: Long,
        peakUsedHeapBytes: Long,
        dryRun: Boolean = true,
        networkEnabled: Boolean = false,
        allowDurableWrites: Boolean = false,
        readinessBudgetMs: Long = DEFAULT_READINESS_TIMEOUT_MS,
        totalWallBudgetMs: Long = DEFAULT_TOTAL_WALL_TIME_MS
    ): PrewarmedCliResult {
        val elapsed = (monotonicClock.nowMs() - processStartMonoMs).coerceAtLeast(0L)
        val report = PrewarmedCompletionExecutionReport(
            exitCode = 2,
            dryRun = dryRun,
            networkEnabled = networkEnabled,
            allowDurableWrites = allowDurableWrites,
            physicalConnectionsOpened = 0,
            readinessBudgetMs = readinessBudgetMs,
            readinessElapsedMs = 0L,
            remainingRunBudgetMs = 0L,
            runStageElapsedMs = 0L,
            totalWallBudgetMs = totalWallBudgetMs,
            totalWallElapsedMs = elapsed,
            jvmMaxHeapBytes = maxHeapBytes,
            jvmUsedHeapBytesPeak = peakUsedHeapBytes,
            sanitizedDetail = errorMessage,
            completedAtUtc = wallClock.nowInstant()
        )
        val savedPath = writeReportBestEffort(outputWrapperReportFile, report)
        return PrewarmedCliResult(
            exitCode = 2,
            wrapperReport = report,
            cliResult = null,
            outputWrapperReportPath = savedPath
        )
    }

    private fun writeReportBestEffort(file: File?, report: PrewarmedCompletionExecutionReport): String? {
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
                "Unsupported positional argument '${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(trimmed, secretsToRedact)}'; all arguments must be --key=value flags"
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

    private fun parseIntFlag(flags: Map<String, String>, key: String, defaultValue: Int): Int {
        val raw = flags[key] ?: return defaultValue
        return raw.toIntOrNull()
            ?: throw IllegalArgumentException("--$key must be a valid 32-bit integer (was '$raw')")
    }

    private fun parseLongFlag(flags: Map<String, String>, key: String, defaultValue: Long): Long {
        val raw = flags[key] ?: return defaultValue
        return raw.toLongOrNull()
            ?: throw IllegalArgumentException("--$key must be a valid 64-bit integer (was '$raw')")
    }

    private fun parseClusterUrlOverrides(rawOverride: String?): Map<String, String> {
        val defaults = ExecutionOptions().baseUrlByCluster
        val base = LinkedHashMap(defaults)
        if (rawOverride.isNullOrBlank()) return base
        for (pair in rawOverride.split(",")) {
            val trimmed = pair.trim()
            if (trimmed.isEmpty()) continue
            val eq = trimmed.indexOf('=')
            require(eq > 0 && eq < trimmed.length - 1) {
                "Malformed --cluster-url-override entry '$trimmed' (expected cluster=url)"
            }
            val cluster = trimmed.substring(0, eq).trim().lowercase(Locale.ROOT)
            val url = trimmed.substring(eq + 1).trim()
            require(cluster in defaults.keys) {
                "Unsupported cluster '$cluster' in --cluster-url-override"
            }
            base[cluster] = url
        }
        return base
    }

    private fun buildValidatedDataSourceFromFlagsOrEnv(
        flags: Map<String, String>,
        env: Map<String, String>,
        readinessTimeoutMs: Long,
        dbTimeoutMs: Long,
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
            "Off-production completion CLI requires a jdbc:postgresql:// URL (received '${OffProdAdditiveIdentitySchemaSetupCli.sanitizeErrorMessage(jdbcUrl, secretsToRedact)}')"
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

        val readinessSec = ceil(readinessTimeoutMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
        val sqlSec = ceil(dbTimeoutMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
        val lockMs = minOf(DEFAULT_LOCK_TIMEOUT_MS, dbTimeoutMs).coerceAtLeast(1L)

        return PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = dbUser
            password = dbPass
            connectTimeout = readinessSec
            socketTimeout = maxOf(readinessSec, sqlSec)
            this.options = "-c search_path=public,pg_catalog -c lock_timeout=${lockMs}ms -c statement_timeout=${dbTimeoutMs}ms"
        }
    }
}
