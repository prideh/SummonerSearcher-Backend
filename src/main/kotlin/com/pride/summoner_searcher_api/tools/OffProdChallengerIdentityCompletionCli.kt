package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.CompletionRunReport
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ExecutionOptions
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.KeyCoordinationMode
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ManifestEntry
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.RunOutcome
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.SnapshotManifest
import org.postgresql.ds.PGSimpleDataSource
import java.io.File
import java.sql.Connection
import java.sql.PreparedStatement
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
 * Standalone off-production CLI entrypoint for bounded Challenger missing-identity completion.
 *
 * Architectural & Safety Invariants:
 * 1. Does NOT boot the Spring Boot web server, `CacheWarmer`, or `CohortCollectionScheduler`.
 * 2. Defaults to `dryRun = true`, `networkEnabled = false`, `allowDurableWrites = false` (0 HTTP calls, 0 DB writes).
 * 3. Uses a lightweight single-connection `PGSimpleDataSource` (zero background HikariCP pool threads).
 * 4. Supports `--apply-additive-ddl=true` ONLY when `--dry-run=false` and `--allow-durable-writes=true`,
 *    executing the reviewed classpath migration `/db/migration/V20261109__create_player_riot_identities.sql`
 *    under a strict JDBC deadline fence.
 * 5. Emits both a structured JSON [CompletionRunReport] (`--output-report-file`) and a deterministic
 *    resumable [SnapshotManifest] (`--output-remaining-manifest-file`) containing `report.remainingEntries`.
 */
object OffProdChallengerIdentityCompletionCli {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    data class CliExecutionResult(
        val exitCode: Int,
        val report: CompletionRunReport?,
        val ddlApplied: Boolean,
        val outputReportPath: String?,
        val outputRemainingManifestPath: String?,
        val errorMessage: String? = null
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    internal data class RawManifestEntryDto(
        @field:JsonProperty("puuid")
        @get:JsonProperty("puuid")
        val puuid: String = "",
        @field:JsonProperty("platformRegion")
        @get:JsonProperty("platformRegion")
        val platformRegion: String = ""
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    internal data class RawSnapshotManifestDto(
        @field:JsonProperty("snapshotCapturedAtUtc")
        @get:JsonProperty("snapshotCapturedAtUtc")
        val snapshotCapturedAtUtc: String = "",
        @field:JsonProperty("isLiveLadder")
        @get:JsonProperty("isLiveLadder")
        @field:JsonAlias("liveLadder")
        val isLiveLadder: Boolean = false,
        @field:JsonProperty("entries")
        @get:JsonProperty("entries")
        val entries: List<RawManifestEntryDto> = emptyList()
    )

    @JvmStatic
    fun main(args: Array<String>) {
        val result = runCli(args.toList())
        if (result.errorMessage != null) {
            System.err.println("[OffProdChallengerIdentityCompletionCli] ERROR: ${result.errorMessage}")
        }
        exitProcess(result.exitCode)
    }

    fun runCli(
        args: List<String>,
        env: Map<String, String> = System.getenv(),
        dataSourceOverride: DataSource? = null,
        httpTransportOverride: BoundedAccountV1Transport? = null,
        monotonicClock: MonotonicClock = SystemMonotonicClock,
        wallClock: WallClock = SystemUtcWallClock
    ): CliExecutionResult {
        val cliStartMonoMs = monotonicClock.nowMs()
        val flags = parseFlags(args)
        val manifestPath = flags["manifest-file"]?.trim().orEmpty()
        if (manifestPath.isEmpty()) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Missing required argument --manifest-file=<path>"
            )
        }

        val manifestFile = File(manifestPath)
        if (!manifestFile.isFile) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Manifest file does not exist: ${manifestFile.absolutePath}"
            )
        }

        val manifest = try {
            readSnapshotManifest(manifestFile)
        } catch (e: Exception) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Invalid manifest file '${manifestFile.name}': ${e.message}"
            )
        }

        val dryRun = flags["dry-run"]?.toBooleanStrictOrNull() ?: true
        val networkEnabled = flags["network-enabled"]?.toBooleanStrictOrNull() ?: false
        val allowDurableWrites = flags["allow-durable-writes"]?.toBooleanStrictOrNull() ?: false
        val allowIndexedPlayersBlankUpdate = flags["allow-indexed-players-blank-update"]?.toBooleanStrictOrNull() ?: false
        val applyAdditiveDdl = flags["apply-additive-ddl"]?.toBooleanStrictOrNull() ?: false
        val allowLoopbackHttpForTesting = flags["allow-loopback-http-for-testing"]?.toBooleanStrictOrNull() ?: false

        if (applyAdditiveDdl && (dryRun || !allowDurableWrites)) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "--apply-additive-ddl=true requires both --dry-run=false and --allow-durable-writes=true"
            )
        }

        val coordinationMode = try {
            flags["key-coordination-mode"]?.trim()?.takeIf { it.isNotEmpty() }
                ?.let { KeyCoordinationMode.valueOf(it.uppercase(Locale.ROOT)) }
                ?: KeyCoordinationMode.UNCOORDINATED
        } catch (e: IllegalArgumentException) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Invalid --key-coordination-mode: ${e.message}"
            )
        }
        val coordinationAttestation = flags["coordination-attestation"]?.trim().orEmpty()

        val maxHttpAttempts = flags["max-http-attempts"]?.toIntOrNull()
            ?: OffProdChallengerIdentityCompletionTool.MAX_ALLOWED_HTTP_ATTEMPTS
        val maxWallTimeMs = flags["max-wall-time-ms"]?.toLongOrNull()
            ?: OffProdChallengerIdentityCompletionTool.DEFAULT_MAX_WALL_TIME_MS
        val dbTimeoutMs = flags["db-timeout-ms"]?.toLongOrNull()
            ?: OffProdChallengerIdentityCompletionTool.DEFAULT_DB_OPERATION_TIMEOUT_MS
        val connectTimeoutMs = flags["http-connect-timeout-ms"]?.toIntOrNull()
            ?: OffProdChallengerIdentityCompletionTool.DEFAULT_CONNECT_TIMEOUT_MS
        val readTimeoutMs = flags["http-read-timeout-ms"]?.toIntOrNull()
            ?: OffProdChallengerIdentityCompletionTool.DEFAULT_READ_TIMEOUT_MS
        val callDeadlineMs = flags["http-call-deadline-ms"]?.toLongOrNull()
            ?: OffProdChallengerIdentityCompletionTool.DEFAULT_CALL_DEADLINE_MS
        val maxBodyBytes = flags["max-body-bytes"]?.toIntOrNull()
            ?: OffProdChallengerIdentityCompletionTool.MAX_RESPONSE_BODY_BYTES

        if (maxWallTimeMs <= 0L) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Invalid execution options: maxWallTimeMs must be > 0"
            )
        }

        val baseUrlByCluster = try {
            parseClusterUrlOverrides(flags["cluster-url-override"])
        } catch (e: IllegalArgumentException) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = e.message
            )
        }

        val dataSource = try {
            dataSourceOverride ?: buildDataSourceFromFlagsOrEnv(flags, env)
        } catch (e: Exception) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = false,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Failed to configure JDBC DataSource: ${e.message}"
            )
        }

        var ddlApplied = false
        if (applyAdditiveDdl) {
            val elapsedBeforeDdlMs = (monotonicClock.nowMs() - cliStartMonoMs).coerceAtLeast(0L)
            val remainingGlobalBeforeDdlMs = maxWallTimeMs - elapsedBeforeDdlMs
            val effectiveDdlTimeoutMs = minOf(dbTimeoutMs, remainingGlobalBeforeDdlMs)
            val ddlResult = applyReviewedAdditiveDdlBounded(dataSource, effectiveDdlTimeoutMs, monotonicClock)
            if (!ddlResult.first) {
                val remainingGlobalAfterDdlMs = maxWallTimeMs - (monotonicClock.nowMs() - cliStartMonoMs).coerceAtLeast(0L)
                val ddlExitCode = if (remainingGlobalAfterDdlMs <= 0L) 10 else 3
                return CliExecutionResult(
                    exitCode = ddlExitCode,
                    report = null,
                    ddlApplied = false,
                    outputReportPath = null,
                    outputRemainingManifestPath = null,
                    errorMessage = "Failed to apply additive migration DDL: ${ddlResult.second}"
                )
            }
            ddlApplied = true
        }

        val elapsedBeforeRunMs = (monotonicClock.nowMs() - cliStartMonoMs).coerceAtLeast(0L)
        val remainingWallTimeForRunMs = maxWallTimeMs - elapsedBeforeRunMs
        if (remainingWallTimeForRunMs <= 0L) {
            return CliExecutionResult(
                exitCode = 10,
                report = null,
                ddlApplied = ddlApplied,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Shared global wall-time budget (${maxWallTimeMs}ms) exhausted before runBounded stage"
            )
        }

        val options = try {
            ExecutionOptions(
                dryRun = dryRun,
                networkEnabled = networkEnabled,
                allowDurableWrites = allowDurableWrites,
                enforceAbsentManifestScope = !allowIndexedPlayersBlankUpdate,
                keyCoordinationMode = coordinationMode,
                coordinationAttestation = coordinationAttestation,
                maxTotalHttpAttempts = maxHttpAttempts,
                maxWallTimeMs = remainingWallTimeForRunMs,
                dbOperationTimeoutMs = dbTimeoutMs,
                connectTimeoutMs = connectTimeoutMs,
                readTimeoutMs = readTimeoutMs,
                callDeadlineMs = callDeadlineMs,
                maxBodyBytes = maxBodyBytes,
                allowLoopbackHttpForTesting = allowLoopbackHttpForTesting,
                baseUrlByCluster = baseUrlByCluster
            )
        } catch (e: IllegalArgumentException) {
            return CliExecutionResult(
                exitCode = 2,
                report = null,
                ddlApplied = ddlApplied,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Invalid execution options: ${e.message}"
            )
        }

        val persistence = JdbcIdentityCompletionPersistenceAdapter(
            dataSource = dataSource,
            allowIndexedPlayersBlankUpdate = allowIndexedPlayersBlankUpdate
        )
        val transport = httpTransportOverride ?: HttpUrlConnectionAccountV1Transport()
        val tool = OffProdChallengerIdentityCompletionTool(
            persistencePort = persistence,
            httpTransport = transport,
            monotonicClock = monotonicClock,
            wallClock = wallClock
        )

        val apiKey = flags["riot-api-key"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["OFFPROD_RIOT_API_KEY"]?.trim().orEmpty()

        val report = try {
            tool.runBounded(
                manifest = manifest,
                options = options,
                riotApiKeyProvider = { apiKey }
            )
        } catch (e: Exception) {
            return CliExecutionResult(
                exitCode = 1,
                report = null,
                ddlApplied = ddlApplied,
                outputReportPath = null,
                outputRemainingManifestPath = null,
                errorMessage = "Execution failed: ${e.message}"
            )
        }

        val reportOutPath = flags["output-report-file"]?.trim()?.takeIf { it.isNotEmpty() }
        if (reportOutPath != null) {
            val outFile = File(reportOutPath)
            outFile.parentFile?.mkdirs()
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(outFile, report)
        }

        val remainingManifestOutPath = flags["output-remaining-manifest-file"]?.trim()?.takeIf { it.isNotEmpty() }
        if (remainingManifestOutPath != null) {
            val remFile = File(remainingManifestOutPath)
            remFile.parentFile?.mkdirs()
            writeSnapshotManifest(
                file = remFile,
                snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
                entries = report.remainingEntries
            )
        }

        val exitCode = when (report.outcome) {
            RunOutcome.DRY_RUN_DB_RECHECK_ONLY_COMPLETED,
            RunOutcome.STAGED_ALL_CANDIDATES -> 0
            RunOutcome.PARTIAL_COMPLETION_ATTEMPT_CAP_REACHED,
            RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED -> 10
            else -> 1
        }

        return CliExecutionResult(
            exitCode = exitCode,
            report = report,
            ddlApplied = ddlApplied,
            outputReportPath = reportOutPath,
            outputRemainingManifestPath = remainingManifestOutPath
        )
    }

    fun readSnapshotManifest(file: File): SnapshotManifest {
        val raw = objectMapper.readValue(file, RawSnapshotManifestDto::class.java)
        return SnapshotManifest(
            snapshotCapturedAtUtc = raw.snapshotCapturedAtUtc,
            isLiveLadder = raw.isLiveLadder,
            entries = raw.entries.map { ManifestEntry(puuid = it.puuid, platformRegion = it.platformRegion) }
        )
    }

    fun writeSnapshotManifest(
        file: File,
        snapshotCapturedAtUtc: String,
        entries: List<ManifestEntry>
    ) {
        val dto = RawSnapshotManifestDto(
            snapshotCapturedAtUtc = snapshotCapturedAtUtc,
            isLiveLadder = false,
            entries = entries.map {
                RawManifestEntryDto(
                    puuid = it.normalizedPuuid,
                    platformRegion = it.normalizedPlatformRegion
                )
            }
        )
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, dto)
    }

    internal fun applyReviewedAdditiveDdlBounded(
        dataSource: DataSource,
        timeoutMs: Long,
        monotonicClock: MonotonicClock = SystemMonotonicClock
    ): Pair<Boolean, String> {
        if (timeoutMs <= 0L) {
            return false to "DDL timeout budget (${timeoutMs}ms) exhausted"
        }
        val ddlSql = JdbcIdentityCompletionPersistenceAdapter.loadReviewedAdditiveMigrationSql()
        val connRef = AtomicReference<Connection?>(null)
        val stmtRef = AtomicReference<PreparedStatement?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val startMonoMs = monotonicClock.nowMs()
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)

        fun remainingBudgetMs(): Long {
            val remNanoMs = TimeUnit.NANOSECONDS.toMillis(deadlineNano - System.nanoTime())
            val remMonoMs = timeoutMs - (monotonicClock.nowMs() - startMonoMs)
            return minOf(remNanoMs, remMonoMs)
        }

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || remainingBudgetMs() <= 0L

        val netTimeoutExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-cli-ddl-net-timeout").apply { isDaemon = true }
        }
        val callExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-cli-ddl-call").apply { isDaemon = true }
        }

        val future = callExec.submit(Callable<Pair<Boolean, String>> {
            try {
                val initialRemMs = remainingBudgetMs()
                if (initialRemMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable false to "Deadline expired before DDL connection acquisition"
                }
                dataSource.loginTimeout = ceil(initialRemMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                if (remainingBudgetMs() <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable false to "Deadline expired after setLoginTimeout before DDL connection acquisition"
                }
                val rawConn = dataSource.connection
                connRef.set(rawConn)
                if (isCancelledOrPastDeadline()) {
                    connRef.set(null)
                    runCatching { rawConn.abort(netTimeoutExec) }
                    runCatching { rawConn.close() }
                    return@Callable false to "DDL connection acquired after deadline; closed without executing DDL"
                }
                rawConn.use { conn ->
                    val remAfterAcquire = remainingBudgetMs()
                    if (remAfterAcquire <= 0L || isCancelledOrPastDeadline()) {
                        return@Callable false to "Deadline expired immediately after DDL connection acquisition"
                    }
                    conn.isReadOnly = false
                    conn.setNetworkTimeout(netTimeoutExec, remAfterAcquire.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1))
                    conn.autoCommit = true
                    val wasAutoCommit = conn.autoCommit

                    conn.prepareStatement(ddlSql).use { ps ->
                        stmtRef.set(ps)
                        val remStmtMs = remainingBudgetMs()
                        if (remStmtMs <= 0L || isCancelledOrPastDeadline()) {
                            return@Callable false to "Deadline expired before DDL statement setup"
                        }
                        ps.queryTimeout = ceil(remStmtMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        if (remainingBudgetMs() <= 0L || isCancelledOrPastDeadline()) {
                            return@Callable false to "Deadline expired directly before DDL executeUpdate"
                        }
                        ps.executeUpdate()
                        stmtRef.set(null)
                    }
                    if (!wasAutoCommit) {
                        if (remainingBudgetMs() <= 0L || isCancelledOrPastDeadline()) {
                            return@Callable false to "Deadline expired directly before DDL commit"
                        }
                        conn.commit()
                    }
                }
                true to "Applied reviewed additive migration ${JdbcIdentityCompletionPersistenceAdapter.MIGRATION_RESOURCE_PATH}"
            } catch (e: Exception) {
                false to "${e.javaClass.simpleName}: ${e.message}"
            } finally {
                stmtRef.set(null)
                connRef.set(null)
            }
        })

        return try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            val s = stmtRef.getAndSet(null)
            val c = connRef.getAndSet(null)
            Thread({
                runCatching { s?.cancel() }
                runCatching { c?.abort(netTimeoutExec) }
                runCatching { s?.close() }
                runCatching { c?.close() }
            }, "offprod-cli-ddl-abort").apply {
                isDaemon = true
                start()
            }
            false to "DDL execution exceeded ${timeoutMs}ms deadline"
        } finally {
            callExec.shutdownNow()
            netTimeoutExec.shutdown()
        }
    }

    private fun parseFlags(args: List<String>): Map<String, String> {
        val result = LinkedHashMap<String, String>()
        for (raw in args) {
            val trimmed = raw.trim()
            if (!trimmed.startsWith("--")) continue
            val withoutPrefix = trimmed.removePrefix("--")
            val eqIdx = withoutPrefix.indexOf('=')
            if (eqIdx > 0) {
                val key = withoutPrefix.substring(0, eqIdx).trim().lowercase(Locale.ROOT)
                val value = withoutPrefix.substring(eqIdx + 1).trim()
                result[key] = value
            } else {
                result[withoutPrefix.lowercase(Locale.ROOT)] = "true"
            }
        }
        return result
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

    private fun buildDataSourceFromFlagsOrEnv(
        flags: Map<String, String>,
        env: Map<String, String>
    ): DataSource {
        val jdbcUrl = flags["jdbc-url"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["OFFPROD_JDBC_URL"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: run {
                val host = flags["db-host"] ?: env["OFFPROD_DB_HOST"] ?: env["DB_HOST"] ?: ""
                val port = flags["db-port"] ?: env["OFFPROD_DB_PORT"] ?: env["DB_PORT"] ?: "5432"
                val name = flags["db-name"] ?: env["OFFPROD_DB_NAME"] ?: env["DB_NAME"] ?: "summoner_searcher"
                require(host.isNotBlank()) {
                    "Missing JDBC target: provide --jdbc-url or OFFPROD_JDBC_URL (or --db-host / OFFPROD_DB_HOST)"
                }
                "jdbc:postgresql://$host:$port/$name"
            }
        val dbUser = flags["db-user"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["OFFPROD_DB_USER"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: env["DB_USER"]?.trim()?.takeIf { it.isNotEmpty() }
            ?: "postgres"
        val dbPass = flags["db-pass"]
            ?: env["OFFPROD_DB_PASS"]
            ?: env["DB_PASS"]
            ?: ""

        require(jdbcUrl.startsWith("jdbc:postgresql://")) {
            "Off-production CLI requires a jdbc:postgresql:// URL (received '$jdbcUrl')"
        }
        return PGSimpleDataSource().apply {
            setURL(jdbcUrl)
            user = dbUser
            password = dbPass
        }
    }
}
