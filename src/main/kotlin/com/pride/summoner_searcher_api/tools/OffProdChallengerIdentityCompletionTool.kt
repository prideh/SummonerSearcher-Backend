package com.pride.summoner_searcher_api.tools

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import kotlin.math.ceil

/**
 * Isolated off-production one-shot tool for bounded missing-only Challenger Riot ID staging.
 *
 * IMPORTANT SCOPE & NON-COMPLETION NOTICE:
 * - This class is an isolated library component ONLY (no Spring `@Service`/`@Component`/`@Scheduled`,
 *   no CLI `main` entrypoint wired, no Flyway/schema migration applied, and `ChallengerLeagueService`
 *   read path does NOT yet query `player_riot_identities`). Writing to `player_riot_identities`
 *   stages verified identities off-production and does NOT by itself complete live Challenger
 *   leaderboard resolution.
 *
 * SAFETY & ISOLATION GUARANTEES:
 * - Defaults to no-effects mode: [ExecutionOptions.dryRun] = true, [ExecutionOptions.networkEnabled] = false,
 *   [ExecutionOptions.allowDurableWrites] = false, [ExecutionOptions.keyCoordinationMode] = UNCOORDINATED.
 * - Separates [MonotonicClock] (strictly for elapsed deadline/budget accounting and sleeps) from
 *   [WallClock] (strictly for UTC `Instant` timestamps such as `verified_at`). Never derives UTC
 *   timestamps from `System.nanoTime()`.
 * - Strict 180s ([DEFAULT_MAX_WALL_TIME_MS]) wall-time budget covers ALL stages: JDBC `DataSource.getConnection()`,
 *   pre-call read-only DB recheck, header-driven sleeps, HTTP Account-V1 attempts, durable DB writes,
 *   and post-write read verification.
 * - Fail-closed JDBC timeout configuration: if `Connection.setReadOnly`, `Connection.setNetworkTimeout`,
 *   or `PreparedStatement.setQueryTimeout` throws or is unsupported by the driver, execution aborts
 *   immediately ([RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT]) instead of silently running unbounded SQL.
 * - No uncaught `SQLException` or timeout escapes [runBounded]: any DB failure during pre-call recheck,
 *   write, or post-write verification returns a structured, resumable [CompletionRunReport].
 * - Post-write verification proof: [ItemStatus.COMPLETED_DURABLE_WRITE_VERIFIED] is only emitted when
 *   a write succeeds AND a bounded post-write read proves the non-blank identity is stored. If the
 *   write affects 0 rows because a concurrent writer already populated a non-blank identity, the post-write
 *   read classifies the item as [ItemStatus.CONCURRENT_ALREADY_COMPLETE_VERIFIED]. If post-write proof
 *   cannot confirm a valid identity, the run aborts with [RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT].
 * - Strict 78-char base64url PUUID validation (`^[A-Za-z0-9_-]{78}$`) on all manifest entries and
 *   returned payloads, preventing malformed PUUIDs or URL path injection.
 * - Production HTTP transport restricts requests strictly to `https://` on approved Riot regional routing
 *   hosts (`europe.api.riotgames.com`, `asia.api.riotgames.com`, `americas.api.riotgames.com`,
 *   `sea.api.riotgames.com`). Loopback `http://127.0.0.1` is rejected unless explicitly enabled for
 *   unit tests via `allowLoopbackHttpForTesting = true`, which in turn blocks any non-test API key
 *   (requiring prefix `TEST-LOOPBACK-ONLY-` and rejecting `RGAPI-*`) so real credentials can never leak.
 */
class OffProdChallengerIdentityCompletionTool(
    private val persistencePort: IdentityCompletionPersistencePort,
    private val httpTransport: BoundedAccountV1Transport = HttpUrlConnectionAccountV1Transport(),
    private val monotonicClock: MonotonicClock = SystemMonotonicClock,
    private val wallClock: WallClock = SystemUtcWallClock,
    private val objectMapper: ObjectMapper = defaultObjectMapper()
) {

    companion object {
        const val MAX_ALLOWED_HTTP_ATTEMPTS: Int = 191
        const val DEFAULT_MAX_WALL_TIME_MS: Long = 180_000L
        const val DEFAULT_CONNECT_TIMEOUT_MS: Int = 2_500
        const val DEFAULT_READ_TIMEOUT_MS: Int = 5_000
        const val DEFAULT_CALL_DEADLINE_MS: Long = 5_500L
        const val MAX_RESPONSE_BODY_BYTES: Int = 4_096
        const val DEFAULT_DB_OPERATION_TIMEOUT_MS: Long = 2_500L
        const val PUUID_EXACT_LENGTH: Int = 78

        val OFFICIAL_PUUID_REGEX: Regex = Regex("^[A-Za-z0-9_-]{78}$")
        val ACCOUNT_V1_PATH_REGEX: Regex = Regex("^/riot/account/v1/accounts/by-puuid/[A-Za-z0-9_-]{78}$")

        val APPROVED_RIOT_CLUSTER_HOSTS: Set<String> = setOf(
            "europe.api.riotgames.com",
            "asia.api.riotgames.com",
            "americas.api.riotgames.com",
            "sea.api.riotgames.com"
        )

        const val LOOPBACK_TEST_API_KEY_PREFIX: String = "TEST-LOOPBACK-ONLY-"

        private val VALID_PLATFORM_TO_CLUSTER: Map<String, String> = mapOf(
            "euw1" to "europe",
            "kr" to "asia",
            "na1" to "americas",
            "eun1" to "europe",
            "tr1" to "europe",
            "ru" to "europe",
            "jp1" to "asia",
            "br1" to "americas",
            "la1" to "americas",
            "la2" to "americas",
            "oc1" to "sea"
        )

        fun defaultObjectMapper(): ObjectMapper =
            ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        fun isValidOfficialPuuid(candidate: String): Boolean =
            candidate.length == PUUID_EXACT_LENGTH && OFFICIAL_PUUID_REGEX.matches(candidate)

        fun validateOfficialPuuid(candidate: String, fieldName: String = "puuid"): String {
            val trimmed = candidate.trim()
            require(isValidOfficialPuuid(trimmed)) {
                "Invalid $fieldName: must be an exact 78-character base64url string matching ^[A-Za-z0-9_-]{78}$ " +
                    "(actual length=${trimmed.length})"
            }
            return trimmed
        }

        fun computeManifestSha256(entries: List<ManifestEntry>): String {
            val digest = MessageDigest.getInstance("SHA-256")
            for (entry in entries) {
                val canonicalLine = "${entry.normalizedPlatformRegion}:${entry.normalizedPuuid}\n"
                digest.update(canonicalLine.toByteArray(StandardCharsets.UTF_8))
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        fun mapPlatformToRoutingCluster(platformRegion: String): String {
            val normalized = platformRegion.trim().lowercase(Locale.ROOT)
            return VALID_PLATFORM_TO_CLUSTER[normalized]
                ?: throw IllegalArgumentException("Unsupported platform region for Account-V1 routing: '$platformRegion'")
        }

        /**
         * Validates that [url] targets an approved `https` Riot Account-V1 regional cluster endpoint
         * with an exact 78-char base64url PUUID path and no query/fragment/userInfo.
         * When [allowLoopbackHttpForTesting] is true, permits `http://127.0.0.1:<port>` or `http://localhost:<port>`
         * ONLY if [apiKey] starts with [LOOPBACK_TEST_API_KEY_PREFIX] and does NOT start with `RGAPI-`.
         */
        fun validateTargetUrlAndKeySafety(
            url: String,
            apiKey: String,
            allowLoopbackHttpForTesting: Boolean
        ): URI {
            val uri = try {
                URI.create(url)
            } catch (e: Exception) {
                throw IllegalArgumentException("Malformed target URI: '$url'", e)
            }

            val scheme = uri.scheme?.lowercase(Locale.ROOT)
                ?: throw IllegalArgumentException("Target URI missing scheme: '$url'")
            val host = uri.host?.lowercase(Locale.ROOT)
                ?: throw IllegalArgumentException("Target URI missing host: '$url'")

            require(uri.userInfo == null) { "Target URI must not contain userInfo" }
            require(uri.rawQuery == null) { "Target URI must not contain query parameters" }
            require(uri.rawFragment == null) { "Target URI must not contain fragment" }

            val rawPath = uri.rawPath ?: throw IllegalArgumentException("Target URI missing path")
            require(ACCOUNT_V1_PATH_REGEX.matches(rawPath)) {
                "Target URI path must match ^/riot/account/v1/accounts/by-puuid/[A-Za-z0-9_-]{78}$ (was '$rawPath')"
            }

            if (scheme == "https") {
                require(host in APPROVED_RIOT_CLUSTER_HOSTS) {
                    "Disallowed HTTPS host '$host': must be one of $APPROVED_RIOT_CLUSTER_HOSTS"
                }
                require(uri.port == -1 || uri.port == 443) {
                    "Disallowed HTTPS port ${uri.port} for approved Riot host '$host'"
                }
                return uri
            }

            if (scheme == "http" && allowLoopbackHttpForTesting) {
                require(host == "127.0.0.1" || host == "localhost") {
                    "Loopback test override only permits 127.0.0.1 or localhost (was '$host')"
                }
                val trimmedKey = apiKey.trim()
                require(
                    trimmedKey.startsWith(LOOPBACK_TEST_API_KEY_PREFIX) &&
                        !trimmedKey.uppercase(Locale.ROOT).startsWith("RGAPI-") &&
                        !trimmedKey.uppercase(Locale.ROOT).contains("RGAPI-")
                ) {
                    "Refusing to send non-test or RGAPI-* credential over loopback HTTP test override"
                }
                return uri
            }

            throw IllegalArgumentException(
                "Disallowed URI scheme '$scheme' for host '$host': production transport requires https:// to $APPROVED_RIOT_CLUSTER_HOSTS"
            )
        }
    }

    enum class KeyCoordinationMode {
        /** Default fail-closed state: stops before any HTTP attempt because shared-key headers race with prod. */
        UNCOORDINATED,
        /** An isolated/dedicated Riot API key not used by the live production JVM. */
        EXCLUSIVE_KEY,
        /** Production traffic / background callers on the shared key are quiesced or coordinated externally. */
        COORDINATED_MAINTENANCE_WINDOW
    }

    data class ManifestEntry(
        val puuid: String,
        val platformRegion: String
    ) {
        val normalizedPuuid: String = validateOfficialPuuid(puuid, "ManifestEntry.puuid")
        val normalizedPlatformRegion: String = platformRegion.trim().lowercase(Locale.ROOT).also {
            require(it.isNotEmpty()) { "Manifest entry platformRegion must not be blank" }
        }
        val routingCluster: String = mapPlatformToRoutingCluster(normalizedPlatformRegion)
    }

    data class SnapshotManifest(
        val snapshotCapturedAtUtc: String,
        val isLiveLadder: Boolean = false,
        val entries: List<ManifestEntry>
    ) {
        init {
            require(!isLiveLadder) {
                "Historical snapshot manifest must not be marked as current live ladder (isLiveLadder must be false)"
            }
            require(entries.size <= MAX_ALLOWED_HTTP_ATTEMPTS) {
                "Manifest size (${entries.size}) exceeds hard ceiling of $MAX_ALLOWED_HTTP_ATTEMPTS entries"
            }
            val uniquePuuids = entries.map { it.normalizedPuuid }.toSet()
            require(uniquePuuids.size == entries.size) {
                "Manifest entries must contain unique PUUIDs (found ${entries.size - uniquePuuids.size} duplicates)"
            }
        }
    }

    data class ExecutionOptions(
        val dryRun: Boolean = true,
        val networkEnabled: Boolean = false,
        val allowDurableWrites: Boolean = false,
        val keyCoordinationMode: KeyCoordinationMode = KeyCoordinationMode.UNCOORDINATED,
        val coordinationAttestation: String = "",
        val maxTotalHttpAttempts: Int = MAX_ALLOWED_HTTP_ATTEMPTS,
        val maxWallTimeMs: Long = DEFAULT_MAX_WALL_TIME_MS,
        val connectTimeoutMs: Int = DEFAULT_CONNECT_TIMEOUT_MS,
        val readTimeoutMs: Int = DEFAULT_READ_TIMEOUT_MS,
        val callDeadlineMs: Long = DEFAULT_CALL_DEADLINE_MS,
        val dbOperationTimeoutMs: Long = DEFAULT_DB_OPERATION_TIMEOUT_MS,
        val maxBodyBytes: Int = MAX_RESPONSE_BODY_BYTES,
        val allowLoopbackHttpForTesting: Boolean = false,
        val baseUrlByCluster: Map<String, String> = mapOf(
            "europe" to "https://europe.api.riotgames.com",
            "asia" to "https://asia.api.riotgames.com",
            "americas" to "https://americas.api.riotgames.com",
            "sea" to "https://sea.api.riotgames.com"
        )
    ) {
        init {
            require(maxTotalHttpAttempts in 1..MAX_ALLOWED_HTTP_ATTEMPTS) {
                "maxTotalHttpAttempts ($maxTotalHttpAttempts) must be within 1..$MAX_ALLOWED_HTTP_ATTEMPTS"
            }
            require(maxWallTimeMs > 0L) { "maxWallTimeMs must be > 0" }
            require(connectTimeoutMs > 0) { "connectTimeoutMs must be > 0" }
            require(readTimeoutMs > 0) { "readTimeoutMs must be > 0" }
            require(callDeadlineMs > 0L) { "callDeadlineMs must be > 0" }
            require(dbOperationTimeoutMs > 0L) { "dbOperationTimeoutMs must be > 0" }
            require(maxBodyBytes in 1..MAX_RESPONSE_BODY_BYTES) {
                "maxBodyBytes ($maxBodyBytes) must be within 1..$MAX_RESPONSE_BODY_BYTES"
            }
        }
    }

    enum class PreCallDbState {
        ALREADY_COMPLETE,
        PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD,
        ABSENT_FROM_INDEXED_PLAYERS,
        UNCHECKED_BEFORE_RECHECK
    }

    enum class ItemStatus {
        SKIPPED_ALREADY_RESOLVED_IN_DB,
        VALIDATED_DRY_RUN_NO_WRITE,
        COMPLETED_DURABLE_WRITE_VERIFIED,
        CONCURRENT_ALREADY_COMPLETE_VERIFIED,
        SKIPPED_NOT_FOUND_404,
        SKIPPED_OMITTED_RIOT_ID_FIELDS,
        REJECTED_PUUID_MISMATCH,
        ABORTED_ON_HTTP_FAILURE,
        ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
        PENDING_UNPROCESSED
    }

    enum class RunOutcome {
        STAGED_ALL_CANDIDATES,
        DRY_RUN_DB_RECHECK_ONLY_COMPLETED,
        BLOCKED_KEY_COORDINATION_REQUIRED,
        BLOCKED_DURABLE_WRITES_NOT_PERMITTED,
        PARTIAL_COMPLETION_ATTEMPT_CAP_REACHED,
        PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED,
        ABORTED_MISSING_OR_INVALID_RATE_HEADERS,
        ABORTED_HTTP_429,
        ABORTED_HTTP_5XX,
        ABORTED_UNEXPECTED_HTTP_STATUS,
        ABORTED_TIMEOUT_OR_IO_ERROR,
        ABORTED_PAYLOAD_TOO_LARGE,
        ABORTED_PUUID_MISMATCH,
        ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
        ABORTED_DISALLOWED_TRANSPORT_TARGET
    }

    data class ItemResult(
        val puuid: String,
        val platformRegion: String,
        val routingCluster: String,
        val preCallDbState: PreCallDbState,
        val status: ItemStatus,
        val httpAttempted: Boolean,
        val httpStatusCode: Int? = null,
        val resolvedGameName: String? = null,
        val resolvedTagLine: String? = null,
        val verifiedAtUtc: Instant? = null,
        val durableWriteTarget: String? = null,
        val detail: String? = null
    )

    data class RateWindowUsage(
        val count: Int,
        val limit: Int,
        val windowSeconds: Int
    ) {
        val isExhausted: Boolean get() = count >= limit
    }

    data class VerifiedClusterRateState(
        val routingCluster: String,
        val appWindows: List<RateWindowUsage>,
        val methodWindows: List<RateWindowUsage>,
        val retryAfterSeconds: Long? = null,
        val rateLimitType: String? = null
    ) {
        fun requiredWaitMsBeforeNextAttempt(): Long {
            val retryWaitMs = (retryAfterSeconds ?: 0L).coerceAtLeast(0L) * 1_000L
            val exhaustedAppWaitMs = appWindows
                .filter { it.isExhausted }
                .maxOfOrNull { it.windowSeconds.toLong() * 1_000L } ?: 0L
            val exhaustedMethodWaitMs = methodWindows
                .filter { it.isExhausted }
                .maxOfOrNull { it.windowSeconds.toLong() * 1_000L } ?: 0L
            return maxOf(retryWaitMs, exhaustedAppWaitMs, exhaustedMethodWaitMs)
        }
    }

    data class CompletionRunReport(
        val outcome: RunOutcome,
        val manifestSha256: String,
        val snapshotCapturedAtUtc: String,
        val dryRun: Boolean,
        val networkEnabled: Boolean,
        val allowDurableWrites: Boolean,
        val keyCoordinationMode: KeyCoordinationMode,
        val manifestTotalCount: Int,
        val alreadyResolvedInDbCount: Int,
        val presentWithBlankFieldInIndexedPlayersCount: Int,
        val absentFromIndexedPlayersCount: Int,
        val httpAttemptsMade: Int,
        val maxTotalHttpAttempts: Int,
        val totalWaitTimeMs: Long,
        val elapsedWallTimeMs: Long,
        val validatedIdentitiesCount: Int,
        val durableIndexedPlayerUpdatesVerifiedCount: Int,
        val durableIdentityTableUpsertsVerifiedCount: Int,
        val concurrentAlreadyCompleteVerifiedCount: Int,
        val dbFailureOrUnknownCommitCount: Int,
        val notFound404Count: Int,
        val omittedRiotIdFieldsCount: Int,
        val puuidMismatchCount: Int,
        val remainingPendingCount: Int,
        val remainingEntries: List<ManifestEntry>,
        val itemResults: List<ItemResult>,
        val lastObservedClusterHeaders: Map<String, VerifiedClusterRateState>,
        val abortReason: String? = null
    )

    /**
     * Execute the bounded single-worker off-production staging workflow against [manifest] with [options].
     * Never throws SQLException or timeout exceptions; always returns a structured [CompletionRunReport].
     */
    fun runBounded(
        manifest: SnapshotManifest,
        options: ExecutionOptions = ExecutionOptions(),
        riotApiKeyProvider: () -> String = { "" }
    ): CompletionRunReport {
        val startMonoMs = monotonicClock.nowMs()
        var totalWaitMs = 0L
        val manifestHash = computeManifestSha256(manifest.entries)

        fun remainingWallBudgetMs(): Long =
            options.maxWallTimeMs - (monotonicClock.nowMs() - startMonoMs)

        if (!options.dryRun && !options.allowDurableWrites) {
            return buildEarlyReport(
                outcome = RunOutcome.BLOCKED_DURABLE_WRITES_NOT_PERMITTED,
                manifest = manifest,
                manifestHash = manifestHash,
                options = options,
                elapsedMs = monotonicClock.nowMs() - startMonoMs,
                reason = "dryRun=false requires explicit allowDurableWrites=true and a write-capable DB credential"
            )
        }

        val initialBudgetMs = remainingWallBudgetMs()
        if (initialBudgetMs <= 0L) {
            return buildEarlyReport(
                outcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED,
                manifest = manifest,
                manifestHash = manifestHash,
                options = options,
                elapsedMs = monotonicClock.nowMs() - startMonoMs,
                reason = "Wall-time budget (${options.maxWallTimeMs}ms) exhausted before pre-call DB recheck"
            )
        }

        // Step 1: Bounded pre-call read-only DB recheck (including connection acquisition) within remaining budget.
        val recheckTimeoutMs = minOf(options.dbOperationTimeoutMs, initialBudgetMs)
        val recheckResult: DbRecheckResult = try {
            persistencePort.recheckExistingIdentitiesBounded(
                entries = manifest.entries,
                timeoutMs = recheckTimeoutMs
            )
        } catch (e: Exception) {
            DbRecheckResult.Failure("Unhandled exception during pre-call DB recheck: ${e.javaClass.simpleName}: ${e.message}")
        }

        val dbStatesByPuuid: Map<String, PreCallDbState> = when (recheckResult) {
            is DbRecheckResult.Success -> recheckResult.statesByPuuid
            is DbRecheckResult.Failure -> {
                val outcome = if (recheckResult.timedOutOrBudgetExhausted && remainingWallBudgetMs() <= 0L) {
                    RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                } else {
                    RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT
                }
                return buildEarlyReport(
                    outcome = outcome,
                    manifest = manifest,
                    manifestHash = manifestHash,
                    options = options,
                    elapsedMs = monotonicClock.nowMs() - startMonoMs,
                    reason = recheckResult.reason
                )
            }
        }

        val itemResults = ArrayList<ItemResult>(manifest.entries.size)
        val candidatesToFetch = ArrayList<ManifestEntry>(manifest.entries.size)

        var alreadyResolvedCount = 0
        var presentWithBlankCount = 0
        var absentCount = 0

        for (entry in manifest.entries) {
            val state = dbStatesByPuuid[entry.normalizedPuuid] ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
            when (state) {
                PreCallDbState.ALREADY_COMPLETE -> {
                    alreadyResolvedCount++
                    itemResults.add(
                        ItemResult(
                            puuid = entry.normalizedPuuid,
                            platformRegion = entry.normalizedPlatformRegion,
                            routingCluster = entry.routingCluster,
                            preCallDbState = state,
                            status = ItemStatus.SKIPPED_ALREADY_RESOLVED_IN_DB,
                            httpAttempted = false,
                            detail = "Complete non-blank game_name and tag_line already present in DB"
                        )
                    )
                }
                PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD -> {
                    presentWithBlankCount++
                    candidatesToFetch.add(entry)
                }
                PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS,
                PreCallDbState.UNCHECKED_BEFORE_RECHECK -> {
                    absentCount++
                    candidatesToFetch.add(entry)
                }
            }
        }

        if (candidatesToFetch.isEmpty()) {
            return CompletionRunReport(
                outcome = RunOutcome.STAGED_ALL_CANDIDATES,
                manifestSha256 = manifestHash,
                snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
                dryRun = options.dryRun,
                networkEnabled = options.networkEnabled,
                allowDurableWrites = options.allowDurableWrites,
                keyCoordinationMode = options.keyCoordinationMode,
                manifestTotalCount = manifest.entries.size,
                alreadyResolvedInDbCount = alreadyResolvedCount,
                presentWithBlankFieldInIndexedPlayersCount = presentWithBlankCount,
                absentFromIndexedPlayersCount = absentCount,
                httpAttemptsMade = 0,
                maxTotalHttpAttempts = options.maxTotalHttpAttempts,
                totalWaitTimeMs = 0L,
                elapsedWallTimeMs = monotonicClock.nowMs() - startMonoMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesVerifiedCount = 0,
                durableIdentityTableUpsertsVerifiedCount = 0,
                concurrentAlreadyCompleteVerifiedCount = 0,
                dbFailureOrUnknownCommitCount = 0,
                notFound404Count = 0,
                omittedRiotIdFieldsCount = 0,
                puuidMismatchCount = 0,
                remainingPendingCount = 0,
                remainingEntries = emptyList(),
                itemResults = itemResults,
                lastObservedClusterHeaders = emptyMap()
            )
        }

        if (!options.networkEnabled) {
            for (pending in candidatesToFetch) {
                val state = dbStatesByPuuid[pending.normalizedPuuid] ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
                itemResults.add(
                    ItemResult(
                        puuid = pending.normalizedPuuid,
                        platformRegion = pending.normalizedPlatformRegion,
                        routingCluster = pending.routingCluster,
                        preCallDbState = state,
                        status = ItemStatus.PENDING_UNPROCESSED,
                        httpAttempted = false,
                        detail = "networkEnabled=false (DB recheck only)"
                    )
                )
            }
            return CompletionRunReport(
                outcome = RunOutcome.DRY_RUN_DB_RECHECK_ONLY_COMPLETED,
                manifestSha256 = manifestHash,
                snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
                dryRun = options.dryRun,
                networkEnabled = false,
                allowDurableWrites = options.allowDurableWrites,
                keyCoordinationMode = options.keyCoordinationMode,
                manifestTotalCount = manifest.entries.size,
                alreadyResolvedInDbCount = alreadyResolvedCount,
                presentWithBlankFieldInIndexedPlayersCount = presentWithBlankCount,
                absentFromIndexedPlayersCount = absentCount,
                httpAttemptsMade = 0,
                maxTotalHttpAttempts = options.maxTotalHttpAttempts,
                totalWaitTimeMs = 0L,
                elapsedWallTimeMs = monotonicClock.nowMs() - startMonoMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesVerifiedCount = 0,
                durableIdentityTableUpsertsVerifiedCount = 0,
                concurrentAlreadyCompleteVerifiedCount = 0,
                dbFailureOrUnknownCommitCount = 0,
                notFound404Count = 0,
                omittedRiotIdFieldsCount = 0,
                puuidMismatchCount = 0,
                remainingPendingCount = candidatesToFetch.size,
                remainingEntries = candidatesToFetch,
                itemResults = itemResults,
                lastObservedClusterHeaders = emptyMap()
            )
        }

        // Step 2: Enforce explicit key coordination before any HTTP call is made.
        if (options.keyCoordinationMode == KeyCoordinationMode.UNCOORDINATED ||
            options.coordinationAttestation.isBlank()
        ) {
            for (pending in candidatesToFetch) {
                val state = dbStatesByPuuid[pending.normalizedPuuid] ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
                itemResults.add(
                    ItemResult(
                        puuid = pending.normalizedPuuid,
                        platformRegion = pending.normalizedPlatformRegion,
                        routingCluster = pending.routingCluster,
                        preCallDbState = state,
                        status = ItemStatus.PENDING_UNPROCESSED,
                        httpAttempted = false,
                        detail = "Blocked: exclusive or coordinated Riot API key budget attestation required"
                    )
                )
            }
            return CompletionRunReport(
                outcome = RunOutcome.BLOCKED_KEY_COORDINATION_REQUIRED,
                manifestSha256 = manifestHash,
                snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
                dryRun = options.dryRun,
                networkEnabled = options.networkEnabled,
                allowDurableWrites = options.allowDurableWrites,
                keyCoordinationMode = options.keyCoordinationMode,
                manifestTotalCount = manifest.entries.size,
                alreadyResolvedInDbCount = alreadyResolvedCount,
                presentWithBlankFieldInIndexedPlayersCount = presentWithBlankCount,
                absentFromIndexedPlayersCount = absentCount,
                httpAttemptsMade = 0,
                maxTotalHttpAttempts = options.maxTotalHttpAttempts,
                totalWaitTimeMs = 0L,
                elapsedWallTimeMs = monotonicClock.nowMs() - startMonoMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesVerifiedCount = 0,
                durableIdentityTableUpsertsVerifiedCount = 0,
                concurrentAlreadyCompleteVerifiedCount = 0,
                dbFailureOrUnknownCommitCount = 0,
                notFound404Count = 0,
                omittedRiotIdFieldsCount = 0,
                puuidMismatchCount = 0,
                remainingPendingCount = candidatesToFetch.size,
                remainingEntries = candidatesToFetch,
                itemResults = itemResults,
                lastObservedClusterHeaders = emptyMap(),
                abortReason = "Uncoordinated shared Riot API key: header usage from an external process races with production JVM. Provide EXCLUSIVE_KEY or COORDINATED_MAINTENANCE_WINDOW with non-blank attestation."
            )
        }

        val apiKey = riotApiKeyProvider().trim()
        require(apiKey.isNotBlank()) { "Riot API key must not be blank when networkEnabled=true" }

        // Step 3: Single-worker serial Account-V1 loop.
        val verifiedClusterStates = LinkedHashMap<String, VerifiedClusterRateState>()
        var httpAttemptsMade = 0
        var validatedIdentitiesCount = 0
        var durableIndexedPlayerUpdatesVerifiedCount = 0
        var durableIdentityTableUpsertsVerifiedCount = 0
        var concurrentAlreadyCompleteVerifiedCount = 0
        var dbFailureOrUnknownCommitCount = 0
        var notFound404Count = 0
        var omittedRiotIdFieldsCount = 0
        var puuidMismatchCount = 0

        var index = 0
        var terminalOutcome: RunOutcome? = null
        var abortReason: String? = null

        while (index < candidatesToFetch.size) {
            val candidate = candidatesToFetch[index]
            val preCallState = dbStatesByPuuid[candidate.normalizedPuuid]
                ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS

            if (httpAttemptsMade >= options.maxTotalHttpAttempts) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_ATTEMPT_CAP_REACHED
                abortReason = "Reached maxTotalHttpAttempts ceiling (${options.maxTotalHttpAttempts})"
                break
            }

            val clusterState = verifiedClusterStates[candidate.routingCluster]
            val requiredWaitMs = clusterState?.requiredWaitMsBeforeNextAttempt() ?: 0L

            val remainingBeforeWaitMs = remainingWallBudgetMs()
            if (remainingBeforeWaitMs <= 0L || requiredWaitMs >= remainingBeforeWaitMs) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                abortReason = if (requiredWaitMs > 0L) {
                    "Required header-driven wait (${requiredWaitMs}ms) on cluster '${candidate.routingCluster}' exceeds remaining wall-time budget (${remainingBeforeWaitMs.coerceAtLeast(0L)}ms)"
                } else {
                    "Wall-time budget (${options.maxWallTimeMs}ms) exhausted"
                }
                break
            }

            if (requiredWaitMs > 0L) {
                monotonicClock.sleepMs(requiredWaitMs)
                totalWaitMs += requiredWaitMs
            }

            val remainingForCallMs = remainingWallBudgetMs()
            if (remainingForCallMs <= 0L) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                abortReason = "Wall-time budget (${options.maxWallTimeMs}ms) exhausted before HTTP dispatch"
                break
            }

            val baseUrl = options.baseUrlByCluster[candidate.routingCluster]?.trim()?.trimEnd('/')
                ?: throw IllegalArgumentException("Missing base URL for routing cluster '${candidate.routingCluster}'")
            val targetUrl = "$baseUrl/riot/account/v1/accounts/by-puuid/${candidate.normalizedPuuid}"

            // Validate target URL scheme/host/path and loopback credential safety BEFORE counting or dispatching.
            try {
                validateTargetUrlAndKeySafety(
                    url = targetUrl,
                    apiKey = apiKey,
                    allowLoopbackHttpForTesting = options.allowLoopbackHttpForTesting
                )
            } catch (e: IllegalArgumentException) {
                terminalOutcome = RunOutcome.ABORTED_DISALLOWED_TRANSPORT_TARGET
                abortReason = e.message ?: "Disallowed transport target or unsafe loopback credential"
                break
            }

            val effectiveDeadlineMs = minOf(options.callDeadlineMs, remainingForCallMs)

            // Every HTTP attempt (including the first discovery call per cluster) counts against the 191 ceiling.
            httpAttemptsMade++

            val transportResult = httpTransport.executeSingleGet(
                url = targetUrl,
                apiKey = apiKey,
                connectTimeoutMs = minOf(options.connectTimeoutMs.toLong(), effectiveDeadlineMs).toInt().coerceAtLeast(1),
                readTimeoutMs = minOf(options.readTimeoutMs.toLong(), effectiveDeadlineMs).toInt().coerceAtLeast(1),
                callDeadlineMs = effectiveDeadlineMs,
                maxBodyBytes = options.maxBodyBytes,
                allowLoopbackHttpForTesting = options.allowLoopbackHttpForTesting
            )

            when (transportResult) {
                is TransportResult.TimeoutOrIoError -> {
                    itemResults.add(
                        ItemResult(
                            puuid = candidate.normalizedPuuid,
                            platformRegion = candidate.normalizedPlatformRegion,
                            routingCluster = candidate.routingCluster,
                            preCallDbState = preCallState,
                            status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                            httpAttempted = true,
                            detail = transportResult.message
                        )
                    )
                    // Do NOT increment index: candidate did not complete and must remain in remainingEntries!
                    terminalOutcome = RunOutcome.ABORTED_TIMEOUT_OR_IO_ERROR
                    abortReason = transportResult.message
                    break
                }

                is TransportResult.PayloadTooLarge -> {
                    itemResults.add(
                        ItemResult(
                            puuid = candidate.normalizedPuuid,
                            platformRegion = candidate.normalizedPlatformRegion,
                            routingCluster = candidate.routingCluster,
                            preCallDbState = preCallState,
                            status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                            httpAttempted = true,
                            httpStatusCode = transportResult.statusCode,
                            detail = "Response body exceeded ${options.maxBodyBytes} bytes cap"
                        )
                    )
                    terminalOutcome = RunOutcome.ABORTED_PAYLOAD_TOO_LARGE
                    abortReason = "Response body for PUUID '${candidate.normalizedPuuid}' exceeded ${options.maxBodyBytes} bytes cap"
                    break
                }

                is TransportResult.HttpResponse -> {
                    val status = transportResult.statusCode
                    val headers = transportResult.headersLowercase

                    if (status == 429) {
                        val parsedHeaders = parseVerifiedClusterHeadersOrNull(candidate.routingCluster, headers)
                        if (parsedHeaders != null) {
                            verifiedClusterStates[candidate.routingCluster] = parsedHeaders
                        }
                        val retryAfterHeader = headers["retry-after"]?.trim()
                        val rateLimitType = headers["x-rate-limit-type"]?.trim()
                        itemResults.add(
                            ItemResult(
                                puuid = candidate.normalizedPuuid,
                                platformRegion = candidate.normalizedPlatformRegion,
                                routingCluster = candidate.routingCluster,
                                preCallDbState = preCallState,
                                status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                                httpAttempted = true,
                                httpStatusCode = 429,
                                detail = "HTTP 429 (type=${rateLimitType ?: "unknown"}, Retry-After=${retryAfterHeader ?: "none"})"
                            )
                        )
                        terminalOutcome = RunOutcome.ABORTED_HTTP_429
                        abortReason = "Aborted on HTTP 429 for cluster '${candidate.routingCluster}' (type=${rateLimitType ?: "unknown"}, Retry-After=${retryAfterHeader ?: "none"})"
                        break
                    }

                    if (status in 500..599) {
                        itemResults.add(
                            ItemResult(
                                puuid = candidate.normalizedPuuid,
                                platformRegion = candidate.normalizedPlatformRegion,
                                routingCluster = candidate.routingCluster,
                                preCallDbState = preCallState,
                                status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                                httpAttempted = true,
                                httpStatusCode = status,
                                detail = "HTTP $status server error"
                            )
                        )
                        terminalOutcome = RunOutcome.ABORTED_HTTP_5XX
                        abortReason = "Aborted on HTTP $status from cluster '${candidate.routingCluster}'"
                        break
                    }

                    val verifiedHeaders = parseVerifiedClusterHeadersOrNull(candidate.routingCluster, headers)
                    if (verifiedHeaders == null) {
                        itemResults.add(
                            ItemResult(
                                puuid = candidate.normalizedPuuid,
                                platformRegion = candidate.normalizedPlatformRegion,
                                routingCluster = candidate.routingCluster,
                                preCallDbState = preCallState,
                                status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                                httpAttempted = true,
                                httpStatusCode = status,
                                detail = "Missing or malformed X-App-Rate-Limit* or X-Method-Rate-Limit* headers"
                            )
                        )
                        terminalOutcome = RunOutcome.ABORTED_MISSING_OR_INVALID_RATE_HEADERS
                        abortReason = "Missing or malformed X-App-Rate-Limit* / X-Method-Rate-Limit* headers on cluster '${candidate.routingCluster}'"
                        break
                    }
                    verifiedClusterStates[candidate.routingCluster] = verifiedHeaders

                    when (status) {
                        404 -> {
                            notFound404Count++
                            itemResults.add(
                                ItemResult(
                                    puuid = candidate.normalizedPuuid,
                                    platformRegion = candidate.normalizedPlatformRegion,
                                    routingCluster = candidate.routingCluster,
                                    preCallDbState = preCallState,
                                    status = ItemStatus.SKIPPED_NOT_FOUND_404,
                                    httpAttempted = true,
                                    httpStatusCode = 404,
                                    detail = "Account-V1 returned 404 Not Found"
                                )
                            )
                            index++
                        }

                        200 -> {
                            val parsedPayload = parseAccountPayloadOrNull(transportResult.bodyBytes)
                            if (parsedPayload == null) {
                                itemResults.add(
                                    ItemResult(
                                        puuid = candidate.normalizedPuuid,
                                        platformRegion = candidate.normalizedPlatformRegion,
                                        routingCluster = candidate.routingCluster,
                                        preCallDbState = preCallState,
                                        status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                                        httpAttempted = true,
                                        httpStatusCode = 200,
                                        detail = "Malformed JSON body from Account-V1"
                                    )
                                )
                                terminalOutcome = RunOutcome.ABORTED_TIMEOUT_OR_IO_ERROR
                                abortReason = "Malformed JSON body returned on HTTP 200 for PUUID '${candidate.normalizedPuuid}'"
                                break
                            }

                            val returnedPuuid = parsedPayload.puuid?.trim().orEmpty()
                            if (!isValidOfficialPuuid(returnedPuuid) || returnedPuuid != candidate.normalizedPuuid) {
                                puuidMismatchCount++
                                itemResults.add(
                                    ItemResult(
                                        puuid = candidate.normalizedPuuid,
                                        platformRegion = candidate.normalizedPlatformRegion,
                                        routingCluster = candidate.routingCluster,
                                        preCallDbState = preCallState,
                                        status = ItemStatus.REJECTED_PUUID_MISMATCH,
                                        httpAttempted = true,
                                        httpStatusCode = 200,
                                        detail = "Account-V1 returned mismatched or invalid 78-char puuid '$returnedPuuid'"
                                    )
                                )
                                terminalOutcome = RunOutcome.ABORTED_PUUID_MISMATCH
                                abortReason = "Account-V1 PUUID mismatch: requested '${candidate.normalizedPuuid}', received '$returnedPuuid'"
                                break
                            }

                            val gameName = parsedPayload.gameName?.trim().orEmpty()
                            val tagLine = parsedPayload.tagLine?.trim().orEmpty()

                            if (gameName.isEmpty() || tagLine.isEmpty()) {
                                omittedRiotIdFieldsCount++
                                itemResults.add(
                                    ItemResult(
                                        puuid = candidate.normalizedPuuid,
                                        platformRegion = candidate.normalizedPlatformRegion,
                                        routingCluster = candidate.routingCluster,
                                        preCallDbState = preCallState,
                                        status = ItemStatus.SKIPPED_OMITTED_RIOT_ID_FIELDS,
                                        httpAttempted = true,
                                        httpStatusCode = 200,
                                        detail = "Account-V1 returned 200 with omitted/blank gameName or tagLine"
                                    )
                                )
                                index++
                            } else {
                                validatedIdentitiesCount++
                                val verifiedAtUtc: Instant = wallClock.nowInstant()

                                if (options.dryRun) {
                                    itemResults.add(
                                        ItemResult(
                                            puuid = candidate.normalizedPuuid,
                                            platformRegion = candidate.normalizedPlatformRegion,
                                            routingCluster = candidate.routingCluster,
                                            preCallDbState = preCallState,
                                            status = ItemStatus.VALIDATED_DRY_RUN_NO_WRITE,
                                            httpAttempted = true,
                                            httpStatusCode = 200,
                                            resolvedGameName = gameName,
                                            resolvedTagLine = tagLine,
                                            verifiedAtUtc = verifiedAtUtc,
                                            durableWriteTarget = "NONE_DRY_RUN",
                                            detail = "Validated non-blank Riot ID in dryRun mode (0 writes executed)"
                                        )
                                    )
                                    index++
                                } else {
                                    // Check remaining wall-time budget BEFORE starting the durable DB write!
                                    val remainingForWriteMs = remainingWallBudgetMs()
                                    if (remainingForWriteMs <= 0L) {
                                        // Do not start write after budget exhaustion; leave current candidate in remainingEntries!
                                        terminalOutcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                                        abortReason = "Wall-time budget (${options.maxWallTimeMs}ms) exhausted before durable DB write for '${candidate.normalizedPuuid}'"
                                        break
                                    }

                                    val effectiveDbWriteTimeoutMs = minOf(options.dbOperationTimeoutMs, remainingForWriteMs)
                                    val writeVerification = try {
                                        persistencePort.persistAndVerifyIdentityBounded(
                                            puuid = candidate.normalizedPuuid,
                                            platformRegion = candidate.normalizedPlatformRegion,
                                            gameName = gameName,
                                            tagLine = tagLine,
                                            verifiedAt = verifiedAtUtc,
                                            preCallDbState = preCallState,
                                            timeoutMs = effectiveDbWriteTimeoutMs
                                        )
                                    } catch (e: Exception) {
                                        DbWriteVerificationResult.FailureOrUnknownCommit(
                                            reason = "Unhandled DB write exception: ${e.javaClass.simpleName}: ${e.message}"
                                        )
                                    }

                                    when (writeVerification) {
                                        is DbWriteVerificationResult.WrittenAndVerified -> {
                                            if (writeVerification.indexedPlayerBlankFieldUpdated) {
                                                durableIndexedPlayerUpdatesVerifiedCount++
                                            }
                                            if (writeVerification.identityOnlyTableUpserted) {
                                                durableIdentityTableUpsertsVerifiedCount++
                                            }
                                            itemResults.add(
                                                ItemResult(
                                                    puuid = candidate.normalizedPuuid,
                                                    platformRegion = candidate.normalizedPlatformRegion,
                                                    routingCluster = candidate.routingCluster,
                                                    preCallDbState = preCallState,
                                                    status = ItemStatus.COMPLETED_DURABLE_WRITE_VERIFIED,
                                                    httpAttempted = true,
                                                    httpStatusCode = 200,
                                                    resolvedGameName = writeVerification.verifiedGameName,
                                                    resolvedTagLine = writeVerification.verifiedTagLine,
                                                    verifiedAtUtc = writeVerification.verifiedAtUtc,
                                                    durableWriteTarget = writeVerification.writeTargetDescription,
                                                    detail = "Durable write verified by post-write read"
                                                )
                                            )
                                            index++
                                        }

                                        is DbWriteVerificationResult.ConcurrentAlreadyCompleteVerified -> {
                                            concurrentAlreadyCompleteVerifiedCount++
                                            itemResults.add(
                                                ItemResult(
                                                    puuid = candidate.normalizedPuuid,
                                                    platformRegion = candidate.normalizedPlatformRegion,
                                                    routingCluster = candidate.routingCluster,
                                                    preCallDbState = preCallState,
                                                    status = ItemStatus.CONCURRENT_ALREADY_COMPLETE_VERIFIED,
                                                    httpAttempted = true,
                                                    httpStatusCode = 200,
                                                    resolvedGameName = writeVerification.verifiedGameName,
                                                    resolvedTagLine = writeVerification.verifiedTagLine,
                                                    verifiedAtUtc = writeVerification.verifiedAtUtc,
                                                    durableWriteTarget = "PRESERVED_CONCURRENT_COMPLETE_ROW",
                                                    detail = "0 rows mutated; post-write read verified complete non-blank identity already present"
                                                )
                                            )
                                            index++
                                        }

                                        is DbWriteVerificationResult.FailureOrUnknownCommit -> {
                                            dbFailureOrUnknownCommitCount++
                                            itemResults.add(
                                                ItemResult(
                                                    puuid = candidate.normalizedPuuid,
                                                    platformRegion = candidate.normalizedPlatformRegion,
                                                    routingCluster = candidate.routingCluster,
                                                    preCallDbState = preCallState,
                                                    status = ItemStatus.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT,
                                                    httpAttempted = true,
                                                    httpStatusCode = 200,
                                                    resolvedGameName = gameName,
                                                    resolvedTagLine = tagLine,
                                                    verifiedAtUtc = verifiedAtUtc,
                                                    durableWriteTarget = "FAILED_OR_UNKNOWN_COMMIT",
                                                    detail = writeVerification.reason
                                                )
                                            )
                                            // Do NOT increment index: this candidate did NOT complete durable persistence
                                            // and must remain in remainingEntries (pending >= 1)!
                                            terminalOutcome = RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT
                                            abortReason = writeVerification.reason
                                            break
                                        }
                                    }
                                }
                            }
                        }

                        else -> {
                            itemResults.add(
                                ItemResult(
                                    puuid = candidate.normalizedPuuid,
                                    platformRegion = candidate.normalizedPlatformRegion,
                                    routingCluster = candidate.routingCluster,
                                    preCallDbState = preCallState,
                                    status = ItemStatus.ABORTED_ON_HTTP_FAILURE,
                                    httpAttempted = true,
                                    httpStatusCode = status,
                                    detail = "Unexpected HTTP status $status"
                                )
                            )
                            terminalOutcome = RunOutcome.ABORTED_UNEXPECTED_HTTP_STATUS
                            abortReason = "Unexpected HTTP status $status on cluster '${candidate.routingCluster}'"
                            break
                        }
                    }
                }
            }
        }

        val alreadyRecordedPuuids = itemResults.map { it.puuid }.toHashSet()
        val remainingEntries = ArrayList<ManifestEntry>()
        while (index < candidatesToFetch.size) {
            val pending = candidatesToFetch[index]
            remainingEntries.add(pending)
            if (pending.normalizedPuuid !in alreadyRecordedPuuids) {
                val state = dbStatesByPuuid[pending.normalizedPuuid] ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
                itemResults.add(
                    ItemResult(
                        puuid = pending.normalizedPuuid,
                        platformRegion = pending.normalizedPlatformRegion,
                        routingCluster = pending.routingCluster,
                        preCallDbState = state,
                        status = ItemStatus.PENDING_UNPROCESSED,
                        httpAttempted = false,
                        detail = abortReason ?: "Unprocessed due to budget stop"
                    )
                )
            }
            index++
        }

        val finalOutcome = terminalOutcome ?: RunOutcome.STAGED_ALL_CANDIDATES

        return CompletionRunReport(
            outcome = finalOutcome,
            manifestSha256 = manifestHash,
            snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
            dryRun = options.dryRun,
            networkEnabled = options.networkEnabled,
            allowDurableWrites = options.allowDurableWrites,
            keyCoordinationMode = options.keyCoordinationMode,
            manifestTotalCount = manifest.entries.size,
            alreadyResolvedInDbCount = alreadyResolvedCount,
            presentWithBlankFieldInIndexedPlayersCount = presentWithBlankCount,
            absentFromIndexedPlayersCount = absentCount,
            httpAttemptsMade = httpAttemptsMade,
            maxTotalHttpAttempts = options.maxTotalHttpAttempts,
            totalWaitTimeMs = totalWaitMs,
            elapsedWallTimeMs = monotonicClock.nowMs() - startMonoMs,
            validatedIdentitiesCount = validatedIdentitiesCount,
            durableIndexedPlayerUpdatesVerifiedCount = durableIndexedPlayerUpdatesVerifiedCount,
            durableIdentityTableUpsertsVerifiedCount = durableIdentityTableUpsertsVerifiedCount,
            concurrentAlreadyCompleteVerifiedCount = concurrentAlreadyCompleteVerifiedCount,
            dbFailureOrUnknownCommitCount = dbFailureOrUnknownCommitCount,
            notFound404Count = notFound404Count,
            omittedRiotIdFieldsCount = omittedRiotIdFieldsCount,
            puuidMismatchCount = puuidMismatchCount,
            remainingPendingCount = remainingEntries.size,
            remainingEntries = remainingEntries,
            itemResults = itemResults,
            lastObservedClusterHeaders = verifiedClusterStates.toMap(),
            abortReason = abortReason
        )
    }

    private fun buildEarlyReport(
        outcome: RunOutcome,
        manifest: SnapshotManifest,
        manifestHash: String,
        options: ExecutionOptions,
        elapsedMs: Long,
        reason: String
    ): CompletionRunReport {
        val pendingItems = manifest.entries.map { entry ->
            ItemResult(
                puuid = entry.normalizedPuuid,
                platformRegion = entry.normalizedPlatformRegion,
                routingCluster = entry.routingCluster,
                preCallDbState = PreCallDbState.UNCHECKED_BEFORE_RECHECK,
                status = ItemStatus.PENDING_UNPROCESSED,
                httpAttempted = false,
                detail = reason
            )
        }
        return CompletionRunReport(
            outcome = outcome,
            manifestSha256 = manifestHash,
            snapshotCapturedAtUtc = manifest.snapshotCapturedAtUtc,
            dryRun = options.dryRun,
            networkEnabled = options.networkEnabled,
            allowDurableWrites = options.allowDurableWrites,
            keyCoordinationMode = options.keyCoordinationMode,
            manifestTotalCount = manifest.entries.size,
            alreadyResolvedInDbCount = 0,
            presentWithBlankFieldInIndexedPlayersCount = 0,
            absentFromIndexedPlayersCount = 0,
            httpAttemptsMade = 0,
            maxTotalHttpAttempts = options.maxTotalHttpAttempts,
            totalWaitTimeMs = 0L,
            elapsedWallTimeMs = elapsedMs,
            validatedIdentitiesCount = 0,
            durableIndexedPlayerUpdatesVerifiedCount = 0,
            durableIdentityTableUpsertsVerifiedCount = 0,
            concurrentAlreadyCompleteVerifiedCount = 0,
            dbFailureOrUnknownCommitCount = if (outcome == RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT) 1 else 0,
            notFound404Count = 0,
            omittedRiotIdFieldsCount = 0,
            puuidMismatchCount = 0,
            remainingPendingCount = manifest.entries.size,
            remainingEntries = manifest.entries,
            itemResults = pendingItems,
            lastObservedClusterHeaders = emptyMap(),
            abortReason = reason
        )
    }

    private data class RawAccountDto(
        val puuid: String? = null,
        val gameName: String? = null,
        val tagLine: String? = null
    )

    private fun parseAccountPayloadOrNull(bodyBytes: ByteArray): RawAccountDto? {
        if (bodyBytes.isEmpty()) return null
        return try {
            objectMapper.readValue(bodyBytes, RawAccountDto::class.java)
        } catch (_: Exception) {
            null
        }
    }

    internal fun parseVerifiedClusterHeadersOrNull(
        routingCluster: String,
        headersLowercase: Map<String, String>
    ): VerifiedClusterRateState? {
        val appLimitRaw = headersLowercase["x-app-rate-limit"]?.trim().orEmpty()
        val appCountRaw = headersLowercase["x-app-rate-limit-count"]?.trim().orEmpty()
        val methodLimitRaw = headersLowercase["x-method-rate-limit"]?.trim().orEmpty()
        val methodCountRaw = headersLowercase["x-method-rate-limit-count"]?.trim().orEmpty()

        if (appLimitRaw.isEmpty() || appCountRaw.isEmpty() || methodLimitRaw.isEmpty() || methodCountRaw.isEmpty()) {
            return null
        }

        val appWindows = parseWindowPairsOrNull(appLimitRaw, appCountRaw) ?: return null
        val methodWindows = parseWindowPairsOrNull(methodLimitRaw, methodCountRaw) ?: return null

        val retryAfterSeconds = headersLowercase["retry-after"]?.trim()?.let { raw ->
            raw.toLongOrNull()?.takeIf { it >= 0L } ?: return null
        }
        val rateLimitType = headersLowercase["x-rate-limit-type"]?.trim()
        val effectiveRetryAfter = if (rateLimitType.equals("service", ignoreCase = true)) {
            maxOf(retryAfterSeconds ?: 0L, 10L)
        } else {
            retryAfterSeconds
        }

        return VerifiedClusterRateState(
            routingCluster = routingCluster,
            appWindows = appWindows,
            methodWindows = methodWindows,
            retryAfterSeconds = effectiveRetryAfter,
            rateLimitType = rateLimitType
        )
    }

    private fun parseWindowPairsOrNull(limitHeader: String, countHeader: String): List<RateWindowUsage>? {
        val limitsByWindow = parseHeaderMapOrNull(limitHeader, requirePositiveFirstNumber = true) ?: return null
        val countsByWindow = parseHeaderMapOrNull(countHeader, requirePositiveFirstNumber = false) ?: return null
        if (limitsByWindow.isEmpty() || limitsByWindow.keys != countsByWindow.keys) {
            return null
        }
        return limitsByWindow.entries.map { (windowSeconds, limit) ->
            val count = countsByWindow[windowSeconds] ?: return null
            RateWindowUsage(count = count, limit = limit, windowSeconds = windowSeconds)
        }
    }

    private fun parseHeaderMapOrNull(header: String, requirePositiveFirstNumber: Boolean): Map<Int, Int>? {
        val result = LinkedHashMap<Int, Int>()
        val tokens = header.split(",")
        for (token in tokens) {
            val trimmed = token.trim()
            if (trimmed.isEmpty()) return null
            val parts = trimmed.split(":")
            if (parts.size != 2) return null
            val value = parts[0].trim().toIntOrNull() ?: return null
            val windowSeconds = parts[1].trim().toIntOrNull() ?: return null
            if (windowSeconds <= 0) return null
            if (requirePositiveFirstNumber && value <= 0) return null
            if (!requirePositiveFirstNumber && value < 0) return null
            if (result.put(windowSeconds, value) != null) return null
        }
        return if (result.isEmpty()) null else result
    }
}

/**
 * Monotonic clock strictly for elapsed wall-time budget and sleep accounting.
 * Must NEVER be converted to a calendar/UTC [Instant].
 */
interface MonotonicClock {
    fun nowMs(): Long
    fun sleepMs(durationMs: Long)
}

object SystemMonotonicClock : MonotonicClock {
    override fun nowMs(): Long = System.nanoTime() / 1_000_000L
    override fun sleepMs(durationMs: Long) {
        if (durationMs > 0L) {
            Thread.sleep(durationMs)
        }
    }
}

/**
 * UTC wall clock strictly for stamping `verified_at` timestamps (`Instant.now()`).
 */
fun interface WallClock {
    fun nowInstant(): Instant
}

object SystemUtcWallClock : WallClock {
    override fun nowInstant(): Instant = Instant.now()
}

sealed class TransportResult {
    data class HttpResponse(
        val statusCode: Int,
        val headersLowercase: Map<String, String>,
        val bodyBytes: ByteArray
    ) : TransportResult()

    data class PayloadTooLarge(
        val statusCode: Int,
        val maxBodyBytes: Int
    ) : TransportResult()

    data class TimeoutOrIoError(
        val message: String
    ) : TransportResult()
}

fun interface BoundedAccountV1Transport {
    fun executeSingleGet(
        url: String,
        apiKey: String,
        connectTimeoutMs: Int,
        readTimeoutMs: Int,
        callDeadlineMs: Long,
        maxBodyBytes: Int,
        allowLoopbackHttpForTesting: Boolean
    ): TransportResult
}

/**
 * Single-attempt HTTP transport that enforces:
 * - Strict URI host/scheme/path allowlist (`https://` to approved Riot regional clusters by default;
 *   `http://127.0.0.1` permitted ONLY when `allowLoopbackHttpForTesting = true` AND `apiKey` starts with
 *   `TEST-LOOPBACK-ONLY-` and contains no `RGAPI-` prefix).
 * - Zero automatic retries (`instanceFollowRedirects = false`)
 * - Strict connect and read socket timeouts
 * - Off-thread wall-clock deadline that actively closes the stream and calls `HttpURLConnection.disconnect()`
 * - Strict `maxBodyBytes` (4 KiB) cap while reading the response stream
 */
class HttpUrlConnectionAccountV1Transport(
    private val onAsyncAbortInvoked: (() -> Unit)? = null
) : BoundedAccountV1Transport {

    override fun executeSingleGet(
        url: String,
        apiKey: String,
        connectTimeoutMs: Int,
        readTimeoutMs: Int,
        callDeadlineMs: Long,
        maxBodyBytes: Int,
        allowLoopbackHttpForTesting: Boolean
    ): TransportResult {
        val validatedUri = try {
            OffProdChallengerIdentityCompletionTool.validateTargetUrlAndKeySafety(
                url = url,
                apiKey = apiKey,
                allowLoopbackHttpForTesting = allowLoopbackHttpForTesting
            )
        } catch (e: IllegalArgumentException) {
            return TransportResult.TimeoutOrIoError("Rejected transport target: ${e.message}")
        }

        val connRef = AtomicReference<HttpURLConnection?>(null)
        val streamRef = AtomicReference<InputStream?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(callDeadlineMs.coerceAtLeast(1L))

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || System.nanoTime() >= deadlineNano

        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "offprod-account-v1-single-call").apply { isDaemon = true }
        }

        val future: Future<TransportResult> = executor.submit(Callable {
            if (isCancelledOrPastDeadline()) {
                return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired before HTTP openConnection")
            }
            var connection: HttpURLConnection? = null
            try {
                connection = (validatedUri.toURL().openConnection() as HttpURLConnection).apply {
                    requestMethod = "GET"
                    instanceFollowRedirects = false
                    useCaches = false
                    connectTimeout = connectTimeoutMs
                    readTimeout = readTimeoutMs
                    setRequestProperty("X-Riot-Token", apiKey)
                    setRequestProperty("Accept", "application/json")
                    setRequestProperty("Connection", "close")
                }
                connRef.set(connection)
                if (isCancelledOrPastDeadline()) {
                    connRef.set(null)
                    triggerNonBlockingHttpAbortAndClose(null, connection)
                    return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired before HTTP connect")
                }

                val statusCode = connection.responseCode
                if (isCancelledOrPastDeadline()) {
                    triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
                    return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired after HTTP responseCode")
                }

                val headersLower = LinkedHashMap<String, String>()
                for ((key, values) in connection.headerFields) {
                    if (key != null && !values.isNullOrEmpty()) {
                        headersLower[key.lowercase(Locale.ROOT)] = values.first()
                    }
                }

                val rawStream: InputStream? = try {
                    if (statusCode in 200..299) connection.inputStream else connection.errorStream
                } catch (_: Exception) {
                    connection.errorStream
                }
                streamRef.set(rawStream)
                if (isCancelledOrPastDeadline()) {
                    triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
                    return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired before reading HTTP stream")
                }

                val bodyBytes = if (rawStream != null) {
                    when (val readOutcome = readBoundedStreamFenced(rawStream, maxBodyBytes, ::isCancelledOrPastDeadline)) {
                        is StreamReadOutcome.Success -> readOutcome.bytes
                        is StreamReadOutcome.TooLarge -> {
                            triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
                            return@Callable TransportResult.PayloadTooLarge(statusCode, maxBodyBytes)
                        }
                        is StreamReadOutcome.CancelledOrTimedOut -> {
                            triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
                            return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired while reading HTTP stream")
                        }
                    }
                } else {
                    ByteArray(0)
                }

                if (isCancelledOrPastDeadline()) {
                    triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
                    return@Callable TransportResult.TimeoutOrIoError("Cancelled or deadline expired after reading HTTP body")
                }

                TransportResult.HttpResponse(
                    statusCode = statusCode,
                    headersLowercase = headersLower,
                    bodyBytes = bodyBytes
                )
            } catch (e: Exception) {
                TransportResult.TimeoutOrIoError("${e.javaClass.simpleName}: ${e.message ?: "HTTP transport failure"}")
            } finally {
                val s = streamRef.getAndSet(null)
                val c = connRef.getAndSet(null)
                if (s != null || c != null) {
                    triggerNonBlockingHttpAbortAndClose(s, c)
                }
            }
        })

        return try {
            future.get(callDeadlineMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
            TransportResult.TimeoutOrIoError("Call deadline (${callDeadlineMs}ms) exceeded; socket disconnect dispatched asynchronously")
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            triggerNonBlockingHttpAbortAndClose(streamRef.getAndSet(null), connRef.getAndSet(null))
            TransportResult.TimeoutOrIoError("Interrupted/failed waiting for HTTP call: ${e.message}")
        } finally {
            executor.shutdownNow()
        }
    }

    private sealed class StreamReadOutcome {
        data class Success(val bytes: ByteArray) : StreamReadOutcome()
        object TooLarge : StreamReadOutcome()
        object CancelledOrTimedOut : StreamReadOutcome()
    }

    private fun readBoundedStreamFenced(
        input: InputStream,
        maxBodyBytes: Int,
        isCancelledOrPastDeadline: () -> Boolean
    ): StreamReadOutcome {
        val out = ByteArrayOutputStream(minOf(maxBodyBytes, 1024))
        val buffer = ByteArray(1024)
        var total = 0
        while (true) {
            if (isCancelledOrPastDeadline()) {
                return StreamReadOutcome.CancelledOrTimedOut
            }
            val read = input.read(buffer)
            if (isCancelledOrPastDeadline()) {
                return StreamReadOutcome.CancelledOrTimedOut
            }
            if (read == -1) break
            total += read
            if (total > maxBodyBytes) {
                return StreamReadOutcome.TooLarge
            }
            out.write(buffer, 0, read)
        }
        return StreamReadOutcome.Success(out.toByteArray())
    }

    /**
     * Dispatches `HttpURLConnection.disconnect()` and `InputStream.close()` on a daemon thread
     * so that the caller thread NEVER blocks past `callDeadlineMs` waiting on socket/stream teardown.
     */
    private fun triggerNonBlockingHttpAbortAndClose(
        stream: InputStream?,
        connection: HttpURLConnection?
    ) {
        if (stream == null && connection == null) return
        Thread({
            try {
                onAsyncAbortInvoked?.invoke()
            } catch (_: Exception) {
            }
            try {
                connection?.disconnect()
            } catch (_: Exception) {
            }
            try {
                stream?.close()
            } catch (_: Exception) {
            }
        }, "offprod-http-async-abort").apply {
            isDaemon = true
            start()
        }
    }
}

sealed class DbRecheckResult {
    data class Success(
        val statesByPuuid: Map<String, OffProdChallengerIdentityCompletionTool.PreCallDbState>
    ) : DbRecheckResult()

    data class Failure(
        val reason: String,
        val timedOutOrBudgetExhausted: Boolean = false
    ) : DbRecheckResult()
}

sealed class DbWriteVerificationResult {
    data class WrittenAndVerified(
        val indexedPlayerBlankFieldUpdated: Boolean,
        val identityOnlyTableUpserted: Boolean,
        val verifiedGameName: String,
        val verifiedTagLine: String,
        val verifiedAtUtc: Instant,
        val writeTargetDescription: String
    ) : DbWriteVerificationResult()

    data class ConcurrentAlreadyCompleteVerified(
        val verifiedGameName: String,
        val verifiedTagLine: String,
        val verifiedAtUtc: Instant?
    ) : DbWriteVerificationResult()

    data class FailureOrUnknownCommit(
        val reason: String
    ) : DbWriteVerificationResult()
}

/**
 * Persistence boundary for bounded pre-call read-only recheck and race-safe durable writes
 * with mandatory post-write read verification.
 */
interface IdentityCompletionPersistencePort {
    fun recheckExistingIdentitiesBounded(
        entries: List<OffProdChallengerIdentityCompletionTool.ManifestEntry>,
        timeoutMs: Long
    ): DbRecheckResult

    fun persistAndVerifyIdentityBounded(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant,
        preCallDbState: OffProdChallengerIdentityCompletionTool.PreCallDbState,
        timeoutMs: Long
    ): DbWriteVerificationResult
}

/**
 * JDBC implementation of [IdentityCompletionPersistencePort] that:
 * 1. Enforces strict cancellation and monotonic deadline fencing before `dataSource.connection`,
 *    immediately after `dataSource.connection` (closing any late-acquired connection WITHOUT preparing
 *    or executing any SQL if the deadline has already expired!), and before/between every SQL statement.
 * 2. Fails closed if `DataSource.setLoginTimeout`, `Connection.setReadOnly`, `Connection.setNetworkTimeout`,
 *    or `PreparedStatement.setQueryTimeout` throws an exception.
 * 3. Proves durable commit: requires `writeConn.autoCommit = true` (verified via `writeConn.autoCommit == true`)
 *    OR explicit `writeConn.commit()` followed by a post-commit verification `SELECT` on a separate
 *    connection (`verifyConn !== writeConn`). A same-connection read with `autoCommit == false` is rejected
 *    as [DbWriteVerificationResult.FailureOrUnknownCommit] because it would read uncommitted session state.
 */
class JdbcIdentityCompletionPersistenceAdapter(
    private val dataSource: DataSource,
    private val checkIdentityTableInPreCall: Boolean = true
) : IdentityCompletionPersistencePort {

    companion object {
        val DDL_CREATE_IDENTITY_ONLY_TABLE: String = """
            CREATE TABLE IF NOT EXISTS player_riot_identities (
                puuid           VARCHAR(78)  PRIMARY KEY,
                platform_region VARCHAR(16)  NOT NULL,
                game_name       VARCHAR(255) NOT NULL,
                tag_line        VARCHAR(255) NOT NULL,
                verified_at     TIMESTAMPTZ  NOT NULL,
                CONSTRAINT chk_player_riot_identities_nonblank
                    CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
            )
        """.trimIndent()

        val SQL_UPDATE_INDEXED_PLAYERS_BLANK_FIELDS_ONLY: String = """
            UPDATE indexed_players
            SET game_name = CASE WHEN BTRIM(COALESCE(game_name, '')) = '' THEN ? ELSE game_name END,
                tag_line  = CASE WHEN BTRIM(COALESCE(tag_line, '')) = ''  THEN ? ELSE tag_line  END
            WHERE puuid = ?
              AND (BTRIM(COALESCE(game_name, '')) = '' OR BTRIM(COALESCE(tag_line, '')) = '')
              AND BTRIM(COALESCE(?, '')) <> ''
              AND BTRIM(COALESCE(?, '')) <> ''
        """.trimIndent()

        val SQL_UPSERT_PLAYER_RIOT_IDENTITIES_RACE_SAFE: String = """
            INSERT INTO player_riot_identities (
                puuid,
                platform_region,
                game_name,
                tag_line,
                verified_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (puuid) DO UPDATE
            SET game_name = CASE
                    WHEN BTRIM(COALESCE(player_riot_identities.game_name, '')) = '' THEN EXCLUDED.game_name
                    ELSE player_riot_identities.game_name
                END,
                tag_line = CASE
                    WHEN BTRIM(COALESCE(player_riot_identities.tag_line, '')) = '' THEN EXCLUDED.tag_line
                    ELSE player_riot_identities.tag_line
                END,
                platform_region = CASE
                    WHEN BTRIM(COALESCE(player_riot_identities.platform_region, '')) = '' THEN EXCLUDED.platform_region
                    ELSE player_riot_identities.platform_region
                END,
                verified_at = GREATEST(player_riot_identities.verified_at, EXCLUDED.verified_at)
            WHERE (
                BTRIM(COALESCE(player_riot_identities.game_name, '')) = ''
                OR BTRIM(COALESCE(player_riot_identities.tag_line, '')) = ''
            )
            AND BTRIM(COALESCE(EXCLUDED.game_name, '')) <> ''
            AND BTRIM(COALESCE(EXCLUDED.tag_line, '')) <> ''
        """.trimIndent()

        val SQL_SELECT_VERIFY_IDENTITY_ONLY_BY_PUUID: String = """
            SELECT game_name, tag_line, verified_at
            FROM player_riot_identities
            WHERE puuid = ?
        """.trimIndent()

        val SQL_SELECT_VERIFY_INDEXED_PLAYER_BY_PUUID: String = """
            SELECT game_name, tag_line
            FROM indexed_players
            WHERE puuid = ?
        """.trimIndent()
    }

    override fun recheckExistingIdentitiesBounded(
        entries: List<OffProdChallengerIdentityCompletionTool.ManifestEntry>,
        timeoutMs: Long
    ): DbRecheckResult {
        if (entries.isEmpty()) return DbRecheckResult.Success(emptyMap())
        if (timeoutMs <= 0L) {
            return DbRecheckResult.Failure(
                reason = "DB recheck timeout budget (${timeoutMs}ms) exhausted before connection acquisition",
                timedOutOrBudgetExhausted = true
            )
        }

        val puuids = entries.map { it.normalizedPuuid }.distinct()
        val connRef = AtomicReference<Connection?>(null)
        val stmtRef = AtomicReference<PreparedStatement?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || System.nanoTime() >= deadlineNano

        fun remainingBudgetMs(): Long =
            TimeUnit.NANOSECONDS.toMillis(deadlineNano - System.nanoTime())

        val netTimeoutExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-net-timeout").apply { isDaemon = true }
        }
        val callExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-recheck-call").apply { isDaemon = true }
        }

        val future = callExec.submit(Callable<DbRecheckResult> {
            if (isCancelledOrPastDeadline()) {
                return@Callable DbRecheckResult.Failure(
                    reason = "DB recheck cancelled or deadline expired before connection acquisition",
                    timedOutOrBudgetExhausted = true
                )
            }

            val result = LinkedHashMap<String, OffProdChallengerIdentityCompletionTool.PreCallDbState>(puuids.size)
            for (p in puuids) {
                result[p] = OffProdChallengerIdentityCompletionTool.PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
            }

            val placeholders = puuids.joinToString(",") { "?" }
            val indexedPlayersSql =
                "SELECT puuid, game_name, tag_line FROM indexed_players WHERE puuid IN ($placeholders)"

            try {
                val initialRemMs = remainingBudgetMs()
                if (initialRemMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable DbRecheckResult.Failure(
                        reason = "DB recheck deadline expired before getConnection",
                        timedOutOrBudgetExhausted = true
                    )
                }
                dataSource.loginTimeout = ceil(initialRemMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)

                val rawConn = dataSource.connection
                connRef.set(rawConn)
                // Critical fence immediately after connection acquisition:
                if (isCancelledOrPastDeadline()) {
                    connRef.set(null)
                    triggerNonBlockingAsyncAbortAndClose(null, rawConn, netTimeoutExec)
                    return@Callable DbRecheckResult.Failure(
                        reason = "DB recheck connection acquired after deadline/cancellation; closed without SQL execution",
                        timedOutOrBudgetExhausted = true
                    )
                }

                rawConn.use { conn ->
                    val remAfterAcquireMs = remainingBudgetMs()
                    if (remAfterAcquireMs <= 0L || isCancelledOrPastDeadline()) {
                        connRef.set(null)
                        triggerNonBlockingAsyncAbortAndClose(null, conn, netTimeoutExec)
                        return@Callable DbRecheckResult.Failure(
                            reason = "DB recheck deadline expired immediately after connection acquisition",
                            timedOutOrBudgetExhausted = true
                        )
                    }

                    applyBoundedConnectionSettingsFailClosed(
                        conn = conn,
                        readOnly = true,
                        networkTimeoutMs = remAfterAcquireMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1),
                        executor = netTimeoutExec
                    )

                    if (isCancelledOrPastDeadline()) {
                        return@Callable DbRecheckResult.Failure(
                            reason = "DB recheck deadline expired after applying connection settings",
                            timedOutOrBudgetExhausted = true
                        )
                    }

                    conn.prepareStatement(indexedPlayersSql).use { ps ->
                        stmtRef.set(ps)
                        val remQueryMs = remainingBudgetMs()
                        if (remQueryMs <= 0L || isCancelledOrPastDeadline()) {
                            stmtRef.set(null)
                            triggerNonBlockingAsyncAbortAndClose(ps, conn, netTimeoutExec)
                            return@Callable DbRecheckResult.Failure(
                                reason = "DB recheck deadline expired before indexed_players query execution",
                                timedOutOrBudgetExhausted = true
                            )
                        }
                        ps.queryTimeout = ceil(remQueryMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        for ((idx, puuid) in puuids.withIndex()) {
                            ps.setString(idx + 1, puuid)
                        }
                        ps.executeQuery().use { rs ->
                            while (rs.next()) {
                                if (isCancelledOrPastDeadline()) {
                                    return@Callable DbRecheckResult.Failure(
                                        reason = "DB recheck deadline expired while reading indexed_players ResultSet",
                                        timedOutOrBudgetExhausted = true
                                    )
                                }
                                val p = rs.getString("puuid")?.trim().orEmpty()
                                if (p.isEmpty()) continue
                                val gn = rs.getString("game_name")?.trim().orEmpty()
                                val tl = rs.getString("tag_line")?.trim().orEmpty()
                                result[p] = if (gn.isNotEmpty() && tl.isNotEmpty()) {
                                    OffProdChallengerIdentityCompletionTool.PreCallDbState.ALREADY_COMPLETE
                                } else {
                                    OffProdChallengerIdentityCompletionTool.PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD
                                }
                            }
                        }
                        stmtRef.set(null)
                    }

                    if (checkIdentityTableInPreCall) {
                        val unresolvedPuuids = result.entries
                            .filter { it.value != OffProdChallengerIdentityCompletionTool.PreCallDbState.ALREADY_COMPLETE }
                            .map { it.key }
                        if (unresolvedPuuids.isNotEmpty()) {
                            val remIdQueryMs = remainingBudgetMs()
                            if (remIdQueryMs <= 0L || isCancelledOrPastDeadline()) {
                                return@Callable DbRecheckResult.Failure(
                                    reason = "DB recheck deadline expired before player_riot_identities query",
                                    timedOutOrBudgetExhausted = true
                                )
                            }
                            val idPlaceholders = unresolvedPuuids.joinToString(",") { "?" }
                            val identitySql =
                                "SELECT puuid, game_name, tag_line FROM player_riot_identities WHERE puuid IN ($idPlaceholders)"
                            conn.prepareStatement(identitySql).use { ps ->
                                stmtRef.set(ps)
                                if (isCancelledOrPastDeadline()) {
                                    stmtRef.set(null)
                                    triggerNonBlockingAsyncAbortAndClose(ps, conn, netTimeoutExec)
                                    return@Callable DbRecheckResult.Failure(
                                        reason = "DB recheck cancelled before player_riot_identities executeQuery",
                                        timedOutOrBudgetExhausted = true
                                    )
                                }
                                ps.queryTimeout = ceil(remIdQueryMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                                for ((idx, puuid) in unresolvedPuuids.withIndex()) {
                                    ps.setString(idx + 1, puuid)
                                }
                                ps.executeQuery().use { rs ->
                                    while (rs.next()) {
                                        if (isCancelledOrPastDeadline()) {
                                            return@Callable DbRecheckResult.Failure(
                                                reason = "DB recheck deadline expired while reading player_riot_identities ResultSet",
                                                timedOutOrBudgetExhausted = true
                                            )
                                        }
                                        val p = rs.getString("puuid")?.trim().orEmpty()
                                        val gn = rs.getString("game_name")?.trim().orEmpty()
                                        val tl = rs.getString("tag_line")?.trim().orEmpty()
                                        if (p.isNotEmpty() && gn.isNotEmpty() && tl.isNotEmpty()) {
                                            result[p] = OffProdChallengerIdentityCompletionTool.PreCallDbState.ALREADY_COMPLETE
                                        }
                                    }
                                }
                                stmtRef.set(null)
                            }
                        }
                    }
                }
                if (isCancelledOrPastDeadline()) {
                    DbRecheckResult.Failure(
                        reason = "DB recheck deadline expired upon completion",
                        timedOutOrBudgetExhausted = true
                    )
                } else {
                    DbRecheckResult.Success(result)
                }
            } catch (e: Exception) {
                DbRecheckResult.Failure(
                    reason = "Pre-call DB recheck failed (${e.javaClass.simpleName}): ${e.message}",
                    timedOutOrBudgetExhausted = isCancelledOrPastDeadline()
                )
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
            triggerNonBlockingAsyncAbortAndClose(stmtRef.getAndSet(null), connRef.getAndSet(null), netTimeoutExec)
            DbRecheckResult.Failure(
                reason = "Pre-call DB recheck (including connection acquisition and cleanup) exceeded ${timeoutMs}ms deadline",
                timedOutOrBudgetExhausted = true
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            triggerNonBlockingAsyncAbortAndClose(stmtRef.getAndSet(null), connRef.getAndSet(null), netTimeoutExec)
            DbRecheckResult.Failure(
                reason = "Pre-call DB recheck interrupted/failed: ${e.message}",
                timedOutOrBudgetExhausted = false
            )
        } finally {
            callExec.shutdownNow()
            netTimeoutExec.shutdown()
        }
    }

    override fun persistAndVerifyIdentityBounded(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant,
        preCallDbState: OffProdChallengerIdentityCompletionTool.PreCallDbState,
        timeoutMs: Long
    ): DbWriteVerificationResult {
        val validatedPuuid = OffProdChallengerIdentityCompletionTool.validateOfficialPuuid(puuid, "puuid")
        val trimmedRegion = platformRegion.trim().lowercase(Locale.ROOT)
        val trimmedGameName = gameName.trim()
        val trimmedTagLine = tagLine.trim()
        if (trimmedRegion.isEmpty() || trimmedGameName.isEmpty() || trimmedTagLine.isEmpty()) {
            return DbWriteVerificationResult.FailureOrUnknownCommit(
                "persistAndVerifyIdentityBounded requires non-blank platformRegion, gameName, and tagLine"
            )
        }
        if (timeoutMs <= 0L) {
            return DbWriteVerificationResult.FailureOrUnknownCommit(
                "DB write timeout budget (${timeoutMs}ms) exhausted before connection acquisition"
            )
        }

        val connRef = AtomicReference<Connection?>(null)
        val stmtRef = AtomicReference<PreparedStatement?>(null)
        val cancelledOrTimedOut = AtomicBoolean(false)
        val deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)

        fun isCancelledOrPastDeadline(): Boolean =
            cancelledOrTimedOut.get() || Thread.currentThread().isInterrupted || System.nanoTime() >= deadlineNano

        fun remainingBudgetMs(): Long =
            TimeUnit.NANOSECONDS.toMillis(deadlineNano - System.nanoTime())

        val netTimeoutExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-write-net-timeout").apply { isDaemon = true }
        }
        val callExec = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-write-call").apply { isDaemon = true }
        }

        val future = callExec.submit(Callable<DbWriteVerificationResult> {
            if (isCancelledOrPastDeadline()) {
                return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                    "DB write cancelled or deadline expired before connection acquisition"
                )
            }

            try {
                val initialRemMs = remainingBudgetMs()
                if (initialRemMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                        "DB write deadline expired before getConnection"
                    )
                }
                dataSource.loginTimeout = ceil(initialRemMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)

                var indexedUpdated = false
                var identityUpserted = false
                var writeConnWasAutoCommit = false
                var writeConnIdentityRef: Connection? = null

                val rawWriteConn = dataSource.connection
                connRef.set(rawWriteConn)
                // Critical fence immediately after connection acquisition:
                // If caller already timed out while getConnection() was blocked, close immediately WITHOUT executing SQL!
                if (isCancelledOrPastDeadline()) {
                    connRef.set(null)
                    triggerNonBlockingAsyncAbortAndClose(null, rawWriteConn, netTimeoutExec)
                    return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                        "DB write connection acquired after deadline/cancellation; closed without executing any SQL write"
                    )
                }

                rawWriteConn.use { writeConn ->
                    writeConnIdentityRef = writeConn
                    val remAfterAcquireMs = remainingBudgetMs()
                    if (remAfterAcquireMs <= 0L || isCancelledOrPastDeadline()) {
                        connRef.set(null)
                        triggerNonBlockingAsyncAbortAndClose(null, writeConn, netTimeoutExec)
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "DB write deadline expired immediately after connection acquisition; closed without SQL write"
                        )
                    }

                    applyBoundedConnectionSettingsFailClosed(
                        conn = writeConn,
                        readOnly = false,
                        networkTimeoutMs = remAfterAcquireMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1),
                        executor = netTimeoutExec
                    )

                    // Ensure durable commit semantics: attempt autoCommit = true first.
                    writeConn.autoCommit = true
                    writeConnWasAutoCommit = writeConn.autoCommit

                    if (isCancelledOrPastDeadline()) {
                        connRef.set(null)
                        triggerNonBlockingAsyncAbortAndClose(null, writeConn, netTimeoutExec)
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "DB write deadline expired after connection setup; closed without executing any SQL write"
                        )
                    }

                    if (preCallDbState == OffProdChallengerIdentityCompletionTool.PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD) {
                        val remUpdateMs = remainingBudgetMs()
                        if (remUpdateMs <= 0L || isCancelledOrPastDeadline()) {
                            connRef.set(null)
                            triggerNonBlockingAsyncAbortAndClose(null, writeConn, netTimeoutExec)
                            return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                "DB write deadline expired before indexed_players UPDATE"
                            )
                        }
                        writeConn.prepareStatement(SQL_UPDATE_INDEXED_PLAYERS_BLANK_FIELDS_ONLY).use { ps ->
                            stmtRef.set(ps)
                            if (isCancelledOrPastDeadline()) {
                                stmtRef.set(null)
                                connRef.set(null)
                                triggerNonBlockingAsyncAbortAndClose(ps, writeConn, netTimeoutExec)
                                return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                    "DB write cancelled before indexed_players executeUpdate; 0 SQL writes executed"
                                )
                            }
                            ps.queryTimeout = ceil(remUpdateMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                            ps.setString(1, trimmedGameName)
                            ps.setString(2, trimmedTagLine)
                            ps.setString(3, validatedPuuid)
                            ps.setString(4, trimmedGameName)
                            ps.setString(5, trimmedTagLine)
                            indexedUpdated = ps.executeUpdate() > 0
                            stmtRef.set(null)
                        }
                    }

                    val remUpsertMs = remainingBudgetMs()
                    if (remUpsertMs <= 0L || isCancelledOrPastDeadline()) {
                        connRef.set(null)
                        triggerNonBlockingAsyncAbortAndClose(null, writeConn, netTimeoutExec)
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "DB write deadline expired before player_riot_identities UPSERT; commit state unknown if indexed_players updated"
                        )
                    }

                    identityUpserted = writeConn.prepareStatement(SQL_UPSERT_PLAYER_RIOT_IDENTITIES_RACE_SAFE).use { ps ->
                        stmtRef.set(ps)
                        if (isCancelledOrPastDeadline()) {
                            stmtRef.set(null)
                            connRef.set(null)
                            triggerNonBlockingAsyncAbortAndClose(ps, writeConn, netTimeoutExec)
                            return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                "DB write cancelled before player_riot_identities executeUpdate"
                            )
                        }
                        ps.queryTimeout = ceil(remUpsertMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        ps.setString(1, validatedPuuid)
                        ps.setString(2, trimmedRegion)
                        ps.setString(3, trimmedGameName)
                        ps.setString(4, trimmedTagLine)
                        ps.setTimestamp(5, Timestamp.from(verifiedAt))
                        val updated = ps.executeUpdate() > 0
                        stmtRef.set(null)
                        updated
                    }

                    // If the driver did not enable autoCommit=true, explicitly commit before post-commit verification!
                    if (!writeConnWasAutoCommit) {
                        if (isCancelledOrPastDeadline()) {
                            connRef.set(null)
                            triggerNonBlockingAsyncAbortAndClose(null, writeConn, netTimeoutExec)
                            return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                "DB write deadline expired before explicit commit; transaction rolled back/aborted"
                            )
                        }
                        writeConn.commit()
                    }
                }
                connRef.set(null)

                // Mandatory post-commit verification read:
                val remVerifyMs = remainingBudgetMs()
                if (remVerifyMs <= 0L || isCancelledOrPastDeadline()) {
                    return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                        "DB write completed but deadline expired before post-commit verification read; commit state unverified"
                    )
                }

                dataSource.loginTimeout = ceil(remVerifyMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                val rawVerifyConn = dataSource.connection
                connRef.set(rawVerifyConn)
                if (isCancelledOrPastDeadline()) {
                    connRef.set(null)
                    triggerNonBlockingAsyncAbortAndClose(null, rawVerifyConn, netTimeoutExec)
                    return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                        "Post-commit verification connection acquired after deadline; commit state unverified"
                    )
                }

                rawVerifyConn.use { verifyConn ->
                    // Durability proof invariant:
                    // If the write connection had autoCommit == false, a post-write read on the SAME Connection instance
                    // does NOT prove durable commit (it could read uncommitted session state if commit was a no-op).
                    // Therefore, when writeConnWasAutoCommit is false, we require either verifyConn !== writeConnIdentityRef
                    // OR verifyConn.autoCommit == true; otherwise fail closed!
                    if (!writeConnWasAutoCommit && verifyConn === writeConnIdentityRef && !verifyConn.autoCommit) {
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "Post-commit durability proof failed: write connection had autoCommit=false and DataSource returned the same uncommitted Connection instance for verification"
                        )
                    }

                    val remVerifyNetMs = remainingBudgetMs()
                    if (remVerifyNetMs <= 0L || isCancelledOrPastDeadline()) {
                        connRef.set(null)
                        triggerNonBlockingAsyncAbortAndClose(null, verifyConn, netTimeoutExec)
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "Deadline expired before post-commit verification query"
                        )
                    }

                    applyBoundedConnectionSettingsFailClosed(
                        conn = verifyConn,
                        readOnly = true,
                        networkTimeoutMs = remVerifyNetMs.coerceAtMost(Int.MAX_VALUE.toLong()).toInt().coerceAtLeast(1),
                        executor = netTimeoutExec
                    )

                    var verifiedIdentityGn = ""
                    var verifiedIdentityTl = ""
                    var verifiedIdentityTs: Instant? = null

                    verifyConn.prepareStatement(SQL_SELECT_VERIFY_IDENTITY_ONLY_BY_PUUID).use { ps ->
                        stmtRef.set(ps)
                        val remSelectMs = remainingBudgetMs()
                        if (remSelectMs <= 0L || isCancelledOrPastDeadline()) {
                            stmtRef.set(null)
                            connRef.set(null)
                            triggerNonBlockingAsyncAbortAndClose(ps, verifyConn, netTimeoutExec)
                            return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                "Deadline expired before post-commit verification executeQuery"
                            )
                        }
                        ps.queryTimeout = ceil(remSelectMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                        ps.setString(1, validatedPuuid)
                        ps.executeQuery().use { rs ->
                            if (rs.next()) {
                                verifiedIdentityGn = rs.getString("game_name")?.trim().orEmpty()
                                verifiedIdentityTl = rs.getString("tag_line")?.trim().orEmpty()
                                verifiedIdentityTs = rs.getTimestamp("verified_at")?.toInstant()
                            }
                        }
                        stmtRef.set(null)
                    }

                    if (preCallDbState == OffProdChallengerIdentityCompletionTool.PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD) {
                        var indexedGn = ""
                        var indexedTl = ""
                        verifyConn.prepareStatement(SQL_SELECT_VERIFY_INDEXED_PLAYER_BY_PUUID).use { ps ->
                            stmtRef.set(ps)
                            val remIdxSelectMs = remainingBudgetMs()
                            if (remIdxSelectMs <= 0L || isCancelledOrPastDeadline()) {
                                stmtRef.set(null)
                                connRef.set(null)
                                triggerNonBlockingAsyncAbortAndClose(ps, verifyConn, netTimeoutExec)
                                return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                    "Deadline expired before indexed_players post-commit verification executeQuery"
                                )
                            }
                            ps.queryTimeout = ceil(remIdxSelectMs.toDouble() / 1_000.0).toInt().coerceAtLeast(1)
                            ps.setString(1, validatedPuuid)
                            ps.executeQuery().use { rs ->
                                if (rs.next()) {
                                    indexedGn = rs.getString("game_name")?.trim().orEmpty()
                                    indexedTl = rs.getString("tag_line")?.trim().orEmpty()
                                }
                            }
                            stmtRef.set(null)
                        }
                        if (indexedGn.isEmpty() || indexedTl.isEmpty()) {
                            return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                                "Post-write verification failed: indexed_players row for '$validatedPuuid' still has blank game_name or tag_line"
                            )
                        }
                    }

                    if (verifiedIdentityGn.isEmpty() || verifiedIdentityTl.isEmpty() || verifiedIdentityTs == null) {
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "Post-write verification failed: player_riot_identities row for '$validatedPuuid' missing or blank after upsert"
                        )
                    }

                    if (isCancelledOrPastDeadline()) {
                        return@Callable DbWriteVerificationResult.FailureOrUnknownCommit(
                            "Deadline expired during post-commit verification; conservative unknown commit status"
                        )
                    }

                    if (indexedUpdated || identityUpserted) {
                        val targetDesc = if (preCallDbState == OffProdChallengerIdentityCompletionTool.PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD) {
                            "INDEXED_PLAYERS_BLANK_ONLY_AND_PLAYER_RIOT_IDENTITIES"
                        } else {
                            "PLAYER_RIOT_IDENTITIES_ONLY"
                        }
                        DbWriteVerificationResult.WrittenAndVerified(
                            indexedPlayerBlankFieldUpdated = indexedUpdated,
                            identityOnlyTableUpserted = identityUpserted,
                            verifiedGameName = verifiedIdentityGn,
                            verifiedTagLine = verifiedIdentityTl,
                            verifiedAtUtc = verifiedIdentityTs!!,
                            writeTargetDescription = targetDesc
                        )
                    } else {
                        DbWriteVerificationResult.ConcurrentAlreadyCompleteVerified(
                            verifiedGameName = verifiedIdentityGn,
                            verifiedTagLine = verifiedIdentityTl,
                            verifiedAtUtc = verifiedIdentityTs
                        )
                    }
                }
            } catch (e: Exception) {
                DbWriteVerificationResult.FailureOrUnknownCommit(
                    "DB write or post-write verification failed (${e.javaClass.simpleName}): ${e.message} (commit state unknown if failure occurred during commit)"
                )
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
            triggerNonBlockingAsyncAbortAndClose(stmtRef.getAndSet(null), connRef.getAndSet(null), netTimeoutExec)
            DbWriteVerificationResult.FailureOrUnknownCommit(
                "DB write/verification for '$validatedPuuid' exceeded ${timeoutMs}ms deadline; connection aborted asynchronously, commit state unknown"
            )
        } catch (e: Exception) {
            cancelledOrTimedOut.set(true)
            future.cancel(true)
            triggerNonBlockingAsyncAbortAndClose(stmtRef.getAndSet(null), connRef.getAndSet(null), netTimeoutExec)
            DbWriteVerificationResult.FailureOrUnknownCommit(
                "DB write/verification interrupted for '$validatedPuuid': ${e.message}; commit state unknown"
            )
        } finally {
            callExec.shutdownNow()
            netTimeoutExec.shutdown()
        }
    }

    /**
     * Applies readOnly and networkTimeout settings in fail-closed mode:
     * any SQLException, SQLFeatureNotSupportedException, or UnsupportedOperationException
     * propagates immediately instead of being swallowed.
     */
    @Throws(SQLException::class)
    private fun applyBoundedConnectionSettingsFailClosed(
        conn: Connection,
        readOnly: Boolean,
        networkTimeoutMs: Int,
        executor: java.util.concurrent.Executor
    ) {
        conn.isReadOnly = readOnly
        conn.setNetworkTimeout(executor, networkTimeoutMs)
    }

    /**
     * Dispatches JDBC statement cancellation and connection abort/close on a daemon thread
     * so that the caller thread NEVER blocks past its deadline waiting on a driver mutex.
     */
    private fun triggerNonBlockingAsyncAbortAndClose(
        stmt: PreparedStatement?,
        conn: Connection?,
        abortExecutor: java.util.concurrent.Executor
    ) {
        if (stmt == null && conn == null) return
        Thread({
            try {
                stmt?.cancel()
            } catch (_: Exception) {
            }
            try {
                conn?.abort(abortExecutor)
            } catch (_: Exception) {
            }
            try {
                stmt?.close()
            } catch (_: Exception) {
            }
            try {
                conn?.close()
            } catch (_: Exception) {
            }
        }, "offprod-db-async-abort").apply {
            isDaemon = true
            start()
        }
    }
}
