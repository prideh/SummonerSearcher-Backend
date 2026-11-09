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

/**
 * Isolated off-production one-shot tool for bounded missing-only Challenger Riot ID completion.
 *
 * SAFETY & ISOLATION GUARANTEES:
 * - NOT a Spring bean (@Service / @Component / @Scheduled are intentionally omitted). Never instantiated
 *   or scheduled inside the production JVM.
 * - Defaults to no-effects mode: [ExecutionOptions.dryRun] = true, [ExecutionOptions.networkEnabled] = false,
 *   [ExecutionOptions.allowDurableWrites] = false, [ExecutionOptions.keyCoordinationMode] = UNCOORDINATED.
 * - Single worker only (strictly serial loop, concurrency = 1), hard ceiling of 191 total HTTP attempts
 *   ([MAX_ALLOWED_HTTP_ATTEMPTS]), fixed 180s default wall-time budget ([DEFAULT_MAX_WALL_TIME_MS]),
 *   single attempt per PUUID (zero automatic retries), strict 4 KiB ([MAX_RESPONSE_BODY_BYTES]) payload cap,
 *   and explicit socket disconnect + stream close on timeout or oversized payload.
 * - Does NOT claim cross-process rate-limit headroom when sharing a Riot API key with production:
 *   requires explicit [KeyCoordinationMode.EXCLUSIVE_KEY] or [KeyCoordinationMode.COORDINATED_MAINTENANCE_WINDOW]
 *   attestation before any network call is permitted; stops immediately as [RunOutcome.BLOCKED_KEY_COORDINATION_REQUIRED]
 *   when uncoordinated.
 * - No assumed 20/s or 100/120 rate limits: routing cluster limits start UNVERIFIED, the first cluster
 *   discovery call counts against the 191 HTTP attempt cap, and any response missing valid
 *   X-App-Rate-Limit, X-App-Rate-Limit-Count, X-Method-Rate-Limit, or X-Method-Rate-Limit-Count headers
 *   aborts immediately ([RunOutcome.ABORTED_MISSING_OR_INVALID_RATE_HEADERS]).
 * - Persistence uses the dedicated identity-only table (`player_riot_identities`) instead of fabricating
 *   `profile_icon_id`, `summoner_level`, or `last_seen_at` in `indexed_players`. For rows that already
 *   exist in `indexed_players` with a blank `game_name` or `tag_line`, `UPDATE indexed_players` preserves
 *   each already-nonblank column independently.
 */
class OffProdChallengerIdentityCompletionTool(
    private val persistencePort: IdentityCompletionPersistencePort,
    private val httpTransport: BoundedAccountV1Transport = HttpUrlConnectionAccountV1Transport(),
    private val clock: MonotonicClock = SystemMonotonicClock,
    private val objectMapper: ObjectMapper = defaultObjectMapper()
) {

    companion object {
        const val MAX_ALLOWED_HTTP_ATTEMPTS: Int = 191
        const val DEFAULT_MAX_WALL_TIME_MS: Long = 180_000L
        const val DEFAULT_CONNECT_TIMEOUT_MS: Int = 2_500
        const val DEFAULT_READ_TIMEOUT_MS: Int = 5_000
        const val DEFAULT_CALL_DEADLINE_MS: Long = 5_500L
        const val MAX_RESPONSE_BODY_BYTES: Int = 4_096
        const val DB_QUERY_TIMEOUT_SECONDS: Int = 2
        const val DB_NETWORK_TIMEOUT_MS: Int = 2_500

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

        fun computeManifestSha256(entries: List<ManifestEntry>): String {
            val digest = MessageDigest.getInstance("SHA-256")
            for (entry in entries) {
                val canonicalLine = "${entry.platformRegion.trim().lowercase(Locale.ROOT)}:${entry.puuid.trim()}\n"
                digest.update(canonicalLine.toByteArray(StandardCharsets.UTF_8))
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        fun mapPlatformToRoutingCluster(platformRegion: String): String {
            val normalized = platformRegion.trim().lowercase(Locale.ROOT)
            return VALID_PLATFORM_TO_CLUSTER[normalized]
                ?: throw IllegalArgumentException("Unsupported platform region for Account-V1 routing: '$platformRegion'")
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
        init {
            require(puuid.isNotBlank()) { "Manifest entry puuid must not be blank" }
            require(platformRegion.isNotBlank()) { "Manifest entry platformRegion must not be blank" }
        }

        val normalizedPuuid: String = puuid.trim()
        val normalizedPlatformRegion: String = platformRegion.trim().lowercase(Locale.ROOT)
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
        val maxBodyBytes: Int = MAX_RESPONSE_BODY_BYTES,
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
            require(maxBodyBytes in 1..MAX_RESPONSE_BODY_BYTES) {
                "maxBodyBytes ($maxBodyBytes) must be within 1..$MAX_RESPONSE_BODY_BYTES"
            }
        }
    }

    enum class PreCallDbState {
        ALREADY_COMPLETE,
        PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD,
        ABSENT_FROM_INDEXED_PLAYERS
    }

    enum class ItemStatus {
        SKIPPED_ALREADY_RESOLVED_IN_DB,
        VALIDATED_DRY_RUN_NO_WRITE,
        COMPLETED_DURABLE_WRITE,
        SKIPPED_NOT_FOUND_404,
        SKIPPED_OMITTED_RIOT_ID_FIELDS,
        REJECTED_PUUID_MISMATCH,
        ABORTED_ON_HTTP_FAILURE,
        PENDING_UNPROCESSED
    }

    enum class RunOutcome {
        COMPLETED_ALL_CANDIDATES,
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
        ABORTED_PUUID_MISMATCH
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
        val durableIndexedPlayerUpdatesCount: Int,
        val durableIdentityTableUpsertsCount: Int,
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
     * Execute the bounded single-worker completion workflow against [manifest] with [options].
     */
    fun runBounded(
        manifest: SnapshotManifest,
        options: ExecutionOptions = ExecutionOptions(),
        riotApiKeyProvider: () -> String = { "" }
    ): CompletionRunReport {
        val startMs = clock.nowMs()
        var totalWaitMs = 0L
        val manifestHash = computeManifestSha256(manifest.entries)

        if (!options.dryRun && !options.allowDurableWrites) {
            return buildEarlyBlockedReport(
                outcome = RunOutcome.BLOCKED_DURABLE_WRITES_NOT_PERMITTED,
                manifest = manifest,
                manifestHash = manifestHash,
                options = options,
                elapsedMs = clock.nowMs() - startMs,
                reason = "dryRun=false requires explicit allowDurableWrites=true and a write-capable DB credential"
            )
        }

        // Step 1: Pre-call read-only DB recheck across indexed_players and player_riot_identities.
        val dbStatesByPuuid: Map<String, PreCallDbState> =
            persistencePort.recheckExistingIdentitiesBounded(manifest.entries)

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
                PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS -> {
                    absentCount++
                    candidatesToFetch.add(entry)
                }
            }
        }

        // If all 191 items are already resolved in DB, complete immediately with 0 network calls.
        if (candidatesToFetch.isEmpty()) {
            return CompletionRunReport(
                outcome = RunOutcome.COMPLETED_ALL_CANDIDATES,
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
                elapsedWallTimeMs = clock.nowMs() - startMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesCount = 0,
                durableIdentityTableUpsertsCount = 0,
                notFound404Count = 0,
                omittedRiotIdFieldsCount = 0,
                puuidMismatchCount = 0,
                remainingPendingCount = 0,
                remainingEntries = emptyList(),
                itemResults = itemResults,
                lastObservedClusterHeaders = emptyMap()
            )
        }

        // If networkEnabled is false (default), return the DB recheck diff without making any HTTP attempts.
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
                elapsedWallTimeMs = clock.nowMs() - startMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesCount = 0,
                durableIdentityTableUpsertsCount = 0,
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
        // Cross-process header inspection races with production traffic on a shared Riot key,
        // so execution without explicit coordination is blocked upfront.
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
                elapsedWallTimeMs = clock.nowMs() - startMs,
                validatedIdentitiesCount = 0,
                durableIndexedPlayerUpdatesCount = 0,
                durableIdentityTableUpsertsCount = 0,
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
        // No assumed 20:1 or 100:120 defaults: cluster rate state starts empty (unverified).
        // The first call to each cluster is the discovery call and counts against maxTotalHttpAttempts (<= 191).
        val verifiedClusterStates = LinkedHashMap<String, VerifiedClusterRateState>()
        var httpAttemptsMade = 0
        var validatedIdentitiesCount = 0
        var durableIndexedPlayerUpdatesCount = 0
        var durableIdentityTableUpsertsCount = 0
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

            // Check attempt ceiling before issuing call.
            if (httpAttemptsMade >= options.maxTotalHttpAttempts) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_ATTEMPT_CAP_REACHED
                abortReason = "Reached maxTotalHttpAttempts ceiling (${options.maxTotalHttpAttempts})"
                break
            }

            // Check header-driven wait requirement on the target routing cluster (if already discovered).
            val clusterState = verifiedClusterStates[candidate.routingCluster]
            val requiredWaitMs = clusterState?.requiredWaitMsBeforeNextAttempt() ?: 0L

            val elapsedBeforeWaitMs = clock.nowMs() - startMs
            val remainingWallTimeMs = options.maxWallTimeMs - elapsedBeforeWaitMs
            // Require enough wall-time budget for any required header wait plus at least 1ms for dispatch.
            if (remainingWallTimeMs <= 0L || requiredWaitMs >= remainingWallTimeMs) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                abortReason = if (requiredWaitMs > 0L) {
                    "Required header-driven wait (${requiredWaitMs}ms) on cluster '${candidate.routingCluster}' exceeds remaining wall-time budget (${remainingWallTimeMs.coerceAtLeast(0L)}ms)"
                } else {
                    "Wall-time budget (${options.maxWallTimeMs}ms) exhausted"
                }
                break
            }

            if (requiredWaitMs > 0L) {
                // Single header-driven sleep (never tight polling).
                clock.sleepMs(requiredWaitMs)
                totalWaitMs += requiredWaitMs
            }

            // Re-check wall-time budget right before socket dispatch.
            val elapsedBeforeCallMs = clock.nowMs() - startMs
            val remainingForCallMs = options.maxWallTimeMs - elapsedBeforeCallMs
            if (remainingForCallMs <= 0L) {
                terminalOutcome = RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
                abortReason = "Wall-time budget (${options.maxWallTimeMs}ms) exhausted before HTTP dispatch"
                break
            }

            val baseUrl = options.baseUrlByCluster[candidate.routingCluster]
                ?: throw IllegalArgumentException("Missing base URL for routing cluster '${candidate.routingCluster}'")
            val effectiveDeadlineMs = minOf(options.callDeadlineMs, remainingForCallMs)

            // Every HTTP attempt (including the first discovery call per cluster) counts against the 191 ceiling.
            httpAttemptsMade++

            val transportResult = httpTransport.executeSingleGet(
                url = "$baseUrl/riot/account/v1/accounts/by-puuid/${candidate.normalizedPuuid}",
                apiKey = apiKey,
                connectTimeoutMs = minOf(options.connectTimeoutMs.toLong(), effectiveDeadlineMs).toInt().coerceAtLeast(1),
                readTimeoutMs = minOf(options.readTimeoutMs.toLong(), effectiveDeadlineMs).toInt().coerceAtLeast(1),
                callDeadlineMs = effectiveDeadlineMs,
                maxBodyBytes = options.maxBodyBytes
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
                    index++
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
                    index++
                    terminalOutcome = RunOutcome.ABORTED_PAYLOAD_TOO_LARGE
                    abortReason = "Response body for PUUID '${candidate.normalizedPuuid}' exceeded ${options.maxBodyBytes} bytes cap"
                    break
                }

                is TransportResult.HttpResponse -> {
                    val status = transportResult.statusCode
                    val headers = transportResult.headersLowercase

                    // Check for 429 or 5xx BEFORE proceeding. Zero automatic retries: abort immediately.
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
                        index++
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
                        index++
                        terminalOutcome = RunOutcome.ABORTED_HTTP_5XX
                        abortReason = "Aborted on HTTP $status from cluster '${candidate.routingCluster}'"
                        break
                    }

                    // Enforce missing/invalid rate-limit header fail-safe on all non-429/5xx responses.
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
                        index++
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
                                index++
                                terminalOutcome = RunOutcome.ABORTED_TIMEOUT_OR_IO_ERROR
                                abortReason = "Malformed JSON body returned on HTTP 200 for PUUID '${candidate.normalizedPuuid}'"
                                break
                            }

                            val returnedPuuid = parsedPayload.puuid?.trim().orEmpty()
                            if (returnedPuuid != candidate.normalizedPuuid) {
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
                                        detail = "Account-V1 returned mismatched puuid '$returnedPuuid'"
                                    )
                                )
                                index++
                                terminalOutcome = RunOutcome.ABORTED_PUUID_MISMATCH
                                abortReason = "Account-V1 PUUID mismatch: requested '${candidate.normalizedPuuid}', received '$returnedPuuid'"
                                break
                            }

                            val gameName = parsedPayload.gameName?.trim().orEmpty()
                            val tagLine = parsedPayload.tagLine?.trim().orEmpty()

                            // Official Riot Account-V1 schema allows omitted gameName/tagLine.
                            // Never write blank strings; record explicit SKIPPED_OMITTED_RIOT_ID_FIELDS status.
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
                                            durableWriteTarget = "NONE_DRY_RUN",
                                            detail = "Validated non-blank Riot ID in dryRun mode (0 writes executed)"
                                        )
                                    )
                                } else {
                                    val writeTime = Instant.ofEpochMilli(clock.nowMs())
                                    val writeTarget = when (preCallState) {
                                        PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD -> {
                                            val updated = persistencePort.updateBlankFieldsOnExistingIndexedPlayer(
                                                puuid = candidate.normalizedPuuid,
                                                gameName = gameName,
                                                tagLine = tagLine
                                            )
                                            if (updated) durableIndexedPlayerUpdatesCount++
                                            // Also record verified identity in the dedicated identity-only table
                                            // without overwriting any concurrent newer/non-blank verified identity.
                                            val upserted = persistencePort.upsertVerifiedIdentityOnly(
                                                puuid = candidate.normalizedPuuid,
                                                platformRegion = candidate.normalizedPlatformRegion,
                                                gameName = gameName,
                                                tagLine = tagLine,
                                                verifiedAt = writeTime
                                            )
                                            if (upserted) durableIdentityTableUpsertsCount++
                                            "INDEXED_PLAYERS_BLANK_ONLY_AND_PLAYER_RIOT_IDENTITIES"
                                        }

                                        PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS,
                                        PreCallDbState.ALREADY_COMPLETE -> {
                                            val upserted = persistencePort.upsertVerifiedIdentityOnly(
                                                puuid = candidate.normalizedPuuid,
                                                platformRegion = candidate.normalizedPlatformRegion,
                                                gameName = gameName,
                                                tagLine = tagLine,
                                                verifiedAt = writeTime
                                            )
                                            if (upserted) durableIdentityTableUpsertsCount++
                                            "PLAYER_RIOT_IDENTITIES_ONLY"
                                        }
                                    }

                                    itemResults.add(
                                        ItemResult(
                                            puuid = candidate.normalizedPuuid,
                                            platformRegion = candidate.normalizedPlatformRegion,
                                            routingCluster = candidate.routingCluster,
                                            preCallDbState = preCallState,
                                            status = ItemStatus.COMPLETED_DURABLE_WRITE,
                                            httpAttempted = true,
                                            httpStatusCode = 200,
                                            resolvedGameName = gameName,
                                            resolvedTagLine = tagLine,
                                            durableWriteTarget = writeTarget,
                                            detail = "Durable identity persisted without fabricating match metadata"
                                        )
                                    )
                                }
                                index++
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
                            index++
                            terminalOutcome = RunOutcome.ABORTED_UNEXPECTED_HTTP_STATUS
                            abortReason = "Unexpected HTTP status $status on cluster '${candidate.routingCluster}'"
                            break
                        }
                    }
                }
            }
        }

        // Record any remaining unprocessed candidates as PENDING_UNPROCESSED for resumable reporting.
        val remainingEntries = ArrayList<ManifestEntry>()
        while (index < candidatesToFetch.size) {
            val pending = candidatesToFetch[index]
            val state = dbStatesByPuuid[pending.normalizedPuuid] ?: PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
            remainingEntries.add(pending)
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
            index++
        }

        val finalOutcome = terminalOutcome ?: RunOutcome.COMPLETED_ALL_CANDIDATES

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
            elapsedWallTimeMs = clock.nowMs() - startMs,
            validatedIdentitiesCount = validatedIdentitiesCount,
            durableIndexedPlayerUpdatesCount = durableIndexedPlayerUpdatesCount,
            durableIdentityTableUpsertsCount = durableIdentityTableUpsertsCount,
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

    private fun buildEarlyBlockedReport(
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
                preCallDbState = PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS,
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
            absentFromIndexedPlayersCount = manifest.entries.size,
            httpAttemptsMade = 0,
            maxTotalHttpAttempts = options.maxTotalHttpAttempts,
            totalWaitTimeMs = 0L,
            elapsedWallTimeMs = elapsedMs,
            validatedIdentitiesCount = 0,
            durableIndexedPlayerUpdatesCount = 0,
            durableIdentityTableUpsertsCount = 0,
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

    /**
     * Strictly parses all 4 required rate-limit headers:
     * - X-App-Rate-Limit
     * - X-App-Rate-Limit-Count
     * - X-Method-Rate-Limit
     * - X-Method-Rate-Limit-Count
     *
     * Returns null if any header is missing, blank, malformed, or if count windows do not align with limit windows.
     */
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
 * Clock abstraction for deterministic wall-time and header-driven wait testing without real sleeps.
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
 * Result of a single bounded HTTP GET to Account-V1.
 */
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
        maxBodyBytes: Int
    ): TransportResult
}

/**
 * Production-safe single-attempt HTTP transport that enforces:
 * - Zero automatic retries (`instanceFollowRedirects = false`)
 * - Strict connect and read socket timeouts
 * - Off-thread wall-clock deadline that actively calls `HttpURLConnection.disconnect()` and closes streams
 * - Strict `maxBodyBytes` (4 KiB) cap while reading the response stream
 */
class HttpUrlConnectionAccountV1Transport : BoundedAccountV1Transport {

    override fun executeSingleGet(
        url: String,
        apiKey: String,
        connectTimeoutMs: Int,
        readTimeoutMs: Int,
        callDeadlineMs: Long,
        maxBodyBytes: Int
    ): TransportResult {
        val connRef = AtomicReference<HttpURLConnection?>(null)
        val streamRef = AtomicReference<InputStream?>(null)
        val cancelled = AtomicBoolean(false)

        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "offprod-account-v1-single-call").apply { isDaemon = true }
        }

        val future: Future<TransportResult> = executor.submit(Callable {
            var connection: HttpURLConnection? = null
            try {
                connection = (URI.create(url).toURL().openConnection() as HttpURLConnection).apply {
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
                if (cancelled.get()) {
                    connection.disconnect()
                    return@Callable TransportResult.TimeoutOrIoError("Cancelled before connect")
                }

                val statusCode = connection.responseCode
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

                val bodyBytes = if (rawStream != null) {
                    readBoundedStream(rawStream, maxBodyBytes)
                        ?: return@Callable TransportResult.PayloadTooLarge(statusCode, maxBodyBytes)
                } else {
                    ByteArray(0)
                }

                TransportResult.HttpResponse(
                    statusCode = statusCode,
                    headersLowercase = headersLower,
                    bodyBytes = bodyBytes
                )
            } catch (e: Exception) {
                TransportResult.TimeoutOrIoError("${e.javaClass.simpleName}: ${e.message ?: "HTTP transport failure"}")
            } finally {
                try {
                    streamRef.getAndSet(null)?.close()
                } catch (_: Exception) {
                }
                try {
                    connRef.getAndSet(null)?.disconnect()
                } catch (_: Exception) {
                }
            }
        })

        return try {
            future.get(callDeadlineMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            cancelled.set(true)
            future.cancel(true)
            try {
                streamRef.getAndSet(null)?.close()
            } catch (_: Exception) {
            }
            try {
                connRef.getAndSet(null)?.disconnect()
            } catch (_: Exception) {
            }
            TransportResult.TimeoutOrIoError("Call deadline (${callDeadlineMs}ms) exceeded; socket disconnected")
        } catch (e: Exception) {
            cancelled.set(true)
            future.cancel(true)
            try {
                streamRef.getAndSet(null)?.close()
            } catch (_: Exception) {
            }
            try {
                connRef.getAndSet(null)?.disconnect()
            } catch (_: Exception) {
            }
            TransportResult.TimeoutOrIoError("Interrupted/failed waiting for HTTP call: ${e.message}")
        } finally {
            executor.shutdownNow()
        }
    }

    private fun readBoundedStream(input: InputStream, maxBodyBytes: Int): ByteArray? {
        val out = ByteArrayOutputStream(minOf(maxBodyBytes, 1024))
        val buffer = ByteArray(1024)
        var total = 0
        while (true) {
            val read = input.read(buffer)
            if (read == -1) break
            total += read
            if (total > maxBodyBytes) {
                return null
            }
            out.write(buffer, 0, read)
        }
        return out.toByteArray()
    }
}

/**
 * Persistence boundary for pre-call read-only recheck and race-safe identity-only durable writes.
 *
 * Notice there are ZERO methods that insert into `indexed_players` or fabricate `profile_icon_id`,
 * `summoner_level`, or `last_seen_at`.
 */
interface IdentityCompletionPersistencePort {
    fun recheckExistingIdentitiesBounded(
        entries: List<OffProdChallengerIdentityCompletionTool.ManifestEntry>
    ): Map<String, OffProdChallengerIdentityCompletionTool.PreCallDbState>

    /**
     * For rows that ALREADY exist in `indexed_players` with at least one blank identity column,
     * fills ONLY the blank column(s) while preserving any already-nonblank column and all match metadata.
     */
    fun updateBlankFieldsOnExistingIndexedPlayer(
        puuid: String,
        gameName: String,
        tagLine: String
    ): Boolean

    /**
     * Inserts or updates a row in the dedicated `player_riot_identities` table without fabricating
     * `indexed_players` metadata and without overwriting any concurrent newer or already-nonblank
     * verified identity.
     */
    fun upsertVerifiedIdentityOnly(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant
    ): Boolean
}

/**
 * JDBC implementation of [IdentityCompletionPersistencePort] with race-safe SQL statements
 * and bounded query/network timeouts.
 */
class JdbcIdentityCompletionPersistenceAdapter(
    private val dataSource: DataSource,
    private val checkIdentityTableInPreCall: Boolean = true
) : IdentityCompletionPersistencePort {

    companion object {
        val DDL_CREATE_IDENTITY_ONLY_TABLE: String = """
            CREATE TABLE IF NOT EXISTS player_riot_identities (
                puuid           VARCHAR(255) PRIMARY KEY,
                platform_region VARCHAR(16)  NOT NULL,
                game_name       VARCHAR(255) NOT NULL,
                tag_line        VARCHAR(255) NOT NULL,
                verified_at     TIMESTAMPTZ  NOT NULL,
                CONSTRAINT chk_player_riot_identities_nonblank
                    CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
            )
        """.trimIndent()

        /**
         * Race-safe blank-only update on existing `indexed_players` rows:
         * - Preserves `game_name` if `game_name` is already non-blank.
         * - Preserves `tag_line` if `tag_line` is already non-blank.
         * - Never modifies `region`, `profile_icon_id`, `summoner_level`, or `last_seen_at`.
         * - Affects 0 rows if both `game_name` and `tag_line` are already non-blank.
         */
        val SQL_UPDATE_INDEXED_PLAYERS_BLANK_FIELDS_ONLY: String = """
            UPDATE indexed_players
            SET game_name = CASE WHEN BTRIM(COALESCE(game_name, '')) = '' THEN ? ELSE game_name END,
                tag_line  = CASE WHEN BTRIM(COALESCE(tag_line, '')) = ''  THEN ? ELSE tag_line  END
            WHERE puuid = ?
              AND (BTRIM(COALESCE(game_name, '')) = '' OR BTRIM(COALESCE(tag_line, '')) = '')
              AND BTRIM(COALESCE(?, '')) <> ''
              AND BTRIM(COALESCE(?, '')) <> ''
        """.trimIndent()

        /**
         * Race-safe upsert into the dedicated identity-only table `player_riot_identities`:
         * - On conflict (`puuid`), NEVER overwrites a concurrent row that already has non-blank
         *   `game_name` AND non-blank `tag_line` when `player_riot_identities.verified_at >= EXCLUDED.verified_at`
         *   (and in missing-only mode, preserves any already-nonblank field unless strictly newer AND non-blank).
         * - Preserves each already-nonblank column if a partial row existed, or updates only when
         *   a field was blank or `EXCLUDED.verified_at > player_riot_identities.verified_at` while existing
         *   is older, and never regresses `verified_at`.
         */
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
    }

    override fun recheckExistingIdentitiesBounded(
        entries: List<OffProdChallengerIdentityCompletionTool.ManifestEntry>
    ): Map<String, OffProdChallengerIdentityCompletionTool.PreCallDbState> {
        if (entries.isEmpty()) return emptyMap()
        val puuids = entries.map { it.normalizedPuuid }.distinct()
        val result = LinkedHashMap<String, OffProdChallengerIdentityCompletionTool.PreCallDbState>(puuids.size)
        for (p in puuids) {
            result[p] = OffProdChallengerIdentityCompletionTool.PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
        }

        val placeholders = puuids.joinToString(",") { "?" }
        val indexedPlayersSql =
            "SELECT puuid, game_name, tag_line FROM indexed_players WHERE puuid IN ($placeholders)"

        val networkTimeoutExecutor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-net-timeout").apply { isDaemon = true }
        }

        try {
            dataSource.connection.use { conn ->
                applyBoundedConnectionSettings(conn, readOnly = true, networkTimeoutExecutor)
                conn.prepareStatement(indexedPlayersSql).use { ps ->
                    ps.queryTimeout = OffProdChallengerIdentityCompletionTool.DB_QUERY_TIMEOUT_SECONDS
                    for ((idx, puuid) in puuids.withIndex()) {
                        ps.setString(idx + 1, puuid)
                    }
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
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
                }

                if (checkIdentityTableInPreCall) {
                    val unresolvedPuuids = result.entries
                        .filter { it.value != OffProdChallengerIdentityCompletionTool.PreCallDbState.ALREADY_COMPLETE }
                        .map { it.key }
                    if (unresolvedPuuids.isNotEmpty()) {
                        val idPlaceholders = unresolvedPuuids.joinToString(",") { "?" }
                        val identitySql =
                            "SELECT puuid, game_name, tag_line FROM player_riot_identities WHERE puuid IN ($idPlaceholders)"
                        conn.prepareStatement(identitySql).use { ps ->
                            ps.queryTimeout = OffProdChallengerIdentityCompletionTool.DB_QUERY_TIMEOUT_SECONDS
                            for ((idx, puuid) in unresolvedPuuids.withIndex()) {
                                ps.setString(idx + 1, puuid)
                            }
                            ps.executeQuery().use { rs ->
                                while (rs.next()) {
                                    val p = rs.getString("puuid")?.trim().orEmpty()
                                    val gn = rs.getString("game_name")?.trim().orEmpty()
                                    val tl = rs.getString("tag_line")?.trim().orEmpty()
                                    if (p.isNotEmpty() && gn.isNotEmpty() && tl.isNotEmpty()) {
                                        result[p] = OffProdChallengerIdentityCompletionTool.PreCallDbState.ALREADY_COMPLETE
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            networkTimeoutExecutor.shutdownNow()
        }

        return result
    }

    override fun updateBlankFieldsOnExistingIndexedPlayer(
        puuid: String,
        gameName: String,
        tagLine: String
    ): Boolean {
        val trimmedPuuid = puuid.trim()
        val trimmedGameName = gameName.trim()
        val trimmedTagLine = tagLine.trim()
        require(trimmedPuuid.isNotEmpty() && trimmedGameName.isNotEmpty() && trimmedTagLine.isNotEmpty()) {
            "updateBlankFieldsOnExistingIndexedPlayer requires non-blank puuid, gameName, and tagLine"
        }

        val networkTimeoutExecutor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-net-timeout").apply { isDaemon = true }
        }
        return try {
            dataSource.connection.use { conn ->
                applyBoundedConnectionSettings(conn, readOnly = false, networkTimeoutExecutor)
                conn.prepareStatement(SQL_UPDATE_INDEXED_PLAYERS_BLANK_FIELDS_ONLY).use { ps ->
                    ps.queryTimeout = OffProdChallengerIdentityCompletionTool.DB_QUERY_TIMEOUT_SECONDS
                    ps.setString(1, trimmedGameName)
                    ps.setString(2, trimmedTagLine)
                    ps.setString(3, trimmedPuuid)
                    ps.setString(4, trimmedGameName)
                    ps.setString(5, trimmedTagLine)
                    ps.executeUpdate() > 0
                }
            }
        } finally {
            networkTimeoutExecutor.shutdownNow()
        }
    }

    override fun upsertVerifiedIdentityOnly(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant
    ): Boolean {
        val trimmedPuuid = puuid.trim()
        val trimmedRegion = platformRegion.trim().lowercase(Locale.ROOT)
        val trimmedGameName = gameName.trim()
        val trimmedTagLine = tagLine.trim()
        require(
            trimmedPuuid.isNotEmpty() &&
                trimmedRegion.isNotEmpty() &&
                trimmedGameName.isNotEmpty() &&
                trimmedTagLine.isNotEmpty()
        ) {
            "upsertVerifiedIdentityOnly requires non-blank puuid, platformRegion, gameName, and tagLine"
        }

        val networkTimeoutExecutor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "offprod-db-net-timeout").apply { isDaemon = true }
        }
        return try {
            dataSource.connection.use { conn ->
                applyBoundedConnectionSettings(conn, readOnly = false, networkTimeoutExecutor)
                conn.prepareStatement(SQL_UPSERT_PLAYER_RIOT_IDENTITIES_RACE_SAFE).use { ps ->
                    ps.queryTimeout = OffProdChallengerIdentityCompletionTool.DB_QUERY_TIMEOUT_SECONDS
                    ps.setString(1, trimmedPuuid)
                    ps.setString(2, trimmedRegion)
                    ps.setString(3, trimmedGameName)
                    ps.setString(4, trimmedTagLine)
                    ps.setTimestamp(5, Timestamp.from(verifiedAt))
                    ps.executeUpdate() > 0
                }
            }
        } finally {
            networkTimeoutExecutor.shutdownNow()
        }
    }

    private fun applyBoundedConnectionSettings(
        conn: Connection,
        readOnly: Boolean,
        executor: java.util.concurrent.Executor
    ) {
        try {
            conn.isReadOnly = readOnly
        } catch (_: Exception) {
        }
        try {
            conn.setNetworkTimeout(executor, OffProdChallengerIdentityCompletionTool.DB_NETWORK_TIMEOUT_MS)
        } catch (_: Exception) {
        }
    }
}
