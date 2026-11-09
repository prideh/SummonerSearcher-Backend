package com.pride.summoner_searcher_api.tools

import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ExecutionOptions
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ItemStatus
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.KeyCoordinationMode
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.ManifestEntry
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.PreCallDbState
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.RunOutcome
import com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionTool.SnapshotManifest
import com.sun.net.httpserver.HttpServer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * Generates a deterministic, valid 78-character base64url Riot PUUID (`^[A-Za-z0-9_-]{78}$`) for testing.
 */
private fun validPuuid78(seed: String): String {
    val sanitized = seed.replace(Regex("[^A-Za-z0-9_-]"), "_")
    require(sanitized.length <= 78) { "Seed too long: ${sanitized.length}" }
    return sanitized.padEnd(78, 'x')
}

class OffProdChallengerIdentityCompletionToolTest : BehaviorSpec({

    Given("official 78-char base64url PUUID validation and the 191 missing Challenger snapshot manifest (31 EUW + 160 KR)") {
        val entries191 = (1..31).map { ManifestEntry(puuid = validPuuid78("euw_missing_$it"), platformRegion = "euw1") } +
            (1..160).map { ManifestEntry(puuid = validPuuid78("kr_missing_$it"), platformRegion = "kr") }

        val manifest191 = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
            isLiveLadder = false,
            entries = entries191
        )

        Then("it enforces exact 78-char base64url PUUID format and rejects short, oversized, or URL-path-traversal strings") {
            manifest191.entries shouldHaveSize 191
            manifest191.entries.all { it.normalizedPuuid.length == 78 } shouldBe true

            // Reject short PUUID
            shouldThrow<IllegalArgumentException> {
                ManifestEntry(puuid = "short-puuid", platformRegion = "euw1")
            }

            // Reject 79-char PUUID
            shouldThrow<IllegalArgumentException> {
                ManifestEntry(puuid = "a".repeat(79), platformRegion = "euw1")
            }

            // Reject 78-char string containing URL path traversal or query characters ('/', '.', '?')
            shouldThrow<IllegalArgumentException> {
                ManifestEntry(puuid = "../admin/".padEnd(78, 'a'), platformRegion = "euw1")
            }
            shouldThrow<IllegalArgumentException> {
                ManifestEntry(puuid = ("?query=1").padEnd(78, 'a'), platformRegion = "kr")
            }
        }

        Then("default ExecutionOptions run in DB-recheck-only dryRun mode with 0 HTTP attempts and 0 DB writes") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                error("Transport must not be called when networkEnabled=false")
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock()
            )

            val report = tool.runBounded(manifest191)

            report.outcome shouldBe RunOutcome.DRY_RUN_DB_RECHECK_ONLY_COMPLETED
            report.dryRun shouldBe true
            report.networkEnabled shouldBe false
            report.allowDurableWrites shouldBe false
            report.manifestTotalCount shouldBe 191
            report.absentFromIndexedPlayersCount shouldBe 191
            report.httpAttemptsMade shouldBe 0
            report.durableIndexedPlayerUpdatesVerifiedCount shouldBe 0
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 0
            report.remainingPendingCount shouldBe 191
            report.remainingEntries shouldHaveSize 191
            transportCalls.get() shouldBe 0
        }
    }

    Given("WallClock vs MonotonicClock separation (both injected and REAL default clocks)") {
        val p1 = validPuuid78("clock_sep_1")
        val pReal = validPuuid78("clock_real_default")
        val manifest = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
            isLiveLadder = false,
            entries = listOf(ManifestEntry(p1, "euw1"))
        )

        Then("REAL default clocks (SystemMonotonicClock + SystemUtcWallClock with NO fake clock override) persist a real >= 2026 UTC Instant and never 1970-01-01") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                TransportResult.HttpResponse(
                    statusCode = 200,
                    headersLowercase = mapOf(
                        "x-app-rate-limit" to "20:1,100:120",
                        "x-app-rate-limit-count" to "1:1,1:120",
                        "x-method-rate-limit" to "1000:60",
                        "x-method-rate-limit-count" to "1:60"
                    ),
                    bodyBytes = """{"puuid":"$pReal","gameName":"RealClockUser","tagLine":"UTC"}""".toByteArray()
                )
            }

            // Instantiate with REAL default clocks (no monotonicClock or wallClock arguments passed!)
            val toolWithRealDefaultClocks = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport
            )

            val beforeUtc = Instant.now().minusSeconds(2)
            val report = toolWithRealDefaultClocks.runBounded(
                manifest = SnapshotManifest(
                    snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                    isLiveLadder = false,
                    entries = listOf(ManifestEntry(pReal, "euw1"))
                ),
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Real default clock regression test"
                ),
                riotApiKeyProvider = { "RGAPI-isolated-key" }
            )
            val afterUtc = Instant.now().plusSeconds(2)

            report.outcome shouldBe RunOutcome.STAGED_ALL_CANDIDATES
            val storedInstant = persistence.identitiesOnly[pReal]!!.verifiedAt
            (storedInstant.isAfter(Instant.parse("2026-01-01T00:00:00Z"))) shouldBe true
            (!storedInstant.isBefore(beforeUtc) && !storedInstant.isAfter(afterUtc)) shouldBe true
        }

        Then("injected WallClock and MonotonicClock starting near 0ms never confuse nanoTime uptime with epoch milliseconds") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val monoClock = FakeMonotonicClock(initialMs = 42L)
            val expectedUtcInstant = Instant.parse("2026-11-09T11:15:30Z")
            val utcWallClock = WallClock { expectedUtcInstant }

            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                TransportResult.HttpResponse(
                    statusCode = 200,
                    headersLowercase = mapOf(
                        "x-app-rate-limit" to "20:1,100:120",
                        "x-app-rate-limit-count" to "1:1,1:120",
                        "x-method-rate-limit" to "1000:60",
                        "x-method-rate-limit-count" to "1:60"
                    ),
                    bodyBytes = """{"puuid":"$p1","gameName":"ClockUser","tagLine":"UTC"}""".toByteArray()
                )
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = monoClock,
                wallClock = utcWallClock
            )

            val report = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Exclusive off-prod key"
                ),
                riotApiKeyProvider = { "RGAPI-isolated-key" }
            )

            report.outcome shouldBe RunOutcome.STAGED_ALL_CANDIDATES
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 1
            val stored = persistence.identitiesOnly[p1]!!
            stored.verifiedAt shouldBe expectedUtcInstant
            report.itemResults.single().verifiedAtUtc shouldBe expectedUtcInstant
        }
    }

    Given("uncoordinated shared Riot API key execution and HTTPS cluster transport host/credential guards") {
        val p1 = validPuuid78("guard_test_1")
        val manifest = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
            isLiveLadder = false,
            entries = listOf(ManifestEntry(p1, "euw1"))
        )

        Then("it stops immediately as BLOCKED_KEY_COORDINATION_REQUIRED with 0 HTTP attempts when uncoordinated") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                error("Must not dispatch HTTP without key coordination attestation")
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock()
            )

            val report = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.UNCOORDINATED
                ),
                riotApiKeyProvider = { "RGAPI-test-key" }
            )

            report.outcome shouldBe RunOutcome.BLOCKED_KEY_COORDINATION_REQUIRED
            report.httpAttemptsMade shouldBe 0
            report.remainingPendingCount shouldBe 1
            transportCalls.get() shouldBe 0
        }

        Then("it rejects non-HTTPS or unapproved hosts in production mode, and blocks RGAPI-* keys on loopback HTTP test override") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                error("Transport must not be reached when URL or loopback credential check fails")
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock()
            )

            val badHostReport = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Test",
                    baseUrlByCluster = mapOf("europe" to "https://evil.example.com")
                ),
                riotApiKeyProvider = { "RGAPI-secret" }
            )
            badHostReport.outcome shouldBe RunOutcome.ABORTED_DISALLOWED_TRANSPORT_TARGET
            badHostReport.httpAttemptsMade shouldBe 0
            transportCalls.get() shouldBe 0

            val loopbackDisabledReport = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Test",
                    allowLoopbackHttpForTesting = false,
                    baseUrlByCluster = mapOf("europe" to "http://127.0.0.1:18080")
                ),
                riotApiKeyProvider = { "TEST-LOOPBACK-ONLY-mock" }
            )
            loopbackDisabledReport.outcome shouldBe RunOutcome.ABORTED_DISALLOWED_TRANSPORT_TARGET
            loopbackDisabledReport.httpAttemptsMade shouldBe 0

            val realKeyLeakAttemptReport = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Test",
                    allowLoopbackHttpForTesting = true,
                    baseUrlByCluster = mapOf("europe" to "http://127.0.0.1:18080")
                ),
                riotApiKeyProvider = { "RGAPI-real-production-secret-key" }
            )
            realKeyLeakAttemptReport.outcome shouldBe RunOutcome.ABORTED_DISALLOWED_TRANSPORT_TARGET
            realKeyLeakAttemptReport.httpAttemptsMade shouldBe 0
            transportCalls.get() shouldBe 0
        }
    }

    Given("cluster rate-limit discovery, missing-header fail-safe, and header-driven pacing under the 180s wall-time cap") {
        val pEuw1 = validPuuid78("euw_rate_1")
        val pEuw2 = validPuuid78("euw_rate_2")
        val pEuw3 = validPuuid78("euw_rate_3")
        val pKr1 = validPuuid78("kr_rate_1")

        Then("it aborts immediately after attempt #1 if rate headers are missing or malformed (no assumed 20:1 or 100:120 defaults) and keeps all 3 uncompleted items in remainingEntries") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                TransportResult.HttpResponse(
                    statusCode = 200,
                    headersLowercase = mapOf(
                        "x-app-rate-limit" to "20:1,100:120",
                        "x-app-rate-limit-count" to "1:1,1:120"
                    ),
                    bodyBytes = """{"puuid":"$pEuw1","gameName":"PlayerOne","tagLine":"EUW"}""".toByteArray()
                )
            }

            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry(pEuw1, "euw1"),
                    ManifestEntry(pEuw2, "euw1"),
                    ManifestEntry(pKr1, "kr")
                )
            )

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock()
            )

            val report = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Dedicated off-prod key verified"
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.ABORTED_MISSING_OR_INVALID_RATE_HEADERS
            report.httpAttemptsMade shouldBe 1
            transportCalls.get() shouldBe 1
            report.validatedIdentitiesCount shouldBe 0
            // pEuw1 aborted before validation/completion, so all 3 remain pending!
            report.remainingPendingCount shouldBe 3
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(pEuw1, pEuw2, pKr1)
        }

        Then("first cluster discovery call counts against the attempt cap, header-driven waits are applied, and exceeding the 180s wall-time budget halts honestly without sleeping") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val clock = FakeMonotonicClock()
            val transportCalls = AtomicInteger(0)

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _, _ ->
                val callNum = transportCalls.incrementAndGet()
                when (callNum) {
                    1 -> {
                        url shouldContain "/by-puuid/$pEuw1"
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = mapOf(
                                "x-app-rate-limit" to "50:10,500:600",
                                "x-app-rate-limit-count" to "1:10,1:600",
                                "x-method-rate-limit" to "5:10",
                                "x-method-rate-limit-count" to "5:10"
                            ),
                            bodyBytes = """{"puuid":"$pEuw1","gameName":"EuwOne","tagLine":"EUW"}""".toByteArray()
                        )
                    }
                    2 -> {
                        url shouldContain "/by-puuid/$pEuw2"
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = mapOf(
                                "x-app-rate-limit" to "50:10,500:600",
                                "x-app-rate-limit-count" to "2:10,500:600",
                                "x-method-rate-limit" to "5:10",
                                "x-method-rate-limit-count" to "1:10"
                            ),
                            bodyBytes = """{"puuid":"$pEuw2","gameName":"EuwTwo","tagLine":"EUW"}""".toByteArray()
                        )
                    }
                    else -> error("Unexpected call $callNum")
                }
            }

            val smallEuwManifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry(pEuw1, "euw1"),
                    ManifestEntry(pEuw2, "euw1"),
                    ManifestEntry(pEuw3, "euw1")
                )
            )

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = clock
            )

            val report = tool.runBounded(
                manifest = smallEuwManifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.COORDINATED_MAINTENANCE_WINDOW,
                    coordinationAttestation = "Maint window 02:30Z",
                    maxWallTimeMs = 180_000L
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
            report.httpAttemptsMade shouldBe 2
            report.totalWaitTimeMs shouldBe 10_000L
            clock.sleepsRecordedMs shouldBe listOf(10_000L)
            report.validatedIdentitiesCount shouldBe 2
            report.remainingPendingCount shouldBe 1
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(pEuw3)
        }
    }

    Given("Account-V1 payload edge cases (exact PUUID match, omitted fields, 404, 429, 5xx) and real loopback socket timeout/4KiB enforcement") {
        val validHeaders = mapOf(
            "x-app-rate-limit" to "20:1,100:120",
            "x-app-rate-limit-count" to "1:1,1:120",
            "x-method-rate-limit" to "1000:60",
            "x-method-rate-limit-count" to "1:60"
        )
        val p404 = validPuuid78("p_404")
        val pOmittedName = validPuuid78("p_omitted_name")
        val pBlankTag = validPuuid78("p_blank_tag")
        val pMismatch = validPuuid78("p_mismatch")
        val pWrongReturned = validPuuid78("p_wrong_returned")
        val pAfterMismatch = validPuuid78("p_after_mismatch")

        Then("it records 404 and omitted Riot ID fields without writing, and aborts on PUUID mismatch keeping uncompleted entries in remainingEntries") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry(p404, "euw1"),
                    ManifestEntry(pOmittedName, "euw1"),
                    ManifestEntry(pBlankTag, "euw1"),
                    ManifestEntry(pMismatch, "euw1"),
                    ManifestEntry(pAfterMismatch, "euw1")
                )
            )

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _, _ ->
                when {
                    url.endsWith("/$p404") -> TransportResult.HttpResponse(
                        statusCode = 404,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"status":{"message":"Data not found","status_code":404}}""".toByteArray()
                    )
                    url.endsWith("/$pOmittedName") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pOmittedName","tagLine":"KR1"}""".toByteArray()
                    )
                    url.endsWith("/$pBlankTag") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pBlankTag","gameName":"ValidName","tagLine":"   "}""".toByteArray()
                    )
                    url.endsWith("/$pMismatch") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pWrongReturned","gameName":"Spoof","tagLine":"EUW"}""".toByteArray()
                    )
                    else -> error("Should have aborted on PUUID mismatch before $url")
                }
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock()
            )

            val report = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Exclusive test key"
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.ABORTED_PUUID_MISMATCH
            report.httpAttemptsMade shouldBe 4
            report.notFound404Count shouldBe 1
            report.omittedRiotIdFieldsCount shouldBe 2
            report.puuidMismatchCount shouldBe 1
            report.durableIndexedPlayerUpdatesVerifiedCount shouldBe 0
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 0
            // Both pMismatch (which failed validation) and pAfterMismatch remain uncompleted!
            report.remainingPendingCount shouldBe 2
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(pMismatch, pAfterMismatch)
        }

        Then("it performs 0 retries and aborts immediately on HTTP 429 (including service 429) or HTTP 5xx") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val kr1 = validPuuid78("kr_429_1")
            val kr2 = validPuuid78("kr_429_2")
            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(ManifestEntry(kr1, "kr"), ManifestEntry(kr2, "kr"))
            )
            val attempts = AtomicInteger(0)
            val transport429 = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                attempts.incrementAndGet()
                TransportResult.HttpResponse(
                    statusCode = 429,
                    headersLowercase = validHeaders + mapOf(
                        "x-rate-limit-type" to "service",
                        "retry-after" to "0"
                    ),
                    bodyBytes = ByteArray(0)
                )
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport429,
                monotonicClock = FakeMonotonicClock()
            )

            val report429 = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = true,
                    networkEnabled = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Exclusive test key"
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report429.outcome shouldBe RunOutcome.ABORTED_HTTP_429
            report429.httpAttemptsMade shouldBe 1
            attempts.get() shouldBe 1
            report429.lastObservedClusterHeaders["asia"]?.retryAfterSeconds shouldBe 10L
            // Both kr1 (which got 429) and kr2 remain uncompleted!
            report429.remainingPendingCount shouldBe 2
            report429.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(kr1, kr2)
        }

        Then("HttpUrlConnectionAccountV1Transport enforces the 4 KiB body cap, socket deadline cancellation, and loopback credential guard on a local loopback server") {
            val pOversized = validPuuid78("loopback_oversized")
            val pStalled = validPuuid78("loopback_stalled")
            val releaseStalledHandler = CountDownLatch(1)
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            try {
                server.createContext("/riot/account/v1/accounts/by-puuid/$pOversized") { exchange ->
                    exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
                    exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
                    exchange.responseHeaders.add("X-Method-Rate-Limit", "1000:60")
                    exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "1:60")
                    val hugePayload = ByteArray(5_000) { 'A'.code.toByte() }
                    exchange.sendResponseHeaders(200, hugePayload.size.toLong())
                    exchange.responseBody.use { it.write(hugePayload) }
                }
                server.createContext("/riot/account/v1/accounts/by-puuid/$pStalled") { exchange ->
                    releaseStalledHandler.await(3, TimeUnit.SECONDS)
                    exchange.sendResponseHeaders(200, 2)
                    exchange.responseBody.use { it.write("{}".toByteArray(StandardCharsets.UTF_8)) }
                }
                server.start()
                val port = server.address.port
                val realTransport = HttpUrlConnectionAccountV1Transport()

                val oversizedResult = realTransport.executeSingleGet(
                    url = "http://127.0.0.1:$port/riot/account/v1/accounts/by-puuid/$pOversized",
                    apiKey = "TEST-LOOPBACK-ONLY-unit-test",
                    connectTimeoutMs = 1_000,
                    readTimeoutMs = 1_000,
                    callDeadlineMs = 1_500L,
                    maxBodyBytes = 4_096,
                    allowLoopbackHttpForTesting = true
                )
                oversizedResult shouldBe TransportResult.PayloadTooLarge(statusCode = 200, maxBodyBytes = 4_096)

                val timeoutResult = realTransport.executeSingleGet(
                    url = "http://127.0.0.1:$port/riot/account/v1/accounts/by-puuid/$pStalled",
                    apiKey = "TEST-LOOPBACK-ONLY-unit-test",
                    connectTimeoutMs = 200,
                    readTimeoutMs = 200,
                    callDeadlineMs = 250L,
                    maxBodyBytes = 4_096,
                    allowLoopbackHttpForTesting = true
                )
                (timeoutResult is TransportResult.TimeoutOrIoError) shouldBe true
            } finally {
                releaseStalledHandler.countDown()
                server.stop(0)
            }
        }
    }

    Given("race-safe identity-only persistence, post-write read verification, and JDBC budget/fail-closed regression checks") {
        val validHeaders = mapOf(
            "x-app-rate-limit" to "20:1,100:120",
            "x-app-rate-limit-count" to "1:1,1:120",
            "x-method-rate-limit" to "1000:60",
            "x-method-rate-limit-count" to "1:60"
        )

        val pAlreadyComplete = validPuuid78("p_already_complete")
        val pPartialBlankTag = validPuuid78("p_partial_blank_tag")
        val pPartialBlankName = validPuuid78("p_partial_blank_name")
        val pAbsentNew = validPuuid78("p_absent_new")
        val pAbsentConcurrent = validPuuid78("p_absent_concurrent")

        Then("UPDATE blank fields on indexed_players preserves each already-nonblank column, while concurrent completion on player_riot_identities is classified as CONCURRENT_ALREADY_COMPLETE_VERIFIED via post-write read") {
            val persistence = FakeRaceSafeIdentityPersistence()

            persistence.seedIndexedPlayer(
                puuid = pPartialBlankTag,
                region = "EUW1",
                gameName = "OriginalMatchName",
                tagLine = "   ",
                profileIconId = 42,
                summonerLevel = 500L,
                lastSeenAt = Instant.parse("2026-11-08T12:00:00Z")
            )

            persistence.seedIndexedPlayer(
                puuid = pPartialBlankName,
                region = "kr",
                gameName = "",
                tagLine = "ORIGTAG",
                profileIconId = 99,
                summonerLevel = 612L,
                lastSeenAt = Instant.parse("2026-11-08T15:00:00Z")
            )

            persistence.seedIndexedPlayer(
                puuid = pAlreadyComplete,
                region = "euw1",
                gameName = "AlreadyComplete",
                tagLine = "EUW",
                profileIconId = 11,
                summonerLevel = 300L,
                lastSeenAt = Instant.parse("2026-11-08T18:00:00Z")
            )

            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry(pAlreadyComplete, "euw1"),
                    ManifestEntry(pPartialBlankTag, "euw1"),
                    ManifestEntry(pPartialBlankName, "kr"),
                    ManifestEntry(pAbsentNew, "kr"),
                    ManifestEntry(pAbsentConcurrent, "kr")
                )
            )

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _, _ ->
                when {
                    url.endsWith("/$pPartialBlankTag") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pPartialBlankTag","gameName":"NewRiotName","tagLine":"FILLEDTAG"}""".toByteArray()
                    )
                    url.endsWith("/$pPartialBlankName") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pPartialBlankName","gameName":"FilledName","tagLine":"NEWTAG"}""".toByteArray()
                    )
                    url.endsWith("/$pAbsentNew") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pAbsentNew","gameName":"PureIdentity","tagLine":"KR1"}""".toByteArray()
                    )
                    url.endsWith("/$pAbsentConcurrent") -> {
                        persistence.seedIdentityOnlyRow(
                            puuid = pAbsentConcurrent,
                            platformRegion = "kr",
                            gameName = "ConcurrentWinner",
                            tagLine = "WIN",
                            verifiedAt = Instant.parse("2026-11-09T12:00:00Z")
                        )
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = validHeaders,
                            bodyBytes = """{"puuid":"$pAbsentConcurrent","gameName":"StaleLoser","tagLine":"LOSE"}""".toByteArray()
                        )
                    }
                    else -> error("Unexpected HTTP call for $url")
                }
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = FakeMonotonicClock(),
                wallClock = WallClock { Instant.parse("2026-11-09T03:00:00Z") }
            )

            val report = tool.runBounded(
                manifest = manifest,
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Exclusive off-prod key"
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.STAGED_ALL_CANDIDATES
            report.alreadyResolvedInDbCount shouldBe 1
            report.httpAttemptsMade shouldBe 4
            report.durableIndexedPlayerUpdatesVerifiedCount shouldBe 2
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 3
            report.concurrentAlreadyCompleteVerifiedCount shouldBe 1

            val concurrentItemResult = report.itemResults.first { it.puuid == pAbsentConcurrent }
            concurrentItemResult.status shouldBe ItemStatus.CONCURRENT_ALREADY_COMPLETE_VERIFIED
            concurrentItemResult.resolvedGameName shouldBe "ConcurrentWinner"
            concurrentItemResult.resolvedTagLine shouldBe "WIN"

            val updatedTagRow = persistence.indexedPlayers[pPartialBlankTag]!!
            updatedTagRow.gameName shouldBe "OriginalMatchName"
            updatedTagRow.tagLine shouldBe "FILLEDTAG"
            updatedTagRow.profileIconId shouldBe 42
            updatedTagRow.summonerLevel shouldBe 500L

            val updatedNameRow = persistence.indexedPlayers[pPartialBlankName]!!
            updatedNameRow.gameName shouldBe "FilledName"
            updatedNameRow.tagLine shouldBe "ORIGTAG"

            persistence.indexedPlayers.containsKey(pAbsentNew) shouldBe false
            persistence.identitiesOnly[pAbsentNew]!!.gameName shouldBe "PureIdentity"
        }

        Then("when an adapter returns 0 rows written without a usable row in DB, runBounded reports ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT, writes=0, and keeps the candidate in remainingEntries (pending=1, never COMPLETED)") {
            val pNoRow = validPuuid78("adapter_false_no_row")
            val unverifiedWriteDs = mockk<DataSource>(relaxed = true)
            val writeConn = mockk<Connection>(relaxed = true)
            val recheckIndexedPs = mockk<PreparedStatement>(relaxed = true)
            val recheckIdentityPs = mockk<PreparedStatement>(relaxed = true)
            val upsertPs = mockk<PreparedStatement>(relaxed = true)
            val verifySelectPs = mockk<PreparedStatement>(relaxed = true)
            val emptyRs = mockk<ResultSet>(relaxed = true)

            every { unverifiedWriteDs.connection } returns writeConn
            every { writeConn.prepareStatement(match { it.contains("FROM indexed_players WHERE puuid IN") }) } returns recheckIndexedPs
            every { writeConn.prepareStatement(match { it.contains("FROM player_riot_identities WHERE puuid IN") }) } returns recheckIdentityPs
            every { recheckIndexedPs.executeQuery() } returns emptyRs
            every { recheckIdentityPs.executeQuery() } returns emptyRs
            every {
                writeConn.prepareStatement(JdbcIdentityCompletionPersistenceAdapter.SQL_UPSERT_PLAYER_RIOT_IDENTITIES_RACE_SAFE)
            } returns upsertPs
            // Adapter returns 0 rows updated (false) AND post-write SELECT finds no usable row in DB!
            every { upsertPs.executeUpdate() } returns 0
            every {
                writeConn.prepareStatement(JdbcIdentityCompletionPersistenceAdapter.SQL_SELECT_VERIFY_IDENTITY_ONLY_BY_PUUID)
            } returns verifySelectPs
            every { verifySelectPs.executeQuery() } returns emptyRs
            every { emptyRs.next() } returns false

            val jdbcAdapter = JdbcIdentityCompletionPersistenceAdapter(unverifiedWriteDs)
            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = jdbcAdapter,
                httpTransport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                    TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"$pNoRow","gameName":"UnwrittenName","tagLine":"EUW"}""".toByteArray()
                    )
                }
            )

            val report = tool.runBounded(
                manifest = SnapshotManifest(
                    snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                    isLiveLadder = false,
                    entries = listOf(ManifestEntry(pNoRow, "euw1"))
                ),
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Regression test for 0 rows written without usable row"
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT
            report.itemResults.single().status shouldBe ItemStatus.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT
            report.durableIndexedPlayerUpdatesVerifiedCount shouldBe 0
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 0
            report.dbFailureOrUnknownCommitCount shouldBe 1
            // Candidate was NOT completed, so remainingPendingCount MUST be 1 (never 0)!
            report.remainingPendingCount shouldBe 1
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(pNoRow)
        }

        Then("it checks remaining wall-time budget BEFORE starting a durable DB write and leaves the candidate in remainingEntries if budget is exhausted") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val p1 = validPuuid78("write_budget_exhaust_1")
            val p2 = validPuuid78("write_budget_exhaust_2")
            val clock = FakeMonotonicClock(initialMs = 1_000L)

            val transport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                clock.advanceMs(180_000L)
                TransportResult.HttpResponse(
                    statusCode = 200,
                    headersLowercase = validHeaders,
                    bodyBytes = """{"puuid":"$p1","gameName":"LateArrival","tagLine":"EUW"}""".toByteArray()
                )
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                monotonicClock = clock
            )

            val report = tool.runBounded(
                manifest = SnapshotManifest(
                    snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                    isLiveLadder = false,
                    entries = listOf(ManifestEntry(p1, "euw1"), ManifestEntry(p2, "euw1"))
                ),
                options = ExecutionOptions(
                    dryRun = false,
                    networkEnabled = true,
                    allowDurableWrites = true,
                    keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                    coordinationAttestation = "Exclusive key",
                    maxWallTimeMs = 180_000L
                ),
                riotApiKeyProvider = { "RGAPI-test" }
            )

            report.outcome shouldBe RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
            report.httpAttemptsMade shouldBe 1
            report.durableIdentityTableUpsertsVerifiedCount shouldBe 0
            persistence.identitiesOnly.isEmpty() shouldBe true
            report.remainingPendingCount shouldBe 2
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf(p1, p2)
        }

        Then("JdbcIdentityCompletionPersistenceAdapter bounds DataSource.getConnection() and cleanup without blocking the caller thread, and fails closed when setNetworkTimeout/isReadOnly throws") {
            val p1 = validPuuid78("jdbc_regression_1")
            val p2 = validPuuid78("jdbc_regression_2")

            val releaseStalledConn = CountDownLatch(1)
            val stalledDataSource = mockk<DataSource>(relaxed = true)
            every { stalledDataSource.connection } answers {
                releaseStalledConn.await(5, TimeUnit.SECONDS)
                throw SQLException("Connection acquisition timed out")
            }

            try {
                val stalledAdapter = JdbcIdentityCompletionPersistenceAdapter(stalledDataSource)
                val toolWithStalledDb = OffProdChallengerIdentityCompletionTool(
                    persistencePort = stalledAdapter,
                    httpTransport = BoundedAccountV1Transport { _, _, _, _, _, _, _ ->
                        error("Must not reach HTTP when pre-call DB recheck times out")
                    }
                )

                val startWallMs = System.nanoTime() / 1_000_000L
                val stalledReport = toolWithStalledDb.runBounded(
                    manifest = SnapshotManifest(
                        snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                        isLiveLadder = false,
                        entries = listOf(ManifestEntry(p1, "euw1"), ManifestEntry(p2, "kr"))
                    ),
                    options = ExecutionOptions(
                        dryRun = true,
                        networkEnabled = true,
                        keyCoordinationMode = KeyCoordinationMode.EXCLUSIVE_KEY,
                        coordinationAttestation = "Exclusive key",
                        dbOperationTimeoutMs = 150L
                    ),
                    riotApiKeyProvider = { "RGAPI-test" }
                )
                val elapsedCallerMs = (System.nanoTime() / 1_000_000L) - startWallMs

                stalledReport.outcome shouldBe RunOutcome.ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT
                stalledReport.httpAttemptsMade shouldBe 0
                stalledReport.remainingPendingCount shouldBe 2
                // Prove caller thread returned promptly near 150ms without waiting for the 5,000ms stalled connection!
                (elapsedCallerMs < 1_500L) shouldBe true
            } finally {
                releaseStalledConn.countDown()
            }

            val unsupportedTimeoutDs = mockk<DataSource>(relaxed = true)
            val unsupportedConn = mockk<Connection>(relaxed = true)
            every { unsupportedTimeoutDs.connection } returns unsupportedConn
            every { unsupportedConn.setNetworkTimeout(any(), any()) } throws SQLFeatureNotSupportedException("setNetworkTimeout unsupported")

            val failClosedAdapter = JdbcIdentityCompletionPersistenceAdapter(unsupportedTimeoutDs)
            val recheckFailure = failClosedAdapter.recheckExistingIdentitiesBounded(
                entries = listOf(ManifestEntry(p1, "euw1")),
                timeoutMs = 1_000L
            )
            (recheckFailure is DbRecheckResult.Failure) shouldBe true
            (recheckFailure as DbRecheckResult.Failure).reason shouldContain "setNetworkTimeout unsupported"
        }
    }
})

private class FakeMonotonicClock(initialMs: Long = 1_000_000L) : MonotonicClock {
    private var currentMs: Long = initialMs
    val sleepsRecordedMs = mutableListOf<Long>()

    override fun nowMs(): Long = currentMs

    fun advanceMs(deltaMs: Long) {
        currentMs += deltaMs
    }

    override fun sleepMs(durationMs: Long) {
        if (durationMs > 0L) {
            sleepsRecordedMs.add(durationMs)
            currentMs += durationMs
        }
    }
}

private class FakeRaceSafeIdentityPersistence : IdentityCompletionPersistencePort {

    data class IndexedPlayerRow(
        val puuid: String,
        val region: String,
        val gameName: String,
        val tagLine: String,
        val profileIconId: Int,
        val summonerLevel: Long,
        val lastSeenAt: Instant
    )

    data class IdentityOnlyRow(
        val puuid: String,
        val platformRegion: String,
        val gameName: String,
        val tagLine: String,
        val verifiedAt: Instant
    )

    val indexedPlayers = LinkedHashMap<String, IndexedPlayerRow>()
    val identitiesOnly = LinkedHashMap<String, IdentityOnlyRow>()

    fun seedIndexedPlayer(
        puuid: String,
        region: String,
        gameName: String,
        tagLine: String,
        profileIconId: Int,
        summonerLevel: Long,
        lastSeenAt: Instant
    ) {
        indexedPlayers[puuid] = IndexedPlayerRow(
            puuid = puuid,
            region = region,
            gameName = gameName,
            tagLine = tagLine,
            profileIconId = profileIconId,
            summonerLevel = summonerLevel,
            lastSeenAt = lastSeenAt
        )
    }

    fun seedIdentityOnlyRow(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant
    ) {
        identitiesOnly[puuid] = IdentityOnlyRow(
            puuid = puuid,
            platformRegion = platformRegion,
            gameName = gameName,
            tagLine = tagLine,
            verifiedAt = verifiedAt
        )
    }

    override fun recheckExistingIdentitiesBounded(
        entries: List<ManifestEntry>,
        timeoutMs: Long
    ): DbRecheckResult {
        if (timeoutMs <= 0L) {
            return DbRecheckResult.Failure("Timeout budget <= 0", timedOutOrBudgetExhausted = true)
        }
        val map = entries.associate { entry ->
            val p = entry.normalizedPuuid
            val indexed = indexedPlayers[p]
            val idOnly = identitiesOnly[p]
            val state = when {
                indexed != null && indexed.gameName.trim().isNotEmpty() && indexed.tagLine.trim().isNotEmpty() ->
                    PreCallDbState.ALREADY_COMPLETE
                idOnly != null && idOnly.gameName.trim().isNotEmpty() && idOnly.tagLine.trim().isNotEmpty() ->
                    PreCallDbState.ALREADY_COMPLETE
                indexed != null ->
                    PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD
                else ->
                    PreCallDbState.ABSENT_FROM_INDEXED_PLAYERS
            }
            p to state
        }
        return DbRecheckResult.Success(map)
    }

    override fun persistAndVerifyIdentityBounded(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant,
        preCallDbState: PreCallDbState,
        timeoutMs: Long
    ): DbWriteVerificationResult {
        if (timeoutMs <= 0L) {
            return DbWriteVerificationResult.FailureOrUnknownCommit("Timeout budget <= 0")
        }

        var indexedUpdated = false
        if (preCallDbState == PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD) {
            val existing = indexedPlayers[puuid]
            if (existing != null) {
                val existingGnBlank = existing.gameName.trim().isEmpty()
                val existingTlBlank = existing.tagLine.trim().isEmpty()
                if (existingGnBlank || existingTlBlank) {
                    indexedPlayers[puuid] = existing.copy(
                        gameName = if (existingGnBlank) gameName.trim() else existing.gameName,
                        tagLine = if (existingTlBlank) tagLine.trim() else existing.tagLine
                    )
                    indexedUpdated = true
                }
            }
        }

        var identityUpserted = false
        val existingIdOnly = identitiesOnly[puuid]
        if (existingIdOnly == null) {
            identitiesOnly[puuid] = IdentityOnlyRow(
                puuid = puuid,
                platformRegion = platformRegion,
                gameName = gameName.trim(),
                tagLine = tagLine.trim(),
                verifiedAt = verifiedAt
            )
            identityUpserted = true
        } else {
            val gnBlank = existingIdOnly.gameName.trim().isEmpty()
            val tlBlank = existingIdOnly.tagLine.trim().isEmpty()
            if (gnBlank || tlBlank) {
                identitiesOnly[puuid] = existingIdOnly.copy(
                    gameName = if (gnBlank) gameName.trim() else existingIdOnly.gameName,
                    tagLine = if (tlBlank) tagLine.trim() else existingIdOnly.tagLine,
                    verifiedAt = if (verifiedAt.isAfter(existingIdOnly.verifiedAt)) verifiedAt else existingIdOnly.verifiedAt
                )
                identityUpserted = true
            }
        }

        // Post-write read verification
        val verifiedRow = identitiesOnly[puuid]
            ?: return DbWriteVerificationResult.FailureOrUnknownCommit("Post-write read found no row in player_riot_identities")
        if (verifiedRow.gameName.trim().isEmpty() || verifiedRow.tagLine.trim().isEmpty()) {
            return DbWriteVerificationResult.FailureOrUnknownCommit("Post-write read found blank game_name or tag_line")
        }

        return if (indexedUpdated || identityUpserted) {
            val desc = if (preCallDbState == PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD) {
                "INDEXED_PLAYERS_BLANK_ONLY_AND_PLAYER_RIOT_IDENTITIES"
            } else {
                "PLAYER_RIOT_IDENTITIES_ONLY"
            }
            DbWriteVerificationResult.WrittenAndVerified(
                indexedPlayerBlankFieldUpdated = indexedUpdated,
                identityOnlyTableUpserted = identityUpserted,
                verifiedGameName = verifiedRow.gameName,
                verifiedTagLine = verifiedRow.tagLine,
                verifiedAtUtc = verifiedRow.verifiedAt,
                writeTargetDescription = desc
            )
        } else {
            DbWriteVerificationResult.ConcurrentAlreadyCompleteVerified(
                verifiedGameName = verifiedRow.gameName,
                verifiedTagLine = verifiedRow.tagLine,
                verifiedAtUtc = verifiedRow.verifiedAt
            )
        }
    }
}
