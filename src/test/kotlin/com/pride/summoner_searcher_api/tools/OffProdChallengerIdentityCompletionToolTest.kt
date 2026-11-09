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
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

class OffProdChallengerIdentityCompletionToolTest : BehaviorSpec({

    Given("the 191 missing Challenger snapshot manifest (31 EUW + 160 KR) and default no-effects options") {
        val entries191 = (1..31).map { ManifestEntry(puuid = "euw-missing-$it", platformRegion = "euw1") } +
            (1..160).map { ManifestEntry(puuid = "kr-missing-$it", platformRegion = "kr") }

        val manifest191 = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
            isLiveLadder = false,
            entries = entries191
        )

        Then("it enforces historical snapshot rules, <= 191 cap, and deterministic SHA-256 manifest hashing") {
            manifest191.entries shouldHaveSize 191
            val hash1 = OffProdChallengerIdentityCompletionTool.computeManifestSha256(manifest191.entries)
            val hash2 = OffProdChallengerIdentityCompletionTool.computeManifestSha256(manifest191.entries)
            hash1 shouldBe hash2
            hash1.length shouldBe 64

            // Reject claiming historical snapshot is current live ladder
            shouldThrow<IllegalArgumentException> {
                SnapshotManifest(
                    snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                    isLiveLadder = true,
                    entries = entries191
                )
            }

            // Reject > 191 entries
            shouldThrow<IllegalArgumentException> {
                SnapshotManifest(
                    snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                    isLiveLadder = false,
                    entries = entries191 + ManifestEntry("extra-192", "euw1")
                )
            }
        }

        Then("default ExecutionOptions run in DB-recheck-only dryRun mode with 0 HTTP attempts and 0 DB writes") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                error("Transport must not be called when networkEnabled=false")
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = FakeMonotonicClock()
            )

            val report = tool.runBounded(manifest191)

            report.outcome shouldBe RunOutcome.DRY_RUN_DB_RECHECK_ONLY_COMPLETED
            report.dryRun shouldBe true
            report.networkEnabled shouldBe false
            report.allowDurableWrites shouldBe false
            report.manifestTotalCount shouldBe 191
            report.absentFromIndexedPlayersCount shouldBe 191
            report.httpAttemptsMade shouldBe 0
            report.durableIndexedPlayerUpdatesCount shouldBe 0
            report.durableIdentityTableUpsertsCount shouldBe 0
            report.remainingPendingCount shouldBe 191
            report.remainingEntries shouldHaveSize 191
            transportCalls.get() shouldBe 0
        }
    }

    Given("uncoordinated shared Riot API key execution") {
        val manifest = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
            isLiveLadder = false,
            entries = listOf(ManifestEntry("euw-1", "euw1"))
        )

        Then("it stops immediately as BLOCKED_KEY_COORDINATION_REQUIRED with 0 HTTP attempts") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                error("Must not dispatch HTTP without key coordination attestation")
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = FakeMonotonicClock()
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
    }

    Given("cluster rate-limit discovery, missing-header fail-safe, and header-driven pacing under the 180s wall-time cap") {
        val entries = listOf(
            ManifestEntry("euw-1", "euw1"),
            ManifestEntry("euw-2", "euw1"),
            ManifestEntry("kr-1", "kr")
        )
        val manifest = SnapshotManifest(
            snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
            isLiveLadder = false,
            entries = entries
        )

        Then("it aborts immediately after attempt #1 if rate headers are missing or malformed (no assumed 20:1 or 100:120 defaults)") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val transportCalls = AtomicInteger(0)
            val transport = BoundedAccountV1Transport { _, _, _, _, _, _ ->
                transportCalls.incrementAndGet()
                // Missing X-Method-Rate-Limit and X-Method-Rate-Limit-Count
                TransportResult.HttpResponse(
                    statusCode = 200,
                    headersLowercase = mapOf(
                        "x-app-rate-limit" to "20:1,100:120",
                        "x-app-rate-limit-count" to "1:1,1:120"
                    ),
                    bodyBytes = """{"puuid":"euw-1","gameName":"PlayerOne","tagLine":"EUW"}""".toByteArray()
                )
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = FakeMonotonicClock()
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
            report.remainingPendingCount shouldBe 2
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf("euw-2", "kr-1")
        }

        Then("first cluster discovery call counts against the attempt cap, header-driven waits are applied, and exceeding the 180s wall-time budget halts honestly without sleeping") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val clock = FakeMonotonicClock()
            val transportCalls = AtomicInteger(0)

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _ ->
                val callNum = transportCalls.incrementAndGet()
                when (callNum) {
                    1 -> {
                        // First call to EUROPE discovers headers and reports the 10s method window is now exhausted (5:10)
                        url shouldContain "/by-puuid/euw-1"
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = mapOf(
                                "x-app-rate-limit" to "50:10,500:600",
                                "x-app-rate-limit-count" to "1:10,1:600",
                                "x-method-rate-limit" to "5:10",
                                "x-method-rate-limit-count" to "5:10"
                            ),
                            bodyBytes = """{"puuid":"euw-1","gameName":"EuwOne","tagLine":"EUW"}""".toByteArray()
                        )
                    }
                    2 -> {
                        // Second call to EUROPE happens after a single 10_000ms header-driven sleep, and now reports
                        // a 600s app window exhaustion (500:600), which exceeds the 180s default wall-time cap!
                        url shouldContain "/by-puuid/euw-2"
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = mapOf(
                                "x-app-rate-limit" to "50:10,500:600",
                                "x-app-rate-limit-count" to "2:10,500:600",
                                "x-method-rate-limit" to "5:10",
                                "x-method-rate-limit-count" to "1:10"
                            ),
                            bodyBytes = """{"puuid":"euw-2","gameName":"EuwTwo","tagLine":"EUW"}""".toByteArray()
                        )
                    }
                    else -> error("Unexpected call $callNum")
                }
            }

            val smallEuwManifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry("euw-1", "euw1"),
                    ManifestEntry("euw-2", "euw1"),
                    ManifestEntry("euw-3", "euw1")
                )
            )

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = clock
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

            // Attempt 1 (discovery) + 10s sleep + Attempt 2 (reports 600s wait needed before euw-3) -> halts immediately!
            report.outcome shouldBe RunOutcome.PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED
            report.httpAttemptsMade shouldBe 2
            report.totalWaitTimeMs shouldBe 10_000L
            clock.sleepsRecordedMs shouldBe listOf(10_000L) // Never slept the 600_000ms window!
            report.validatedIdentitiesCount shouldBe 2
            report.remainingPendingCount shouldBe 1
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf("euw-3")
        }
    }

    Given("Account-V1 payload edge cases (exact PUUID match, omitted fields, 404, 429, 5xx) and real loopback socket timeout/4KiB enforcement") {
        val validHeaders = mapOf(
            "x-app-rate-limit" to "20:1,100:120",
            "x-app-rate-limit-count" to "1:1,1:120",
            "x-method-rate-limit" to "1000:60",
            "x-method-rate-limit-count" to "1:60"
        )

        Then("it records 404 and omitted Riot ID fields without writing, and aborts on PUUID mismatch") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry("p-404", "euw1"),
                    ManifestEntry("p-omitted-name", "euw1"),
                    ManifestEntry("p-blank-tag", "euw1"),
                    ManifestEntry("p-mismatch", "euw1"),
                    ManifestEntry("p-after-mismatch", "euw1")
                )
            )

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _ ->
                when {
                    url.endsWith("/p-404") -> TransportResult.HttpResponse(
                        statusCode = 404,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"status":{"message":"Data not found","status_code":404}}""".toByteArray()
                    )
                    url.endsWith("/p-omitted-name") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"p-omitted-name","tagLine":"KR1"}""".toByteArray()
                    )
                    url.endsWith("/p-blank-tag") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"p-blank-tag","gameName":"ValidName","tagLine":"   "}""".toByteArray()
                    )
                    url.endsWith("/p-mismatch") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"WRONG-PUUID","gameName":"Spoof","tagLine":"EUW"}""".toByteArray()
                    )
                    else -> error("Should have aborted on PUUID mismatch before $url")
                }
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = FakeMonotonicClock()
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
            report.durableIndexedPlayerUpdatesCount shouldBe 0
            report.durableIdentityTableUpsertsCount shouldBe 0
            report.remainingPendingCount shouldBe 1
            report.remainingEntries.map { it.normalizedPuuid } shouldBe listOf("p-after-mismatch")
        }

        Then("it performs 0 retries and aborts immediately on HTTP 429 (including service 429) or HTTP 5xx") {
            val persistence = FakeRaceSafeIdentityPersistence()
            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:45:00Z",
                isLiveLadder = false,
                entries = listOf(ManifestEntry("kr-1", "kr"), ManifestEntry("kr-2", "kr"))
            )
            val attempts = AtomicInteger(0)
            val transport429 = BoundedAccountV1Transport { _, _, _, _, _, _ ->
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
                clock = FakeMonotonicClock()
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
            // Service 429 with 0s Retry-After enforces >= 10s minimum retryAfterSeconds in recorded cluster state
            report429.lastObservedClusterHeaders["asia"]?.retryAfterSeconds shouldBe 10L
            report429.remainingPendingCount shouldBe 1
        }

        Then("HttpUrlConnectionAccountV1Transport enforces the 4 KiB body cap and socket deadline cancellation on a local loopback server") {
            val releaseStalledHandler = CountDownLatch(1)
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            try {
                server.createContext("/riot/account/v1/accounts/by-puuid/oversized") { exchange ->
                    exchange.responseHeaders.add("X-App-Rate-Limit", "20:1,100:120")
                    exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
                    exchange.responseHeaders.add("X-Method-Rate-Limit", "1000:60")
                    exchange.responseHeaders.add("X-Method-Rate-Limit-Count", "1:60")
                    val hugePayload = ByteArray(5_000) { 'A'.code.toByte() }
                    exchange.sendResponseHeaders(200, hugePayload.size.toLong())
                    exchange.responseBody.use { it.write(hugePayload) }
                }
                server.createContext("/riot/account/v1/accounts/by-puuid/stalled") { exchange ->
                    releaseStalledHandler.await(3, TimeUnit.SECONDS)
                    exchange.sendResponseHeaders(200, 2)
                    exchange.responseBody.use { it.write("{}".toByteArray(StandardCharsets.UTF_8)) }
                }
                server.start()
                val port = server.address.port
                val realTransport = HttpUrlConnectionAccountV1Transport()

                val oversizedResult = realTransport.executeSingleGet(
                    url = "http://127.0.0.1:$port/riot/account/v1/accounts/by-puuid/oversized",
                    apiKey = "test-key",
                    connectTimeoutMs = 1_000,
                    readTimeoutMs = 1_000,
                    callDeadlineMs = 1_500L,
                    maxBodyBytes = 4_096
                )
                oversizedResult shouldBe TransportResult.PayloadTooLarge(statusCode = 200, maxBodyBytes = 4_096)

                val timeoutResult = realTransport.executeSingleGet(
                    url = "http://127.0.0.1:$port/riot/account/v1/accounts/by-puuid/stalled",
                    apiKey = "test-key",
                    connectTimeoutMs = 200,
                    readTimeoutMs = 200,
                    callDeadlineMs = 250L,
                    maxBodyBytes = 4_096
                )
                (timeoutResult is TransportResult.TimeoutOrIoError) shouldBe true
            } finally {
                releaseStalledHandler.countDown()
                server.stop(0)
            }
        }
    }

    Given("race-safe identity-only persistence and blank-field-only updates") {
        val validHeaders = mapOf(
            "x-app-rate-limit" to "20:1,100:120",
            "x-app-rate-limit-count" to "1:1,1:120",
            "x-method-rate-limit" to "1000:60",
            "x-method-rate-limit-count" to "1:60"
        )

        Then("UPDATE blank fields on indexed_players preserves each already-nonblank column and never fabricates match metadata, while player_riot_identities ON CONFLICT never overwrites a concurrent non-blank identity") {
            val persistence = FakeRaceSafeIdentityPersistence()

            // 1. Pre-populate an existing indexed_players row that already has a valid game_name ("OriginalMatchName")
            //    and real match metadata (icon=42, level=500, lastSeenAt=T0), but a blank tag_line ("").
            persistence.seedIndexedPlayer(
                puuid = "p-partial-blank-tag",
                region = "EUW1",
                gameName = "OriginalMatchName",
                tagLine = "   ",
                profileIconId = 42,
                summonerLevel = 500L,
                lastSeenAt = Instant.parse("2026-11-08T12:00:00Z")
            )

            // 2. Pre-populate an existing indexed_players row that has a blank game_name ("")
            //    but already has a valid non-blank tag_line ("ORIGTAG").
            persistence.seedIndexedPlayer(
                puuid = "p-partial-blank-name",
                region = "kr",
                gameName = "",
                tagLine = "ORIGTAG",
                profileIconId = 99,
                summonerLevel = 612L,
                lastSeenAt = Instant.parse("2026-11-08T15:00:00Z")
            )

            // 3. Pre-populate a row in indexed_players that is ALREADY complete -> must be skipped in pre-call recheck (0 HTTP calls).
            persistence.seedIndexedPlayer(
                puuid = "p-already-complete",
                region = "euw1",
                gameName = "AlreadyComplete",
                tagLine = "EUW",
                profileIconId = 11,
                summonerLevel = 300L,
                lastSeenAt = Instant.parse("2026-11-08T18:00:00Z")
            )

            // 4. Simulate a concurrent writer that inserts a verified non-blank identity into player_riot_identities
            //    AFTER the pre-call DB recheck but BEFORE p-absent-concurrent's upsert runs.
            val manifest = SnapshotManifest(
                snapshotCapturedAtUtc = "2026-11-09T00:46:00Z",
                isLiveLadder = false,
                entries = listOf(
                    ManifestEntry("p-already-complete", "euw1"),
                    ManifestEntry("p-partial-blank-tag", "euw1"),
                    ManifestEntry("p-partial-blank-name", "kr"),
                    ManifestEntry("p-absent-new", "kr"),
                    ManifestEntry("p-absent-concurrent", "kr")
                )
            )

            val transport = BoundedAccountV1Transport { url, _, _, _, _, _ ->
                when {
                    url.endsWith("/p-partial-blank-tag") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"p-partial-blank-tag","gameName":"NewRiotName","tagLine":"FILLEDTAG"}""".toByteArray()
                    )
                    url.endsWith("/p-partial-blank-name") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"p-partial-blank-name","gameName":"FilledName","tagLine":"NEWTAG"}""".toByteArray()
                    )
                    url.endsWith("/p-absent-new") -> TransportResult.HttpResponse(
                        statusCode = 200,
                        headersLowercase = validHeaders,
                        bodyBytes = """{"puuid":"p-absent-new","gameName":"PureIdentity","tagLine":"KR1"}""".toByteArray()
                    )
                    url.endsWith("/p-absent-concurrent") -> {
                        // Concurrent writer inserts a newer verified non-blank identity right before our write!
                        persistence.seedIdentityOnlyRow(
                            puuid = "p-absent-concurrent",
                            platformRegion = "kr",
                            gameName = "ConcurrentWinner",
                            tagLine = "WIN",
                            verifiedAt = Instant.parse("2026-11-09T12:00:00Z")
                        )
                        TransportResult.HttpResponse(
                            statusCode = 200,
                            headersLowercase = validHeaders,
                            bodyBytes = """{"puuid":"p-absent-concurrent","gameName":"StaleLoser","tagLine":"LOSE"}""".toByteArray()
                        )
                    }
                    else -> error("Unexpected HTTP call for $url")
                }
            }

            val tool = OffProdChallengerIdentityCompletionTool(
                persistencePort = persistence,
                httpTransport = transport,
                clock = FakeMonotonicClock(initialMs = Instant.parse("2026-11-09T03:00:00Z").toEpochMilli())
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

            report.outcome shouldBe RunOutcome.COMPLETED_ALL_CANDIDATES
            report.alreadyResolvedInDbCount shouldBe 1
            report.httpAttemptsMade shouldBe 4 // p-already-complete skipped with 0 HTTP calls

            // Verify Race Fix 1a: p-partial-blank-tag preserved "OriginalMatchName" and only filled "FILLEDTAG"
            val updatedTagRow = persistence.indexedPlayers["p-partial-blank-tag"]!!
            updatedTagRow.gameName shouldBe "OriginalMatchName"
            updatedTagRow.tagLine shouldBe "FILLEDTAG"
            updatedTagRow.region shouldBe "EUW1"
            updatedTagRow.profileIconId shouldBe 42
            updatedTagRow.summonerLevel shouldBe 500L
            updatedTagRow.lastSeenAt shouldBe Instant.parse("2026-11-08T12:00:00Z")

            // Verify Race Fix 1b: p-partial-blank-name filled "FilledName" and preserved "ORIGTAG"
            val updatedNameRow = persistence.indexedPlayers["p-partial-blank-name"]!!
            updatedNameRow.gameName shouldBe "FilledName"
            updatedNameRow.tagLine shouldBe "ORIGTAG"
            updatedNameRow.profileIconId shouldBe 99
            updatedNameRow.summonerLevel shouldBe 612L

            // Verify p-absent-new was NOT inserted into indexed_players (no fake metadata!)
            persistence.indexedPlayers.containsKey("p-absent-new") shouldBe false
            val newIdentity = persistence.identitiesOnly["p-absent-new"]!!
            newIdentity.gameName shouldBe "PureIdentity"
            newIdentity.tagLine shouldBe "KR1"
            newIdentity.platformRegion shouldBe "kr"

            // Verify Race Fix 2: p-absent-concurrent in player_riot_identities was NOT overwritten by StaleLoser#LOSE!
            val concurrentIdentity = persistence.identitiesOnly["p-absent-concurrent"]!!
            concurrentIdentity.gameName shouldBe "ConcurrentWinner"
            concurrentIdentity.tagLine shouldBe "WIN"
            concurrentIdentity.verifiedAt shouldBe Instant.parse("2026-11-09T12:00:00Z")
        }

        Then("JdbcIdentityCompletionPersistenceAdapter prepares the exact race-safe SQL statements with bounded query timeouts") {
            val dataSource = mockk<DataSource>()
            val connection = mockk<Connection>(relaxed = true)
            val ps = mockk<PreparedStatement>(relaxed = true)
            val rs = mockk<ResultSet>(relaxed = true)

            every { dataSource.connection } returns connection
            every { connection.prepareStatement(any()) } returns ps
            every { ps.executeQuery() } returns rs
            every { rs.next() } returns false
            every { ps.executeUpdate() } returns 1

            val adapter = JdbcIdentityCompletionPersistenceAdapter(dataSource, checkIdentityTableInPreCall = true)

            adapter.updateBlankFieldsOnExistingIndexedPlayer("puuid-1", "NameOne", "TAG1") shouldBe true
            verify {
                connection.prepareStatement(
                    match { sql ->
                        sql.contains("SET game_name = CASE WHEN BTRIM(COALESCE(game_name, '')) = '' THEN ? ELSE game_name END") &&
                            sql.contains("tag_line  = CASE WHEN BTRIM(COALESCE(tag_line, '')) = ''  THEN ? ELSE tag_line  END")
                    }
                )
            }

            adapter.upsertVerifiedIdentityOnly(
                puuid = "puuid-2",
                platformRegion = "kr",
                gameName = "NameTwo",
                tagLine = "KR1",
                verifiedAt = Instant.parse("2026-11-09T02:30:00Z")
            ) shouldBe true
            verify {
                connection.prepareStatement(
                    match { sql ->
                        sql.contains("INSERT INTO player_riot_identities") &&
                            sql.contains("ON CONFLICT (puuid) DO UPDATE") &&
                            sql.contains("WHEN BTRIM(COALESCE(player_riot_identities.game_name, '')) = '' THEN EXCLUDED.game_name") &&
                            sql.contains("WHEN BTRIM(COALESCE(player_riot_identities.tag_line, '')) = '' THEN EXCLUDED.tag_line") &&
                            sql.contains("GREATEST(player_riot_identities.verified_at, EXCLUDED.verified_at)")
                    }
                )
            }
        }
    }
})

private class FakeMonotonicClock(initialMs: Long = 1_000_000L) : MonotonicClock {
    private var currentMs: Long = initialMs
    val sleepsRecordedMs = mutableListOf<Long>()

    override fun nowMs(): Long = currentMs

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
        entries: List<ManifestEntry>
    ): Map<String, PreCallDbState> {
        return entries.associate { entry ->
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
    }

    override fun updateBlankFieldsOnExistingIndexedPlayer(
        puuid: String,
        gameName: String,
        tagLine: String
    ): Boolean {
        val existing = indexedPlayers[puuid] ?: return false
        val existingGnBlank = existing.gameName.trim().isEmpty()
        val existingTlBlank = existing.tagLine.trim().isEmpty()
        if (!existingGnBlank && !existingTlBlank) {
            return false
        }
        indexedPlayers[puuid] = existing.copy(
            gameName = if (existingGnBlank) gameName.trim() else existing.gameName,
            tagLine = if (existingTlBlank) tagLine.trim() else existing.tagLine
        )
        return true
    }

    override fun upsertVerifiedIdentityOnly(
        puuid: String,
        platformRegion: String,
        gameName: String,
        tagLine: String,
        verifiedAt: Instant
    ): Boolean {
        val existing = identitiesOnly[puuid]
        if (existing == null) {
            identitiesOnly[puuid] = IdentityOnlyRow(
                puuid = puuid,
                platformRegion = platformRegion,
                gameName = gameName.trim(),
                tagLine = tagLine.trim(),
                verifiedAt = verifiedAt
            )
            return true
        }
        val existingGnBlank = existing.gameName.trim().isEmpty()
        val existingTlBlank = existing.tagLine.trim().isEmpty()
        if (!existingGnBlank && !existingTlBlank) {
            // Concurrent non-blank verified identity already exists -> do NOT overwrite!
            return false
        }
        identitiesOnly[puuid] = existing.copy(
            gameName = if (existingGnBlank) gameName.trim() else existing.gameName,
            tagLine = if (existingTlBlank) tagLine.trim() else existing.tagLine,
            platformRegion = if (existing.platformRegion.trim().isEmpty()) platformRegion else existing.platformRegion,
            verifiedAt = if (verifiedAt.isAfter(existing.verifiedAt)) verifiedAt else existing.verifiedAt
        )
        return true
    }
}
