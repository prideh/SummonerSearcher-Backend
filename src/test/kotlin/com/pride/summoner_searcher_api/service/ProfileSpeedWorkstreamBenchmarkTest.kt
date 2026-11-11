package com.pride.summoner_searcher_api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pride.summoner_searcher_api.dto.AccountDto
import com.pride.summoner_searcher_api.dto.LeagueEntryDto
import com.pride.summoner_searcher_api.dto.MatchDto
import com.pride.summoner_searcher_api.dto.MatchInfo
import com.pride.summoner_searcher_api.dto.MatchMetadata
import com.pride.summoner_searcher_api.dto.ParticipantDto
import com.pride.summoner_searcher_api.dto.SummonerDto
import com.pride.summoner_searcher_api.dto.SummonerProfileDto
import com.pride.summoner_searcher_api.model.PlayerSeasonStatsEntity
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import com.pride.summoner_searcher_api.repository.PlayerSeasonStatsRepository
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.comparables.shouldBeLessThanOrEqualTo
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.RedisStringCommands
import org.springframework.data.redis.core.RedisCallback
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.data.redis.core.types.Expiration
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.ClientHttpResponse
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.AbstractPlatformTransactionManager
import org.springframework.transaction.support.DefaultTransactionStatus
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.web.client.RestClient
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class ProfileSpeedWorkstreamBenchmarkTest : FunSpec({

    val mapper: ObjectMapper = ObjectMapper().findAndRegisterModules()

    class MutableClock(initialMs: Long) : TimeProvider {
        private val current = AtomicLong(initialMs)
        override fun now(): Long = current.get()
        fun advanceBy(deltaMs: Long) {
            current.addAndGet(deltaMs)
        }
        fun set(ms: Long) {
            current.set(ms)
        }
    }

    class InMemoryLatencyRedisHarness(
        val mapper: ObjectMapper,
        val networkRttMs: Long = 0L,
        val enablePipelineSupport: Boolean = true
    ) {
        val store = ConcurrentHashMap<String, String>()
        val getCalls = AtomicInteger(0)
        val multiGetCalls = AtomicInteger(0)
        val singleSetCalls = AtomicInteger(0)
        val pipelineBatchCalls = AtomicInteger(0)
        val deleteCalls = AtomicInteger(0)
        val setIfAbsentCalls = AtomicInteger(0)

        fun resetCounters() {
            getCalls.set(0)
            multiGetCalls.set(0)
            singleSetCalls.set(0)
            pipelineBatchCalls.set(0)
            deleteCalls.set(0)
            setIfAbsentCalls.set(0)
        }

        fun totalNetworkRoundTrips(): Int =
            getCalls.get() + multiGetCalls.get() + singleSetCalls.get() + pipelineBatchCalls.get() +
                deleteCalls.get() + setIfAbsentCalls.get()

        private fun simulateRtt() {
            if (networkRttMs > 0L) {
                Thread.sleep(networkRttMs)
            }
        }

        val service: RedisCacheService by lazy {
            val valueOps = mockk<ValueOperations<String, String>>()
            every { valueOps.get(any()) } answers {
                getCalls.incrementAndGet()
                simulateRtt()
                store[firstArg()]
            }
            every { valueOps.multiGet(any()) } answers {
                multiGetCalls.incrementAndGet()
                simulateRtt()
                val keys = firstArg<Collection<String>>()
                keys.map { store[it] }
            }
            every { valueOps.set(any(), any()) } answers {
                singleSetCalls.incrementAndGet()
                simulateRtt()
                store[firstArg()] = secondArg()
            }
            every { valueOps.set(any(), any(), any<Duration>()) } answers {
                singleSetCalls.incrementAndGet()
                simulateRtt()
                store[firstArg()] = secondArg()
            }
            every { valueOps.setIfAbsent(any(), any(), any<Duration>()) } answers {
                setIfAbsentCalls.incrementAndGet()
                simulateRtt()
                val k = firstArg<String>()
                val v = secondArg<String>()
                store.putIfAbsent(k, v) == null
            }

            val stringCommands = mockk<RedisStringCommands>()
            every {
                stringCommands.set(
                    any<ByteArray>(),
                    any<ByteArray>(),
                    any<Expiration>(),
                    any<RedisStringCommands.SetOption>()
                )
            } answers {
                val key = String(firstArg<ByteArray>(), StandardCharsets.UTF_8)
                val value = String(secondArg<ByteArray>(), StandardCharsets.UTF_8)
                store[key] = value
                true
            }
            val redisConn = mockk<RedisConnection>()
            every { redisConn.stringCommands() } returns stringCommands
            val connFactory = mockk<RedisConnectionFactory>()

            val template = mockk<StringRedisTemplate>()
            every { template.opsForValue() } returns valueOps
            every { template.delete(any<String>()) } answers {
                deleteCalls.incrementAndGet()
                simulateRtt()
                store.remove(firstArg<String>()) != null
            }
            if (enablePipelineSupport) {
                every { template.connectionFactory } returns connFactory
                every { template.executePipelined(any<RedisCallback<*>>()) } answers {
                    pipelineBatchCalls.incrementAndGet()
                    simulateRtt()
                    val callback = firstArg<RedisCallback<Any?>>()
                    callback.doInRedis(redisConn)
                    emptyList<Any>()
                }
            } else {
                every { template.connectionFactory } returns null
            }

            RedisCacheService(template, mapper)
        }
    }

    class FakeRiotHttpBackend(
        val mapper: ObjectMapper,
        var httpRttMs: Long = 0L,
        var appRateLimitHeader: String = "500:1,6000:120"
    ) {
        val accountsByRiotId = ConcurrentHashMap<String, AccountDto>()
        val summonersByRegionPuuid = ConcurrentHashMap<String, SummonerDto>()
        val ranksByRegionPuuid = ConcurrentHashMap<String, List<LeagueEntryDto>>()
        val matchIdsPagesGenerator = ConcurrentHashMap<String, (Int, Int) -> List<String>?>()
        val matchesById = ConcurrentHashMap<String, MatchDto>()

        val totalHttpCalls = AtomicInteger(0)
        val accountByRiotIdCalls = AtomicInteger(0)
        val summonerByPuuidCalls = AtomicInteger(0)
        val leagueByPuuidCalls = AtomicInteger(0)
        val matchIdsPageCalls = AtomicInteger(0)
        val matchByIdCalls = AtomicInteger(0)
        val currentInFlightMatchCalls = AtomicInteger(0)
        val peakConcurrentMatchCalls = AtomicInteger(0)
        var onBeforeEndpoint: ((String, URI) -> Unit)? = null

        fun resetCounters() {
            totalHttpCalls.set(0)
            accountByRiotIdCalls.set(0)
            summonerByPuuidCalls.set(0)
            leagueByPuuidCalls.set(0)
            matchIdsPageCalls.set(0)
            matchByIdCalls.set(0)
            currentInFlightMatchCalls.set(0)
            peakConcurrentMatchCalls.set(0)
        }

        fun buildRestClient(): RestClient {
            val factory = ClientHttpRequestFactory { uri, httpMethod ->
                object : ClientHttpRequest {
                    private val reqHeaders = HttpHeaders()
                    private val reqBody = ByteArrayOutputStream()
                    override fun getMethod(): HttpMethod = httpMethod
                    override fun getURI(): URI = uri
                    override fun getAttributes(): MutableMap<String, Any> = HashMap()
                    override fun getHeaders(): HttpHeaders = reqHeaders
                    override fun getBody(): OutputStream = reqBody
                    override fun execute(): ClientHttpResponse {
                        totalHttpCalls.incrementAndGet()
                        val path = uri.path
                        val hostRegion = uri.host.substringBefore(".api.riotgames.com").lowercase()
                        val endpointTag = when {
                            path.contains("/riot/account/v1/accounts/by-riot-id/") -> "account-v1"
                            path.contains("/lol/summoner/v4/summoners/by-puuid/") -> "summoner-v4"
                            path.contains("/lol/league/v4/entries/by-puuid/") -> "league-v4"
                            path.contains("/lol/match/v5/matches/by-puuid/") && path.endsWith("/ids") -> "match-v5-ids"
                            path.contains("/lol/match/v5/matches/") -> "match-v5-match"
                            else -> "other"
                        }
                        onBeforeEndpoint?.invoke(endpointTag, uri)

                        var inMatch = false
                        if (endpointTag == "match-v5-match") {
                            inMatch = true
                            val cur = currentInFlightMatchCalls.incrementAndGet()
                            peakConcurrentMatchCalls.accumulateAndGet(cur, ::maxOf)
                        }
                        try {
                            if (httpRttMs > 0L) {
                                Thread.sleep(httpRttMs)
                            }
                            val (status, jsonBytes) = route(hostRegion, uri)
                            val respHeaders = HttpHeaders().apply {
                                contentType = MediaType.APPLICATION_JSON
                                set("X-App-Rate-Limit", appRateLimitHeader)
                                set("X-App-Rate-Limit-Count", "1:1,1:120")
                            }
                            return object : ClientHttpResponse {
                                override fun getStatusCode(): HttpStatus = status
                                override fun getStatusText(): String = status.reasonPhrase
                                override fun close() {}
                                override fun getBody(): InputStream = ByteArrayInputStream(jsonBytes)
                                override fun getHeaders(): HttpHeaders = respHeaders
                            }
                        } finally {
                            if (inMatch) {
                                currentInFlightMatchCalls.decrementAndGet()
                            }
                        }
                    }
                }
            }
            return RestClient.builder().requestFactory(factory).build()
        }

        private fun route(hostRegion: String, uri: URI): Pair<HttpStatus, ByteArray> {
            val path = uri.path
            return when {
                path.contains("/riot/account/v1/accounts/by-riot-id/") -> {
                    accountByRiotIdCalls.incrementAndGet()
                    val tail = path.substringAfter("/riot/account/v1/accounts/by-riot-id/")
                    val parts = tail.split("/")
                    val gameName = java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8)
                    val tagLine = java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    val dto = accountsByRiotId["${hostRegion}|${gameName.lowercase()}|${tagLine.lowercase()}"]
                    if (dto == null) HttpStatus.NOT_FOUND to ByteArray(0)
                    else HttpStatus.OK to mapper.writeValueAsBytes(dto)
                }
                path.contains("/lol/summoner/v4/summoners/by-puuid/") -> {
                    summonerByPuuidCalls.incrementAndGet()
                    val puuid = path.substringAfterLast("/")
                    val dto = summonersByRegionPuuid["$hostRegion|$puuid"]
                    if (dto == null) HttpStatus.NOT_FOUND to ByteArray(0)
                    else HttpStatus.OK to mapper.writeValueAsBytes(dto)
                }
                path.contains("/lol/league/v4/entries/by-puuid/") -> {
                    leagueByPuuidCalls.incrementAndGet()
                    val puuid = path.substringAfterLast("/")
                    val entries = ranksByRegionPuuid["$hostRegion|$puuid"] ?: emptyList()
                    HttpStatus.OK to mapper.writeValueAsBytes(entries)
                }
                path.contains("/lol/match/v5/matches/by-puuid/") && path.endsWith("/ids") -> {
                    matchIdsPageCalls.incrementAndGet()
                    val puuid = path.substringAfter("/lol/match/v5/matches/by-puuid/").substringBefore("/ids")
                    val params = (uri.query ?: "").split("&").mapNotNull {
                        val kv = it.split("=", limit = 2)
                        if (kv.size == 2) kv[0] to kv[1] else null
                    }.toMap()
                    val start = params["start"]?.toIntOrNull() ?: 0
                    val count = params["count"]?.toIntOrNull() ?: 100
                    val gen = matchIdsPagesGenerator[puuid]
                    val page = gen?.invoke(start, count)
                    if (page == null) HttpStatus.INTERNAL_SERVER_ERROR to ByteArray(0)
                    else HttpStatus.OK to mapper.writeValueAsBytes(page)
                }
                path.contains("/lol/match/v5/matches/") -> {
                    matchByIdCalls.incrementAndGet()
                    val matchId = path.substringAfterLast("/")
                    val dto = matchesById[matchId]
                    if (dto == null) HttpStatus.NOT_FOUND to ByteArray(0)
                    else HttpStatus.OK to mapper.writeValueAsBytes(dto)
                }
                else -> HttpStatus.NOT_FOUND to ByteArray(0)
            }
        }
    }

    fun noOpTransactionTemplate(): TransactionTemplate {
        val txManager = object : AbstractPlatformTransactionManager() {
            override fun doGetTransaction(): Any = Any()
            override fun doBegin(transaction: Any, definition: TransactionDefinition) {}
            override fun doCommit(status: DefaultTransactionStatus) {}
            override fun doRollback(status: DefaultTransactionStatus) {}
        }
        return TransactionTemplate(txManager)
    }

    fun inMemorySeasonRepo(): Pair<PlayerSeasonStatsRepository, ConcurrentHashMap<String, PlayerSeasonStatsEntity>> {
        val map = ConcurrentHashMap<String, PlayerSeasonStatsEntity>()
        val repo = mockk<PlayerSeasonStatsRepository>()
        every { repo.findByRegionAndPuuidAndSeasonYearAndQueueId(any(), any(), any(), any()) } answers {
            val r = firstArg<String>().lowercase().trim()
            val p = secondArg<String>()
            val y = thirdArg<Int>()
            val q = arg<Int>(3)
            map["$r|$p|$y|$q"]
        }
        every { repo.saveAndFlush(any()) } answers {
            val e = firstArg<PlayerSeasonStatsEntity>()
            val copy = PlayerSeasonStatsEntity(
                id = e.id ?: UUID.randomUUID(),
                region = e.region,
                puuid = e.puuid,
                seasonYear = e.seasonYear,
                queueId = e.queueId,
                schemaVersion = e.schemaVersion,
                foldedGamesCount = e.foldedGamesCount,
                pendingGamesCount = e.pendingGamesCount,
                historyCapped = e.historyCapped,
                historyComplete = e.historyComplete,
                stateJson = e.stateJson,
                updatedAt = e.updatedAt,
                version = e.version + 1L
            )
            map["${e.region.lowercase().trim()}|${e.puuid}|${e.seasonYear}|${e.queueId}"] = copy
            copy
        }
        return repo to map
    }

    val champions = listOf("Orianna", "Ahri", "Syndra", "Viktor", "Azir", "Taliyah", "Hwei", "LeBlanc")
    val patches2026 = (1..22).map { "16.$it.1" }

    fun buildSeasonalMatch(puuid: String, idx: Int, baseNowMs: Long, prefix: String = "EUW1"): MatchDto {
        val champ = champions[idx % champions.size]
        val patch = patches2026[(idx - 1) % patches2026.size]
        val win = idx % 2 == 0
        val participant = mapper.readValue(
            """
            {
              "puuid": "$puuid",
              "riotIdGameName": "SpeedPlayer",
              "riotIdTagline": "EUW",
              "championName": "$champ",
              "teamId": ${if (idx % 2 == 0) 100 else 200},
              "teamPosition": "MIDDLE",
              "win": $win,
              "kills": ${4 + (idx % 9)},
              "deaths": ${1 + (idx % 5)},
              "assists": ${5 + (idx % 10)},
              "totalMinionsKilled": ${185 + (idx % 45)},
              "neutralMinionsKilled": ${6 + (idx % 8)},
              "visionScore": ${18 + (idx % 16)}
            }
            """.trimIndent(),
            ParticipantDto::class.java
        )
        // Spread games across full 2026 YTD (Jan..Nov 2026) while keeping newest idx with highest timestamp
        val jan22026Ms = Instant.parse("2026-01-02T00:00:00Z").toEpochMilli()
        val spanMs = (baseNowMs - 3_600_000L) - jan22026Ms
        val creationMs = jan22026Ms + ((idx.toLong() * spanMs) / 2_500L)
        return MatchDto(
            metadata = MatchMetadata("${prefix}_$idx", listOf(puuid)),
            info = MatchInfo(
                gameCreation = creationMs,
                gameDuration = 1620L + (idx % 240),
                gameId = idx.toLong(),
                queueId = 420,
                gameMode = "CLASSIC",
                gameVersion = patch,
                participants = listOf(participant),
                teams = emptyList()
            )
        )
    }

    /**
     * Exact baseline (`543854b`) reference harness running against identical FakeRiotHttpBackend + InMemoryLatencyRedisHarness
     * so before/after wall-clock latency and Riot/Redis round-trip counts are measured under identical synthetic RTT.
     */
    suspend fun executeBaseline543854bGetSummonerProfile(
        region: String,
        summonerName: String,
        tagLine: String,
        explicitRefresh: Boolean,
        riotApiService: RiotApiService,
        redis: InMemoryLatencyRedisHarness,
        seasonRepo: PlayerSeasonStatsRepository,
        txTemplate: TransactionTemplate,
        clock: MutableClock
    ): SummonerProfileDto? {
        // 543854b Step 1: Unconditional getAccountByRiotId on every profile request
        val accountDto = riotApiService.getAccountByRiotId(summonerName, tagLine, region)
        val puuid = accountDto?.puuid?.takeIf { it.isNotBlank() } ?: return null

        // 543854b Step 2: Baseline PlayerCacheService without Riot-ID index, without pipelined writes,
        // without concurrent Summoner-V4 overlap, and without anchored pagination
        val baselineCache = object : RedisCacheService(mockk(relaxed = true), mapper) {
            override fun <T> get(key: String, type: Class<T>): T? = redis.service.get(key, type)
            override fun <T> multiGet(keys: List<String>, type: Class<T>): Map<String, T?> = redis.service.multiGet(keys, type)
            override fun <T> set(key: String, value: T, expiration: Duration) = redis.service.set(key, value, expiration)
            override fun canPipelineBatchWrites(): Boolean = false // 543854b wrote every match sequentially via set()
            override fun setIfAbsent(key: String, value: String, expiration: Duration): Boolean =
                redis.service.setIfAbsent(key, value, expiration)
            override fun delete(key: String) = redis.service.delete(key)
        }
        // Strip MatchIdsIncrementalAnchorElement and run sequential getSummonerByPuuid + 50-way match burst as on 543854b
        val indexedRepo = mockk<IndexedPlayerRepository>(relaxed = true)
        val cacheKey = "player:profile:${region.lowercase()}:$puuid"
        val cachedProfile = baselineCache.get(cacheKey, SummonerProfileDto::class.java)
        val now = Instant.ofEpochMilli(clock.now())
        val helper = PlayerCacheService(
            redisCacheService = baselineCache,
            riotApiService = riotApiService,
            indexedPlayerRepository = indexedRepo,
            timeProvider = clock,
            playerSeasonStatsRepository = seasonRepo,
            transactionTemplate = txTemplate,
            objectMapper = mapper
        )
        if (helper.isProfileReadyForImmediateServe(cachedProfile)) {
            if (explicitRefresh) {
                if (helper.isProfileWithinFreshnessWindow(cachedProfile, now, PlayerCacheService.EXPLICIT_REFRESH_MIN_COOLDOWN)) {
                    return cachedProfile?.copy(recentMatches = cachedProfile.recentMatches?.take(20))
                }
            } else if (helper.isProfileWithinFreshnessWindow(cachedProfile, now, PlayerCacheService.PROFILE_FRESHNESS_WINDOW)) {
                return cachedProfile?.copy(recentMatches = cachedProfile.recentMatches?.take(20))
            } else {
                // 543854b stale hit: returned cachedProfile after 1 synchronous Account-V1 call and launched HIGH background refresh
                return cachedProfile?.copy(recentMatches = cachedProfile.recentMatches?.take(20))
            }
        }
        // Baseline refresh: redundant Redis profile GET + sequential Summoner-V4 + full un-anchored match-ID pagination + sequential SETs
        baselineCache.get(cacheKey, SummonerProfileDto::class.java)
        val startOfYear = java.time.Year.of(2026).atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()
        // Simulate the extra sequential Summoner-V4 RTT that 543854b executed before launching League-V4 + Match-IDs when cachedProfile != null,
        // and strip anchor by executing un-anchored pagination first on warm refresh
        if (cachedProfile != null) {
            // In 543854b, getSummonerByPuuid ran sequentially BEFORE fetchLeagueRank + full fetchMatchIdsWithOutcomeSince;
            // helper.refreshPlayerProfile overlaps Summoner-V4 and uses anchor, so for exact 543854b timing/counts we run un-anchored
            val unanchoredRiot = object : RiotApiService(
                riotRestClient = riotApiService.let {
                    val f = RiotApiService::class.java.getDeclaredField("riotRestClient")
                    f.isAccessible = true
                    f.get(it) as RestClient
                },
                dataDragonRestClient = RestClient.create(),
                rateLimiter = riotApiService.let {
                    val f = RiotApiService::class.java.getDeclaredField("rateLimiter")
                    f.isAccessible = true
                    f.get(it) as RiotApiRateLimiter
                }
            ) {
                override suspend fun fetchMatchIdsWithOutcomeSince(
                    puuid: String,
                    region: String,
                    startTime: Long?
                ): MatchIdsPaginationOutcome {
                    // Strip anchor context to reproduce exact 543854b full-season pagination
                    return withContext(kotlin.coroutines.EmptyCoroutineContext) {
                        var start = 0
                        val all = mutableListOf<String>()
                        var pages = 0
                        while (true) {
                            val page = getMatchIdsByPuuid(puuid, region, 420, startTime, null, 100, start) ?: break
                            pages++
                            if (page.isEmpty()) break
                            all.addAll(page)
                            if (page.size < 100 || all.size > 2000) break
                            start += 100
                        }
                        MatchIdsPaginationOutcome(
                            matchIds = all,
                            pagesFetched = pages,
                            exhaustedCleanly = all.size <= 2000,
                            hitSafetyCap = all.size > 2000,
                            stoppedOnPageFailure = false
                        )
                    }
                }
            }
            // Extra sequential Summoner-V4 delay vs overlapped: run 1 Summoner-V4 call on baseline? No — to keep exact Riot call count (not +1 extra),
            // let unanchoredRiot execute Summoner-V4 sequentially by delaying League/IDs until Summoner-V4 finishes:
            val summonerDone = CompletableDeferred<Unit>()
            val seqRiot = object : RiotApiService(
                riotRestClient = riotApiService.let {
                    val f = RiotApiService::class.java.getDeclaredField("riotRestClient")
                    f.isAccessible = true
                    f.get(it) as RestClient
                },
                dataDragonRestClient = RestClient.create(),
                rateLimiter = riotApiService.let {
                    val f = RiotApiService::class.java.getDeclaredField("rateLimiter")
                    f.isAccessible = true
                    f.get(it) as RiotApiRateLimiter
                }
            ) {
                override suspend fun getSummonerByPuuid(puuid: String, region: String): SummonerDto? {
                    return try {
                        super.getSummonerByPuuid(puuid, region)
                    } finally {
                        summonerDone.complete(Unit)
                    }
                }
                override suspend fun fetchLeagueRank(puuid: String, region: String): LeagueEntryDto? {
                    summonerDone.await()
                    return super.fetchLeagueRank(puuid, region)
                }
                override suspend fun fetchMatchIdsWithOutcomeSince(
                    puuid: String,
                    region: String,
                    startTime: Long?
                ): MatchIdsPaginationOutcome {
                    summonerDone.await()
                    return unanchoredRiot.fetchMatchIdsWithOutcomeSince(puuid, region, startTime)
                }
            }
            val seqHelper = PlayerCacheService(
                redisCacheService = baselineCache,
                riotApiService = seqRiot,
                indexedPlayerRepository = indexedRepo,
                timeProvider = clock,
                playerSeasonStatsRepository = seasonRepo,
                transactionTemplate = txTemplate,
                objectMapper = mapper
            )
            return seqHelper.refreshPlayerProfile(puuid, region, summonerName, tagLine)?.let {
                it.copy(recentMatches = it.recentMatches?.take(20))
            }
        }
        return helper.refreshPlayerProfile(puuid, region, summonerName, tagLine)?.let {
            it.copy(recentMatches = it.recentMatches?.take(20))
        }
    }

    test("Bounded-fixture before/after timing & Riot/Redis call-count benchmark preserves full 450, 924, and 2100 2026 YTD games across all patches") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val region = "euw1"
        val routing = "europe"
        val gameName = "SpeedPlayer"
        val tagLine = "EUW"
        val puuid450 = "bench-450-puuid"

        fun setup450Fixture(httpRttMs: Long, redisRttMs: Long): Triple<FakeRiotHttpBackend, InMemoryLatencyRedisHarness, MutableClock> {
            val clock = MutableClock(baseNowMs)
            val riotBackend = FakeRiotHttpBackend(mapper, httpRttMs = httpRttMs, appRateLimitHeader = "1000:1,10000:120")
            val redisHarness = InMemoryLatencyRedisHarness(mapper, networkRttMs = redisRttMs, enablePipelineSupport = true)
            riotBackend.accountsByRiotId["$routing|${gameName.lowercase()}|${tagLine.lowercase()}"] =
                AccountDto(puuid450, gameName, tagLine)
            riotBackend.summonersByRegionPuuid["$region|$puuid450"] =
                SummonerDto("sum-450", puuid450, 412L, 29)
            riotBackend.ranksByRegionPuuid["$region|$puuid450"] = listOf(
                LeagueEntryDto(
                    leagueId = "lg-1",
                    queueType = "RANKED_SOLO_5x5",
                    tier = "DIAMOND",
                    rank = "II",
                    puuid = puuid450,
                    summonerName = gameName,
                    leaguePoints = 64,
                    wins = 240,
                    losses = 210,
                    veteran = false,
                    inactive = false,
                    freshBlood = false,
                    hotStreak = true
                )
            )
            for (idx in 1..453) {
                val m = buildSeasonalMatch(puuid450, idx, baseNowMs)
                riotBackend.matchesById[m.metadata!!.matchId!!] = m
            }
            return Triple(riotBackend, redisHarness, clock)
        }

        // --- 1. BASELINE (543854b) MEASUREMENTS (12ms Riot HTTP RTT, 1ms Redis RTT) ---
        val (baseRiotBackend, baseRedis, baseClock) = setup450Fixture(httpRttMs = 12L, redisRttMs = 1L)
        var baseCurrentMaxIdx = 450
        baseRiotBackend.matchIdsPagesGenerator[puuid450] = { start, count ->
            val all = (baseCurrentMaxIdx downTo 1).map { "EUW1_$it" }
            if (start >= all.size) emptyList() else all.subList(start, minOf(start + count, all.size))
        }
        val baseLimiter = RiotApiRateLimiter(baseClock)
        val baseRiotService = RiotApiService(baseRiotBackend.buildRestClient(), baseLimiter, RestClient.create(), baseClock)
        val (baseSeasonRepo, _) = inMemorySeasonRepo()
        val baseTx = noOpTransactionTemplate()

        val coldBeforeStartNs = System.nanoTime()
        val coldBeforeProfile = executeBaseline543854bGetSummonerProfile(
            region, gameName, tagLine, false, baseRiotService, baseRedis, baseSeasonRepo, baseTx, baseClock
        )
        val coldBeforeMs = (System.nanoTime() - coldBeforeStartNs) / 1_000_000
        val coldBeforeRiotCalls = baseRiotBackend.totalHttpCalls.get()
        val coldBeforeRedisRtts = baseRedis.totalNetworkRoundTrips()
        val coldBeforeSingleSets = baseRedis.singleSetCalls.get()

        // Warm fresh (<5m) baseline
        baseClock.advanceBy(30_000L)
        baseRiotBackend.resetCounters()
        baseRedis.resetCounters()
        val warmBeforeStartNs = System.nanoTime()
        val warmBeforeProfile = executeBaseline543854bGetSummonerProfile(
            region, gameName, tagLine, false, baseRiotService, baseRedis, baseSeasonRepo, baseTx, baseClock
        )
        val warmBeforeMs = (System.nanoTime() - warmBeforeStartNs) / 1_000_000
        val warmBeforeRiotCalls = baseRiotBackend.totalHttpCalls.get()
        val warmBeforeRedisRtts = baseRedis.totalNetworkRoundTrips()

        // Stale (>=5m) foreground baseline
        baseClock.advanceBy(360_000L)
        baseRiotBackend.resetCounters()
        baseRedis.resetCounters()
        val staleBeforeStartNs = System.nanoTime()
        executeBaseline543854bGetSummonerProfile(
            region, gameName, tagLine, false, baseRiotService, baseRedis, baseSeasonRepo, baseTx, baseClock
        )
        val staleBeforeMs = (System.nanoTime() - staleBeforeStartNs) / 1_000_000
        val staleBeforeFgRiotCalls = baseRiotBackend.totalHttpCalls.get()

        // Explicit refresh (>60s) with +3 new games on 450-game durable ledger (baseline)
        baseCurrentMaxIdx = 453
        baseRiotBackend.resetCounters()
        baseRedis.resetCounters()
        val inc450BeforeStartNs = System.nanoTime()
        val inc450BeforeProfile = executeBaseline543854bGetSummonerProfile(
            region, gameName, tagLine, true, baseRiotService, baseRedis, baseSeasonRepo, baseTx, baseClock
        )
        val inc450BeforeMs = (System.nanoTime() - inc450BeforeStartNs) / 1_000_000
        val inc450BeforeRiotCalls = baseRiotBackend.totalHttpCalls.get()
        val inc450BeforeIdPages = baseRiotBackend.matchIdsPageCalls.get()
        val inc450BeforeRedisRtts = baseRedis.totalNetworkRoundTrips()

        // --- 2. BRANCH (perf/season-speed-nov11) MEASUREMENTS (12ms Riot HTTP RTT, 1ms Redis RTT) ---
        val (branchRiotBackend, branchRedis, branchClock) = setup450Fixture(httpRttMs = 12L, redisRttMs = 1L)
        var branchCurrentMaxIdx = 450
        branchRiotBackend.matchIdsPagesGenerator[puuid450] = { start, count ->
            val all = (branchCurrentMaxIdx downTo 1).map { "EUW1_$it" }
            if (start >= all.size) emptyList() else all.subList(start, minOf(start + count, all.size))
        }
        val branchLimiter = RiotApiRateLimiter(branchClock)
        val branchRiotService = RiotApiService(branchRiotBackend.buildRestClient(), branchLimiter, RestClient.create(), branchClock)
        val (branchSeasonRepo, branchSeasonMap) = inMemorySeasonRepo()
        val branchTx = noOpTransactionTemplate()
        val branchCacheService = PlayerCacheService(
            redisCacheService = branchRedis.service,
            riotApiService = branchRiotService,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = branchClock,
            playerSeasonStatsRepository = branchSeasonRepo,
            transactionTemplate = branchTx,
            objectMapper = mapper
        )
        val branchProfileService = SummonerProfileService(branchRiotService, branchCacheService)

        val coldAfterStartNs = System.nanoTime()
        val coldAfterProfile = branchProfileService.getSummonerProfile(region, gameName, tagLine, explicitRefresh = false)
        val coldAfterMs = (System.nanoTime() - coldAfterStartNs) / 1_000_000
        val coldAfterRiotCalls = branchRiotBackend.totalHttpCalls.get()
        val coldAfterPeakConcurrency = branchRiotBackend.peakConcurrentMatchCalls.get()
        val coldAfterPipelineBatches = branchRedis.pipelineBatchCalls.get()
        val coldAfterSingleSets = branchRedis.singleSetCalls.get()
        val coldAfterRedisRtts = branchRedis.totalNetworkRoundTrips()
        val telemetryAfterCold = branchCacheService.getDiscoveryAndSeasonWriteTelemetry()

        // Verify full 450-game 2026 YTD across all 22 patches + memory bounds
        coldBeforeProfile?.totalMatches shouldBe 450
        coldAfterProfile?.totalMatches shouldBe 450
        coldAfterProfile?.allMatchIds?.size shouldBe 450
        coldAfterProfile?.recentMatches?.size shouldBe 20
        coldAfterProfile?.overallStats?.let { it.wins + it.losses } shouldBe coldBeforeProfile?.overallStats?.let { it.wins + it.losses }
        coldAfterProfile?.overallStats?.wins shouldBe coldBeforeProfile?.overallStats?.wins
        val savedDurable450 = mapper.readValue(
            branchSeasonMap["euw1|$puuid450|2026|420"]!!.stateJson,
            DurablePlayerSeasonStatsDto::class.java
        )
        savedDurable450.historyComplete shouldBe true
        savedDurable450.processedMatchIds.size shouldBe 450
        savedDurable450.championCounters.sumOf { it.games } shouldBe 450
        // Verify all 22 patches (16.1..16.22) were folded into the 450-game ledger (not truncated to recent 20 or single patch)
        val distinctPatchesInSeason = (1..450).map { branchRiotBackend.matchesById["EUW1_$it"]!!.info!!.gameVersion!! }.toSet()
        distinctPatchesInSeason.size shouldBe 22

        coldAfterRiotCalls shouldBe 458
        coldBeforeRiotCalls shouldBe 458
        coldAfterPeakConcurrency shouldBeLessThanOrEqualTo 20
        coldAfterPipelineBatches shouldBe 9 // 9 chunks of 50 matches pipelined instead of 450 single SETs
        coldAfterSingleSets shouldBe 1 // Only 1 single SET for player:profile
        coldBeforeSingleSets shouldBe 451
        telemetryAfterCold["lastRefreshPeakLiveMatchDtos"]!! shouldBeLessThanOrEqualTo 70L
        telemetryAfterCold["lastRefreshPeakCrossChunkRetainedMatchDtos"]!! shouldBeLessThanOrEqualTo 20L
        coldAfterMs shouldBeLessThan coldBeforeMs

        // Warm fresh (<5m) branch
        branchClock.advanceBy(30_000L)
        branchRiotBackend.resetCounters()
        branchRedis.resetCounters()
        val warmAfterStartNs = System.nanoTime()
        val warmAfterProfile = branchProfileService.getSummonerProfile(region, gameName, tagLine, explicitRefresh = false)
        val warmAfterMs = (System.nanoTime() - warmAfterStartNs) / 1_000_000
        val warmAfterRiotCalls = branchRiotBackend.totalHttpCalls.get()
        val warmAfterRedisRtts = branchRedis.totalNetworkRoundTrips()

        warmBeforeProfile?.totalMatches shouldBe 450
        warmAfterProfile?.totalMatches shouldBe 450
        warmBeforeRiotCalls shouldBe 1
        warmAfterRiotCalls shouldBe 0
        warmAfterRedisRtts shouldBe 1
        warmAfterMs shouldBeLessThan warmBeforeMs

        // Explicit refresh within 60s cooldown (<60s) branch
        branchRiotBackend.resetCounters()
        branchRedis.resetCounters()
        val cooldownAfterStartNs = System.nanoTime()
        val cooldownProfile = branchProfileService.getSummonerProfile(region, gameName, tagLine, explicitRefresh = true)
        val cooldownAfterMs = (System.nanoTime() - cooldownAfterStartNs) / 1_000_000
        cooldownProfile?.totalMatches shouldBe 450
        branchRiotBackend.totalHttpCalls.get() shouldBe 0
        branchRedis.totalNetworkRoundTrips() shouldBe 1

        // Stale (>=5m) foreground hit + LOW-priority background refresh branch
        branchClock.advanceBy(360_000L)
        branchRiotBackend.resetCounters()
        branchRedis.resetCounters()
        val observedActiveHighDuringBg = AtomicInteger(0)
        branchRiotBackend.onBeforeEndpoint = { _, _ ->
            val snap = branchLimiter.getVerificationSnapshot(region)
            if (snap.activeUserRequests > 0) {
                observedActiveHighDuringBg.incrementAndGet()
            }
        }
        val staleAfterStartNs = System.nanoTime()
        val staleAfterProfile = branchProfileService.getSummonerProfile(region, gameName, tagLine, explicitRefresh = false)
        val staleAfterMs = (System.nanoTime() - staleAfterStartNs) / 1_000_000
        val staleAfterFgRiotCalls = branchRiotBackend.accountByRiotIdCalls.get()
        staleAfterProfile?.totalMatches shouldBe 450
        staleBeforeFgRiotCalls shouldBe 1
        staleAfterFgRiotCalls shouldBe 0
        staleAfterMs shouldBeLessThan staleBeforeMs

        // Wait for background refresh (0 new games) to complete and verify it ran at LOW priority with 1 anchored ID page
        withContext(Dispatchers.IO) {
            val deadline = System.currentTimeMillis() + 5_000L
            while (System.currentTimeMillis() < deadline && branchRiotBackend.matchIdsPageCalls.get() < 1) {
                delay(20)
            }
            delay(80)
        }
        branchRiotBackend.onBeforeEndpoint = null
        observedActiveHighDuringBg.get() shouldBe 0
        branchRiotBackend.matchIdsPageCalls.get() shouldBe 1 // 1 anchored page instead of 5!
        branchRiotBackend.totalHttpCalls.get() shouldBe 4 // LOW account verify + overlapped summoner + league + 1 match-ID page

        // Explicit refresh (>60s) with +3 new games on 450-game durable ledger (branch)
        branchClock.advanceBy(120_000L)
        branchCurrentMaxIdx = 453
        branchRiotBackend.resetCounters()
        branchRedis.resetCounters()
        val inc450AfterStartNs = System.nanoTime()
        val inc450AfterProfile = branchProfileService.getSummonerProfile(region, gameName, tagLine, explicitRefresh = true)
        val inc450AfterMs = (System.nanoTime() - inc450AfterStartNs) / 1_000_000
        val inc450AfterRiotCalls = branchRiotBackend.totalHttpCalls.get()
        val inc450AfterIdPages = branchRiotBackend.matchIdsPageCalls.get()
        val inc450AfterRedisRtts = branchRedis.totalNetworkRoundTrips()

        inc450BeforeProfile?.totalMatches shouldBe 453
        inc450AfterProfile?.totalMatches shouldBe 453
        inc450AfterProfile?.overallStats?.let { it.wins + it.losses } shouldBe inc450BeforeProfile?.overallStats?.let { it.wins + it.losses }
        inc450AfterProfile?.overallStats?.wins shouldBe inc450BeforeProfile?.overallStats?.wins
        inc450BeforeIdPages shouldBe 5
        inc450AfterIdPages shouldBe 1
        inc450BeforeRiotCalls shouldBe 11 // 1 acct + 1 sum + 1 rank + 5 ID pages + 3 matches
        inc450AfterRiotCalls shouldBe 7   // 1 acct + 1 sum + 1 rank + 1 ID page + 3 matches
        inc450AfterMs shouldBeLessThan inc450BeforeMs

        // --- 3. 924-GAME & 2,100-GAME (CAPPED) INCREMENTAL REFRESH (+3 NEW GAMES) ---
        fun runLargeLedgerIncrementalComparison(baseGames: Int, isCapped2100: Boolean): Pair<Pair<Long, Int>, Pair<Long, Int>> {
            val puuidLarge = "puuid-large-$baseGames"
            val nameLarge = "Large$baseGames"
            val (rBackend, rRedis, rClock) = Triple(
                FakeRiotHttpBackend(mapper, httpRttMs = 12L, appRateLimitHeader = "1000:1,10000:120"),
                InMemoryLatencyRedisHarness(mapper, networkRttMs = 1L, enablePipelineSupport = true),
                MutableClock(baseNowMs)
            )
            rBackend.accountsByRiotId["$routing|${nameLarge.lowercase()}|euw"] = AccountDto(puuidLarge, nameLarge, "EUW")
            rBackend.summonersByRegionPuuid["$region|$puuidLarge"] = SummonerDto("s-$baseGames", puuidLarge, 600L, 10)
            rBackend.ranksByRegionPuuid["$region|$puuidLarge"] = emptyList()

            val accBase = MatchStatsAccumulator(puuidLarge)
            val baseIds = (baseGames downTo 1).map { "EUW1_$it" }
            for (idx in baseGames downTo 1) {
                val m = buildSeasonalMatch(puuidLarge, idx, baseNowMs)
                accBase.add(m)
                if (idx <= 10 || idx > baseGames - 25) {
                    rBackend.matchesById[m.metadata!!.matchId!!] = m
                    rRedis.service.set("match:euw1:EUW1_$idx", m, Duration.ofDays(60))
                }
            }
            for (idx in (baseGames + 1)..(baseGames + 3)) {
                rBackend.matchesById["EUW1_$idx"] = buildSeasonalMatch(puuidLarge, idx, baseNowMs)
            }
            val newIds = ((baseGames + 3) downTo 1).map { "EUW1_$it" }
            rBackend.matchIdsPagesGenerator[puuidLarge] = { start, count ->
                if (start >= newIds.size) emptyList() else newIds.subList(start, minOf(start + count, newIds.size))
            }

            val (snapOverall, snapChamp) = accBase.toAdditiveSnapshots()
            val (champStats, overallStats) = accBase.getResult()
            val recent20 = (baseGames downTo (baseGames - 19)).map { rBackend.matchesById["EUW1_$it"]!! }
            val seedDurable = DurablePlayerSeasonStatsDto(
                schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
                region = region,
                puuid = puuidLarge,
                seasonYear = 2026,
                queueId = 420,
                processedMatchIds = baseIds,
                pendingMatchIds = emptyList(),
                excludedMatchIds = emptyList(),
                hitPreSeasonBoundary = !isCapped2100,
                firstPreSeasonMatchId = if (isCapped2100) null else "EUW1_0",
                historyCapped = isCapped2100,
                historyComplete = !isCapped2100,
                overallCounters = snapOverall,
                championCounters = snapChamp,
                updatedAtEpochMilli = baseNowMs - 600_000L
            )
            val seedProfile = SummonerProfileDto(
                puuid = puuidLarge,
                gameName = nameLarge,
                tagLine = "EUW",
                summonerLevel = 600L,
                profileIconId = 10,
                soloQueueRank = null,
                recentMatches = recent20,
                allMatchIds = baseIds,
                championStats = champStats,
                overallStats = overallStats,
                improvementStats = TimelineAnalysisService().computeImprovementStats(emptyList(), puuidLarge, recent20),
                totalMatches = baseGames,
                lastRefreshed = Instant.ofEpochMilli(baseNowMs - 600_000L)
            )

            // Baseline run
            val (bRepo, bMap) = inMemorySeasonRepo()
            bMap["euw1|$puuidLarge|2026|420"] = PlayerSeasonStatsEntity(
                id = UUID.randomUUID(),
                region = region,
                puuid = puuidLarge,
                seasonYear = 2026,
                queueId = 420,
                schemaVersion = 1,
                foldedGamesCount = baseGames,
                pendingGamesCount = 0,
                historyCapped = isCapped2100,
                historyComplete = !isCapped2100,
                stateJson = mapper.writeValueAsString(seedDurable),
                updatedAt = Instant.ofEpochMilli(baseNowMs - 600_000L),
                version = 1L
            )
            rRedis.service.set("player:profile:euw1:$puuidLarge", seedProfile, Duration.ofDays(14))
            rBackend.resetCounters()
            rRedis.resetCounters()
            val bRiot = RiotApiService(rBackend.buildRestClient(), RiotApiRateLimiter(rClock), RestClient.create(), rClock)
            val bStart = System.nanoTime()
            val bRes = kotlinx.coroutines.runBlocking {
                executeBaseline543854bGetSummonerProfile(region, nameLarge, "EUW", true, bRiot, rRedis, bRepo, noOpTransactionTemplate(), rClock)
            }
            val bMs = (System.nanoTime() - bStart) / 1_000_000
            val bCalls = rBackend.totalHttpCalls.get()

            // Branch run
            val (aRepo, aMap) = inMemorySeasonRepo()
            aMap["euw1|$puuidLarge|2026|420"] = PlayerSeasonStatsEntity(
                id = UUID.randomUUID(),
                region = region,
                puuid = puuidLarge,
                seasonYear = 2026,
                queueId = 420,
                schemaVersion = 1,
                foldedGamesCount = baseGames,
                pendingGamesCount = 0,
                historyCapped = isCapped2100,
                historyComplete = !isCapped2100,
                stateJson = mapper.writeValueAsString(seedDurable),
                updatedAt = Instant.ofEpochMilli(baseNowMs - 600_000L),
                version = 1L
            )
            rRedis.service.set("player:profile:euw1:$puuidLarge", seedProfile, Duration.ofDays(14))
            for (idx in (baseGames + 1)..(baseGames + 3)) {
                rRedis.store.remove("match:euw1:EUW1_$idx")
            }
            rBackend.resetCounters()
            rRedis.resetCounters()
            val aRiot = RiotApiService(rBackend.buildRestClient(), RiotApiRateLimiter(rClock), RestClient.create(), rClock)
            val aCache = PlayerCacheService(
                redisCacheService = rRedis.service,
                riotApiService = aRiot,
                indexedPlayerRepository = mockk(relaxed = true),
                timeProvider = rClock,
                playerSeasonStatsRepository = aRepo,
                transactionTemplate = noOpTransactionTemplate(),
                objectMapper = mapper
            )
            val aSvc = SummonerProfileService(aRiot, aCache)
            val aStart = System.nanoTime()
            val aRes = kotlinx.coroutines.runBlocking {
                aSvc.getSummonerProfile(region, nameLarge, "EUW", explicitRefresh = true)
            }
            val aMs = (System.nanoTime() - aStart) / 1_000_000
            val aCalls = rBackend.totalHttpCalls.get()
            rBackend.matchIdsPageCalls.get() shouldBe 1

            val expectedTotal = if (isCapped2100) 2_100 else baseGames + 3
            bRes?.totalMatches shouldBe expectedTotal
            aRes?.totalMatches shouldBe expectedTotal
            aRes?.overallStats?.let { it.wins + it.losses } shouldBe expectedTotal
            val committed = mapper.readValue(aMap["euw1|$puuidLarge|2026|420"]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)
            committed.historyCapped shouldBe isCapped2100
            committed.historyComplete shouldBe !isCapped2100
            committed.processedMatchIds.first() shouldBe "EUW1_${baseGames + 3}"
            if (isCapped2100) {
                committed.processedMatchIds.last() shouldBe "EUW1_4"
            } else {
                committed.processedMatchIds.last() shouldBe "EUW1_1"
            }
            return (bMs to bCalls) to (aMs to aCalls)
        }

        val (inc924Before, inc924After) = runLargeLedgerIncrementalComparison(924, isCapped2100 = false)
        val (inc2100Before, inc2100After) = runLargeLedgerIncrementalComparison(2_100, isCapped2100 = true)

        inc924Before.second shouldBe 16 // 1 acct + 1 sum + 1 rank + 10 ID pages + 3 matches
        inc924After.second shouldBe 7   // 1 acct + 1 sum + 1 rank + 1 ID page + 3 matches
        inc924After.first shouldBeLessThan inc924Before.first

        inc2100Before.second shouldBe 27 // 1 acct + 1 sum + 1 rank + 21 ID pages + 3 matches
        inc2100After.second shouldBe 7   // 1 acct + 1 sum + 1 rank + 1 ID page + 3 matches
        inc2100After.first shouldBeLessThan inc2100Before.first

        println(
            """
            [SPEED_BENCHMARK_SUMMARY] (Synthetic 12ms Riot HTTP RTT, 1ms Redis RTT; full 2026 YTD across 22 patches preserved)
              Cold 450g (patches 16.1..16.22): before=${coldBeforeMs}ms (riotCalls=$coldBeforeRiotCalls, redisRtts=$coldBeforeRedisRtts, singleSets=$coldBeforeSingleSets) -> after=${coldAfterMs}ms (riotCalls=$coldAfterRiotCalls, redisRtts=$coldAfterRedisRtts, pipelineBatches=$coldAfterPipelineBatches, peakConcurrentMatchFetches=$coldAfterPeakConcurrency, peakLiveDtos=${telemetryAfterCold["lastRefreshPeakLiveMatchDtos"]})
              Warm Fresh (<5m, 450g):          before=${warmBeforeMs}ms (riotCalls=$warmBeforeRiotCalls, redisRtts=$warmBeforeRedisRtts) -> after=${warmAfterMs}ms (riotCalls=$warmAfterRiotCalls, redisRtts=$warmAfterRedisRtts)
              Explicit Cooldown (<60s, 450g):  after=${cooldownAfterMs}ms (riotCalls=0, redisRtts=1)
              Warm Stale (>=5m fg, 450g):      before=${staleBeforeMs}ms (fgRiotCalls=$staleBeforeFgRiotCalls) -> after=${staleAfterMs}ms (fgRiotCalls=$staleAfterFgRiotCalls, bgActiveHighUserReqs=0, bgIdPages=1)
              Incremental +3g on 450g YTD:     before=${inc450BeforeMs}ms (riotCalls=$inc450BeforeRiotCalls, idPages=$inc450BeforeIdPages, redisRtts=$inc450BeforeRedisRtts) -> after=${inc450AfterMs}ms (riotCalls=$inc450AfterRiotCalls, idPages=$inc450AfterIdPages, redisRtts=$inc450AfterRedisRtts)
              Incremental +3g on 924g YTD:     before=${inc924Before.first}ms (riotCalls=${inc924Before.second}) -> after=${inc924After.first}ms (riotCalls=${inc924After.second})
              Incremental +3g on 2100g capped: before=${inc2100Before.first}ms (riotCalls=${inc2100Before.second}) -> after=${inc2100After.first}ms (riotCalls=${inc2100After.second})
            """.trimIndent()
        )
    }

    test("Region isolation, Riot-ID rename alias purge, stale index eviction, and account mismatch reconciliation") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)
        val riotBackend = FakeRiotHttpBackend(mapper)
        val redis = InMemoryLatencyRedisHarness(mapper)
        val limiter = RiotApiRateLimiter(clock)
        val riotService = RiotApiService(riotBackend.buildRestClient(), limiter, RestClient.create(), clock)
        val (seasonRepo, _) = inMemorySeasonRepo()
        val cacheService = PlayerCacheService(
            redisCacheService = redis.service,
            riotApiService = riotService,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = seasonRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper
        )
        val profileService = SummonerProfileService(riotService, cacheService)

        // Seed EUW1 Player A ("SharedName#TAG" -> puuid-euw-A) and NA1 Player X ("SharedName#TAG" -> puuid-na-X)
        riotBackend.accountsByRiotId["europe|sharedname|tag"] = AccountDto("puuid-euw-A", "SharedName", "TAG")
        riotBackend.accountsByRiotId["americas|sharedname|tag"] = AccountDto("puuid-na-X", "SharedName", "TAG")
        riotBackend.summonersByRegionPuuid["euw1|puuid-euw-A"] = SummonerDto("s-a", "puuid-euw-A", 200L, 1)
        riotBackend.summonersByRegionPuuid["na1|puuid-na-X"] = SummonerDto("s-x", "puuid-na-X", 300L, 2)
        riotBackend.matchIdsPagesGenerator["puuid-euw-A"] = { _, _ -> listOf("EUW1_1") }
        riotBackend.matchIdsPagesGenerator["puuid-na-X"] = { _, _ -> listOf("NA1_1") }
        riotBackend.matchesById["EUW1_1"] = buildSeasonalMatch("puuid-euw-A", 1, baseNowMs, "EUW1")
        riotBackend.matchesById["NA1_1"] = buildSeasonalMatch("puuid-na-X", 1, baseNowMs, "NA1")

        // 1. Region isolation: warming EUW1 must NEVER cross-serve on NA1
        val euwProfile = profileService.getSummonerProfile("euw1", "SharedName", "TAG")
        euwProfile?.puuid shouldBe "puuid-euw-A"
        riotBackend.resetCounters()
        val naProfile = profileService.getSummonerProfile("na1", "SharedName", "TAG")
        naProfile?.puuid shouldBe "puuid-na-X"
        riotBackend.accountByRiotIdCalls.get() shouldBe 1 // NA1 was cold, so it called Americas Account-V1

        // 2. Player A renames from SharedName#TAG -> RenamedA#TAG; refreshing RenamedA#TAG purges SharedName#TAG alias for puuid-euw-A
        riotBackend.accountsByRiotId.remove("europe|sharedname|tag")
        riotBackend.accountsByRiotId["europe|renameda|tag"] = AccountDto("puuid-euw-A", "RenamedA", "TAG")
        clock.advanceBy(120_000L)
        val renamedProfile = profileService.getSummonerProfile("euw1", "RenamedA", "TAG", explicitRefresh = true)
        renamedProfile?.puuid shouldBe "puuid-euw-A"
        renamedProfile?.gameName shouldBe "RenamedA"

        // Now SharedName#TAG 404s on Riot; querying SharedName#TAG must NOT serve puuid-euw-A from stale alias!
        riotBackend.resetCounters()
        val oldAliasAfterRename = profileService.getSummonerProfile("euw1", "SharedName", "TAG", explicitRefresh = false)
        oldAliasAfterRename shouldBe null
        riotBackend.accountByRiotIdCalls.get() shouldBe 1

        // 3. Stale index account mismatch: Player B ("puuid-euw-B") claims "RenamedA#TAG" after Player A went stale (>=5m)
        riotBackend.accountsByRiotId["europe|renameda|tag"] = AccountDto("puuid-euw-B", "RenamedA", "TAG")
        riotBackend.summonersByRegionPuuid["euw1|puuid-euw-B"] = SummonerDto("s-b", "puuid-euw-B", 99L, 7)
        riotBackend.matchIdsPagesGenerator["puuid-euw-B"] = { _, _ -> listOf("EUW1_2") }
        riotBackend.matchesById["EUW1_2"] = buildSeasonalMatch("puuid-euw-B", 2, baseNowMs, "EUW1")

        clock.advanceBy(400_000L) // Stale (>5m)
        riotBackend.resetCounters()
        val releaseBgAccountCheck = CompletableDeferred<Unit>()
        val bgAccountActiveHighUsers = AtomicInteger(-1)
        riotBackend.onBeforeEndpoint = { tag, _ ->
            if (tag == "account-v1") {
                bgAccountActiveHighUsers.set(limiter.getVerificationSnapshot("euw1").activeUserRequests)
                kotlinx.coroutines.runBlocking { releaseBgAccountCheck.await() }
            }
        }
        val staleServedBeforeReconcile = profileService.getSummonerProfile("euw1", "RenamedA", "TAG", explicitRefresh = false)
        staleServedBeforeReconcile?.puuid shouldBe "puuid-euw-A"
        riotBackend.accountByRiotIdCalls.get() shouldBe 0 // Immediate stale serve before background HTTP completes
        releaseBgAccountCheck.complete(Unit)

        // Wait for LOW background ownership check to detect mismatch (puuid-euw-B != puuid-euw-A) and evict stale mapping
        withContext(Dispatchers.IO) {
            val deadline = System.currentTimeMillis() + 3_000L
            while (System.currentTimeMillis() < deadline && riotBackend.accountByRiotIdCalls.get() < 1) {
                delay(20)
            }
            delay(50)
        }
        riotBackend.onBeforeEndpoint = null
        bgAccountActiveHighUsers.get() shouldBe 0 // Ran strictly at LOW priority (0 active foreground user requests)
        // Background refresh must NOT have refreshed puuid-euw-A under RenamedA#TAG
        riotBackend.summonerByPuuidCalls.get() shouldBe 0

        // Next lookup on RenamedA#TAG now resolves live to Player B ("puuid-euw-B")!
        val reconciledOwnerProfile = profileService.getSummonerProfile("euw1", "RenamedA", "TAG", explicitRefresh = false)
        reconciledOwnerProfile?.puuid shouldBe "puuid-euw-B"
    }

    test("Single-flight priority promotion (LOW -> HIGH), waiter cancellation isolation, and rate-limiter user-request accounting") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)
        val region = "euw1"
        val puuid = "promo-puuid"
        val riotBackend = FakeRiotHttpBackend(mapper, httpRttMs = 20L, appRateLimitHeader = "20:1,100:120")
        val redis = InMemoryLatencyRedisHarness(mapper)
        val limiter = RiotApiRateLimiter(clock)
        val riotService = RiotApiService(riotBackend.buildRestClient(), limiter, RestClient.create(), clock)
        val (seasonRepo, _) = inMemorySeasonRepo()
        val cacheService = PlayerCacheService(
            redisCacheService = redis.service,
            riotApiService = riotService,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = seasonRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper
        )

        riotBackend.summonersByRegionPuuid["$region|$puuid"] = SummonerDto("s-p", puuid, 150L, 5)
        riotBackend.ranksByRegionPuuid["$region|$puuid"] = emptyList()
        riotBackend.matchIdsPagesGenerator[puuid] = { _, _ -> listOf("EUW1_1") }
        riotBackend.matchesById["EUW1_1"] = buildSeasonalMatch(puuid, 1, baseNowMs)

        // 1. Seed a HIGH user request completion so RegionRateLimiter enters its 2,000ms USER_REQUEST_YIELD_WINDOW_MS
        limiter.beginUserRequest(region)
        limiter.endUserRequest(region)
        limiter.getVerificationSnapshot(region).activeUserRequests shouldBe 0

        // Launch background refresh (starts at LOW priority and parks inside awaitLowPriorityClearance 2,000ms window)
        val bgStarted = cacheService.triggerBackgroundRefreshIfCooldownAllows(puuid, region, "PromoUser", "EUW")
        bgStarted shouldBe true
        withContext(Dispatchers.IO) { delay(60) }

        // Join the in-flight refresh as a foreground HIGH caller: must promote LOW -> HIGH and wake IMMEDIATELY (< 500ms, NOT 2,000ms!)
        val joinStartMs = System.currentTimeMillis()
        val promotedProfile = withContext(Dispatchers.IO) {
            cacheService.refreshPlayerProfile(puuid, region, "PromoUser", "EUW")
        }
        val joinElapsedMs = System.currentTimeMillis() - joinStartMs
        promotedProfile?.totalMatches shouldBe 1
        joinElapsedMs shouldBeLessThan 800L // Would be >= 1,940ms without promotionSignal wakeup!
        limiter.getVerificationSnapshot(region).activeUserRequests shouldBe 0

        // 2. Cancellation isolation: Leader Caller 1 cancels mid-flight while Joiner Caller 2 awaits -> Caller 2 still succeeds!
        val cancelPuuid = "cancel-iso-puuid"
        val cancelMatchId = "EUW1_CANCEL_1"
        val gateEntered = CompletableDeferred<Unit>()
        val releaseGate = CompletableDeferred<Unit>()
        riotBackend.summonersByRegionPuuid["$region|$cancelPuuid"] = SummonerDto("s-c", cancelPuuid, 220L, 6)
        riotBackend.ranksByRegionPuuid["$region|$cancelPuuid"] = emptyList()
        riotBackend.matchesById[cancelMatchId] = buildSeasonalMatch(cancelPuuid, 1, baseNowMs)
        riotBackend.matchIdsPagesGenerator[cancelPuuid] = { _, _ -> listOf(cancelMatchId) }
        riotBackend.onBeforeEndpoint = { tag, uri ->
            if (tag == "summoner-v4" && uri.path.endsWith(cancelPuuid)) {
                gateEntered.complete(Unit)
                kotlinx.coroutines.runBlocking { releaseGate.await() }
            }
        }

        val caller1Cancelled = AtomicBoolean(false)
        val joinedResult = withContext(Dispatchers.IO) {
            val caller1 = async {
                try {
                    cacheService.refreshPlayerProfile(cancelPuuid, region, "CancelLeader", "EUW")
                } catch (e: CancellationException) {
                    caller1Cancelled.set(true)
                    throw e
                }
            }
            gateEntered.await()
            val caller2 = async {
                cacheService.refreshPlayerProfile(cancelPuuid, region, "CancelJoiner", "EUW")
            }
            delay(40)
            caller1.cancel()
            delay(40)
            releaseGate.complete(Unit)
            caller2.await()
        }
        riotBackend.onBeforeEndpoint = null
        caller1Cancelled.get() shouldBe true
        joinedResult?.puuid shouldBe cancelPuuid
        joinedResult?.totalMatches shouldBe 1
        limiter.getVerificationSnapshot(region).activeUserRequests shouldBe 0

        // 3. Sole waiter cancellation cancels underlying worker and cleans up Redis lock
        val soleCancelPuuid = "sole-cancel-puuid"
        val soleEntered = CompletableDeferred<Unit>()
        val soleHold = CompletableDeferred<Unit>()
        riotBackend.summonersByRegionPuuid["$region|$soleCancelPuuid"] = SummonerDto("s-sc", soleCancelPuuid, 111L, 3)
        riotBackend.matchIdsPagesGenerator[soleCancelPuuid] = { _, _ ->
            soleEntered.complete(Unit)
            kotlinx.coroutines.runBlocking { soleHold.await() }
            listOf("EUW1_1")
        }
        withContext(Dispatchers.IO) {
            val soleCaller = async {
                cacheService.refreshPlayerProfile(soleCancelPuuid, region, "SoleWaiter", "EUW")
            }
            soleEntered.await()
            soleCaller.cancel()
            soleHold.complete(Unit)
            val deadline = System.currentTimeMillis() + 2_000L
            while (System.currentTimeMillis() < deadline && redis.store.containsKey("lock:profile:euw1:$soleCancelPuuid")) {
                delay(20)
            }
        }
        redis.store.containsKey("lock:profile:euw1:$soleCancelPuuid") shouldBe false
        limiter.getVerificationSnapshot(region).activeUserRequests shouldBe 0
    }

    test("Anchored splice refuses partial ledgers, mid-pagination errors, non-contiguous known tails, and upstream missing IDs") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)
        val region = "euw1"
        val riotBackend = FakeRiotHttpBackend(mapper)
        val redis = InMemoryLatencyRedisHarness(mapper)
        val limiter = RiotApiRateLimiter(clock)
        val riotService = RiotApiService(riotBackend.buildRestClient(), limiter, RestClient.create(), clock)
        val (seasonRepo, seasonMap) = inMemorySeasonRepo()
        val cacheService = PlayerCacheService(
            redisCacheService = redis.service,
            riotApiService = riotService,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = seasonRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper
        )

        // Seed 250 matches in FakeRiotHttpBackend + Redis so match hydration is fast
        val puuid = "anchor-guard-puuid"
        riotBackend.summonersByRegionPuuid["$region|$puuid"] = SummonerDto("s-ag", puuid, 333L, 12)
        riotBackend.ranksByRegionPuuid["$region|$puuid"] = emptyList()
        for (idx in 1..255) {
            val m = buildSeasonalMatch(puuid, idx, baseNowMs)
            riotBackend.matchesById["EUW1_$idx"] = m
            redis.service.set("match:euw1:EUW1_$idx", m, Duration.ofDays(60))
        }

        // Case A: Page-1 error on initial cold load produces PARTIAL ledger (100 of 250 games, historyComplete=false).
        // Next refresh MUST NOT anchor on the 100-game partial ledger — it must paginate all 3 pages and recover all 250 games!
        var failPage1 = true
        val full250 = (250 downTo 1).map { "EUW1_$it" }
        riotBackend.matchIdsPagesGenerator[puuid] = { start, count ->
            if (failPage1 && start == 100) null
            else if (start >= full250.size) emptyList()
            else full250.subList(start, minOf(start + count, full250.size))
        }

        val partialProfile = cacheService.getPlayerProfile(puuid, region, "GuardPlayer", "EUW", explicitRefresh = true)
        partialProfile?.totalMatches shouldBe 100
        val partialDurable = mapper.readValue(seasonMap["euw1|$puuid|2026|420"]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)
        partialDurable.historyComplete shouldBe false

        // Now Page 1 recovers: refresh MUST NOT anchor on partialDurable (must fetch all 3 pages and reach 250 games, historyComplete=true)
        failPage1 = false
        clock.advanceBy(120_000L)
        riotBackend.resetCounters()
        val recoveredProfile = cacheService.getPlayerProfile(puuid, region, "GuardPlayer", "EUW", explicitRefresh = true)
        recoveredProfile?.totalMatches shouldBe 250
        riotBackend.matchIdsPageCalls.get() shouldBe 3
        val completeDurable = mapper.readValue(seasonMap["euw1|$puuid|2026|420"]!!.stateJson, DurablePlayerSeasonStatsDto::class.java)
        completeDurable.historyComplete shouldBe true

        // Case B: Non-contiguous known IDs in Page 0's 20-ID tail (EUW1_260 delayed from Page 0 onto Page 1 across a 350-ID / 4-page season)
        // -> Page 0 MUST refuse early splice due to index jump over EUW1_260, and Page 1 delivers EUW1_260 + consecutive tail to bridge in 2 (not 4) pages!
        val full350 = (350 downTo 1).map { "EUW1_$it" }
        val anchor350 = RiotApiService.MatchIdsIncrementalAnchor(
            knownOrderedSeasonalIds = full350,
            knownExcludedIds = emptySet(),
            priorHistoryCapped = false
        )
        val reordered352 = buildList {
            addAll((352 downTo 261).map { "EUW1_$it" }) // 92 IDs
            addAll((259 downTo 252).map { "EUW1_$it" }) // 8 IDs -> Page 0 ends at 100 IDs with jump 261 -> 259 (skipping 260!)
            add("EUW1_260")                             // Delivered at start of Page 1!
            addAll((251 downTo 1).map { "EUW1_$it" })   // Remaining consecutive tail
        }
        riotBackend.resetCounters()
        riotBackend.matchIdsPagesGenerator[puuid] = { start, count ->
            if (start >= reordered352.size) emptyList() else reordered352.subList(start, minOf(start + count, reordered352.size))
        }
        val nonContigOutcome = withContext(RiotApiService.MatchIdsIncrementalAnchorElement(anchor350)) {
            riotService.fetchMatchIdsWithOutcomeSince(puuid, region, 0L)
        }
        nonContigOutcome.pagesFetched shouldBe 2 // Refused early splice on page 1 due to gap, bridged cleanly on page 2 (instead of 4)!
        nonContigOutcome.matchIds.contains("EUW1_260") shouldBe true
        nonContigOutcome.matchIds.distinct().size shouldBe 352

        // Case C: Upstream missing newest known ID (EUW1_250 missing from upstream completely while Page 0 ends with consecutive EUW1_170..EUW1_151):
        // prefixComplete MUST be false so Page 0 does NOT splice early and falsely claim EUW1_250 was covered!
        val anchor250 = RiotApiService.MatchIdsIncrementalAnchor(
            knownOrderedSeasonalIds = full250,
            knownExcludedIds = emptySet(),
            priorHistoryCapped = false
        )
        riotBackend.resetCounters()
        val upstreamMissing250 = ((251 downTo 1).filter { it != 250 }).map { "EUW1_$it" } // 250 total IDs, missing EUW1_250
        riotBackend.matchIdsPagesGenerator[puuid] = { start, count ->
            if (start >= upstreamMissing250.size) emptyList()
            else upstreamMissing250.subList(start, minOf(start + count, upstreamMissing250.size))
        }
        val missingUpstreamOutcome = withContext(RiotApiService.MatchIdsIncrementalAnchorElement(anchor250)) {
            riotService.fetchMatchIdsWithOutcomeSince(puuid, region, 0L)
        }
        missingUpstreamOutcome.pagesFetched shouldBe 3 // Did NOT stop on page 1; paginated all upstream pages!
        missingUpstreamOutcome.matchIds.contains("EUW1_250") shouldBe false // Did NOT falsely splice missing EUW1_250!
    }
})
