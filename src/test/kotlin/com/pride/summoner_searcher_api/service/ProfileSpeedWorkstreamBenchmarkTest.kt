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

    val healthyContainerMemoryStats = object : MemoryStatsProvider {
        override fun sample(): RawMemorySample = RawMemorySample(
            jvmHeapUsedBytes = 96L * 1024L * 1024L,
            jvmHeapCommittedBytes = 192L * 1024L * 1024L,
            jvmHeapMaxBytes = 384L * 1024L * 1024L,
            cgroupMemoryCurrentBytes = 256L * 1024L * 1024L,
            cgroupMemoryMaxBytes = 512L * 1024L * 1024L
        )
    }

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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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
                objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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

        val coldPipelineTelemetry = branchRedis.service.getPipelineWriteTelemetry()
        coldAfterRiotCalls shouldBe 458
        coldBeforeRiotCalls shouldBe 458
        coldAfterPeakConcurrency shouldBeLessThanOrEqualTo 20
        coldAfterPipelineBatches shouldBe 18 // 9 chunks of 50 matches split into 18 bounded <=25-entry / <=256KiB pipeline sub-batches (vs 450 single SETs)
        coldAfterSingleSets shouldBe 1 // Only 1 single SET for player:profile
        coldBeforeSingleSets shouldBe 451
        telemetryAfterCold["lastRefreshPeakLiveMatchDtos"]!! shouldBeLessThanOrEqualTo 70L
        telemetryAfterCold["lastRefreshPeakCrossChunkRetainedMatchDtos"]!! shouldBeLessThanOrEqualTo 20L
        telemetryAfterCold["lastRefreshPeakLiveMatchJsonBytes"]!! shouldBeLessThanOrEqualTo (1024L * 1024L)
        (telemetryAfterCold["lastRefreshPeakLiveMatchJsonBytes"]!! > 0L) shouldBe true
        telemetryAfterCold["verifiedRedisSeasonsEntries"] shouldBe 0L
        telemetryAfterCold["verifiedRedisSeasonsEstimatedRetainedBytes"]!! shouldBeLessThanOrEqualTo 128L
        coldPipelineTelemetry.maxObservedSubBatchBytes shouldBeLessThanOrEqualTo (256L * 1024L)
        (coldPipelineTelemetry.maxObservedSubBatchBytes > 0L) shouldBe true
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
                objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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
              Cold 450g (patches 16.1..16.22): before=${coldBeforeMs}ms (riotCalls=$coldBeforeRiotCalls, redisRtts=$coldBeforeRedisRtts, singleSets=$coldBeforeSingleSets) -> after=${coldAfterMs}ms (riotCalls=$coldAfterRiotCalls, redisRtts=$coldAfterRedisRtts, pipelineSubBatches=$coldAfterPipelineBatches, peakConcurrentMatchFetches=$coldAfterPeakConcurrency, peakLiveDtos=${telemetryAfterCold["lastRefreshPeakLiveMatchDtos"]}, peakLiveMatchJsonBytes=${telemetryAfterCold["lastRefreshPeakLiveMatchJsonBytes"]}, peakPipelineSubBatchBytes=${coldPipelineTelemetry.maxObservedSubBatchBytes})
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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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
        val cancelMatchId = "CANCEL_1"
        val gateEntered = CompletableDeferred<Unit>()
        val releaseGate = CompletableDeferred<Unit>()
        riotBackend.summonersByRegionPuuid["$region|$cancelPuuid"] = SummonerDto("s-c", cancelPuuid, 220L, 6)
        riotBackend.ranksByRegionPuuid["$region|$cancelPuuid"] = emptyList()
        riotBackend.matchesById[cancelMatchId] = buildSeasonalMatch(cancelPuuid, 1, baseNowMs, prefix = "CANCEL")
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
            soleCaller.join()
            val deadline = System.currentTimeMillis() + 2_000L
            while (System.currentTimeMillis() < deadline &&
                (redis.store.containsKey("lock:profile:euw1:$soleCancelPuuid") ||
                    limiter.getVerificationSnapshot(region).activeUserRequests > 0)
            ) {
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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
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

    test("Strict atomic 512 Riot-ID cap & 64 compact Redis-only season marker cap under concurrent admissions, retained-byte bounds, and 30m owner identity-age SWR contract") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)
        val region = "euw1"
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
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
        )
        val profileService = SummonerProfileService(riotService, cacheService)

        // 1. Multi-thread concurrent Riot-ID admissions (64 coroutines x 32 = 2,048 admissions) MUST never exceed 512 at any instant
        val maxObservedRiotEntries = AtomicLong(0L)
        val stopSampling = AtomicBoolean(false)
        withContext(Dispatchers.Default) {
            val sampler = async {
                while (!stopSampling.get()) {
                    val cur = cacheService.getInMemoryIndexTelemetry()["riotIdIndexEntries"] ?: 0L
                    maxObservedRiotEntries.accumulateAndGet(cur, ::maxOf)
                    Thread.yield()
                }
            }
            val workers = (1..64).map { workerIdx ->
                async {
                    for (itemIdx in 1..32) {
                        val idNum = (workerIdx - 1) * 32 + itemIdx
                        cacheService.recordVerifiedRiotIdOwner(
                            region = region,
                            queriedName = "ConcurrentUser$idNum",
                            queriedTag = "EUW",
                            canonicalName = "ConcurrentUser$idNum",
                            canonicalTag = "EUW",
                            verifiedPuuid = "concurrent-puuid-$idNum".padEnd(78, 'x')
                        )
                    }
                }
            }
            workers.map { it.await() }
            stopSampling.set(true)
            sampler.await()
        }
        val finalIndexTelemetry = cacheService.getInMemoryIndexTelemetry()
        maxObservedRiotEntries.get() shouldBeLessThanOrEqualTo 512L
        finalIndexTelemetry["riotIdIndexEntries"] shouldBe 512L
        finalIndexTelemetry["riotIdIndexEstimatedRetainedBytes"]!! shouldBeLessThanOrEqualTo (256L * 1024L) // <= 256 KiB at full 512 capacity
        finalIndexTelemetry["verifiedRedisSeasonsEntries"] shouldBe 0L // Durable DB mode retains 0 Redis-only markers

        // 2. Redis-only fallback mode (playerSeasonStatsRepository = null): concurrent 96-player refreshes + 2,100-ID capped season
        //    with 80 excluded matches -> strict atomic <= 64 cap and <= 320 KiB total retained bytes (vs ~85 MiB if orderedMatchIds were retained)
        val redisOnlyCache = PlayerCacheService(
            redisCacheService = redis.service,
            riotApiService = riotService,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = null,
            transactionTemplate = null,
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
        )
        for (i in 1..96) {
            val p = "ro-conc-puuid-$i".padEnd(78, 'p')
            riotBackend.summonersByRegionPuuid["$region|$p"] = SummonerDto("s-ro-$i", p, 100L + i, 1)
            riotBackend.ranksByRegionPuuid["$region|$p"] = emptyList()
            val mId = "ROCONC_${i}_1"
            val mDto = buildSeasonalMatch(p, 1, baseNowMs, prefix = "ROCONC_$i")
            riotBackend.matchesById[mId] = mDto
            redis.service.set("match:euw1:$mId", mDto, Duration.ofDays(60))
            riotBackend.matchIdsPagesGenerator[p] = { start, _ -> if (start == 0) listOf(mId) else emptyList() }
        }

        val maxObservedSeasonMarkers = AtomicLong(0L)
        val stopMarkerSampling = AtomicBoolean(false)
        withContext(Dispatchers.Default) {
            val markerSampler = async {
                while (!stopMarkerSampling.get()) {
                    val cur = redisOnlyCache.getInMemoryIndexTelemetry()["verifiedRedisSeasonsEntries"] ?: 0L
                    maxObservedSeasonMarkers.accumulateAndGet(cur, ::maxOf)
                    Thread.yield()
                }
            }
            val refreshWorkers = (1..16).map { w ->
                async {
                    for (k in 1..6) {
                        val idx = (w - 1) * 6 + k
                        val p = "ro-conc-puuid-$idx".padEnd(78, 'p')
                        redisOnlyCache.getPlayerProfile(p, region, "RoConc$idx", "EUW", explicitRefresh = true)
                    }
                }
            }
            refreshWorkers.map { it.await() }
            stopMarkerSampling.set(true)
            markerSampler.await()
        }
        maxObservedSeasonMarkers.get() shouldBeLessThanOrEqualTo 64L
        redisOnlyCache.getInMemoryIndexTelemetry()["verifiedRedisSeasonsEntries"] shouldBe 64L

        // Seed a 2,100-game capped Redis-only player + 80 Arena (queueId=1700) excluded matches to validate worst-case marker size & rolling-hash anchor
        val ro2100Puuid = "ro-2100-capped-puuid".padEnd(78, 'z')
        riotBackend.summonersByRegionPuuid["$region|$ro2100Puuid"] = SummonerDto("s-ro-2100", ro2100Puuid, 777L, 9)
        riotBackend.ranksByRegionPuuid["$region|$ro2100Puuid"] = emptyList()
        val ro2100Batch = LinkedHashMap<String, MatchDto>(60)
        for (idx in 1..2_102) {
            val m = buildSeasonalMatch(ro2100Puuid, idx, baseNowMs, prefix = "RO2100")
            riotBackend.matchesById["RO2100_$idx"] = m
            if (idx <= 2_100) {
                ro2100Batch["match:euw1:RO2100_$idx"] = m
                if (ro2100Batch.size == 50) {
                    redis.service.setAll(ro2100Batch, Duration.ofDays(60))
                    ro2100Batch.clear()
                }
            }
        }
        if (ro2100Batch.isNotEmpty()) {
            redis.service.setAll(ro2100Batch, Duration.ofDays(60))
            ro2100Batch.clear()
        }
        for (exIdx in 1..64) {
            val baseExcluded = buildSeasonalMatch(ro2100Puuid, exIdx, baseNowMs, prefix = "ROEX")
            val exDto = baseExcluded.copy(info = baseExcluded.info!!.copy(gameDuration = 0L))
            riotBackend.matchesById["ROEX_$exIdx"] = exDto
            redis.service.set("match:euw1:ROEX_$exIdx", exDto, Duration.ofDays(60))
        }
        var roMaxIdx = 1_900
        riotBackend.matchIdsPagesGenerator[ro2100Puuid] = { start, count ->
            val all = buildList {
                addAll((roMaxIdx downTo 1_701).map { "RO2100_$it" }) // 200+ consecutive seasonal IDs on Pages 0..1
                addAll((1..64).map { "ROEX_$it" })                   // 64 excluded remakes (exact MAX_REDIS_ONLY_EXCLUDED_IDS worst-case marker)
                addAll((1_700 downTo 1).map { "RO2100_$it" })        // Remaining seasonal tail (1,964 total IDs)
            }
            if (start >= all.size) emptyList() else all.subList(start, minOf(start + count, all.size))
        }
        val roInitial = redisOnlyCache.getPlayerProfile(ro2100Puuid, region, "Ro2100", "EUW", explicitRefresh = true)
        roInitial?.totalMatches shouldBe 1_900

        val roTelemetry = redisOnlyCache.getInMemoryIndexTelemetry()
        roTelemetry["verifiedRedisSeasonsEntries"] shouldBe 64L
        roTelemetry["verifiedRedisSeasonsEstimatedRetainedBytes"]!! shouldBeLessThanOrEqualTo (320L * 1024L) // <= 320 KiB worst-case!

        // Incremental +2 seasonal games on Redis-only player: compact marker + cachedProfile.allMatchIds bridges in 1 page!
        clock.advanceBy(120_000L)
        roMaxIdx = 1_902
        riotBackend.resetCounters()
        val roIncremental = redisOnlyCache.getPlayerProfile(ro2100Puuid, region, "Ro2100", "EUW", explicitRefresh = true)
        roIncremental?.totalMatches shouldBe 1_902
        riotBackend.matchIdsPageCalls.get() shouldBe 1

        // Interior tamper of cachedProfile.allMatchIds (same count, head, tail, but swapped interior elements) MUST fail rolling-hash check!
        val cachedRo = redis.service.get("player:profile:euw1:$ro2100Puuid", SummonerProfileDto::class.java)!!
        val tamperedIds = cachedRo.allMatchIds.toMutableList().apply {
            val tmp = this[500]
            this[500] = this[501]
            this[501] = tmp
        }
        redis.service.set("player:profile:euw1:$ro2100Puuid", cachedRo.copy(allMatchIds = tamperedIds), Duration.ofDays(14))
        clock.advanceBy(120_000L)
        riotBackend.resetCounters()
        val roAfterTamper = redisOnlyCache.getPlayerProfile(ro2100Puuid, region, "Ro2100", "EUW", explicitRefresh = true)
        roAfterTamper?.totalMatches shouldBe 1_902
        (riotBackend.matchIdsPageCalls.get() >= 20) shouldBe true // Refused tampered cachedProfile.allMatchIds and re-paginated!

        // 3. Identity age (`ownerVerifiedAtMs`) non-extension on hits/PUUID refreshes & 30m max unverified SWR cap
        val ownerClock = MutableClock(baseNowMs)
        val ownerLimiter = RiotApiRateLimiter(ownerClock)
        val ownerRiot = RiotApiService(riotBackend.buildRestClient(), ownerLimiter, RestClient.create(), ownerClock)
        val (ownerSeasonRepo, _) = inMemorySeasonRepo()
        val ownerCache = PlayerCacheService(
            redisCacheService = redis.service,
            riotApiService = ownerRiot,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = ownerClock,
            playerSeasonStatsRepository = ownerSeasonRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
        )
        val ownerProfileSvc = SummonerProfileService(ownerRiot, ownerCache)

        riotBackend.accountsByRiotId["europe|ownerswr|tag"] = AccountDto("puuid-owner-1", "OwnerSwr", "TAG")
        riotBackend.summonersByRegionPuuid["euw1|puuid-owner-1"] = SummonerDto("s-o1", "puuid-owner-1", 400L, 4)
        riotBackend.summonersByRegionPuuid["euw1|puuid-owner-2"] = SummonerDto("s-o2", "puuid-owner-2", 500L, 5)
        riotBackend.matchIdsPagesGenerator["puuid-owner-1"] = { start, _ -> if (start == 0) listOf("OWN1_1") else emptyList() }
        riotBackend.matchIdsPagesGenerator["puuid-owner-2"] = { start, _ -> if (start == 0) listOf("OWN2_1") else emptyList() }
        riotBackend.matchesById["OWN1_1"] = buildSeasonalMatch("puuid-owner-1", 1, baseNowMs, prefix = "OWN1")
        riotBackend.matchesById["OWN2_1"] = buildSeasonalMatch("puuid-owner-2", 1, baseNowMs, prefix = "OWN2")

        // t = 0: initial cold lookup authenticates OwnerSwr#TAG -> puuid-owner-1 via Account-V1
        val initialOwner = ownerProfileSvc.getSummonerProfile("euw1", "OwnerSwr", "TAG", explicitRefresh = false)
        initialOwner?.puuid shouldBe "puuid-owner-1"

        // Upstream re-assigns OwnerSwr#TAG -> puuid-owner-2 right after t = 0
        riotBackend.accountsByRiotId["europe|ownerswr|tag"] = AccountDto("puuid-owner-2", "OwnerSwr", "TAG")

        // Keep player:profile:euw1:puuid-owner-1 `lastRefreshed` fresh via PUUID-only refreshes (no Account-V1!)
        // and serve repeated warm Riot-ID hits at +4m, +12m, +20m, +28m: hits MUST NOT extend `ownerVerifiedAtMs`!
        for (step in 1..4) {
            ownerClock.advanceBy(4 * 60_000L) // +4m, +11m, +18m, +25m
            riotBackend.resetCounters()
            val hit = ownerProfileSvc.getSummonerProfile("euw1", "OwnerSwr", "TAG", explicitRefresh = false)
            hit?.puuid shouldBe "puuid-owner-1"
            riotBackend.accountByRiotIdCalls.get() shouldBe 0
            if (step < 4) {
                ownerClock.advanceBy(3 * 60_000L) // +7m, +14m, +21m: PUUID-only refresh keeps profile <5m fresh without Account-V1
                ownerCache.refreshPlayerProfile("puuid-owner-1", "euw1", "OwnerSwr", "TAG")
            }
        }
        // Keep profile fresh one more time at t = +28m (`lastRefreshed` = +28m), then advance to t = +31m (`profile` is 3m fresh,
        // NOT >5m stale, YET `ownerVerifiedAtMs` is 31m >= 30m old!): MUST force foreground Account-V1 re-authentication!
        ownerClock.advanceBy(3 * 60_000L) // t = +28m total
        ownerCache.refreshPlayerProfile("puuid-owner-1", "euw1", "OwnerSwr", "TAG")
        ownerClock.advanceBy(3 * 60_000L) // t = +31m total (>= 30m MAX_UNVERIFIED_OWNER_SWR_AGE)
        riotBackend.resetCounters()
        val reauthedOwner = ownerProfileSvc.getSummonerProfile("euw1", "OwnerSwr", "TAG", explicitRefresh = false)
        riotBackend.accountByRiotIdCalls.get() shouldBe 1 // Forced foreground Account-V1 re-authentication!
        reauthedOwner?.puuid shouldBe "puuid-owner-2"

        println(
            """
            [MEMORY_AND_IDENTITY_BOUND_SUMMARY]
              Riot-ID Index (512 concurrent cap): maxObserved=${maxObservedRiotEntries.get()}, finalEntries=${finalIndexTelemetry["riotIdIndexEntries"]}, estRetainedBytes=${finalIndexTelemetry["riotIdIndexEstimatedRetainedBytes"]}
              Redis-Only Season Markers (64 concurrent cap, 1900g+64ex): maxObserved=${maxObservedSeasonMarkers.get()}, finalEntries=${roTelemetry["verifiedRedisSeasonsEntries"]}, estRetainedBytes=${roTelemetry["verifiedRedisSeasonsEstimatedRetainedBytes"]} (0 bytes in DB mode)
              Identity Age Contract: hits/PUUID-refreshes do not extend ownerVerifiedAtMs; forced Account-V1 re-auth verified at >=30m
            """.trimIndent()
        )
    }

    test("Real StringRedisTemplate pipeline/sub-batch/TTL/fallback contract and real loopback TCP HTTP RestClient profile contract") {
        val baseNowMs = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)

        // 1. Real StringRedisTemplate wired to a wire-level RedisConnectionFactory + RedisConnection recording raw bytes, TTLs, and pipelines
        val rawRedisBytes = ConcurrentHashMap<String, ByteArray>()
        val recordedTtlMs = ConcurrentHashMap<String, Long>()
        val recordedSetOptions = ConcurrentHashMap<String, RedisStringCommands.SetOption>()
        val openPipelineCalls = AtomicInteger(0)
        val closePipelineCalls = AtomicInteger(0)
        val directNonPipelinedSets = AtomicInteger(0)
        val pipelineBatchSizes = java.util.concurrent.CopyOnWriteArrayList<Int>()
        val failPipeline = AtomicBoolean(false)

        val connectionFactory = object : RedisConnectionFactory {
            override fun getConnection(): RedisConnection {
                val inPipeline = AtomicBoolean(false)
                val queuedInBatch = AtomicInteger(0)
                lateinit var connProxy: Any
                val handler = java.lang.reflect.InvocationHandler { _, method, args ->
                    when (method.name) {
                        "stringCommands", "keyCommands" -> connProxy
                        "isPipelined" -> inPipeline.get()
                        "isQueueing", "isClosed" -> false
                        "close" -> null
                        "openPipeline" -> {
                            if (failPipeline.get()) {
                                throw org.springframework.data.redis.RedisSystemException(
                                    "ERR simulated pipeline failure",
                                    RuntimeException("simulated")
                                )
                            }
                            inPipeline.set(true)
                            queuedInBatch.set(0)
                            openPipelineCalls.incrementAndGet()
                            null
                        }
                        "closePipeline" -> {
                            inPipeline.set(false)
                            closePipelineCalls.incrementAndGet()
                            pipelineBatchSizes.add(queuedInBatch.get())
                            emptyList<Any>()
                        }
                        "set" -> {
                            val k = String(args[0] as ByteArray, StandardCharsets.UTF_8)
                            val v = (args[1] as ByteArray).copyOf()
                            val exp = args.getOrNull(2) as? Expiration
                            val opt = args.getOrNull(3) as? RedisStringCommands.SetOption
                            val ttl = if (exp == null || exp.isPersistent) -1L else exp.expirationTimeInMilliseconds
                            if (opt == RedisStringCommands.SetOption.SET_IF_ABSENT) {
                                val inserted = rawRedisBytes.putIfAbsent(k, v) == null
                                if (inserted) recordedTtlMs[k] = ttl
                                if (inPipeline.get()) null else inserted
                            } else {
                                rawRedisBytes[k] = v
                                recordedTtlMs[k] = ttl
                                if (opt != null) recordedSetOptions[k] = opt
                                if (inPipeline.get()) {
                                    queuedInBatch.incrementAndGet()
                                    null
                                } else {
                                    directNonPipelinedSets.incrementAndGet()
                                    true
                                }
                            }
                        }
                        "setEx" -> {
                            val k = String(args[0] as ByteArray, StandardCharsets.UTF_8)
                            val secs = args[1] as Long
                            val v = (args[2] as ByteArray).copyOf()
                            rawRedisBytes[k] = v
                            recordedTtlMs[k] = secs * 1_000L
                            directNonPipelinedSets.incrementAndGet()
                            if (inPipeline.get()) null else true
                        }
                        "pSetEx" -> {
                            val k = String(args[0] as ByteArray, StandardCharsets.UTF_8)
                            val ms = args[1] as Long
                            val v = (args[2] as ByteArray).copyOf()
                            rawRedisBytes[k] = v
                            recordedTtlMs[k] = ms
                            directNonPipelinedSets.incrementAndGet()
                            if (inPipeline.get()) null else true
                        }
                        "setNX" -> {
                            val k = String(args[0] as ByteArray, StandardCharsets.UTF_8)
                            val v = (args[1] as ByteArray).copyOf()
                            val inserted = rawRedisBytes.putIfAbsent(k, v) == null
                            if (inPipeline.get()) null else inserted
                        }
                        "get" -> {
                            val k = String(args[0] as ByteArray, StandardCharsets.UTF_8)
                            rawRedisBytes[k]?.copyOf()
                        }
                        "mGet" -> {
                            @Suppress("UNCHECKED_CAST")
                            val keysArr = args[0] as Array<ByteArray>
                            keysArr.map { rawRedisBytes[String(it, StandardCharsets.UTF_8)]?.copyOf() }
                        }
                        "del" -> {
                            @Suppress("UNCHECKED_CAST")
                            val keysArr = args[0] as Array<ByteArray>
                            keysArr.count { rawRedisBytes.remove(String(it, StandardCharsets.UTF_8)) != null }.toLong()
                        }
                        "toString" -> "RecordingRedisConnectionProxy"
                        "hashCode" -> System.identityHashCode(connProxy)
                        "equals" -> args?.getOrNull(0) === connProxy
                        else -> throw UnsupportedOperationException("Unexpected RedisConnection method: ${method.name}")
                    }
                }
                connProxy = java.lang.reflect.Proxy.newProxyInstance(
                    RedisConnection::class.java.classLoader,
                    arrayOf(
                        RedisConnection::class.java,
                        RedisStringCommands::class.java,
                        org.springframework.data.redis.connection.RedisKeyCommands::class.java
                    ),
                    handler
                )
                return connProxy as RedisConnection
            }

            override fun getConvertPipelineAndTxResults(): Boolean = true
            override fun getClusterConnection() = throw UnsupportedOperationException()
            override fun getSentinelConnection() = throw UnsupportedOperationException()
            override fun translateExceptionIfPossible(ex: RuntimeException) =
                ex as? org.springframework.dao.DataAccessException
                    ?: org.springframework.data.redis.RedisSystemException(ex.message ?: "redis error", ex)
        }

        val realTemplate = StringRedisTemplate(connectionFactory).apply { afterPropertiesSet() }
        val realRedisService = RedisCacheService(realTemplate, mapper)

        // A. 60-entry setAll contract: splits into 3 bounded pipeline flushes (25 + 25 + 10), GZIP+Base64 wire format, exact 60d TTL, UPSERT
        val sixtyMatches = LinkedHashMap<String, MatchDto>()
        for (idx in 1..60) {
            sixtyMatches["match:euw1:WIRE_$idx"] = buildSeasonalMatch("wire-puuid", idx, baseNowMs, prefix = "WIRE")
        }
        realRedisService.setAll(sixtyMatches, Duration.ofDays(60))

        openPipelineCalls.get() shouldBe 3
        closePipelineCalls.get() shouldBe 3
        pipelineBatchSizes.toList() shouldBe listOf(25, 25, 10)
        directNonPipelinedSets.get() shouldBe 0
        val wireTel = realRedisService.getPipelineWriteTelemetry()
        wireTel.pipelineFlushes shouldBe 3L
        wireTel.fallbackWrites shouldBe 0L
        wireTel.maxObservedSubBatchBytes shouldBeLessThanOrEqualTo (256L * 1024L)
        (wireTel.maxObservedSubBatchBytes > 0L) shouldBe true

        // Verify raw wire payload is "GZIP_B64:" + valid Base64 + GZIP magic bytes (0x1f, 0x8b) + exact 60-day TTL (5,184,000,000 ms)
        val rawSampleString = String(rawRedisBytes["match:euw1:WIRE_1"]!!, StandardCharsets.UTF_8)
        rawSampleString.startsWith("GZIP_B64:") shouldBe true
        val decodedCompressed = java.util.Base64.getDecoder().decode(rawSampleString.removePrefix("GZIP_B64:"))
        (decodedCompressed[0].toInt() and 0xFF) shouldBe 0x1F
        (decodedCompressed[1].toInt() and 0xFF) shouldBe 0x8B
        val inflatedJson = java.util.zip.GZIPInputStream(ByteArrayInputStream(decodedCompressed)).use {
            String(it.readBytes(), StandardCharsets.UTF_8)
        }
        inflatedJson.contains("\"matchId\":\"WIRE_1\"") shouldBe true
        recordedTtlMs["match:euw1:WIRE_1"] shouldBe Duration.ofDays(60).toMillis()
        recordedSetOptions["match:euw1:WIRE_1"] shouldBe RedisStringCommands.SetOption.UPSERT

        // Round-trip all 60 entries via real multiGet and single get
        val roundTripped = realRedisService.multiGet(sixtyMatches.keys.toList(), MatchDto::class.java)
        roundTripped.size shouldBe 60
        roundTripped["match:euw1:WIRE_60"]?.metadata?.matchId shouldBe "WIRE_60"
        realRedisService.get("match:euw1:WIRE_30", MatchDto::class.java)?.metadata?.matchId shouldBe "WIRE_30"

        // B. Byte-cap sub-batching contract: 8 incompressible ~50 KiB payloads (~400 KiB > 256 KiB cap) split into multiple flushes <= 256 KiB
        val rng = java.util.Random(42L)
        val incompressibleEntries = LinkedHashMap<String, Map<String, String>>()
        for (i in 1..8) {
            val randomBytes = ByteArray(38_000).also { rng.nextBytes(it) }
            incompressibleEntries["blob:test:$i"] = mapOf("payload" to java.util.Base64.getEncoder().encodeToString(randomBytes))
        }
        pipelineBatchSizes.clear()
        realRedisService.setAll(incompressibleEntries, Duration.ofMinutes(10))
        (pipelineBatchSizes.size >= 2) shouldBe true // Split strictly by 256 KiB byte cap even though 8 < 25 entries!
        realRedisService.getPipelineWriteTelemetry().lastCallPeakSubBatchBytes shouldBeLessThanOrEqualTo (256L * 1024L)

        // C. Pipeline failure fallback contract: when executePipelined throws, setAll falls back cleanly to per-key ValueOperations.set with exact TTL
        failPipeline.set(true)
        directNonPipelinedSets.set(0)
        val fallbackEntries = LinkedHashMap<String, MatchDto>()
        for (idx in 1..6) {
            fallbackEntries["match:euw1:FB_$idx"] = buildSeasonalMatch("wire-puuid", idx, baseNowMs, prefix = "FB")
        }
        realRedisService.setAll(fallbackEntries, Duration.ofDays(60))
        failPipeline.set(false)

        directNonPipelinedSets.get() shouldBe 6
        realRedisService.getPipelineWriteTelemetry().fallbackWrites shouldBe 6L
        recordedTtlMs["match:euw1:FB_6"] shouldBe Duration.ofDays(60).toMillis()
        realRedisService.multiGet(fallbackEntries.keys.toList(), MatchDto::class.java)["match:euw1:FB_6"]?.metadata?.matchId shouldBe "FB_6"

        // 2. Real loopback TCP HTTP server (`HttpServer` on 127.0.0.1:0) + real Spring `RestClient` + real `RedisCacheService` end-to-end profile flow
        val tcpPuuid = "tcp-http-puuid-001"
        val tcpHttpCalls = AtomicInteger(0)
        val tcpMatchIdsPageCalls = AtomicInteger(0)
        var tcpMaxIdx = 5
        val tcpMatches = ConcurrentHashMap<String, MatchDto>()
        for (idx in 1..7) {
            tcpMatches["EUW1_TCP_$idx"] = buildSeasonalMatch(tcpPuuid, idx, baseNowMs, prefix = "EUW1_TCP")
        }

        val httpServer = com.sun.net.httpserver.HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
        httpServer.createContext("/") { exchange ->
            tcpHttpCalls.incrementAndGet()
            val path = exchange.requestURI.path
            val query = exchange.requestURI.rawQuery ?: ""
            val bodyBytes: ByteArray? = when {
                path.startsWith("/riot/account/v1/accounts/by-riot-id/") ->
                    mapper.writeValueAsBytes(AccountDto(tcpPuuid, "TcpPlayer", "EUW"))
                path.startsWith("/lol/summoner/v4/summoners/by-puuid/") ->
                    mapper.writeValueAsBytes(SummonerDto("s-tcp", tcpPuuid, 350L, 21))
                path.startsWith("/lol/league/v4/entries/by-puuid/") ->
                    """[{"leagueId":"lg-1","queueType":"RANKED_SOLO_5x5","tier":"MASTER","rank":"I","puuid":"$tcpPuuid","summonerName":"TcpPlayer","leaguePoints":120,"wins":40,"losses":30,"veteran":false,"inactive":false,"freshBlood":false,"hotStreak":true}]"""
                        .toByteArray(StandardCharsets.UTF_8)
                path.contains("/lol/match/v5/matches/by-puuid/") && path.endsWith("/ids") -> {
                    tcpMatchIdsPageCalls.incrementAndGet()
                    val params = query.split("&").filter { it.contains("=") }.associate {
                        val (k, v) = it.split("=", limit = 2)
                        k to v
                    }
                    val start = params["start"]?.toIntOrNull() ?: 0
                    val count = params["count"]?.toIntOrNull() ?: 100
                    val all = (tcpMaxIdx downTo 1).map { "EUW1_TCP_$it" }
                    val slice = if (start >= all.size) emptyList() else all.subList(start, minOf(start + count, all.size))
                    mapper.writeValueAsBytes(slice)
                }
                path.startsWith("/lol/match/v5/matches/") -> {
                    val mId = path.substringAfterLast("/")
                    tcpMatches[mId]?.let { mapper.writeValueAsBytes(it) }
                }
                else -> null
            }
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.responseHeaders.add("X-App-Rate-Limit", "500:1,6000:120")
            exchange.responseHeaders.add("X-App-Rate-Limit-Count", "1:1,1:120")
            if (bodyBytes != null) {
                exchange.sendResponseHeaders(200, bodyBytes.size.toLong())
                exchange.responseBody.use { it.write(bodyBytes) }
            } else {
                val err = """{"status":{"status_code":404}}""".toByteArray(StandardCharsets.UTF_8)
                exchange.sendResponseHeaders(404, err.size.toLong())
                exchange.responseBody.use { it.write(err) }
            }
        }
        httpServer.start()
        try {
            val localPort = httpServer.address.port
            val underlyingFactory = org.springframework.http.client.SimpleClientHttpRequestFactory().apply {
                setProxy(java.net.Proxy.NO_PROXY)
                setConnectTimeout(Duration.ofSeconds(5))
                setReadTimeout(Duration.ofSeconds(5))
            }
            val loopbackRoutingFactory = ClientHttpRequestFactory { originalUri, httpMethod ->
                val rewrittenUri = URI(
                    "http",
                    null,
                    "127.0.0.1",
                    localPort,
                    originalUri.rawPath,
                    originalUri.rawQuery,
                    null
                )
                underlyingFactory.createRequest(rewrittenUri, httpMethod).apply {
                    headers.set("X-Original-Host", originalUri.host ?: "localhost")
                }
            }
            val realLoopbackRestClient = RestClient.builder().requestFactory(loopbackRoutingFactory).build()
            val tcpLimiter = RiotApiRateLimiter(clock)
            val tcpRiotService = RiotApiService(realLoopbackRestClient, tcpLimiter, RestClient.create(), clock)
            val (tcpSeasonRepo, _) = inMemorySeasonRepo()
            val tcpCacheService = PlayerCacheService(
                redisCacheService = realRedisService,
                riotApiService = tcpRiotService,
                indexedPlayerRepository = mockk(relaxed = true),
                timeProvider = clock,
                playerSeasonStatsRepository = tcpSeasonRepo,
                transactionTemplate = noOpTransactionTemplate(),
                objectMapper = mapper, memoryStatsProvider = healthyContainerMemoryStats
            )
            val tcpProfileService = SummonerProfileService(tcpRiotService, tcpCacheService)

            // Cold load over real loopback TCP HTTP + real StringRedisTemplate pipeline
            openPipelineCalls.set(0)
            val coldTcpProfile = tcpProfileService.getSummonerProfile("euw1", "TcpPlayer", "EUW", explicitRefresh = false)
            coldTcpProfile?.totalMatches shouldBe 5
            coldTcpProfile?.soloQueueRank?.tier shouldBe "MASTER"
            tcpHttpCalls.get() shouldBe 9 // 1 account + 1 summoner + 1 league + 1 matchIds + 5 matches over real TCP HTTP
            openPipelineCalls.get() shouldBe 1 // 5 matches written in 1 real Redis pipeline flush
            recordedTtlMs["match:euw1:EUW1_TCP_1"] shouldBe Duration.ofDays(60).toMillis()
            recordedTtlMs["player:profile:euw1:$tcpPuuid"] shouldBe Duration.ofDays(14).toMillis()

            // Warm <5m load: 0 real TCP HTTP calls
            clock.advanceBy(30_000L)
            tcpHttpCalls.set(0)
            val warmTcpProfile = tcpProfileService.getSummonerProfile("euw1", "TcpPlayer", "EUW", explicitRefresh = false)
            warmTcpProfile?.totalMatches shouldBe 5
            tcpHttpCalls.get() shouldBe 0

            // Stale >=5m (<30m) SWR load + +2 new matches: 0 foreground TCP HTTP calls, LOW background refresh reconciles to 7 matches
            clock.advanceBy(360_000L)
            tcpMaxIdx = 7
            tcpHttpCalls.set(0)
            tcpMatchIdsPageCalls.set(0)
            val staleTcpProfile = tcpProfileService.getSummonerProfile("euw1", "TcpPlayer", "EUW", explicitRefresh = false)
            staleTcpProfile?.totalMatches shouldBe 5
            withContext(Dispatchers.IO) {
                val deadline = System.currentTimeMillis() + 3_000L
                while (System.currentTimeMillis() < deadline &&
                    (realRedisService.get("player:profile:euw1:$tcpPuuid", SummonerProfileDto::class.java)?.totalMatches ?: 0) < 7
                ) {
                    delay(20)
                }
            }
            val refreshedAfterSwr = tcpProfileService.getSummonerProfile("euw1", "TcpPlayer", "EUW", explicitRefresh = false)
            refreshedAfterSwr?.totalMatches shouldBe 7
            tcpMatchIdsPageCalls.get() shouldBe 1 // Anchored incremental splice over real TCP HTTP!
        } finally {
            httpServer.stop(0)
        }
    }

    test("Plain-League player error/status copy never leaks internal field/enum/schema/cache names and preserves unknown vs zero") {
        val baseNowMs = Instant.parse("2026-08-18T12:00:00Z").toEpochMilli()
        val clock = MutableClock(baseNowMs)
        val handler = com.pride.summoner_searcher_api.handler.GlobalExceptionHandler()

        // 1. Error handler contract: internal JSON keys unchanged (`timestamp`, `status`, `error`, `message`),
        //    `failedUrl` and internal schema/variable/cache/upstream identifiers never leaked to players.
        val toxicUrl = "https://euw1.api.riotgames.com/lol/match/v5/matches/by-puuid/secret-puuid-999/ids?queue=420&start=0&count=100"
        val toxicReason = "DEADLINE_EXCEEDED: historyComplete=false pendingMatchIds=300 status_code=429 redisCacheService verifiedCompleteRedisOnlySeasons"
        val expectedCopyByCode = mapOf(
            404 to "Summoner Not Found",
            429 to "Match History Busy",
            503 to "League Servers Unavailable",
            500 to "Profile Unavailable"
        )
        val forbiddenLeakTokens = listOf(
            "failedUrl",
            "secret-puuid-999",
            "queue=420",
            "api.riotgames.com",
            "Riot API",
            "DEADLINE_EXCEEDED",
            "historyComplete",
            "historyCapped",
            "pendingMatchIds",
            "excludedMatchIds",
            "status_code",
            "redisCacheService",
            "verifiedCompleteRedisOnlySeasons",
            "ownerVerifiedAtMs"
        )

        for ((code, expectedTitle) in expectedCopyByCode) {
            val resp = handler.handleRiotApiStatusException(
                com.pride.summoner_searcher_api.exception.RiotApiStatusException(
                    statusCode = HttpStatus.valueOf(code),
                    reason = toxicReason,
                    url = toxicUrl
                )
            )
            resp.statusCode.value() shouldBe code
            val body = resp.body!!
            body.keys shouldBe setOf("timestamp", "status", "error", "message")
            body["status"] shouldBe code
            body["error"] shouldBe expectedTitle
            val json = mapper.writeValueAsString(body)
            for (token in forbiddenLeakTokens) {
                json.contains(token, ignoreCase = true) shouldBe false
            }
            (body["message"] as String).contains("_") shouldBe false
        }

        val timeoutResp = handler.handleResourceAccessException(
            org.springframework.web.client.ResourceAccessException("I/O error on GET request for \"$toxicUrl\": Read timed out; redisCacheService")
        )
        timeoutResp.statusCode shouldBe HttpStatus.GATEWAY_TIMEOUT
        val timeoutBody = timeoutResp.body!!
        timeoutBody.keys shouldBe setOf("timestamp", "status", "error", "message")
        timeoutBody["error"] shouldBe "Match History Timeout"
        val timeoutJson = mapper.writeValueAsString(timeoutBody)
        for (token in forbiddenLeakTokens) {
            timeoutJson.contains(token, ignoreCase = true) shouldBe false
        }

        // 2. Status-V4 contract: raw snake_case enums (`in_progress`, `scheduled`, `warning`, `critical`)
        //    map to plain League words while unknown `incident_severity` / `description` stay `null`.
        val statusFactory = ClientHttpRequestFactory { uri, httpMethod ->
            object : ClientHttpRequest {
                private val reqHeaders = HttpHeaders()
                private val reqBody = ByteArrayOutputStream()
                override fun getMethod(): HttpMethod = httpMethod
                override fun getURI(): URI = uri
                override fun getAttributes(): MutableMap<String, Any> = HashMap()
                override fun getHeaders(): HttpHeaders = reqHeaders
                override fun getBody(): OutputStream = reqBody
                override fun execute(): ClientHttpResponse {
                    val payload = """
                        {
                          "name": "EU West",
                          "locales": ["en_US"],
                          "maintenances": [
                            {
                              "maintenance_status": "in_progress",
                              "incident_severity": "warning",
                              "titles": [{"locale": "en_US", "content": "Ranked Queue Maintenance"}],
                              "updates": [{"id": 1, "author": "Riot", "translations": [{"locale": "en_US", "content": "Ranked Solo/Duo is temporarily offline for patch deploy."}]}],
                              "platforms": ["windows", "macos"]
                            },
                            {
                              "maintenance_status": "scheduled",
                              "incident_severity": null,
                              "titles": [],
                              "updates": [],
                              "platforms": ["windows"]
                            }
                          ],
                          "incidents": [
                            {
                              "active": true,
                              "incident_severity": "critical",
                              "titles": [],
                              "updates": [],
                              "platforms": ["windows"]
                            }
                          ]
                        }
                    """.trimIndent().toByteArray(StandardCharsets.UTF_8)
                    val respHeaders = HttpHeaders().apply {
                        contentType = MediaType.APPLICATION_JSON
                        set("X-App-Rate-Limit", "500:1,6000:120")
                        set("X-App-Rate-Limit-Count", "1:1,1:120")
                    }
                    return object : ClientHttpResponse {
                        override fun getStatusCode(): HttpStatus = HttpStatus.OK
                        override fun getStatusText(): String = "OK"
                        override fun close() {}
                        override fun getBody(): InputStream = ByteArrayInputStream(payload)
                        override fun getHeaders(): HttpHeaders = respHeaders
                    }
                }
            }
        }
        val statusRiotService = RiotApiService(
            RestClient.builder().requestFactory(statusFactory).build(),
            RiotApiRateLimiter(clock),
            RestClient.create(),
            clock
        )
        val platformStatus = statusRiotService.getPlatformData("euw1")!!
        platformStatus.maintenances.size shouldBe 2
        platformStatus.maintenances[0].status shouldBe "In Progress"
        platformStatus.maintenances[0].severity shouldBe "Warning"
        platformStatus.maintenances[0].title shouldBe "Ranked Queue Maintenance"
        platformStatus.maintenances[1].status shouldBe "Scheduled"
        platformStatus.maintenances[1].severity shouldBe null // Unknown preserved as null, never "null" or "UNKNOWN"
        platformStatus.maintenances[1].description shouldBe null // Unknown preserved as null
        platformStatus.maintenances[1].title shouldBe "League Server Maintenance"
        platformStatus.incidents[0].status shouldBe "Active"
        platformStatus.incidents[0].severity shouldBe "Critical"
        platformStatus.incidents[0].title shouldBe "League Server Notice"
        platformStatus.incidents[0].description shouldBe null
        val statusJson = mapper.writeValueAsString(platformStatus)
        statusJson.contains("in_progress") shouldBe false
        statusJson.contains("maintenance_status") shouldBe false
        statusJson.contains("incident_severity") shouldBe false
        statusJson.contains("No Title") shouldBe false

        // 3. Unknown (`null`) vs Zero (`0`) preservation on profile loading:
        //    3a. Cold Match-V5 page-0 failure with NO cache and either NO durable state OR an incomplete 0-game durable row
        //        MUST return `null` (unknown) rather than fabricating `0` matches (`0W 0L`).
        val unknownPuuid = "puuid-unknown-vs-zero-fail"
        val failBackend = FakeRiotHttpBackend(mapper)
        failBackend.accountsByRiotId["europe|unknownplayer|euw"] = AccountDto(unknownPuuid, "UnknownPlayer", "EUW")
        failBackend.summonersByRegionPuuid["euw1|$unknownPuuid"] = SummonerDto("s-unk", unknownPuuid, 220L, 12)
        failBackend.ranksByRegionPuuid["euw1|$unknownPuuid"] = emptyList()
        failBackend.matchIdsPagesGenerator[unknownPuuid] = { _, _ -> null } // Upstream 500 on page 0 -> unknown match history!

        val failRedis = InMemoryLatencyRedisHarness(mapper)
        val (failRepo, failMap) = inMemorySeasonRepo()
        val failRiot = RiotApiService(failBackend.buildRestClient(), RiotApiRateLimiter(clock), RestClient.create(), clock)
        val failCache = PlayerCacheService(
            redisCacheService = failRedis.service,
            riotApiService = failRiot,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = failRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper,
            memoryStatsProvider = healthyContainerMemoryStats
        )
        val failSvc = SummonerProfileService(failRiot, failCache)

        // Case 3a-i: durableState == null -> returns null (unknown, NOT 0 matches)
        val unknownResultNoDb = failSvc.getSummonerProfile("euw1", "UnknownPlayer", "EUW", explicitRefresh = false)
        unknownResultNoDb shouldBe null

        // Case 3a-ii: incomplete 0-game durable row (`historyComplete = false`, `processedMatchIds = emptyList()`) -> still returns null (unknown)
        val emptyAcc = MatchStatsAccumulator(unknownPuuid)
        val (emptySnapOverall, emptySnapChamp) = emptyAcc.toAdditiveSnapshots()
        val incompleteZeroDurable = DurablePlayerSeasonStatsDto(
            schemaVersion = DurablePlayerSeasonStatsDto.CURRENT_SCHEMA_VERSION,
            region = "euw1",
            puuid = unknownPuuid,
            seasonYear = 2026,
            queueId = 420,
            processedMatchIds = emptyList(),
            pendingMatchIds = listOf("EUW1_PENDING_1"),
            excludedMatchIds = emptyList(),
            hitPreSeasonBoundary = false,
            firstPreSeasonMatchId = null,
            historyCapped = false,
            historyComplete = false,
            overallCounters = emptySnapOverall,
            championCounters = emptySnapChamp,
            updatedAtEpochMilli = baseNowMs - 60_000L
        )
        failMap["euw1|$unknownPuuid|2026|420"] = PlayerSeasonStatsEntity(
            id = UUID.randomUUID(),
            region = "euw1",
            puuid = unknownPuuid,
            seasonYear = 2026,
            queueId = 420,
            schemaVersion = 1,
            foldedGamesCount = 0,
            pendingGamesCount = 1,
            historyCapped = false,
            historyComplete = false,
            stateJson = mapper.writeValueAsString(incompleteZeroDurable),
            updatedAt = Instant.ofEpochMilli(baseNowMs - 60_000L),
            version = 1L
        )
        val unknownResultIncompleteZeroDb = failSvc.getSummonerProfile("euw1", "UnknownPlayer", "EUW", explicitRefresh = true)
        unknownResultIncompleteZeroDb shouldBe null

        //    3b. Clean empty season (`matchIds = emptyList()`) with unknown `summonerLevel = null`, `profileIconId = null`, unranked (`ranks = emptyList()`)
        //        MUST return true `0` (`totalMatches == 0`, `wins == 0`, `losses == 0`) while preserving `null` for unknown fields.
        val zeroPuuid = "puuid-true-zero-season"
        val zeroBackend = FakeRiotHttpBackend(mapper)
        zeroBackend.accountsByRiotId["europe|zeroplayer|euw"] = AccountDto(zeroPuuid, "ZeroPlayer", "EUW")
        zeroBackend.summonersByRegionPuuid["euw1|$zeroPuuid"] = SummonerDto("s-zero", zeroPuuid, summonerLevel = null, profileIconId = null)
        zeroBackend.ranksByRegionPuuid["euw1|$zeroPuuid"] = emptyList()
        zeroBackend.matchIdsPagesGenerator[zeroPuuid] = { _, _ -> emptyList() }

        val zeroRedis = InMemoryLatencyRedisHarness(mapper)
        val (zeroRepo, _) = inMemorySeasonRepo()
        val zeroRiot = RiotApiService(zeroBackend.buildRestClient(), RiotApiRateLimiter(clock), RestClient.create(), clock)
        val zeroCache = PlayerCacheService(
            redisCacheService = zeroRedis.service,
            riotApiService = zeroRiot,
            indexedPlayerRepository = mockk(relaxed = true),
            timeProvider = clock,
            playerSeasonStatsRepository = zeroRepo,
            transactionTemplate = noOpTransactionTemplate(),
            objectMapper = mapper,
            memoryStatsProvider = healthyContainerMemoryStats
        )
        val zeroSvc = SummonerProfileService(zeroRiot, zeroCache)
        val trueZeroProfile = zeroSvc.getSummonerProfile("euw1", "ZeroPlayer", "EUW", explicitRefresh = false)!!
        trueZeroProfile.totalMatches shouldBe 0
        trueZeroProfile.allMatchIds shouldBe emptyList()
        trueZeroProfile.overallStats?.wins shouldBe 0
        trueZeroProfile.overallStats?.losses shouldBe 0
        trueZeroProfile.summonerLevel shouldBe null // Unknown preserved as null, never coerced to 0L
        trueZeroProfile.profileIconId shouldBe null // Unknown preserved as null, never coerced to 0
        trueZeroProfile.soloQueueRank shouldBe null // Unranked/unknown preserved as null

        val profileWireJson = mapper.writeValueAsString(trueZeroProfile)
        val internalLedgerIdentifiers = listOf(
            "historyComplete",
            "historyCapped",
            "pendingMatchIds",
            "excludedMatchIds",
            "ownerVerifiedAtMs",
            "verifiedRedisSeasons",
            "verifiedCompleteRedisOnlySeasons",
            "pipelineFlushes"
        )
        for (internalKey in internalLedgerIdentifiers) {
            profileWireJson.contains(internalKey) shouldBe false
        }
    }
})


