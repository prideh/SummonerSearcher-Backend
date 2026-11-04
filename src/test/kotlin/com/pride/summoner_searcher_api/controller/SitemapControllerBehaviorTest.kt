package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.model.IndexedPlayer
import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import com.pride.summoner_searcher_api.service.RedisCacheService
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.http.HttpStatus
import java.time.Instant

class SitemapControllerBehaviorTest : BehaviorSpec({
    val indexedPlayerRepository = mockk<IndexedPlayerRepository>(relaxed = true)
    val redisCacheService = mockk<RedisCacheService>(relaxed = true)
    val controller = SitemapController(
        indexedPlayerRepository = indexedPlayerRepository,
        redisCacheService = redisCacheService,
        baseUrl = "https://summonersearcher.com"
    )

    beforeTest {
        clearMocks(indexedPlayerRepository, redisCacheService, answers = false)
    }

    Given("The dynamic /api/sitemap.xml endpoint (#9)") {
        When("Redis has no cached sitemap XML") {
            every { redisCacheService.get("seo:sitemap:xml", String::class.java) } returns null
            every { indexedPlayerRepository.findTop5000ByOrderByLastSeenAtDesc() } returns listOf(
                IndexedPlayer(
                    puuid = "puuid-1",
                    gameName = "Faker",
                    tagLine = "KR1",
                    region = "kr",
                    profileIconId = 1,
                    summonerLevel = 850L,
                    lastSeenAt = Instant.parse("2026-11-04T08:00:00Z")
                ),
                IndexedPlayer(
                    puuid = "puuid-2",
                    gameName = "Hide on bush",
                    tagLine = "EU&W",
                    region = "euw1",
                    profileIconId = 2,
                    summonerLevel = 420L,
                    lastSeenAt = Instant.parse("2026-11-03T12:00:00Z")
                )
            )

            val response = controller.getSitemap()

            Then("it generates valid sitemap XML with URL-encoded profile paths and caches it in Redis") {
                response.statusCode shouldBe HttpStatus.OK
                val body = response.body!!
                body shouldContain "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                body shouldContain "<loc>https://summonersearcher.com/</loc>"
                body shouldContain "<loc>https://summonersearcher.com/profile/kr/Faker-KR1</loc>"
                body shouldContain "<loc>https://summonersearcher.com/profile/euw1/Hide%20on%20bush-EU%26W</loc>"
                body shouldContain "<lastmod>2026-11-04</lastmod>"
                verify(exactly = 1) { redisCacheService.set("seo:sitemap:xml", any<String>(), any()) }
            }
        }

        When("Redis already has a cached sitemap XML") {
            val cachedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><urlset></urlset>"
            every { redisCacheService.get("seo:sitemap:xml", String::class.java) } returns cachedXml

            val response = controller.getSitemap()

            Then("it serves the cached XML without querying PostgreSQL") {
                response.statusCode shouldBe HttpStatus.OK
                response.body shouldBe cachedXml
                verify(exactly = 0) { indexedPlayerRepository.findTop5000ByOrderByLastSeenAtDesc() }
            }
        }
    }
})
