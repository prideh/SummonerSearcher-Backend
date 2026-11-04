package com.pride.summoner_searcher_api.controller

import com.pride.summoner_searcher_api.repository.IndexedPlayerRepository
import com.pride.summoner_searcher_api.service.RedisCacheService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@RestController
@RequestMapping("/api")
class SitemapController(
    private val indexedPlayerRepository: IndexedPlayerRepository,
    private val redisCacheService: RedisCacheService,
    @Value("\${seo.base-url:https://summonersearcher.com}") private val baseUrl: String
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SITEMAP_CACHE_KEY = "seo:sitemap:xml"
        private val SITEMAP_TTL: Duration = Duration.ofHours(12)
        private val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC)
    }

    @GetMapping("/sitemap.xml", produces = [MediaType.APPLICATION_XML_VALUE])
    fun getSitemap(): ResponseEntity<String> {
        val cachedXml = redisCacheService.get(SITEMAP_CACHE_KEY, String::class.java)
        if (!cachedXml.isNullOrBlank()) {
            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_XML)
                .body(cachedXml)
        }

        val normalizedBase = baseUrl.trimEnd('/')
        val players = try {
            indexedPlayerRepository.findTop5000ByOrderByLastSeenAtDesc()
        } catch (e: Exception) {
            logger.error("Failed to query indexed players for sitemap", e)
            emptyList()
        }

        val sb = StringBuilder(players.size * 160 + 512)
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        sb.append("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")

        // Core landing page
        sb.append("  <url>\n")
        sb.append("    <loc>").append(escapeXml("$normalizedBase/")).append("</loc>\n")
        sb.append("    <changefreq>daily</changefreq>\n")
        sb.append("    <priority>1.0</priority>\n")
        sb.append("  </url>\n")

        for (player in players) {
            val regionSegment = encodePathSegment(player.region.lowercase())
            val riotIdSegment = encodePathSegment("${player.gameName}-${player.tagLine}")
            val loc = "$normalizedBase/profile/$regionSegment/$riotIdSegment"
            val lastMod = DATE_FORMATTER.format(player.lastSeenAt)

            sb.append("  <url>\n")
            sb.append("    <loc>").append(escapeXml(loc)).append("</loc>\n")
            sb.append("    <lastmod>").append(lastMod).append("</lastmod>\n")
            sb.append("    <changefreq>weekly</changefreq>\n")
            sb.append("    <priority>0.7</priority>\n")
            sb.append("  </url>\n")
        }

        sb.append("</urlset>\n")
        val xml = sb.toString()

        redisCacheService.set(SITEMAP_CACHE_KEY, xml, SITEMAP_TTL)

        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_XML)
            .body(xml)
    }

    private fun encodePathSegment(value: String): String =
        URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")

    private fun escapeXml(value: String): String =
        value
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
}
