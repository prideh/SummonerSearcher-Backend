# Dynamic Sitemap Generation Plan

To drastically improve discoverability of summoner profiles, the application needs a dynamic XML sitemap so that search engines can easily discover and index the millions of possible `/profile/:region/:riotId` combinations.

## Backend Kotlin API Changes

### 1. New Sitemap Controller
Create a new `SitemapController` that will expose a `GET /api/sitemap.xml` endpoint. This endpoint will return an `application/xml` response.

### 2. Fetch Popular/Recent Profiles
Since League of Legends has too many players to include in a single sitemap (which is limited to 50,000 URLs), we need a strategy to surface the most important ones:
- **Leaderboards (Challenger/Grandmaster/Master)**: Fetch the top 1000-5000 players across all major regions from the database or Riot API caches. These are high-value pages.
- **Recently Searched**: Pull the 10,000 most recently queried summoners from the application database.
- **High-Activity Players**: Track players who are frequently searched and add them to a priority indexing table.

### 3. XML Generation
Use a library like `jackson-dataformat-xml` or simply build the XML string using Kotlin string templates.
The format must follow the core Sitemap protocol:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
   <url>
      <loc>https://summonersearcher.com/profile/euw1/pride-persi</loc>
      <lastmod>2023-10-01</lastmod>
      <changefreq>daily</changefreq>
      <priority>0.8</priority>
   </url>
   <!-- ... -->
</urlset>
```

### 4. Caching
Sitemap generation can be database-heavy. The backend MUST cache the generated XML sitemap for at least 12-24 hours. Consider using Spring `@Cacheable` or a background scheduled job (`@Scheduled`) that periodically writes the sitemap to memory or a static file, and the controller simply serves that file.

### 5. Sitemap Indexes (Future-proofing)
If the sitemap grows beyond 50,000 URLs, implement a Sitemap Index file (`sitemap-index.xml`) that points to regional sitemaps like `sitemap-euw1.xml`, `sitemap-na1.xml`, etc.
