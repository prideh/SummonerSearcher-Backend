Here are the **Technical Architecture** and **How it Works** sections, tailored for a senior engineering audience and formatted for your `README.md`.

***

## 🏗 Technical Architecture

This application is built as a **Layered Monolith** using **Kotlin** and **Spring Boot 3.5**. It is designed to serve as a robust, fault-tolerant middleware between a frontend client and the Riot Games public API. The architecture emphasizes type safety, asynchronous concurrency, and strict resource management.



### Design Patterns & Principles

* **Controller-Service-Repository (MVC):** The core separation of concerns. Controllers (`/api/riot`, `/api/auth`) handle HTTP mapping, Services contain business logic and transaction boundaries, and Repositories manage PostgreSQL persistence.
* **Facade Pattern:**
    * `RiotApiService`: Acts as a facade, encapsulating the complexity of regional routing, error handling (404/429), and DTO mapping, providing a clean interface for the rest of the application.
    * `RedisCacheService`: A generic wrapper around `StringRedisTemplate` that abstracts JSON serialization/deserialization using Jackson, simplifying cache interactions.
* **Cache-Aside Strategy:** Implemented in `PlayerCacheService`. Data is requested from the cache first; on a miss, it is fetched from the source of truth (Riot API), stored in Redis, and then returned.
* **Strategy Pattern (Implicit):** The `RegionUtil` routes requests to different Riot regional endpoints (Americas, Europe, Asia) based on the input string, effectively switching strategies for API URL construction.
* **Decorator/Interceptor:**
    * `JwtAuthenticationFilter`: Intercepts requests to validate Stateless JWTs before they reach the security chain.
    * `RestClientConfig`: Uses a request interceptor to inject the `X-Riot-Token` header and log rate-limit headers globally.
* **Observer/Scheduler:** `ChallengerCacheWarmer` observes the system time via `@Scheduled` (Cron) to trigger background data refreshing tasks.

### Infrastructure & Persistence
* **Database:** PostgreSQL is used for persistent user data (Accounts, Preferences, 2FA Secrets).
* **Caching:** Redis (via Docker) is used for storing ephemeral, high-cost external API data (Summoner Profiles, Match History, Challenger Leaderboards).
* **Concurrency:** Kotlin Coroutines (`suspend` functions) and `kotlinx-coroutines-reactor` are used for non-blocking I/O, particularly within the Rate Limiter and asynchronous API aggregation.

---

## ⚙️ How it Works

### 1. Intelligent Riot API Integration
The core value of this backend is its ability to normalize and cache data from the Riot Games API while strictly adhering to rate limits.

#### **Dual-Layer Rate Limiter**
To prevent 429 (Too Many Requests) errors, the application implements a custom, thread-safe `RiotApiRateLimiter` using Kotlin Coroutines and Mutexes. It employs two distinct algorithms running in parallel:



1.  **Short-Term Pacing (Leaky Bucket):** Enforces a strict ~55ms interval between outgoing requests to prevent micro-bursts.
2.  **Long-Term throttling (Priority Token Bucket):** Manages the global quota (e.g., 100 requests/2 minutes).
    * **Priority Queuing:** The limiter distinguishes between `HIGH` priority (User real-time requests) and `LOW` priority (Cache warming). Low-priority tasks must leave a "buffer" of tokens available for user traffic, ensuring background tasks never degrade the user experience.

#### **Graceful Retries**
The `RiotApiService` wraps calls in a retry loop that detects `429` responses. If the rate limiter desyncs or a specific upstream shard is overloaded, it parses the `Retry-After` header and suspends the coroutine (non-blocking delay) before retrying.

### 2. Summoner Profile Data Flow
Resolving a "Riot ID" (Name + Tagline) to a playable profile involves a complex orchestration of calls to handle Riot's global account system vs. regional game servers.



1.  **Resolution:** `getAccountByRiotId` resolves `Name#Tag` to a global `PUUID` (Account-V1).
2.  **Validation:** The system verifies the `PUUID` exists on the requested specific region (Summoner-V4). This prevents returning data for a user who exists globally but doesn't play on the queried server.
3.  **Cache "Always-Check" Strategy:**
    * If the profile exists in Redis, the system performs a "light" check against the API for new matches (checking the `gameCreation` timestamp).
    * If new matches are found, they are appended to the cached list, and the TTL is reset. This creates a "Self-Healing" cache that keeps data fresh without blowing away the entire history.

### 3. Background Cache Warming
The `ChallengerCacheWarmer` runs on a scheduled CRON job (02:00 CET) to pre-fetch high-volume data (Top 300 players per region).
* It iterates through major regions (EUW, NA, KR).
* It utilizes `ApiPriority.LOW` to fetch data, ensuring that if a real user logs in during the warming phase, their requests take precedence over the background job.
* It utilizes `async/awaitAll` to enrich player data in parallel batches, significantly reducing the total execution time of the warming job.

### 4. Security & 2FA
* **Authentication:** Stateless JWT (JSON Web Tokens) signed with HMAC-SHA.
* **Two-Factor Authentication:** Implemented using TOTP (Time-Based One-Time Password).
    * Secrets are generated via `GoogleAuthenticator`.
    * **Encryption:** 2FA secrets are **not** stored in plain text. They are encrypted using `AES/GCM/NoPadding` (Galois/Counter Mode) before being saved to PostgreSQL. This ensures that even if the database is compromised, the 2FA seeds remain secure without the separate encryption key.
