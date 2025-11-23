# 🛡️ Summoner Searcher Backend

[![Kotlin](https://img.shields.io/badge/Kotlin-7F52FF?style=for-the-badge&logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![Render](https://img.shields.io/badge/Render-46E3B7?style=for-the-badge&logo=render&logoColor=white)](https://render.com/)
  <a href="https://github.com/prideh/SummonerSearcher-Frontend">
    <img src="https://img.shields.io/badge/🔗_View_Frontend_Repo-React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB" alt="View Frontend Repo" />
  </a>


The backend is a **Kotlin + Spring Boot 3.5** service structured around classic **MVC architecture**, supported by enterprise patterns including Singleton Beans, Filter Chains, and Repository Patterns.

---

## 🏗️ Technical Architecture

The system is split into three primary layers, ensuring separation of concerns and maintainability.



### 1. Controller Layer (Presentation — MVC “C”)
Controllers expose REST endpoints and are intentionally "thin," delegating all logic to services.
* **Endpoints:** `/api/auth`, `/api/user`, `/api/riot`
* **Key Pattern:** Custom `@CurrentUser` annotation integrates with Spring Security to inject authenticated User entities directly into methods.
* **Async:** Uses coroutine-based (`runBlocking`) execution for external API calls.

### 2. Service Layer (Business Logic — MVC “M”)
Encapsulates core business rules, orchestration, and external API communication.
* **Orchestration:** Assembles Summoner Profiles from multiple Riot endpoints.
* **Security Logic:** Handles 2FA, password resets, and email verification.
* **Background Jobs:** Uses `@EnableScheduling` for periodic Leaderboard refreshes.
* **Patterns:** Singleton Beans (`@Bean`) for shared resources like the Riot `RestClient` and `GoogleAuthenticator`.

### 3. Repository Layer
Built on **Spring Data JPA** for transparent persistence.
* **Features:** User lookup (email/token), search history persistence.
* **Transactions:** Managed via `@Transactional` boundaries.
* **Optimization:** Lazy-loading of embedded relationships (e.g., search history).

---

## 🔐 Security Architecture

Security is implemented via a **stateless Filter Chain** in `SecurityConfig`.



### Components
* **JWT Authentication Filter:** Custom filter executing before Spring's default auth to validate tokens per request.
* **Stateless Session:** No server-side session storage; identity is derived from the JWT payload.
* **Custom Entry Points:** Specialized handling for Access Denied and Auth Errors.

### Design Patterns
* **Filter Chain Pattern**
* **Token-based Authentication**
* **Custom Principal Resolver**

---

## 🔄 Data Flow & Logic

### 1. Authentication Flow
#### Login (Standard)
1. User posts credentials to `/api/auth/login`.
2. Backend validates via `UserDetailsService`.
3. Returns `AuthResponse` (JWT + User Preferences).

#### Login (with 2FA)
1. User posts credentials → Backend returns `{ twoFactorRequired: true, tempToken }`.
2. User submits TOTP code + tempToken to `/api/auth/2fa-login`.
3. Backend validates code via `GoogleAuthenticator` → Issues final JWT.

### 2. Riot API Orchestration
**Request:** `GET /api/riot/summoner/{region}/{name}/{tagline}`

1. **Account Lookup:** Converts Riot ID to PUUID.
2. **Summoner & League Lookup:** Fetches level, icon, and Rank/LP.
3. **Match History (Batch):** Fetches and deserializes full match history (`MatchDto`).
4. **DTO Assembly:** Compiles all data into a clean `SummonerProfileDto`.
5. **Caching:** Checks Redis for existing summaries to minimize API calls.
6. **Persistence:** Saves the query to the user's "Recent Search" history.

### 3. Challenger Leaderboard (Background Job)
To prevent API rate-limiting during peak usage, leaderboards are not fetched in real-time.
* **Scheduled Job:** Fetches `LeagueListDTO` periodically.
* **Cache:** Writes data to Redis.
* **Read:** User requests are served instantly from the Redis cache.

---

## ⚙️ Core Design Patterns

| Pattern | Where Used | Purpose |
| :--- | :--- | :--- |
| **MVC** | Controllers → Services → Repositories | Clear separation of concerns. |
| **Singleton Beans** | API Client, Authenticator, Encoders | Efficient resource sharing. |
| **Filter Chain** | `JWTAuthenticationFilter` | Modular security processing. |
| **Repository** | `UserRepository` | Abstraction of data persistence. |
| **DTO Pattern** | All API Responses | Decoupling API contract from Database entities. |
| **Strategy** | 2FA / Login Flows | Encapsulating different verification algorithms. |
| **Cache-Aside** | Leaderboards / Profiles | High-performance data retrieval. |

---

## 🔌 External Integrations

* **Riot Games API:** Accessed via a singleton `RestClient` with automatic `X-Riot-Token` header injection and rate-limit handling.
* **Redis:** Caching layer for Leaderboards and Summoner Profiles.
* **SendGrid:** Email delivery service for verification and password resets.
