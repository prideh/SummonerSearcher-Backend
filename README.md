🏗️ Technical Architecture

The backend is a Kotlin + Spring Boot 3.5 service structured around classic MVC architecture, supported by additional enterprise patterns such as Singleton Beans, DTO Mapping, Filter Chains, and Repository Patterns via Spring Data JPA.

The system is split into three primary layers:

1. Controller Layer (Presentation Layer — MVC “C”)

Controllers expose REST endpoints under:

/api/auth

/api/user

/api/riot

Each controller is intentionally thin and delegates business logic to service classes.
Custom annotation @CurrentUser integrates with Spring Security's @AuthenticationPrincipal to inject the authenticated User entity directly into controller methods.

Key Patterns:

Annotation-driven dependency injection

Request/Response DTO pattern

Thin controller, fat service philosophy

Coroutine-based async endpoints (runBlocking) for Riot API calls

2. Service Layer (Business Logic — MVC “M”)

Service classes encapsulate core business rules such as:

Two-Factor Authentication orchestration

Summoner profile assembly from multiple Riot API endpoints

Challenger leaderboard caching & invalidation

User preference & recent-search management

Secure email-based flows (verification, password reset)

Key Components & Patterns:

Singleton Beans (@Bean) for:

RestClient (Riot API client)

GoogleAuthenticator

Password encoder

Strategy-like operations (e.g., different verification flows: login, 2FA, reset-token validation)

Composition over inheritance — services orchestrate multiple external or internal calls to build final DTOs

Async Capabilities:

@EnableAsync for background tasks (e.g., sending emails)

@EnableScheduling for periodic leaderboard refresh jobs

3. Repository Layer

Built on Spring Data JPA, the Repository layer provides:

User lookup by email, verification token, password reset token

Persistence for recent search history

Transparent transaction boundaries via @Transactional

Key Patterns:

Repository pattern

JPA Entity + Embedded Relationship structure

Lazy-loading of user search history via ORM mapping

4. Security Architecture

Security is implemented via a custom chain in SecurityConfig:

Components:

JWT Authentication Filter (custom filter in front of Spring’s default auth)

CustomAccessDeniedHandler

CustomAuthenticationEntryPoint

Stateless Session Management

URL-level Authorization Rules

JWT authentication is fully stateless; user identity is determined per request through the filter.

Design Patterns:

Filter Chain pattern

Token-based authentication

Custom principal resolver via @CurrentUser

5. External Integrations
Riot Games API

Provided through a dedicated singleton RestClient with global header injection:

Automatic X-Riot-Token insertion

Response interceptors for rate-limit logging

Redis Caching

Used by challenger leaderboard and Summoner Profile caching logic (via dedicated DTO wrappers).

Email (SendGrid)

Used for:

Verification email flow

Password reset flow

🔄 How It Works

This section describes the high-level data flow between frontend → backend → external APIs.

1. Authentication Flow
Login Without 2FA

User posts { email, password } → /api/auth/login

Spring AuthenticationManager validates credentials via UserDetailsService

If success:

Backend issues a JWT

Returns AuthResponse containing:

JWT

Dark mode preference

Recent searches (entity list)

JWT is stored client-side and attached as Authorization: Bearer <token>

Login With 2FA

User posts login credentials.

Backend validates credentials → generates temp JWT.

Returns { twoFactorRequired: true, tempToken: ... }.

User submits a 2FA code via /api/auth/2fa-login.

Temp JWT validated → 2FA code validated via TOTP (GoogleAuthenticator).

Issue final JWT.

Under the hood:

Secrets are encrypted before persistence using EncryptionService (AES).

Codes are validated with a configurable time window (to mitigate drift).

2. Riot ID → Summoner Search Flow
Request:

GET /api/riot/summoner/{region}/{name}/{tagline}

Internal Steps:

Account API lookup
Convert Riot ID → permanent PUUID.

Summoner API lookup
Fetch persistent player metadata (level, icon).

League API lookup
Fetch Solo Queue rank.

Match API lookup (batch)
Fetch and deserialize full match history (MatchDto structure).

DTO Assembly
SummonerProfileService composes all pieces into a clean SummonerProfileDto.

Caching Logic
Summaries (and sometimes leaderboard data) are checked against Redis:

If cached: return cached result

If stale/missing: fetch fresh data & update cache

Recent Search Update
The user’s search query is persisted via UserService.addRecentSearch.

Complex Logic Involved:

Multi-step, multi-endpoint API orchestration

Resilience to Riot API changes (via @JsonIgnoreProperties)

Heavy DTO → domain → DTO transformations

Non-blocking HTTP client via Spring RestClient

Coroutines (runBlocking) for controlled concurrency

3. Challenger Leaderboard Flow

A scheduled background job (via @EnableScheduling) periodically refreshes leaderboard data:

Fetches LeagueListDTO for a region/queue

Wraps it into CachedLeaderboardDto

Writes it to Redis

User request simply returns cached leaderboard

Benefits:

Extremely low-latency reads

No load on Riot API during peak frontend use

Controlled refresh windows

4. User Settings Flow (Dark Mode, Recent Searches, Password Changes)

All /api/user/** routes require authentication.

Dark Mode

Simple boolean flag stored in the User entity.

Recent Searches

Stored as a list of embedded objects.

Capped or trimmed by service logic (if implemented in UserService).

Account Modification

Change password, delete account, enable/disable 2FA

Most endpoints guarded by:

Password check

Dummy-account protection

2FA-validation when disabling 2FA

⚙️ Summary of Core Design Patterns Used
Pattern	Where Used	Purpose
MVC	Controllers → Services → Repositories	Clear separation of concerns
Singleton Beans	API client, authenticator, encoders	Shared global services
Filter Chain Pattern	JWTAuthenticationFilter	Token-based security
Repository Pattern	UserRepository	Abstract persistence
DTO Pattern	All API responses	Clean separation between API and domain model
Strategy-like orchestration	Two-factor flows, login flows	Encapsulates different authentication paths
Cache-aside pattern	Challenger leaderboard & summoner profile	Low-latency data access
