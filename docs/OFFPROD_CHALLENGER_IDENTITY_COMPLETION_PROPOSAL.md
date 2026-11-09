# Off-Production Challenger Missing Identity Integration, Qualification & Minimal Execution Proposal

> [!IMPORTANT]
> **Branch, Commit Lineage & Zero-Effects Status**
> - **Repository**: `prideh/SummonerSearcher-Backend`
> - **Isolated Implementation Branch (pushed with `[skip preview] [skip render]`)**: `feat/offprod-missing-challenger-identity-completion`
> - **Reviewed Markdown Path in Repository**: `docs/OFFPROD_CHALLENGER_IDENTITY_COMPLETION_PROPOSAL.md`
> - **Base Candidate Commit (`5959a703`)**: `5959a7036c9e281f72d17f0a0bd5003ba59476f8` (PR #43 candidate, 100% unmodified)
> - **Production / Rollback Base Commit (`master`)**: `139a7de48e67af9d664c08fd61ce131b6e63bd74` (100% unmodified)
> - **Frontend Workspace (`SummonerSearcher-Frontend`)**: Untouched; owner UI and uncommitted work preserved.
> - **Zero Live Effects**: `0` live Riot API calls, `0` live database DDL/DML executions, `0` existing/read-only DB credentials used, `0` Render deploys/previews, `0` billing.

---

## 1. Fixes Applied Since `44e269a` / `8c56d06`

1. **Pre-DDL Validation of All CLI Flags, `ExecutionOptions`, `180s` Global Budget Cap & Target URLs (`0` SQL on Invalid Options)**:
   - [`OffProdChallengerIdentityCompletionCli.runCli`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionCli.kt) strictly validates all boolean/numeric flags, `ExecutionOptions` (`maxTotalHttpAttempts in 1..191`, `maxWallTimeMs in 1..180_000`, `connectTimeoutMs > 0`, `readTimeoutMs > 0`, `callDeadlineMs > 0`, `dbOperationTimeoutMs > 0`, `ddlTimeoutMs > 0`, `maxBodyBytes in 1..4096`), key coordination, API key presence, and target cluster URLs **before** creating any JDBC `DataSource` or executing `--apply-additive-ddl=true`.
   - Only after `validatedInitialOptions` succeeds does optional `--apply-additive-ddl=true` run, after which `validatedInitialOptions.copy(maxWallTimeMs = remainingWallTimeForRunMs)` is passed to `runBounded`.
   - Regression verified in [`OffProdChallengerIdentityPostgresQualificationTest.kt`](../src/test/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityPostgresQualificationTest.kt): 15 invalid option combinations combined with `--apply-additive-ddl=true --dry-run=false --allow-durable-writes=true` all return `exitCode = 2` with `ddlApplied = false`, **`0` JDBC `getConnection()` calls, `0` SQL statements executed**, and `to_regclass('public.player_riot_identities') == null`.
2. **Removed `DROP TABLE` Rollback Guidance (Data-Preserving Rollback Only)**:
   - [`src/main/resources/db/migration/V20261109__create_player_riot_identities.sql`](../src/main/resources/db/migration/V20261109__create_player_riot_identities.sql) removes `DROP TABLE` rollback guidance. Rollback is strictly **application-only** (reverting the service to `139a7de48e67af9d664c08fd61ce131b6e63bd74` or `5959a7036c9e281f72d17f0a0bd5003ba59476f8` while leaving `player_riot_identities` and all verified rows intact).
3. **Enforced `191` Absent-Manifest Scope (`0` `indexed_players` Updates; Immediate Stop on Partial `indexed_players` Row)**:
   - [`OffProdChallengerIdentityCompletionTool.ExecutionOptions`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionTool.kt) (`enforceAbsentManifestScope = true` default), [`JdbcIdentityCompletionPersistenceAdapter`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionTool.kt) (`allowIndexedPlayersBlankUpdate = false` default), and [`OffProdChallengerIdentityCompletionCli`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionCli.kt) (`--allow-indexed-players-blank-update=false` default) strictly scope writes to `player_riot_identities` only.
   - If the pre-call DB recheck (in either `--dry-run=true` or `--dry-run=false` mode) detects **any** manifest PUUID now present in `indexed_players` with a blank `game_name` or `tag_line` (`PreCallDbState.PRESENT_IN_INDEXED_PLAYERS_WITH_BLANK_FIELD`), execution **stops immediately** (`exitCode = 1`, `outcome = ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT`, `httpAttemptsMade = 0`, `durableIndexedPlayerUpdatesVerifiedCount = 0`) and never executes `UPDATE indexed_players`.
4. **Explicitly Separated DDL Stage Budget + Shared Overall `180s` Process Ceiling**:
   - [`OffProdChallengerIdentityCompletionCli`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionCli.kt) bounds optional `--apply-additive-ddl=true` with its own `--ddl-timeout-ms` (`2,500 ms` default, capped by remaining global wall-time), deducts `ddlStageElapsedMs` from `maxWallTimeMs` (`180,000 ms`) before invoking `runBounded`, returns `exitCode = 10` if the shared budget is exhausted before `runBounded`, and records `ddlStageBudgetMs`, `ddlStageElapsedMs`, `runStageBudgetMs`, `totalProcessBudgetMs`, and `totalProcessElapsedMs` on `CliExecutionResult`.
5. **Hard PostgreSQL Binary Check (`0` Test Skips)**:
   - [`OffProdChallengerIdentityPostgresQualificationTest.kt`](../src/test/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityPostgresQualificationTest.kt) uses `check(binDir != null)` instead of JUnit `assumeTrue` so missing PostgreSQL binaries fail hard rather than skipping.

---

## 2. Single Minimal Eventual Execution Proposal

> [!NOTE]
> **No-Effects & Non-Billing Posture**
> - Current read-only database credentials have **no** DDL/DML privileges and have **not** been used.
> - Zero SQL has been executed against any shared or production database.
> - Requires **no** paid database tier upgrade, **no** paid Render instance upgrade, and **no** paid Riot API tier.

### 2.1 Exact SQL, Least-Privilege Non-Billing Grants, Indexes & Measured Fixture Bytes

1. **Stage 1 — One-Time Additive DDL & Least-Privilege Table-Scoped Grants (Executed Separately Before CLI Run, `< 2.5s` Lock/Statement Timeout)**:
   - Rather than granting broad database-wide `READWRITE` or superuser privileges to the off-production CLI, provision a least-privilege role (`offprod_identity_stager`) scoped strictly to `SELECT` on `public.indexed_players` and `SELECT, INSERT, UPDATE` on `public.player_riot_identities`:
   ```sql
   SET lock_timeout = '2s';
   SET statement_timeout = '2500ms';

   CREATE TABLE IF NOT EXISTS public.player_riot_identities (
       puuid           VARCHAR(78)  PRIMARY KEY,
       platform_region VARCHAR(16)  NOT NULL,
       game_name       VARCHAR(255) NOT NULL,
       tag_line        VARCHAR(255) NOT NULL,
       verified_at     TIMESTAMPTZ  NOT NULL,
       CONSTRAINT chk_player_riot_identities_nonblank
           CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
   );

   -- Least-privilege table-scoped grants for off-prod CLI role (NOT a broad DB RW grant):
   GRANT USAGE ON SCHEMA public TO offprod_identity_stager;
   GRANT SELECT ON TABLE public.indexed_players TO offprod_identity_stager;
   GRANT SELECT, INSERT, UPDATE ON TABLE public.player_riot_identities TO offprod_identity_stager;
   REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE public.indexed_players FROM offprod_identity_stager;

   -- Read-only SELECT grant for the application service role (if distinct from table owner):
   GRANT SELECT ON TABLE public.player_riot_identities TO <app_db_role>;
   ```
2. **Exact SQL Executed by Off-Production CLI (`--apply-additive-ddl=false`, Absent-Manifest `191`-Only Scope)**:
   - **Pre-Call Read-Only Recheck (`conn.isReadOnly = true`)**:
     ```sql
     SELECT puuid, game_name, tag_line FROM indexed_players WHERE puuid IN (?, ..., ?);
     SELECT puuid, game_name, tag_line FROM player_riot_identities WHERE puuid IN (?, ..., ?);
     ```
     - If **any** candidate is in `indexed_players` with blank `game_name` or `tag_line`, CLI **stops immediately** (`exitCode = 1`, `0` HTTP calls, `0` DB writes) and **never** runs `UPDATE indexed_players`.
   - **Per-Candidate Race-Safe Upsert (`conn.isReadOnly = false`, `conn.autoCommit = true`)**:
     ```sql
     INSERT INTO player_riot_identities (
         puuid, platform_region, game_name, tag_line, verified_at
     ) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT (puuid) DO UPDATE
     SET game_name = CASE
             WHEN BTRIM(COALESCE(player_riot_identities.game_name, '')) = '' THEN EXCLUDED.game_name
             ELSE player_riot_identities.game_name
         END,
         tag_line = CASE
             WHEN BTRIM(COALESCE(player_riot_identities.tag_line, '')) = '' THEN EXCLUDED.tag_line
             ELSE player_riot_identities.tag_line
         END,
         platform_region = CASE
             WHEN BTRIM(COALESCE(player_riot_identities.platform_region, '')) = '' THEN EXCLUDED.platform_region
             ELSE player_riot_identities.platform_region
         END,
         verified_at = GREATEST(player_riot_identities.verified_at, EXCLUDED.verified_at)
     WHERE (
         BTRIM(COALESCE(player_riot_identities.game_name, '')) = ''
         OR BTRIM(COALESCE(player_riot_identities.tag_line, '')) = ''
     )
     AND BTRIM(COALESCE(EXCLUDED.game_name, '')) <> ''
     AND BTRIM(COALESCE(EXCLUDED.tag_line, '')) <> '';
     ```
   - **Mandatory Post-Commit Verification Read (`conn.isReadOnly = true` on a separate connection)**:
     ```sql
     SELECT game_name, tag_line, verified_at FROM player_riot_identities WHERE puuid = ?;
     ```
3. **Exact Indexes**:
   - **1 index total**: Primary key B-tree index `player_riot_identities_pkey` on `(puuid)`. Zero secondary indexes, zero trigram/GIN indexes.
4. **Exact Measured Fixture Bytes (`191` Verified Rows: `31` `euw1` + `160` `kr` on Real PostgreSQL 15.19 & 512 MiB Container)**:

   | Component | Metric / Measurement Query | Exact Measured Value |
   | :--- | :--- | :--- |
   | **PostgreSQL Live Tuples (`191` rows)** | `SUM(pg_column_size(t.*))` (`min` / `max` / `avg`) | **`24,696` bytes (`24.12 KiB`)** (`128 B` min / `136 B` max / `129.30 B` avg) |
   | **PostgreSQL Main Heap** | `pg_relation_size('public.player_riot_identities')` | **`32,768` bytes (`32.00 KiB`, `4` x 8 KiB pages)** |
   | **PostgreSQL PK B-Tree Index** | `pg_indexes_size('public.player_riot_identities')` (`player_riot_identities_pkey`) | **`40,960` bytes (`40.00 KiB`, `5` x 8 KiB pages)** |
   | **PostgreSQL Total Relation Size** | `pg_total_relation_size('public.player_riot_identities')` (heap + PK + FSM/VM) | **`114,688` bytes (`112.00 KiB`, `14` x 8 KiB pages)** |
   | **Redis `leaderboard:euw1::RANKED_SOLO_5x5`** | Full `300/300` GZIP payload in Redis (`acceptance_summary.json`) | **`6,265` bytes (`6.12 KiB`)** |
   | **Redis `leaderboard:kr::RANKED_SOLO_5x5`** | Full `300/300` GZIP payload in Redis (`acceptance_summary.json`) | **`6,681` bytes (`6.52 KiB`)** |

---

### 2.2 Grounded Key-Budget Coordination Mechanism (No Invented Redis Guard; Explicit Blocker If No Dedicated Key)

- **Grounded Codebase Fact**:
  - In `SummonerSearcher-Backend`, [`RiotApiRateLimiter.kt`](../src/main/kotlin/com/pride/summoner_searcher_api/service/RiotApiRateLimiter.kt) is strictly an **in-JVM memory** rate limiter (`ConcurrentHashMap` inside the Spring Boot process).
  - Live production callers (`RiotApiService`, `ChallengerLeagueService`, `CacheWarmer`, `/api/summoner/*`, `/api/matches/*`) **do not** consult any Redis key, Redis lock, or cross-process coordinator before dispatching requests with `RIOT_API_KEY`.
- **Therefore**:
  1. **Route A (Dedicated Non-Billing Riot API Key — Recommended, Zero Production Impact)**:
     - Use a separate Riot API key (`OFFPROD_RIOT_API_KEY`) issued under an isolated Riot Developer project/application that is **not** configured as `RIOT_API_KEY` on the live Render backend (`sha256(OFFPROD_RIOT_API_KEY) != sha256(RENDER_PROD_RIOT_API_KEY)`).
     - Because Riot enforces `X-App-Rate-Limit` and `X-Method-Rate-Limit` per Riot application/key at each regional routing cluster (`europe.api.riotgames.com`, `asia.api.riotgames.com`), an isolated key has zero shared-bucket contention with the running Render container.
     - Pass `--key-coordination-mode=EXCLUSIVE_KEY --coordination-attestation="isolated-key-sha256-verified"`.
  2. **Route B (If No Dedicated Key Is Provided — Explicit Blocker While Render Service Is Running)**:
     - Because **no** existing cross-process key coordinator is enforced by all production callers, running the off-prod CLI with the shared production `RIOT_API_KEY` while the Render backend container is running is **EXPLICITLY BLOCKED**.
     - If the existing key must be used without modifying all production callers, the operator must temporarily **suspend / scale to `0` the Render backend service** for the `<= 180s` CLI window so zero other processes hold `RIOT_API_KEY`, then pass `--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW --coordination-attestation="render-service-suspended-sole-key-holder"`.

---

### 2.3 Off-Production Process Limits, Stage Budgets & Partial / `404` / Omitted / `unknown_commit` Handling

1. **Off-Production Process & Resource Limits**:
   - Executed on an operator workstation or ephemeral off-production runner (never inside the Render 512 MiB container) with JVM heap capped at `-Xms16m -Xmx96m`:
     - **Stage 2 (Mandatory Dry-Run Precheck, `0` HTTP / `0` Writes, `< 5s`)**:
       ```bash
       java -Xms16m -Xmx96m \
         -cp build/libs/summoner-searcher-api-0.0.1-SNAPSHOT-plain.jar:<runtime-classpath> \
         com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionCli \
         --manifest-file=/path/to/missing_191_manifest.json \
         --jdbc-url="$OFFPROD_JDBC_URL" \
         --db-user=offprod_identity_stager \
         --db-pass="$OFFPROD_DB_PASS" \
         --dry-run=true \
         --network-enabled=false \
         --allow-durable-writes=false \
         --db-timeout-ms=2500 \
         --max-wall-time-ms=5000 \
         --output-report-file=/tmp/precheck_dryrun_report.json
       ```
       - Verify `exitCode == 0`, `outcome == "DRY_RUN_DB_RECHECK_ONLY_COMPLETED"`, `presentWithBlankFieldInIndexedPlayersCount == 0`, `absentFromIndexedPlayersCount + alreadyResolvedInDbCount == 191`. (If `presentWithBlankFieldInIndexedPlayersCount > 0`, CLI stops with `exitCode = 1` and does not proceed.)
     - **Stage 3 (Bounded Completion Run, Separate from DDL Stage, Hard `180s` / `191` Attempt Cap)**:
       ```bash
       java -Xms16m -Xmx96m \
         -cp build/libs/summoner-searcher-api-0.0.1-SNAPSHOT-plain.jar:<runtime-classpath> \
         com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionCli \
         --manifest-file=/path/to/missing_191_manifest.json \
         --jdbc-url="$OFFPROD_JDBC_URL" \
         --db-user=offprod_identity_stager \
         --db-pass="$OFFPROD_DB_PASS" \
         --apply-additive-ddl=false \
         --dry-run=false \
         --network-enabled=true \
         --allow-durable-writes=true \
         --allow-indexed-players-blank-update=false \
         --key-coordination-mode=EXCLUSIVE_KEY \
         --coordination-attestation="isolated-key-sha256-verified" \
         --riot-api-key="$OFFPROD_RIOT_API_KEY" \
         --max-http-attempts=191 \
         --max-wall-time-ms=180000 \
         --db-timeout-ms=2500 \
         --http-connect-timeout-ms=2500 \
         --http-read-timeout-ms=5000 \
         --http-call-deadline-ms=5500 \
         --max-body-bytes=4096 \
         --output-report-file=/tmp/completion_report_pass1.json \
         --output-remaining-manifest-file=/tmp/remaining_manifest_pass1.json
       ```
2. **Deterministic Handling of Partial `404`, Omitted Fields, Rate/Budget Exhaustion & `unknown_commit`**:
   - **HTTP `404` (`ItemStatus.SKIPPED_NOT_FOUND_404`) & HTTP `200` with Omitted/Blank `gameName`/`tagLine` (`ItemStatus.SKIPPED_OMITTED_RIOT_ID_FIELDS`)**:
     - Executes **`0` DB writes** (never fabricates blank/sentinel rows). Recorded in `notFound404Count` / `omittedRiotIdFieldsCount` in `completion_report_pass1.json`.
   - **Rate-Limit Window Wait > Remaining `180s` Budget (`PARTIAL_COMPLETION_WALL_TIME_EXHAUSTED`, `exitCode = 10`) or HTTP `429`/`5xx`/Timeout (`exitCode = 1`)**:
     - Performs **`0` retries**, halts immediately, and writes all uncompleted PUUIDs in canonical order to `/tmp/remaining_manifest_pass1.json`.
   - **`unknown_commit` (`ABORTED_DB_FAILURE_OR_UNKNOWN_COMMIT`, `exitCode = 1`)**:
     - Occurs if a DB timeout or connection drop happens while `executeUpdate()` or `commit()` is already in flight on PostgreSQL, or before the post-commit verification `SELECT` on a separate connection completes.
     - The candidate is **not** counted in `durableIdentityTableUpsertsVerifiedCount` and is retained in `/tmp/remaining_manifest_pass1.json`.
     - **Resume Protocol**: Before running a resume pass on `/tmp/remaining_manifest_pass1.json`, run Stage 2 (`--dry-run=true --network-enabled=false`) against `/tmp/remaining_manifest_pass1.json`. Any candidate whose in-flight transaction during `unknown_commit` actually committed on PostgreSQL is detected in `player_riot_identities` and classified as `SKIPPED_ALREADY_RESOLVED_IN_DB` with **`0` extra HTTP calls and `0` duplicate writes**; only candidates still absent in PostgreSQL are fetched on the resume pass.

---

### 2.4 Final Read Coverage Verification & Data-Preserving Rollback

1. **Final Read Coverage Verification**:
   - Confirm CLI report: `outcome == "STAGED_ALL_CANDIDATES"`, `durableIndexedPlayerUpdatesVerifiedCount == 0`, `durableIdentityTableUpsertsVerifiedCount == 191`, `remainingPendingCount == 0`.
   - Confirm PostgreSQL counts (`euw1 = 31`, `kr = 160`, total `191` in `player_riot_identities`).
   - On candidate deployment, confirm `GET /api/leaderboards/challenger/euw1` (`300/300` non-blank `gameName`/`tagLine`) and `GET /api/leaderboards/challenger/kr` (`300/300` non-blank `gameName`/`tagLine`) = **`600/600` hydrated entries** with `indexed-first` precedence and `0` Account-V1 fanout calls.
2. **Data-Preserving Rollback Plan**:
   - **Application-Only Rollback (`0` DDL / `0` Data Deletion)**: Revert the backend deployment to `139a7de48e67af9d664c08fd61ce131b6e63bd74` (or `5959a7036c9e281f72d17f0a0bd5003ba59476f8`) and **leave `public.player_riot_identities` and all `191` verified rows intact**.
   - `139a7de48e67af9d664c08fd61ce131b6e63bd74` never queries `player_riot_identities`, serves `euw1`, `kr`, and `na1` Challenger leaderboards (`HTTP 200`, `300` entries each), and `/api/search/autocomplete` (which queries `indexed_players` only) remains 100% unpolluted while preserving all `191` verified rows for future forward rollout.

---

## 3. Single Remaining Operator Decision

1. **Riot Key Coordination Route for Off-Production Execution**:
   - **Option 1 (Recommended)**: Provide an isolated non-billing Riot Developer key (`OFFPROD_RIOT_API_KEY` from a separate Riot project not used by Render production, `--key-coordination-mode=EXCLUSIVE_KEY`), plus the least-privilege table-scoped PostgreSQL grant (`SELECT` on `indexed_players`, `SELECT, INSERT, UPDATE` on `player_riot_identities`), **OR**
   - **Option 2**: Keep off-production execution **blocked** until a maintenance window where the Render backend service is temporarily suspended (`--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW`) so the shared key has zero concurrent callers.
