# Off-Production Challenger Missing Identity Integration, Qualification & Minimal Execution Proposal

> [!IMPORTANT]
> **Branch, Exact Commit SHAs, Raw Artifact URLs & Zero-Effects Status**
> - **Repository**: `prideh/SummonerSearcher-Backend`
> - **Isolated Implementation Branch (pushed with `[skip preview] [skip render]`)**: `feat/offprod-missing-challenger-identity-completion`
> - **Qualified Code & Test Commit SHA**: **`fef2ed6d41d82fdac38384974cec6ca0a73e6aae`** (`0` code/test diff between `fef2ed6d41d82fdac38384974cec6ca0a73e6aae` and branch `HEAD`; only this documentation file is updated in the head doc commit)
> - **Pushed Markdown Path in Repository**: `docs/OFFPROD_CHALLENGER_IDENTITY_COMPLETION_PROPOSAL.md`
> - **512 MiB Cgroup + 104-Test JDK 21 Proof Workflow**:
>   - **Proof Branch**: `test/challenger-leaderboard-512m-proof-5959a70` (`39e3e350194ebf580e79ab87b9871dbd9bf07aa6`)
>   - **GitHub Actions Run URL**: `https://github.com/prideh/SummonerSearcher-Backend/actions/runs/42629632345` (Job ID `129202275891`, `conclusion = success`)
>   - **Raw Proof Artifact**: `challenger-leaderboard-512m-proof-fef2ed6` — Artifact ID **`13680432346`** (`74,389` bytes; API URL: `https://api.github.com/repos/prideh/SummonerSearcher-Backend/actions/artifacts/13680432346/zip`, Run Artifact UI URL: `https://github.com/prideh/SummonerSearcher-Backend/actions/runs/42629632345/artifacts/13680432346`)
> - **Base Candidate Commit (`5959a703`)**: `5959a7036c9e281f72d17f0a0bd5003ba59476f8` (PR #43 candidate, 100% unmodified)
> - **Production / Rollback Base Commit (`master`)**: `139a7de48e67af9d664c08fd61ce131b6e63bd74` (100% unmodified)
> - **Frontend Workspace (`SummonerSearcher-Frontend`)**: Untouched; owner UI and uncommitted work preserved.
> - **Zero Live Effects**: `0` live Riot API calls, `0` live database DDL/DML executions, `0` existing/read-only DB credentials used, `0` Render deploys/previews, `0` billing.

---

## 1. Verified Code Fixes & Qualification Evidence (`fef2ed6d41d82fdac38384974cec6ca0a73e6aae`)

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
5. **Qualification Test & 512 MiB Container Results on `fef2ed6d41d82fdac38384974cec6ca0a73e6aae`**:
   - **Local Real PostgreSQL 15.19 (`task-698`)**: **`27/27` passed, `0` skipped, `0` failed** (`OffProdChallengerIdentityCompletionToolTest` `18/18`, `OffProdChallengerIdentityPostgresQualificationTest` `2/2`, `ChallengerLeagueServiceTest` `7/7`).
   - **CI JDK 21 Full Suite (Run `42629632345`, Artifact `13680432346`)**: **`104/104` passed across 16 JUnit XML suites, `0` skipped, `0` failed, `test.exit = 0`**.
   - **CI Real 512 MiB Cgroup Container Qualification (`acceptance_summary.json` in Artifact `13680432346`)**:
     - `status = "PASSED"`, `candidateCommitSha = "fef2ed6d41d82fdac38384974cec6ca0a73e6aae"`.
     - Candidate `fef2ed6` max `memory.current`: **`357,965,824` bytes (`341.38 MiB`, `66.68%` of `512.00 MiB`)**; max `memory.peak`: **`358,268,928` bytes (`341.67 MiB`, `66.73%` of `512.00 MiB`; `170.33 MiB` / `178,601,984` bytes free headroom at peak)**, `oomKilled = false`, `restartCount = 0`.
     - Data-preserving rollback container (`139a7de48e67af9d664c08fd61ce131b6e63bd74`) on the migrated database with `player_riot_identities` populated: serves `euw1`, `kr`, and `na1` (`HTTP 200`, `300` entries each), `/api/search/autocomplete` unpolluted (`autocompleteUnpollutedByIdentityOnlyTable = true`), peak memory **`316,424,192` bytes (`301.77 MiB`, `58.94%`)**, `oomKilled = false`, `restartCount = 0`.

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
4. **Exact Measured Fixture Bytes (Upper-Bound `191` Verified Rows: `31` `euw1` + `160` `kr` on Real PostgreSQL 15.19 & 512 MiB Container)**:

   | Component | Metric / Measurement Query | Exact Measured Value (at 191 Rows) |
   | :--- | :--- | :--- |
   | **PostgreSQL Live Tuples (`191` rows)** | `SUM(pg_column_size(t.*))` (`min` / `max` / `avg`) | **`24,696` bytes (`24.12 KiB`)** (`128 B` min / `136 B` max / `129.30 B` avg) |
   | **PostgreSQL Main Heap** | `pg_relation_size('public.player_riot_identities')` | **`32,768` bytes (`32.00 KiB`, `4` x 8 KiB pages)** |
   | **PostgreSQL PK B-Tree Index** | `pg_indexes_size('public.player_riot_identities')` (`player_riot_identities_pkey`) | **`40,960` bytes (`40.00 KiB`, `5` x 8 KiB pages)** |
   | **PostgreSQL Total Relation Size** | `pg_total_relation_size('public.player_riot_identities')` (heap + PK + FSM/VM) | **`114,688` bytes (`112.00 KiB`, `14` x 8 KiB pages)** |
   | **Redis `leaderboard:euw1::RANKED_SOLO_5x5`** | Full `300/300` GZIP payload in Redis (`acceptance_summary.json`) | **`6,269` bytes (`6.12 KiB`)** |
   | **Redis `leaderboard:kr::RANKED_SOLO_5x5`** | Full `300/300` GZIP payload in Redis (`acceptance_summary.json`) | **`6,681` bytes (`6.52 KiB`)** |

---

### 2.2 Grounded Key-Budget Coordination & Explicit Blocker Analysis

- **Grounded Codebase Fact**:
  - In `SummonerSearcher-Backend`, [`RiotApiRateLimiter.kt`](../src/main/kotlin/com/pride/summoner_searcher_api/service/RiotApiRateLimiter.kt) is strictly an **in-JVM memory** rate limiter (`ConcurrentHashMap` inside the Spring Boot process).
  - Live production callers (`RiotApiService`, `ChallengerLeagueService`, `CacheWarmer`, `/api/summoner/*`, `/api/matches/*`) **do not** consult any Redis key, Redis lock, or cross-process coordinator before dispatching requests with `RIOT_API_KEY`.
- **Grounded Coordination Requirements & Explicit Blockers**:
  1. **Route A (Separate Authorized Riot Application Rate-Limit Budget)**:
     - Merely having a different token string is **insufficient** if two keys belong to the same Riot application/project rate-limit bucket or if the key is not authorized for this use.
     - Route A requires a credential (`OFFPROD_RIOT_API_KEY`) backed by a **separate authorized Riot Developer application/project budget** whose `X-App-Rate-Limit` / `X-Method-Rate-Limit` bucket at `europe.api.riotgames.com` and `asia.api.riotgames.com` is completely independent of the production service and has **zero** concurrent callers during the `<= 180s` CLI run (`--key-coordination-mode=EXCLUSIVE_KEY`).
  2. **Route B (Shared Production Application Key — Explicit Blocker Without Owner Downtime Authorization & Single-Holder Proof)**:
     - Because **no** cross-process key coordinator is enforced by production callers, running against the shared production Riot application key while production is live is **EXPLICITLY BLOCKED**.
     - Furthermore, suspending the Render production service is an **owner/product downtime decision** (not an engineering grant) and does **not** automatically prove that no other developer workstation, script, or external process is concurrently using that key. Route B is therefore **blocked** unless the owner explicitly authorizes a production maintenance stop **and** confirms that zero other processes/users hold or invoke that Riot application key during the `<= 180s` window (`--key-coordination-mode=COORDINATED_MAINTENANCE_WINDOW`).

---

### 2.3 Off-Production Process Limits, Secret-Safe Env Invocation & Partial / `404` / Omitted / `unknown_commit` Handling

1. **Off-Production Process & Secret-Safe Environment Invocation (`0` Secret CLI Args)**:
   - Executed on an operator workstation or ephemeral off-production runner (never inside the Render 512 MiB container) with JVM heap capped at `-Xms16m -Xmx96m`.
   - **Never pass `--db-pass` or `--riot-api-key` on the command line** (which would expose credentials in OS process lists `/proc/<pid>/cmdline`). [`OffProdChallengerIdentityCompletionCli`](../src/main/kotlin/com/pride/summoner_searcher_api/tools/OffProdChallengerIdentityCompletionCli.kt) reads `OFFPROD_JDBC_URL`, `OFFPROD_DB_USER`, `OFFPROD_DB_PASS`, and `OFFPROD_RIOT_API_KEY` directly from environment variables:
     - **Stage 2 (Mandatory Dry-Run Precheck, `0` HTTP / `0` Writes, `< 5s` Budget)**:
       ```bash
       export OFFPROD_JDBC_URL="jdbc:postgresql://<db-host>:5432/<db-name>"
       export OFFPROD_DB_USER="offprod_identity_stager"
       export OFFPROD_DB_PASS="<secret-from-vault-or-prompt>"

       java -Xms16m -Xmx96m \
         -cp build/libs/summoner-searcher-api-0.0.1-SNAPSHOT-plain.jar:<runtime-classpath> \
         com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionCli \
         --manifest-file=/path/to/missing_191_manifest.json \
         --dry-run=true \
         --network-enabled=false \
         --allow-durable-writes=false \
         --db-timeout-ms=2500 \
         --max-wall-time-ms=5000 \
         --output-report-file=/tmp/precheck_dryrun_report.json
       ```
       - Verify `exitCode == 0`, `outcome == "DRY_RUN_DB_RECHECK_ONLY_COMPLETED"`, `presentWithBlankFieldInIndexedPlayersCount == 0`, and `absentFromIndexedPlayersCount + alreadyResolvedInDbCount == 191`. (If `presentWithBlankFieldInIndexedPlayersCount > 0`, CLI stops immediately with `exitCode = 1` and does not proceed.)
     - **Stage 3 (Bounded Completion Run, Separate from DDL Stage, Hard `180s` / `191` Attempt Cap, Secrets via Env Only)**:
       ```bash
       export OFFPROD_JDBC_URL="jdbc:postgresql://<db-host>:5432/<db-name>"
       export OFFPROD_DB_USER="offprod_identity_stager"
       export OFFPROD_DB_PASS="<secret-from-vault-or-prompt>"
       export OFFPROD_RIOT_API_KEY="<isolated-riot-app-key>"

       java -Xms16m -Xmx96m \
         -cp build/libs/summoner-searcher-api-0.0.1-SNAPSHOT-plain.jar:<runtime-classpath> \
         com.pride.summoner_searcher_api.tools.OffProdChallengerIdentityCompletionCli \
         --manifest-file=/path/to/missing_191_manifest.json \
         --apply-additive-ddl=false \
         --dry-run=false \
         --network-enabled=true \
         --allow-durable-writes=true \
         --allow-indexed-players-blank-update=false \
         --key-coordination-mode=EXCLUSIVE_KEY \
         --coordination-attestation="isolated-riot-app-budget-sole-holder" \
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

### 2.4 Honest Per-PUUID Completion Accounting, Separate Live Roster Measurement & Data-Preserving Rollback

1. **Per-PUUID Completion Accounting for the Historical `191`-Entry Manifest (Not Requiring `191` New Writes)**:
   - Because the 191-entry manifest was captured from a historical Challenger snapshot (`2026-11-09T11:34:00Z`), by the time of eventual execution:
     - Zero or more of the 191 PUUIDs may already have been resolved into `indexed_players` via normal user searches/match indexing (`alreadyResolvedInDbCount`, `ItemStatus.SKIPPED_ALREADY_RESOLVED_IN_DB`, requiring `0` HTTP calls and `0` writes to `player_riot_identities`).
     - Zero or more of the remaining PUUIDs may return upstream `404` (`notFound404Count`) or `200` with omitted/blank `gameName`/`tagLine` (`omittedRiotIdFieldsCount`), which deliberately perform `0` DB writes.
   - Therefore, completion of the 191-entry manifest does **not** require `durableIdentityTableUpsertsVerifiedCount == 191` or `COUNT(*) FROM player_riot_identities == 191`. Instead, verify the exact per-PUUID accounting invariant in `completion_report_pass1.json`:
     - `manifestTotalCount == 191`
     - `durableIndexedPlayerUpdatesVerifiedCount == 0`
     - `alreadyResolvedInDbCount + durableIdentityTableUpsertsVerifiedCount + concurrentAlreadyCompleteVerifiedCount + notFound404Count + omittedRiotIdFieldsCount + remainingPendingCount == 191`
     - When `remainingPendingCount == 0` and `dbFailureOrUnknownCommitCount == 0`, every single one of the 191 historical manifest PUUIDs is accounted for as either verified in DB (`alreadyResolvedInDbCount + durableIdentityTableUpsertsVerifiedCount + concurrentAlreadyCompleteVerifiedCount`) or upstream-unresolved (`notFound404Count + omittedRiotIdFieldsCount`), and `COUNT(*) FROM player_riot_identities` equals `durableIdentityTableUpsertsVerifiedCount` (`<= 191`).
2. **Separate Live Challenger Roster Coverage Measurement (Not Promising `600/600` on Rotating Live Ladder)**:
   - While the synthetic qualification fixture deterministically proves `600/600` (`300/300` EUW1 + `300/300` KR) when the 300-player ladder matches the snapshot, the real live Challenger 300+300 ladder rotates over time as players promote/demote.
   - Post-deployment verification therefore measures ** two distinct metrics**:
     1. **Historical 191 Manifest Resolution**: How many of the 191 historical snapshot PUUIDs are now non-blank in `indexed_players UNION player_riot_identities` vs upstream `404`/omitted.
     2. **Current Live Ladder Hydrated Coverage**: Actual hydrated vs missing count on `GET /api/riot/leaderboards/challenger/euw1` and `GET /api/riot/leaderboards/challenger/kr` at refresh time (where any newly promoted Challenger PUUIDs that entered the top 300 after `2026-11-09T11:34:00Z` and are not yet in `indexed_players` will remain unhydrated until indexed or included in a future snapshot manifest, with `0` live Account-V1 fanout calls during leaderboard refresh).
3. **Data-Preserving Rollback Plan**:
   - **Application-Only Rollback (`0` DDL / `0` Data Deletion)**: Revert the backend deployment to `139a7de48e67af9d664c08fd61ce131b6e63bd74` (or `5959a7036c9e281f72d17f0a0bd5003ba59476f8`) and **leave `public.player_riot_identities` and all verified rows intact**.
   - `139a7de48e67af9d664c08fd61ce131b6e63bd74` never queries `player_riot_identities`, serves `euw1`, `kr`, and `na1` Challenger leaderboards (`HTTP 200`, `300` entries each), and `/api/search/autocomplete` (which queries `indexed_players` only) remains 100% unpolluted while preserving all verified rows in `player_riot_identities`.

---

## 3. Single Remaining Owner Decision

1. **Authorization & Key-Budget Coordination Decision**:
   - **Option 1**: Provide a credential (`OFFPROD_RIOT_API_KEY` via env) backed by a **separate authorized Riot Developer application/project rate-limit budget** (`--key-coordination-mode=EXCLUSIVE_KEY`) + the least-privilege table-scoped PostgreSQL grant (`GRANT SELECT ON TABLE public.indexed_players`, `GRANT SELECT, INSERT, UPDATE ON TABLE public.player_riot_identities`), **OR**
   - **Option 2 (Explicit Blocker)**: Keep off-production network execution **blocked** (unless the owner explicitly authorizes a production maintenance stop **and** confirms zero other processes/users invoke the shared Riot application key during the `<= 180s` CLI window).
