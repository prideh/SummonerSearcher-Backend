-- ============================================================================
-- Migration: V20261109__create_player_riot_identities.sql
-- Purpose  : Additive identity-only storage for verified Riot IDs (game_name, tag_line)
--            keyed by official 78-character base64url PUUID.
-- Guarantees:
--   1. Does NOT fabricate match metadata (profile_icon_id, summoner_level, last_seen_at)
--      or alter existing `indexed_players` table schema/constraints/indexes.
--   2. Enforces non-blank game_name and tag_line via `chk_player_riot_identities_nonblank`.
--   3. Uses only the PRIMARY KEY B-tree index `player_riot_identities_pkey` on `(puuid)`
--      for O(log N) batched `WHERE puuid IN (...)` lookups and `ON CONFLICT (puuid)` upserts.
--   4. 100% backward-compatible with rollback commit 139a7de48e67af9d664c08fd61ce131b6e63bd74
--      (which ignores `player_riot_identities` and reads only `indexed_players`).
-- Rollback : DROP TABLE IF EXISTS player_riot_identities;
-- ============================================================================

CREATE TABLE IF NOT EXISTS player_riot_identities (
    puuid           VARCHAR(78)  PRIMARY KEY,
    platform_region VARCHAR(16)  NOT NULL,
    game_name       VARCHAR(255) NOT NULL,
    tag_line        VARCHAR(255) NOT NULL,
    verified_at     TIMESTAMPTZ  NOT NULL,
    CONSTRAINT chk_player_riot_identities_nonblank
        CHECK (BTRIM(game_name) <> '' AND BTRIM(tag_line) <> '')
);
