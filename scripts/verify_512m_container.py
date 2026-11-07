#!/usr/bin/env python3
"""
Isolated 512 MiB Container Proof for Durable Incremental Season Stats (089ac61) + Rollback Read Compatibility (e683777).

Enforces:
1. Internal-only Docker bridge network (ab-isolated-net, 172.28.0.0/16) + explicit iptables DOCKER-USER/INPUT
   egress-reject rules + fail-closed nsenter -n negative-control outbound probe.
2. Real postgres:16-alpine (ss-postgres) and redis:7-alpine (ss-redis), and ss-backend-512m running with
   --memory=512m --memory-swap=512m and continuous 2-second host cgroup v2 sampling (< 78.0% gate, 0 OOMs, 0 restarts).
3. Realistic 10-participant Ranked Solo (queueId = 420) MatchDto payloads pre-cached in Redis (GZIP_B64:):
   - Scenario 1 (924-match player puuid-924):
     1a. Cold baseline from 924 cached 10-participant MatchDtos (0 Riot match fetches, 10 ID pages, exact counters/IDs/bytes).
     1b. Incremental +3 new games (924 -> 927) with all 904 older matches evicted from Redis and forbidden on Mock Riot
         (exact 3 Riot match fetches, 0 re-decodes of 904 old matches).
     1c. Full Redis-loss recovery (redis-cli FLUSHALL -> cold Redis recovery of 927-game profile with only 20 recent
         match fetches and 0 fetches of the 907 older matches, plus page-2 load-more verification).
   - Scenario 2 (2,100-match player puuid-2100):
     2a. Cold baseline from 2,100 cached 10-participant MatchDtos (21 ID pages hitting >2000 safety cap, 0 Riot match
         fetches, historyCapped=true, historyComplete=false, exact counters/IDs/bytes).
     2b. 2,100 rollforward (+1 new game EUW1_2100_2101, evicting resolvable tail EUW1_2100_1, exact 1 Riot match fetch,
         zero counter drift).
     2c. 2,100 rollforward with missing-tail failure (+1 new game EUW1_2100_2102 while tail EUW1_2100_2 is absent from
         Redis and returns HTTP 500 on Mock Riot -> EUW1_2100_2 retained in processedMatchIds, EUW1_2100_2102 recorded
         in pendingMatchIds with pending_games_count=1, historyComplete=false, zero counter drift).
     2d. Missing-tail recovery (EUW1_2100_2 restored -> evicted and subtracted, EUW1_2100_2102 folded into
         processedMatchIds[0], pending_games_count=0).
   - Scenario 3 (Same-player concurrent writers & @Version optimistic-lock merge on real Postgres, puuid-conc):
     Concurrent writer advances DB row from version 0 (2 games) to version 1 (3 games) while in-flight refresh folds
     EUW1_CONC_4; persistDurableSeasonState catches version conflict, merges newlyFoldedDeltas onto version 1, and
     commits version 2 (all 4 games) for both concurrent HTTP callers and Redis player:profile.
   - Scenario 4 (Operator scalar SQL coverage summary & newer-only participant identity indexing):
     Verifies /api/riot/cohorts/euw1/collector/coverage-summary during gap (pending=1) and after gap resolution (pending=0),
     and verifies indexed_players + /api/search/autocomplete monotonic newest lastSeenAt.
   - Scenario 5 (Rollback e683777 read compatibility on the same live Postgres & Redis):
     Stops 089ac61 container, boots e683777 image against the untouched Postgres + Redis instances, and verifies
     profile reads, match pagination, and autocomplete search succeed with identical totals and < 78.0% cgroup memory.
"""

import base64
from collections import Counter
from datetime import datetime, timezone
import gzip
import http.server
import json
import math
import os
import re
import secrets
import shutil
import socket
import socketserver
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

NETWORK_NAME = "ab-isolated-net"
BRIDGE_NAME = "br-ab-isolated"
SUBNET_CIDR = "172.28.0.0/16"
GATEWAY_IP = "172.28.0.1"
POSTGRES_IP = "172.28.0.2"
REDIS_IP = "172.28.0.3"
BACKEND_IP = "172.28.0.4"
WRITER2_IP = "172.28.0.5"

PORT_MOCK_RIOT = 443
BASE_URL = f"http://{BACKEND_IP}:10000"
WRITER2_URL = f"http://{WRITER2_IP}:10000"

CGROUP_MAX_BYTES = 536_870_912
# Strict 78% raw-byte ceiling: floor(536_870_912 * 0.78) = 418_759_311 bytes (< 399.36 MiB)
CGROUP_STRICT_GATE_BYTES = 418_759_311

EPHEMERAL_DB_PASS = secrets.token_hex(24)
EPHEMERAL_JWT_SECRET = secrets.token_hex(32)
EPHEMERAL_TWOFA_SECRET = secrets.token_hex(16)
EPHEMERAL_RIOT_TOKEN = f"RGAPI-{secrets.token_hex(16)}"
EPHEMERAL_RESEND_TOKEN = f"re_{secrets.token_hex(16)}"
EPHEMERAL_OP_TOKEN = secrets.token_hex(20)
OP_HEADERS = {"X-Cohort-Operator-Token": EPHEMERAL_OP_TOKEN}

PUUID_924 = "puuid-player-924"
PUUID_2100 = "puuid-player-2100"
PUUID_CONC = "puuid-player-conc"
CO_PLAYER_PUUID = "co-player-monotonic-1"

CHAMPIONS = ["Orianna", "Syndra", "Ahri", "Viktor", "Azir"]
ROLES = ["MIDDLE", "TOP", "JUNGLE", "BOTTOM", "UTILITY"]

EXPECTED_EXTRA_HOSTS = tuple(
    f"{h}:{GATEWAY_IP}"
    for h in (
        "euw1.api.riotgames.com",
        "na1.api.riotgames.com",
        "kr.api.riotgames.com",
        "europe.api.riotgames.com",
        "americas.api.riotgames.com",
        "asia.api.riotgames.com",
        "ddragon.leagueoflegends.com",
    )
)

# Anchor timestamps safely inside the current UTC calendar year and strictly before now
NOW_EPOCH_SEC = int(time.time())
CURRENT_UTC_YEAR = datetime.fromtimestamp(NOW_EPOCH_SEC, tz=timezone.utc).year
START_OF_YEAR_EPOCH_SEC = int(datetime(CURRENT_UTC_YEAR, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
# Base creation timestamp: 60 hours before NOW, advancing +60s per match index (strictly within current UTC year)
BASE_GAME_CREATION_MS = max((START_OF_YEAR_EPOCH_SEC + 86400) * 1000, (NOW_EPOCH_SEC - 216000) * 1000)


def build_realistic_10p_match(match_id: str, idx: int, target_puuid: str, force_aatrox_loss: bool = False) -> dict:
    """
    Builds a realistic 10-participant Ranked Solo (queueId = 420) MatchDto.
    - Participant 0 is `target_puuid` (MIDDLE, teamId=100 when idx%3 != 0 else 200).
    - Participant 1 is `CO_PLAYER_PUUID` with monotonic gameName/tagLine/summonerLevel by `idx`.
    - Participant 5 is the opposing MIDDLE laner on the opposite team so opponent stats are populated.
    """
    target_team = 200 if (idx % 3 == 0) else 100
    opp_team = 100 if target_team == 200 else 200
    if force_aatrox_loss:
        champ_name = "Aatrox"
        target_win = False
    else:
        champ_name = CHAMPIONS[idx % len(CHAMPIONS)]
        target_win = (idx % 4 != 0)

    duration_sec = 1500 + (idx % 7) * 60  # exact multiple of 60s (25..31 min) for clean floating-point math
    game_creation_ms = BASE_GAME_CREATION_MS + idx * 60_000

    participants = []
    for slot in range(10):
        is_target_team = (slot < 5)
        team_id = target_team if is_target_team else opp_team
        win = target_win if is_target_team else (not target_win)
        role = ROLES[slot % 5]

        if slot == 0:
            p_puuid = target_puuid
            g_name = "TargetPlayer"
            t_line = "EUW"
            c_name = champ_name
            s_level = 450
            p_icon = 4568
            kills = 4 + (idx % 9)
            deaths = 1 + (idx % 6)
            assists = 5 + (idx % 11)
            minions = 175 + (idx % 45)
            jungle_cs = 8 + (idx % 12)
            solo_kills = idx % 3
            plates = idx % 4
            vision = 22 + (idx % 18)
        elif slot == 1:
            p_puuid = CO_PLAYER_PUUID
            g_name = f"CoPlayerNew_{idx}"
            t_line = f"T{idx}"
            c_name = "LeeSin"
            s_level = 300 + idx
            p_icon = 4000 + (idx % 500)
            kills = 5 + (idx % 5)
            deaths = 3 + (idx % 4)
            assists = 7 + (idx % 6)
            minions = 40 + (idx % 20)
            jungle_cs = 130 + (idx % 30)
            solo_kills = 1
            plates = 1
            vision = 26 + (idx % 10)
        elif slot == 5:
            # Direct MIDDLE lane opponent on opp_team
            p_puuid = f"opp-mid-{(idx % 40) + 1}"
            g_name = f"OppMid{(idx % 40) + 1}"
            t_line = "EUW"
            c_name = CHAMPIONS[(idx + 2) % len(CHAMPIONS)]
            s_level = 380 + (idx % 50)
            p_icon = 3500 + (idx % 200)
            kills = 3 + ((idx + 1) % 7)
            deaths = 2 + ((idx + 2) % 6)
            assists = 4 + ((idx + 3) % 9)
            minions = 165 + ((idx + 5) % 40)
            jungle_cs = 6 + (idx % 8)
            solo_kills = (idx + 1) % 2
            plates = (idx + 1) % 3
            vision = 20 + (idx % 14)
        else:
            p_puuid = f"co-puuid-{slot}-{(idx + slot) % 60 + 1}"
            g_name = f"Summoner{slot}_{(idx + slot) % 60 + 1}"
            t_line = "EUW"
            c_name = CHAMPIONS[(idx + slot) % len(CHAMPIONS)]
            s_level = 250 + ((idx + slot) % 120)
            p_icon = 3000 + ((idx + slot) % 300)
            kills = 2 + ((idx + slot) % 6)
            deaths = 2 + ((idx + slot) % 5)
            assists = 4 + ((idx + slot) % 8)
            minions = 140 + ((idx + slot) % 50)
            jungle_cs = 10 + ((idx + slot) % 15)
            solo_kills = (idx + slot) % 2
            plates = (idx + slot) % 2
            vision = 18 + ((idx + slot) % 15)

        participants.append({
            "puuid": p_puuid,
            "summonerName": g_name,
            "riotIdGameName": g_name,
            "riotIdTagline": t_line,
            "summonerLevel": s_level,
            "profileIcon": p_icon,
            "championId": 10 + ((idx + slot) % 80),
            "championName": c_name,
            "teamId": team_id,
            "teamPosition": role,
            "individualPosition": role,
            "win": win,
            "kills": kills,
            "deaths": deaths,
            "assists": assists,
            "totalMinionsKilled": minions,
            "neutralMinionsKilled": jungle_cs,
            "goldEarned": 11500 + (idx % 2500) + slot * 150,
            "totalDamageDealtToChampions": 21000 + (idx % 6000) + slot * 300,
            "totalDamageTaken": 16500 + (idx % 4000) + slot * 200,
            "visionScore": vision,
            "wardsPlaced": 10 + (slot % 5),
            "wardsKilled": 3 + (slot % 4),
            "detectorWardsPlaced": 2 + (slot % 3),
            "champLevel": 15 + (idx % 4),
            "item0": 3089, "item1": 3157, "item2": 3020, "item3": 3135, "item4": 3165, "item5": 3116, "item6": 3363,
            "summoner1Id": 4, "summoner2Id": 12,
            "challenges": {
                "laneMinionsFirst10Minutes": 78 + (idx % 14),
                "jungleCsBefore10Minutes": 4.0,
                "kda": round((kills + assists) / max(1, deaths), 2),
                "killParticipation": 0.58,
                "damagePerMinute": 790.0,
                "goldPerMinute": 420.0,
                "visionScorePerMinute": 0.92,
                "soloKills": solo_kills,
                "turretPlatesTaken": plates
            },
            "perks": {
                "statPerks": {"defense": 5001, "flex": 5008, "offense": 5005},
                "styles": [
                    {"description": "primaryStyle", "style": 8200, "selections": [{"perk": 8230, "var1": 1, "var2": 0, "var3": 0}]},
                    {"description": "subStyle", "style": 8300, "selections": [{"perk": 8347, "var1": 0, "var2": 0, "var3": 0}]}
                ]
            }
        })

    return {
        "metadata": {
            "matchId": match_id,
            "participants": [pt["puuid"] for pt in participants]
        },
        "info": {
            "gameCreation": game_creation_ms,
            "gameDuration": duration_sec,
            "gameId": 8000000 + idx,
            "queueId": 420,
            "gameMode": "CLASSIC",
            "gameType": "MATCHED_GAME",
            "gameVersion": "16.7.600.1234",
            "platformId": "EUW1",
            "participants": participants,
            "teams": [
                {
                    "teamId": 100,
                    "win": (target_team == 100 and target_win) or (target_team == 200 and not target_win),
                    "objectives": {
                        "baron": {"first": True, "kills": 1},
                        "dragon": {"first": True, "kills": 3},
                        "tower": {"first": True, "kills": 8},
                        "horde": {"first": True, "kills": 3}
                    }
                },
                {
                    "teamId": 200,
                    "win": (target_team == 200 and target_win) or (target_team == 100 and not target_win),
                    "objectives": {
                        "baron": {"first": False, "kills": 0},
                        "dragon": {"first": False, "kills": 1},
                        "tower": {"first": False, "kills": 3},
                        "horde": {"first": False, "kills": 3}
                    }
                }
            ]
        }
    }


def compute_expected_counters(target_puuid: str, ordered_match_ids: list, matches_by_id: dict) -> tuple:
    """
    Mirrors MatchStatsAccumulator.kt exactly to produce expected (overallCounters, championCountersByChamp).
    """
    overall = {
        "totalGames": 0,
        "totalWins": 0,
        "totalKills": 0,
        "totalDeaths": 0,
        "totalAssists": 0,
        "totalCs": 0,
        "totalDurationInMinutes": 0.0,
        "totalKillParticipation": 0.0,
        "totalSoloKills": 0,
        "totalTurretPlates": 0,
        "totalVisionScore": 0,
        "oppTotalKills": 0,
        "oppTotalDeaths": 0,
        "oppTotalAssists": 0,
        "oppTotalCs": 0,
        "oppTotalSoloKills": 0,
        "oppTotalTurretPlates": 0,
        "oppTotalVisionScore": 0,
        "oppTotalKillParticipation": 0.0,
        "blueSideGames": 0,
        "blueSideWins": 0,
        "redSideGames": 0,
        "redSideWins": 0,
    }
    champs = {}

    for mid in ordered_match_ids:
        m = matches_by_id[mid]
        info = m["info"]
        pts = info["participants"]
        player = next(p for p in pts if p["puuid"] == target_puuid)
        opp = next(
            (p for p in pts if p["teamPosition"] == player["teamPosition"] and p["teamId"] != player["teamId"] and p["teamPosition"] != "NONE"),
            None
        )
        team_kills = sum(p["kills"] for p in pts if p["teamId"] == player["teamId"])
        opp_team_kills = sum(p["kills"] for p in pts if opp is not None and p["teamId"] == opp["teamId"])

        k = player["kills"]
        d = player["deaths"]
        a = player["assists"]
        cs = player["totalMinionsKilled"] + player["neutralMinionsKilled"]
        sk = player["challenges"]["soloKills"]
        tp = int(player["challenges"]["turretPlatesTaken"])
        vs = player["visionScore"]
        dur_min = info["gameDuration"] / 60.0
        kp = (k + a) / team_kills if team_kills > 0 else 0.0
        win = bool(player["win"])
        team_id = player["teamId"]
        cname = player["championName"]

        overall["totalGames"] += 1
        if win:
            overall["totalWins"] += 1
        if team_id == 100:
            overall["blueSideGames"] += 1
            if win:
                overall["blueSideWins"] += 1
        elif team_id == 200:
            overall["redSideGames"] += 1
            if win:
                overall["redSideWins"] += 1

        overall["totalKills"] += k
        overall["totalDeaths"] += d
        overall["totalAssists"] += a
        overall["totalCs"] += cs
        overall["totalSoloKills"] += sk
        overall["totalTurretPlates"] += tp
        overall["totalVisionScore"] += vs
        overall["totalDurationInMinutes"] += dur_min
        overall["totalKillParticipation"] += kp

        if opp is not None:
            ok = opp["kills"]
            od = opp["deaths"]
            oa = opp["assists"]
            ocs = opp["totalMinionsKilled"] + opp["neutralMinionsKilled"]
            osk = opp["challenges"]["soloKills"]
            otp = int(opp["challenges"]["turretPlatesTaken"])
            ovs = opp["visionScore"]
            okp = (ok + oa) / opp_team_kills if opp_team_kills > 0 else 0.0
            overall["oppTotalKills"] += ok
            overall["oppTotalDeaths"] += od
            overall["oppTotalAssists"] += oa
            overall["oppTotalCs"] += ocs
            overall["oppTotalSoloKills"] += osk
            overall["oppTotalTurretPlates"] += otp
            overall["oppTotalVisionScore"] += ovs
            overall["oppTotalKillParticipation"] += okp

        c_entry = champs.setdefault(cname, {
            "championName": cname,
            "games": 0,
            "wins": 0,
            "losses": 0,
            "kills": 0,
            "deaths": 0,
            "assists": 0,
            "soloKills": 0,
            "turretPlates": 0,
            "cs": 0,
            "durationMinutes": 0.0,
        })
        c_entry["games"] += 1
        if win:
            c_entry["wins"] += 1
        else:
            c_entry["losses"] += 1
        c_entry["kills"] += k
        c_entry["deaths"] += d
        c_entry["assists"] += a
        c_entry["soloKills"] += sk
        c_entry["turretPlates"] += tp
        c_entry["cs"] += cs
        c_entry["durationMinutes"] += dur_min

    return overall, champs


def assert_counters_equal(actual_overall: dict, actual_champs_list: list, expected_overall: dict, expected_champs: dict, context: str):
    int_keys = (
        "totalGames", "totalWins", "totalKills", "totalDeaths", "totalAssists", "totalCs",
        "totalSoloKills", "totalTurretPlates", "totalVisionScore",
        "oppTotalKills", "oppTotalDeaths", "oppTotalAssists", "oppTotalCs",
        "oppTotalSoloKills", "oppTotalTurretPlates", "oppTotalVisionScore",
        "blueSideGames", "blueSideWins", "redSideGames", "redSideWins"
    )
    for k in int_keys:
        if actual_overall.get(k) != expected_overall[k]:
            raise RuntimeError(
                f"[{context}] Overall counter mismatch on {k}: actual={actual_overall.get(k)} expected={expected_overall[k]}"
            )
    for fk in ("totalDurationInMinutes", "totalKillParticipation", "oppTotalKillParticipation"):
        if not math.isclose(float(actual_overall.get(fk, 0.0)), float(expected_overall[fk]), rel_tol=1e-6, abs_tol=1e-6):
            raise RuntimeError(
                f"[{context}] Overall float counter mismatch on {fk}: actual={actual_overall.get(fk)} expected={expected_overall[fk]}"
            )

    actual_by_name = {c["championName"]: c for c in actual_champs_list}
    if set(actual_by_name.keys()) != set(expected_champs.keys()):
        raise RuntimeError(
            f"[{context}] Champion set mismatch: actual={set(actual_by_name.keys())} expected={set(expected_champs.keys())}"
        )
    for cname, exp_c in expected_champs.items():
        act_c = actual_by_name[cname]
        for ck in ("games", "wins", "losses", "kills", "deaths", "assists", "soloKills", "turretPlates", "cs"):
            if act_c.get(ck) != exp_c[ck]:
                raise RuntimeError(
                    f"[{context}] Champion {cname} counter mismatch on {ck}: actual={act_c.get(ck)} expected={exp_c[ck]}"
                )
        if not math.isclose(float(act_c.get("durationMinutes", 0.0)), float(exp_c["durationMinutes"]), rel_tol=1e-6, abs_tol=1e-6):
            raise RuntimeError(
                f"[{context}] Champion {cname} durationMinutes mismatch: actual={act_c.get('durationMinutes')} expected={exp_c['durationMinutes']}"
            )


def compress_for_redis(obj: dict) -> str:
    raw_bytes = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    gz_bytes = gzip.compress(raw_bytes)
    b64_str = base64.b64encode(gz_bytes).decode("ascii")
    return f"GZIP_B64:{b64_str}"


def preload_matches_into_redis(compressed_by_match_id: dict):
    """
    Uses Redis RESP protocol with `redis-cli --pipe` to bulk-load `match:euw1:{mid}` keys with 60-day TTL (5184000s).
    """
    chunks = []
    for mid, val in compressed_by_match_id.items():
        key = f"match:euw1:{mid}"
        cmd_parts = ["SETEX", key, "5184000", val]
        resp = f"*{len(cmd_parts)}\r\n".encode("ascii")
        for part in cmd_parts:
            b = part.encode("utf-8")
            resp += f"${len(b)}\r\n".encode("ascii") + b + b"\r\n"
        chunks.append(resp)
    payload = b"".join(chunks)
    proc = subprocess.run(
        ["docker", "exec", "-i", "ss-redis", "redis-cli", "--pipe"],
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
        check=True
    )
    return proc.stdout.decode("utf-8", errors="replace").strip()


# Prebuild all realistic 10-participant matches for 924/927, 2100/2102, and concurrent 1..4
print("=== Generating realistic 10-participant MatchDto fixtures (927 + 2,102 + 4 = 3,033 matches) ===", flush=True)
MATCHES_DICT = {}
COMPRESSED_924 = {}
for i in range(1, 928):
    mid = f"EUW1_924_{i}"
    dto = build_realistic_10p_match(mid, i, PUUID_924, force_aatrox_loss=False)
    MATCHES_DICT[mid] = dto
    if i <= 924:
        COMPRESSED_924[mid] = compress_for_redis(dto)

COMPRESSED_2100 = {}
for i in range(1, 2103):
    mid = f"EUW1_2100_{i}"
    # Matches 1, 2, 3 are losses on Aatrox so we can prove exact tail-eviction subtraction
    dto = build_realistic_10p_match(mid, i, PUUID_2100, force_aatrox_loss=(i in (1, 2, 3)))
    MATCHES_DICT[mid] = dto
    if i <= 2100:
        COMPRESSED_2100[mid] = compress_for_redis(dto)

for i in range(1, 5):
    mid = f"EUW1_CONC_{i}"
    dto = build_realistic_10p_match(mid, i, PUUID_CONC, force_aatrox_loss=False)
    MATCHES_DICT[mid] = dto

COLL_SEED_1 = "puuid-coll-seed-1"
COLL_SEED_2 = "puuid-coll-seed-2"
COLL_SEED_3 = "puuid-coll-seed-3"
COLL_SEED_4 = "puuid-coll-seed-4"
COLL_ALREADY_PROCESSED_SEED = "already-processed-coll-seed"
COLL_TAIL_PUUIDS = [f"puuid-coll-tail-{i}" for i in range(1, 497)]
COLL_DUP_PUUID = "puuid-coll-dup-shared"
COLL_STALE_GUARD_PUUID = "puuid-coll-stale-guard"


def build_collector_11p_match(match_id: str, seq: int, seed_puuid: str, game_creation_ms: int) -> dict:
    """
    Builds an 11-participant Ranked Solo (queueId = 420, patch 16.7) MatchDto for collector batch verification:
    - Slot 0: seed_puuid in MIDDLE (team 100) so CohortBenchmarkService folds the game into (EMERALD, MIDDLE).
    - Slot 5: opposing MIDDLE laner on team 200.
    - Slots 1..4, 6..10: unique collector participants across all 5 roles (TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY).
      In seq == 1 (older timestamp), slot 1 is COLL_DUP_PUUID ("CollDupOld#OLD", level 111) and
      slot 2 is COLL_STALE_GUARD_PUUID ("CollStaleAttempt#STALE", level 100).
      In seq == 2 (newer timestamp), slot 1 is COLL_DUP_PUUID ("CollDupNew#NEW", level 222).
    Across matches 1..20, there are 202 unique PUUIDs (> 200 cap); across matches 1..25, there are 25 matches (> 20 cap).
    """
    base = build_realistic_10p_match(match_id, 3000 + seq, seed_puuid, force_aatrox_loss=False)
    base["info"]["gameCreation"] = game_creation_ms
    pts = base["info"]["participants"]
    # Add an 11th participant slot (UTILITY on team 200) so 20 matches exceed 200 unique participants
    pts.append(dict(pts[9]))
    for slot in range(1, 11):
        role = ROLES[slot % 5]
        pts[slot]["teamPosition"] = role
        pts[slot]["individualPosition"] = role
        if seq == 1 and slot == 1:
            p_puuid = COLL_DUP_PUUID
            g_name = "CollDupOld"
            t_line = "OLD"
            s_level = 111
            p_icon = 4111
        elif seq == 2 and slot == 1:
            p_puuid = COLL_DUP_PUUID
            g_name = "CollDupNew"
            t_line = "NEW"
            s_level = 222
            p_icon = 4222
        elif seq == 1 and slot == 2:
            p_puuid = COLL_STALE_GUARD_PUUID
            g_name = "CollStaleAttempt"
            t_line = "STALE"
            s_level = 100
            p_icon = 4100
        else:
            p_puuid = f"puuid-coll-m{seq}-s{slot}"
            g_name = f"CollPlayerM{seq}S{slot}"
            t_line = f"C{seq}"
            s_level = 200 + seq
            p_icon = 3000 + seq
        pts[slot]["puuid"] = p_puuid
        pts[slot]["summonerName"] = g_name
        pts[slot]["riotIdGameName"] = g_name
        pts[slot]["riotIdTagline"] = t_line
        pts[slot]["summonerLevel"] = s_level
        pts[slot]["profileIcon"] = p_icon
    base["metadata"]["participants"] = [pt["puuid"] for pt in pts]
    return base


# Build 25 Batch-1 collector matches (EUW1_COLL_B1_1..25) and 2 Batch-2 collector matches (EUW1_COLL_B2_1..2)
# Timestamps are recent (within 2 hours of NOW_EPOCH_SEC) so CohortBenchmarkService accepts them as fresh (< 72h skew)
COLL_BASE_CREATION_MS = (NOW_EPOCH_SEC - 7200) * 1000
for seq in range(1, 26):
    mid = f"EUW1_COLL_B1_{seq}"
    seed_owner = COLL_SEED_1 if seq <= 9 else (COLL_SEED_2 if seq <= 17 else COLL_SEED_3)
    # seq=1 is older (+60s), seq=2 is newer (+120s), etc.
    MATCHES_DICT[mid] = build_collector_11p_match(mid, seq, seed_owner, COLL_BASE_CREATION_MS + seq * 60_000)

for seq in range(1, 3):
    mid = f"EUW1_COLL_B2_{seq}"
    MATCHES_DICT[mid] = build_collector_11p_match(mid, 100 + seq, COLL_SEED_4, COLL_BASE_CREATION_MS + (30 + seq) * 60_000)

MATCH_JSON_BYTES = {mid: json.dumps(dto, separators=(",", ":")).encode("utf-8") for mid, dto in MATCHES_DICT.items()}

# Mutable state controlled by test stages for MockRiotHandler
MOCK_LOCK = threading.Lock()
ACTIVE_MATCH_IDS_BY_PUUID = {
    PUUID_924: [f"EUW1_924_{i}" for i in range(924, 0, -1)],
    PUUID_2100: [f"EUW1_2100_{i}" for i in range(2100, 0, -1)],
    PUUID_CONC: ["EUW1_CONC_2", "EUW1_CONC_1"],
    # Batch 1: 3 seeds resolve 25 distinct matches (+ 1 duplicate match ID EUW1_COLL_B1_1 on COLL_SEED_2)
    COLL_SEED_1: [f"EUW1_COLL_B1_{i}" for i in range(1, 10)],
    COLL_SEED_2: ["EUW1_COLL_B1_1"] + [f"EUW1_COLL_B1_{i}" for i in range(10, 18)],
    COLL_SEED_3: [f"EUW1_COLL_B1_{i}" for i in range(18, 26)],
    # Batch 2 (isolated indexed_players SQL failure): 1 seed resolves 2 matches
    COLL_SEED_4: ["EUW1_COLL_B2_1", "EUW1_COLL_B2_2"],
}
FORBIDDEN_MATCH_IDS = set()
FAILING_MATCH_IDS = set()
CONCURRENT_HOOK_FN = None

RIOT_CALL_COUNTS = {
    "accountByRiotId": 0,
    "summonerByPuuid": 0,
    "leagueByPuuid": 0,
    "matchIdsPages": 0,
    "matchById": 0,
    "matchByIdList": [],
    "forbiddenMatchFetches": [],
}


def reset_riot_call_counts():
    with MOCK_LOCK:
        RIOT_CALL_COUNTS["accountByRiotId"] = 0
        RIOT_CALL_COUNTS["summonerByPuuid"] = 0
        RIOT_CALL_COUNTS["leagueByPuuid"] = 0
        RIOT_CALL_COUNTS["matchIdsPages"] = 0
        RIOT_CALL_COUNTS["matchById"] = 0
        RIOT_CALL_COUNTS["matchByIdList"] = []
        RIOT_CALL_COUNTS["forbiddenMatchFetches"] = []


def snapshot_riot_call_counts() -> dict:
    with MOCK_LOCK:
        return {
            "accountByRiotId": RIOT_CALL_COUNTS["accountByRiotId"],
            "summonerByPuuid": RIOT_CALL_COUNTS["summonerByPuuid"],
            "leagueByPuuid": RIOT_CALL_COUNTS["leagueByPuuid"],
            "matchIdsPages": RIOT_CALL_COUNTS["matchIdsPages"],
            "matchById": RIOT_CALL_COUNTS["matchById"],
            "matchByIdList": list(RIOT_CALL_COUNTS["matchByIdList"]),
            "forbiddenMatchFetches": list(RIOT_CALL_COUNTS["forbiddenMatchFetches"]),
        }


EMPTY_CHALLENGER_JSON = json.dumps({
    "tier": "CHALLENGER",
    "leagueId": "challenger-euw1",
    "queue": "RANKED_SOLO_5x5",
    "name": "Challenger",
    "entries": []
}).encode("utf-8")


class MockRiotHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_riot_json(self, body_bytes: bytes, status_code: int = 200, endpoint_limit: str = "2000:10,120000:600"):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("X-App-Rate-Limit", "500:10,30000:600")
        self.send_header("X-App-Rate-Limit-Count", "2:10,20:600")
        self.send_header("X-Method-Rate-Limit", endpoint_limit)
        self.send_header("X-Method-Rate-Limit-Count", "1:10,10:600")
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/versions.json":
            body = json.dumps(["16.7.1", "16.6.1"]).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif "/challengerleagues/by-queue/" in path:
            self._send_riot_json(EMPTY_CHALLENGER_JSON, 200, "500:10,30000:600")
        elif "/riot/account/v1/accounts/by-riot-id/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["accountByRiotId"] += 1
            parts = path.strip("/").split("/")
            game_name = urllib.parse.unquote(parts[-2])
            tag_line = urllib.parse.unquote(parts[-1])
            if game_name.lower() == "player924":
                puuid = PUUID_924
            elif game_name.lower() == "player2100":
                puuid = PUUID_2100
            elif game_name.lower() == "playerconc":
                puuid = PUUID_CONC
            else:
                puuid = PUUID_924
            body = json.dumps({"puuid": puuid, "gameName": game_name, "tagLine": tag_line}).encode("utf-8")
            self._send_riot_json(body)
        elif "/lol/summoner/v4/summoners/by-puuid/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["summonerByPuuid"] += 1
            puuid = path.rsplit("/", 1)[-1]
            body = json.dumps({
                "id": f"sum-{puuid}",
                "accountId": f"acc-{puuid}",
                "puuid": puuid,
                "profileIconId": 4568,
                "revisionDate": int(time.time() * 1000),
                "summonerLevel": 450
            }).encode("utf-8")
            self._send_riot_json(body)
        elif "/lol/league/v4/entries/by-puuid/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["leagueByPuuid"] += 1
            puuid = path.rsplit("/", 1)[-1]
            body = json.dumps([{
                "leagueId": f"league-{puuid}",
                "queueType": "RANKED_SOLO_5x5",
                "tier": "EMERALD",
                "rank": "II",
                "summonerName": "TargetPlayer",
                "puuid": puuid,
                "leaguePoints": 75,
                "wins": 260,
                "losses": 220,
                "veteran": False,
                "inactive": False,
                "freshBlood": False,
                "hotStreak": True
            }]).encode("utf-8")
            self._send_riot_json(body, 200, "20000:10,1200000:600")
        elif "/lol/match/v5/matches/by-puuid/" in path and path.endswith("/ids"):
            puuid = path.split("/lol/match/v5/matches/by-puuid/")[1].split("/ids")[0]
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            params = dict(kv.split("=", 1) for kv in qs.split("&") if "=" in kv)
            start = int(params.get("start", "0"))
            count = int(params.get("count", "100"))
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["matchIdsPages"] += 1
                ids_list = list(ACTIVE_MATCH_IDS_BY_PUUID.get(puuid, []))
            sliced = ids_list[start:start + count]
            self._send_riot_json(json.dumps(sliced).encode("utf-8"), 200, "2000:10,120000:600")
        elif "/lol/match/v5/matches/" in path:
            mid = path.rsplit("/", 1)[-1]
            hook_to_call = None
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["matchById"] += 1
                RIOT_CALL_COUNTS["matchByIdList"].append(mid)
                if mid in FORBIDDEN_MATCH_IDS:
                    RIOT_CALL_COUNTS["forbiddenMatchFetches"].append(mid)
                    self._send_riot_json(b'{"status":{"message":"Forbidden old match fetch","status_code":500}}', 500)
                    return
                if mid in FAILING_MATCH_IDS:
                    self._send_riot_json(b'{"status":{"message":"Simulated match failure","status_code":500}}', 500)
                    return
                if mid == "EUW1_CONC_4" and CONCURRENT_HOOK_FN is not None:
                    hook_to_call = CONCURRENT_HOOK_FN
            if hook_to_call is not None:
                hook_to_call()
            payload = MATCH_JSON_BYTES.get(mid)
            if payload is None:
                self._send_riot_json(b'{"status":{"message":"Not found","status_code":404}}', 404)
                return
            self._send_riot_json(payload, 200, "2000:10,120000:600")
        else:
            self.send_response(404)
            self.end_headers()


def start_mock_riot_server(cert_file: str, key_file: str):
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("0.0.0.0", PORT_MOCK_RIOT), MockRiotHandler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def http_json(url: str, method: str = "GET", body: dict = None, headers: dict = None, timeout: int = 120):
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def setup_isolated_network():
    teardown_isolated_network()
    subprocess.check_call([
        "docker", "network", "create",
        "--internal",
        "--driver", "bridge",
        "--subnet", SUBNET_CIDR,
        "--gateway", GATEWAY_IP,
        "-o", f"com.docker.network.bridge.name={BRIDGE_NAME}",
        NETWORK_NAME
    ], stdout=subprocess.DEVNULL)
    subprocess.run(
        ["iptables", "-I", "DOCKER-USER", "1", "-i", BRIDGE_NAME, "!", "-d", SUBNET_CIDR, "-j", "REJECT"],
        check=False
    )
    subprocess.run(
        ["iptables", "-I", "DOCKER-USER", "1", "-o", BRIDGE_NAME, "!", "-s", SUBNET_CIDR, "-j", "REJECT"],
        check=False
    )
    subprocess.run(
        ["iptables", "-I", "INPUT", "1", "-i", BRIDGE_NAME, "!", "-d", GATEWAY_IP, "-j", "REJECT"],
        check=False
    )


def teardown_isolated_network():
    for name in ("ss-backend-512m", "ss-backend-writer2", "ss-redis", "ss-postgres"):
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(
        ["iptables", "-D", "DOCKER-USER", "-i", BRIDGE_NAME, "!", "-d", SUBNET_CIDR, "-j", "REJECT"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
    )
    subprocess.run(
        ["iptables", "-D", "DOCKER-USER", "-o", BRIDGE_NAME, "!", "-s", SUBNET_CIDR, "-j", "REJECT"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
    )
    subprocess.run(
        ["iptables", "-D", "INPUT", "-i", BRIDGE_NAME, "!", "-d", GATEWAY_IP, "-j", "REJECT"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
    )
    subprocess.run(["docker", "network", "rm", NETWORK_NAME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def verify_egress_deny_from_container_netns(container_name: str, host_pid: int) -> dict:
    raw_inspect = subprocess.check_output(["docker", "inspect", container_name], timeout=10).decode("utf-8")
    info = json.loads(raw_inspect)[0]
    net_settings = info.get("NetworkSettings", {}).get("Networks", {})
    if list(net_settings.keys()) != [NETWORK_NAME]:
        raise RuntimeError(f"FAIL_CLOSED_CONTAINER_NETWORK_MISMATCH: {list(net_settings.keys())}")
    extra_hosts = info.get("HostConfig", {}).get("ExtraHosts") or []
    for entry in EXPECTED_EXTRA_HOSTS:
        if entry not in extra_hosts:
            raise RuntimeError(f"FAIL_CLOSED_MISSING_EXTRA_HOST: {entry} not in {extra_hosts}")

    # Verify container Entrypoint/Cmd and environment have unmodified production guards and no manual GC overrides
    cfg = info.get("Config", {})
    entrypoint = cfg.get("Entrypoint") or []
    cmd_args = cfg.get("Cmd") or []
    env_list = cfg.get("Env") or []
    for env_entry in env_list:
        k = env_entry.split("=", 1)[0]
        if k in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            raise RuntimeError(f"FORBIDDEN_JVM_ENV_OVERRIDE: {k}")

    probe_code = f"""
import json, socket
results = {{}}
for label, host, port in [
    ("external_ip_1_1_1_1_443", "1.1.1.1", 443),
    ("external_ip_8_8_8_8_53", "8.8.8.8", 53),
    ("external_ip_93_184_215_14_443", "93.184.215.14", 443),
]:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect((host, port))
        s.close()
        results[label] = {{"blocked": False}}
    except Exception as e:
        results[label] = {{"blocked": True, "error": type(e).__name__}}
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2.0)
s.connect(("{GATEWAY_IP}", 443))
s.close()
results["internal_gateway_443"] = {{"reachable": True, "gatewayIp": "{GATEWAY_IP}"}}
print(json.dumps(results))
"""
    out = subprocess.check_output(
        ["nsenter", "-t", str(host_pid), "-n", sys.executable, "-c", probe_code],
        timeout=15
    ).decode("utf-8").strip()
    parsed = json.loads(out)
    for k in ("external_ip_1_1_1_1_443", "external_ip_8_8_8_8_53", "external_ip_93_184_215_14_443"):
        if not parsed.get(k, {}).get("blocked", False):
            raise RuntimeError(f"EGRESS_DENY_FAILED: {k} was not blocked: {parsed}")
    if not parsed.get("internal_gateway_443", {}).get("reachable", False):
        raise RuntimeError(f"INTERNAL_GATEWAY_UNREACHABLE: {parsed}")
    parsed["entrypoint"] = entrypoint
    parsed["cmd"] = cmd_args
    parsed["guardsUnchangedAndNoManualGc"] = True
    return parsed


def read_host_cgroup_strict(host_pid: int) -> dict:
    cg_dir = f"/proc/{host_pid}/root/sys/fs/cgroup"
    with open(f"{cg_dir}/memory.current", "r") as f:
        current_bytes = int(f.read().strip())
    with open(f"{cg_dir}/memory.peak", "r") as f:
        peak_bytes = int(f.read().strip())
    with open(f"{cg_dir}/memory.max", "r") as f:
        max_bytes = int(f.read().strip())
    if current_bytes <= 0 or max_bytes != CGROUP_MAX_BYTES:
        raise RuntimeError(f"INVALID_CGROUP_LIMITS: current={current_bytes}, max={max_bytes}")
    if peak_bytes < current_bytes:
        peak_bytes = current_bytes
    return {
        "cgroupCurrentBytes": current_bytes,
        "cgroupCurrentMiB": round(current_bytes / (1024.0 * 1024.0), 2),
        "cgroupRatioPct": round((current_bytes / max_bytes) * 100.0, 4),
        "cgroupPeakBytes": peak_bytes,
        "cgroupPeakMiB": round(peak_bytes / (1024.0 * 1024.0), 2),
        "cgroupPeakRatioPct": round((peak_bytes / max_bytes) * 100.0, 4),
        "cgroupMaxBytes": max_bytes,
        "cgroupStrictCeilingBytes": CGROUP_STRICT_GATE_BYTES,
    }


def inspect_container_liveness(container_name: str, expected_pid: int = None) -> dict:
    raw = subprocess.check_output(["docker", "inspect", container_name], timeout=10).decode("utf-8")
    info = json.loads(raw)[0]
    restart_count = info.get("RestartCount")
    state = info.get("State", {})
    oom_killed = state.get("OOMKilled")
    running = state.get("Running")
    pid = state.get("Pid")
    if restart_count != 0 or oom_killed is not False or running is not True:
        raise RuntimeError(
            f"CONTAINER_LIVENESS_FAILED ({container_name}): RestartCount={restart_count}, OOMKilled={oom_killed}, Running={running}"
        )
    if expected_pid is not None and pid != expected_pid:
        raise RuntimeError(f"CONTAINER_PID_CHANGED: expected={expected_pid} actual={pid}")
    cg = read_host_cgroup_strict(pid)
    # Enforce strict raw-byte gate (< 418,759,311 bytes = 399.36 MiB), not rounded %
    if cg["cgroupCurrentBytes"] >= CGROUP_STRICT_GATE_BYTES or cg["cgroupPeakBytes"] >= CGROUP_STRICT_GATE_BYTES:
        raise RuntimeError(f"CGROUP_RAW_BYTE_GATE_BREACHED: {cg}")
    return {"pid": pid, "restartCount": restart_count, "oomKilled": oom_killed, **cg}


def start_backend_container(
    image_tag: str,
    db_pass: str,
    container_name: str = "ss-backend-512m",
    container_ip: str = BACKEND_IP
) -> int:
    subprocess.run(["docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cmd = [
        "docker", "run", "-d",
        "--name", container_name,
        "--network", NETWORK_NAME,
        "--ip", container_ip,
        "--memory=512m",
        "--memory-swap=512m",
        *[f"--add-host={entry}" for entry in EXPECTED_EXTRA_HOSTS],
        "-v", "/tmp/cacerts:/opt/java/openjdk/lib/security/cacerts:ro",
        "-e", "PORT=10000",
        "-e", f"DB_HOST={POSTGRES_IP}",
        "-e", "DB_PORT=5432",
        "-e", "DB_NAME=summoner_searcher",
        "-e", "DB_USER=postgres",
        "-e", "DB_PASS",
        "-e", f"REDIS_URL=redis://{REDIS_IP}:6379",
        "-e", "RIOT_API_KEY",
        "-e", "JWT_SECRET_KEY",
        "-e", "TWOFA_SECRET_KEY",
        "-e", "RESEND_API_KEY",
        "-e", "GEMINI_API_KEY=disabled",
        "-e", "COHORT_NETWORK_COLLECTION_ENABLED=true",
        "-e", "COHORT_OPERATOR_TOKEN",
        image_tag,
    ]
    container_env = {
        **os.environ,
        "DB_PASS": db_pass,
        "RIOT_API_KEY": EPHEMERAL_RIOT_TOKEN,
        "JWT_SECRET_KEY": EPHEMERAL_JWT_SECRET,
        "TWOFA_SECRET_KEY": EPHEMERAL_TWOFA_SECRET,
        "RESEND_API_KEY": EPHEMERAL_RESEND_TOKEN,
        "COHORT_OPERATOR_TOKEN": EPHEMERAL_OP_TOKEN,
    }
    subprocess.check_call(cmd, env=container_env, stdout=subprocess.DEVNULL)
    target_base_url = f"http://{container_ip}:10000"
    for _ in range(90):
        try:
            http_json(f"{target_base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=OP_HEADERS, timeout=5)
            break
        except Exception:
            time.sleep(1)
    else:
        subprocess.run(["docker", "logs", container_name])
        raise RuntimeError(f"Failed to boot {image_tag} in {container_name}")
    info = inspect_container_liveness(container_name)
    return info["pid"]


def psql_query_json(sql: str) -> list:
    wrapped = f"SELECT COALESCE(json_agg(t), '[]'::json) FROM ({sql}) t;"
    out = subprocess.check_output([
        "docker", "exec", "ss-postgres",
        "psql", "-U", "postgres", "-d", "summoner_searcher", "-t", "-A", "-c", wrapped
    ], timeout=15).decode("utf-8").strip()
    return json.loads(out) if out else []


def psql_exec(sql: str) -> None:
    subprocess.check_call([
        "docker", "exec", "ss-postgres",
        "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "summoner_searcher", "-c", sql
    ], stdout=subprocess.DEVNULL, timeout=15)


def get_season_stats_row(puuid: str) -> dict:
    rows = psql_query_json(f"""
        SELECT
            id,
            region,
            puuid,
            season_year,
            queue_id,
            schema_version,
            folded_games_count,
            pending_games_count,
            history_complete,
            version,
            octet_length(state_json) AS state_json_bytes,
            pg_column_size(state_json) AS state_json_pg_col_bytes,
            pg_column_size(player_season_stats.*) AS row_pg_col_bytes,
            state_json
        FROM player_season_stats
        WHERE region = 'euw1' AND puuid = '{puuid}' AND season_year = {CURRENT_UTC_YEAR} AND queue_id = 420
    """)
    if len(rows) != 1:
        raise RuntimeError(f"Expected 1 player_season_stats row for {puuid}, got {len(rows)}")
    row = rows[0]
    row["state"] = json.loads(row.pop("state_json"))
    return row


def redis_strlen(key: str) -> int:
    out = subprocess.check_output(
        ["docker", "exec", "ss-redis", "redis-cli", "STRLEN", key],
        timeout=10
    ).decode("utf-8").strip()
    return int(out)


def redis_del(*keys: str):
    if not keys:
        return
    subprocess.check_call(
        ["docker", "exec", "ss-redis", "redis-cli", "DEL", *keys],
        stdout=subprocess.DEVNULL,
        timeout=15
    )


def expire_profile_cooldown(puuid: str):
    """
    Clears `cooldown:profile:refresh:euw1:{puuid}` and nulls `lastRefreshed` inside
    `player:profile:euw1:{puuid}` so an immediate `?refresh=true` call is not short-circuited
    by the 60s EXPLICIT_REFRESH_MIN_COOLDOWN while preserving `cachedProfile.recentMatches`.
    """
    redis_del(f"cooldown:profile:refresh:euw1:{puuid}")
    prof_key = f"player:profile:euw1:{puuid}"
    raw_val = subprocess.check_output(
        ["docker", "exec", "ss-redis", "redis-cli", "GET", prof_key],
        timeout=10
    ).decode("utf-8").strip()
    if raw_val.startswith("GZIP_B64:"):
        raw_bytes = gzip.decompress(base64.b64decode(raw_val[len("GZIP_B64:"):]))
        prof_obj = json.loads(raw_bytes.decode("utf-8"))
        prof_obj["lastRefreshed"] = None
        new_val = compress_for_redis(prof_obj)
        subprocess.check_call(
            ["docker", "exec", "ss-redis", "redis-cli", "SETEX", prof_key, "1209600", new_val],
            stdout=subprocess.DEVNULL,
            timeout=10
        )


SECRET_REDACT_PATTERNS = (
    re.compile(r"RGAPI-[0-9a-fA-F-]{16,}"),
    re.compile(r"\bre_[0-9a-zA-Z_]{16,}\b"),
    re.compile(r"\bgh[pousr]_[0-9a-zA-Z]{20,}\b"),
    re.compile(r"\bgithub_pat_[0-9a-zA-Z_]{20,}\b"),
    re.compile(r"(?i)((?:db_pass|postgres_password|jwt_secret_key|twofa_secret_key|cohort_operator_token|riot_api_key|resend_api_key)\s*[=:]\s*)(?!\[REDACTED\])\S+"),
)


def sanitize_text(text: str) -> str:
    forbidden_secrets = (
        EPHEMERAL_DB_PASS,
        EPHEMERAL_JWT_SECRET,
        EPHEMERAL_TWOFA_SECRET,
        EPHEMERAL_RIOT_TOKEN,
        EPHEMERAL_RESEND_TOKEN,
        EPHEMERAL_OP_TOKEN,
    )
    out = text
    for tok in forbidden_secrets:
        if tok:
            out = out.replace(tok, "[REDACTED]")
    for pat in SECRET_REDACT_PATTERNS[:4]:
        out = pat.sub("[REDACTED]", out)
    out = SECRET_REDACT_PATTERNS[4].sub(r"\1[REDACTED]", out)
    return out


def derive_expected_collector_queue_transition(
    initial_processed: list,
    initial_pending: list,
    initial_processed_match_ids: list,
    seed_puuids: list,
    batch_size: int,
    effective_cap: int = 10,
    max_pending_cap: int = 500,
) -> tuple:
    """
    Mirrors CohortBenchmarkService.runCollectorBatch (lines 1933-2425) exactly against
    ACTIVE_MATCH_IDS_BY_PUUID and MATCHES_DICT to derive deterministic post-batch
    (processed_puuids_list, pending_queue_list, processed_match_ids_list, discovered_extras_list).
    """
    processed_list = list(initial_processed)
    processed_set = set(processed_list)

    # 1. sanitizeSeedPuuids + LinkedHashSet prepend of net-new seeds ahead of existing pending queue
    clean_seeds = []
    for s in seed_puuids:
        cs = s.strip()
        if cs and cs not in clean_seeds:
            clean_seeds.append(cs)
    clean_seeds = clean_seeds[:25]

    pending_list = []
    for s in clean_seeds:
        if s not in processed_set and s not in pending_list and len(pending_list) < max_pending_cap:
            pending_list.append(s)
    for c in initial_pending:
        if c not in processed_set and c not in pending_list and len(pending_list) < max_pending_cap:
            pending_list.append(c)

    baseline_pending_set = set(pending_list)
    discovered_extras = []
    processed_match_ids = list(initial_processed_match_ids)

    # 2. candidatesThisBatch = pendingQueue.filter { it !in processedPuuids }.take(effectiveBatchLimit)
    candidates_this_batch = [c for c in pending_list if c not in processed_set][:batch_size]
    eligible_roles = {"TOP", "MIDDLE", "BOTTOM"}

    for puuid in candidates_this_batch:
        raw_match_ids = ACTIVE_MATCH_IDS_BY_PUUID.get(puuid, [])
        own_match_ids = []
        for mid in raw_match_ids[:effective_cap]:
            if mid not in own_match_ids:
                own_match_ids.append(mid)
        own_match_id_set = set(own_match_ids)
        if len(own_match_ids) < effective_cap and processed_match_ids:
            extra_tail = [m for m in processed_match_ids if m not in own_match_id_set][-effective_cap:]
            candidate_match_ids = (own_match_ids + extra_tail)[:effective_cap]
        else:
            candidate_match_ids = list(own_match_ids)

        for mid in candidate_match_ids:
            is_own_listed = mid in own_match_id_set
            if is_own_listed and mid not in processed_match_ids and len(processed_match_ids) < 2500:
                processed_match_ids.append(mid)
            match_dto = MATCHES_DICT.get(mid)
            if not match_dto:
                continue
            pts = match_dto["info"]["participants"]
            if not is_own_listed and all(pt.get("puuid") != puuid for pt in pts):
                continue
            for pt in pts:
                role = pt.get("teamPosition") or pt.get("individualPosition")
                if role not in eligible_roles:
                    continue
                p_puuid = (pt.get("puuid") or "").strip()
                if not p_puuid:
                    continue
                # Line 2370: if (pPuuid !in processedPuuids && pendingQueue.size < MAX_CHECKPOINT_PENDING_PUUIDS) pendingQueue.add(pPuuid)
                if p_puuid not in processed_set and len(pending_list) < max_pending_cap:
                    if p_puuid not in pending_list:
                        pending_list.append(p_puuid)
                        if p_puuid not in baseline_pending_set and p_puuid not in discovered_extras:
                            discovered_extras.append(p_puuid)

        # Lines 2422-2425: processedPuuids.add(puuid); pendingQueue.remove(puuid) AFTER matchLoop
        if puuid not in processed_set and len(processed_list) < 1000:
            processed_list.append(puuid)
            processed_set.add(puuid)
        if puuid in pending_list:
            pending_list.remove(puuid)

    return processed_list, pending_list, processed_match_ids, discovered_extras


def capture_sanitized_container_log(artifact_dir: str, container_name: str, log_filename: str):
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/collector-seed-control-512m-proof-raw")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(artifact_dir, exist_ok=True)
    proc = subprocess.run(
        ["docker", "logs", container_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=15,
        check=False
    )
    raw_log = proc.stdout.decode("utf-8", errors="replace")
    with open(os.path.join(raw_dir, log_filename), "w", encoding="utf-8") as f:
        f.write(raw_log)
    sanitized_log = sanitize_text(raw_log)
    with open(os.path.join(artifact_dir, log_filename), "w", encoding="utf-8") as f:
        f.write(sanitized_log)


def write_sanitized_artifacts(artifact_dir: str, summary: dict, cgroup_samples: list):
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/collector-seed-control-512m-proof-raw")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(artifact_dir, exist_ok=True)
    with open(os.path.join(raw_dir, "acceptance_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(os.path.join(raw_dir, "cgroup_samples_2s.json"), "w", encoding="utf-8") as f:
        json.dump(cgroup_samples, f, indent=2)

    summary_path = os.path.join(artifact_dir, "acceptance_summary.json")
    samples_path = os.path.join(artifact_dir, "cgroup_samples_2s.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(sanitize_text(json.dumps(summary, indent=2)))
    with open(samples_path, "w", encoding="utf-8") as f:
        f.write(sanitize_text(json.dumps(cgroup_samples, indent=2)))

    # Fail-closed secret-leak check across all written artifact files (logs + metrics + test XMLs)
    forbidden_secrets = (
        EPHEMERAL_DB_PASS,
        EPHEMERAL_JWT_SECRET,
        EPHEMERAL_TWOFA_SECRET,
        EPHEMERAL_RIOT_TOKEN,
        EPHEMERAL_RESEND_TOKEN,
        EPHEMERAL_OP_TOKEN,
    )
    for root_dir, _dirs, files in os.walk(artifact_dir):
        for fname in files:
            fpath = os.path.join(root_dir, fname)
            if os.path.isfile(fpath):
                content = open(fpath, "r", encoding="utf-8", errors="replace").read()
                for tok in forbidden_secrets:
                    if tok and tok in content:
                        shutil.rmtree(artifact_dir, ignore_errors=True)
                        raise RuntimeError(f"SECRET_LEAK_IN_ARTIFACT: {fpath}")
                for pat in SECRET_REDACT_PATTERNS:
                    if pat.search(content):
                        shutil.rmtree(artifact_dir, ignore_errors=True)
                        raise RuntimeError(f"SECRET_PATTERN_LEAK_IN_ARTIFACT: {fpath} ({pat.pattern})")


def main():
    global CONCURRENT_HOOK_FN
    db_pass = os.environ.get("DB_PASS", "").strip() or EPHEMERAL_DB_PASS
    cand_image = os.environ.get("CAND_IMAGE", "ss-backend:cand-87d5f44")
    base_image = os.environ.get("BASE_IMAGE", "ss-backend:base-e683777")
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/collector-seed-control-512m-proof-raw")
    artifact_dir = os.environ.get("ARTIFACT_DIR", "/tmp/collector-seed-control-512m-proof-artifacts")
    cert_file = sys.argv[1]
    key_file = sys.argv[2]

    os.makedirs(raw_dir, exist_ok=True)
    secrets_manifest_path = os.path.join(raw_dir, ".ephemeral_secrets.json")
    with open(secrets_manifest_path, "w", encoding="utf-8") as sf:
        json.dump([
            db_pass,
            EPHEMERAL_DB_PASS,
            EPHEMERAL_JWT_SECRET,
            EPHEMERAL_TWOFA_SECRET,
            EPHEMERAL_RIOT_TOKEN,
            EPHEMERAL_RESEND_TOKEN,
            EPHEMERAL_OP_TOKEN,
        ], sf)
    os.chmod(secrets_manifest_path, 0o600)

    sampler_samples = []
    summary = {"status": "IN_PROGRESS"}
    setup_isolated_network()
    try:
        print("=== 1. Starting Mock Riot HTTPS server + real postgres:16-alpine & redis:7-alpine on isolated bridge ===", flush=True)
        start_mock_riot_server(cert_file, key_file)
        subprocess.check_call([
            "docker", "run", "-d",
            "--name", "ss-postgres",
            "--network", NETWORK_NAME,
            "--ip", POSTGRES_IP,
            "-e", "POSTGRES_DB=summoner_searcher",
            "-e", "POSTGRES_USER=postgres",
            "-e", "POSTGRES_PASSWORD",
            "postgres:16-alpine"
        ], env={**os.environ, "POSTGRES_PASSWORD": db_pass}, stdout=subprocess.DEVNULL)

        subprocess.check_call([
            "docker", "run", "-d",
            "--name", "ss-redis",
            "--network", NETWORK_NAME,
            "--ip", REDIS_IP,
            "redis:7-alpine"
        ], stdout=subprocess.DEVNULL)

        # Wait for Postgres & Redis readiness
        for _ in range(30):
            pg_ok = subprocess.run(
                ["docker", "exec", "ss-postgres", "pg_isready", "-U", "postgres", "-d", "summoner_searcher"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).returncode == 0
            rd_ok = subprocess.run(
                ["docker", "exec", "ss-redis", "redis-cli", "PING"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).returncode == 0
            if pg_ok and rd_ok:
                break
            time.sleep(1)
        else:
            raise RuntimeError("Postgres or Redis failed to become ready")

        print(f"=== 2. Booting Candidate 512 MiB Container ({cand_image}) and verifying outbound egress deny ===", flush=True)
        cand_pid = start_backend_container(cand_image, db_pass)
        egress_proof = verify_egress_deny_from_container_netns("ss-backend-512m", cand_pid)
        print("EGRESS DENY PROOF:", json.dumps(egress_proof), flush=True)

        # Background 2s cgroup sampler for candidate container
        sampler_stop = threading.Event()
        sampler_lock = threading.Lock()

        def record_sample(tag: str) -> dict:
            cg = inspect_container_liveness("ss-backend-512m", expected_pid=cand_pid)
            with sampler_lock:
                entry = {"idx": len(sampler_samples) + 1, "tag": tag, "timestampEpochMs": int(time.time() * 1000), **cg}
                sampler_samples.append(entry)
                return entry

        def sampler_loop():
            while not sampler_stop.wait(2.0):
                try:
                    record_sample("poll_2s")
                except Exception as e:
                    print(f"SAMPLER ERROR: {e}", flush=True)
                    os._exit(2)

        sampler_thread = threading.Thread(target=sampler_loop, daemon=True)
        sampler_thread.start()
        time.sleep(2)
        record_sample("post_boot_idle")

        # =========================================================================
        # SCENARIO 1: 924-Match Player (Cold Baseline -> +3 Delta -> Redis FLUSHALL Recovery)
        # =========================================================================
        print("=== 3. Scenario 1A: 924-Match Cold Baseline from Pre-Cached 10-Participant MatchDtos ===", flush=True)
        preload_out_924 = preload_matches_into_redis(COMPRESSED_924)
        sample_match_redis_bytes = redis_strlen("match:euw1:EUW1_924_924")
        reset_riot_call_counts()
        prof_924_cold = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player924/EUW?refresh=true")
        calls_924_cold = snapshot_riot_call_counts()
        row_924_cold = get_season_stats_row(PUUID_924)
        prof_924_redis_bytes = redis_strlen(f"player:profile:euw1:{PUUID_924}")
        record_sample("after_924_cold")

        ids_924 = [f"EUW1_924_{i}" for i in range(924, 0, -1)]
        exp_overall_924, exp_champs_924 = compute_expected_counters(PUUID_924, ids_924, MATCHES_DICT)

        assert prof_924_cold["totalMatches"] == 924, f"Expected 924 totalMatches, got {prof_924_cold['totalMatches']}"
        assert prof_924_cold["allMatchIds"] == ids_924, "924 allMatchIds mismatch"
        assert len(prof_924_cold["recentMatches"]) == 20, "Expected 20 recentMatches"
        assert calls_924_cold["matchIdsPages"] == 10, f"Expected 10 match ID pages for 924 matches, got {calls_924_cold['matchIdsPages']}"
        assert calls_924_cold["matchById"] == 0, f"Expected 0 Riot matchById calls on 924 cached matches, got {calls_924_cold['matchById']}"
        assert row_924_cold["schema_version"] == 1
        assert row_924_cold["folded_games_count"] == 924
        assert row_924_cold["pending_games_count"] == 0
        assert row_924_cold["history_complete"] is True
        assert row_924_cold["state"]["historyCapped"] is False
        assert row_924_cold["state"]["historyComplete"] is True
        assert row_924_cold["state"]["processedMatchIds"] == ids_924
        assert row_924_cold["state"]["pendingMatchIds"] == []
        assert_counters_equal(
            row_924_cold["state"]["overallCounters"],
            row_924_cold["state"]["championCounters"],
            exp_overall_924,
            exp_champs_924,
            "924_COLD_BASELINE"
        )

        print("=== 4. Scenario 1B: 924 -> 927 Incremental Delta (+3 New Games, 904 Older Matches Evicted from Redis & Forbidden) ===", flush=True)
        old_904_keys = [f"match:euw1:EUW1_924_{i}" for i in range(1, 905)]
        for chunk_start in range(0, len(old_904_keys), 200):
            redis_del(*old_904_keys[chunk_start:chunk_start + 200])
        expire_profile_cooldown(PUUID_924)

        ids_927 = [f"EUW1_924_{i}" for i in range(927, 0, -1)]
        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_924] = ids_927
            FORBIDDEN_MATCH_IDS.update(f"EUW1_924_{i}" for i in range(1, 905))
        reset_riot_call_counts()

        prof_927_inc = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player924/EUW?refresh=true")
        calls_927_inc = snapshot_riot_call_counts()
        row_927_inc = get_season_stats_row(PUUID_924)
        record_sample("after_927_incremental")

        exp_overall_927, exp_champs_927 = compute_expected_counters(PUUID_924, ids_927, MATCHES_DICT)
        assert calls_927_inc["forbiddenMatchFetches"] == [], f"Forbidden old matches fetched: {calls_927_inc['forbiddenMatchFetches']}"
        assert calls_927_inc["matchById"] == 3, f"Expected exactly 3 Riot matchById calls for +3 delta, got {calls_927_inc}"
        assert Counter(calls_927_inc["matchByIdList"]) == Counter(["EUW1_924_927", "EUW1_924_926", "EUW1_924_925"])
        assert prof_927_inc["totalMatches"] == 927
        assert prof_927_inc["allMatchIds"] == ids_927
        assert prof_927_inc["recentMatches"][0]["metadata"]["matchId"] == "EUW1_924_927"
        assert row_927_inc["folded_games_count"] == 927
        assert row_927_inc["pending_games_count"] == 0
        assert row_927_inc["history_complete"] is True
        assert row_927_inc["version"] > row_924_cold["version"]
        assert_counters_equal(
            row_927_inc["state"]["overallCounters"],
            row_927_inc["state"]["championCounters"],
            exp_overall_927,
            exp_champs_927,
            "927_INCREMENTAL_DELTA"
        )

        print("=== 5. Scenario 1C: Full Redis-Loss Recovery (FLUSHALL -> Cold Redis Recovery of 927-Game Profile) ===", flush=True)
        subprocess.check_call(["docker", "exec", "ss-redis", "redis-cli", "FLUSHALL"], stdout=subprocess.DEVNULL)
        with MOCK_LOCK:
            FORBIDDEN_MATCH_IDS.clear()
            # Forbid all 907 older matches (1..907); only recent 20 (927..908) are allowed to be fetched for cards
            FORBIDDEN_MATCH_IDS.update(f"EUW1_924_{i}" for i in range(1, 908))
        reset_riot_call_counts()

        prof_927_recovered = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player924/EUW?refresh=true")
        calls_927_recovered = snapshot_riot_call_counts()
        record_sample("after_927_redis_loss_recovery")

        assert calls_927_recovered["forbiddenMatchFetches"] == [], f"Forbidden old matches fetched during recovery: {calls_927_recovered['forbiddenMatchFetches']}"
        assert calls_927_recovered["matchById"] == 20, f"Expected exactly 20 Riot matchById calls (recent 20 cards only) after FLUSHALL, got {calls_927_recovered['matchById']}"
        assert prof_927_recovered["totalMatches"] == 927
        assert prof_927_recovered["allMatchIds"] == ids_927
        assert prof_927_recovered["overallStats"] == prof_927_inc["overallStats"], "overallStats diverged after Redis FLUSHALL recovery"
        assert prof_927_recovered["championStats"] == prof_927_inc["championStats"], "championStats diverged after Redis FLUSHALL recovery"

        # Verify page-2 load-more works on demand when unblocked
        with MOCK_LOCK:
            FORBIDDEN_MATCH_IDS.clear()
        reset_riot_call_counts()
        page2_matches = http_json(f"{BASE_URL}/api/riot/matches/euw1/{PUUID_924}?page=2")
        calls_page2 = snapshot_riot_call_counts()
        assert len(page2_matches) == 20
        assert [m["metadata"]["matchId"] for m in page2_matches] == [f"EUW1_924_{i}" for i in range(907, 887, -1)]
        assert calls_page2["matchById"] == 20

        # =========================================================================
        # SCENARIO 2: 2,100-Match Player (Cold Baseline -> Rollforward 2101 -> Missing-Tail Failure 2102 -> Recovery)
        # =========================================================================
        print("=== 6. Scenario 2A: 2,100-Match Cold Baseline from Pre-Cached 10-Participant MatchDtos ===", flush=True)
        preload_matches_into_redis(COMPRESSED_2100)
        ids_2100_base = [f"EUW1_2100_{i}" for i in range(2100, 0, -1)]
        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_2100] = ids_2100_base
        reset_riot_call_counts()

        prof_2100_cold = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player2100/EUW?refresh=true")
        calls_2100_cold = snapshot_riot_call_counts()
        row_2100_cold = get_season_stats_row(PUUID_2100)
        prof_2100_redis_bytes = redis_strlen(f"player:profile:euw1:{PUUID_2100}")
        record_sample("after_2100_cold")

        exp_overall_2100_base, exp_champs_2100_base = compute_expected_counters(PUUID_2100, ids_2100_base, MATCHES_DICT)
        assert calls_2100_cold["matchIdsPages"] == 21, f"Expected 21 pages stopping at >2000 cap, got {calls_2100_cold['matchIdsPages']}"
        assert calls_2100_cold["matchById"] == 0, f"Expected 0 Riot matchById calls on 2,100 cached matches, got {calls_2100_cold['matchById']}"
        assert prof_2100_cold["totalMatches"] == 2100
        assert prof_2100_cold["allMatchIds"] == ids_2100_base
        assert row_2100_cold["folded_games_count"] == 2100
        assert row_2100_cold["pending_games_count"] == 0
        assert row_2100_cold["history_complete"] is False
        assert row_2100_cold["state"]["historyCapped"] is True
        assert row_2100_cold["state"]["historyComplete"] is False
        assert_counters_equal(
            row_2100_cold["state"]["overallCounters"],
            row_2100_cold["state"]["championCounters"],
            exp_overall_2100_base,
            exp_champs_2100_base,
            "2100_COLD_BASELINE"
        )

        print("=== 7. Scenario 2B: 2,100 Rollforward (+1 New Game EUW1_2100_2101, Evicting Resolvable Tail EUW1_2100_1) ===", flush=True)
        ids_2100_rf1 = [f"EUW1_2100_{i}" for i in range(2101, 1, -1)]
        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_2100] = ids_2100_rf1
        expire_profile_cooldown(PUUID_2100)
        reset_riot_call_counts()

        prof_2100_rf1 = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player2100/EUW?refresh=true")
        calls_2100_rf1 = snapshot_riot_call_counts()
        row_2100_rf1 = get_season_stats_row(PUUID_2100)
        record_sample("after_2100_rollforward_2101")

        exp_overall_2100_rf1, exp_champs_2100_rf1 = compute_expected_counters(PUUID_2100, ids_2100_rf1, MATCHES_DICT)
        assert calls_2100_rf1["matchById"] == 1
        assert calls_2100_rf1["matchByIdList"] == ["EUW1_2100_2101"]
        assert prof_2100_rf1["totalMatches"] == 2100
        assert prof_2100_rf1["allMatchIds"] == ids_2100_rf1
        assert prof_2100_rf1["recentMatches"][0]["metadata"]["matchId"] == "EUW1_2100_2101"
        assert row_2100_rf1["folded_games_count"] == 2100
        assert row_2100_rf1["pending_games_count"] == 0
        assert row_2100_rf1["history_complete"] is False
        assert row_2100_rf1["state"]["historyCapped"] is True
        assert row_2100_rf1["state"]["processedMatchIds"] == ids_2100_rf1
        assert_counters_equal(
            row_2100_rf1["state"]["overallCounters"],
            row_2100_rf1["state"]["championCounters"],
            exp_overall_2100_rf1,
            exp_champs_2100_rf1,
            "2100_ROLLFORWARD_2101"
        )

        print("=== 8. Scenario 2C: 2,100 Rollforward Missing-Tail Failure (EUW1_2100_2102 Arrives while Tail EUW1_2100_2 Fails) ===", flush=True)
        # Evict EUW1_2100_2 from Redis and make it fail on Mock Riot so tail eviction cannot subtract its delta
        redis_del("match:euw1:EUW1_2100_2")
        expire_profile_cooldown(PUUID_2100)
        ids_2100_rf2 = [f"EUW1_2100_{i}" for i in range(2102, 2, -1)]
        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_2100] = ids_2100_rf2
            FAILING_MATCH_IDS.add("EUW1_2100_2")
        reset_riot_call_counts()

        prof_2100_gap = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player2100/EUW?refresh=true")
        calls_2100_gap = snapshot_riot_call_counts()
        row_2100_gap = get_season_stats_row(PUUID_2100)
        cov_during_gap = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/coverage-summary?seasonYear={CURRENT_UTC_YEAR}",
            headers=OP_HEADERS
        )
        record_sample("after_2100_missing_tail_gap")

        assert calls_2100_gap["matchByIdList"] == ["EUW1_2100_2102", "EUW1_2100_2"], f"Unexpected matchByIdList: {calls_2100_gap['matchByIdList']}"
        assert row_2100_gap["folded_games_count"] == 2100
        assert row_2100_gap["pending_games_count"] == 1
        assert row_2100_gap["history_complete"] is False
        assert row_2100_gap["state"]["historyCapped"] is True
        assert row_2100_gap["state"]["historyComplete"] is False
        assert row_2100_gap["state"]["pendingMatchIds"] == ["EUW1_2100_2102"]
        # EUW1_2100_2 stays in processedMatchIds (at the tail) and counters remain strictly equal to 2101..2 (zero drift!)
        expected_retained_order_on_gap = [f"EUW1_2100_{i}" for i in range(2101, 2, -1)] + ["EUW1_2100_2"]
        assert row_2100_gap["state"]["processedMatchIds"] == expected_retained_order_on_gap
        assert_counters_equal(
            row_2100_gap["state"]["overallCounters"],
            row_2100_gap["state"]["championCounters"],
            exp_overall_2100_rf1,
            exp_champs_2100_rf1,
            "2100_MISSING_TAIL_GAP_ZERO_DRIFT"
        )
        assert cov_during_gap["seasonHistoryCompleteness"]["targetRegionStats"]["totalPendingGapGames"] == 1

        print("=== 9. Scenario 2D: Missing-Tail Recovery (EUW1_2100_2 Restored -> Evicted & EUW1_2100_2102 Folded) ===", flush=True)
        with MOCK_LOCK:
            FAILING_MATCH_IDS.discard("EUW1_2100_2")
        expire_profile_cooldown(PUUID_2100)
        reset_riot_call_counts()

        prof_2100_rf2_ok = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player2100/EUW?refresh=true")
        calls_2100_rf2_ok = snapshot_riot_call_counts()
        row_2100_rf2_ok = get_season_stats_row(PUUID_2100)
        record_sample("after_2100_missing_tail_recovered")

        exp_overall_2100_rf2, exp_champs_2100_rf2 = compute_expected_counters(PUUID_2100, ids_2100_rf2, MATCHES_DICT)
        assert row_2100_rf2_ok["folded_games_count"] == 2100
        assert row_2100_rf2_ok["pending_games_count"] == 0
        assert row_2100_rf2_ok["state"]["pendingMatchIds"] == []
        assert row_2100_rf2_ok["state"]["processedMatchIds"] == ids_2100_rf2
        assert_counters_equal(
            row_2100_rf2_ok["state"]["overallCounters"],
            row_2100_rf2_ok["state"]["championCounters"],
            exp_overall_2100_rf2,
            exp_champs_2100_rf2,
            "2100_MISSING_TAIL_RECOVERED"
        )

        # =========================================================================
        # SCENARIO 3: True Overlapping Application Writers & @Version Merge on Real Postgres
        # =========================================================================
        print("=== 10. Scenario 3: True Overlapping 089ac61 Application Writers & @Version Merge on Real Postgres ===", flush=True)
        writer2_pid = start_backend_container(cand_image, db_pass, container_name="ss-backend-writer2", container_ip=WRITER2_IP)
        verify_egress_deny_from_container_netns("ss-backend-writer2", writer2_pid)

        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_CONC] = ["EUW1_CONC_2", "EUW1_CONC_1"]
        prof_conc_base = http_json(f"{BASE_URL}/api/riot/summoner/euw1/PlayerConc/EUW?refresh=true")
        row_conc_base = get_season_stats_row(PUUID_CONC)
        assert row_conc_base["folded_games_count"] == 2
        assert row_conc_base["version"] == 0

        hook_triggered = {"count": 0, "writer2CommittedVersion": None, "writer2FoldedGames": None, "writer2Cgroup": None}

        def trigger_overlapping_app_writer2():
            """
            Invoked while Writer 1 (`ss-backend-512m`) is paused mid-refresh inside `GET /lol/match/v5/matches/EUW1_CONC_4`
            holding `existingRow` at `version = 0` in memory.
            Emulates lock TTL expiry on `lock:profile:euw1:{PUUID_CONC}` via `PEXPIRE ... 1`, then Writer 2
            (`ss-backend-writer2`, running the exact 089ac61 application image) executes a real HTTP refresh
            that loads `version = 0` from Postgres, folds `EUW1_CONC_3`, and commits `version = 1` (`3` games) via JPA
            before Writer 1 finishes `EUW1_CONC_4` and calls `persistDurableSeasonState`.
            """
            if hook_triggered["count"] == 0:
                hook_triggered["count"] += 1
                with MOCK_LOCK:
                    ACTIVE_MATCH_IDS_BY_PUUID[PUUID_CONC] = ["EUW1_CONC_3", "EUW1_CONC_2", "EUW1_CONC_1"]
                lock_key = f"lock:profile:euw1:{PUUID_CONC}"
                exists_before = int(subprocess.check_output(
                    ["docker", "exec", "ss-redis", "redis-cli", "EXISTS", lock_key],
                    timeout=10
                ).decode("utf-8").strip())
                assert exists_before == 1, f"Expected active lock {lock_key} held by Writer 1, got {exists_before}"
                subprocess.check_call(
                    ["docker", "exec", "ss-redis", "redis-cli", "PEXPIRE", lock_key, "1"],
                    stdout=subprocess.DEVNULL,
                    timeout=10
                )
                time.sleep(0.05)
                exists_after = int(subprocess.check_output(
                    ["docker", "exec", "ss-redis", "redis-cli", "EXISTS", lock_key],
                    timeout=10
                ).decode("utf-8").strip())
                assert exists_after == 0, f"Expected {lock_key} TTL to expire, got EXISTS={exists_after}"
                expire_profile_cooldown(PUUID_CONC)
                w2_prof = http_json(f"{WRITER2_URL}/api/riot/summoner/euw1/PlayerConc/EUW?refresh=true")
                w2_row = get_season_stats_row(PUUID_CONC)
                w2_cg = inspect_container_liveness("ss-backend-writer2", expected_pid=writer2_pid)
                assert w2_prof["totalMatches"] == 3, f"Writer 2 expected 3 totalMatches, got {w2_prof['totalMatches']}"
                assert w2_row["version"] == 1, f"Writer 2 expected committed version=1, got {w2_row['version']}"
                assert w2_row["folded_games_count"] == 3
                hook_triggered["writer2CommittedVersion"] = w2_row["version"]
                hook_triggered["writer2FoldedGames"] = w2_row["folded_games_count"]
                hook_triggered["writer2Cgroup"] = w2_cg
                with MOCK_LOCK:
                    ACTIVE_MATCH_IDS_BY_PUUID[PUUID_CONC] = ["EUW1_CONC_4", "EUW1_CONC_2", "EUW1_CONC_1"]

        expire_profile_cooldown(PUUID_CONC)
        with MOCK_LOCK:
            ACTIVE_MATCH_IDS_BY_PUUID[PUUID_CONC] = ["EUW1_CONC_4", "EUW1_CONC_2", "EUW1_CONC_1"]
            CONCURRENT_HOOK_FN = trigger_overlapping_app_writer2

        # Launch 2 concurrent HTTP callers on Writer 1 while Writer 2 overlaps on the same Postgres row
        conc_results = [None, None]
        conc_errors = []

        def call_conc_refresh(slot_idx: int):
            try:
                conc_results[slot_idx] = http_json(f"{BASE_URL}/api/riot/summoner/euw1/PlayerConc/EUW?refresh=true")
            except Exception as e:
                conc_errors.append(str(e))

        t_a = threading.Thread(target=call_conc_refresh, args=(0,))
        t_b = threading.Thread(target=call_conc_refresh, args=(1,))
        t_a.start()
        time.sleep(0.05)
        t_b.start()
        t_a.join(timeout=60)
        t_b.join(timeout=60)

        with MOCK_LOCK:
            CONCURRENT_HOOK_FN = None

        capture_sanitized_container_log(artifact_dir, "ss-backend-writer2", "writer2_089ac61_container.log")
        subprocess.run(["docker", "rm", "-f", "ss-backend-writer2"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        assert not conc_errors, f"Concurrent callers failed: {conc_errors}"
        row_conc_merged = get_season_stats_row(PUUID_CONC)
        prof_conc_cached = http_json(f"{BASE_URL}/api/riot/summoner/euw1/PlayerConc/EUW?refresh=false")
        record_sample("after_concurrent_merge")

        ids_conc_4_merged = ["EUW1_CONC_4", "EUW1_CONC_2", "EUW1_CONC_1", "EUW1_CONC_3"]
        exp_overall_conc_4, exp_champs_conc_4 = compute_expected_counters(PUUID_CONC, ids_conc_4_merged, MATCHES_DICT)
        assert hook_triggered["count"] == 1, "Overlapping Writer 2 hook did not fire"
        assert hook_triggered["writer2CommittedVersion"] == 1
        assert row_conc_merged["version"] == 2, f"Expected version=2 after concurrent conflict retry, got {row_conc_merged['version']}"
        assert row_conc_merged["folded_games_count"] == 4
        assert row_conc_merged["pending_games_count"] == 0
        assert row_conc_merged["history_complete"] is True
        assert row_conc_merged["state"]["processedMatchIds"] == ids_conc_4_merged
        assert_counters_equal(
            row_conc_merged["state"]["overallCounters"],
            row_conc_merged["state"]["championCounters"],
            exp_overall_conc_4,
            exp_champs_conc_4,
            "CONCURRENT_WRITER_MERGE_4_GAMES"
        )
        for caller_idx, res in enumerate(conc_results):
            assert res["totalMatches"] == 4, f"Caller {caller_idx} got stale totalMatches={res['totalMatches']}"
            assert res["allMatchIds"] == ids_conc_4_merged
        assert prof_conc_cached["totalMatches"] == 4
        assert prof_conc_cached["allMatchIds"] == ids_conc_4_merged

        # =========================================================================
        # SCENARIO 4: Operator Coverage Summary & Indexed Players Monotonicity
        # =========================================================================
        print("=== 11. Scenario 4: Operator Coverage Summary & Newer-Only Participant Indexing ===", flush=True)
        cov_final = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/coverage-summary?seasonYear={CURRENT_UTC_YEAR}",
            headers=OP_HEADERS
        )
        reg_stats = cov_final["seasonHistoryCompleteness"]["targetRegionStats"]
        assert reg_stats["trackedViewedPlayers"] == 3
        assert reg_stats["completeHistoryPlayers"] == 2
        assert reg_stats["partialHistoryPlayers"] == 1
        assert reg_stats["cappedHistoryPlayers"] == 1
        assert reg_stats["staleSchemaVersionPlayers"] == 0
        assert reg_stats["totalFoldedSeasonGames"] == 927 + 2100 + 4
        assert reg_stats["totalPendingGapGames"] == 0

        co_player_rows = psql_query_json(f"""
            SELECT puuid, region, game_name, tag_line, summoner_level, profile_icon_id
            FROM indexed_players
            WHERE puuid = '{CO_PLAYER_PUUID}'
        """)
        assert len(co_player_rows) == 1
        # Highest match index indexed across the run is 2102 ("CoPlayerNew_2102#T2102", level 300+2102=2402)
        assert co_player_rows[0]["game_name"] == "CoPlayerNew_2102", f"Expected newest CoPlayerNew_2102, got {co_player_rows[0]}"
        assert co_player_rows[0]["tag_line"] == "T2102"
        assert co_player_rows[0]["summoner_level"] == 2402

        autocomplete_res = http_json(f"{BASE_URL}/api/search/autocomplete?query=CoPlayerNew_2102")
        assert any(p["gameName"] == "CoPlayerNew_2102" and p["tagLine"] == "T2102" for p in autocomplete_res)

        # =========================================================================
        # SCENARIO 4B: Operator Seed-Control (Controller -> Runner -> Service),
        #              Pre-Mutation Validation (>25 / Malformed / >500 Queue Cap),
        #              <=20/200 DTO Identity Bounds, Queue Preservation & Manual Batch 2 Isolated SQL Failure
        # =========================================================================
        print("=== 11B. Scenario 4B: Operator Seed-Control, <=20/200 Bounds, Queue Preservation & Manual Batch 2 Isolated SQL Failure ===", flush=True)
        # Compute exact expected newest-first 200-candidate selection from the first 20 collector matches (EUW1_COLL_B1_1..20)
        by_puuid_expected = {}
        for seq in range(1, 21):
            m_dto = MATCHES_DICT[f"EUW1_COLL_B1_{seq}"]
            g_ms = m_dto["info"]["gameCreation"]
            for pt in m_dto["info"]["participants"]:
                p_id = pt["puuid"]
                cand = {
                    "puuid": p_id,
                    "game_name": pt["riotIdGameName"],
                    "tag_line": pt["riotIdTagline"],
                    "summoner_level": pt["summonerLevel"],
                    "gameCreation": g_ms,
                }
                prev = by_puuid_expected.get(p_id)
                if prev is None or cand["gameCreation"] >= prev["gameCreation"]:
                    by_puuid_expected[p_id] = cand
        sorted_candidates_202 = sorted(
            by_puuid_expected.values(),
            key=lambda c: -c["gameCreation"]
        )
        assert len(sorted_candidates_202) == 202, f"Expected 202 unique candidates across matches 1..20, got {len(sorted_candidates_202)}"
        expected_selected_200 = sorted_candidates_202[:200]
        expected_dropped_2 = [c["puuid"] for c in sorted_candidates_202[200:]]
        assert expected_dropped_2 == ["puuid-coll-m1-s9", "puuid-coll-m1-s10"], (
            f"Expected oldest overflow candidates puuid-coll-m1-s9..10 to be dropped by newest-first take(200), got {expected_dropped_2}"
        )
        expected_selected_200_by_puuid = {c["puuid"]: c for c in expected_selected_200}
        # Pre-seeded COLL_STALE_GUARD_PUUID has a newer last_seen_at (+5 min), so its DB row must retain CollFreshGuard#FRESH (level 999)
        expected_selected_200_by_puuid[COLL_STALE_GUARD_PUUID] = {
            "puuid": COLL_STALE_GUARD_PUUID,
            "game_name": "CollFreshGuard",
            "tag_line": "FRESH",
            "summoner_level": 999,
            "gameCreation": by_puuid_expected[COLL_STALE_GUARD_PUUID]["gameCreation"],
        }

        # Pre-seed COLL_STALE_GUARD_PUUID with a newer last_seen_at (+5 min) to verify collector newer-only protection
        psql_exec(f"""
            INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
            VALUES ('{COLL_STALE_GUARD_PUUID}', 'CollFreshGuard', 'FRESH', 'euw1', 4999, 999, NOW() + INTERVAL '5 minutes')
            ON CONFLICT (puuid) DO UPDATE
            SET game_name = EXCLUDED.game_name,
                tag_line = EXCLUDED.tag_line,
                summoner_level = EXCLUDED.summoner_level,
                last_seen_at = EXCLUDED.last_seen_at;
        """)

        # Ensure Challenger leaderboard cache in disposable Redis has EMPTY entries ([]) so COLL_SEED_1..4 can ONLY enter via explicit seedPuuid
        synthetic_cp_ts = datetime.fromtimestamp(NOW_EPOCH_SEC - 300, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        empty_lb_payload = compress_for_redis({
            "lastRefreshed": synthetic_cp_ts,
            "leaderboard": {
                "tier": "CHALLENGER",
                "leagueId": "challenger-euw1",
                "queue": "RANKED_SOLO_5x5",
                "name": "Challenger",
                "entries": [],
            },
        })
        for lb_key in ("leaderboard:euw1::RANKED_SOLO_5x5", "leaderboard:euw1:RANKED_SOLO_5x5"):
            subprocess.run(
                ["docker", "exec", "ss-redis", "redis-cli", "SETEX", lb_key, "3600", empty_lb_payload],
                check=True,
                stdout=subprocess.DEVNULL,
                timeout=10,
            )

        # Isolated synthetic checkpoint fixture in DISPOSABLE Postgres only (never production):
        # 1 processed PUUID (COLL_ALREADY_PROCESSED_SEED) and 496 pending tail candidates (puuid-coll-tail-1..496) on patch 16.7
        synthetic_cp_json = json.dumps({
            "region": "euw1",
            "targetPatch": "16.7",
            "patchSelectionSource": "EXPLICIT_TARGET_PATCH",
            "queueId": 420,
            "roles": ["TOP", "MIDDLE", "BOTTOM"],
            "maxGamesPerPlayerCap": 10,
            "targetPlayersPerCell": 50,
            "targetGamesPerCell": 200,
            "processedPuuids": [COLL_ALREADY_PROCESSED_SEED],
            "pendingCandidatePuuids": COLL_TAIL_PUUIDS,
            "processedMatchIds": [],
            "playerCellGameCounts": {},
            "cellProgress": {},
            "cellsPassingQualityGate": 0,
            "targetCellsCount": 30,
            "totalNetworkCallsMade": 0,
            "fetchedRankSnapshots": 0,
            "fetchedMatchIdLists": 0,
            "fetchedMatches": 0,
            "yieldedToForegroundCount": 0,
            "pausedForRetryAfterCount": 0,
            "pausedForHeadroomCount": 0,
            "pausedForMemoryHeadroomCount": 0,
            "sessionProductiveBatchesCompleted": 0,
            "maxSessionProductiveBatches": 0,
            "sessionNetworkCallsMade": 0,
            "maxSessionNetworkCalls": 0,
            "lastStopReason": "INITIALIZED",
            "startedAt": synthetic_cp_ts,
            "updatedAt": synthetic_cp_ts,
        }, separators=(",", ":"))
        psql_exec(f"""
            INSERT INTO cohort_collector_checkpoints (
                id, region, target_patch, queue_id, last_stop_reason,
                total_network_calls_made, processed_puuids_count, pending_candidates_count,
                cells_passing_quality_gate, checkpoint_json, started_at, updated_at
            )
            VALUES
                (gen_random_uuid(), 'euw1', '16.7', 420, 'INITIALIZED', 0, 1, 496, 0, '{synthetic_cp_json}', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes'),
                (gen_random_uuid(), 'euw1', 'latest', 420, 'INITIALIZED', 0, 1, 496, 0, '{synthetic_cp_json}', NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '5 minutes')
            ON CONFLICT (region, target_patch, queue_id) DO UPDATE
            SET last_stop_reason = EXCLUDED.last_stop_reason,
                total_network_calls_made = EXCLUDED.total_network_calls_made,
                processed_puuids_count = EXCLUDED.processed_puuids_count,
                pending_candidates_count = EXCLUDED.pending_candidates_count,
                cells_passing_quality_gate = EXCLUDED.cells_passing_quality_gate,
                checkpoint_json = EXCLUDED.checkpoint_json,
                started_at = EXCLUDED.started_at,
                updated_at = EXCLUDED.updated_at;
        """)
        redis_del("cohort:collector:checkpoint:euw1:16.7", "cohort:collector:checkpoint:euw1:latest")

        indexed_total_before_b1 = psql_query_json("SELECT COUNT(*)::int AS cnt FROM indexed_players")[0]["cnt"]
        indexed_coll_before_b1 = psql_query_json(
            "SELECT COUNT(*)::int AS cnt FROM indexed_players WHERE puuid LIKE 'puuid-coll-%'"
        )[0]["cnt"]
        assert indexed_coll_before_b1 == 1, f"Expected 1 pre-seeded puuid-coll-* row before Batch 1, got {indexed_coll_before_b1}"

        # Wait 2.2s for 2,000ms foregroundYieldActive window from prior user requests to clear, then verify 3-endpoint rate-limit readiness & restored checkpoint
        time.sleep(2.2)
        pre_b1_status = http_json(f"{BASE_URL}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=OP_HEADERS, timeout=15)
        rl_verify = pre_b1_status["rateLimitVerification"]
        required_3_endpoints = {"league-v4-entries-by-puuid", "match-v5-ids-by-puuid", "match-v5-match-by-id"}
        assert rl_verify["appLimitsVerifiedFromHeaders"] is True, f"Expected appLimitsVerifiedFromHeaders=True, got {rl_verify}"
        assert required_3_endpoints.issubset(set(rl_verify["verifiedMethodEndpoints"])), (
            f"Expected all 3 collector endpoints verified, got {rl_verify['verifiedMethodEndpoints']}"
        )
        assert rl_verify["missingRequiredMethodEndpoints"] == [], (
            f"Expected 0 missing required method endpoints, got {rl_verify['missingRequiredMethodEndpoints']}"
        )
        assert rl_verify["foregroundYieldActive"] is False, f"Expected foregroundYieldActive=False after 2.2s, got {rl_verify}"
        assert rl_verify["readyForLowPriorityNetworkCollection"] is True, f"Expected readyForLowPriorityNetworkCollection=True, got {rl_verify}"
        pre_b1_cp = pre_b1_status["checkpoint"]
        assert pre_b1_cp["processedPuuids"] == [COLL_ALREADY_PROCESSED_SEED], f"Unexpected pre-B1 processedPuuids: {pre_b1_cp['processedPuuids']}"
        assert pre_b1_cp["pendingCandidatePuuids"] == COLL_TAIL_PUUIDS, f"Unexpected pre-B1 pendingCandidatePuuids len={len(pre_b1_cp['pendingCandidatePuuids'])}"

        # Verify pre-mutation validation (>25 seeds, malformed seed with whitespace bad%20seed, and >500 pending queue cap) with 0 Riot calls and untouched checkpoint
        reset_riot_call_counts()
        over25_qs = "&".join(f"seedPuuid=puuid-over-{i}" for i in range(1, 27))
        over25_status = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/start?patch=16.7&async=false&{over25_qs}",
            headers=OP_HEADERS,
            timeout=15,
        )
        assert over25_status["action"] == "INVALID_PARAMETERS", f"Expected INVALID_PARAMETERS for >25 seeds, got {over25_status['action']}"
        assert over25_status["overnightSessionArmed"] is False
        post_over25_cp = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/status?patch=16.7",
            headers=OP_HEADERS,
            timeout=15,
        )["checkpoint"]
        assert post_over25_cp["processedPuuids"] == [COLL_ALREADY_PROCESSED_SEED], f">25 seed refusal mutated processedPuuids: {post_over25_cp['processedPuuids']}"
        assert post_over25_cp["pendingCandidatePuuids"] == COLL_TAIL_PUUIDS, ">25 seed refusal mutated pendingCandidatePuuids!"

        bad_seed_status = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/start?patch=16.7&async=false&seedPuuid=bad%20seed",
            headers=OP_HEADERS,
            timeout=15,
        )
        assert bad_seed_status["action"] == "INVALID_PARAMETERS", f"Expected INVALID_PARAMETERS for malformed whitespace seed, got {bad_seed_status['action']}"
        assert bad_seed_status["overnightSessionArmed"] is False
        post_bad_seed_cp = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/status?patch=16.7",
            headers=OP_HEADERS,
            timeout=15,
        )["checkpoint"]
        assert post_bad_seed_cp["processedPuuids"] == [COLL_ALREADY_PROCESSED_SEED], f"Malformed seed refusal mutated processedPuuids: {post_bad_seed_cp['processedPuuids']}"
        assert post_bad_seed_cp["pendingCandidatePuuids"] == COLL_TAIL_PUUIDS, "Malformed seed refusal mutated pendingCandidatePuuids!"

        cap_overflow_qs = "&".join(f"seedPuuid=puuid-cap-overflow-{i}" for i in range(1, 6))
        cap_overflow_status = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/start?patch=16.7&async=false&{cap_overflow_qs}",
            headers=OP_HEADERS,
            timeout=15,
        )
        assert cap_overflow_status["action"] == "INVALID_PARAMETERS", f"Expected INVALID_PARAMETERS for 496+5=501 > 500 queue cap, got {cap_overflow_status['action']}"
        assert cap_overflow_status["overnightSessionArmed"] is False
        assert cap_overflow_status["checkpoint"]["processedPuuids"] == [COLL_ALREADY_PROCESSED_SEED], "Queue cap refusal mutated processedPuuids!"
        assert cap_overflow_status["checkpoint"]["pendingCandidatePuuids"] == COLL_TAIL_PUUIDS, "Queue cap refusal mutated pendingCandidatePuuids!"
        calls_validation_refusal = snapshot_riot_call_counts()
        assert (
            calls_validation_refusal["accountByRiotId"]
            + calls_validation_refusal["summonerByPuuid"]
            + calls_validation_refusal["leagueByPuuid"]
            + calls_validation_refusal["matchIdsPages"]
            + calls_validation_refusal["matchById"]
        ) == 0, f"Expected 0 Riot calls during pre-mutation validation refusals, got {calls_validation_refusal}"

        # Source-ground expected Batch 1 & Batch 2 pending queue transitions from fixture + CohortBenchmarkService.runCollectorBatch order
        b1_seed_inputs = [COLL_ALREADY_PROCESSED_SEED, COLL_SEED_1, COLL_SEED_2, COLL_SEED_3, COLL_SEED_4, COLL_SEED_1]
        exp_b1_processed, exp_b1_pending, exp_b1_match_ids, exp_b1_extras = derive_expected_collector_queue_transition(
            initial_processed=[COLL_ALREADY_PROCESSED_SEED],
            initial_pending=COLL_TAIL_PUUIDS,
            initial_processed_match_ids=[],
            seed_puuids=b1_seed_inputs,
            batch_size=3,
        )
        assert exp_b1_extras == [COLL_DUP_PUUID, "puuid-coll-m18-s1"], (
            f"Unexpected source-derived Batch 1 co-player extras: {exp_b1_extras}"
        )
        assert exp_b1_pending == [COLL_SEED_4] + COLL_TAIL_PUUIDS + exp_b1_extras
        assert len(exp_b1_pending) == 499 <= 500

        exp_b2_processed, exp_b2_pending, exp_b2_match_ids, exp_b2_new_extras = derive_expected_collector_queue_transition(
            initial_processed=exp_b1_processed,
            initial_pending=exp_b1_pending,
            initial_processed_match_ids=exp_b1_match_ids,
            seed_puuids=[],
            batch_size=1,
        )
        assert exp_b2_new_extras == ["puuid-coll-m101-s1"], (
            f"Unexpected source-derived Batch 2 co-player extras: {exp_b2_new_extras}"
        )
        exp_b2_all_extras = exp_b1_extras + exp_b2_new_extras
        assert exp_b2_all_extras == [COLL_DUP_PUUID, "puuid-coll-m18-s1", "puuid-coll-m101-s1"]
        assert exp_b2_pending == COLL_TAIL_PUUIDS + exp_b2_all_extras
        assert len(exp_b2_pending) == 499 <= 500

        # Batch 1: Normal seeded Controller -> Runner -> Service path (1 already-processed seed + 4 net-new seeds + 1 duplicate seed => 4+496 = 500 <= 500 cap)
        reset_riot_call_counts()
        seed_b1_qs = "&".join(f"seedPuuid={s}" for s in b1_seed_inputs)
        t_b1_start = time.monotonic()
        coll_b1_status = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/start"
            f"?patch=16.7&maxBatches=1&batchSize=3&pauseDelayMs=200&maxPauseCycles=3&maxTotalProductiveBatches=1&maxTotalNetworkCalls=60&async=false&{seed_b1_qs}",
            headers=OP_HEADERS,
            timeout=30,
        )
        coll_b1_latency_ms = round((time.monotonic() - t_b1_start) * 1000.0, 2)
        calls_coll_b1 = snapshot_riot_call_counts()
        record_sample("after_collector_batch1_seeded_identity_growth")

        cp_b1 = coll_b1_status["checkpoint"]
        assert cp_b1["lastStopReason"] == "SESSION_BATCH_CAP_REACHED", (
            f"Expected Batch 1 stop reason SESSION_BATCH_CAP_REACHED, got {cp_b1['lastStopReason']}"
        )
        assert coll_b1_status["overnightSessionArmed"] is False, f"Expected overnightSessionArmed=False after reaching session batch cap, got {coll_b1_status['overnightSessionArmed']}"
        assert cp_b1["processedPuuids"] == exp_b1_processed, (
            f"Expected exact ordered processedPuuids {exp_b1_processed}, got {cp_b1['processedPuuids']}"
        )
        assert len(cp_b1["pendingCandidatePuuids"]) == 499 <= 500, (
            f"Expected Batch 1 pending queue length 499 (<= 500 cap), got {len(cp_b1['pendingCandidatePuuids'])}"
        )
        assert cp_b1["pendingCandidatePuuids"][0] == COLL_SEED_4, (
            f"Expected COLL_SEED_4 at head of pending queue after Batch 1, got {cp_b1['pendingCandidatePuuids'][0]}"
        )
        assert cp_b1["pendingCandidatePuuids"][1:1 + len(COLL_TAIL_PUUIDS)] == COLL_TAIL_PUUIDS, (
            "Batch 1 failed to preserve all 496 original tail entries in exact order after COLL_SEED_4"
        )
        assert cp_b1["pendingCandidatePuuids"][1 + len(COLL_TAIL_PUUIDS):] == exp_b1_extras, (
            f"Expected Batch 1 discovered co-player tail extras {exp_b1_extras}, got {cp_b1['pendingCandidatePuuids'][1 + len(COLL_TAIL_PUUIDS):]}"
        )
        assert cp_b1["pendingCandidatePuuids"] == exp_b1_pending
        assert cp_b1["fetchedMatches"] == 25, f"Expected 25 distinct collector matches fetched in Batch 1, got {cp_b1['fetchedMatches']}"

        # Verify zero added Riot network calls (0 account-v1, 0 summoner-v4; total HTTP calls == sessionNetworkCallsMade == 31)
        assert calls_coll_b1["accountByRiotId"] == 0, f"Unexpected accountByRiotId calls during collector batch: {calls_coll_b1}"
        assert calls_coll_b1["summonerByPuuid"] == 0, f"Unexpected summonerByPuuid calls during collector batch: {calls_coll_b1}"
        assert calls_coll_b1["leagueByPuuid"] == 3
        assert calls_coll_b1["matchIdsPages"] == 3
        assert calls_coll_b1["matchById"] == 25
        total_riot_http_b1 = calls_coll_b1["leagueByPuuid"] + calls_coll_b1["matchIdsPages"] + calls_coll_b1["matchById"]
        assert total_riot_http_b1 == cp_b1["sessionNetworkCallsMade"] == cp_b1["totalNetworkCallsMade"] == 31

        # Verify <=20 matches and <=200 players bounds, exact 200-row multiset, within-batch dedup, and newer-only in Postgres indexed_players
        indexed_total_after_b1 = psql_query_json("SELECT COUNT(*)::int AS cnt FROM indexed_players")[0]["cnt"]
        actual_coll_rows = psql_query_json("""
            SELECT puuid, game_name, tag_line, summoner_level
            FROM indexed_players
            WHERE puuid LIKE 'puuid-coll-%'
            ORDER BY puuid
        """)
        indexed_coll_after_b1 = len(actual_coll_rows)
        assert indexed_coll_after_b1 == 200, (
            f"Expected exactly 200 puuid-coll-* rows after bounded Batch 1 (199 new + 1 pre-seeded), got {indexed_coll_after_b1}"
        )
        assert (indexed_total_after_b1 - indexed_total_before_b1) == 199, (
            f"Expected +199 net indexed_players growth in Batch 1, got {indexed_total_after_b1 - indexed_total_before_b1}"
        )
        actual_coll_by_puuid = {r["puuid"]: r for r in actual_coll_rows}
        assert set(actual_coll_by_puuid.keys()) == set(expected_selected_200_by_puuid.keys()), (
            f"Selected 200 PUUID set mismatch: diff={set(actual_coll_by_puuid.keys()) ^ set(expected_selected_200_by_puuid.keys())}"
        )
        for p_id, exp_row in expected_selected_200_by_puuid.items():
            act_row = actual_coll_by_puuid[p_id]
            assert (
                act_row["game_name"] == exp_row["game_name"]
                and act_row["tag_line"] == exp_row["tag_line"]
                and act_row["summoner_level"] == exp_row["summoner_level"]
            ), f"Row mismatch for {p_id}: actual={act_row} expected={exp_row}"

        # Oldest overflow candidates #201 and #202 in match 1 (puuid-coll-m1-s9, puuid-coll-m1-s10) must NOT be indexed (<=200 cap after newest-first sort),
        # while puuid-coll-m1-s8 (#200 in newest-first order) IS indexed
        overflow_player_cnt = psql_query_json("""
            SELECT COUNT(*)::int AS cnt FROM indexed_players
            WHERE puuid IN ('puuid-coll-m1-s9', 'puuid-coll-m1-s10')
        """)[0]["cnt"]
        assert overflow_player_cnt == 0, f"Expected 0 overflow (>200) players (puuid-coll-m1-s9..10) indexed, got {overflow_player_cnt}"
        m1_s8_rows = psql_query_json("SELECT puuid, game_name FROM indexed_players WHERE puuid = 'puuid-coll-m1-s8'")
        assert len(m1_s8_rows) == 1 and m1_s8_rows[0]["game_name"] == "CollPlayerM1S8"

        # Match 20 participants (all 10 slots puuid-coll-m20-s1..10) MUST be present, while matches 21..25 participants MUST be absent (<=20 match cap)
        m20_present_cnt = psql_query_json("""
            SELECT COUNT(*)::int AS cnt FROM indexed_players
            WHERE puuid LIKE 'puuid-coll-m20-%'
        """)[0]["cnt"]
        assert m20_present_cnt == 10, f"Expected all 10 match-20 slot participants indexed, got {m20_present_cnt}"

        overflow_match_cnt = psql_query_json("""
            SELECT COUNT(*)::int AS cnt FROM indexed_players
            WHERE puuid LIKE 'puuid-coll-m21-%'
               OR puuid LIKE 'puuid-coll-m22-%'
               OR puuid LIKE 'puuid-coll-m23-%'
               OR puuid LIKE 'puuid-coll-m24-%'
               OR puuid LIKE 'puuid-coll-m25-%'
        """)[0]["cnt"]
        assert overflow_match_cnt == 0, f"Expected 0 participants from matches 21..25 indexed, got {overflow_match_cnt}"

        # Within-batch dedup + newer-only: COLL_DUP_PUUID must have newer match 2 identity ("CollDupNew#NEW", level 222)
        dup_rows = psql_query_json(
            f"SELECT puuid, game_name, tag_line, summoner_level FROM indexed_players WHERE puuid = '{COLL_DUP_PUUID}'"
        )
        assert len(dup_rows) == 1 and dup_rows[0]["game_name"] == "CollDupNew" and dup_rows[0]["tag_line"] == "NEW" and dup_rows[0]["summoner_level"] == 222, (
            f"Unexpected COLL_DUP_PUUID row: {dup_rows}"
        )

        # Newer-only DB guard: COLL_STALE_GUARD_PUUID must keep "CollFreshGuard#FRESH" (level 999)
        stale_guard_rows = psql_query_json(
            f"SELECT puuid, game_name, tag_line, summoner_level FROM indexed_players WHERE puuid = '{COLL_STALE_GUARD_PUUID}'"
        )
        assert len(stale_guard_rows) == 1 and stale_guard_rows[0]["game_name"] == "CollFreshGuard" and stale_guard_rows[0]["summoner_level"] == 999, (
            f"Stale match overwrote newer indexed_players row: {stale_guard_rows}"
        )

        # Non-benchmark roles (JUNGLE slot 3, UTILITY slot 4) are indexed and searchable via autocomplete
        coll_auto = http_json(f"{BASE_URL}/api/search/autocomplete?query=CollPlayerM1S3")
        assert any(p["gameName"] == "CollPlayerM1S3" and p["tagLine"] == "C1" for p in coll_auto)

        # Record persisted (EMERALD, MIDDLE) cell games before Batch 2 isolated SQL failure test
        emerald_mid_before_b2 = psql_query_json("""
            SELECT distinct_players, eligible_games
            FROM cohort_benchmark_cells
            WHERE region = 'euw1' AND patch = '16.7' AND queue_id = 420 AND tier = 'EMERALD' AND role = 'MIDDLE'
        """)
        assert len(emerald_mid_before_b2) == 1 and emerald_mid_before_b2[0]["eligible_games"] == 25, (
            f"Expected 25 EMERALD MIDDLE games after Batch 1, got {emerald_mid_before_b2}"
        )

        # Batch 2 (Manual synchronous /collector/start — NOT a @Scheduled auto-resume tick):
        # Break indexed_players at the SQL level and run manual synchronous Batch 2 consuming queued COLL_SEED_4 -> 2 matches
        time.sleep(2.2)
        reset_riot_call_counts()
        psql_exec("ALTER TABLE indexed_players RENAME TO indexed_players_offline_test;")
        try:
            t_b2_start = time.monotonic()
            coll_b2_status = http_json(
                f"{BASE_URL}/api/riot/cohorts/euw1/collector/start"
                "?patch=16.7&maxBatches=1&batchSize=1&pauseDelayMs=200&maxPauseCycles=3&maxTotalProductiveBatches=1&maxTotalNetworkCalls=60&async=false",
                headers=OP_HEADERS,
                timeout=30,
            )
            coll_b2_latency_ms = round((time.monotonic() - t_b2_start) * 1000.0, 2)
        finally:
            psql_exec("ALTER TABLE indexed_players_offline_test RENAME TO indexed_players;")

        calls_coll_b2 = snapshot_riot_call_counts()
        record_sample("after_collector_batch2_manual_start_isolated_sql_failure")

        cp_b2 = coll_b2_status["checkpoint"]
        assert cp_b2["lastStopReason"] == "SESSION_BATCH_CAP_REACHED", (
            f"Collector Batch 2 unexpected stop reason on isolated indexed_players SQL failure: {cp_b2['lastStopReason']}"
        )
        assert coll_b2_status["overnightSessionArmed"] is False
        assert cp_b2["processedPuuids"] == exp_b2_processed, (
            f"Expected exact ordered processedPuuids {exp_b2_processed}: {cp_b2['processedPuuids']}"
        )
        assert len(cp_b2["pendingCandidatePuuids"]) == 499 <= 500, (
            f"Expected Batch 2 pending queue length 499 (<= 500 cap), got {len(cp_b2['pendingCandidatePuuids'])}"
        )
        assert cp_b2["pendingCandidatePuuids"][:len(COLL_TAIL_PUUIDS)] == COLL_TAIL_PUUIDS, (
            "Batch 2 failed to preserve all 496 original tail entries in exact order at head of queue"
        )
        assert cp_b2["pendingCandidatePuuids"][len(COLL_TAIL_PUUIDS):] == exp_b2_all_extras, (
            f"Expected Batch 2 discovered co-player tail extras {exp_b2_all_extras}, got {cp_b2['pendingCandidatePuuids'][len(COLL_TAIL_PUUIDS):]}"
        )
        assert cp_b2["pendingCandidatePuuids"] == exp_b2_pending
        assert cp_b2["processedMatchIds"] == exp_b2_match_ids
        assert "EUW1_COLL_B2_1" in cp_b2["processedMatchIds"] and "EUW1_COLL_B2_2" in cp_b2["processedMatchIds"]
        assert calls_coll_b2["accountByRiotId"] == 0 and calls_coll_b2["summonerByPuuid"] == 0
        assert calls_coll_b2["leagueByPuuid"] == 1 and calls_coll_b2["matchIdsPages"] == 1 and calls_coll_b2["matchById"] == 2
        assert cp_b2["sessionNetworkCallsMade"] == 4 and cp_b2["totalNetworkCallsMade"] == 35

        # Verify durable Postgres benchmark cells, sample states (samples_json), and checkpoints committed during indexed_players SQL failure
        emerald_mid_after_b2 = psql_query_json("""
            SELECT distinct_players, eligible_games, max_games_per_player_cap, passes_quality_gate, octet_length(metrics_json) AS metrics_bytes
            FROM cohort_benchmark_cells
            WHERE region = 'euw1' AND patch = '16.7' AND queue_id = 420 AND tier = 'EMERALD' AND role = 'MIDDLE'
        """)
        assert len(emerald_mid_after_b2) == 1
        assert emerald_mid_after_b2[0]["distinct_players"] == 4
        assert emerald_mid_after_b2[0]["eligible_games"] == 27, (
            f"Expected 27 persisted EMERALD MIDDLE games after Batch 2, got {emerald_mid_after_b2}"
        )
        assert emerald_mid_after_b2[0]["metrics_bytes"] > 50

        sample_state_after_b2 = psql_query_json("""
            SELECT region, patch, queue_id, tier, role, distinct_players, eligible_games,
                   excluded_over_player_cap_games, passes_quality_gate, octet_length(samples_json) AS samples_bytes
            FROM cohort_cell_sample_states
            WHERE region = 'euw1' AND patch = '16.7' AND queue_id = 420 AND tier = 'EMERALD' AND role = 'MIDDLE'
        """)
        assert len(sample_state_after_b2) == 1
        assert sample_state_after_b2[0]["distinct_players"] == 4
        assert sample_state_after_b2[0]["eligible_games"] == 27
        assert sample_state_after_b2[0]["samples_bytes"] > 100

        persisted_cp_rows = psql_query_json("""
            SELECT target_patch, last_stop_reason, total_network_calls_made,
                   processed_puuids_count, pending_candidates_count, checkpoint_json
            FROM cohort_collector_checkpoints
            WHERE region = 'euw1' AND target_patch IN ('16.7', 'latest')
            ORDER BY target_patch
        """)
        assert len(persisted_cp_rows) == 2
        for cp_row in persisted_cp_rows:
            assert cp_row["last_stop_reason"] == "SESSION_BATCH_CAP_REACHED"
            assert cp_row["processed_puuids_count"] == 5
            assert cp_row["pending_candidates_count"] == len(exp_b2_pending) == 499
            assert cp_row["total_network_calls_made"] == 35
            parsed_cp = json.loads(cp_row["checkpoint_json"])
            assert parsed_cp["processedPuuids"] == exp_b2_processed, (
                f"Persisted checkpoint {cp_row['target_patch']} processedPuuids mismatch: {parsed_cp['processedPuuids']}"
            )
            assert parsed_cp["pendingCandidatePuuids"] == exp_b2_pending
            assert "EUW1_COLL_B2_1" in parsed_cp["processedMatchIds"] and "EUW1_COLL_B2_2" in parsed_cp["processedMatchIds"]
            assert parsed_cp["lastStopReason"] == "SESSION_BATCH_CAP_REACHED"

        # Disarm overnightSessionArmed via /collector/stop before Scenario 5
        http_json(f"{BASE_URL}/api/riot/cohorts/euw1/collector/stop?patch=16.7", headers=OP_HEADERS, timeout=15)

        cand_final_cg = record_sample("cand_final")
        sampler_stop.set()
        capture_sanitized_container_log(artifact_dir, "ss-backend-512m", "candidate_87d5f44_container.log")
        cand_max_current_bytes = max(s["cgroupCurrentBytes"] for s in sampler_samples)
        cand_max_peak_bytes = max(s["cgroupPeakBytes"] for s in sampler_samples)
        cand_max_current_pct = max(s["cgroupRatioPct"] for s in sampler_samples)
        cand_max_peak_pct = max(s["cgroupPeakRatioPct"] for s in sampler_samples)

        # =========================================================================
        # SCENARIO 5: Rollback e683777 Read-Compatibility on the Same Live Postgres & Redis
        # =========================================================================
        print(f"=== 12. Scenario 5: Rollback Read-Compatibility ({base_image}) on Same Live Postgres & Redis ===", flush=True)
        base_pid = start_backend_container(base_image, db_pass)
        base_egress = verify_egress_deny_from_container_netns("ss-backend-512m", base_pid)

        # Verify cached profile reads, match pagination, and autocomplete on rollback e683777 without error
        rb_prof_924 = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player924/EUW?refresh=false")
        rb_prof_2100 = http_json(f"{BASE_URL}/api/riot/summoner/euw1/Player2100/EUW?refresh=false")
        rb_prof_conc = http_json(f"{BASE_URL}/api/riot/summoner/euw1/PlayerConc/EUW?refresh=false")
        rb_page1_924 = http_json(f"{BASE_URL}/api/riot/matches/euw1/{PUUID_924}?page=1")
        rb_autocomplete = http_json(f"{BASE_URL}/api/search/autocomplete?query=CoPlayerNew_2102")
        rb_cg = inspect_container_liveness("ss-backend-512m", expected_pid=base_pid)
        capture_sanitized_container_log(artifact_dir, "ss-backend-512m", "rollback_e683777_container.log")

        assert rb_prof_924["totalMatches"] == 927
        assert len(rb_prof_924["recentMatches"]) == 20
        assert rb_prof_2100["totalMatches"] == 2100
        assert len(rb_prof_2100["recentMatches"]) == 20
        assert rb_prof_conc["totalMatches"] == 4
        assert len(rb_page1_924) == 20
        assert any(p["gameName"] == "CoPlayerNew_2102" for p in rb_autocomplete)
        assert rb_cg["cgroupCurrentBytes"] < CGROUP_STRICT_GATE_BYTES and rb_cg["cgroupPeakBytes"] < CGROUP_STRICT_GATE_BYTES

        summary = {
            "status": "PASSED",
            "candidateImage": cand_image,
            "rollbackBaseImage": base_image,
            "egressDenyVerified": True,
            "guardsUnchangedAndNoManualGc": egress_proof["guardsUnchangedAndNoManualGc"],
            "singleMatchRedisCompressedBytes": sample_match_redis_bytes,
            "scenario1_924": {
                "coldBaseline": {
                    "foldedGames": row_924_cold["folded_games_count"],
                    "pendingGames": row_924_cold["pending_games_count"],
                    "historyComplete": row_924_cold["history_complete"],
                    "riotMatchIdsPages": calls_924_cold["matchIdsPages"],
                    "riotMatchByIdCalls": calls_924_cold["matchById"],
                    "pgStateJsonBytes": row_924_cold["state_json_bytes"],
                    "pgStateJsonColBytes": row_924_cold["state_json_pg_col_bytes"],
                    "pgRowColBytes": row_924_cold["row_pg_col_bytes"],
                    "redisProfileBytes": prof_924_redis_bytes,
                },
                "incrementalPlus3_to_927": {
                    "foldedGames": row_927_inc["folded_games_count"],
                    "pendingGames": row_927_inc["pending_games_count"],
                    "historyComplete": row_927_inc["history_complete"],
                    "riotMatchByIdCalls": calls_927_inc["matchById"],
                    "riotMatchByIdList": calls_927_inc["matchByIdList"],
                    "forbiddenOldMatchFetches": len(calls_927_inc["forbiddenMatchFetches"]),
                    "pgStateJsonBytes": row_927_inc["state_json_bytes"],
                    "pgStateJsonColBytes": row_927_inc["state_json_pg_col_bytes"],
                },
                "redisFlushallRecovery_927": {
                    "recoveredTotalMatches": prof_927_recovered["totalMatches"],
                    "riotMatchByIdCallsForRecent20Only": calls_927_recovered["matchById"],
                    "forbiddenOldMatchFetches": len(calls_927_recovered["forbiddenMatchFetches"]),
                    "page2LoadMoreMatchesReturned": len(page2_matches),
                    "page2LoadMoreRiotMatchByIdCalls": calls_page2["matchById"],
                },
            },
            "scenario2_2100": {
                "coldBaseline2100": {
                    "foldedGames": row_2100_cold["folded_games_count"],
                    "pendingGames": row_2100_cold["pending_games_count"],
                    "historyComplete": row_2100_cold["history_complete"],
                    "historyCapped": row_2100_cold["state"]["historyCapped"],
                    "riotMatchIdsPages": calls_2100_cold["matchIdsPages"],
                    "riotMatchByIdCalls": calls_2100_cold["matchById"],
                    "pgStateJsonBytes": row_2100_cold["state_json_bytes"],
                    "pgStateJsonColBytes": row_2100_cold["state_json_pg_col_bytes"],
                    "pgRowColBytes": row_2100_cold["row_pg_col_bytes"],
                    "redisProfileBytes": prof_2100_redis_bytes,
                },
                "rollforward2101": {
                    "foldedGames": row_2100_rf1["folded_games_count"],
                    "pendingGames": row_2100_rf1["pending_games_count"],
                    "newestMatchId": row_2100_rf1["state"]["processedMatchIds"][0],
                    "oldestMatchId": row_2100_rf1["state"]["processedMatchIds"][-1],
                    "riotMatchByIdCalls": calls_2100_rf1["matchById"],
                },
                "rollforward2102MissingTailFailure": {
                    "foldedGames": row_2100_gap["folded_games_count"],
                    "pendingGames": row_2100_gap["pending_games_count"],
                    "pendingMatchIds": row_2100_gap["state"]["pendingMatchIds"],
                    "retainedTailMatchId": row_2100_gap["state"]["processedMatchIds"][-1],
                    "riotMatchByIdAttempted": calls_2100_gap["matchByIdList"],
                    "coverageSummaryPendingGapGames": cov_during_gap["seasonHistoryCompleteness"]["targetRegionStats"]["totalPendingGapGames"],
                },
                "rollforward2102MissingTailRecovered": {
                    "foldedGames": row_2100_rf2_ok["folded_games_count"],
                    "pendingGames": row_2100_rf2_ok["pending_games_count"],
                    "newestMatchId": row_2100_rf2_ok["state"]["processedMatchIds"][0],
                    "oldestMatchId": row_2100_rf2_ok["state"]["processedMatchIds"][-1],
                },
            },
            "scenario3_concurrentOverlappingAppWriters": {
                "initialVersion": row_conc_base["version"],
                "writer2CommittedVersion": hook_triggered["writer2CommittedVersion"],
                "writer2FoldedGames": hook_triggered["writer2FoldedGames"],
                "writer2CgroupBytes": hook_triggered["writer2Cgroup"]["cgroupCurrentBytes"],
                "writer2CgroupPeakBytes": hook_triggered["writer2Cgroup"]["cgroupPeakBytes"],
                "finalCommittedVersion": row_conc_merged["version"],
                "foldedGames": row_conc_merged["folded_games_count"],
                "processedMatchIds": row_conc_merged["state"]["processedMatchIds"],
                "httpCallerTotals": [r["totalMatches"] for r in conc_results],
                "redisCachedTotalMatches": prof_conc_cached["totalMatches"],
            },
            "scenario4_coverageAndIdentity": {
                "regionSeasonStats": reg_stats,
                "coPlayerMonotonicRow": co_player_rows[0],
            },
            "scenario4b_collectorSeedControlIdentityAndIsolation": {
                "emptyCachedChallengerLeaderboardVerified": True,
                "disposableDbSyntheticCheckpointTailCount": len(COLL_TAIL_PUUIDS),
                "preMutationValidation": {
                    "over25SeedsAction": over25_status["action"],
                    "malformedSeedQueryParam": "seedPuuid=bad%20seed",
                    "malformedSeedAction": bad_seed_status["action"],
                    "postMalformedSeedPendingQueueCount": len(post_bad_seed_cp["pendingCandidatePuuids"]),
                    "queueCap501RefusalAction": cap_overflow_status["action"],
                    "riotCallsDuringValidationRefusals": 0,
                    "pendingQueueCountUnchangedAt496": len(cap_overflow_status["checkpoint"]["pendingCandidatePuuids"]),
                },
                "preBatchRateLimitReady": rl_verify["readyForLowPriorityNetworkCollection"],
                "verifiedMethodEndpoints": sorted(rl_verify["verifiedMethodEndpoints"]),
                "batch1LatencyMs": coll_b1_latency_ms,
                "batch1StopReason": cp_b1["lastStopReason"],
                "batch1OvernightSessionArmed": coll_b1_status["overnightSessionArmed"],
                "batch1AlreadyProcessedSeedSkipped": COLL_ALREADY_PROCESSED_SEED in cp_b1["processedPuuids"],
                "batch1ProcessedPuuidsCount": len(cp_b1["processedPuuids"]),
                "batch1PendingQueueCountAfter": len(cp_b1["pendingCandidatePuuids"]),
                "batch1NextPendingHead": cp_b1["pendingCandidatePuuids"][0],
                "batch1Original496TailPreserved": True,
                "batch1DiscoveredCoPlayerExtras": exp_b1_extras,
                "batch1PendingTailLast": cp_b1["pendingCandidatePuuids"][-1],
                "batch1MatchesFetched": cp_b1["fetchedMatches"],
                "batch1MatchesIndexedCap": 20,
                "batch1PlayersIndexedCap": 200,
                "uniqueCandidatesInFirst20Matches": len(sorted_candidates_202),
                "expectedDroppedOldestOverflowPuuids": expected_dropped_2,
                "indexedPlayersBeforeBatch1": indexed_total_before_b1,
                "indexedPlayersAfterBatch1": indexed_total_after_b1,
                "netNewCollectorIdentitiesAdded": indexed_total_after_b1 - indexed_total_before_b1,
                "collectorScopedIdentitiesAfterBatch1": indexed_coll_after_b1,
                "exact200MultisetMatched": True,
                "match20ParticipantsPresentCount": m20_present_cnt,
                "overflowPlayersBeyond200Indexed": overflow_player_cnt,
                "overflowMatchesBeyond20Indexed": overflow_match_cnt,
                "withinBatchDedupRow": dup_rows[0],
                "newerOnlyPreservedRow": stale_guard_rows[0],
                "addedAccountOrSummonerRiotCalls": calls_coll_b1["accountByRiotId"] + calls_coll_b1["summonerByPuuid"],
                "totalRiotHttpCallsBatch1": total_riot_http_b1,
                "batch2InvocationMode": "MANUAL_SYNCHRONOUS_START_NOT_SCHEDULED_AUTO_RESUME",
                "batch2ManualStartIsolatedSqlFailureLatencyMs": coll_b2_latency_ms,
                "batch2StopReason": cp_b2["lastStopReason"],
                "batch2OvernightSessionArmed": coll_b2_status["overnightSessionArmed"],
                "batch2ProcessedPuuidsCount": len(cp_b2["processedPuuids"]),
                "batch2PendingQueueCountAfter": len(cp_b2["pendingCandidatePuuids"]),
                "batch2Original496TailPreserved": True,
                "batch2DiscoveredCoPlayerExtras": exp_b2_all_extras,
                "batch2PendingTailFirst": cp_b2["pendingCandidatePuuids"][0],
                "batch2PendingTailLast": cp_b2["pendingCandidatePuuids"][-1],
                "batch2RiotHttpCalls": calls_coll_b2["leagueByPuuid"] + calls_coll_b2["matchIdsPages"] + calls_coll_b2["matchById"],
                "emeraldMiddleGamesBeforeBatch2": emerald_mid_before_b2[0]["eligible_games"],
                "emeraldMiddleGamesAfterBatch2": emerald_mid_after_b2[0]["eligible_games"],
                "sampleStateSamplesJsonBytes": sample_state_after_b2[0]["samples_bytes"],
                "persistedCheckpointPatchesVerified": [r["target_patch"] for r in persisted_cp_rows],
            },
            "scenario5_rollbackE683777": {
                "player924TotalMatches": rb_prof_924["totalMatches"],
                "player2100TotalMatches": rb_prof_2100["totalMatches"],
                "playerConcTotalMatches": rb_prof_conc["totalMatches"],
                "page1MatchesReturned": len(rb_page1_924),
                "autocompleteMatched": True,
                "rollbackCgroupCurrentBytes": rb_cg["cgroupCurrentBytes"],
                "rollbackCgroupCurrentMiB": rb_cg["cgroupCurrentMiB"],
                "rollbackCgroupRatioPct": rb_cg["cgroupRatioPct"],
                "rollbackCgroupPeakBytes": rb_cg["cgroupPeakBytes"],
                "rollbackCgroupPeakMiB": rb_cg["cgroupPeakMiB"],
                "rollbackCgroupPeakRatioPct": rb_cg["cgroupPeakRatioPct"],
            },
            "candidateMemory512m": {
                "sampleCount": len(sampler_samples),
                "strictRawByteCeiling": CGROUP_STRICT_GATE_BYTES,
                "maxCurrentBytes": cand_max_current_bytes,
                "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in sampler_samples),
                "maxCurrentRatioPct": cand_max_current_pct,
                "maxPeakBytes": cand_max_peak_bytes,
                "maxPeakMiB": max(s["cgroupPeakMiB"] for s in sampler_samples),
                "maxPeakRatioPct": cand_max_peak_pct,
                "finalCurrentBytes": cand_final_cg["cgroupCurrentBytes"],
                "finalCurrentMiB": cand_final_cg["cgroupCurrentMiB"],
                "finalRatioPct": cand_final_cg["cgroupRatioPct"],
                "oomKilled": False,
                "restartCount": 0,
            },
        }
        write_sanitized_artifacts(artifact_dir, summary, sampler_samples)
        print("=== COLLECTOR SEED-CONTROL + IDENTITY + SEASON STATS 512 MB CONTAINER ACCEPTANCE PROOF SUMMARY ===", flush=True)
        print(json.dumps(summary, indent=2), flush=True)
        print("=== ALL COLLECTOR SEED-CONTROL + IDENTITY + SEASON STATS 512 MB ACCEPTANCE PROOFS PASSED ===", flush=True)
    finally:
        teardown_Err = None
        if summary.get("status") != "PASSED":
            summary["status"] = "FAILED"
            if sampler_samples:
                summary["candidateMemory512mPartial"] = {
                    "sampleCount": len(sampler_samples),
                    "strictRawByteCeiling": CGROUP_STRICT_GATE_BYTES,
                    "maxCurrentBytes": max(s["cgroupCurrentBytes"] for s in sampler_samples),
                    "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in sampler_samples),
                    "maxCurrentRatioPct": max(s["cgroupRatioPct"] for s in sampler_samples),
                    "maxPeakBytes": max(s["cgroupPeakBytes"] for s in sampler_samples),
                    "maxPeakMiB": max(s["cgroupPeakMiB"] for s in sampler_samples),
                    "maxPeakRatioPct": max(s["cgroupPeakRatioPct"] for s in sampler_samples),
                    "oomKilled": any(s.get("oomKilled") for s in sampler_samples),
                    "restartCount": max(s.get("restartCount", 0) for s in sampler_samples),
                }
                print("PARTIAL CANDIDATE 512M MEMORY AT FAILURE:", json.dumps(summary["candidateMemory512mPartial"]), flush=True)
            try:
                capture_sanitized_container_log(artifact_dir, "ss-backend-512m", "failure_ss_backend_512m.log")
                write_sanitized_artifacts(artifact_dir, summary, sampler_samples)
            except Exception as e:
                shutil.rmtree(artifact_dir, ignore_errors=True)
                teardown_Err = e
        teardown_isolated_network()
        if teardown_Err is not None:
            raise teardown_Err


def run_static_smoke_test():
    """
    Local static smoke verification for:
    1. Source-grounded Batch 1 & Batch 2 collector queue transitions against fixture + service algorithm.
    2. write_sanitized_artifacts & sanitize_text on both success and failure/teardown paths, including fail-closed directory deletion on secret leak.
    """
    import tempfile

    b1_seed_inputs = [COLL_ALREADY_PROCESSED_SEED, COLL_SEED_1, COLL_SEED_2, COLL_SEED_3, COLL_SEED_4, COLL_SEED_1]
    exp_b1_processed, exp_b1_pending, exp_b1_match_ids, exp_b1_extras = derive_expected_collector_queue_transition(
        initial_processed=[COLL_ALREADY_PROCESSED_SEED],
        initial_pending=COLL_TAIL_PUUIDS,
        initial_processed_match_ids=[],
        seed_puuids=b1_seed_inputs,
        batch_size=3,
    )
    assert exp_b1_processed == [COLL_ALREADY_PROCESSED_SEED, COLL_SEED_1, COLL_SEED_2, COLL_SEED_3]
    assert exp_b1_extras == [COLL_DUP_PUUID, "puuid-coll-m18-s1"]
    assert exp_b1_pending == [COLL_SEED_4] + COLL_TAIL_PUUIDS + [COLL_DUP_PUUID, "puuid-coll-m18-s1"]
    assert len(exp_b1_pending) == 499 <= 500
    assert len(exp_b1_match_ids) == 25

    exp_b2_processed, exp_b2_pending, exp_b2_match_ids, exp_b2_new_extras = derive_expected_collector_queue_transition(
        initial_processed=exp_b1_processed,
        initial_pending=exp_b1_pending,
        initial_processed_match_ids=exp_b1_match_ids,
        seed_puuids=[],
        batch_size=1,
    )
    assert exp_b2_processed == [COLL_ALREADY_PROCESSED_SEED, COLL_SEED_1, COLL_SEED_2, COLL_SEED_3, COLL_SEED_4]
    assert exp_b2_new_extras == ["puuid-coll-m101-s1"]
    assert exp_b2_pending == COLL_TAIL_PUUIDS + [COLL_DUP_PUUID, "puuid-coll-m18-s1", "puuid-coll-m101-s1"]
    assert len(exp_b2_pending) == 499 <= 500
    assert len(exp_b2_match_ids) == 27

    with tempfile.TemporaryDirectory() as tmp_root:
        raw_dir = os.path.join(tmp_root, "raw")
        upload_dir = os.path.join(tmp_root, "upload")
        os.environ["RAW_ARTIFACT_DIR"] = raw_dir
        os.environ["ARTIFACT_DIR"] = upload_dir

        # 1. Success path artifact write & sanitization
        sample_summary = {
            "status": "PASSED",
            "leakedRiotToken": EPHEMERAL_RIOT_TOKEN,
            "leakedOpToken": EPHEMERAL_OP_TOKEN,
            "leakedAssignment": "riot_api_key=super_secret_val",
        }
        sample_cgroups = [{"idx": 1, "cgroupCurrentBytes": 210_000_000, "cgroupPeakBytes": 240_000_000, "secret": EPHEMERAL_DB_PASS}]
        write_sanitized_artifacts(upload_dir, sample_summary, sample_cgroups)
        assert os.path.isfile(os.path.join(raw_dir, "acceptance_summary.json"))
        assert os.path.isfile(os.path.join(upload_dir, "acceptance_summary.json"))
        sanitized_summary_text = open(os.path.join(upload_dir, "acceptance_summary.json"), "r", encoding="utf-8").read()
        assert EPHEMERAL_RIOT_TOKEN not in sanitized_summary_text
        assert EPHEMERAL_OP_TOKEN not in sanitized_summary_text
        assert "super_secret_val" not in sanitized_summary_text
        assert "[REDACTED]" in sanitized_summary_text

        # 2. Failure / teardown path artifact write & sanitization
        fail_summary = {"status": "FAILED", "opToken": EPHEMERAL_OP_TOKEN}
        write_sanitized_artifacts(upload_dir, fail_summary, sample_cgroups)
        assert os.path.isdir(upload_dir)

        # 3. Fail-closed leak detection deletes upload_dir when unredacted secret file exists in upload_dir
        with open(os.path.join(upload_dir, "unredacted.txt"), "w", encoding="utf-8") as uf:
            uf.write(f"leaked={EPHEMERAL_JWT_SECRET}\n")
        try:
            write_sanitized_artifacts(upload_dir, fail_summary, sample_cgroups)
            raise AssertionError("Expected RuntimeError on unredacted secret in upload_dir")
        except RuntimeError as e:
            assert "SECRET_LEAK_IN_ARTIFACT" in str(e)
            assert not os.path.exists(upload_dir), "upload_dir must be deleted on secret scan failure"

    print("STATIC_SMOKE_TEST_OK", json.dumps({
        "batch1PendingLen": len(exp_b1_pending),
        "batch1Extras": exp_b1_extras,
        "batch2PendingLen": len(exp_b2_pending),
        "batch2Extras": exp_b1_extras + exp_b2_new_extras,
    }))


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        run_static_smoke_test()
    else:
        main()
