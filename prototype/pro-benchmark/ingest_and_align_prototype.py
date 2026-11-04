#!/usr/bin/env python3
"""
Private Prototype: Completed-Game Pro Comparisons & Feed-Clock Alignment Test (Issue #15)

One-off reproducible ingestion and clock-alignment verification tool for official
LoL Esports completed games via:
  1. lolesports.com BFF GraphQL persisted query (`homeEvents`, eventState=["completed"])
  2. feed.lolesports.com/livestats/v1/window/{gameId}
  3. feed.lolesports.com/livestats/v1/details/{gameId}

Constraints:
  - Private prototype only; no recurring job, no live UI/endpoint exposure.
  - Polite request pacing with exponential backoff.
"""

import argparse
import json
import math
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DEFAULT_GAME_IDS = [
    ("115660540726095110", "IG vs TES G1"),
    ("115660540726095111", "IG vs TES G2"),
    ("115660540726095112", "IG vs TES G3"),
    ("115660540726095113", "IG vs TES G4"),
    ("115660540726095094", "BLG vs HLE G1"),
    ("115660540726095095", "BLG vs HLE G2"),
    ("115660540726095082", "G2 vs HLE G1"),
    ("115660540726095083", "G2 vs HLE G2"),
]

HOME_EVENTS_SHA256 = "e4da9fd0ab89ecdb783912d74799688d1f52f1edc9b87497d3409c74d9b4291a"

# Season 2026 (Patch 26.x / internal 16.20):
# Passive gold begins at 1:05.0 (65.0s in-game clock) at 20.4g/10s (2.04g/s).
# First integer jump from 500 -> 502 (+2.0g) occurs at 65.0 + (2.0 / 2.04) = 65.980s.
THEORETICAL_PASSIVE_TICK_IN_GAME_S = round(65.0 + (2.0 / 2.04), 3)


def parse_ts(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def to_10s_str(dt: datetime) -> str:
    sec_floor = (dt.second // 10) * 10
    floored = dt.replace(second=sec_floor, microsecond=0)
    return floored.strftime("%Y-%m-%dT%H:%M:%SZ")


def diff_seconds(iso_a: str | None, iso_b: str | None) -> float | None:
    if not iso_a or not iso_b:
        return None
    return round((parse_ts(iso_a) - parse_ts(iso_b)).total_seconds(), 3)


class PoliteFeedClient:
    """HTTP client with polite request pacing and exponential backoff."""

    def __init__(self, min_delay_s: float = 0.15, max_retries: int = 4):
        self.min_delay_s = min_delay_s
        self.max_retries = max_retries
        self._last_request_ts = 0.0

    def get_json(self, url: str, extra_headers: dict | None = None) -> dict | list:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self.min_delay_s:
            time.sleep(self.min_delay_s - elapsed)

        headers = {"User-Agent": "Mozilla/5.0 (SummonerSearcher-Prototype/1.0)"}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(self.max_retries):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=15) as resp:
                    body = resp.read().decode("utf-8")
                    self._last_request_ts = time.time()
                    return json.loads(body) if body else {}
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                backoff = 0.4 * (2 ** attempt)
                time.sleep(backoff)
        return {}


def discover_completed_games_from_bff(
    client: PoliteFeedClient,
    date_start: str = "2026-10-28T00:00:00.000Z",
    date_end: str = "2026-11-05T00:00:00.000Z",
) -> dict:
    """Queries lolesports.com BFF homeEvents persisted query for completed matches and VOD offsets."""
    params = urllib.parse.urlencode(
        {
            "operationName": "homeEvents",
            "variables": json.dumps(
                {
                    "hl": "en-GB",
                    "sport": "lol",
                    "eventDateStart": date_start,
                    "eventDateEnd": date_end,
                    "eventState": ["completed"],
                    "eventType": "match",
                    "pageSize": 40,
                }
            ),
            "extensions": json.dumps(
                {"persistedQuery": {"version": 1, "sha256Hash": HOME_EVENTS_SHA256}}
            ),
        }
    )
    url = f"https://lolesports.com/api/gql?{params}"
    data = client.get_json(
        url,
        extra_headers={
            "Content-Type": "application/json",
            "apollographql-client-name": "Esports Web",
            "apollographql-client-version": "031f21f",
            "Referer": "https://lolesports.com/en-GB",
        },
    )
    if not isinstance(data, dict):
        return {}
    vod_by_gid = {}
    for ev in data.get("data", {}).get("esports", {}).get("events", []):
        for g in ev.get("match", {}).get("games", []):
            if g.get("state") != "completed":
                continue
            vods = g.get("vods", [])
            v0 = vods[0] if vods else {}
            vod_by_gid[g.get("id")] = {
                "eventId": ev.get("id"),
                "blockName": ev.get("blockName"),
                "eventStartTime": ev.get("startTime"),
                "gameNumber": g.get("number"),
                "vodFirstFrameTime": v0.get("firstFrameTime"),
                "vodStartMillis": v0.get("startMillis"),
                "vodEndMillis": v0.get("endMillis"),
                "vodParameter": v0.get("parameter"),
            }
    return vod_by_gid


def load_ddragon_items(client: PoliteFeedClient) -> tuple[str, dict]:
    versions = client.get_json("https://ddragon.leagueoflegends.com/api/versions.json")
    dd_ver = versions[0] if isinstance(versions, list) and versions else "16.22.1"
    items_payload = client.get_json(
        f"https://ddragon.leagueoflegends.com/cdn/{dd_ver}/data/en_US/item.json"
    )
    data = items_payload.get("data", {}) if isinstance(items_payload, dict) else {}
    return dd_ver, data


def classify_item(iid: int, items_d: dict) -> tuple[str, bool, bool, int]:
    meta = items_d.get(str(iid), {})
    name = meta.get("name", f"Item {iid}")
    tags = meta.get("tags", [])
    total_gold = meta.get("gold", {}).get("total", 0)
    into = meta.get("into")
    is_boots = ("Boots" in tags) and (iid not in (1001, 2422))
    is_completed = (not is_boots) and (total_gold >= 2000) and (not into)
    return name, is_boots, is_completed, total_gold


def ingest_single_game(
    client: PoliteFeedClient,
    gid: str,
    label: str,
    vod_info: dict,
    items_d: dict,
) -> dict:
    """Ingests a single completed game, runs clock-alignment + pause detection, and extracts metrics."""
    init_w = client.get_json(f"https://feed.lolesports.com/livestats/v1/window/{gid}")
    meta = init_w["gameMetadata"]
    f0_str = init_w["frames"][0]["rfc460Timestamp"]
    f0_dt = parse_ts(f0_str)
    base_dt = f0_dt.replace(second=(f0_dt.second // 10) * 10, microsecond=0)

    participants_meta = {}
    role_by_pid = {}
    for side, key in [("blue", "blueTeamMetadata"), ("red", "redTeamMetadata")]:
        tmeta = meta[key]
        for pm in tmeta["participantMetadata"]:
            pid = pm["participantId"]
            role_by_pid[pid] = pm["role"]
            participants_meta[pid] = {
                "participantId": pid,
                "side": side,
                "teamId": tmeta["esportsTeamId"],
                "esportsPlayerId": pm["esportsPlayerId"],
                "summonerName": pm["summonerName"],
                "championId": pm["championId"],
                "role": pm["role"],
            }

    # 1. Early-game 10s window scan (0s..110s) for exact sub-second clock anchors
    early_frames = []
    seen_early = set()
    for step in range(0, 12):
        st_str = (base_dt + timedelta(seconds=step * 10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        wd = client.get_json(
            f"https://feed.lolesports.com/livestats/v1/window/{gid}?startingTime={st_str}"
        )
        for fr in wd.get("frames", []):
            ts = fr["rfc460Timestamp"]
            if ts not in seen_early:
                seen_early.add(ts)
                early_frames.append(fr)
    early_frames.sort(key=lambda x: x["rfc460Timestamp"])

    # 2. End-of-game clamp query (+3 hours clamps to final 10 frames of the game)
    end_query = (base_dt + timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_wd = client.get_json(
        f"https://feed.lolesports.com/livestats/v1/window/{gid}?startingTime={end_query}"
    )
    t_finished = end_wd["frames"][-1]["rfc460Timestamp"]
    t_fin_dt = parse_ts(t_finished)

    t_spawn = None
    t_passive_gold = None
    t_first_mid_cs = None
    t_first_side_cs = None
    t_first_jg_cs = None
    prev_cs = {pid: 0 for pid in range(1, 11)}
    prev_gold = {pid: 0 for pid in range(1, 11)}

    for fr in early_frames:
        ts_str = fr["rfc460Timestamp"]
        ps = fr["blueTeam"]["participants"] + fr["redTeam"]["participants"]
        if t_spawn is None and all(p["totalGold"] >= 500 and p["maxHealth"] > 0 for p in ps):
            t_spawn = ts_str

        if t_spawn is not None:
            untouched_ticked = [
                p["participantId"]
                for p in ps
                if prev_gold.get(p["participantId"]) == 500 and p["totalGold"] in (502, 503)
            ]
            if t_passive_gold is None and len(untouched_ticked) >= 4:
                t_passive_gold = ts_str

            el_spawn = (parse_ts(ts_str) - parse_ts(t_spawn)).total_seconds()
            for p in ps:
                pid = p["participantId"]
                role = role_by_pid[pid]
                d_cs = p["creepScore"] - prev_cs[pid]
                d_g = p["totalGold"] - prev_gold[pid]
                if d_cs > 0 and el_spawn >= 55.0:
                    if role == "mid" and d_g >= 20 and t_first_mid_cs is None:
                        t_first_mid_cs = ts_str
                    elif role in ("top", "bottom") and d_g >= 20 and t_first_side_cs is None:
                        t_first_side_cs = ts_str
                    elif role == "jungle" and t_first_jg_cs is None:
                        t_first_jg_cs = ts_str

        for p in ps:
            prev_cs[p["participantId"]] = p["creepScore"]
            prev_gold[p["participantId"]] = p["totalGold"]

    t_spawn_dt = parse_ts(t_spawn)

    # 3. Sample /details across the game (every 60s + benchmark checkpoints + end)
    target_windows = set()
    cur = t_spawn_dt
    while cur <= t_fin_dt:
        target_windows.add(to_10s_str(cur))
        cur += timedelta(seconds=60)
    for mark_min in [5, 8, 10, 12, 14, 15, 18, 20, 22, 25, 28, 30]:
        for anchor in [f0_dt, t_spawn_dt]:
            dt_mark = anchor + timedelta(minutes=mark_min)
            if dt_mark <= t_fin_dt:
                target_windows.add(to_10s_str(dt_mark))
    target_windows.add(to_10s_str(t_fin_dt))

    details_frames = []
    seen_dts = set()
    for st_str in sorted(target_windows):
        dd = client.get_json(
            f"https://feed.lolesports.com/livestats/v1/details/{gid}?startingTime={st_str}"
        )
        for fr in dd.get("frames", []):
            ts = fr["rfc460Timestamp"]
            if ts not in seen_dts:
                seen_dts.add(ts)
                details_frames.append(fr)
    details_frames.sort(key=lambda x: x["rfc460Timestamp"])

    # 4. Detect mid-game pauses via zero-gold freeze or passive-gold lower bound
    pauses = []
    for i in range(1, len(details_frames)):
        f_prev = details_frames[i - 1]
        f_curr = details_frames[i]
        t1 = parse_ts(f_prev["rfc460Timestamp"])
        t2 = parse_ts(f_curr["rfc460Timestamp"])
        if (t1 - t_spawn_dt).total_seconds() < 66.0 or t2 > t_fin_dt:
            continue
        dt = (t2 - t1).total_seconds()
        if dt >= 1.8:
            g_prev = {p["participantId"]: p["totalGoldEarned"] for p in f_prev["participants"]}
            g_curr = {p["participantId"]: p["totalGoldEarned"] for p in f_curr["participants"]}
            deltas = [g_curr[pid] - g_prev[pid] for pid in range(1, 11)]
            if max(deltas) == 0:
                pauses.append(
                    {
                        "pause_start": f_prev["rfc460Timestamp"],
                        "pause_end": f_curr["rfc460Timestamp"],
                        "duration_s": round(dt, 3),
                        "elapsed_from_spawn_at_start_s": round((t1 - t_spawn_dt).total_seconds(), 3),
                        "detected_by": "gameState==paused",
                    }
                )

    total_pause_s = round(sum(p["duration_s"] for p in pauses), 3)

    # If a pause shifted benchmark checkpoints, fetch the pause-adjusted 10s windows too
    if total_pause_s > 0:
        extra_windows = set()
        for mark_min in [5, 10, 14, 15, 20, 25]:
            target_dt = t_spawn_dt + timedelta(minutes=mark_min)
            for p in pauses:
                if parse_ts(p["pause_start"]) <= target_dt:
                    target_dt += parse_ts(p["pause_end"]) - parse_ts(p["pause_start"])
            if target_dt <= t_fin_dt:
                extra_windows.add(to_10s_str(target_dt))
        for st_str in sorted(extra_windows - target_windows):
            dd = client.get_json(
                f"https://feed.lolesports.com/livestats/v1/details/{gid}?startingTime={st_str}"
            )
            for fr in dd.get("frames", []):
                ts = fr["rfc460Timestamp"]
                if ts not in seen_dts:
                    seen_dts.add(ts)
                    details_frames.append(fr)
        details_frames.sort(key=lambda x: x["rfc460Timestamp"])

    def wall_ts_for_in_game_sec(t_game_s: float) -> datetime:
        target = t_spawn_dt + timedelta(seconds=t_game_s)
        for p in pauses:
            ps = parse_ts(p["pause_start"])
            pe = parse_ts(p["pause_end"])
            if ps <= target:
                target += pe - ps
        return target

    def closest_dframe(target_dt: datetime) -> tuple[dict, float]:
        best = min(
            details_frames,
            key=lambda fr: abs((parse_ts(fr["rfc460Timestamp"]) - target_dt).total_seconds()),
        )
        err = round((parse_ts(best["rfc460Timestamp"]) - target_dt).total_seconds(), 3)
        return best, err

    true_in_game_duration_s = round((t_fin_dt - t_spawn_dt).total_seconds() - total_pause_s, 3)
    aligned_checkpoints = {}
    for mark_min in [5, 10, 14, 15, 20, 25]:
        t_game_s = mark_min * 60
        if t_game_s <= true_in_game_duration_s:
            dt_naive = f0_dt + timedelta(seconds=t_game_s)
            dt_spawn_only = t_spawn_dt + timedelta(seconds=t_game_s)
            dt_true_game = wall_ts_for_in_game_sec(t_game_s)

            df_naive, err_n = closest_dframe(dt_naive)
            df_spawn, err_s = closest_dframe(dt_spawn_only)
            df_true, err_t = closest_dframe(dt_true_game)

            def extract_arrays(dframe: dict) -> dict:
                ps_sorted = sorted(dframe["participants"], key=lambda x: x["participantId"])
                return {
                    "pCs": [p["creepScore"] for p in ps_sorted],
                    "pGolds": [p["totalGoldEarned"] for p in ps_sorted],
                    "pLevels": [p["level"] for p in ps_sorted],
                }

            aligned_checkpoints[f"{mark_min}m"] = {
                "naive_first_frame": {
                    "wall_ts": df_naive["rfc460Timestamp"],
                    "sample_err_s": err_n,
                    "clock_bias_vs_true_s": round((dt_naive - dt_true_game).total_seconds(), 3),
                    **extract_arrays(df_naive),
                },
                "spawn_unadjusted": {
                    "wall_ts": df_spawn["rfc460Timestamp"],
                    "sample_err_s": err_s,
                    "clock_bias_vs_true_s": round((dt_spawn_only - dt_true_game).total_seconds(), 3),
                    **extract_arrays(df_spawn),
                },
                "true_in_game_clock": {
                    "wall_ts": df_true["rfc460Timestamp"],
                    "sample_err_s": err_t,
                    "clock_bias_vs_true_s": 0.0,
                    **extract_arrays(df_true),
                },
            }

    # Track item completion timings (pause-adjusted in-game clock)
    first_seen_item = {pid: {} for pid in range(1, 11)}
    for fr in details_frames:
        ts_dt = parse_ts(fr["rfc460Timestamp"])
        if ts_dt < t_spawn_dt:
            continue
        el_sp = round((ts_dt - t_spawn_dt).total_seconds(), 1)
        el_f0 = round((ts_dt - f0_dt).total_seconds(), 1)
        pause_deduct = sum(
            p["duration_s"]
            for p in pauses
            if ts_dt >= parse_ts(p["pause_end"])
        )
        in_game_s = round(el_sp - pause_deduct, 1)
        for p in fr["participants"]:
            pid = p["participantId"]
            for iid in set(p.get("items", [])):
                if iid not in first_seen_item[pid]:
                    name, is_b, is_c, cost = classify_item(iid, items_d)
                    if is_b or is_c:
                        first_seen_item[pid][iid] = {
                            "itemId": iid,
                            "itemName": name,
                            "totalGoldCost": cost,
                            "elapsed_from_spawn_s": el_sp,
                            "elapsed_from_first_frame_s": el_f0,
                            "in_game_clock_s": in_game_s,
                            "is_boots": is_b,
                            "is_completed_item": is_c,
                        }

    final_fr = details_frames[-1]
    final_participants = {}
    for p in final_fr["participants"]:
        pid = p["participantId"]
        items_chron = sorted(first_seen_item[pid].values(), key=lambda x: x["in_game_clock_s"])
        boots_timing = next((x for x in items_chron if x["is_boots"]), None)
        completed_items = [x for x in items_chron if x["is_completed_item"]]
        final_participants[str(pid)] = {
            **participants_meta[pid],
            "finalLevel": p["level"],
            "finalKills": p["kills"],
            "finalDeaths": p["deaths"],
            "finalAssists": p["assists"],
            "finalGold": p["totalGoldEarned"],
            "finalCs": p["creepScore"],
            "killParticipation": round(p.get("killParticipation", 0.0), 4),
            "championDamageShare": round(p.get("championDamageShare", 0.0), 4),
            "wardsPlaced": p.get("wardsPlaced", 0),
            "wardsDestroyed": p.get("wardsDestroyed", 0),
            "finalItems": p.get("items", []),
            "abilities": p.get("abilities", []),
            "perkMetadata": p.get("perkMetadata", {}),
            "bootsTiming": boots_timing,
            "completedItemTimings": completed_items[:4],
        }

    return {
        "gameId": gid,
        "label": label,
        "patch": meta.get("patchVersion"),
        "t_first_frame": f0_str,
        "t_spawn": t_spawn,
        "t_passive_gold": t_passive_gold,
        "t_first_mid_cs": t_first_mid_cs,
        "t_first_side_cs": t_first_side_cs,
        "t_first_jg_cs": t_first_jg_cs,
        "t_finished": t_finished,
        "pre_spawn_offset_s": diff_seconds(t_spawn, f0_str),
        "passive_gold_from_first_frame_s": diff_seconds(t_passive_gold, f0_str),
        "passive_gold_from_spawn_s": diff_seconds(t_passive_gold, t_spawn),
        "first_mid_cs_from_spawn_s": diff_seconds(t_first_mid_cs, t_spawn),
        "first_side_cs_from_spawn_s": diff_seconds(t_first_side_cs, t_spawn),
        "first_jg_cs_from_spawn_s": diff_seconds(t_first_jg_cs, t_spawn),
        "game_duration_from_first_frame_s": diff_seconds(t_finished, f0_str),
        "game_duration_from_spawn_s": diff_seconds(t_finished, t_spawn),
        "true_in_game_duration_s": true_in_game_duration_s,
        "total_pause_s": total_pause_s,
        "pauses_verified": pauses,
        "vod": vod_info,
        "aligned_checkpoints": aligned_checkpoints,
        "participants": final_participants,
    }


def main():
    parser = argparse.ArgumentParser(description="Run private pro-benchmark prototype")
    parser.add_argument(
        "--output",
        default="prototype/pro-benchmark/pro_benchmark_sample_baselines.json",
        help="Output JSON path for private baseline dataset",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.15,
        help="Polite minimum delay in seconds between HTTP requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Verify configuration without issuing network requests",
    )
    args = parser.parse_args()

    if args.dry_run:
        print(f"[DRY-RUN] Configured polite delay={args.delay}s -> output={args.output}")
        print(f"[DRY-RUN] Sample games ({len(DEFAULT_GAME_IDS)}): {[g[0] for g in DEFAULT_GAME_IDS]}")
        return

    client = PoliteFeedClient(min_delay_s=args.delay)
    dd_ver, items_d = load_ddragon_items(client)
    vod_by_gid = discover_completed_games_from_bff(client)

    games = []
    for gid, label in DEFAULT_GAME_IDS:
        g_data = ingest_single_game(client, gid, label, vod_by_gid.get(gid, {}), items_d)
        games.append(g_data)
        print(
            f"Ingested {label} ({gid}): pre_spawn={g_data['pre_spawn_offset_s']}s | "
            f"passive_from_spawn={g_data['passive_gold_from_spawn_s']}s | "
            f"pause={g_data['total_pause_s']}s"
        )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump({"ddragonVersion": dd_ver, "games": games}, f, indent=2)
    print(f"Wrote {len(games)} games to {args.output}")


if __name__ == "__main__":
    main()
