#!/usr/bin/env python3
import http.server
import json
import os
import re
import socketserver
import ssl
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

PORT_MOCK_RIOT = 443

def build_match_payload(match_id: str, idx: int) -> dict:
    roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    participants = []
    for p in range(10):
        puuid = f"puuid-player-{(idx + p) % 45 + 1}" if p > 0 else "puuid-main-450"
        if p == 1:
            puuid = "seed-puuid-1"
        participants.append({
            "puuid": puuid,
            "summonerName": f"Player{p}",
            "riotIdGameName": f"Player{p}",
            "riotIdTagline": "EUW",
            "championId": 1 + ((idx + p) % 50),
            "championName": "Orianna",
            "teamId": 100 if p < 5 else 200,
            "teamPosition": roles[p % 5],
            "individualPosition": roles[p % 5],
            "win": (p < 5) if (idx % 2 == 0) else (p >= 5),
            "kills": 6 + (p % 5),
            "deaths": 3 + (p % 4),
            "assists": 8 + (p % 6),
            "totalMinionsKilled": 195 + (idx % 35),
            "neutralMinionsKilled": 12 + (idx % 10),
            "goldEarned": 12800 + (idx % 1500),
            "totalDamageDealtToChampions": 24500 + (idx % 4000),
            "totalDamageTaken": 18200 + (idx % 3000),
            "visionScore": 28 + (idx % 15),
            "wardsPlaced": 11,
            "wardsKilled": 4,
            "detectorWardsPlaced": 3,
            "champLevel": 16,
            "item0": 3089, "item1": 3157, "item2": 3020, "item3": 3135, "item4": 3165, "item5": 3116, "item6": 3363,
            "summoner1Id": 4, "summoner2Id": 12,
            "challenges": {
                "laneMinionsFirst10Minutes": 82 + (idx % 12),
                "jungleCsBefore10Minutes": 4.0,
                "kda": 3.5,
                "killParticipation": 0.62,
                "damagePerMinute": 810.0,
                "goldPerMinute": 430.0,
                "visionScorePerMinute": 0.95,
                "soloKills": 1
            },
            "perks": {
                "statPerks": {"defense": 5001, "flex": 5008, "offense": 5005},
                "styles": [
                    {"description": "primaryStyle", "style": 8200, "selections": [{"perk": 8230, "var1": 1, "var2": 0, "var3": 0}]},
                    {"description": "subStyle", "style": 8300, "selections": [{"perk": 8347, "var1": 0, "var2": 0, "var3": 0}]}
                ]
            }
        })
    now_ms = int(time.time() * 1000)
    return {
        "metadata": {
            "matchId": match_id,
            "participants": [pt["puuid"] for pt in participants]
        },
        "info": {
            "gameCreation": now_ms - idx * 120000,
            "gameDuration": 1800,
            "gameId": 7000000 + idx,
            "queueId": 420,
            "gameMode": "CLASSIC",
            "gameType": "MATCHED_GAME",
            "gameVersion": "16.7.600.1234",
            "platformId": "EUW1",
            "participants": participants,
            "teams": [
                {"teamId": 100, "win": idx % 2 == 0, "objectives": {"baron": {"first": True, "kills": 1}, "dragon": {"first": True, "kills": 3}, "tower": {"first": True, "kills": 8}}},
                {"teamId": 200, "win": idx % 2 != 0, "objectives": {"baron": {"first": False, "kills": 0}, "dragon": {"first": False, "kills": 1}, "tower": {"first": False, "kills": 3}}}
            ]
        }
    }

MATCH_IDS_450 = [f"EUW1_{i}" for i in range(1, 451)]
PREBUILT_MATCH_JSON = {}
for i in range(1, 451):
    mid = f"EUW1_{i}"
    PREBUILT_MATCH_JSON[mid] = json.dumps(build_match_payload(mid, i)).encode("utf-8")

CHALLENGER_JSON = json.dumps({
    "tier": "CHALLENGER",
    "leagueId": "challenger-euw1",
    "queue": "RANKED_SOLO_5x5",
    "name": "Challenger",
    "entries": [
        {
            "puuid": f"puuid-player-{i}",
            "summonerName": f"Challenger{i}",
            "leaguePoints": 1200 - i,
            "rank": "I",
            "wins": 120,
            "losses": 90,
            "veteran": True,
            "inactive": False,
            "freshBlood": False,
            "hotStreak": False
        }
        for i in range(1, 46)
    ]
}).encode("utf-8")

class MockRiotHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_riot_json(self, body_bytes: bytes, endpoint_limit: str = "2000:10,120000:600"):
        self.send_response(200)
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
        if "/challengerleagues/by-queue/" in path:
            self._send_riot_json(CHALLENGER_JSON, "500:10,30000:600")
        elif "/riot/account/v1/accounts/by-riot-id/" in path:
            body = json.dumps({"puuid": "puuid-main-450", "gameName": "TestPlayer", "tagLine": "EUW"}).encode("utf-8")
            self._send_riot_json(body)
        elif "/riot/account/v1/accounts/by-puuid/" in path:
            puuid = path.rsplit("/", 1)[-1]
            body = json.dumps({"puuid": puuid, "gameName": f"Name_{puuid[-4:]}", "tagLine": "EUW"}).encode("utf-8")
            self._send_riot_json(body)
        elif "/lol/summoner/v4/summoners/by-puuid/" in path:
            puuid = path.rsplit("/", 1)[-1]
            body = json.dumps({
                "id": f"sum-{puuid}",
                "accountId": f"acc-{puuid}",
                "puuid": puuid,
                "profileIconId": 4568,
                "revisionDate": int(time.time() * 1000),
                "summonerLevel": 412
            }).encode("utf-8")
            self._send_riot_json(body)
        elif "/lol/league/v4/entries/by-puuid/" in path:
            puuid = path.rsplit("/", 1)[-1]
            body = json.dumps([{
                "leagueId": "league-emerald",
                "queueType": "RANKED_SOLO_5x5",
                "tier": "EMERALD",
                "rank": "II",
                "summonerName": "TestPlayer",
                "puuid": puuid,
                "leaguePoints": 64,
                "wins": 240,
                "losses": 210,
                "veteran": False,
                "inactive": False,
                "freshBlood": False,
                "hotStreak": True
            }]).encode("utf-8")
            self._send_riot_json(body, "20000:10,1200000:600")
        elif "/lol/match/v5/matches/by-puuid/" in path and path.endswith("/ids"):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            params = dict(kv.split("=", 1) for kv in qs.split("&") if "=" in kv)
            start = int(params.get("start", "0"))
            count = int(params.get("count", "100"))
            sliced = MATCH_IDS_450[start:start + count]
            self._send_riot_json(json.dumps(sliced).encode("utf-8"), "2000:10,120000:600")
        elif "/lol/match/v5/matches/" in path:
            mid = path.rsplit("/", 1)[-1]
            payload = PREBUILT_MATCH_JSON.get(mid, PREBUILT_MATCH_JSON["EUW1_1"])
            self._send_riot_json(payload, "2000:10,120000:600")
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

def http_json(url: str, method: str = "GET", body: dict = None, headers: dict = None, timeout: int = 120) -> dict:
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

def parse_kv_bytes_file(path: str) -> dict:
    out = {}
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2 and parts[1].isdigit():
                    out[parts[0]] = int(parts[1])
    except Exception:
        pass
    return out

def parse_proc_kb_file(path: str) -> dict:
    out = {}
    try:
        with open(path, "r") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                v_parts = v.strip().split()
                if v_parts and v_parts[0].isdigit():
                    out[k.strip()] = int(v_parts[0]) * 1024
    except Exception:
        pass
    return out

def parse_proc_smaps(host_pid: int) -> dict:
    vmas = {}
    try:
        with open(f"/proc/{host_pid}/smaps", "r") as f:
            cur_key = None
            cur_info = None
            for line in f:
                if "-" in line and ":" not in line.split()[0]:
                    parts = line.strip().split(None, 5)
                    addr = parts[0]
                    perms = parts[1] if len(parts) > 1 else ""
                    name = parts[5] if len(parts) > 5 else "[anon]"
                    cur_key = f"{addr} {perms} {name}"
                    cur_info = {"addr": addr, "perms": perms, "name": name, "Size": 0, "Rss": 0, "Pss": 0, "Anonymous": 0, "AnonHugePages": 0}
                    vmas[cur_key] = cur_info
                elif cur_info is not None and ":" in line:
                    k, v = line.split(":", 1)
                    k = k.strip()
                    if k in cur_info:
                        vp = v.strip().split()
                        if vp and vp[0].isdigit():
                            cur_info[k] = int(vp[0]) * 1024
    except Exception:
        pass
    return vmas

def sample_host_proc_and_cgroup(host_pid: int) -> dict:
    mstat = parse_kv_bytes_file(f"/proc/{host_pid}/root/sys/fs/cgroup/memory.stat")
    status = parse_proc_kb_file(f"/proc/{host_pid}/status")
    rollup = parse_proc_kb_file(f"/proc/{host_pid}/smaps_rollup")
    smaps = parse_proc_smaps(host_pid)
    return {
        "mstat": mstat,
        "status": status,
        "rollup": rollup,
        "smaps": smaps
    }

def read_container_events_and_cgroup(container_name: str) -> dict:
    inspect_out = subprocess.check_output(["docker", "inspect", container_name]).decode("utf-8")
    info = json.loads(inspect_out)[0]
    state = info["State"]
    stats_out = subprocess.check_output(
        ["docker", "exec", container_name, "sh", "-c",
         "cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes; echo '---'; "
         "cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes; echo '---'; "
         "cat /sys/fs/cgroup/memory.events 2>/dev/null || true"]
    ).decode("utf-8").splitlines()
    return {
        "oomKilled": state.get("OOMKilled", False),
        "restartCount": info.get("RestartCount", 0),
        "rawCgroup": "\n".join(stats_out)
    }

def main():
    cert_file = sys.argv[1]
    key_file = sys.argv[2]
    print("=== Starting Mock Riot HTTPS API server on port 443 ===", flush=True)
    httpd = start_mock_riot_server(cert_file, key_file)
    time.sleep(1)

    print("=== Starting ss-backend-512m Docker container (--memory=512m --memory-swap=512m, MALLOC_ARENA_MAX=2) ===", flush=True)
    subprocess.check_call([
        "docker", "run", "-d",
        "--name", "ss-backend-512m",
        "--memory=512m",
        "--memory-swap=512m",
        "-p", "10000:10000",
        "--add-host=host.docker.internal:host-gateway",
        "--add-host=euw1.api.riotgames.com:host-gateway",
        "--add-host=na1.api.riotgames.com:host-gateway",
        "--add-host=kr.api.riotgames.com:host-gateway",
        "--add-host=europe.api.riotgames.com:host-gateway",
        "--add-host=americas.api.riotgames.com:host-gateway",
        "--add-host=asia.api.riotgames.com:host-gateway",
        "-v", "/tmp/cacerts:/opt/java/openjdk/lib/security/cacerts:ro",
        "-e", "MALLOC_ARENA_MAX=2",
        "-e", "PORT=10000",
        "-e", "DB_HOST=host.docker.internal",
        "-e", "DB_PORT=5432",
        "-e", "DB_NAME=summoner_searcher",
        "-e", "DB_USER=postgres",
        "-e", "DB_PASS=postgres",
        "-e", "REDIS_URL=redis://host.docker.internal:6379",
        "-e", "RIOT_API_KEY=RGAPI-test-512m-proof",
        "-e", "JWT_SECRET_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "-e", "TWOFA_SECRET_KEY=0123456789abcdef0123456789abcdef",
        "-e", "RESEND_API_KEY=re_test_123456789",
        "-e", "GEMINI_API_KEY=disabled",
        "-e", "COHORT_NETWORK_COLLECTION_ENABLED=true",
        "-e", "COHORT_OPERATOR_TOKEN=test-op-token",
        "ss-backend:512m-proof"
    ])

    op_headers = {"X-Cohort-Operator-Token": "test-op-token"}
    base_url = "http://localhost:10000"
    print("=== Waiting for backend container on http://localhost:10000 ===", flush=True)
    boot_t0 = time.time()
    status_data = None
    for attempt in range(90):
        try:
            status_data = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=5)
            break
        except Exception:
            time.sleep(1)
    if status_data is None:
        subprocess.run(["docker", "logs", "ss-backend-512m"])
        sys.exit(1)
    boot_sec = time.time() - boot_t0
    print(f"=== 1. COLD BOOT SUCCEEDED in {boot_sec:.2f}s (512 MB Container) ===", flush=True)

    host_pid = int(subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", "ss-backend-512m"]).decode("utf-8").strip())

    # Wait 3s for startup ChallengerCacheWarmer to finish sequential warming
    time.sleep(3)
    idle_status = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=10)
    idle_mem = idle_status["memoryHeadroom"]
    cg_idle = read_container_events_and_cgroup("ss-backend-512m")
    idle_host = sample_host_proc_and_cgroup(host_pid)

    print("=== 2. IDLE + POST-WARMER MEMORY HEADROOM SNAPSHOT ===", flush=True)
    print(json.dumps(idle_mem, indent=2), flush=True)
    print("=== CGROUP V2 EVENTS (IDLE) ===", flush=True)
    print(cg_idle["rawCgroup"], flush=True)

    # Run bounded diagnostic probe (3 calls) to verify rate limits and persist a checkpoint to Redis + Postgres
    probe_res = http_json(
        f"{base_url}/api/riot/cohorts/euw1/collector/probe?patch=16.7&seedPuuid=seed-puuid-1",
        method="POST",
        headers=op_headers,
        timeout=30
    )
    print("=== DIAGNOSTIC PROBE RESULT ===", flush=True)
    print(json.dumps({
        "probeStatus": probe_res.get("probeStatus"),
        "totalHttpAttemptsMade": probe_res.get("totalHttpAttemptsMade"),
        "checkpoint": {
            "totalNetworkCallsMade": probe_res.get("checkpoint", {}).get("totalNetworkCallsMade"),
            "lastStopReason": probe_res.get("checkpoint", {}).get("lastStopReason")
        }
    }, indent=2), flush=True)

    # Run 1 productive collector batch (batchSize=2) so checkpoint has processedPuuids, pendingCandidatePuuids, and cached matches
    batch_res = http_json(
        f"{base_url}/api/riot/cohorts/euw1/collector/start?patch=16.7&maxBatches=1&batchSize=2&async=false",
        method="POST",
        headers=op_headers,
        timeout=60
    )
    print("=== 1-BATCH COLLECTOR RUNNER RESULT ===", flush=True)
    print(json.dumps({
        "productiveBatchesCompleted": batch_res.get("productiveBatchesCompleted"),
        "processedPuuids": batch_res.get("checkpoint", {}).get("processedPuuids"),
        "pendingCount": len(batch_res.get("checkpoint", {}).get("pendingCandidatePuuids", [])),
        "totalNetworkCallsMade": batch_res.get("checkpoint", {}).get("totalNetworkCallsMade"),
        "lastStopReason": batch_res.get("checkpoint", {}).get("lastStopReason")
    }, indent=2), flush=True)

    # Set NMT baseline once before the 10-cycle loop so the separate non-gating step can diff against Pre-Cycle
    try:
        http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=baseline", headers=op_headers, timeout=15)
    except Exception:
        pass
    pre_cycle_host = sample_host_proc_and_cgroup(host_pid)
    host_samples_by_stage = {
        "idle": idle_host,
        "pre_cycle": pre_cycle_host,
        "cycles": {}
    }

    print("=== 3. RUNNING 10 CYCLES OF (450-MATCH PROFILE LOAD + COHORT REPORT + MEMORY SNAPSHOT) ===", flush=True)
    cycle_metrics = []
    for cycle in range(1, 11):
        t0 = time.time()
        # Flush the player profile + cooldown and cohort report + lock keys before cycles 2..10 so every cycle executes full work
        if cycle > 1:
            subprocess.check_call(
                ["docker", "exec", "ss-redis", "redis-cli", "DEL",
                 "player:profile:euw1:puuid-main-450",
                 "cooldown:profile:refresh:euw1:puuid-main-450",
                 "cohort:report:euw1:16.7",
                 "lock:cohort:report:euw1:16.7"],
                stdout=subprocess.DEVNULL
            )
        prof = http_json(
            f"{base_url}/api/riot/summoner/euw1/TestPlayer/EUW?refresh=true",
            method="GET",
            timeout=120
        )
        t_prof = (time.time() - t0) * 1000.0
        t1 = time.time()
        rep = http_json(f"{base_url}/api/riot/cohorts/euw1/report?patch=16.7", timeout=60)
        t_rep = (time.time() - t1) * 1000.0
        snap = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)["memoryHeadroom"]
        curr_host = sample_host_proc_and_cgroup(host_pid)
        host_samples_by_stage["cycles"][str(cycle)] = curr_host

        row = {
            "cycle": cycle,
            "recentMatchesCount": len(prof.get("recentMatches") or []),
            "reportSource": rep.get("reportSource"),
            "profileLoadMs": round(t_prof, 1),
            "reportMs": round(t_rep, 1),
            "cgroupCurrentMiB": round(snap["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2),
            "cgroupRatioPct": round(snap["containerUtilizationRatio"] * 100.0, 2),
            "heapUsedMiB": round(snap["jvmHeapUsedBytes"] / (1024 * 1024), 2),
            "heapCommittedMiB": round(snap["jvmHeapCommittedBytes"] / (1024 * 1024), 2),
            "heapUnusedCommittedMiB": round(snap["jvmHeapUnusedCommittedBytes"] / (1024 * 1024), 2),
            "metaspacePoolUsedMiB": round(snap["jvmMetaspaceUsedBytes"] / (1024 * 1024), 2),
            "metaspacePoolCommittedMiB": round(snap["jvmMetaspaceCommittedBytes"] / (1024 * 1024), 2),
            "compressedClassUsedMiB": round(snap["jvmCompressedClassSpaceUsedBytes"] / (1024 * 1024), 2),
            "compressedClassCommittedMiB": round(snap["jvmCompressedClassSpaceCommittedBytes"] / (1024 * 1024), 2),
            "codeCacheUsedMiB": round(snap["jvmCodeCacheUsedBytes"] / (1024 * 1024), 2),
            "nonHeapTotalUsedMiB": round(snap["jvmNonHeapUsedBytes"] / (1024 * 1024), 2),
            "directUsedMiB": round(snap["directBufferUsedBytes"] / (1024 * 1024), 2),
            "nativeResidualMiB": round(snap["estimatedNativeResidualBytes"] / (1024 * 1024), 2),
            "threads": snap["jvmThreadCount"],
            "hasHeadroom": snap["hasHeadroomForLowPriority"]
        }
        cycle_metrics.append(row)
        print(f"CYCLE {cycle:02d}: {json.dumps(row)}", flush=True)

    # Persist passive host samples so the separate non-gating diagnostic step can inspect them even if an assert fails below
    with open("/tmp/proof_host_samples.json", "w") as f:
        json.dump({
            "hostPid": host_pid,
            "cycleMetrics": cycle_metrics,
            "samples": host_samples_by_stage
        }, f)

    print("=== 4. VERIFYING POSTGRES CHECKPOINT DURABILITY ACROSS REDIS FLUSHALL ===", flush=True)
    pg_rows = subprocess.check_output([
        "docker", "exec", "ss-postgres",
        "psql", "-U", "postgres", "-d", "summoner_searcher", "-t", "-A", "-c",
        "SELECT region, target_patch, queue_id, last_stop_reason, total_network_calls_made, processed_puuids_count, pending_candidates_count FROM cohort_collector_checkpoints ORDER BY target_patch;"
    ]).decode("utf-8").strip()
    print("POSTGRES cohort_collector_checkpoints ROWS:")
    print(pg_rows, flush=True)

    subprocess.check_call(["docker", "exec", "ss-redis", "redis-cli", "FLUSHALL"])
    after_flush_status = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)
    recovered_cp = after_flush_status.get("checkpoint")
    print("RECOVERED CHECKPOINT AFTER REDIS FLUSHALL:")
    print(json.dumps({
        "region": recovered_cp.get("region"),
        "targetPatch": recovered_cp.get("targetPatch"),
        "processedPuuids": recovered_cp.get("processedPuuids"),
        "pendingCount": len(recovered_cp.get("pendingCandidatePuuids", [])),
        "totalNetworkCallsMade": recovered_cp.get("totalNetworkCallsMade"),
        "lastStopReason": recovered_cp.get("lastStopReason")
    }, indent=2), flush=True)

    cg_final = read_container_events_and_cgroup("ss-backend-512m")
    print("=== FINAL CGROUP V2 EVENTS & OOM CHECK ===", flush=True)
    print(json.dumps(cg_final, indent=2), flush=True)

    assert not cg_final["oomKilled"], "Container was OOMKilled!"
    assert cg_final["restartCount"] == 0, "Container restarted unexpectedly!"
    assert recovered_cp is not None and recovered_cp.get("totalNetworkCallsMade", 0) > 0, "Postgres checkpoint recovery failed!"
    assert all(c["cgroupRatioPct"] < 78.0 for c in cycle_metrics), (
        f"Memory exceeded 78.0% gate during 10-cycle load! Max was {max(c['cgroupRatioPct'] for c in cycle_metrics):.2f}%"
    )
    print("=== ALL 512 MB CONTAINER PROOFS PASSED ===", flush=True)

if __name__ == "__main__":
    main()

