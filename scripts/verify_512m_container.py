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
        if path == "/api/versions.json":
            if self.headers.get("X-Riot-Token") is not None:
                self.send_response(400)
                self.end_headers()
                return
            body = json.dumps(["16.7.1", "16.6.1"]).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif "/challengerleagues/by-queue/" in path:
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

def read_int_file(path: str, default: int = 0) -> int:
    try:
        with open(path, "r") as f:
            val = f.read().strip()
            return int(val) if val.isdigit() else default
    except Exception:
        return default

def read_host_cgroup_snapshot(host_pid: int) -> dict:
    cg_dir = f"/proc/{host_pid}/root/sys/fs/cgroup"
    current_bytes = read_int_file(f"{cg_dir}/memory.current", 0)
    peak_bytes = read_int_file(f"{cg_dir}/memory.peak", 0)
    max_bytes = read_int_file(f"{cg_dir}/memory.max", 536870912)
    if max_bytes <= 0:
        max_bytes = 536870912
    if peak_bytes <= 0:
        peak_bytes = current_bytes
    return {
        "cgroupCurrentBytes": current_bytes,
        "cgroupCurrentMiB": round(current_bytes / (1024 * 1024), 2),
        "cgroupRatioPct": round((current_bytes / max_bytes) * 100.0, 2),
        "cgroupPeakBytes": peak_bytes,
        "cgroupPeakMiB": round(peak_bytes / (1024 * 1024), 2),
        "cgroupPeakRatioPct": round((peak_bytes / max_bytes) * 100.0, 2),
        "cgroupMaxBytes": max_bytes
    }

def sample_host_proc_and_cgroup(host_pid: int) -> dict:
    mstat = parse_kv_bytes_file(f"/proc/{host_pid}/root/sys/fs/cgroup/memory.stat")
    status = parse_proc_kb_file(f"/proc/{host_pid}/status")
    rollup = parse_proc_kb_file(f"/proc/{host_pid}/smaps_rollup")
    smaps = parse_proc_smaps(host_pid)
    cg = read_host_cgroup_snapshot(host_pid)
    return {
        "cgroup": cg,
        "mstat": mstat,
        "status": status,
        "rollup": rollup,
        "smaps": smaps
    }

def read_container_events_and_cgroup(container_name: str, host_pid: int) -> dict:
    inspect_out = subprocess.check_output(["docker", "inspect", container_name]).decode("utf-8")
    info = json.loads(inspect_out)[0]
    state = info["State"]
    cg_dir = f"/proc/{host_pid}/root/sys/fs/cgroup"
    cg_snap = read_host_cgroup_snapshot(host_pid)
    events_raw = ""
    try:
        with open(f"{cg_dir}/memory.events", "r") as f:
            events_raw = f.read().strip()
    except Exception:
        pass
    raw_cgroup = (
        f"current={cg_snap['cgroupCurrentBytes']} ({cg_snap['cgroupCurrentMiB']} MiB, {cg_snap['cgroupRatioPct']}%)\n"
        f"peak={cg_snap['cgroupPeakBytes']} ({cg_snap['cgroupPeakMiB']} MiB, {cg_snap['cgroupPeakRatioPct']}%)\n"
        f"max={cg_snap['cgroupMaxBytes']}\n"
        f"---\n{events_raw}"
    )
    return {
        "oomKilled": state.get("OOMKilled", False),
        "restartCount": info.get("RestartCount", 0),
        "cgroupCurrentBytes": cg_snap["cgroupCurrentBytes"],
        "cgroupCurrentMiB": cg_snap["cgroupCurrentMiB"],
        "cgroupRatioPct": cg_snap["cgroupRatioPct"],
        "cgroupPeakBytes": cg_snap["cgroupPeakBytes"],
        "cgroupPeakMiB": cg_snap["cgroupPeakMiB"],
        "cgroupPeakRatioPct": cg_snap["cgroupPeakRatioPct"],
        "cgroupMaxBytes": cg_snap["cgroupMaxBytes"],
        "rawCgroup": raw_cgroup
    }


def main():
    cert_file = sys.argv[1]
    key_file = sys.argv[2]
    print("=== Starting Mock Riot HTTPS API server on port 443 ===", flush=True)
    httpd = start_mock_riot_server(cert_file, key_file)
    time.sleep(1)

    print("=== Starting ss-backend-512m Docker container (--memory=512m --memory-swap=512m, -XX:TieredStopAtLevel=1) ===", flush=True)
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
        "--add-host=ddragon.leagueoflegends.com:host-gateway",
        "-v", "/tmp/cacerts:/opt/java/openjdk/lib/security/cacerts:ro",
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
    entrypoint_json = subprocess.check_output(["docker", "inspect", "-f", "{{json .Config.Entrypoint}}", "ss-backend-512m"]).decode("utf-8").strip()
    print("=== CONTAINER ENTRYPOINT (FROM REPO DOCKERFILE) ===", flush=True)
    print(entrypoint_json, flush=True)

    sampler_samples = []
    sampler_lock = threading.Lock()
    sampler_stop = threading.Event()
    sampler_t0 = time.time()

    def record_5s_sample(tag: str = "timer") -> dict:
        cg = read_host_cgroup_snapshot(host_pid)
        with sampler_lock:
            entry = {
                "sampleIdx": len(sampler_samples) + 1,
                "elapsedSec": round(time.time() - sampler_t0, 2),
                "tag": tag,
                "cgroupCurrentBytes": cg["cgroupCurrentBytes"],
                "cgroupCurrentMiB": cg["cgroupCurrentMiB"],
                "cgroupRatioPct": cg["cgroupRatioPct"],
                "cgroupPeakBytes": cg["cgroupPeakBytes"],
                "cgroupPeakMiB": cg["cgroupPeakMiB"],
                "cgroupPeakRatioPct": cg["cgroupPeakRatioPct"]
            }
            sampler_samples.append(entry)
            return entry

    def get_5s_sampler_max() -> dict:
        with sampler_lock:
            if not sampler_samples:
                cg = read_host_cgroup_snapshot(host_pid)
                return {
                    "sampleCount": 0,
                    "maxCurrentBytes": cg["cgroupCurrentBytes"],
                    "maxCurrentMiB": cg["cgroupCurrentMiB"],
                    "maxCurrentRatioPct": cg["cgroupRatioPct"],
                    "maxPeakBytes": cg["cgroupPeakBytes"],
                    "maxPeakMiB": cg["cgroupPeakMiB"],
                    "maxPeakRatioPct": cg["cgroupPeakRatioPct"]
                }
            return {
                "sampleCount": len(sampler_samples),
                "maxCurrentBytes": max(s["cgroupCurrentBytes"] for s in sampler_samples),
                "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in sampler_samples),
                "maxCurrentRatioPct": max(s["cgroupRatioPct"] for s in sampler_samples),
                "maxPeakBytes": max(s["cgroupPeakBytes"] for s in sampler_samples),
                "maxPeakMiB": max(s["cgroupPeakMiB"] for s in sampler_samples),
                "maxPeakRatioPct": max(s["cgroupPeakRatioPct"] for s in sampler_samples)
            }

    def sampler_loop():
        record_5s_sample("boot")
        while not sampler_stop.wait(5.0):
            record_5s_sample("5s_poll")

    sampler_thread = threading.Thread(target=sampler_loop, daemon=True)
    sampler_thread.start()

    # Wait 3s for startup ChallengerCacheWarmer to finish sequential warming
    time.sleep(3)
    idle_status = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=10)
    idle_mem = idle_status["memoryHeadroom"]
    cg_idle = read_container_events_and_cgroup("ss-backend-512m", host_pid)
    idle_host = sample_host_proc_and_cgroup(host_pid)
    record_5s_sample("idle")

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

    # Verify JFR finite profiling readiness inside the 512 MiB container:
    # (a) Prove binary-only jcmd (+ libattach.so) on unmodified eclipse-temurin:21-jre-alpine fails without jdk.jcmd in lib/modules
    subprocess.check_call(["docker", "cp", "ss-backend-512m:/opt/java/openjdk/bin/jcmd", "/tmp/jcmd_bin_only"])
    subprocess.check_call(["docker", "cp", "ss-backend-512m:/opt/java/openjdk/lib/libattach.so", "/tmp/libattach_only.so"])
    bin_only_proc = subprocess.run([
        "docker", "run", "--rm", "--entrypoint", "sh",
        "-v", "/tmp/jcmd_bin_only:/opt/java/openjdk/bin/jcmd:ro",
        "-v", "/tmp/libattach_only.so:/opt/java/openjdk/lib/libattach.so:ro",
        "eclipse-temurin:21-jre-alpine", "-c",
        "ls -lh /opt/java/openjdk/lib/modules && /opt/java/openjdk/bin/jcmd -h 2>&1"
    ], capture_output=True, text=True)
    container_modules_ls = subprocess.check_output([
        "docker", "exec", "ss-backend-512m", "sh", "-c",
        "ls -lh /opt/java/openjdk/bin/jcmd /opt/java/openjdk/lib/libattach.so /opt/java/openjdk/lib/modules && /opt/java/openjdk/bin/java --list-modules | grep -E '^jdk\\.(jcmd|attach|internal\\.jvmstat|jfr|management\\.jfr)'"
    ], text=True).strip()
    print("=== BINARY-ONLY jcmd TEST ON UNMODIFIED eclipse-temurin:21-jre-alpine ===", flush=True)
    print(f"exit_code={bin_only_proc.returncode}\n{bin_only_proc.stdout.strip()}", flush=True)
    print("=== JLINKED jre-with-jcmd ARTIFACTS & MODULES IN ss-backend-512m ===", flush=True)
    print(container_modules_ls, flush=True)
    assert bin_only_proc.returncode != 0 and "Module jdk.jcmd not found" in bin_only_proc.stdout, (
        f"Expected Module jdk.jcmd not found on binary-only copy, got: {bin_only_proc.stdout}"
    )

    # (b) Verify jcmd + jfr version/module parity, no auto-start on boot, and bounded FlightRecorderOptions (stackdepth=32, memorysize=2m)
    jcmd_cmd = [
        "docker", "exec", "ss-backend-512m",
        "/opt/java/openjdk/bin/jcmd",
        "-J-Xms8m", "-J-Xmx16m", "-J-XX:+UseSerialGC", "-J-XX:TieredStopAtLevel=1",
        "1"
    ]
    pre_jfr_check = subprocess.check_output(jcmd_cmd + ["JFR.check"], text=True).strip()
    pre_jfr_cfg = subprocess.check_output(jcmd_cmd + ["JFR.configure"], text=True).strip()
    print("=== PRE-START JFR.check (MUST HAVE NO AUTO-STARTED RECORDING) ===", flush=True)
    print(pre_jfr_check, flush=True)
    print("=== PRE-START JFR.configure (BOUNDED stackdepth=32, memorysize=2m) ===", flush=True)
    print(pre_jfr_cfg, flush=True)
    assert "[warning][cds]" not in pre_jfr_check, f"Unexpected CDS mismatch warning: {pre_jfr_check}"
    assert "No available recordings" in pre_jfr_check, f"Unexpected auto-started JFR recording: {pre_jfr_check}"
    assert "Stack depth: 32" in pre_jfr_cfg and "Memory size: 2.0 MB" in pre_jfr_cfg, (
        f"Expected Stack depth: 32 and Memory size: 2.0 MB in JFR.configure: {pre_jfr_cfg}"
    )

    # (c) Operator-started finite JFR recording around the 1-batch collector run (duration=5m, maxsize=16m, maxage=5m, path-to-gc-roots=false, env/sysprop disabled)
    cg_before_jfr = read_host_cgroup_snapshot(host_pid)
    jfr_start_out = subprocess.check_output(jcmd_cmd + [
        "JFR.start",
        "name=collector_prof",
        "settings=default",
        "duration=5m",
        "maxsize=16m",
        "maxage=5m",
        "dumponexit=false",
        "path-to-gc-roots=false",
        "filename=/tmp/collector_prof.jfr",
        "jdk.ObjectAllocationSample#enabled=true",
        "jdk.ObjectAllocationSample#throttle=150/s",
        "jdk.ObjectAllocationSample#stackTrace=true",
        "jdk.OldObjectSample#enabled=true",
        "jdk.OldObjectSample#stackTrace=true",
        "jdk.OldObjectSample#cutoff=0ns",
        "jdk.InitialEnvironmentVariable#enabled=false",
        "jdk.InitialSystemProperty#enabled=false",
        "jdk.SystemProcess#enabled=false",
        "jdk.JVMInformation#enabled=false"
    ], text=True).strip()
    jfr_running_check = subprocess.check_output(jcmd_cmd + ["JFR.check"], text=True).strip()
    print("=== JFR.start & JFR.check DURING COLLECTOR BATCH ===", flush=True)
    print(jfr_start_out, flush=True)
    print(jfr_running_check, flush=True)
    assert "running" in jfr_running_check and "maxsize=16.0MB" in jfr_running_check, (
        f"Expected running JFR recording with maxsize=16.0MB: {jfr_running_check}"
    )

    # Run 1 productive collector batch (batchSize=2, maxTotalProductiveBatches=1, maxTotalNetworkCalls=30) so checkpoint has processedPuuids, pendingCandidatePuuids, cached matches, and disarms at session cap
    batch_res = http_json(
        f"{base_url}/api/riot/cohorts/euw1/collector/start?patch=16.7&maxBatches=1&batchSize=2&maxTotalProductiveBatches=1&maxTotalNetworkCalls=30&async=false",
        method="POST",
        headers=op_headers,
        timeout=60
    )
    cg_during_jfr = read_host_cgroup_snapshot(host_pid)
    jfr_stop_out = subprocess.check_output(jcmd_cmd + ["JFR.stop", "name=collector_prof"], text=True).strip()
    jfr_summary_out = subprocess.check_output([
        "docker", "exec", "ss-backend-512m",
        "/opt/java/openjdk/bin/jfr",
        "-J-Xms8m", "-J-Xmx32m", "-J-XX:+UseSerialGC", "-J-XX:TieredStopAtLevel=1",
        "summary", "/tmp/collector_prof.jfr"
    ], text=True).strip()
    jfr_weighted_stacks_out = subprocess.check_output([
        "docker", "exec", "ss-backend-512m", "sh", "-c",
        """/opt/java/openjdk/bin/jfr -J-Xms8m -J-Xmx32m -J-XX:+UseSerialGC -J-XX:TieredStopAtLevel=1 print --events jdk.ObjectAllocationSample --stack-depth 32 /tmp/collector_prof.jfr | awk '
function parse_bytes(val, unit,   v) {
  v = val + 0.0
  if (unit == "B" || unit == "bytes") return v
  if (unit == "kB" || unit == "KB") return v * 1024.0
  if (unit == "MB" || unit == "MiB") return v * 1048576.0
  if (unit == "GB" || unit == "GiB") return v * 1073741824.0
  return v
}
/^jdk\\.ObjectAllocationSample \\{/ { in_ev = 1; obj = "unknown"; w_bytes = 0; in_stack = 0; depth = 0; stack_sig = ""; next }
in_ev && /^  objectClass = / { sub(/^  objectClass = /, "", $0); sub(/ \\(classLoader = .*\\)$/, "", $0); obj = $0; next }
in_ev && /^  weight = / { w_bytes = parse_bytes($3, $4); next }
in_ev && /^  stackTrace = \\[/ { in_stack = 1; depth = 0; stack_sig = ""; next }
in_ev && in_stack {
  if ($0 ~ /^  \\]/) { in_stack = 0; next }
  if (depth < 6) { line = $0; sub(/^[[:space:]]+/, "", line); stack_sig = (stack_sig == "" ? line : stack_sig " <- " line); depth++ }
  next
}
in_ev && /^\\}/ {
  key = obj " | " (stack_sig == "" ? "<no-stack>" : stack_sig)
  cnt[key] += 1; bytes[key] += w_bytes; total_cnt += 1; total_bytes += w_bytes; in_ev = 0
}
END {
  printf "TOTAL_ALLOC_SAMPLES=%d TOTAL_WEIGHT_MIB=%.2f\\n", total_cnt, total_bytes / 1048576.0
  for (k in bytes) printf "%10.2f MiB | %5d samples | %s\\n", bytes[k] / 1048576.0, cnt[k], k
}' | sort -rn | head -n 15"""
    ], text=True).strip()
    subprocess.check_call(["docker", "exec", "ss-backend-512m", "rm", "-f", "/tmp/collector_prof.jfr"])
    cg_after_jfr_cleanup = read_host_cgroup_snapshot(host_pid)

    print("=== JFR.stop & PRIVATE SUMMARY / TOP WEIGHTED ALLOCATION STACKS ===", flush=True)
    print(jfr_stop_out, flush=True)
    print("--- jfr summary (head 35 lines) ---", flush=True)
    print("\n".join(jfr_summary_out.splitlines()[:35]), flush=True)
    print("--- Top weighted ObjectAllocationSample stacks ---", flush=True)
    print(jfr_weighted_stacks_out, flush=True)
    print("=== JFR CGROUP MEMORY DELTAS (BEFORE -> DURING -> AFTER RM) ===", flush=True)
    print(json.dumps({
        "beforeJfrCurrentMiB": cg_before_jfr["cgroupCurrentMiB"],
        "duringJfrCurrentMiB": cg_during_jfr["cgroupCurrentMiB"],
        "afterJfrCleanupCurrentMiB": cg_after_jfr_cleanup["cgroupCurrentMiB"],
        "afterJfrPeakMiB": cg_after_jfr_cleanup["cgroupPeakMiB"]
    }, indent=2), flush=True)
    assert "jdk.ObjectAllocationSample" in jfr_summary_out, "Expected jdk.ObjectAllocationSample in jfr summary!"
    for disabled_ev in ("jdk.InitialEnvironmentVariable", "jdk.InitialSystemProperty", "jdk.SystemProcess", "jdk.JVMInformation"):
        assert re.search(rf"^\s*{re.escape(disabled_ev)}\s+0\s+0\s*$", jfr_summary_out, re.MULTILINE), (
            f"Expected {disabled_ev} count to be 0 in jfr summary!"
        )

    print("=== 1-BATCH COLLECTOR RUNNER RESULT ===", flush=True)
    print(json.dumps({
        "productiveBatchesCompleted": batch_res.get("productiveBatchesCompleted"),
        "sessionProductiveBatchesCompleted": batch_res.get("sessionProductiveBatchesCompleted"),
        "maxSessionProductiveBatchesConfigured": batch_res.get("maxSessionProductiveBatchesConfigured"),
        "sessionNetworkCallsMade": batch_res.get("sessionNetworkCallsMade"),
        "maxSessionNetworkCallsConfigured": batch_res.get("maxSessionNetworkCallsConfigured"),
        "overnightSessionArmed": batch_res.get("overnightSessionArmed"),
        "processedPuuids": batch_res.get("checkpoint", {}).get("processedPuuids"),
        "pendingCount": len(batch_res.get("checkpoint", {}).get("pendingCandidatePuuids", [])),
        "totalNetworkCallsMade": batch_res.get("checkpoint", {}).get("totalNetworkCallsMade"),
        "lastStopReason": batch_res.get("checkpoint", {}).get("lastStopReason")
    }, indent=2), flush=True)

    # Only set NMT baseline if NMT is enabled in the container so we do not trigger unnecessary MBean overhead when NMT is off
    if os.environ.get("ENABLE_NMT", "0") == "1":
        try:
            http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=baseline", headers=op_headers, timeout=15)
        except Exception:
            pass
    pre_cycle_host = sample_host_proc_and_cgroup(host_pid)
    record_5s_sample("pre_cycle")
    host_samples_by_stage = {
        "idle": idle_host,
        "pre_cycle": pre_cycle_host,
        "cycles": {}
    }

    proof_mode = os.environ.get("PROOF_MODE", "10_CYCLE")
    cycle_metrics = []
    if proof_mode == "SUSTAINED_30M":
        print("=== 3. RUNNING SUSTAINED 30-MINUTE (1800s) GATED LOAD (SAMPLED AT LEAST EVERY 30s + 5s BACKGROUND SAMPLER + memory.peak, MOCK RIOT SERVER != LIVE INTERNET) ===", flush=True)
        sustained_t0 = time.time()
        last_sample_ts = sustained_t0
        # Record initial sample 0 at t=0.0s before the ~13.5s cold 450-match load so sample interval is strictly <= 30s
        snap0 = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)["memoryHeadroom"]
        cg0 = record_5s_sample("sustained_sample_0")
        smax0 = get_5s_sampler_max()
        mstat0 = parse_kv_bytes_file(f"/proc/{host_pid}/root/sys/fs/cgroup/memory.stat")
        rollup0 = parse_proc_kb_file(f"/proc/{host_pid}/smaps_rollup")
        row0 = {
            "sample": 0,
            "elapsedSec": 0.0,
            "deltaSec": 0.0,
            "recentMatchesCount": 0,
            "reportSource": "PRE_LOAD_BASELINE",
            "profileLoadMs": 0.0,
            "reportMs": 0.0,
            "cgroupCurrentBytes": snap0["cgroupMemoryCurrentBytes"],
            "cgroupCurrentMiB": round(snap0["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2),
            "cgroupRatioPct": round(snap0["containerUtilizationRatio"] * 100.0, 2),
            "cgroupPeakBytes": cg0["cgroupPeakBytes"],
            "cgroupPeakMiB": cg0["cgroupPeakMiB"],
            "cgroupPeakRatioPct": cg0["cgroupPeakRatioPct"],
            "sampler5sMaxCurrentMiB": smax0["maxCurrentMiB"],
            "sampler5sMaxRatioPct": smax0["maxCurrentRatioPct"],
            "cgroupAnonMiB": round(mstat0.get("anon", 0) / (1024 * 1024), 2),
            "cgroupFileMiB": round(mstat0.get("file", 0) / (1024 * 1024), 2),
            "cgroupSlabMiB": round(mstat0.get("slab", 0) / (1024 * 1024), 2),
            "cgroupSockMiB": round(mstat0.get("sock", 0) / (1024 * 1024), 2),
            "smapsRssMiB": round(rollup0.get("Rss", 0) / (1024 * 1024), 2),
            "heapUsedMiB": round(snap0["jvmHeapUsedBytes"] / (1024 * 1024), 2),
            "heapCommittedMiB": round(snap0["jvmHeapCommittedBytes"] / (1024 * 1024), 2),
            "heapUnusedCommittedMiB": round(snap0["jvmHeapUnusedCommittedBytes"] / (1024 * 1024), 2),
            "metaspacePoolUsedMiB": round(snap0["jvmMetaspaceUsedBytes"] / (1024 * 1024), 2),
            "metaspacePoolCommittedMiB": round(snap0["jvmMetaspaceCommittedBytes"] / (1024 * 1024), 2),
            "compressedClassUsedMiB": round(snap0["jvmCompressedClassSpaceUsedBytes"] / (1024 * 1024), 2),
            "compressedClassCommittedMiB": round(snap0["jvmCompressedClassSpaceCommittedBytes"] / (1024 * 1024), 2),
            "codeCacheUsedMiB": round(snap0["jvmCodeCacheUsedBytes"] / (1024 * 1024), 2),
            "nonHeapTotalUsedMiB": round(snap0["jvmNonHeapUsedBytes"] / (1024 * 1024), 2),
            "directUsedMiB": round(snap0["directBufferUsedBytes"] / (1024 * 1024), 2),
            "nativeResidualMiB": round(snap0["estimatedNativeResidualBytes"] / (1024 * 1024), 2),
            "threads": snap0["jvmThreadCount"],
            "hasHeadroom": snap0["hasHeadroomForLowPriority"]
        }
        cycle_metrics.append(row0)
        print(f"SUSTAINED_SAMPLE 00: {json.dumps(row0)}", flush=True)

        sample_idx = 0
        while True:
            sample_idx += 1
            t0 = time.time()
            if sample_idx > 1:
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
            cg_now = record_5s_sample(f"sustained_sample_{sample_idx}")
            smax_now = get_5s_sampler_max()
            now_ts = time.time()
            elapsed_sec = now_ts - sustained_t0
            delta_sec = now_ts - last_sample_ts
            last_sample_ts = now_ts

            mstat = parse_kv_bytes_file(f"/proc/{host_pid}/root/sys/fs/cgroup/memory.stat")
            rollup = parse_proc_kb_file(f"/proc/{host_pid}/smaps_rollup")
            if sample_idx in (1, 6, 7) or elapsed_sec >= 1800.0:
                curr_host = sample_host_proc_and_cgroup(host_pid)
                if sample_idx in (1, 6, 7):
                    host_samples_by_stage["cycles"][str(sample_idx)] = curr_host
                if elapsed_sec >= 1800.0:
                    host_samples_by_stage["cycles"]["10"] = curr_host

            row = {
                "sample": sample_idx,
                "elapsedSec": round(elapsed_sec, 2),
                "deltaSec": round(delta_sec, 2),
                "recentMatchesCount": len(prof.get("recentMatches") or []),
                "reportSource": rep.get("reportSource"),
                "profileLoadMs": round(t_prof, 1),
                "reportMs": round(t_rep, 1),
                "cgroupCurrentBytes": snap["cgroupMemoryCurrentBytes"],
                "cgroupCurrentMiB": round(snap["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2),
                "cgroupRatioPct": round(snap["containerUtilizationRatio"] * 100.0, 2),
                "cgroupPeakBytes": cg_now["cgroupPeakBytes"],
                "cgroupPeakMiB": cg_now["cgroupPeakMiB"],
                "cgroupPeakRatioPct": cg_now["cgroupPeakRatioPct"],
                "sampler5sMaxCurrentMiB": smax_now["maxCurrentMiB"],
                "sampler5sMaxRatioPct": smax_now["maxCurrentRatioPct"],
                "cgroupAnonMiB": round(mstat.get("anon", 0) / (1024 * 1024), 2),
                "cgroupFileMiB": round(mstat.get("file", 0) / (1024 * 1024), 2),
                "cgroupSlabMiB": round(mstat.get("slab", 0) / (1024 * 1024), 2),
                "cgroupSockMiB": round(mstat.get("sock", 0) / (1024 * 1024), 2),
                "smapsRssMiB": round(rollup.get("Rss", 0) / (1024 * 1024), 2),
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
            print(f"SUSTAINED_SAMPLE {sample_idx:02d}: {json.dumps(row)}", flush=True)
            if elapsed_sec >= 1800.0:
                break
            remaining_to_1800 = 1800.0 - elapsed_sec
            sleep_sec = min(25.0, max(1.0, remaining_to_1800))
            time.sleep(sleep_sec)

        warm_rows = [r for r in cycle_metrics if r.get("sample", 0) >= 2]
        max_delta_sec = max(r["deltaSec"] for r in cycle_metrics)
        smax_end = get_5s_sampler_max()
        sustained_summary = {
            "proofMode": "SUSTAINED_30M",
            "totalDurationSec": cycle_metrics[-1]["elapsedSec"],
            "sampleCount": len(cycle_metrics),
            "maxSampleIntervalSec": max_delta_sec,
            "minCgroupCurrentMiB": min(r["cgroupCurrentMiB"] for r in cycle_metrics),
            "maxCgroupCurrentMiB": max(r["cgroupCurrentMiB"] for r in cycle_metrics),
            "finalCgroupCurrentMiB": cycle_metrics[-1]["cgroupCurrentMiB"],
            "minCgroupRatioPct": min(r["cgroupRatioPct"] for r in cycle_metrics),
            "maxCgroupRatioPct": max(r["cgroupRatioPct"] for r in cycle_metrics),
            "finalCgroupRatioPct": cycle_metrics[-1]["cgroupRatioPct"],
            "maxCgroupPeakBytes": max(r["cgroupPeakBytes"] for r in cycle_metrics),
            "maxCgroupPeakMiB": max(r["cgroupPeakMiB"] for r in cycle_metrics),
            "maxCgroupPeakRatioPct": max(r["cgroupPeakRatioPct"] for r in cycle_metrics),
            "sampler5sSampleCount": smax_end["sampleCount"],
            "sampler5sMaxCurrentBytes": smax_end["maxCurrentBytes"],
            "sampler5sMaxCurrentMiB": smax_end["maxCurrentMiB"],
            "sampler5sMaxCurrentRatioPct": smax_end["maxCurrentRatioPct"],
            "sampler5sMaxPeakBytes": smax_end["maxPeakBytes"],
            "sampler5sMaxPeakMiB": smax_end["maxPeakMiB"],
            "sampler5sMaxPeakRatioPct": smax_end["maxPeakRatioPct"],
            "warmProfileLoadMsMin": min(r["profileLoadMs"] for r in warm_rows),
            "warmProfileLoadMsMax": max(r["profileLoadMs"] for r in warm_rows),
            "warmProfileLoadMsAvg": round(sum(r["profileLoadMs"] for r in warm_rows) / len(warm_rows), 1),
            "warmReportMsMin": min(r["reportMs"] for r in warm_rows),
            "warmReportMsMax": max(r["reportMs"] for r in warm_rows),
            "warmReportMsAvg": round(sum(r["reportMs"] for r in warm_rows) / len(warm_rows), 1),
            "liveRiotMemoryProofNote": "Mocked local HTTPS Riot server does not prove live internet Riot memory."
        }
        print("=== SUSTAINED 30-MINUTE SUMMARY ===", flush=True)
        print(json.dumps(sustained_summary, indent=2), flush=True)
    else:
        print("=== 3. RUNNING 10 CYCLES OF (450-MATCH PROFILE LOAD + COHORT REPORT + MEMORY SNAPSHOT + 5s SAMPLER + memory.peak) ===", flush=True)
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
            cg_now = record_5s_sample(f"cycle_{cycle}")
            smax_now = get_5s_sampler_max()
            curr_host = sample_host_proc_and_cgroup(host_pid)
            host_samples_by_stage["cycles"][str(cycle)] = curr_host

            row = {
                "cycle": cycle,
                "recentMatchesCount": len(prof.get("recentMatches") or []),
                "reportSource": rep.get("reportSource"),
                "profileLoadMs": round(t_prof, 1),
                "reportMs": round(t_rep, 1),
                "cgroupCurrentBytes": snap["cgroupMemoryCurrentBytes"],
                "cgroupCurrentMiB": round(snap["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2),
                "cgroupRatioPct": round(snap["containerUtilizationRatio"] * 100.0, 2),
                "cgroupPeakBytes": cg_now["cgroupPeakBytes"],
                "cgroupPeakMiB": cg_now["cgroupPeakMiB"],
                "cgroupPeakRatioPct": cg_now["cgroupPeakRatioPct"],
                "sampler5sMaxCurrentMiB": smax_now["maxCurrentMiB"],
                "sampler5sMaxRatioPct": smax_now["maxCurrentRatioPct"],
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

        warm_rows_10 = [r for r in cycle_metrics if r.get("cycle", 0) >= 2]
        smax_10 = get_5s_sampler_max()
        ten_cycle_summary = {
            "proofMode": "10_CYCLE",
            "cycleCount": len(cycle_metrics),
            "minCgroupCurrentBytes": min(r["cgroupCurrentBytes"] for r in cycle_metrics),
            "minCgroupCurrentMiB": min(r["cgroupCurrentMiB"] for r in cycle_metrics),
            "maxCgroupCurrentBytes": max(r["cgroupCurrentBytes"] for r in cycle_metrics),
            "maxCgroupCurrentMiB": max(r["cgroupCurrentMiB"] for r in cycle_metrics),
            "finalCgroupCurrentBytes": cycle_metrics[-1]["cgroupCurrentBytes"],
            "finalCgroupCurrentMiB": cycle_metrics[-1]["cgroupCurrentMiB"],
            "minCgroupRatioPct": min(r["cgroupRatioPct"] for r in cycle_metrics),
            "maxCgroupRatioPct": max(r["cgroupRatioPct"] for r in cycle_metrics),
            "finalCgroupRatioPct": cycle_metrics[-1]["cgroupRatioPct"],
            "maxCgroupPeakBytes": max(r["cgroupPeakBytes"] for r in cycle_metrics),
            "maxCgroupPeakMiB": max(r["cgroupPeakMiB"] for r in cycle_metrics),
            "maxCgroupPeakRatioPct": max(r["cgroupPeakRatioPct"] for r in cycle_metrics),
            "sampler5sSampleCount": smax_10["sampleCount"],
            "sampler5sMaxCurrentBytes": smax_10["maxCurrentBytes"],
            "sampler5sMaxCurrentMiB": smax_10["maxCurrentMiB"],
            "sampler5sMaxCurrentRatioPct": smax_10["maxCurrentRatioPct"],
            "sampler5sMaxPeakBytes": smax_10["maxPeakBytes"],
            "sampler5sMaxPeakMiB": smax_10["maxPeakMiB"],
            "sampler5sMaxPeakRatioPct": smax_10["maxPeakRatioPct"],
            "warmProfileLoadMsMin": min(r["profileLoadMs"] for r in warm_rows_10),
            "warmProfileLoadMsMax": max(r["profileLoadMs"] for r in warm_rows_10),
            "warmProfileLoadMsAvg": round(sum(r["profileLoadMs"] for r in warm_rows_10) / len(warm_rows_10), 1),
            "warmReportMsMin": min(r["reportMs"] for r in warm_rows_10),
            "warmReportMsMax": max(r["reportMs"] for r in warm_rows_10),
            "warmReportMsAvg": round(sum(r["reportMs"] for r in warm_rows_10) / len(warm_rows_10), 1),
            "liveRiotMemoryProofNote": "Mocked local HTTPS Riot server does not prove live internet Riot memory."
        }
        print("=== 10-CYCLE SUMMARY ===", flush=True)
        print(json.dumps(ten_cycle_summary, indent=2), flush=True)

    print("=== 4. VERIFYING POSTGRES CHECKPOINT & COHORT CELL DURABILITY ACROSS REDIS FLUSHALL ===", flush=True)
    pg_rows = subprocess.check_output([
        "docker", "exec", "ss-postgres",
        "psql", "-U", "postgres", "-d", "summoner_searcher", "-t", "-A", "-c",
        "SELECT region, target_patch, queue_id, last_stop_reason, total_network_calls_made, processed_puuids_count, pending_candidates_count FROM cohort_collector_checkpoints ORDER BY target_patch;"
    ]).decode("utf-8").strip()
    print("POSTGRES cohort_collector_checkpoints ROWS:")
    print(pg_rows, flush=True)

    pg_sample_rows = subprocess.check_output([
        "docker", "exec", "ss-postgres",
        "psql", "-U", "postgres", "-d", "summoner_searcher", "-t", "-A", "-c",
        "SELECT region, patch, queue_id, tier, role, distinct_players, eligible_games, passes_quality_gate FROM cohort_cell_sample_states ORDER BY tier, role;"
    ]).decode("utf-8").strip()
    print("POSTGRES cohort_cell_sample_states ROWS:")
    print(pg_sample_rows, flush=True)

    pg_cell_rows = subprocess.check_output([
        "docker", "exec", "ss-postgres",
        "psql", "-U", "postgres", "-d", "summoner_searcher", "-t", "-A", "-c",
        "SELECT region, patch, queue_id, tier, role, distinct_players, eligible_games, passes_quality_gate FROM cohort_benchmark_cells ORDER BY tier, role;"
    ]).decode("utf-8").strip()
    print("POSTGRES cohort_benchmark_cells ROWS:")
    print(pg_cell_rows, flush=True)

    subprocess.check_call(["docker", "exec", "ss-redis", "redis-cli", "FLUSHALL"])
    after_flush_status = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)
    after_flush_cells = http_json(f"{base_url}/api/riot/cohorts/euw1?patch=16.7", timeout=15)
    after_flush_report = http_json(f"{base_url}/api/riot/cohorts/euw1/report?patch=16.7", timeout=30)
    after_flush_mem = after_flush_status["memoryHeadroom"]
    after_flush_cg = record_5s_sample("after_flush")
    recovered_cp = after_flush_status.get("checkpoint")
    print("RECOVERED CHECKPOINT & CELLS AFTER REDIS FLUSHALL:")
    print(json.dumps({
        "region": recovered_cp.get("region"),
        "targetPatch": recovered_cp.get("targetPatch"),
        "processedPuuids": recovered_cp.get("processedPuuids"),
        "pendingCount": len(recovered_cp.get("pendingCandidatePuuids", [])),
        "totalNetworkCallsMade": recovered_cp.get("totalNetworkCallsMade"),
        "lastStopReason": recovered_cp.get("lastStopReason"),
        "persistedCellsCountAfterFlush": len(after_flush_cells),
        "reportCellsCountAfterFlush": len(after_flush_report.get("cells", [])),
        "reportEligibleSamplesAfterFlush": after_flush_report.get("inventory", {}).get("eligibleCohortSamples", 0)
    }, indent=2), flush=True)

    after_flush_row = {
        "stage": "AFTER_REDIS_FLUSHALL_AND_PG_CHECKPOINT_RECOVERY",
        "cgroupCurrentBytes": after_flush_mem["cgroupMemoryCurrentBytes"],
        "cgroupCurrentMiB": round(after_flush_mem["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2),
        "cgroupRatioPct": round(after_flush_mem["containerUtilizationRatio"] * 100.0, 2),
        "hostCgroupCurrentBytes": after_flush_cg["cgroupCurrentBytes"],
        "hostCgroupCurrentMiB": after_flush_cg["cgroupCurrentMiB"],
        "hostCgroupRatioPct": after_flush_cg["cgroupRatioPct"],
        "cgroupPeakBytes": after_flush_cg["cgroupPeakBytes"],
        "cgroupPeakMiB": after_flush_cg["cgroupPeakMiB"],
        "cgroupPeakRatioPct": after_flush_cg["cgroupPeakRatioPct"],
        "heapUsedMiB": round(after_flush_mem["jvmHeapUsedBytes"] / (1024 * 1024), 2),
        "heapCommittedMiB": round(after_flush_mem["jvmHeapCommittedBytes"] / (1024 * 1024), 2),
        "nonHeapTotalUsedMiB": round(after_flush_mem["jvmNonHeapUsedBytes"] / (1024 * 1024), 2),
        "nativeResidualMiB": round(after_flush_mem["estimatedNativeResidualBytes"] / (1024 * 1024), 2),
        "hasHeadroom": after_flush_mem["hasHeadroomForLowPriority"]
    }
    print("=== AFTER-FLUSH CHECKPOINT RECOVERY MEMORY SNAPSHOT ===", flush=True)
    print(json.dumps(after_flush_row, indent=2), flush=True)

    cg_final = read_container_events_and_cgroup("ss-backend-512m", host_pid)
    record_5s_sample("final")
    sampler_stop.set()
    sampler_summary = get_5s_sampler_max()
    print("=== 5-SECOND BACKGROUND CGROUP SAMPLER SUMMARY ===", flush=True)
    print(json.dumps(sampler_summary, indent=2), flush=True)
    print("=== FINAL CGROUP V2 EVENTS & OOM CHECK ===", flush=True)
    print(json.dumps(cg_final, indent=2), flush=True)

    # Persist passive host samples so the separate non-gating diagnostic step can inspect them even if an assert fails below
    with open("/tmp/proof_host_samples.json", "w") as f:
        json.dump({
            "hostPid": host_pid,
            "cycleMetrics": cycle_metrics,
            "afterFlush": after_flush_row,
            "samplerSummary": sampler_summary,
            "finalCgroup": cg_final,
            "samples": host_samples_by_stage
        }, f)

    assert not cg_final["oomKilled"], "Container was OOMKilled!"
    assert cg_final["restartCount"] == 0, "Container restarted unexpectedly!"
    assert recovered_cp is not None and recovered_cp.get("totalNetworkCallsMade", 0) > 0, "Postgres checkpoint recovery failed!"
    assert len(after_flush_cells) > 0 and after_flush_report.get("inventory", {}).get("eligibleCohortSamples", 0) > 0, (
        "Durable Postgres cohort cells recovery after Redis FLUSHALL failed!"
    )
    if proof_mode == "SUSTAINED_30M":
        assert cycle_metrics[-1]["elapsedSec"] >= 1800.0, f"Sustained run was shorter than 1800s ({cycle_metrics[-1]['elapsedSec']}s)!"
        assert max(r["deltaSec"] for r in cycle_metrics) <= 30.0, f"Sample interval exceeded 30s ({max(r['deltaSec'] for r in cycle_metrics)}s)!"
    assert all(c["cgroupRatioPct"] < 78.0 for c in cycle_metrics), (
        f"Checkpoint cgroupRatioPct exceeded 78.0% gate! Max was {max(c['cgroupRatioPct'] for c in cycle_metrics):.2f}%"
    )
    assert all(c["cgroupPeakRatioPct"] < 78.0 for c in cycle_metrics), (
        f"Cycle cgroupPeakRatioPct (memory.peak) exceeded 78.0% gate! Max was {max(c['cgroupPeakRatioPct'] for c in cycle_metrics):.2f}%"
    )
    assert sampler_summary["maxCurrentRatioPct"] < 78.0, (
        f"5s background sampler maxCurrentRatioPct exceeded 78.0% gate! Max was {sampler_summary['maxCurrentRatioPct']:.2f}%"
    )
    assert sampler_summary["maxPeakRatioPct"] < 78.0, (
        f"5s background sampler maxPeakRatioPct (memory.peak) exceeded 78.0% gate! Max was {sampler_summary['maxPeakRatioPct']:.2f}%"
    )
    assert after_flush_row["cgroupRatioPct"] < 78.0, (
        f"After-FLUSHALL checkpoint recovery cgroupRatioPct exceeded 78.0% gate! Was {after_flush_row['cgroupRatioPct']:.2f}%"
    )
    assert after_flush_row["hostCgroupRatioPct"] < 78.0, (
        f"After-FLUSHALL hostCgroupRatioPct exceeded 78.0% gate! Was {after_flush_row['hostCgroupRatioPct']:.2f}%"
    )
    assert after_flush_row["cgroupPeakRatioPct"] < 78.0, (
        f"After-FLUSHALL cgroupPeakRatioPct (memory.peak) exceeded 78.0% gate! Was {after_flush_row['cgroupPeakRatioPct']:.2f}%"
    )
    assert cg_final["cgroupRatioPct"] < 78.0, (
        f"Final cgroupRatioPct exceeded 78.0% gate! Was {cg_final['cgroupRatioPct']:.2f}%"
    )
    assert cg_final["cgroupPeakRatioPct"] < 78.0, (
        f"Final cgroupPeakRatioPct (memory.peak) exceeded 78.0% gate! Was {cg_final['cgroupPeakRatioPct']:.2f}%"
    )
    print("=== ALL 512 MB CONTAINER PROOFS PASSED (CHECKPOINTS, AFTER-WORK, 5s SAMPLER, AND memory.peak ALL < 78.0%) ===", flush=True)

if __name__ == "__main__":
    main()


