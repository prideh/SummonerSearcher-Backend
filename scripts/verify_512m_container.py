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

def format_mib_diff(before_bytes: int, after_bytes: int) -> str:
    b_mib = before_bytes / (1024 * 1024)
    a_mib = after_bytes / (1024 * 1024)
    d_mib = a_mib - b_mib
    sign = "+" if d_mib >= 0 else ""
    return f"{b_mib:.2f} -> {a_mib:.2f} MiB ({sign}{d_mib:.2f} MiB)"

def print_host_diff(label: str, prev: dict, curr: dict):
    print(f"=== HOST CGROUP & PROC DIFF: {label} ===")
    for k in ["anon", "file", "kernel", "slab", "sock", "shmem", "anon_thp", "file_mapped"]:
        b = prev["mstat"].get(k, 0)
        a = curr["mstat"].get(k, 0)
        if b > 0 or a > 0:
            print(f"  cgroup memory.stat[{k}]: {format_mib_diff(b, a)}")
    for k in ["VmPeak", "VmSize", "VmRSS", "RssAnon", "RssFile", "VmData", "VmStk"]:
        b = prev["status"].get(k, 0)
        a = curr["status"].get(k, 0)
        if b > 0 or a > 0:
            print(f"  /proc/pid/status[{k}]: {format_mib_diff(b, a)}")
    for k in ["Rss", "Pss", "Anonymous", "AnonHugePages", "Private_Dirty", "Private_Clean"]:
        b = prev["rollup"].get(k, 0)
        a = curr["rollup"].get(k, 0)
        if b > 0 or a > 0:
            print(f"  /proc/pid/smaps_rollup[{k}]: {format_mib_diff(b, a)}")

    # Compute VMA diffs
    all_keys = set(prev["smaps"].keys()) | set(curr["smaps"].keys())
    vma_diffs = []
    for k in all_keys:
        b_vma = prev["smaps"].get(k, {"Size": 0, "Rss": 0, "Anonymous": 0, "AnonHugePages": 0})
        a_vma = curr["smaps"].get(k, {"Size": 0, "Rss": 0, "Anonymous": 0, "AnonHugePages": 0})
        d_rss = a_vma["Rss"] - b_vma["Rss"]
        d_size = a_vma["Size"] - b_vma["Size"]
        if abs(d_rss) >= 65536 or abs(d_size) >= 262144:
            vma_diffs.append((d_rss, d_size, k, b_vma, a_vma))
    vma_diffs.sort(key=lambda x: abs(x[0]), reverse=True)
    print("  Top /proc/pid/smaps VMA changes (by |delta RSS|):")
    for d_rss, d_size, k, b_vma, a_vma in vma_diffs[:15]:
        print(
            f"    {k}: RSS {format_mib_diff(b_vma['Rss'], a_vma['Rss'])} | "
            f"Size {format_mib_diff(b_vma['Size'], a_vma['Size'])} | "
            f"Anon {format_mib_diff(b_vma['Anonymous'], a_vma['Anonymous'])}"
        )

def extract_detail_diff_highlights(detail_diff_text: str) -> str:
    lines = detail_diff_text.splitlines()
    out = []
    # 1. Include everything up to "Virtual memory map:" or first 55 lines (the Summary diff section)
    # 2. Include any Virtual memory map region or Call site that has a non-zero "+/-" diff
    in_vmap = False
    cur_block = []
    for line in lines:
        if "Virtual memory map:" in line:
            in_vmap = True
            out.append(line)
            continue
        if not in_vmap:
            out.append(line)
        else:
            if line.startswith("[0x") or line.startswith("0x") or (line.strip().startswith("[0x")):
                if cur_block:
                    block_txt = "\n".join(cur_block)
                    if "+" in block_txt or "-" in block_txt:
                        out.extend(cur_block)
                cur_block = [line]
            elif line.startswith("Details:"):
                if cur_block:
                    block_txt = "\n".join(cur_block)
                    if "+" in block_txt or "-" in block_txt:
                        out.extend(cur_block)
                cur_block = [line]
                break
            else:
                cur_block.append(line)
    # Also grab top non-zero call sites from Details:
    if "Details:" in detail_diff_text:
        details_part = detail_diff_text.split("Details:", 1)[1]
        out.append("Details (top non-zero native allocation call sites):")
        site_blocks = details_part.split("\n\n")
        matched_sites = 0
        for sb in site_blocks:
            if ("+" in sb or "-" in sb) and ("committed=" in sb or "malloc=" in sb):
                out.append(sb.strip())
                matched_sites += 1
                if matched_sites >= 15:
                    break
    return "\n".join(out)

def main():
    cert_file = sys.argv[1]
    key_file = sys.argv[2]
    print("=== Starting Mock Riot HTTPS API server on port 443 ===", flush=True)
    httpd = start_mock_riot_server(cert_file, key_file)
    time.sleep(1)

    print("=== Starting ss-backend-512m Docker container (--memory=512m --memory-swap=512m) ===", flush=True)
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

    # Check container libc and host PID once before warmer finishes
    host_pid = int(subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", "ss-backend-512m"]).decode("utf-8").strip())
    libc_info = subprocess.check_output(["docker", "exec", "ss-backend-512m", "sh", "-c", "ldd --version 2>&1 || true"]).decode("utf-8", errors="replace").strip()
    print(f"Container Host PID: {host_pid}")
    print(f"Container libc info:\n{libc_info}")

    # Wait 3s for startup ChallengerCacheWarmer to finish sequential warming
    time.sleep(3)
    idle_status = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=10)
    idle_mem = idle_status["memoryHeadroom"]
    idle_nmt_summary = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=summary", headers=op_headers, timeout=15)
    http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=baseline", headers=op_headers, timeout=15)
    prev_host = sample_host_proc_and_cgroup(host_pid)
    idle_host = prev_host

    print("=== 2. IDLE + POST-WARMER MEMORY HEADROOM SNAPSHOT ===", flush=True)
    print(json.dumps(idle_mem, indent=2))
    print("=== IDLE JVM ARGS, MALLOC_ARENA_MAX, THREADS, POOLS, BUFFER POOLS, GC BEANS ===")
    print(json.dumps({
        "vmArgs": idle_nmt_summary.get("vmArgs"),
        "mallocArenaMax": idle_nmt_summary.get("mallocArenaMax"),
        "threadCount": idle_nmt_summary.get("threadCount"),
        "peakThreadCount": idle_nmt_summary.get("peakThreadCount"),
        "nonHeapMemoryUsage": idle_nmt_summary.get("nonHeapMemoryUsage"),
        "heapMemoryUsage": idle_nmt_summary.get("heapMemoryUsage"),
        "memoryPools": idle_nmt_summary.get("memoryPools"),
        "bufferPools": idle_nmt_summary.get("bufferPools"),
        "gcBeans": idle_nmt_summary.get("gcBeans")
    }, indent=2))
    print("=== IDLE NMT SUMMARY ===")
    print(idle_nmt_summary.get("nmtOutput", ""))

    # Run bounded diagnostic probe (3 calls) and 1 collector batch
    probe_res = http_json(
        f"{base_url}/api/riot/cohorts/euw1/collector/probe?patch=16.7&seedPuuid=seed-puuid-1",
        method="POST",
        headers=op_headers,
        timeout=30
    )
    batch_res = http_json(
        f"{base_url}/api/riot/cohorts/euw1/collector/start?patch=16.7&maxBatches=1&batchSize=2&maxGamesPerPlayer=10&async=false&seedPuuids=seed-puuid-1",
        method="POST",
        headers=op_headers,
        timeout=60
    )
    print("=== PRE-CYCLE BASELINE RESET (AFTER PROBE + 1 BATCH) ===", flush=True)
    http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=baseline", headers=op_headers, timeout=15)
    prev_host = sample_host_proc_and_cgroup(host_pid)
    prev_cgroup_mib = idle_mem["cgroupMemoryCurrentBytes"] / (1024 * 1024)

    print("=== 3. RUNNING 10 CYCLES OF (450-MATCH PROFILE LOAD + COHORT REPORT + NMT/CGROUP/SMAPS DIFF) ===", flush=True)
    cycle_metrics = []
    for cycle in range(1, 11):
        t0 = time.time()
        if cycle > 1:
            subprocess.check_call(["docker", "exec", "ss-redis", "redis-cli", "DEL", "player:profile:euw1:puuid-main-450"], stdout=subprocess.DEVNULL)
        prof = http_json(
            f"{base_url}/api/riot/summoner/euw1/TestPlayer/EUW?refresh=true",
            method="GET",
            timeout=120
        )
        t_prof = (time.time() - t0) * 1000.0
        t1 = time.time()
        rep = http_json(f"{base_url}/api/riot/cohorts/euw1/report?patch=16.7&recompute=true", timeout=60)
        t_rep = (time.time() - t1) * 1000.0
        snap = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)["memoryHeadroom"]
        nmt_diff = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=detail.diff", headers=op_headers, timeout=20)
        curr_host = sample_host_proc_and_cgroup(host_pid)

        cgroup_mib = round(snap["cgroupMemoryCurrentBytes"] / (1024 * 1024), 2)
        gc_beans = {g["name"]: g for g in (nmt_diff.get("gcBeans") or [])}
        pools_by_name = {p["name"]: p for p in (nmt_diff.get("memoryPools") or [])}
        tenured = pools_by_name.get("Tenured Gen", {})
        eden = pools_by_name.get("Eden Space", {})

        row = {
            "cycle": cycle,
            "profileLoadMs": round(t_prof, 1),
            "reportMs": round(t_rep, 1),
            "cgroupCurrentMiB": cgroup_mib,
            "cgroupRatioPct": round(snap["containerUtilizationRatio"] * 100.0, 2),
            "cgroupAnonMiB": round(curr_host["mstat"].get("anon", 0) / (1024 * 1024), 2),
            "cgroupFileMiB": round(curr_host["mstat"].get("file", 0) / (1024 * 1024), 2),
            "cgroupSlabMiB": round(curr_host["mstat"].get("slab", 0) / (1024 * 1024), 2),
            "cgroupSockMiB": round(curr_host["mstat"].get("sock", 0) / (1024 * 1024), 2),
            "smapsRssMiB": round(curr_host["rollup"].get("Rss", 0) / (1024 * 1024), 2),
            "smapsAnonMiB": round(curr_host["rollup"].get("Anonymous", 0) / (1024 * 1024), 2),
            "smapsAnonHugeMiB": round(curr_host["rollup"].get("AnonHugePages", 0) / (1024 * 1024), 2),
            "heapUsedMiB": round(snap["jvmHeapUsedBytes"] / (1024 * 1024), 2),
            "heapCommittedMiB": round(snap["jvmHeapCommittedBytes"] / (1024 * 1024), 2),
            "tenuredUsedMiB": round(tenured.get("used", 0) / (1024 * 1024), 2),
            "tenuredCommittedMiB": round(tenured.get("committed", 0) / (1024 * 1024), 2),
            "edenUsedMiB": round(eden.get("used", 0) / (1024 * 1024), 2),
            "edenCommittedMiB": round(eden.get("committed", 0) / (1024 * 1024), 2),
            "metaspacePoolUsedMiB": round(snap["jvmMetaspaceUsedBytes"] / (1024 * 1024), 2),
            "metaspacePoolCommittedMiB": round(snap["jvmMetaspaceCommittedBytes"] / (1024 * 1024), 2),
            "compressedClassUsedMiB": round(snap["jvmCompressedClassSpaceUsedBytes"] / (1024 * 1024), 2),
            "compressedClassCommittedMiB": round(snap["jvmCompressedClassSpaceCommittedBytes"] / (1024 * 1024), 2),
            "codeCacheUsedMiB": round(snap["jvmCodeCacheUsedBytes"] / (1024 * 1024), 2),
            "nonHeapTotalUsedMiB": round(snap["jvmNonHeapUsedBytes"] / (1024 * 1024), 2),
            "directUsedMiB": round(snap["directBufferUsedBytes"] / (1024 * 1024), 2),
            "nativeResidualMiB": round(snap["estimatedNativeResidualBytes"] / (1024 * 1024), 2),
            "threads": snap["jvmThreadCount"],
            "peakThreads": nmt_diff.get("peakThreadCount"),
            "copyGcCount": gc_beans.get("Copy", {}).get("collectionCount"),
            "copyGcMs": gc_beans.get("Copy", {}).get("collectionTimeMs"),
            "markSweepGcCount": gc_beans.get("MarkSweepCompact", {}).get("collectionCount"),
            "markSweepGcMs": gc_beans.get("MarkSweepCompact", {}).get("collectionTimeMs"),
            "hasHeadroom": snap["hasHeadroomForLowPriority"]
        }
        cycle_metrics.append(row)
        print(f"CYCLE {cycle:02d}: {json.dumps(row)}", flush=True)

        if cycle in (1, 6, 7, 10) or abs(cgroup_mib - prev_cgroup_mib) >= 8.0:
            print_host_diff(f"Cycle {cycle - 1} -> Cycle {cycle}", prev_host, curr_host)
            print(f"=== NMT DETAIL DIFF (Cycle {cycle - 1} -> Cycle {cycle}) ===")
            print(extract_detail_diff_highlights(nmt_diff.get("nmtOutput", "")), flush=True)

        # Reset NMT baseline to current cycle so next cycle diffs cleanly against this cycle
        http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=baseline", headers=op_headers, timeout=15)
        prev_host = curr_host
        prev_cgroup_mib = cgroup_mib

    print("=== 4. POST-CYCLE-10 EXPLICIT GC CHECK ===", flush=True)
    gc_nmt = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=gc", headers=op_headers, timeout=20)
    post_gc_snap = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)["memoryHeadroom"]
    post_gc_host = sample_host_proc_and_cgroup(host_pid)
    print("POST-GC MEMORY HEADROOM SNAPSHOT:")
    print(json.dumps(post_gc_snap, indent=2))
    print_host_diff("Cycle 10 -> Post-GC", prev_host, post_gc_host)
    print("=== POST-GC NMT DIFF (Cycle 10 -> Post-GC) ===")
    print(gc_nmt.get("nmtOutput", ""))

    post_gc_summary = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=summary", headers=op_headers, timeout=20)
    print("=== POST-GC FULL NMT SUMMARY ===")
    print(post_gc_summary.get("nmtOutput", ""), flush=True)

if __name__ == "__main__":
    main()
