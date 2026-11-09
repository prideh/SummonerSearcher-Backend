#!/usr/bin/env python3
"""
Isolated 512 MiB Container Proof for Challenger Leaderboard Cache Resilience & Bounded Recovery
(Candidate 5959a7036c9e281f72d17f0a0bd5003ba59476f8 + Rollback Base 139a7de48e67af9d664c08fd61ce131b6e63bd74).

Enforces:
1. Internal-only Docker bridge network (ab-isolated-net, 172.28.0.0/16) + explicit iptables DOCKER-USER/INPUT
   egress-reject rules + fail-closed nsenter -n negative-control outbound probe + zero manual JVM/GC overrides.
2. Real postgres:16-alpine (ss-postgres) and redis:7-alpine (ss-redis), and ss-backend-512m running with
   --memory=512m --memory-swap=512m and continuous 2-second host cgroup v2 sampling (< 78.0% / 64 MiB guard,
   cgroupCurrentBytes < 418,759,311, cgroupPeakBytes < 418,759,311, 0 OOMs, 0 restarts).
3. Challenger Leaderboard 512 MiB Container Verification Scenarios:
   - Scenario 1: Bounded EUW (300/300 local DB identities) & KR (5/300 local DB identities, 295 null) 300-entry
     hydration with 1 League-V4 call per region and 0 account-v1 fanout calls; cold NA1 >512 KiB chunked stream
     abort -> HTTP 503 Service Unavailable (never caching empty 200) + 30s failure backoff suppression.
   - Scenario 2: Singleflight concurrency coalescing on Redis cache miss (16 concurrent callers on EUW1 ->
     exact 1 upstream League-V4 call, all 16 callers receive 300 complete entries, 0 account-v1 calls).
   - Scenario 3: Real PostgreSQL ACCESS EXCLUSIVE lock on indexed_players -> bounded 1.5s JDBC query timeout &
     off-thread Statement.cancel() on dbCancelExecutor without hanging refresh or stranding workers, followed by
     post-lock DB identity lookup recovery.
   - Scenario 4: Upstream pre-header / response stall (> 5s) deadline abort on EUW1 -> aborts stalled HTTP call at
     ~5.0s deadline and serves/reseeds complete 300-entry last-known-good snapshot.
   - Scenario 5: Full Redis cache loss (redis-cli FLUSHALL) + upstream HTTP 503 outage on KR -> serves complete
     300-entry last-known-good snapshot (5/300 identities) and suppresses repeat upstream calls during 30s backoff.
   - Scenario 6: Cold NA1 recovery after 30s backoff expiry (HTTP 503 -> HTTP 200 with 300/300 DB identities) +
     warmed NA1 >512 KiB chunked body stream abort -> complete 300-entry last-known-good fallback with all 3
     supported regions (EUW1, KR, NA1) simultaneously resident under the 78% / 64 MiB cgroup memory guard.
   - Scenario 7: Rollback Base (139a7de48e67af9d664c08fd61ce131b6e63bd74) read compatibility against the live
     Postgres + Redis state under --memory=512m --memory-swap=512m.
"""

import http.server
import json
import os
import re
import secrets
import shutil
import socketserver
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

CANDIDATE_COMMIT_SHA = "5959a7036c9e281f72d17f0a0bd5003ba59476f8"
ROLLBACK_BASE_COMMIT_SHA = "139a7de48e67af9d664c08fd61ce131b6e63bd74"

NETWORK_NAME = "ab-isolated-net"
BRIDGE_NAME = "br-ab-isolated"
SUBNET_CIDR = "172.28.0.0/16"
GATEWAY_IP = "172.28.0.1"
POSTGRES_IP = "172.28.0.2"
REDIS_IP = "172.28.0.3"
BACKEND_IP = "172.28.0.4"
PORT_MOCK_RIOT = 443
BASE_URL = f"http://{BACKEND_IP}:10000"

CGROUP_MAX_BYTES = 536_870_912
# Strict raw-byte ceiling corresponding to 78.0% of 512 MiB (399.36 MiB = 418,759,311 bytes),
# leaving > 112.64 MiB free (> 64 MiB min-free-memory guard).
CGROUP_STRICT_GATE_BYTES = 418_759_311
MIN_FREE_HEADROOM_BYTES = 67_108_864

EPHEMERAL_DB_PASS = f"ephem_db_{secrets.token_hex(12)}"
EPHEMERAL_JWT_SECRET = secrets.token_hex(32)
EPHEMERAL_TWOFA_SECRET = secrets.token_hex(32)
EPHEMERAL_RIOT_TOKEN = f"ephem_riot_{secrets.token_hex(12)}"
EPHEMERAL_RESEND_TOKEN = f"ephem_resend_{secrets.token_hex(12)}"
EPHEMERAL_OP_TOKEN = f"ephem_op_{secrets.token_hex(12)}"
OP_HEADERS = {"X-Cohort-Operator-Token": EPHEMERAL_OP_TOKEN}

SECRET_REDACT_PATTERNS = [
    re.compile(r"RGAPI-[0-9a-fA-F-]{16,}"),
    re.compile(r"\bre_[0-9a-zA-Z_]{16,}\b"),
    re.compile(r"\bgh[pousr]_[0-9a-zA-Z]{20,}\b"),
    re.compile(r"\bgithub_pat_[0-9a-zA-Z_]{20,}\b"),
    re.compile(
        r'(?i)((?:db_pass|postgres_password|jwt_secret_key|twofa_secret_key|cohort_operator_token|riot_api_key|resend_api_key)\s*[=:]\s*)(?!\[REDACTED\])[^\s"\',;<>\]}]+'
    ),
]

EXPECTED_EXTRA_HOSTS = [
    f"euw1.api.riotgames.com:{GATEWAY_IP}",
    f"na1.api.riotgames.com:{GATEWAY_IP}",
    f"kr.api.riotgames.com:{GATEWAY_IP}",
    f"europe.api.riotgames.com:{GATEWAY_IP}",
    f"americas.api.riotgames.com:{GATEWAY_IP}",
    f"asia.api.riotgames.com:{GATEWAY_IP}",
    f"ddragon.leagueoflegends.com:{GATEWAY_IP}",
]


def sanitize_text(text: str) -> str:
    if not text:
        return text
    out = text
    for tok in (
        EPHEMERAL_DB_PASS,
        EPHEMERAL_JWT_SECRET,
        EPHEMERAL_TWOFA_SECRET,
        EPHEMERAL_RIOT_TOKEN,
        EPHEMERAL_RESEND_TOKEN,
        EPHEMERAL_OP_TOKEN,
    ):
        if tok:
            out = out.replace(tok, "[REDACTED]")
    for pat in SECRET_REDACT_PATTERNS[:4]:
        out = pat.sub("[REDACTED]", out)
    out = SECRET_REDACT_PATTERNS[4].sub(r"\1[REDACTED]", out)
    return out


def sanitize_json_value(val):
    if isinstance(val, str):
        return sanitize_text(val)
    if isinstance(val, dict):
        return {sanitize_json_value(k): sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [sanitize_json_value(x) for x in val]
    return val


def build_raw_challenger_league_json(region: str, entry_count: int = 300) -> bytes:
    """
    Builds a realistic Riot League-V4 Challenger LeagueListDTO payload with `entry_count` entries.
    Matches real Riot League-V4 behavior where `gameName` and `tagLine` are omitted/null on league entries,
    and entries arrive in unsorted order so ChallengerLeagueService must sort descending by `leaguePoints`.
    """
    entries = []
    # Build in ascending LP order so service sorting (descending LP) is verified
    for rank_idx in range(entry_count, 0, -1):
        lp = 1000 + (entry_count - rank_idx + 1) * 3
        entries.append({
            "puuid": f"puuid-{region}-{rank_idx}",
            "leaguePoints": lp,
            "rank": "I",
            "wins": 180 + (rank_idx % 70),
            "losses": 140 + (rank_idx % 50),
            "veteran": (rank_idx % 5 == 0),
            "inactive": False,
            "freshBlood": (rank_idx % 11 == 0),
            "hotStreak": (rank_idx % 3 == 0),
        })
    dto = {
        "tier": "CHALLENGER",
        "leagueId": f"challenger-league-{region}",
        "queue": "RANKED_SOLO_5x5",
        "name": f"Challenger {region.upper()}",
        "entries": entries,
    }
    return json.dumps(dto, separators=(",", ":")).encode("utf-8")


CHALLENGER_300_BYTES_BY_REGION = {
    "euw1": build_raw_challenger_league_json("euw1", 300),
    "kr": build_raw_challenger_league_json("kr", 300),
    "na1": build_raw_challenger_league_json("na1", 300),
}
OVERSIZED_CHALLENGER_640K_BYTES = build_raw_challenger_league_json("na1", 4500)

MOCK_LOCK = threading.Lock()
# Modes per region: "OK_300", "CHUNKED_OVERSIZE_640K", "STALL_6_5S", "HTTP_503", "HTTP_429"
REGION_BEHAVIOR = {
    "euw1": {"mode": "OK_300", "delaySec": 0.0},
    "kr": {"mode": "OK_300", "delaySec": 0.0},
    "na1": {"mode": "CHUNKED_OVERSIZE_640K", "delaySec": 0.0},
}

RIOT_CALL_COUNTS = {
    "challengerByRegion": {"euw1": 0, "kr": 0, "na1": 0},
    "accountByPuuid": 0,
    "accountByRiotId": 0,
    "summonerByPuuid": 0,
    "otherCalls": 0,
}


def reset_riot_call_counts():
    with MOCK_LOCK:
        for r in ("euw1", "kr", "na1"):
            RIOT_CALL_COUNTS["challengerByRegion"][r] = 0
        RIOT_CALL_COUNTS["accountByPuuid"] = 0
        RIOT_CALL_COUNTS["accountByRiotId"] = 0
        RIOT_CALL_COUNTS["summonerByPuuid"] = 0
        RIOT_CALL_COUNTS["otherCalls"] = 0


def snapshot_riot_call_counts() -> dict:
    with MOCK_LOCK:
        return {
            "challengerByRegion": dict(RIOT_CALL_COUNTS["challengerByRegion"]),
            "accountByPuuid": RIOT_CALL_COUNTS["accountByPuuid"],
            "accountByRiotId": RIOT_CALL_COUNTS["accountByRiotId"],
            "summonerByPuuid": RIOT_CALL_COUNTS["summonerByPuuid"],
            "otherCalls": RIOT_CALL_COUNTS["otherCalls"],
        }


class MockRiotHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def _send_riot_json(self, body: bytes, status: int = 200, method_limit: str = "500:10,30000:600"):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-App-Rate-Limit", "20:1,100:120")
        self.send_header("X-App-Rate-Limit-Count", "1:1,1:120")
        self.send_header("X-Method-Rate-Limit", method_limit)
        self.send_header("X-Method-Rate-Limit-Count", "1:10,1:600")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _send_chunked_oversized(self, chunk_size: int = 16_384):
        """
        Streams a valid >640 KiB JSON payload using HTTP/1.1 Transfer-Encoding: chunked without Content-Length,
        exercising RiotApiService.BoundedLeagueInputStream mid-stream abort (> 524,288 bytes -> RESPONSE_TOO_LARGE)
        inside the 512 MiB container.
        """
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("X-App-Rate-Limit", "20:1,100:120")
        self.send_header("X-App-Rate-Limit-Count", "1:1,1:120")
        self.send_header("X-Method-Rate-Limit", "500:10,30000:600")
        self.send_header("X-Method-Rate-Limit-Count", "1:10,1:600")
        self.end_headers()

        payload = OVERSIZED_CHALLENGER_640K_BYTES
        offset = 0
        try:
            while offset < len(payload):
                chunk = payload[offset:offset + chunk_size]
                self.wfile.write(f"{len(chunk):x}\r\n".encode("ascii") + chunk + b"\r\n")
                self.wfile.flush()
                offset += len(chunk)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError, OSError):
            # Expected when the client aborts the connection after crossing 524,288 bytes
            pass

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        host_hdr = (self.headers.get("Host") or "").split(":", 1)[0].lower()
        region_prefix = host_hdr.split(".", 1)[0] if "." in host_hdr else "euw1"

        if path.endswith("/versions.json"):
            body = json.dumps(["16.7.1", "16.6.1"]).encode("utf-8")
            self._send_riot_json(body)
        elif "/lol/league/v4/challengerleagues/by-queue/" in path:
            with MOCK_LOCK:
                if region_prefix in RIOT_CALL_COUNTS["challengerByRegion"]:
                    RIOT_CALL_COUNTS["challengerByRegion"][region_prefix] += 1
                cfg = dict(REGION_BEHAVIOR.get(region_prefix, {"mode": "OK_300", "delaySec": 0.0}))
            delay_sec = float(cfg.get("delaySec", 0.0))
            mode = cfg.get("mode", "OK_300")
            if delay_sec > 0.0:
                time.sleep(delay_sec)
            if mode == "OK_300":
                payload = CHALLENGER_300_BYTES_BY_REGION.get(region_prefix, CHALLENGER_300_BYTES_BY_REGION["euw1"])
                self._send_riot_json(payload, 200)
            elif mode == "CHUNKED_OVERSIZE_640K":
                self._send_chunked_oversized(16_384)
            elif mode == "STALL_6_5S":
                try:
                    time.sleep(6.5)
                    payload = CHALLENGER_300_BYTES_BY_REGION.get(region_prefix, CHALLENGER_300_BYTES_BY_REGION["euw1"])
                    self._send_riot_json(payload, 200)
                except (BrokenPipeError, ConnectionResetError, ssl.SSLError, OSError):
                    pass
            elif mode == "HTTP_503":
                self._send_riot_json(b'{"status":{"message":"Service unavailable","status_code":503}}', 503)
            elif mode == "HTTP_429":
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "30")
                self.send_header("X-Rate-Limit-Type", "service")
                body = b'{"status":{"message":"Rate limit exceeded","status_code":429}}'
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
            else:
                self._send_riot_json(b'{"status":{"message":"Unexpected mode","status_code":500}}', 500)
        elif "/riot/account/v1/accounts/by-puuid/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["accountByPuuid"] += 1
            self._send_riot_json(b'{"status":{"message":"Forbidden account-v1 fanout","status_code":500}}', 500)
        elif "/riot/account/v1/accounts/by-riot-id/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["accountByRiotId"] += 1
            self._send_riot_json(b'{"status":{"message":"Unexpected accountByRiotId","status_code":404}}', 404)
        elif "/lol/summoner/v4/summoners/by-puuid/" in path:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["summonerByPuuid"] += 1
            self._send_riot_json(b'{"status":{"message":"Unexpected summonerByPuuid","status_code":404}}', 404)
        else:
            with MOCK_LOCK:
                RIOT_CALL_COUNTS["otherCalls"] += 1
            self.send_response(404)
            self.send_header("Content-Length", "0")
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


def http_request_status_and_json(url: str, method: str = "GET", headers: dict = None, timeout: int = 30) -> tuple:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        parsed = None
        if raw.strip():
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"rawBody": raw}
        return e.code, parsed


def http_json(url: str, method: str = "GET", headers: dict = None, timeout: int = 30):
    status, data = http_request_status_and_json(url, method=method, headers=headers, timeout=timeout)
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status} from {url}: {data}")
    return data


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
    for name in ("ss-backend-512m", "ss-redis", "ss-postgres"):
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
        "cgroupFreeBytes": max_bytes - current_bytes,
        "cgroupMinFreeBytesAtPeak": max_bytes - peak_bytes,
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
    if cg["cgroupCurrentBytes"] >= CGROUP_STRICT_GATE_BYTES or cg["cgroupPeakBytes"] >= CGROUP_STRICT_GATE_BYTES:
        raise RuntimeError(f"CGROUP_RAW_BYTE_GATE_BREACHED: {cg}")
    if cg["cgroupFreeBytes"] < MIN_FREE_HEADROOM_BYTES or cg["cgroupMinFreeBytesAtPeak"] < MIN_FREE_HEADROOM_BYTES:
        raise RuntimeError(f"CGROUP_64MIB_FREE_GUARD_BREACHED: {cg}")
    return {"pid": pid, "restartCount": restart_count, "oomKilled": oom_killed, **cg}


SAMPLER_JOIN_TIMEOUT_SEC = 15.0


class SamplerController:
    def __init__(self, inspect_fn, poll_interval_sec: float = 2.0, join_timeout_sec: float = SAMPLER_JOIN_TIMEOUT_SEC):
        self.inspect_fn = inspect_fn
        self.poll_interval_sec = poll_interval_sec
        self.join_timeout_sec = join_timeout_sec
        self.samples = []
        self.errors = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None

    def check_health(self):
        with self.lock:
            if self.errors:
                raise RuntimeError(f"Background cgroup sampler failed: {self.errors[0]}") from self.errors[0]

    def record_sample(self, tag: str, check_prior_error: bool = True) -> dict:
        if check_prior_error:
            self.check_health()
        cg = self.inspect_fn()
        with self.lock:
            entry = {"idx": len(self.samples) + 1, "tag": tag, "timestampEpochMs": int(time.time() * 1000), **cg}
            self.samples.append(entry)
            return entry

    def _loop(self):
        while not self.stop_event.wait(self.poll_interval_sec):
            try:
                self.record_sample("poll_2s", check_prior_error=False)
            except Exception as e:
                print(f"SAMPLER ERROR: {e}", flush=True)
                with self.lock:
                    self.errors.append(e)
                self.stop_event.set()
                return

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop_and_join(self, check_errors: bool = True):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=self.join_timeout_sec)
            if self.thread.is_alive():
                raise RuntimeError(
                    f"SAMPLER_THREAD_JOIN_TIMEOUT: sampler thread still alive after {self.join_timeout_sec}s"
                )
        if check_errors:
            self.check_health()

    def snapshot_samples(self) -> list:
        with self.lock:
            return list(self.samples)


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
        "-e", "COHORT_NETWORK_COLLECTION_ENABLED=false",
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
    ], stdout=subprocess.DEVNULL, timeout=20)


def redis_exists(key: str) -> int:
    out = subprocess.check_output(
        ["docker", "exec", "ss-redis", "redis-cli", "EXISTS", key],
        timeout=10
    ).decode("utf-8").strip()
    return int(out)


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


def redis_flushall():
    subprocess.check_call(
        ["docker", "exec", "ss-redis", "redis-cli", "FLUSHALL"],
        stdout=subprocess.DEVNULL,
        timeout=15
    )


def seed_initial_indexed_players():
    """
    Pre-creates `indexed_players` matching IndexedPlayer.kt and seeds:
    - 300 EUW1 Challenger identities (puuid-euw1-1..300 -> EuwChallenger1#EUW .. EuwChallenger300#EUW)
    - 5 KR Challenger identities (puuid-kr-1..5 -> KrChallenger1#KR .. KrChallenger5#KR; 295 KR PUUIDs absent)
    - 300 NA1 Challenger identities (puuid-na1-1..300 -> NaChallenger1#NA1 .. NaChallenger300#NA1)
    """
    psql_exec("""
        CREATE TABLE IF NOT EXISTS indexed_players (
            puuid varchar(255) NOT NULL PRIMARY KEY,
            game_name varchar(255) NOT NULL,
            tag_line varchar(255) NOT NULL,
            region varchar(255) NOT NULL,
            profile_icon_id integer NOT NULL,
            summoner_level bigint NOT NULL,
            last_seen_at timestamp(6) with time zone NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_game_name ON indexed_players (game_name);
        CREATE INDEX IF NOT EXISTS idx_region ON indexed_players (region);
        TRUNCATE TABLE indexed_players;
        INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
        SELECT
            'puuid-euw1-' || gs,
            'EuwChallenger' || gs,
            'EUW',
            'euw1',
            4000 + (gs % 500),
            300 + gs,
            NOW()
        FROM generate_series(1, 300) AS gs;

        INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
        SELECT
            'puuid-kr-' || gs,
            'KrChallenger' || gs,
            'KR',
            'kr',
            4500 + gs,
            400 + gs,
            NOW()
        FROM generate_series(1, 5) AS gs;

        INSERT INTO indexed_players (puuid, game_name, tag_line, region, profile_icon_id, summoner_level, last_seen_at)
        SELECT
            'puuid-na1-' || gs,
            'NaChallenger' || gs,
            'NA1',
            'na1',
            5000 + (gs % 500),
            350 + gs,
            NOW()
        FROM generate_series(1, 300) AS gs;
    """)


def verify_leaderboard_response(dto: dict, region: str, expected_hydrated_count: int, expected_top1_name: str) -> dict:
    assert isinstance(dto, dict), f"[{region}] Expected dict, got {type(dto)}"
    entries = dto.get("entries")
    assert isinstance(entries, list), f"[{region}] Expected entries list, got {type(entries)}"
    assert len(entries) == 300, f"[{region}] Expected exactly 300 entries, got {len(entries)}"
    lps = [e.get("leaguePoints") for e in entries]
    assert lps == sorted(lps, reverse=True), f"[{region}] Entries are not sorted descending by leaguePoints"
    assert entries[0]["puuid"] == f"puuid-{region}-1", f"[{region}] Expected top-1 puuid-{region}-1, got {entries[0]['puuid']}"
    assert entries[-1]["puuid"] == f"puuid-{region}-300", f"[{region}] Expected rank-300 puuid-{region}-300, got {entries[-1]['puuid']}"
    assert entries[0].get("gameName") == expected_top1_name, (
        f"[{region}] Expected top-1 gameName={expected_top1_name}, got {entries[0].get('gameName')}"
    )
    hydrated = sum(
        1 for e in entries
        if bool((e.get("gameName") or "").strip()) and bool((e.get("tagLine") or "").strip())
    )
    missing = sum(
        1 for e in entries
        if e.get("gameName") is None and e.get("tagLine") is None
    )
    assert hydrated == expected_hydrated_count, (
        f"[{region}] Expected {expected_hydrated_count}/300 hydrated identities, got {hydrated}/300"
    )
    assert hydrated + missing == 300, (
        f"[{region}] Expected hydrated({hydrated}) + missing({missing}) == 300"
    )
    return {
        "entryCount": len(entries),
        "hydratedIdentities": hydrated,
        "missingIdentities": missing,
        "top1Puuid": entries[0]["puuid"],
        "top1GameName": entries[0].get("gameName"),
        "top1TagLine": entries[0].get("tagLine"),
        "top1LeaguePoints": entries[0].get("leaguePoints"),
        "rank300Puuid": entries[-1]["puuid"],
        "rank300LeaguePoints": entries[-1].get("leaguePoints"),
    }


def capture_sanitized_container_log(artifact_dir: str, container_name: str, log_filename: str):
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/challenger-leaderboard-512m-proof-raw")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(artifact_dir, exist_ok=True)
    proc = subprocess.run(
        ["docker", "logs", container_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
        check=False
    )
    raw_log = proc.stdout.decode("utf-8", errors="replace")
    with open(os.path.join(raw_dir, log_filename), "w", encoding="utf-8") as f:
        f.write(raw_log)
    sanitized_log = sanitize_text(raw_log)
    with open(os.path.join(artifact_dir, log_filename), "w", encoding="utf-8") as f:
        f.write(sanitized_log)


def write_sanitized_artifacts(artifact_dir: str, summary: dict, cgroup_samples: list):
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/challenger-leaderboard-512m-proof-raw")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(artifact_dir, exist_ok=True)
    with open(os.path.join(raw_dir, "acceptance_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(os.path.join(raw_dir, "cgroup_samples_2s.json"), "w", encoding="utf-8") as f:
        json.dump(cgroup_samples, f, indent=2)

    summary_path = os.path.join(artifact_dir, "acceptance_summary.json")
    samples_path = os.path.join(artifact_dir, "cgroup_samples_2s.json")
    sanitized_summary = sanitize_json_value(summary)
    sanitized_samples = sanitize_json_value(cgroup_samples)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(sanitized_summary, f, indent=2)
    with open(samples_path, "w", encoding="utf-8") as f:
        json.dump(sanitized_samples, f, indent=2)

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
                if fname.endswith(".json"):
                    try:
                        json.loads(content)
                    except Exception as e:
                        shutil.rmtree(artifact_dir, ignore_errors=True)
                        raise RuntimeError(f"INVALID_JSON_IN_ARTIFACT: {fpath}: {e}")


def finalize_teardown(
    sampler: SamplerController,
    proof_completed_ok: bool,
    summary: dict,
    artifact_dir: str,
    capture_log_fn,
    write_artifacts_fn,
    teardown_network_fn,
):
    teardown_err = None
    try:
        sampler.stop_and_join(check_errors=not proof_completed_ok)
    except Exception as e:
        proof_completed_ok = False
        teardown_err = e

    final_samples_snapshot = sampler.snapshot_samples()
    if not proof_completed_ok:
        summary["status"] = "FAILED"
        if final_samples_snapshot:
            summary["candidateMemory512mPartial"] = {
                "sampleCount": len(final_samples_snapshot),
                "strictRawByteCeiling": CGROUP_STRICT_GATE_BYTES,
                "maxCurrentBytes": max(s["cgroupCurrentBytes"] for s in final_samples_snapshot),
                "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in final_samples_snapshot),
                "maxCurrentRatioPct": max(s["cgroupRatioPct"] for s in final_samples_snapshot),
                "maxPeakBytes": max(s["cgroupPeakBytes"] for s in final_samples_snapshot),
                "maxPeakMiB": max(s["cgroupPeakMiB"] for s in final_samples_snapshot),
                "maxPeakRatioPct": max(s["cgroupPeakRatioPct"] for s in final_samples_snapshot),
                "oomKilled": any(s.get("oomKilled") for s in final_samples_snapshot),
                "restartCount": max(s.get("restartCount", 0) for s in final_samples_snapshot),
            }
            print("PARTIAL CANDIDATE 512M MEMORY AT FAILURE:", json.dumps(summary["candidateMemory512mPartial"]), flush=True)
        if teardown_err is not None and "SAMPLER_THREAD_JOIN_TIMEOUT" in str(teardown_err):
            shutil.rmtree(artifact_dir, ignore_errors=True)
        else:
            try:
                capture_log_fn(artifact_dir, "ss-backend-512m", "failure_ss_backend_512m.log")
                write_artifacts_fn(artifact_dir, summary, final_samples_snapshot)
            except Exception as e:
                shutil.rmtree(artifact_dir, ignore_errors=True)
                if teardown_err is None:
                    teardown_err = e
    teardown_network_fn()
    if teardown_err is not None and sys.exc_info()[0] is None:
        raise teardown_err


def main():
    db_pass = os.environ.get("DB_PASS", "").strip() or EPHEMERAL_DB_PASS
    cand_image = os.environ.get("CAND_IMAGE", "ss-backend:cand-5959a70")
    base_image = os.environ.get("BASE_IMAGE", "ss-backend:base-139a7de")
    raw_dir = os.environ.get("RAW_ARTIFACT_DIR", "/tmp/challenger-leaderboard-512m-proof-raw")
    artifact_dir = os.environ.get("ARTIFACT_DIR", "/tmp/challenger-leaderboard-512m-proof-artifacts")
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

    cand_pid_holder = [None]
    sampler = SamplerController(
        inspect_fn=lambda: inspect_container_liveness("ss-backend-512m", expected_pid=cand_pid_holder[0]),
        poll_interval_sec=2.0,
        join_timeout_sec=SAMPLER_JOIN_TIMEOUT_SEC,
    )
    proof_completed_ok = False
    summary = {
        "status": "IN_PROGRESS",
        "candidateCommitSha": CANDIDATE_COMMIT_SHA,
        "rollbackBaseCommitSha": ROLLBACK_BASE_COMMIT_SHA,
    }
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

        print("=== 2. Seeding Postgres indexed_players (300 EUW1, 5 KR, 300 NA1) before container boot ===", flush=True)
        seed_initial_indexed_players()
        db_counts = psql_query_json("SELECT region, COUNT(*)::int AS cnt FROM indexed_players GROUP BY region ORDER BY region")
        print("SEEDED INDEXED_PLAYERS:", json.dumps(db_counts), flush=True)

        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["euw1"] = {"mode": "OK_300", "delaySec": 0.0}
            REGION_BEHAVIOR["kr"] = {"mode": "OK_300", "delaySec": 0.0}
            REGION_BEHAVIOR["na1"] = {"mode": "CHUNKED_OVERSIZE_640K", "delaySec": 0.0}

        print(f"=== 3. Booting Candidate 512 MiB Container ({cand_image}, SHA {CANDIDATE_COMMIT_SHA}) ===", flush=True)
        cand_pid = start_backend_container(cand_image, db_pass)
        boot_started_mono = time.monotonic()
        cand_pid_holder[0] = cand_pid
        egress_proof = verify_egress_deny_from_container_netns("ss-backend-512m", cand_pid)
        print("EGRESS DENY PROOF:", json.dumps(egress_proof), flush=True)

        record_sample = sampler.record_sample
        sampler.start()
        record_sample("post_boot_after_startup_warmer")

        # =========================================================================
        # SCENARIO 1: Startup 300-Entry Hydration (EUW 300/300, KR 5/300) + Cold NA1 >512 KiB Abort -> HTTP 503
        # =========================================================================
        print("=== 4. Scenario 1: Verify EUW (300/300 DB identities), KR (5/300 DB identities), and Cold NA1 (>512 KiB Chunked Abort -> HTTP 503) ===", flush=True)
        startup_calls = snapshot_riot_call_counts()
        assert startup_calls["challengerByRegion"]["euw1"] == 1, f"Expected 1 startup League-V4 call for euw1, got {startup_calls}"
        assert startup_calls["challengerByRegion"]["kr"] == 1, f"Expected 1 startup League-V4 call for kr, got {startup_calls}"
        assert startup_calls["challengerByRegion"]["na1"] == 1, f"Expected 1 startup League-V4 call for na1, got {startup_calls}"
        assert startup_calls["accountByPuuid"] == 0, f"Expected 0 accountByPuuid calls, got {startup_calls}"

        euw_status, euw_dto = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/euw1")
        assert euw_status == 200, f"Expected HTTP 200 for euw1, got {euw_status}"
        euw_check = verify_leaderboard_response(euw_dto, "euw1", expected_hydrated_count=300, expected_top1_name="EuwChallenger1")
        euw_redis_bytes = redis_strlen("leaderboard:euw1::RANKED_SOLO_5x5")
        assert euw_redis_bytes > 1000

        kr_status, kr_dto = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/kr")
        assert kr_status == 200, f"Expected HTTP 200 for kr, got {kr_status}"
        kr_check = verify_leaderboard_response(kr_dto, "kr", expected_hydrated_count=5, expected_top1_name="KrChallenger1")
        kr_redis_bytes = redis_strlen("leaderboard:kr::RANKED_SOLO_5x5")
        assert kr_redis_bytes > 1000

        # Cold NA1 had its >512 KiB (640 KiB) chunked stream aborted during startup warmer and has no last-known-good snapshot
        na1_cold_status_1, _ = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/na1")
        na1_cold_status_2, _ = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/na1")
        calls_after_na1_503 = snapshot_riot_call_counts()
        assert na1_cold_status_1 == 503 and na1_cold_status_2 == 503, (
            f"Expected HTTP 503 on cold na1 after >512 KiB stream abort, got {na1_cold_status_1}, {na1_cold_status_2}"
        )
        assert redis_exists("leaderboard:na1::RANKED_SOLO_5x5") == 0, "Cold failed na1 must not cache empty key in Redis"
        assert calls_after_na1_503["challengerByRegion"]["na1"] == 1, (
            f"Expected 30s failure backoff to suppress additional na1 upstream calls, got {calls_after_na1_503}"
        )
        record_sample("after_scenario1_euw_kr_na1_initial")

        # =========================================================================
        # SCENARIO 2: Singleflight Concurrency Coalescing (16 Concurrent Callers on EUW1 Cache Miss)
        # =========================================================================
        print("=== 5. Scenario 2: Singleflight Concurrency Coalescing (16 Concurrent Callers on EUW1 Cache Miss -> 1 Upstream Call) ===", flush=True)
        redis_del("leaderboard:euw1::RANKED_SOLO_5x5")
        assert redis_exists("leaderboard:euw1::RANKED_SOLO_5x5") == 0
        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["euw1"] = {"mode": "OK_300", "delaySec": 0.20}

        conc_count = 16
        conc_statuses = [None] * conc_count
        conc_entry_counts = [None] * conc_count
        conc_hydrated_counts = [None] * conc_count
        conc_errors = []
        start_barrier = threading.Barrier(conc_count)

        def run_concurrent_euw_caller(idx: int):
            try:
                start_barrier.wait(timeout=10.0)
                st, body = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/euw1", timeout=15)
                conc_statuses[idx] = st
                if isinstance(body, dict) and isinstance(body.get("entries"), list):
                    conc_entry_counts[idx] = len(body["entries"])
                    conc_hydrated_counts[idx] = sum(
                        1 for e in body["entries"]
                        if bool((e.get("gameName") or "").strip()) and bool((e.get("tagLine") or "").strip())
                    )
            except Exception as e:
                conc_errors.append(str(e))

        threads = [threading.Thread(target=run_concurrent_euw_caller, args=(i,)) for i in range(conc_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20.0)

        with MOCK_LOCK:
            REGION_BEHAVIOR["euw1"] = {"mode": "OK_300", "delaySec": 0.0}

        calls_conc = snapshot_riot_call_counts()
        assert not conc_errors, f"Concurrent callers raised errors: {conc_errors}"
        assert conc_statuses == [200] * conc_count, f"Expected all 16 callers HTTP 200, got {conc_statuses}"
        assert conc_entry_counts == [300] * conc_count, f"Expected all 16 callers 300 entries, got {conc_entry_counts}"
        assert conc_hydrated_counts == [300] * conc_count, f"Expected all 16 callers 300 hydrated, got {conc_hydrated_counts}"
        assert calls_conc["challengerByRegion"]["euw1"] == 1, (
            f"Singleflight violation: expected exactly 1 upstream League-V4 call for 16 concurrent callers, got {calls_conc}"
        )
        assert calls_conc["accountByPuuid"] == 0
        record_sample("after_scenario2_singleflight_16_callers")

        # =========================================================================
        # SCENARIO 3: Real PostgreSQL ACCESS EXCLUSIVE Table Lock -> Bounded 1.5s JDBC Timeout & Off-Thread Statement.cancel()
        # =========================================================================
        print("=== 6. Scenario 3: Real PostgreSQL ACCESS EXCLUSIVE Lock on indexed_players -> Bounded 1.5s JDBC Timeout & Statement.cancel() ===", flush=True)
        # Update top-1 EUW player name in Postgres right before acquiring ACCESS EXCLUSIVE lock:
        # While locked, the 1.5s JDBC timeout must cancel the query and fall back to previousSnapshot ("EuwChallenger1").
        # Once unlocked, the next refresh must read the updated DB value ("EuwPostLockUpdated1").
        psql_exec("UPDATE indexed_players SET game_name = 'EuwPostLockUpdated1' WHERE puuid = 'puuid-euw1-1';")

        lock_proc = subprocess.Popen([
            "docker", "exec", "ss-postgres",
            "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "summoner_searcher",
            "-c", "BEGIN; LOCK TABLE indexed_players IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(6.5); COMMIT;"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        try:
            for _ in range(40):
                locks = psql_query_json("""
                    SELECT mode, granted
                    FROM pg_locks
                    WHERE relation = 'indexed_players'::regclass
                      AND mode = 'AccessExclusiveLock'
                      AND granted = true
                """)
                if locks:
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError("Failed to observe granted AccessExclusiveLock on indexed_players")

            redis_del("leaderboard:euw1::RANKED_SOLO_5x5")
            reset_riot_call_counts()
            t_lock_req_start = time.monotonic()
            locked_status, locked_dto = http_request_status_and_json(
                f"{BASE_URL}/api/riot/leaderboards/challenger/euw1",
                timeout=15
            )
            locked_latency_ms = round((time.monotonic() - t_lock_req_start) * 1000.0, 2)
            calls_during_db_lock = snapshot_riot_call_counts()
            assert locked_status == 200, f"Expected HTTP 200 during Postgres table lock, got {locked_status}"
            assert 1200.0 <= locked_latency_ms <= 4200.0, (
                f"Expected JDBC timeout (~1500ms) to bound refresh latency well below 5000ms deadline, got {locked_latency_ms}ms"
            )
            # Because the DB query was canceled by the 1.5s JDBC timeout, top-1 entry preserves previousSnapshot identity ("EuwChallenger1")
            locked_check = verify_leaderboard_response(
                locked_dto, "euw1", expected_hydrated_count=300, expected_top1_name="EuwChallenger1"
            )
            assert calls_during_db_lock["challengerByRegion"]["euw1"] == 1
        finally:
            lock_proc.wait(timeout=15)

        # Now that the Postgres table lock is released, evict Redis and verify DB lookup succeeds immediately
        redis_del("leaderboard:euw1::RANKED_SOLO_5x5")
        t_unlock_start = time.monotonic()
        unlocked_status, unlocked_dto = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/euw1",
            timeout=15
        )
        unlocked_latency_ms = round((time.monotonic() - t_unlock_start) * 1000.0, 2)
        assert unlocked_status == 200
        assert unlocked_latency_ms < 1200.0, f"Expected fast unlocked DB lookup (<1200ms), got {unlocked_latency_ms}ms"
        unlocked_check = verify_leaderboard_response(
            unlocked_dto, "euw1", expected_hydrated_count=300, expected_top1_name="EuwPostLockUpdated1"
        )
        record_sample("after_scenario3_postgres_lock_jdbc_cancel_and_recovery")

        # =========================================================================
        # SCENARIO 4: Upstream Pre-Header / Response Stall (> 5s) Deadline Abort on EUW1
        # =========================================================================
        print("=== 7. Scenario 4: Upstream Pre-Header Stall (6.5s > 5.0s Deadline) Abort & Last-Known-Good Recovery on EUW1 ===", flush=True)
        redis_del("leaderboard:euw1::RANKED_SOLO_5x5")
        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["euw1"] = {"mode": "STALL_6_5S", "delaySec": 0.0}

        t_stall_start = time.monotonic()
        stall_status, stall_dto = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/euw1",
            timeout=15
        )
        stall_latency_ms = round((time.monotonic() - t_stall_start) * 1000.0, 2)
        with MOCK_LOCK:
            REGION_BEHAVIOR["euw1"] = {"mode": "OK_300", "delaySec": 0.0}

        calls_stall = snapshot_riot_call_counts()
        assert stall_status == 200, f"Expected HTTP 200 fallback on EUW1 stall deadline abort, got {stall_status}"
        assert 4500.0 <= stall_latency_ms <= 6000.0, (
            f"Expected 5000ms deadline abort to return around ~5.0s (before 6.5s upstream stall finishes), got {stall_latency_ms}ms"
        )
        stall_check = verify_leaderboard_response(
            stall_dto, "euw1", expected_hydrated_count=300, expected_top1_name="EuwPostLockUpdated1"
        )
        assert redis_exists("leaderboard:euw1::RANKED_SOLO_5x5") == 1, "Expected EUW1 Redis cache reseeded from last-known-good snapshot"
        assert calls_stall["challengerByRegion"]["euw1"] == 1
        record_sample("after_scenario4_deadline_abort_euw1")

        # =========================================================================
        # SCENARIO 5: Full Redis FLUSHALL + Upstream HTTP 503 Outage + Last-Known-Good Fallback + 30s Backoff on KR
        # =========================================================================
        print("=== 8. Scenario 5: Full Redis FLUSHALL + Upstream HTTP 503 Outage on KR -> Complete 300-Entry Fallback + 30s Backoff ===", flush=True)
        redis_flushall()
        assert redis_exists("leaderboard:euw1::RANKED_SOLO_5x5") == 0
        assert redis_exists("leaderboard:kr::RANKED_SOLO_5x5") == 0
        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["kr"] = {"mode": "HTTP_503", "delaySec": 0.0}

        kr_outage_status_1, kr_outage_dto_1 = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/kr",
            timeout=15
        )
        calls_kr_outage_1 = snapshot_riot_call_counts()
        assert kr_outage_status_1 == 200, f"Expected HTTP 200 from KR last-known-good snapshot after FLUSHALL, got {kr_outage_status_1}"
        kr_outage_check = verify_leaderboard_response(
            kr_outage_dto_1, "kr", expected_hydrated_count=5, expected_top1_name="KrChallenger1"
        )
        assert redis_exists("leaderboard:kr::RANKED_SOLO_5x5") == 1, "Expected KR Redis key reseeded from last-known-good snapshot"
        assert calls_kr_outage_1["challengerByRegion"]["kr"] == 1

        # Evict KR Redis key again during the 30s failure backoff window -> must serve fallback with 0 new upstream calls
        redis_del("leaderboard:kr::RANKED_SOLO_5x5")
        kr_outage_status_2, kr_outage_dto_2 = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/kr",
            timeout=15
        )
        calls_kr_outage_2 = snapshot_riot_call_counts()
        assert kr_outage_status_2 == 200
        assert len(kr_outage_dto_2["entries"]) == 300
        assert calls_kr_outage_2["challengerByRegion"]["kr"] == 1, (
            f"Expected 0 additional KR upstream calls during 30s failure backoff, got {calls_kr_outage_2}"
        )

        # Also reseed EUW1 (which is currently in its 30s backoff from Scenario 4) by reading it -> serves 300 entries with 0 upstream calls
        euw_backoff_status, euw_backoff_dto = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/euw1",
            timeout=15
        )
        assert euw_backoff_status == 200 and len(euw_backoff_dto["entries"]) == 300
        assert redis_exists("leaderboard:euw1::RANKED_SOLO_5x5") == 1
        record_sample("after_scenario5_flushall_and_backoff_recovery")

        # =========================================================================
        # SCENARIO 6: Cold NA1 Self-Heal After 30s Backoff + Warmed NA1 >512 KiB Chunked Body Abort + 3-Region Memory Guard
        # =========================================================================
        print("=== 9. Scenario 6: Cold NA1 Recovery After 30s Backoff Expiry + Warmed NA1 >512 KiB Chunked Body Abort ===", flush=True)
        elapsed_since_boot = time.monotonic() - boot_started_mono
        remaining_na1_backoff = max(0.0, 31.5 - elapsed_since_boot)
        if remaining_na1_backoff > 0.0:
            print(f"Waiting {remaining_na1_backoff:.1f}s for NA1 initial 30s failure backoff window to expire...", flush=True)
            time.sleep(remaining_na1_backoff)

        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["na1"] = {"mode": "OK_300", "delaySec": 0.0}

        na1_heal_status, na1_heal_dto = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/na1",
            timeout=15
        )
        calls_na1_heal = snapshot_riot_call_counts()
        assert na1_heal_status == 200, f"Expected NA1 to self-heal to HTTP 200 after backoff expiry, got {na1_heal_status}"
        na1_heal_check = verify_leaderboard_response(
            na1_heal_dto, "na1", expected_hydrated_count=300, expected_top1_name="NaChallenger1"
        )
        assert calls_na1_heal["challengerByRegion"]["na1"] == 1
        assert calls_na1_heal["accountByPuuid"] == 0
        na1_redis_bytes = redis_strlen("leaderboard:na1::RANKED_SOLO_5x5")
        assert na1_redis_bytes > 1000

        # Now test >512 KiB chunked stream abort on warmed NA1 -> aborts stream at 512 KiB and serves 300-entry fallback
        redis_del("leaderboard:na1::RANKED_SOLO_5x5")
        reset_riot_call_counts()
        with MOCK_LOCK:
            REGION_BEHAVIOR["na1"] = {"mode": "CHUNKED_OVERSIZE_640K", "delaySec": 0.0}

        na1_oversize_status, na1_oversize_dto = http_request_status_and_json(
            f"{BASE_URL}/api/riot/leaderboards/challenger/na1",
            timeout=15
        )
        calls_na1_oversize = snapshot_riot_call_counts()
        assert na1_oversize_status == 200, f"Expected HTTP 200 fallback on warmed NA1 >512 KiB chunked abort, got {na1_oversize_status}"
        na1_oversize_check = verify_leaderboard_response(
            na1_oversize_dto, "na1", expected_hydrated_count=300, expected_top1_name="NaChallenger1"
        )
        assert calls_na1_oversize["challengerByRegion"]["na1"] == 1
        assert redis_exists("leaderboard:na1::RANKED_SOLO_5x5") == 1

        # Verify application-level memoryHeadroom endpoint with all 3 regions resident in memory & Redis
        collector_status = http_json(
            f"{BASE_URL}/api/riot/cohorts/euw1/collector/status?patch=16.7",
            headers=OP_HEADERS,
            timeout=10
        )
        app_mem_headroom = collector_status.get("memoryHeadroom") or {}
        assert app_mem_headroom.get("usedRatioPct", 100.0) < 78.0, f"App memoryHeadroom ratio >= 78%: {app_mem_headroom}"
        assert app_mem_headroom.get("freeBytes", 0) >= MIN_FREE_HEADROOM_BYTES, f"App memoryHeadroom freeBytes < 64 MiB: {app_mem_headroom}"

        record_sample("after_scenario6_all_3_regions_resident_and_chunked_abort")

        # Stop sampler cleanly before swapping to rollback base container
        sampler.stop_and_join(check_errors=True)
        capture_sanitized_container_log(artifact_dir, "ss-backend-512m", "candidate_5959a70_container.log")

        # Verify container log contains the expected JDBC timeout/cancellation and >512 KiB stream abort evidence
        cand_log_text = open(os.path.join(artifact_dir, "candidate_5959a70_container.log"), "r", encoding="utf-8").read()
        assert "RESPONSE_TOO_LARGE" in cand_log_text, "Expected >512 KiB stream abort (RESPONSE_TOO_LARGE) log entry in container log"
        assert (
            "Local DB identity lookup timed out after 1500ms" in cand_log_text
            or "canceling statement due to user request" in cand_log_text
        ), "Expected bounded JDBC timeout / Statement.cancel() log entry in container log"

        # =========================================================================
        # SCENARIO 7: Rollback Base (139a7de48e67af9d664c08fd61ce131b6e63bd74) Read Compatibility
        # =========================================================================
        print(f"=== 10. Scenario 7: Booting Rollback Base ({base_image}, SHA {ROLLBACK_BASE_COMMIT_SHA}) on Same Live Postgres & Redis ===", flush=True)
        rb_pid = start_backend_container(base_image, db_pass)
        rb_egress = verify_egress_deny_from_container_netns("ss-backend-512m", rb_pid)

        rb_euw_status, rb_euw_dto = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/euw1")
        rb_kr_status, rb_kr_dto = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/kr")
        rb_na1_status, rb_na1_dto = http_request_status_and_json(f"{BASE_URL}/api/riot/leaderboards/challenger/na1")
        rb_autocomplete = http_json(f"{BASE_URL}/api/search/autocomplete?query=EuwPostLock&region=euw1")
        rb_cg = inspect_container_liveness("ss-backend-512m", expected_pid=rb_pid)
        capture_sanitized_container_log(artifact_dir, "ss-backend-512m", "rollback_139a7de_container.log")

        assert rb_euw_status == 200 and len(rb_euw_dto["entries"]) == 300
        assert rb_kr_status == 200 and len(rb_kr_dto["entries"]) == 300
        assert rb_na1_status == 200 and len(rb_na1_dto["entries"]) == 300
        assert any(p.get("gameName") == "EuwPostLockUpdated1" for p in rb_autocomplete), (
            f"Expected EuwPostLockUpdated1 in rollback autocomplete: {rb_autocomplete}"
        )

        cand_samples_snapshot = sampler.snapshot_samples()
        cand_max_current_bytes = max(s["cgroupCurrentBytes"] for s in cand_samples_snapshot)
        cand_max_current_pct = max(s["cgroupRatioPct"] for s in cand_samples_snapshot)
        cand_max_peak_bytes = max(s["cgroupPeakBytes"] for s in cand_samples_snapshot)
        cand_max_peak_pct = max(s["cgroupPeakRatioPct"] for s in cand_samples_snapshot)
        cand_min_free_at_peak = min(s["cgroupMinFreeBytesAtPeak"] for s in cand_samples_snapshot)

        assert cand_max_current_bytes < CGROUP_STRICT_GATE_BYTES
        assert cand_max_peak_bytes < CGROUP_STRICT_GATE_BYTES
        assert cand_min_free_at_peak >= MIN_FREE_HEADROOM_BYTES

        summary.update({
            "status": "PASSED",
            "candidateCommitSha": CANDIDATE_COMMIT_SHA,
            "rollbackBaseCommitSha": ROLLBACK_BASE_COMMIT_SHA,
            "egressIsolation": egress_proof,
            "rollbackEgressIsolation": rb_egress,
            "scenario1_hydrationAndColdOversizeAbort": {
                "euw1": {**euw_check, "redisCompressedBytes": euw_redis_bytes},
                "kr": {**kr_check, "redisCompressedBytes": kr_redis_bytes},
                "na1ColdOversizeChunkedAbort": {
                    "firstCallerHttpStatus": na1_cold_status_1,
                    "secondCallerBackoffHttpStatus": na1_cold_status_2,
                    "redisKeyCachedOnFailure": False,
                    "upstreamCallsDuringBackoff": calls_after_na1_503["challengerByRegion"]["na1"],
                },
                "accountV1ByPuuidCalls": startup_calls["accountByPuuid"],
            },
            "scenario2_singleflightConcurrency": {
                "concurrentCallers": conc_count,
                "callerHttpStatuses": conc_statuses,
                "callerEntryCounts": conc_entry_counts,
                "callerHydratedCounts": conc_hydrated_counts,
                "upstreamLeagueV4Calls": calls_conc["challengerByRegion"]["euw1"],
                "accountV1ByPuuidCalls": calls_conc["accountByPuuid"],
            },
            "scenario3_postgresLockJdbcCancelAndRecovery": {
                "postgresLockMode": "ACCESS EXCLUSIVE",
                "lockedRefreshHttpStatus": locked_status,
                "lockedRefreshLatencyMs": locked_latency_ms,
                "lockedTop1GameNamePreservedFromSnapshot": locked_check["top1GameName"],
                "unlockedRefreshHttpStatus": unlocked_status,
                "unlockedRefreshLatencyMs": unlocked_latency_ms,
                "unlockedTop1GameNameFromDb": unlocked_check["top1GameName"],
            },
            "scenario4_upstreamDeadlineStallAbort": {
                "upstreamStallSeconds": 6.5,
                "configuredRefreshDeadlineMs": 5000,
                "httpStatus": stall_status,
                "latencyMs": stall_latency_ms,
                "returnedEntries": stall_check["entryCount"],
                "redisReseeded": True,
            },
            "scenario5_redisFlushallAndUpstream503Fallback": {
                "krHttpStatusAfterFlushallAnd503": kr_outage_status_1,
                "krReturnedEntries": kr_outage_check["entryCount"],
                "krHydratedIdentities": kr_outage_check["hydratedIdentities"],
                "krMissingIdentities": kr_outage_check["missingIdentities"],
                "krRedisReseeded": True,
                "krSecondEvictionDuringBackoffHttpStatus": kr_outage_status_2,
                "krTotalUpstreamCallsAcrossBothEvictions": calls_kr_outage_2["challengerByRegion"]["kr"],
            },
            "scenario6_na1SelfHealAndWarmChunkedOversizeAbort": {
                "na1SelfHealHttpStatus": na1_heal_status,
                "na1SelfHealEntries": na1_heal_check["entryCount"],
                "na1SelfHealHydratedIdentities": na1_heal_check["hydratedIdentities"],
                "na1RedisCompressedBytes": na1_redis_bytes,
                "na1WarmOversizeChunkedAbortHttpStatus": na1_oversize_status,
                "na1WarmOversizeFallbackEntries": na1_oversize_check["entryCount"],
                "appMemoryHeadroom": app_mem_headroom,
            },
            "scenario7_rollbackBase139a7deCompatibility": {
                "euw1Status": rb_euw_status,
                "euw1Entries": len(rb_euw_dto["entries"]),
                "krStatus": rb_kr_status,
                "krEntries": len(rb_kr_dto["entries"]),
                "na1Status": rb_na1_status,
                "na1Entries": len(rb_na1_dto["entries"]),
                "autocompleteMatchedUpdatedRow": True,
                "rollbackCgroupCurrentBytes": rb_cg["cgroupCurrentBytes"],
                "rollbackCgroupCurrentMiB": rb_cg["cgroupCurrentMiB"],
                "rollbackCgroupRatioPct": rb_cg["cgroupRatioPct"],
                "rollbackCgroupPeakBytes": rb_cg["cgroupPeakBytes"],
                "rollbackCgroupPeakMiB": rb_cg["cgroupPeakMiB"],
                "rollbackCgroupPeakRatioPct": rb_cg["cgroupPeakRatioPct"],
            },
            "candidateMemory512m": {
                "sampleCount": len(cand_samples_snapshot),
                "cgroupLimitBytes": CGROUP_MAX_BYTES,
                "strictRawByteCeiling": CGROUP_STRICT_GATE_BYTES,
                "minFreeGuardBytes": MIN_FREE_HEADROOM_BYTES,
                "maxCurrentBytes": cand_max_current_bytes,
                "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in cand_samples_snapshot),
                "maxCurrentRatioPct": cand_max_current_pct,
                "maxPeakBytes": cand_max_peak_bytes,
                "maxPeakMiB": max(s["cgroupPeakMiB"] for s in cand_samples_snapshot),
                "maxPeakRatioPct": cand_max_peak_pct,
                "minFreeBytesAtPeak": cand_min_free_at_peak,
                "minFreeMiBAtPeak": round(cand_min_free_at_peak / (1024.0 * 1024.0), 2),
                "oomKilled": False,
                "restartCount": 0,
            },
        })

        write_sanitized_artifacts(artifact_dir, summary, cand_samples_snapshot)
        proof_completed_ok = True
        print("=== ALL 512 MiB CHALLENGER LEADERBOARD CONTAINER PROOF CHECKS PASSED ===", flush=True)
        print(json.dumps(summary["candidateMemory512m"], indent=2), flush=True)
    finally:
        finalize_teardown(
            sampler=sampler,
            proof_completed_ok=proof_completed_ok,
            summary=summary,
            artifact_dir=artifact_dir,
            capture_log_fn=capture_sanitized_container_log,
            write_artifacts_fn=write_sanitized_artifacts,
            teardown_network_fn=teardown_isolated_network,
        )


def run_static_smoke_test():
    for region in ("euw1", "kr", "na1"):
        raw = CHALLENGER_300_BYTES_BY_REGION[region]
        parsed = json.loads(raw.decode("utf-8"))
        assert len(parsed["entries"]) == 300
        assert parsed["entries"][0]["puuid"] == f"puuid-{region}-300"
        assert parsed["entries"][-1]["puuid"] == f"puuid-{region}-1"

    with tempfile.TemporaryDirectory() as tmp_root:
        os.environ["RAW_ARTIFACT_DIR"] = os.path.join(tmp_root, "raw")
        upload_dir = os.path.join(tmp_root, "upload")
        sample_summary = {
            "status": "PASSED",
            "candidateCommitSha": CANDIDATE_COMMIT_SHA,
            "secretCheck": f"db_pass={EPHEMERAL_DB_PASS}",
        }
        sample_cg = [{
            "idx": 1,
            "tag": "smoke",
            "cgroupCurrentBytes": 300_000_000,
            "cgroupCurrentMiB": 286.10,
            "cgroupRatioPct": 55.88,
            "cgroupPeakBytes": 320_000_000,
            "cgroupPeakMiB": 305.18,
            "cgroupPeakRatioPct": 59.60,
            "cgroupFreeBytes": 236_870_912,
            "cgroupMinFreeBytesAtPeak": 216_870_912,
            "cgroupMaxBytes": CGROUP_MAX_BYTES,
            "cgroupStrictCeilingBytes": CGROUP_STRICT_GATE_BYTES,
            "oomKilled": False,
            "restartCount": 0,
        }]
        write_sanitized_artifacts(upload_dir, sample_summary, sample_cg)
        persisted = json.load(open(os.path.join(upload_dir, "acceptance_summary.json"), "r", encoding="utf-8"))
        assert "[REDACTED]" in persisted["secretCheck"]
        assert EPHEMERAL_DB_PASS not in json.dumps(persisted)

    print("STATIC_SMOKE_TEST_OK", json.dumps({
        "candidateCommitSha": CANDIDATE_COMMIT_SHA,
        "rollbackBaseCommitSha": ROLLBACK_BASE_COMMIT_SHA,
        "euw300PayloadBytes": len(CHALLENGER_300_BYTES_BY_REGION["euw1"]),
    }))


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        run_static_smoke_test()
    else:
        main()
