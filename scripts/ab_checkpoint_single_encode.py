#!/usr/bin/env python3
"""
Isolated same-fixture A/B harness comparing:
  - Base arm:      176f67f (PR #24 head: session memory series instrumentation)
  - Candidate arm: 92617dd (PR #25 head: single-encode collector checkpoint across patch and latest Redis keys)

Guarantees enforced by this script:
1. Reuses the exact checked-in MockRiotHandler fixture from scripts/verify_512m_container.py.
2. Enforces container egress deny via an internal-only Docker bridge network (ab-isolated-net, 172.28.0.0/16)
   plus explicit iptables DOCKER-USER/INPUT rules, and runs a fail-closed negative-control probe inside the
   container's network namespace (nsenter -n) before any collector work starts.
3. Uses the actual deployed collector routes (/api/riot/cohorts/euw1/collector/{status,probe,start,stop})
   and query parameters (patch, maxBatches, batchSize, pauseDelayMs, maxPauseCycles,
   maxTotalProductiveBatches, maxTotalNetworkCalls, async=true).
4. Starts each session only if pre-session cgroup current memory is strictly below 350.0 MiB (< 350.0 MiB).
5. Starts each session with async=true and runs an independent 2-second host cgroup watcher thread that
   immediately stops the collector and aborts the arm if cgroup current memory exceeds 360.0 MiB (> 360.0 MiB).
6. Verifies top-level Docker RestartCount (info["RestartCount"]) and State.OOMKilled (info["State"]["OOMKilled"])
   plus container PID and StartedAt invariance, failing closed if any field is missing or non-zero.
7. Fails closed if either arm completes zero productive sessions, verifies exact work and Riot call parity
   across paired sessions, and classifies the outcome honestly (FULL_5_SESSION_COMPARABLE, PARTIAL_COMPARABLE,
   INCOMPARABLE_MISMATCHED_WORK_OR_CALLS, or FAILED_ZERO_PRODUCTIVE_SESSIONS).
"""

import json
import os
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_512m_container as fixture

NETWORK_NAME = "ab-isolated-net"
BRIDGE_NAME = "br-ab-isolated"
SUBNET_CIDR = "172.28.0.0/16"
GATEWAY_IP = "172.28.0.1"
POSTGRES_IP = "172.28.0.2"
REDIS_IP = "172.28.0.3"
BACKEND_IP = "172.28.0.4"

BASE_URL = f"http://{BACKEND_IP}:10000"
OP_HEADERS = {"X-Cohort-Operator-Token": "test-op-token"}
REGION = "euw1"
PATCH = "16.7"

MAX_SESSIONS = int(os.environ.get("AB_MAX_SESSIONS", "5"))
MAX_BATCHES = int(os.environ.get("AB_MAX_BATCHES", "3"))
BATCH_SIZE = int(os.environ.get("AB_BATCH_SIZE", "5"))
PAUSE_DELAY_MS = int(os.environ.get("AB_PAUSE_DELAY_MS", "2500"))
MAX_PAUSE_CYCLES = int(os.environ.get("AB_MAX_PAUSE_CYCLES", "240"))
MAX_TOTAL_PRODUCTIVE_BATCHES = int(os.environ.get("AB_MAX_TOTAL_PRODUCTIVE_BATCHES", str(MAX_BATCHES)))
MAX_TOTAL_NETWORK_CALLS = int(os.environ.get("AB_MAX_TOTAL_NETWORK_CALLS", "60"))
POST_SESSION_WAIT_SEC = int(os.environ.get("AB_POST_GC_WAIT_SEC", "30"))

START_MAX_MIB_EXCLUSIVE = 350.0
HARD_STOP_MIB_THRESHOLD = 360.0
WATCHER_INTERVAL_SEC = 2.0

WORK_PARITY_KEYS = (
    "productiveBatchesCompleted",
    "sessionProductiveBatchesCompleted",
    "sessionNetworkCallsMade",
    "checkpointTotalNetworkCallsMade",
    "fetchedRankSnapshots",
    "fetchedMatchIdLists",
    "fetchedMatches",
    "processedPuuidsCount",
    "processedMatchIdsCount",
    "lastStopReason",
)

DELTA_DTO_KEYS = (
    "postGcCaptured",
    "startAnonMiB",
    "peakCurrentMiB",
    "endPreGcAnonMiB",
    "endPostGcAnonMiB",
    "totalRetainedAnonPreGcMiB",
    "totalRetainedAnonPostGcMiB",
    "reclaimedByEndFullGcMiB",
    "deltaHeapUsedPreGcMiB",
    "deltaHeapCommittedPreGcMiB",
    "deltaNativeAnonResidualPreGcMiB",
    "deltaHeapUsedPostGcMiB",
    "deltaHeapCommittedPostGcMiB",
    "deltaMetaspaceCommittedMiB",
    "deltaNativeAnonResidualPostGcMiB",
)


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
    # Explicit iptables defense-in-depth on br-ab-isolated
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


def verify_egress_deny_from_container_netns(host_pid: int) -> dict:
    """
    Executes a fail-closed negative-control probe inside the container's network namespace (nsenter -n)
    without entering its cgroup namespace (so zero container memory perturbation).
    Proves that outbound connections to public IPv4 addresses and non-mocked hosts fail,
    while the internal bridge mock Riot server (172.28.0.1:443) is reachable.
    """
    probe_code = f"""
import json, socket, sys

results = {{}}

def check_blocked(label, host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect((host, port))
        s.close()
        results[label] = {{"blocked": False, "error": None}}
    except Exception as e:
        results[label] = {{"blocked": True, "error": type(e).__name__ + ": " + str(e)}}

check_blocked("external_ip_1_1_1_1_443", "1.1.1.1", 443)
check_blocked("external_ip_8_8_8_8_53", "8.8.8.8", 53)
check_blocked("external_ip_93_184_215_14_443", "93.184.215.14", 443)

# Verify internal mock Riot resolution and reachability on GATEWAY_IP:443
resolved_riot = socket.gethostbyname("euw1.api.riotgames.com")
resolved_ddragon = socket.gethostbyname("ddragon.leagueoflegends.com")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2.0)
s.connect(("{GATEWAY_IP}", 443))
s.close()
results["internal_mock_riot_443"] = {{
    "reachable": True,
    "resolvedRiotIp": resolved_riot,
    "resolvedDdragonIp": resolved_ddragon
}}
print(json.dumps(results))
"""
    raw = subprocess.check_output(
        ["nsenter", "-t", str(host_pid), "-n", sys.executable, "-c", probe_code],
        timeout=15
    ).decode("utf-8").strip()
    parsed = json.loads(raw)
    for neg_key in ("external_ip_1_1_1_1_443", "external_ip_8_8_8_8_53", "external_ip_93_184_215_14_443"):
        if not parsed.get(neg_key, {}).get("blocked", False):
            raise RuntimeError(f"EGRESS_DENY_NEGATIVE_CONTROL_FAILED: {neg_key} was NOT blocked: {parsed}")
    internal = parsed.get("internal_mock_riot_443", {})
    if not internal.get("reachable"):
        raise RuntimeError(f"INTERNAL_MOCK_RIOT_UNREACHABLE: {parsed}")
    return parsed


def inspect_container_fail_closed(container_name: str, expected_pid: int = None, expected_started_at: str = None) -> dict:
    """
    Reads `docker inspect` and fails closed if top-level `RestartCount` or `State.OOMKilled` is absent,
    if `RestartCount != 0`, if `OOMKilled` is True, or if PID / StartedAt changed.
    """
    raw = subprocess.check_output(["docker", "inspect", container_name], timeout=10).decode("utf-8")
    arr = json.loads(raw)
    if not isinstance(arr, list) or len(arr) != 1 or not isinstance(arr[0], dict):
        raise RuntimeError(f"FAIL_CLOSED_INSPECT_INVALID_ROOT: {raw[:200]}")
    info = arr[0]

    if "RestartCount" not in info or not isinstance(info["RestartCount"], int) or isinstance(info["RestartCount"], bool):
        raise RuntimeError(f"FAIL_CLOSED_MISSING_TOP_LEVEL_RESTART_COUNT: keys={list(info.keys())}")
    restart_count = info["RestartCount"]

    state = info.get("State")
    if not isinstance(state, dict):
        raise RuntimeError("FAIL_CLOSED_MISSING_STATE_DICT")
    if "OOMKilled" not in state or not isinstance(state["OOMKilled"], bool):
        raise RuntimeError(f"FAIL_CLOSED_MISSING_STATE_OOMKILLED: state_keys={list(state.keys())}")
    oom_killed = state["OOMKilled"]
    running = state.get("Running")
    pid = state.get("Pid")
    started_at = state.get("StartedAt")

    if restart_count != 0:
        raise RuntimeError(f"CONTAINER_RESTARTED_DURING_ARM: RestartCount={restart_count}")
    if oom_killed is not False:
        raise RuntimeError(f"CONTAINER_OOM_KILLED_DURING_ARM: OOMKilled={oom_killed}")
    if running is not True:
        raise RuntimeError(f"CONTAINER_NOT_RUNNING: Running={running}, ExitCode={state.get('ExitCode')}")
    if expected_pid is not None and pid != expected_pid:
        raise RuntimeError(f"CONTAINER_PID_CHANGED: expected={expected_pid}, actual={pid}")
    if expected_started_at is not None and started_at != expected_started_at:
        raise RuntimeError(f"CONTAINER_STARTED_AT_CHANGED: expected={expected_started_at}, actual={started_at}")

    return {
        "restartCount": restart_count,
        "oomKilled": oom_killed,
        "running": running,
        "pid": pid,
        "startedAt": started_at
    }


def combined_current_mib(host_pid: int, status: dict = None) -> dict:
    cg = fixture.read_host_cgroup_snapshot(host_pid)
    host_mib = cg["cgroupCurrentMiB"]
    peak_mib = cg["cgroupPeakMiB"]
    status_mib = None
    if status is not None:
        mem = status.get("memoryHeadroom") or {}
        c_bytes = mem.get("cgroupMemoryCurrentBytes")
        if isinstance(c_bytes, int) and c_bytes > 0:
            status_mib = round(c_bytes / (1024.0 * 1024.0), 2)
    effective_mib = max(host_mib, status_mib) if status_mib is not None else host_mib
    return {
        "hostCgroupCurrentMiB": host_mib,
        "hostCgroupPeakMiB": peak_mib,
        "statusCgroupCurrentMiB": status_mib,
        "effectiveCurrentMiB": effective_mib
    }


class Cgroup2sWatcher:
    """
    Independent 2-second host cgroup watcher thread.
    Samples /proc/{host_pid}/root/sys/fs/cgroup/memory.current and memory.peak every 2.0s.
    If cgroupCurrentMiB > 360.0 MiB at any sample, immediately requests collector stop
    (/api/riot/cohorts/euw1/collector/stop) and records the breach.
    """
    def __init__(self, host_pid: int):
        self.host_pid = host_pid
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.samples = []
        self.breach = None
        self.t0 = time.time()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self.sample_now("watcher_start")
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=5.0)

    def sample_now(self, tag: str) -> dict:
        cg = fixture.read_host_cgroup_snapshot(self.host_pid)
        mstat = fixture.parse_kv_bytes_file(f"/proc/{self.host_pid}/root/sys/fs/cgroup/memory.stat")
        anon_mib = round(mstat.get("anon", 0) / (1024.0 * 1024.0), 2)
        entry = {
            "idx": len(self.samples) + 1,
            "elapsedSec": round(time.time() - self.t0, 2),
            "tag": tag,
            "cgroupCurrentMiB": cg["cgroupCurrentMiB"],
            "cgroupPeakMiB": cg["cgroupPeakMiB"],
            "cgroupAnonMiB": anon_mib
        }
        triggered = False
        with self.lock:
            self.samples.append(entry)
            if entry["cgroupCurrentMiB"] > HARD_STOP_MIB_THRESHOLD and self.breach is None:
                self.breach = entry
                triggered = True
        if triggered:
            try:
                fixture.http_json(
                    f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/stop?patch={PATCH}",
                    method="POST",
                    headers=OP_HEADERS,
                    timeout=5
                )
            except Exception:
                pass
        return entry

    def _loop(self):
        while not self.stop_event.wait(WATCHER_INTERVAL_SEC):
            self.sample_now("2s_tick")

    def get_breach(self):
        with self.lock:
            return self.breach

    def get_summary(self) -> dict:
        with self.lock:
            if not self.samples:
                return {"sampleCount": 0}
            return {
                "sampleCount": len(self.samples),
                "maxCurrentMiB": max(s["cgroupCurrentMiB"] for s in self.samples),
                "maxPeakMiB": max(s["cgroupPeakMiB"] for s in self.samples),
                "maxAnonMiB": max(s["cgroupAnonMiB"] for s in self.samples),
                "breach": self.breach
            }


def run_arm(arm_label: str, image_tag: str) -> dict:
    setup_isolated_network()

    # 1. Start fresh isolated Postgres & Redis on ab-isolated-net
    subprocess.check_call([
        "docker", "run", "-d",
        "--name", "ss-postgres",
        "--network", NETWORK_NAME,
        "--ip", POSTGRES_IP,
        "-e", "POSTGRES_PASSWORD=postgres",
        "-e", "POSTGRES_DB=summoner_searcher",
        "postgres:16-alpine"
    ], stdout=subprocess.DEVNULL)

    subprocess.check_call([
        "docker", "run", "-d",
        "--name", "ss-redis",
        "--network", NETWORK_NAME,
        "--ip", REDIS_IP,
        "redis:7-alpine"
    ], stdout=subprocess.DEVNULL)
    time.sleep(3)

    # 2. Start 512 MiB backend container on ab-isolated-net (no container restart allowed within arm)
    subprocess.check_call([
        "docker", "run", "-d",
        "--name", "ss-backend-512m",
        "--network", NETWORK_NAME,
        "--ip", BACKEND_IP,
        "--memory=512m",
        "--memory-swap=512m",
        f"--add-host=euw1.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=na1.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=kr.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=europe.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=americas.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=asia.api.riotgames.com:{GATEWAY_IP}",
        f"--add-host=ddragon.leagueoflegends.com:{GATEWAY_IP}",
        "-v", "/tmp/cacerts:/opt/java/openjdk/lib/security/cacerts:ro",
        "-e", "PORT=10000",
        "-e", f"DB_HOST={POSTGRES_IP}",
        "-e", "DB_PORT=5432",
        "-e", "DB_NAME=summoner_searcher",
        "-e", "DB_USER=postgres",
        "-e", "DB_PASS=postgres",
        "-e", f"REDIS_URL=redis://{REDIS_IP}:6379",
        "-e", "RIOT_API_KEY=RGAPI-test-512m-proof",
        "-e", "JWT_SECRET_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "-e", "TWOFA_SECRET_KEY=0123456789abcdef0123456789abcdef",
        "-e", "RESEND_API_KEY=re_test_123456789",
        "-e", "GEMINI_API_KEY=disabled",
        "-e", "COHORT_NETWORK_COLLECTION_ENABLED=true",
        "-e", "COHORT_OPERATOR_TOKEN=test-op-token",
        image_tag
    ], stdout=subprocess.DEVNULL)

    # 3. Wait for readiness on GET /api/riot/cohorts/euw1/collector/status?patch=16.7
    boot_status = None
    for _ in range(90):
        try:
            boot_status = fixture.http_json(
                f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/status?patch={PATCH}",
                headers=OP_HEADERS,
                timeout=5
            )
            break
        except Exception:
            time.sleep(1)
    if boot_status is None:
        subprocess.run(["docker", "logs", "ss-backend-512m"])
        raise RuntimeError(f"[{arm_label}] Failed to boot container {image_tag}")

    initial_inspect = inspect_container_fail_closed("ss-backend-512m")
    host_pid = initial_inspect["pid"]
    started_at = initial_inspect["startedAt"]

    # 4. Run fail-closed negative-control egress deny verification inside container netns
    egress_proof = verify_egress_deny_from_container_netns(host_pid)

    # 5. Wait 3s for ChallengerCacheWarmer and start independent 2s cgroup watcher
    time.sleep(3)
    watcher = Cgroup2sWatcher(host_pid)
    watcher.start()

    try:
        # 6. Run bounded rate-limit probe (3 HTTP attempts) via deployed probe route
        probe_res = fixture.http_json(
            f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/probe?patch={PATCH}&seedPuuid=seed-puuid-1",
            method="POST",
            headers=OP_HEADERS,
            timeout=30
        )
        if probe_res.get("stoppedOnRateLimit429") or probe_res.get("probeStatus") == "PAUSED_MEMORY_HEADROOM":
            raise RuntimeError(f"[{arm_label}] Probe stopped early: {probe_res.get('probeStatus')}")

        sessions = []
        stop_reason_for_arm = "COMPLETED_MAX_SESSIONS"

        for session_idx in range(1, MAX_SESSIONS + 1):
            inspect_container_fail_closed("ss-backend-512m", expected_pid=host_pid, expected_started_at=started_at)

            if watcher.get_breach() is not None:
                stop_reason_for_arm = f"HARD_STOP_ABOVE_360_MIB_WATCHER_BEFORE_SESSION_{session_idx}"
                break

            pre_status = fixture.http_json(
                f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/status?patch={PATCH}",
                headers=OP_HEADERS,
                timeout=15
            )
            pre_mem = combined_current_mib(host_pid, pre_status)
            pre_mib = pre_mem["effectiveCurrentMiB"]

            if pre_mib > HARD_STOP_MIB_THRESHOLD:
                stop_reason_for_arm = f"HARD_STOP_ABOVE_360_MIB_BEFORE_SESSION_{session_idx} ({pre_mib} MiB)"
                break
            if not (pre_mib < START_MAX_MIB_EXCLUSIVE):
                stop_reason_for_arm = f"START_MEMORY_NOT_BELOW_350_MIB_BEFORE_SESSION_{session_idx} ({pre_mib} MiB)"
                break

            # Start session asynchronously (async=true) on the actual deployed route & parameter names
            start_url = (
                f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/start"
                f"?patch={PATCH}"
                f"&maxBatches={MAX_BATCHES}"
                f"&batchSize={BATCH_SIZE}"
                f"&pauseDelayMs={PAUSE_DELAY_MS}"
                f"&maxPauseCycles={MAX_PAUSE_CYCLES}"
                f"&maxTotalProductiveBatches={MAX_TOTAL_PRODUCTIVE_BATCHES}"
                f"&maxTotalNetworkCalls={MAX_TOTAL_NETWORK_CALLS}"
                f"&async=true"
            )
            try:
                start_ack = fixture.http_json(start_url, method="POST", headers=OP_HEADERS, timeout=30)
            except Exception as exc:
                stop_reason_for_arm = f"HTTP_ERROR_ON_START_SESSION_{session_idx}: {exc}"
                break

            # Poll every 2.0s while async session runs; 2s watcher thread concurrently enforces > 360 MiB stop
            poll_deadline = time.time() + 180.0
            end_status = None
            while time.time() < poll_deadline:
                inspect_container_fail_closed("ss-backend-512m", expected_pid=host_pid, expected_started_at=started_at)
                watcher.sample_now(f"session_{session_idx}_poll")
                cur_status = fixture.http_json(
                    f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/status?patch={PATCH}",
                    headers=OP_HEADERS,
                    timeout=15
                )
                cur_mem = combined_current_mib(host_pid, cur_status)
                if cur_mem["effectiveCurrentMiB"] > HARD_STOP_MIB_THRESHOLD and watcher.get_breach() is None:
                    # Trigger stop if status memory exceeded 360.0 MiB between watcher ticks
                    try:
                        fixture.http_json(
                            f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/stop?patch={PATCH}",
                            method="POST",
                            headers=OP_HEADERS,
                            timeout=10
                        )
                    except Exception:
                        pass
                if not cur_status.get("runningInProcess", False) and not cur_status.get("overnightSessionArmed", False):
                    end_status = cur_status
                    break
                time.sleep(WATCHER_INTERVAL_SEC)

            if end_status is None:
                raise RuntimeError(f"[{arm_label}] Session {session_idx} timed out after 180s")

            end_mem = combined_current_mib(host_pid, end_status)
            watcher.sample_now(f"session_{session_idx}_end_immediate")

            # Passive post-session wait to allow natural Old Gen GC check on GET status (if any occurs naturally)
            if POST_SESSION_WAIT_SEC > 0 and watcher.get_breach() is None and end_mem["effectiveCurrentMiB"] <= HARD_STOP_MIB_THRESHOLD:
                wait_end = time.time() + POST_SESSION_WAIT_SEC
                while time.time() < wait_end:
                    if watcher.get_breach() is not None:
                        break
                    time.sleep(min(WATCHER_INTERVAL_SEC, max(0.1, wait_end - time.time())))

            post_wait_status = fixture.http_json(
                f"{BASE_URL}/api/riot/cohorts/{REGION}/collector/status?patch={PATCH}",
                headers=OP_HEADERS,
                timeout=15
            )
            post_wait_mem = combined_current_mib(host_pid, post_wait_status)
            inspect_container_fail_closed("ss-backend-512m", expected_pid=host_pid, expected_started_at=started_at)

            cp = post_wait_status.get("checkpoint") or {}
            rl = post_wait_status.get("rateLimitVerification") or {}
            last_stop = cp.get("lastStopReason")

            session_record = {
                "session": session_idx,
                "preSessionMemory": pre_mem,
                "endImmediateMemory": end_mem,
                "postWaitMemory": post_wait_mem,
                "productiveBatchesCompleted": post_wait_status.get("productiveBatchesCompleted", 0),
                "sessionProductiveBatchesCompleted": post_wait_status.get("sessionProductiveBatchesCompleted", 0),
                "sessionNetworkCallsMade": post_wait_status.get("sessionNetworkCallsMade", 0),
                "checkpointTotalNetworkCallsMade": cp.get("totalNetworkCallsMade", 0),
                "fetchedRankSnapshots": cp.get("fetchedRankSnapshots", 0),
                "fetchedMatchIdLists": cp.get("fetchedMatchIdLists", 0),
                "fetchedMatches": cp.get("fetchedMatches", 0),
                "processedPuuidsCount": len(cp.get("processedPuuids") or []),
                "pendingCandidatePuuidsCount": len(cp.get("pendingCandidatePuuids") or []),
                "processedMatchIdsCount": len(cp.get("processedMatchIds") or []),
                "pausedForRetryAfterCount": cp.get("pausedForRetryAfterCount", 0),
                "pausedForMemoryHeadroomCount": cp.get("pausedForMemoryHeadroomCount", 0),
                "lastStopReason": last_stop,
                "sessionMemoryDeltaImmediate": end_status.get("sessionMemoryDelta"),
                "sessionMemoryDeltaPostWait": post_wait_status.get("sessionMemoryDelta"),
                "sessionMemorySeriesPostWait": post_wait_status.get("sessionMemorySeries")
            }
            sessions.append(session_record)

            # Check hard stop or error/429 conditions
            if watcher.get_breach() is not None:
                stop_reason_for_arm = f"HARD_STOP_ABOVE_360_MIB_WATCHER_DURING_SESSION_{session_idx}"
                break
            if end_mem["effectiveCurrentMiB"] > HARD_STOP_MIB_THRESHOLD or post_wait_mem["effectiveCurrentMiB"] > HARD_STOP_MIB_THRESHOLD:
                stop_reason_for_arm = f"HARD_STOP_ABOVE_360_MIB_AFTER_SESSION_{session_idx}"
                break
            if (
                last_stop in ("SESSION_ERROR_ABORT", "PAUSED_RETRY_AFTER_BACKOFF", "PAUSED_MEMORY_HEADROOM", "PAUSED_NETWORK_TIMEOUT")
                or rl.get("serviceRateLimitActive", False)
                or rl.get("retryAfterUntil") is not None
                or (cp.get("pausedForRetryAfterCount") or 0) > 0
                or (cp.get("pausedForMemoryHeadroomCount") or 0) > 0
            ):
                stop_reason_for_arm = f"STOPPED_ON_ERROR_OR_429_OR_HEADROOM_IN_SESSION_{session_idx} ({last_stop})"
                break
            if last_stop in ("NO_CANDIDATES_REMAINING", "ALL_TARGET_CELLS_SATISFIED"):
                stop_reason_for_arm = f"STOPPED_CANDIDATES_OR_CELLS_COMPLETE_AFTER_SESSION_{session_idx} ({last_stop})"
                break

        final_inspect = inspect_container_fail_closed("ss-backend-512m", expected_pid=host_pid, expected_started_at=started_at)
    finally:
        watcher.stop()

    productive_sessions = [
        s for s in sessions
        if (s.get("productiveBatchesCompleted") or 0) > 0 and (s.get("sessionNetworkCallsMade") or 0) > 0
    ]

    return {
        "arm": arm_label,
        "image": image_tag,
        "egressNegativeControlProof": egress_proof,
        "containerInspect": final_inspect,
        "watcher2sSummary": watcher.get_summary(),
        "armStopReason": stop_reason_for_arm,
        "totalSessionsAttempted": len(sessions),
        "productiveSessionsCompleted": len(productive_sessions),
        "sessions": sessions
    }


def classify_and_compare(base_arm: dict, cand_arm: dict) -> dict:
    base_prod = [
        s for s in base_arm["sessions"]
        if (s.get("productiveBatchesCompleted") or 0) > 0 and (s.get("sessionNetworkCallsMade") or 0) > 0
    ]
    cand_prod = [
        s for s in cand_arm["sessions"]
        if (s.get("productiveBatchesCompleted") or 0) > 0 and (s.get("sessionNetworkCallsMade") or 0) > 0
    ]

    if len(base_prod) == 0 or len(cand_prod) == 0:
        return {
            "classification": "FAILED_ZERO_PRODUCTIVE_SESSIONS",
            "comparableSessionCount": 0,
            "mismatches": [
                f"Zero productive sessions: base={len(base_prod)}, candidate={len(cand_prod)}"
            ],
            "pairedSessionComparisons": []
        }

    paired_count = min(len(base_prod), len(cand_prod))
    mismatches = []
    paired_comparisons = []

    for idx in range(paired_count):
        sa = base_prod[idx]
        sb = cand_prod[idx]
        s_num = sa["session"]
        session_mismatches = []
        for k in WORK_PARITY_KEYS:
            if sa.get(k) != sb.get(k):
                msg = f"Session {s_num} field '{k}' mismatch: base={sa.get(k)} vs candidate={sb.get(k)}"
                session_mismatches.append(msg)
                mismatches.append(msg)

        da = sa.get("sessionMemoryDeltaPostWait") or {}
        db = sb.get("sessionMemoryDeltaPostWait") or {}
        delta_comparison = {}
        for dk in DELTA_DTO_KEYS:
            va = da.get(dk)
            vb = db.get(dk)
            diff = round(vb - va, 2) if isinstance(va, (int, float)) and isinstance(vb, (int, float)) else None
            delta_comparison[dk] = {
                "base": va,
                "candidate": vb,
                "candidateMinusBase": diff
            }

        paired_comparisons.append({
            "session": s_num,
            "workMatched": len(session_mismatches) == 0,
            "workMismatches": session_mismatches,
            "productiveBatchesCompleted": sa.get("productiveBatchesCompleted"),
            "sessionNetworkCallsMade": sa.get("sessionNetworkCallsMade"),
            "checkpointTotalNetworkCallsMade": sa.get("checkpointTotalNetworkCallsMade"),
            "processedPuuidsCount": sa.get("processedPuuidsCount"),
            "lastStopReason": sa.get("lastStopReason"),
            "preSessionEffectiveCurrentMiB": {
                "base": sa["preSessionMemory"]["effectiveCurrentMiB"],
                "candidate": sb["preSessionMemory"]["effectiveCurrentMiB"],
                "candidateMinusBase": round(
                    sb["preSessionMemory"]["effectiveCurrentMiB"] - sa["preSessionMemory"]["effectiveCurrentMiB"], 2
                )
            },
            "sessionMemoryDeltaComparison": delta_comparison
        })

    if mismatches:
        classification = "INCOMPARABLE_MISMATCHED_WORK_OR_CALLS"
    elif len(base_prod) == MAX_SESSIONS and len(cand_prod) == MAX_SESSIONS and len(base_arm["sessions"]) == len(cand_arm["sessions"]):
        classification = "FULL_5_SESSION_COMPARABLE"
    else:
        classification = "PARTIAL_COMPARABLE"

    return {
        "classification": classification,
        "baseProductiveSessions": len(base_prod),
        "candidateProductiveSessions": len(cand_prod),
        "comparableMatchedSessionCount": paired_count if not mismatches else 0,
        "mismatches": mismatches,
        "pairedSessionComparisons": paired_comparisons
    }


def main():
    if len(sys.argv) < 3:
        print("Usage: ab_checkpoint_single_encode.py <cert_file> <key_file>", file=sys.stderr)
        sys.exit(1)

    cert_file = sys.argv[1]
    key_file = sys.argv[2]
    base_image = os.environ.get("AB_BASE_IMAGE", "ss-backend:ab-base-176f67f")
    cand_image = os.environ.get("AB_CAND_IMAGE", "ss-backend:ab-cand-92617dd")

    print("=== Starting checked-in MockRiotHandler HTTPS server on port 443 ===", flush=True)
    fixture.start_mock_riot_server(cert_file, key_file)
    time.sleep(1)

    try:
        print(f"=== Running ARM A (Base: {base_image}) ===", flush=True)
        base_res = run_arm("base_176f67f", base_image)

        print(f"=== Running ARM B (Candidate: {cand_image}) ===", flush=True)
        cand_res = run_arm("candidate_92617dd", cand_image)
    finally:
        teardown_isolated_network()

    comparison = classify_and_compare(base_res, cand_res)
    output = {
        "methodologyNote": (
            "Observed single-run differences only; not statistically proven causality. "
            "Single-encoding Redis checkpoint saves does not claim to fix overall session memory retention."
        ),
        "comparison": comparison,
        "baseArm": base_res,
        "candidateArm": cand_res
    }

    with open("/tmp/ab_checkpoint_single_encode_report.json", "w") as f:
        json.dump(output, f, indent=2)

    print("=== A/B COMPARISON REPORT JSON ===", flush=True)
    print(json.dumps(output, indent=2), flush=True)

    if comparison["classification"] == "FAILED_ZERO_PRODUCTIVE_SESSIONS":
        sys.exit(2)
    if comparison["classification"] == "INCOMPARABLE_MISMATCHED_WORK_OR_CALLS":
        sys.exit(3)


if __name__ == "__main__":
    main()
