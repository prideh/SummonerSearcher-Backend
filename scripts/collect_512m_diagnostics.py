#!/usr/bin/env python3
import json
import os
import subprocess
import urllib.request

def http_json(url: str, headers: dict = None, timeout: int = 20) -> dict:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def format_mib_diff(before_bytes: int, after_bytes: int) -> str:
    b_mib = before_bytes / (1024 * 1024)
    a_mib = after_bytes / (1024 * 1024)
    d_mib = a_mib - b_mib
    sign = "+" if d_mib >= 0 else ""
    return f"{b_mib:.2f} -> {a_mib:.2f} MiB ({sign}{d_mib:.2f} MiB)"

def print_host_diff(label: str, prev: dict, curr: dict):
    print(f"=== HOST CGROUP & PROC DIFF: {label} ===", flush=True)
    for k in ["anon", "file", "kernel", "slab", "sock", "shmem", "anon_thp", "file_mapped"]:
        b = prev.get("mstat", {}).get(k, 0)
        a = curr.get("mstat", {}).get(k, 0)
        if b > 0 or a > 0:
            print(f"  cgroup memory.stat[{k}]: {format_mib_diff(b, a)}")
    for k in ["VmPeak", "VmSize", "VmRSS", "RssAnon", "RssFile", "VmData", "VmStk"]:
        b = prev.get("status", {}).get(k, 0)
        a = curr.get("status", {}).get(k, 0)
        if b > 0 or a > 0:
            print(f"  /proc/pid/status[{k}]: {format_mib_diff(b, a)}")
    for k in ["Rss", "Pss", "Anonymous", "AnonHugePages", "Private_Dirty", "Private_Clean"]:
        b = prev.get("rollup", {}).get(k, 0)
        a = curr.get("rollup", {}).get(k, 0)
        if b > 0 or a > 0:
            print(f"  /proc/pid/smaps_rollup[{k}]: {format_mib_diff(b, a)}")

    all_keys = set(prev.get("smaps", {}).keys()) | set(curr.get("smaps", {}).keys())
    vma_diffs = []
    for k in all_keys:
        b_vma = prev.get("smaps", {}).get(k, {"Size": 0, "Rss": 0, "Anonymous": 0, "AnonHugePages": 0})
        a_vma = curr.get("smaps", {}).get(k, {"Size": 0, "Rss": 0, "Anonymous": 0, "AnonHugePages": 0})
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
    print("=== NON-GATING DIAGNOSTICS STEP (NMT, memory.stat, smaps_rollup, smaps) ===", flush=True)
    libc_info = subprocess.check_output(
        ["docker", "exec", "ss-backend-512m", "sh", "-c", "ldd --version 2>&1 || true"]
    ).decode("utf-8", errors="replace").strip()
    print(f"Container libc info:\n{libc_info}", flush=True)

    if os.path.exists("/tmp/proof_host_samples.json"):
        with open("/tmp/proof_host_samples.json", "r") as f:
            saved = json.load(f)
        samples = saved.get("samples", {})
        cycles = samples.get("cycles", {})
        print("=== PER-CYCLE HOST memory.stat & smaps_rollup BREAKDOWN ===", flush=True)
        for c_idx in range(1, 11):
            c_sample = cycles.get(str(c_idx), {})
            mstat = c_sample.get("mstat", {})
            rollup = c_sample.get("rollup", {})
            diag_row = {
                "cycle": c_idx,
                "cgroupAnonMiB": round(mstat.get("anon", 0) / (1024 * 1024), 2),
                "cgroupFileMiB": round(mstat.get("file", 0) / (1024 * 1024), 2),
                "cgroupSlabMiB": round(mstat.get("slab", 0) / (1024 * 1024), 2),
                "cgroupSockMiB": round(mstat.get("sock", 0) / (1024 * 1024), 2),
                "cgroupKernelMiB": round(mstat.get("kernel", 0) / (1024 * 1024), 2),
                "smapsRssMiB": round(rollup.get("Rss", 0) / (1024 * 1024), 2),
                "smapsPssMiB": round(rollup.get("Pss", 0) / (1024 * 1024), 2),
                "smapsAnonMiB": round(rollup.get("Anonymous", 0) / (1024 * 1024), 2),
                "smapsAnonHugeMiB": round(rollup.get("AnonHugePages", 0) / (1024 * 1024), 2),
                "smapsPrivateDirtyMiB": round(rollup.get("Private_Dirty", 0) / (1024 * 1024), 2),
                "smapsPrivateCleanMiB": round(rollup.get("Private_Clean", 0) / (1024 * 1024), 2),
            }
            print(f"DIAG_CYCLE {c_idx:02d}: {json.dumps(diag_row)}", flush=True)

        pre_cycle = samples.get("pre_cycle", {})
        if pre_cycle and "1" in cycles:
            print_host_diff("Pre-Cycle -> Cycle 1", pre_cycle, cycles["1"])
        for c_idx in range(2, 11):
            prev_c = cycles.get(str(c_idx - 1), {})
            curr_c = cycles.get(str(c_idx), {})
            if not prev_c or not curr_c:
                continue
            d_anon_mib = abs(curr_c.get("mstat", {}).get("anon", 0) - prev_c.get("mstat", {}).get("anon", 0)) / (1024 * 1024)
            if c_idx in (6, 7, 10) or d_anon_mib >= 4.0:
                print_host_diff(f"Cycle {c_idx - 1} -> Cycle {c_idx}", prev_c, curr_c)
        if pre_cycle and "10" in cycles:
            print_host_diff("Pre-Cycle -> Cycle 10", pre_cycle, cycles["10"])

    base_url = "http://localhost:10000"
    op_headers = {"X-Cohort-Operator-Token": "test-op-token"}
    nmt_diff = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=detail.diff", headers=op_headers, timeout=25)
    print("=== JVM ARGS, MALLOC_ARENA_MAX, THREADS, POOLS, BUFFER POOLS, GC BEANS ===", flush=True)
    print(json.dumps({
        "vmArgs": nmt_diff.get("vmArgs"),
        "mallocArenaMax": nmt_diff.get("mallocArenaMax"),
        "threadCount": nmt_diff.get("threadCount"),
        "peakThreadCount": nmt_diff.get("peakThreadCount"),
        "nonHeapMemoryUsage": nmt_diff.get("nonHeapMemoryUsage"),
        "heapMemoryUsage": nmt_diff.get("heapMemoryUsage"),
        "memoryPools": nmt_diff.get("memoryPools"),
        "bufferPools": nmt_diff.get("bufferPools"),
        "gcBeans": nmt_diff.get("gcBeans")
    }, indent=2), flush=True)

    print("=== NMT DETAIL DIFF (Pre-Cycle -> Post-Cycle-10) ===", flush=True)
    print(extract_detail_diff_highlights(nmt_diff.get("nmtOutput", "")), flush=True)

    nmt_summary = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=summary", headers=op_headers, timeout=20)
    print("=== NMT FULL SUMMARY (Post-Cycle-10) ===", flush=True)
    print(nmt_summary.get("nmtOutput", ""), flush=True)

    gc_nmt = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/nmt?cmd=gc", headers=op_headers, timeout=20)
    post_gc_snap = http_json(f"{base_url}/api/riot/cohorts/euw1/collector/status?patch=16.7", headers=op_headers, timeout=15)["memoryHeadroom"]
    print("=== POST-EXPLICIT-GC MEMORY HEADROOM SNAPSHOT ===", flush=True)
    print(json.dumps(post_gc_snap, indent=2), flush=True)
    print("=== POST-EXPLICIT-GC NMT SUMMARY DIFF ===", flush=True)
    print(gc_nmt.get("nmtOutput", ""), flush=True)

if __name__ == "__main__":
    main()
