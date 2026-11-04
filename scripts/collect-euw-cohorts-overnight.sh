#!/usr/bin/env bash
set -euo pipefail

# Resumable, checkpointed LOW-priority cohort collector script for EUW Ranked Solo (queueId=420).
# - Targets: region=euw1, current patch, roles=TOP/MIDDLE/BOTTOM, all 10 rank tiers,
#   cap=10 games/player per (tier, role) cell, quality gate=50 players + 200 games per cell.
# - Network calls are OFF by default (COHORT_NETWORK_COLLECTION_ENABLED=false) and only execute
#   after actual Riot X-App-Rate-Limit and X-Method-Rate-Limit headers and headroom are verified.
# - Yields to foreground user requests and respects 429 Retry-After / service rate limits.
# - NOTE: Progress is checkpointed in Redis (cohort:collector:checkpoint:euw1:*). No guaranteed
#   morning coverage is promised; actual throughput is bounded by Riot API rate limits and cache state.
#   Zero paid AI calls or paid API plan changes are used.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if ! command -v java >/dev/null 2>&1; then
  echo "[cohort-collector] ERROR: java not found on PATH (JDK 21 required)." >&2
  exit 1
fi

JAVA_VER="$(java -version 2>&1 | head -n 1)"
if [[ "${JAVA_VER}" != *"21."* ]]; then
  echo "[cohort-collector] ERROR: Expected JDK 21, found: ${JAVA_VER}" >&2
  exit 1
fi

REGION="${COLLECT_REGION:-euw1}"
TARGET_PATCH="${COLLECT_PATCH:-}"
MAX_BATCHES="${COLLECT_MAX_BATCHES:-20}"
BATCH_SIZE="${COLLECT_BATCH_SIZE:-25}"
NETWORK_ENABLED="${COHORT_NETWORK_COLLECTION_ENABLED:-false}"

echo "[cohort-collector] Region=${REGION} | Patch=${TARGET_PATCH:-auto-cached} | MaxBatches=${MAX_BATCHES} | BatchSize=${BATCH_SIZE} | NetworkEnabled=${NETWORK_ENABLED}"
echo "[cohort-collector] Guardrails: cap=10 games/player, gate=50 players + 200 games/cell, LOW priority, yields to foreground & 429 Retry-After."
echo "[cohort-collector] Notice: Resumable checkpointed run; coverage depends on verified Riot rate limits and candidate pool (no morning coverage promise)."

ARGS="--collect-cohorts --collect-region=${REGION} --collect-max-batches=${MAX_BATCHES} --collect-batch-size=${BATCH_SIZE}"
if [[ -n "${TARGET_PATCH}" ]]; then
  ARGS="${ARGS} --collect-patch=${TARGET_PATCH}"
fi

./gradlew bootRun \
  -Pargs="${ARGS}" \
  -Dspring.main.web-application-type=none \
  -Dcohort.network-collection.enabled="${NETWORK_ENABLED}"
