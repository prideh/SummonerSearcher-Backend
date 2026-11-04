#!/usr/bin/env bash
set -euo pipefail

# Same-process guarded LOW-priority cohort collector control script for EUW Ranked Solo (queueId=420).
# - Operates inside the SAME running backend JVM (default http://127.0.0.1:8080) so the collector shares
#   the live in-memory RiotApiRateLimiter (verified X-App-Rate-Limit / X-Method-Rate-Limit headers and
#   process-local activeUserRequests / foregroundYieldActive counters).
# - Supports both `curl` and `wget` (for minimal container shells).
# - Actions:
#     probe  : Guarded bounded diagnostic probe from cached EUW seed (max 1 LOW-priority call per missing required endpoint)
#              to observe real App + Method headers BEFORE enabling bulk network collection.
#     status : Read-only same-process status + rate-limit verification + checkpoint inspection.
#     start  : Guarded same-process start/resume (runs bounded probe first if required headers are missing).
#     stop   : Signals clean stop at next call boundary and saves checkpoint.
# - Network calls remain OFF by default on the server (cohort.network-collection.enabled=false).
#   No guaranteed morning coverage is promised; zero paid AI calls or paid API plan changes are used.

ACTION="${1:-${COLLECT_ACTION:-start}}"
REGION="${COLLECT_REGION:-euw1}"
TARGET_PATCH="${COLLECT_PATCH:-}"
SEED_PUUID="${COLLECT_SEED_PUUID:-}"
SEED_MATCH_ID="${COLLECT_SEED_MATCH_ID:-}"
MAX_BATCHES="${COLLECT_MAX_BATCHES:-20}"
BATCH_SIZE="${COLLECT_BATCH_SIZE:-25}"
PAUSE_DELAY_MS="${COLLECT_PAUSE_MS:-2500}"
POLL_SECONDS="${COLLECT_POLL_SECONDS:-5}"
WAIT_FOR_COMPLETION="${COLLECT_WAIT:-true}"
BASE_URL="${COLLECT_BASE_URL:-http://127.0.0.1:${PORT:-8080}}"
OPERATOR_TOKEN="${COHORT_OPERATOR_TOKEN:-}"

http_get() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    if [[ -n "${OPERATOR_TOKEN}" ]]; then
      curl -fsS -H "Accept: application/json" -H "X-Cohort-Operator-Token: ${OPERATOR_TOKEN}" "${url}"
    else
      curl -fsS -H "Accept: application/json" "${url}"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if [[ -n "${OPERATOR_TOKEN}" ]]; then
      wget -qO- --header="Accept: application/json" --header="X-Cohort-Operator-Token: ${OPERATOR_TOKEN}" "${url}"
    else
      wget -qO- --header="Accept: application/json" "${url}"
    fi
  else
    echo "[cohort-collector] ERROR: Neither curl nor wget is installed." >&2
    return 1
  fi
}

http_post() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    if [[ -n "${OPERATOR_TOKEN}" ]]; then
      curl -fsS -X POST -H "Accept: application/json" -H "X-Cohort-Operator-Token: ${OPERATOR_TOKEN}" "${url}"
    else
      curl -fsS -X POST -H "Accept: application/json" "${url}"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if [[ -n "${OPERATOR_TOKEN}" ]]; then
      wget -qO- --post-data="" --header="Accept: application/json" --header="X-Cohort-Operator-Token: ${OPERATOR_TOKEN}" "${url}"
    else
      wget -qO- --post-data="" --header="Accept: application/json" "${url}"
    fi
  else
    echo "[cohort-collector] ERROR: Neither curl nor wget is installed." >&2
    return 1
  fi
}

PATCH_PARAM=""
if [[ -n "${TARGET_PATCH}" ]]; then
  PATCH_PARAM="patch=${TARGET_PATCH}"
fi

STATUS_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/status"
if [[ -n "${PATCH_PARAM}" ]]; then
  STATUS_URL="${STATUS_URL}?${PATCH_PARAM}"
fi

PROBE_QUERY=""
if [[ -n "${PATCH_PARAM}" ]]; then
  PROBE_QUERY="${PATCH_PARAM}"
fi
if [[ -n "${SEED_PUUID}" ]]; then
  PROBE_QUERY="${PROBE_QUERY:+${PROBE_QUERY}&}seedPuuid=${SEED_PUUID}"
fi
if [[ -n "${SEED_MATCH_ID}" ]]; then
  PROBE_QUERY="${PROBE_QUERY:+${PROBE_QUERY}&}seedMatchId=${SEED_MATCH_ID}"
fi
PROBE_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/probe"
if [[ -n "${PROBE_QUERY}" ]]; then
  PROBE_URL="${PROBE_URL}?${PROBE_QUERY}"
fi

echo "[cohort-collector] Mode=SAME_PROCESS_GUARDED | Action=${ACTION} | BaseUrl=${BASE_URL} | Region=${REGION} | Patch=${TARGET_PATCH:-auto-cached}"
echo "[cohort-collector] Guardrails: cap=10 games/player, gate=50 players + 200 games/cell, LOW priority, yields to same-process foreground traffic & 429 Retry-After."
echo "[cohort-collector] Notice: Resumable checkpointed run; coverage depends on verified Riot rate limits and candidate pool (no morning coverage promise)."

if ! PRE_STATUS="$(http_get "${STATUS_URL}" 2>/dev/null)"; then
  echo "[cohort-collector] ERROR: Could not reach guarded same-process collector endpoint at ${STATUS_URL}." >&2
  echo "[cohort-collector] The collector MUST run inside the live backend server JVM so it shares in-memory RiotApiRateLimiter headers and foreground request counters." >&2
  echo "[cohort-collector] Ensure the backend server is running on ${BASE_URL} (network OFF by default: COHORT_NETWORK_COLLECTION_ENABLED=false ./gradlew bootRun) and retry." >&2
  exit 1
fi

case "${ACTION}" in
  status)
    echo "[cohort-collector] Live same-process collector status:"
    echo "${PRE_STATUS}"
    ;;
  probe)
    echo "[cohort-collector] Running guarded bounded diagnostic probe (max 1 LOW-priority call per missing required endpoint):"
    PROBE_RESP="$(http_post "${PROBE_URL}")"
    echo "${PROBE_RESP}"
    ;;
  stop)
    STOP_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/stop"
    if [[ -n "${PATCH_PARAM}" ]]; then
      STOP_URL="${STOP_URL}?${PATCH_PARAM}"
    fi
    STOP_RESP="$(http_post "${STOP_URL}")"
    echo "[cohort-collector] Stop response:"
    echo "${STOP_RESP}"
    ;;
  start|resume)
    echo "[cohort-collector] Pre-flight same-process status:"
    echo "${PRE_STATUS}"
    if [[ "${PRE_STATUS}" == *'"appLimitsVerifiedFromHeaders":false'* ]] || [[ "${PRE_STATUS}" != *'"missingRequiredMethodEndpoints":[]'* ]]; then
      echo "[cohort-collector] Required rate-limit headers not yet verified in-process; executing bounded diagnostic probe first (max 1 call/endpoint)..."
      PROBE_RESP="$(http_post "${PROBE_URL}")"
      echo "[cohort-collector] Diagnostic probe response:"
      echo "${PROBE_RESP}"
    fi
    START_QUERY="maxBatches=${MAX_BATCHES}&batchSize=${BATCH_SIZE}&pauseDelayMs=${PAUSE_DELAY_MS}&async=true"
    if [[ -n "${PATCH_PARAM}" ]]; then
      START_QUERY="${START_QUERY}&${PATCH_PARAM}"
    fi
    START_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/start?${START_QUERY}"
    START_RESP="$(http_post "${START_URL}")"
    echo "[cohort-collector] Start/resume response:"
    echo "${START_RESP}"

    if [[ "${WAIT_FOR_COMPLETION}" == "true" ]]; then
      while true; do
        CURRENT_STATUS="$(http_get "${STATUS_URL}")"
        if [[ "${CURRENT_STATUS}" != *'"runningInProcess":true'* ]]; then
          echo "[cohort-collector] Same-process collector run finished or paused:"
          echo "${CURRENT_STATUS}"
          break
        fi
        echo "[cohort-collector] In-process collector active; polling again in ${POLL_SECONDS}s..."
        sleep "${POLL_SECONDS}"
      done
    fi
    ;;
  *)
    echo "[cohort-collector] ERROR: Unsupported action '${ACTION}'. Expected: probe | status | start | resume | stop" >&2
    exit 1
    ;;
esac


