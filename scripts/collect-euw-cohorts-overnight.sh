#!/usr/bin/env bash
set -euo pipefail

# Same-process guarded LOW-priority cohort collector control script for EUW Ranked Solo (queueId=420).
# - Operates inside the SAME running backend JVM (default http://127.0.0.1:8080) so the collector shares
#   the live in-memory RiotApiRateLimiter (verified X-App-Rate-Limit / X-Method-Rate-Limit headers and
#   process-local activeUserRequests / foregroundYieldActive counters).
# - Never spawns a separate headless JVM (which would start with an empty rate limiter and miss foreground user traffic).
# - Network calls remain OFF by default on the server (cohort.network-collection.enabled=false) until actual
#   App + Method rate-limit headers and headroom are verified in-process.
# - Checkpointed in Redis (cohort:collector:checkpoint:euw1:*). No guaranteed morning coverage is promised;
#   zero paid AI calls or paid API plan changes are used.

ACTION="${1:-${COLLECT_ACTION:-start}}"
REGION="${COLLECT_REGION:-euw1}"
TARGET_PATCH="${COLLECT_PATCH:-}"
MAX_BATCHES="${COLLECT_MAX_BATCHES:-20}"
BATCH_SIZE="${COLLECT_BATCH_SIZE:-25}"
PAUSE_DELAY_MS="${COLLECT_PAUSE_MS:-2500}"
POLL_SECONDS="${COLLECT_POLL_SECONDS:-5}"
WAIT_FOR_COMPLETION="${COLLECT_WAIT:-true}"
BASE_URL="${COLLECT_BASE_URL:-http://127.0.0.1:${PORT:-8080}}"
OPERATOR_TOKEN="${COHORT_OPERATOR_TOKEN:-}"

if ! command -v curl >/dev/null 2>&1; then
  echo "[cohort-collector] ERROR: curl is required to communicate with the same-process backend server." >&2
  exit 1
fi

CURL_HEADERS=(-H "Accept: application/json")
if [[ -n "${OPERATOR_TOKEN}" ]]; then
  CURL_HEADERS+=(-H "X-Cohort-Operator-Token: ${OPERATOR_TOKEN}")
fi

PATCH_PARAM=""
if [[ -n "${TARGET_PATCH}" ]]; then
  PATCH_PARAM="patch=${TARGET_PATCH}"
fi

STATUS_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/status"
if [[ -n "${PATCH_PARAM}" ]]; then
  STATUS_URL="${STATUS_URL}?${PATCH_PARAM}"
fi

echo "[cohort-collector] Mode=SAME_PROCESS_GUARDED | Action=${ACTION} | BaseUrl=${BASE_URL} | Region=${REGION} | Patch=${TARGET_PATCH:-auto-cached}"
echo "[cohort-collector] Guardrails: cap=10 games/player, gate=50 players + 200 games/cell, LOW priority, yields to same-process foreground traffic & 429 Retry-After."
echo "[cohort-collector] Notice: Resumable checkpointed run; coverage depends on verified Riot rate limits and candidate pool (no morning coverage promise)."

if ! PRE_STATUS="$(curl -fsS "${CURL_HEADERS[@]}" "${STATUS_URL}" 2>/dev/null)"; then
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
  stop)
    STOP_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/stop"
    if [[ -n "${PATCH_PARAM}" ]]; then
      STOP_URL="${STOP_URL}?${PATCH_PARAM}"
    fi
    STOP_RESP="$(curl -fsS -X POST "${CURL_HEADERS[@]}" "${STOP_URL}")"
    echo "[cohort-collector] Stop response:"
    echo "${STOP_RESP}"
    ;;
  start|resume)
    echo "[cohort-collector] Pre-flight same-process status:"
    echo "${PRE_STATUS}"
    START_QUERY="maxBatches=${MAX_BATCHES}&batchSize=${BATCH_SIZE}&pauseDelayMs=${PAUSE_DELAY_MS}&async=true"
    if [[ -n "${PATCH_PARAM}" ]]; then
      START_QUERY="${START_QUERY}&${PATCH_PARAM}"
    fi
    START_URL="${BASE_URL}/api/riot/cohorts/${REGION}/collector/start?${START_QUERY}"
    START_RESP="$(curl -fsS -X POST "${CURL_HEADERS[@]}" "${START_URL}")"
    echo "[cohort-collector] Start/resume response:"
    echo "${START_RESP}"

    if [[ "${WAIT_FOR_COMPLETION}" == "true" ]]; then
      while true; do
        CURRENT_STATUS="$(curl -fsS "${CURL_HEADERS[@]}" "${STATUS_URL}")"
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
    echo "[cohort-collector] ERROR: Unsupported action '${ACTION}'. Expected: start | resume | status | stop" >&2
    exit 1
    ;;
esac

