#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: java not found on PATH. Please install JDK 21 and set JAVA_HOME." >&2
  exit 1
fi

JAVA_VER_OUTPUT="$(java -version 2>&1)"
if ! echo "$JAVA_VER_OUTPUT" | head -n 1 | grep -Eq '"21(\.|")'; then
  echo "ERROR: JDK 21 is required, but found:" >&2
  echo "$JAVA_VER_OUTPUT" >&2
  exit 1
fi

echo "[preflight] Verified JDK 21:"
echo "$JAVA_VER_OUTPUT" | head -n 2

START_TS=$(date +%s)

echo "[preflight] Step 1/2: Compiling main and test Kotlin sources (./gradlew compileKotlin compileTestKotlin)..."
bash ./gradlew compileKotlin compileTestKotlin

echo "[preflight] Step 2/2: Running full test suite (./gradlew test)..."
bash ./gradlew test

END_TS=$(date +%s)
echo "[preflight] SUCCESS in $((END_TS - START_TS))s"
