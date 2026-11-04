#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# 1. Resolve JDK 21 (prefer existing JAVA_HOME if valid, otherwise worker-local /home/antigravity/.local/jdk21)
if [[ -z "${JAVA_HOME:-}" || ! -x "${JAVA_HOME}/bin/java" ]]; then
  if [[ -x "/home/antigravity/.local/jdk21/bin/java" ]]; then
    export JAVA_HOME="/home/antigravity/.local/jdk21"
  fi
fi

if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: java not found on PATH and /home/antigravity/.local/jdk21 is unavailable." >&2
  exit 1
fi

echo "[preflight] Using Java:"
java -version 2>&1 | head -n 2

# 2. Configure JVM proxy properties & local proxy CA truststore when running behind a localhost proxy
GRADLE_PROXY_ARGS=()
PROXY_URL="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
if [[ -n "$PROXY_URL" ]]; then
  PROXY_HOST_PORT="$(echo "$PROXY_URL" | sed -E 's|^[a-zA-Z]+://||; s|/.*$||')"
  PROXY_HOST="${PROXY_HOST_PORT%%:*}"
  PROXY_PORT="${PROXY_HOST_PORT##*:}"
  if [[ -n "$PROXY_HOST" && -n "$PROXY_PORT" && "$PROXY_HOST" != "$PROXY_PORT" ]]; then
    GRADLE_PROXY_ARGS+=(
      "-Dhttp.proxyHost=${PROXY_HOST}"
      "-Dhttp.proxyPort=${PROXY_PORT}"
      "-Dhttps.proxyHost=${PROXY_HOST}"
      "-Dhttps.proxyPort=${PROXY_PORT}"
    )
  fi

  # Ensure worker-local JDK cacerts trusts the system proxy CA if present in /etc/ssl/certs/ca-certificates.crt
  if [[ -n "${JAVA_HOME:-}" && -w "${JAVA_HOME}/lib/security/cacerts" && -f "/etc/ssl/certs/ca-certificates.crt" ]]; then
    if ! "${JAVA_HOME}/bin/keytool" -list -cacerts -storepass changeit -alias antigravity-proxy-ca >/dev/null 2>&1; then
      if grep -q "Antigravity" /etc/ssl/certs/ca-certificates.crt 2>/dev/null && command -v python3 >/dev/null 2>&1; then
        python3 - << 'PYEOF'
import os, re, subprocess, tempfile
java_home = os.environ["JAVA_HOME"]
with open("/etc/ssl/certs/ca-certificates.crt") as f:
    pem = f.read()
for cert in re.findall(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", pem, re.DOTALL):
    p = subprocess.run(["openssl", "x509", "-noout", "-subject"], input=cert, text=True, capture_output=True)
    if "Antigravity" in p.stdout:
        with tempfile.NamedTemporaryFile("w", suffix=".crt", delete=False) as tf:
            tf.write(cert)
            cert_path = tf.name
        subprocess.run([
            f"{java_home}/bin/keytool", "-importcert", "-noprompt", "-trustcacerts",
            "-alias", "antigravity-proxy-ca", "-file", cert_path,
            "-cacerts", "-storepass", "changeit"
        ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.unlink(cert_path)
        break
PYEOF
      fi
    fi
  fi
fi

# Restore tracked .gradle/ and build/ directories on exit during local preflight (while issue #8 is held)
cleanup_local_build_tree() {
  if [[ "${CI:-false}" != "true" && "${KEEP_BUILD_ARTIFACTS:-false}" != "true" ]]; then
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      git checkout -- .gradle build >/dev/null 2>&1 || true
      git clean -fd .gradle build >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup_local_build_tree EXIT

START_TS=$(date +%s)

echo "[preflight] Step 1/2: Running JDK 21 compilation (compileKotlin compileTestKotlin)..."
bash ./gradlew compileKotlin compileTestKotlin "${GRADLE_PROXY_ARGS[@]}"

echo "[preflight] Step 2/2: Running test suite (./gradlew test $*)..."
bash ./gradlew test "${GRADLE_PROXY_ARGS[@]}" "$@"

END_TS=$(date +%s)
echo "[preflight] SUCCESS in $((END_TS - START_TS))s"
