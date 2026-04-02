#!/usr/bin/env bash
# Bootstrap Kind cluster + port-forward UI/HTTP/RPC (YT_UI_PORT, YT_HTTP_PROXY_PORT, YT_RPC_PROXY_PORT).
# Usage: from python/examples/kind: ./scripts/up.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-default}"
BOOTSTRAP_RETRIES="${BOOTSTRAP_RETRIES:-2}"

kill_kubectl_listener() {
  local port="$1"
  local pid
  for pid in $(fuser -n tcp "$port" 2>/dev/null || true); do
    if [[ "$(ps -o comm= -p "$pid" 2>/dev/null | tr -d ' ')" == "kubectl" ]]; then
      kill "$pid" 2>/dev/null || true
    else
      echo "ERROR: port $port is busy by pid $pid ($(ps -o args= -p "$pid" 2>/dev/null))." >&2
      echo "Stop that process or choose another port via YT_UI_PORT / YT_HTTP_PROXY_PORT / YT_RPC_PROXY_PORT." >&2
      exit 1
    fi
  done
}

cleanup_recovery_pods() {
  local entry
  local name

  while IFS= read -r entry; do
    name="${entry##*/}"
    case "$name" in
      hp-0|ms-0|sch-0|ca-0|tnd-0|dnd-0|rp-0|qt-0|yqla-0|ytsaurus-ui-deployment-*|yt-strawberry-controller-init-job-*)
        kubectl delete -n "$KUBE_NAMESPACE" "$entry" --ignore-not-found=true >/dev/null 2>&1 || true
        ;;
    esac
  done < <(kubectl get pod -n "$KUBE_NAMESPACE" -o name 2>/dev/null || true)
}

run_bootstrap_with_retry() {
  local attempt

  for (( attempt = 1; attempt <= BOOTSTRAP_RETRIES; attempt++ )); do
    if FLYT_KIND_AUTO_FORWARD=1 "$SCRIPT_DIR/bootstrap.sh"; then
      return 0
    fi

    if (( attempt == BOOTSTRAP_RETRIES )); then
      break
    fi

    echo "WARN: bootstrap attempt ${attempt}/${BOOTSTRAP_RETRIES} failed; cleaning stale pods and retrying..." >&2
    cleanup_recovery_pods
    sleep 5
  done

  echo "ERROR: ./scripts/up.sh could not reconcile the Kind cluster after ${BOOTSTRAP_RETRIES} attempts." >&2
  return 1
}

YT_UI_PORT="${YT_UI_PORT:-50004}"
YT_HTTP_PROXY_PORT="${YT_HTTP_PROXY_PORT:-50005}"
YT_RPC_PROXY_PORT="${YT_RPC_PROXY_PORT:-50006}"

run_bootstrap_with_retry

kill_kubectl_listener "$YT_UI_PORT"
kill_kubectl_listener "$YT_HTTP_PROXY_PORT"
kill_kubectl_listener "$YT_RPC_PROXY_PORT"

exec "$SCRIPT_DIR/port-forward-ui.sh"
