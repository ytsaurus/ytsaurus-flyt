#!/usr/bin/env bash
# Port-forward UI, HTTP proxy, RPC (YT_UI_PORT, YT_HTTP_PROXY_PORT, YT_RPC_PROXY_PORT).
# Usage: from python/examples/kind: ./scripts/port-forward-ui.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIND_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_CR="${KIND_ROOT}/manifests/cluster_v1_flyt_local.yaml"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-default}"

# shellcheck source=./kind-common.sh
source "$SCRIPT_DIR/kind-common.sh"

YT_UI_PORT="${YT_UI_PORT:-50004}"
YT_HTTP_PROXY_PORT="${YT_HTTP_PROXY_PORT:-50005}"
YT_RPC_PROXY_PORT="${YT_RPC_PROXY_PORT:-50006}"

kind_need_cmd kubectl
kind_require_cluster_cr "$CLUSTER_CR"

YTS_NAME="$(kind_cluster_name "$CLUSTER_CR")"
if [[ -z "$YTS_NAME" ]]; then
  YTS_NAME="minisaurus"
fi
HTTP_SVC="$(kind_http_service "$CLUSTER_CR" "$YTS_NAME")"

pids=()
cleanup() {
  local p
  for p in "${pids[@]}"; do
    kill "$p" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

if ! kind_wait_for_access_targets "$KUBE_NAMESPACE" "$CLUSTER_CR" "$YTS_NAME" 120; then
  echo "ERROR: UI / proxy access targets are not ready yet." >&2
  exit 1
fi

RPC_TARGET="$(kind_rpc_target "$KUBE_NAMESPACE" "$CLUSTER_CR" "$YTS_NAME")"

kubectl port-forward -n "$KUBE_NAMESPACE" service/ytsaurus-ui "${YT_UI_PORT}:80" &
pids+=($!)

kubectl port-forward -n "$KUBE_NAMESPACE" service/"${HTTP_SVC}" "${YT_HTTP_PROXY_PORT}:80" &
pids+=($!)

kubectl port-forward -n "$KUBE_NAMESPACE" "${RPC_TARGET}" "${YT_RPC_PROXY_PORT}:9013" &
pids+=($!)

echo "==> Port-forwards (Ctrl+C to stop all)"
echo "    UI:          http://localhost:${YT_UI_PORT}"
echo "    HTTP proxy:  http://localhost:${YT_HTTP_PROXY_PORT}  (spec.ui.externalProxy must be localhost:${YT_HTTP_PROXY_PORT} without http://)"
echo "    RPC proxy:   localhost:${YT_RPC_PROXY_PORT}"
echo "    Test: curl -sS -o /dev/null -w '%{http_code}\\n' http://localhost:${YT_HTTP_PROXY_PORT}/ping"

wait
