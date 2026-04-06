#!/usr/bin/env bash

kind_need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    exit 1
  }
}

# Warn when the Kind node cgroup is close to its PID limit (Podman default ~2048).
# Symptom: ytserver CrashLoop with "failed to create thread" / exit 132 on data nodes.
kind_warn_podman_pid_pressure() {
  local container cur max pct
  if ! command -v podman >/dev/null 2>&1; then
    return 0
  fi
  container="$(podman ps --filter name=flyt-local-control-plane --format '{{.Names}}' 2>/dev/null | head -1)"
  [[ -n "$container" ]] || return 0
  cur="$(podman exec "$container" cat /sys/fs/cgroup/pids.current 2>/dev/null || true)"
  max="$(podman exec "$container" cat /sys/fs/cgroup/pids.max 2>/dev/null || true)"
  [[ "$cur" =~ ^[0-9]+$ && "$max" =~ ^[0-9]+$ ]] || return 0
  if [[ "$max" -eq 0 ]]; then
    return 0
  fi
  pct=$((cur * 100 / max))
  if (( pct >= 85 )); then
    echo "WARN: Kind node PID usage is ${cur}/${max} (${pct}%). YTsaurus pods may fail with 'failed to create thread'." >&2
    echo "      Fix: raise Podman pids_limit (see TROUBLESHOOTING.md), e.g. ~/.config/containers/containers.conf:" >&2
    echo "        [containers]" >&2
    echo "        pids_limit = 16384" >&2
    echo "      Then recreate the Kind cluster." >&2
  fi
}

kind_require_cluster_cr() {
  local cluster_cr="$1"
  if [[ ! -f "$cluster_cr" ]]; then
    echo "ERROR: missing $cluster_cr" >&2
    exit 1
  fi
}

kind_wait_for_deployment() {
  local ns="$1"
  local deployment="$2"
  local timeout_s="${3:-300}"

  if ! kubectl get deploy "$deployment" -n "$ns" >/dev/null 2>&1; then
    echo "ERROR: deployment/$deployment not found in namespace $ns." >&2
    return 1
  fi

  kubectl rollout status "deploy/${deployment}" -n "$ns" --timeout="${timeout_s}s"
}

kind_wait_for_service_endpoints() {
  local ns="$1"
  local service="$2"
  local timeout_s="${3:-180}"
  local description="${4:-$service}"
  local deadline endpoint first_endpoint

  deadline=$((SECONDS + timeout_s))
  while (( SECONDS < deadline )); do
    endpoint="$(kubectl get endpoints "$service" -n "$ns" \
      -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null || true)"
    if [[ -n "${endpoint//$'\n'/}" ]]; then
      first_endpoint="${endpoint%%$'\n'*}"
      echo "    OK: ${description}: ${first_endpoint}"
      return 0
    fi
    sleep 2
  done

  echo "ERROR: ${description} has no ready endpoints after ${timeout_s}s." >&2
  kubectl get svc "$service" -n "$ns" >&2 || true
  kubectl get endpoints "$service" -n "$ns" >&2 || true
  return 1
}

# Default matches README (Kind example). Override with CERT_MANAGER_MANIFEST_URL.
kind_install_cert_manager_if_missing() {
  local ns="${1:-cert-manager}"
  local manifest_url="${CERT_MANAGER_MANIFEST_URL:-https://github.com/cert-manager/cert-manager/releases/download/v1.17.1/cert-manager.yaml}"
  local deadline

  if kubectl get namespace "$ns" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${FLYT_CERT_MANAGER_AUTO_INSTALL:-1}" != "1" ]]; then
    echo "ERROR: namespace/$ns not found. Install cert-manager (see README) or set FLYT_CERT_MANAGER_AUTO_INSTALL=1." >&2
    return 1
  fi

  echo "==> Installing cert-manager (${manifest_url})..."
  kubectl apply -f "$manifest_url"

  deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if kubectl get deploy cert-manager -n "$ns" >/dev/null 2>&1 \
      && kubectl get deploy cert-manager-cainjector -n "$ns" >/dev/null 2>&1 \
      && kubectl get deploy cert-manager-webhook -n "$ns" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: cert-manager Deployments did not appear after install." >&2
  return 1
}

kind_ensure_cert_manager_ready() {
  local ns="${1:-cert-manager}"
  local timeout_s="${2:-300}"
  local deployment

  kind_install_cert_manager_if_missing "$ns" || return 1

  echo "==> Waiting for cert-manager webhook stack..."
  for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
    kubectl scale deploy "$deployment" -n "$ns" --replicas=1 >/dev/null 2>&1 || true
  done
  for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
    kind_wait_for_deployment "$ns" "$deployment" "$timeout_s" || return 1
  done
  kind_wait_for_service_endpoints "$ns" cert-manager-webhook "$timeout_s" "cert-manager webhook"
}

kind_cluster_name() {
  local cluster_cr="$1"
  kubectl get -f "$cluster_cr" -o jsonpath='{.metadata.name}' 2>/dev/null || true
}

kind_use_short_names() {
  local cluster_cr="$1"
  if grep -qE '^[[:space:]]*useShortNames:[[:space:]]*true[[:space:]]*$' "$cluster_cr"; then
    return 0
  fi
  return 1
}

kind_http_service() {
  local cluster_cr="$1"
  local yts_name="$2"
  if kind_use_short_names "$cluster_cr"; then
    echo "http-proxies-lb"
  else
    echo "http-proxies-lb-${yts_name}"
  fi
}

kind_rpc_lb_service() {
  local cluster_cr="$1"
  local yts_name="$2"
  if kind_use_short_names "$cluster_cr"; then
    echo "rpc-proxies-lb"
  else
    echo "rpc-proxies-lb-${yts_name}"
  fi
}

kind_rpc_target() {
  local ns="$1"
  local cluster_cr="$2"
  local yts_name="$3"
  local rpc_lb_svc
  local pod

  rpc_lb_svc="$(kind_rpc_lb_service "$cluster_cr" "$yts_name")"
  if kubectl get svc -n "$ns" "$rpc_lb_svc" -o name >/dev/null 2>&1; then
    echo "service/${rpc_lb_svc}"
    return
  fi

  pod="$(kubectl get pod -n "$ns" -l "yt_component=${yts_name}-yt-rpc-proxy" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    pod="$(kubectl get pod -n "$ns" -l app.kubernetes.io/component=yt-rpc-proxy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  fi
  if [[ -z "$pod" ]]; then
    return 1
  fi

  echo "pod/${pod}"
}

kind_cleanup_removed_query_components() {
  local ns="$1"
  local entry
  local name
  local deleted=0

  while IFS= read -r entry; do
    name="${entry##*/}"
    case "$name" in
      qt|qt-*|yqla|yqla-*|yt-query-tracker-init-job-*|yt-strawberry-controller-init-job-*|strawberry-controller|strawberry-controller-*)
        kubectl delete -n "$ns" "$entry" --ignore-not-found=true >/dev/null 2>&1 || true
        deleted=1
        ;;
    esac
  done < <(kubectl get statefulset,pod,job,service -n "$ns" -o name 2>/dev/null || true)

  if (( deleted == 1 )); then
    echo "==> Deleted stale query-oriented resources removed from cluster_v1_flyt_local.yaml"
  fi
}

kind_wait_for_access_targets() {
  local ns="$1"
  local cluster_cr="$2"
  local yts_name="$3"
  local timeout_s="${4:-1200}"
  local http_svc
  local deadline

  http_svc="$(kind_http_service "$cluster_cr" "$yts_name")"
  deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    if kubectl get svc -n "$ns" ytsaurus-ui >/dev/null 2>&1 \
      && kubectl get svc -n "$ns" "$http_svc" >/dev/null 2>&1 \
      && kind_rpc_target "$ns" "$cluster_cr" "$yts_name" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  return 1
}

kind_patch_http_lb_service() {
  local ns="$1"
  local cluster_cr="$2"
  local yts_name="$3"
  local http_svc

  http_svc="$(kind_http_service "$cluster_cr" "$yts_name")"
  if kubectl get svc -n "$ns" "$http_svc" >/dev/null 2>&1; then
    kubectl patch svc "$http_svc" -n "$ns" --type merge -p '{"spec":{"publishNotReadyAddresses":true}}' >/dev/null
  fi
}
