#!/usr/bin/env bash
# YTsaurus on Kind. https://ytsaurus.tech/docs/en/overview/try-yt
# Usage: from python/examples/kind: ./scripts/bootstrap.sh
#        (kubectl context = Kind cluster; optional KIND_CLUSTER_NAME=flyt-local)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIND_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MIN_INOTIFY_INSTANCES="${MIN_INOTIFY_INSTANCES:-512}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-default}"
CERT_MANAGER_NAMESPACE="${CERT_MANAGER_NAMESPACE:-cert-manager}"
CERT_MANAGER_TIMEOUT="${CERT_MANAGER_TIMEOUT:-300}"
OPERATOR_WEBHOOK_SERVICE="${OPERATOR_WEBHOOK_SERVICE:-ytsaurus-ytop-chart-webhook-service}"
OPERATOR_WEBHOOK_TIMEOUT="${OPERATOR_WEBHOOK_TIMEOUT:-180}"
OPERATOR_HELM_RETRIES="${OPERATOR_HELM_RETRIES:-3}"

# shellcheck source=./kind-common.sh
source "$SCRIPT_DIR/kind-common.sh"

# Helm is often installed to ~/.local/bin; interactive shells may not have it in PATH.
PATH="${HOME}/.local/bin:${PATH}"
if [[ -d "${HOME}/.linuxbrew/bin" ]]; then PATH="${HOME}/.linuxbrew/bin:${PATH}"; fi
if [[ -d /snap/bin ]]; then PATH="/snap/bin:${PATH}"; fi
export PATH

need_inotify() {
  local cur
  cur="$(cat /proc/sys/fs/inotify/max_user_instances 2>/dev/null || echo 0)"
  if [[ "$cur" -lt "$MIN_INOTIFY_INSTANCES" ]]; then
    echo "ERROR: fs.inotify.max_user_instances is $cur (need >= $MIN_INOTIFY_INSTANCES)." >&2
    echo "On Linux + Podman/Kind this often breaks the YTsaurus operator (informers / 'too many open files')." >&2
    echo >&2
    echo "Fix (one-shot):" >&2
    echo "  sudo sysctl -w fs.inotify.max_user_instances=8192" >&2
    echo "  sudo sysctl -w fs.inotify.max_user_watches=524288" >&2
    echo >&2
    echo "Or install persistently:" >&2
    echo "  sudo cp $KIND_ROOT/sysctl-flyt-kind.conf /etc/sysctl.d/99-flyt-kind.conf" >&2
    echo "  sudo sysctl --system" >&2
    exit 1
  fi
}

ensure_kubernetes_api_ready() {
  local context
  local cluster_name

  context="$(kubectl config current-context 2>/dev/null || true)"
  if kubectl version --request-timeout=10s >/dev/null 2>&1; then
    return 0
  fi

  echo "ERROR: Kubernetes API is not reachable for context ${context:-<unknown>}." >&2
  if [[ "$context" == kind-* ]]; then
    cluster_name="${context#kind-}"
    echo "The Kind node container may have exited. Recreate the cluster if it is no longer running:" >&2
    echo "  kind delete cluster --name ${cluster_name}" >&2
    echo "  kind create cluster --name ${cluster_name}" >&2
  fi
  return 1
}

upgrade_operator_chart() {
  local attempt

  for (( attempt = 1; attempt <= OPERATOR_HELM_RETRIES; attempt++ )); do
    if ! kind_ensure_cert_manager_ready "$CERT_MANAGER_NAMESPACE" "$CERT_MANAGER_TIMEOUT"; then
      if (( attempt == OPERATOR_HELM_RETRIES )); then
        break
      fi
      echo "WARN: cert-manager is not ready; restarting webhook stack and retrying..." >&2
      kubectl rollout restart deploy/cert-manager -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
      kubectl rollout restart deploy/cert-manager-cainjector -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
      kubectl rollout restart deploy/cert-manager-webhook -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
      sleep 5
      continue
    fi

    echo "==> Helm: upgrading YTsaurus operator (chart 0.28.0 + values fixes), attempt ${attempt}/${OPERATOR_HELM_RETRIES}..."
    if helm upgrade --install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version 0.28.0 \
      --reset-values \
      -f "$KIND_ROOT/manifests/ytop-chart-values.yaml"; then
      return 0
    fi

    if (( attempt == OPERATOR_HELM_RETRIES )); then
      break
    fi

    echo "WARN: helm upgrade failed; restarting cert-manager webhook stack and retrying..." >&2
    kubectl rollout restart deploy/cert-manager -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
    kubectl rollout restart deploy/cert-manager-cainjector -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
    kubectl rollout restart deploy/cert-manager-webhook -n "$CERT_MANAGER_NAMESPACE" >/dev/null 2>&1 || true
    sleep 5
  done

  echo "ERROR: failed to install or upgrade the YTsaurus operator after ${OPERATOR_HELM_RETRIES} attempts." >&2
  return 1
}

need_inotify
kind_need_cmd kubectl
ensure_kubernetes_api_ready
kind_warn_podman_pid_pressure

if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm not found in PATH." >&2
  echo "Install Helm (without sudo into ~/.local/bin), then re-run:" >&2
  echo '  mkdir -p "$HOME/.local/bin"' >&2
  echo '  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | HELM_INSTALL_DIR="$HOME/.local/bin" USE_SUDO=false bash' >&2
  echo "Or: sudo apt install helm  (if available)" >&2
  echo "And add to ~/.bashrc if needed: export PATH=\"\$HOME/.local/bin:\$PATH\"" >&2
  exit 1
fi

echo "==> kubectl context: $(kubectl config current-context)"
upgrade_operator_chart

echo "==> Waiting for operator Deployment (rollout)..."
if ! kubectl rollout status deploy -l control-plane=controller-manager --timeout=5m; then
  echo "ERROR: operator Deployment did not become ready." >&2
  echo "If logs show 'too many open files' or RemoteYtsaurus informer timeouts, raise host inotify limits (see README step 0)." >&2
  kubectl get pods -l control-plane=controller-manager
  kubectl logs -l control-plane=controller-manager -c manager --tail=40 >&2 || true
  exit 1
fi
kubectl get pods -l control-plane=controller-manager
echo "==> Waiting for operator webhook endpoints..."
kind_wait_for_service_endpoints "$KUBE_NAMESPACE" "$OPERATOR_WEBHOOK_SERVICE" "$OPERATOR_WEBHOOK_TIMEOUT" "operator webhook"

CLUSTER_CR="$KIND_ROOT/manifests/cluster_v1_flyt_local.yaml"
kind_require_cluster_cr "$CLUSTER_CR"
echo "==> Applying Ytsaurus cluster CR (exec node sized for FLYT MICRO)..."
kubectl apply -f "$CLUSTER_CR"
kind_cleanup_removed_query_components "$KUBE_NAMESPACE"

YTS_NAME="$(kind_cluster_name "$CLUSTER_CR")"
HTTP_SVC="$(kind_http_service "$CLUSTER_CR" "$YTS_NAME")"

echo "==> Waiting for UI, HTTP proxy, RPC target (up to ~20 min on slow hosts)..."
if ! kind_wait_for_access_targets "$KUBE_NAMESPACE" "$CLUSTER_CR" "$YTS_NAME" 1200; then
  echo "ERROR: timed out waiting for YTsaurus access targets." >&2
  echo "Check: kubectl get ytsaurus \"$YTS_NAME\" -n \"$KUBE_NAMESPACE\" -o yaml" >&2
  kubectl get svc -n "$KUBE_NAMESPACE" 2>/dev/null || true
  kubectl get pods -n "$KUBE_NAMESPACE" 2>/dev/null || true
  exit 1
fi

echo "Access targets are ready."
kubectl get svc -n "$KUBE_NAMESPACE" ytsaurus-ui "$HTTP_SVC"
echo "RPC target: $(kind_rpc_target "$KUBE_NAMESPACE" "$CLUSTER_CR" "$YTS_NAME")"

# Without ready endpoints, kube-proxy refuses ClusterIP connections; the UI pod then fails server-side
# requests (e.g. /api/cluster-info) with ECONNREFUSED to 10.96.x.x:80. Publishing not-ready addresses
# keeps traffic routable while the HTTP proxy is still failing readiness (common on overloaded Kind).
echo "==> Patching ${HTTP_SVC} (publishNotReadyAddresses) for UI to proxy connectivity..."
if kubectl get svc "$HTTP_SVC" -n "$KUBE_NAMESPACE" >/dev/null 2>&1; then
  kind_patch_http_lb_service "$KUBE_NAMESPACE" "$CLUSTER_CR" "$YTS_NAME" \
    && echo "    OK: $HTTP_SVC" || echo "    WARN: patch failed (ignore if Service is managed differently)." >&2
fi

echo "==> Done."

if [[ "${FLYT_KIND_AUTO_FORWARD:-0}" != "1" ]]; then
  echo "Next: start UI + HTTP + RPC forwards in one shell (ports match spec.ui.externalProxy):"
  echo "  $KIND_ROOT/scripts/port-forward-ui.sh"
  echo "Then load the FLYT image (see README) and run flyt with --proxy localhost:50005"
fi
