#!/usr/bin/env bash
# Podman's default cgroup PID limit (~2048) is too small for YTsaurus on single-node Kind
# (three data nodes + masters + proxies + UI). Without raising it, pods fail with
# "failed to create thread"; with only two data nodes you hit
# "Not enough data nodes available to write chunk" (RF=3).
#
# Run this ONCE on the host before `kind create cluster`, then recreate the Kind cluster
# so the new limit applies to the Kind node container.
#
# Usage: from python/examples/kind: ./scripts/ensure-podman-pids-for-kind.sh
#        ./scripts/ensure-podman-pids-for-kind.sh --print-only
#
set -euo pipefail

PRINT_ONLY=0
if [[ "${1:-}" == "--print-only" ]]; then
  PRINT_ONLY=1
fi

CONFIG="${CONTAINERS_CONF:-${HOME}/.config/containers/containers.conf}"
MIN_PIDS_LIMIT="${MIN_PIDS_LIMIT:-8192}"

ensure_block() {
  local target="$1"
  mkdir -p "$(dirname "$target")"
  if [[ ! -f "$target" ]]; then
    printf '[containers]\npids_limit = 16384\n' >"$target"
    echo "Created $target with pids_limit = 16384"
    return 0
  fi
  if grep -qE '^[[:space:]]*pids_limit[[:space:]]*=' "$target" 2>/dev/null; then
    echo "OK: $target already sets pids_limit."
    grep -E '^[[:space:]]*pids_limit[[:space:]]*=' "$target" || true
    return 0
  fi
  printf '\n# Added by ensure-podman-pids-for-kind.sh (YTsaurus on Kind)\n[containers]\npids_limit = 16384\n' >>"$target"
  echo "Appended [containers] pids_limit = 16384 to $target"
}

if (( PRINT_ONLY == 1 )); then
  echo "Add to ${CONFIG} (or set CONTAINERS_CONF):"
  echo ""
  echo "  [containers]"
  echo "  pids_limit = 16384"
  echo ""
  echo "Then: kind delete cluster --name <name> && kind create cluster --name <name>"
  exit 0
fi

if ! command -v podman >/dev/null 2>&1; then
  echo "podman not found; this helper is for Podman-backed Kind. Skipping." >&2
  exit 0
fi

ensure_block "$CONFIG"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-flyt-local}"
kind_cp="${KIND_CLUSTER_NAME}-control-plane"
container="$(
  podman ps --filter "name=${kind_cp}" --format '{{.Names}}' 2>/dev/null | head -1
)"
if [[ -z "$container" ]]; then
  echo "No running Podman container matching *${kind_cp}*. Set KIND_CLUSTER_NAME to your Kind cluster name (default: flyt-local). After editing containers.conf, recreate the Kind cluster."
  exit 0
fi

cur="$(podman exec "$container" cat /sys/fs/cgroup/pids.current 2>/dev/null || echo "?")"
max="$(podman exec "$container" cat /sys/fs/cgroup/pids.max 2>/dev/null || echo "?")"
echo "Kind node cgroup PIDs: ${cur}/${max}"
if [[ "$max" =~ ^[0-9]+$ ]] && (( max < MIN_PIDS_LIMIT )); then
  echo "WARN: pids.max (${max}) is below ${MIN_PIDS_LIMIT}. Recreate the Kind cluster after fixing containers.conf." >&2
  exit 1
fi
