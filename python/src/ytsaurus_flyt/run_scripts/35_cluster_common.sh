get_slot_ip() {{
    # Try to get the IP address from YT environment variable
    local ip="${{YT_IP_ADDRESS_DEFAULT:-}}"
    if [ -n "$ip" ]; then
        echo "$ip"
        return 0
    fi

    # Try IPv6 from veth0
    ip="$(ip -o -6 addr show dev veth0 scope global 2>/dev/null | awk '{{print $4}}' | cut -d/ -f1 | head -1)"
    if [ -n "$ip" ]; then
        echo "$ip"
        return 0
    fi

    # Fallback to hostname resolution
    getent hosts "$(hostname -f 2>/dev/null || hostname)" 2>/dev/null | awk '{{print $1}}' | head -1
}}

cd "$SERVICE_DIR"
export PYTHONPATH="$PYTHONPATH:$SERVICE_DIR"

# Credentials from secure_vault (YT injects YT_SECURE_VAULT_<KEY> for each vault key)
if command -v compgen >/dev/null 2>&1; then
  for _flyt_var in $(compgen -e | grep '^YT_SECURE_VAULT_' || true); do
    _flyt_key="${{_flyt_var#YT_SECURE_VAULT_}}"
    _flyt_val="$(printenv "${{_flyt_var}}")"
    export "${{_flyt_key}}=${{_flyt_val}}"
  done
fi
unset _flyt_var _flyt_key _flyt_val 2>/dev/null || true

export SERVICE_NAME="{service_name}"

# In-job helper: JobManager discovery through the YT API and operation completion (stdlib only).
cat > "$ROOT_DIR/{job_helper_filename}" <<'FLYT_JOB_HELPER_EOF'
{job_helper}
FLYT_JOB_HELPER_EOF
FLYT_HELPER=("$PYTHON_BIN" "$ROOT_DIR/{job_helper_filename}")

if [ -z "${{YT_OPERATION_ID:-}}" ]; then
    echo "ERROR: YT_OPERATION_ID must be set for application mode" 1>&2
    exit 1
fi

# Flink options shared by both roles (memory, ports, slots, restart strategy, flink_config overrides).
FLYT_FLINK_ARGS=({flink_args})
# SLOT_IP is the address reachable by clients (Web UI). Job-to-job traffic must use the fastbone
# address where YT provides one: the default (backbone) address is filtered between containers.
SLOT_IP=$(get_slot_ip)
CLUSTER_IP="${{YT_IP_ADDRESS_FASTBONE:-$SLOT_IP}}"
if [ -z "$CLUSTER_IP" ]; then
    CLUSTER_IP="$(hostname -f 2>/dev/null || hostname)"
fi
echo "Slot address: ${{SLOT_IP:-<none>}}; cluster address: $CLUSTER_IP" 1>&2
