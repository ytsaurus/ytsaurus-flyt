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

echo "START FLINK" 1>&2

# Detect slot IP for Flink Web UI (port 27050)
SLOT_IP=$(get_slot_ip)
if [ -n "$SLOT_IP" ]; then
    UI_HOST=$([[ "$SLOT_IP" == *:* ]] && echo "[$SLOT_IP]" || echo "$SLOT_IP")
    echo "Flink Web UI (cluster-internal only): http://${{UI_HOST}}:27050" 1>&2
    echo "From your client this URL likely will not open (Pod IPs are not routed on the host)." 1>&2
    echo "Port-forward the exec pod (Kind default is end-0), then use localhost:" 1>&2
    echo "  kubectl port-forward -n default pod/end-0 27050:27050" 1>&2
    echo "  # If your exec StatefulSet has another name/index, adjust pod/<name>." 1>&2
    echo "Pipeline must set rest.bind-address=0.0.0.0 (see simple_wordcount/pipeline.py) or forward fails." 1>&2
    echo "Then open: http://localhost:27050" 1>&2
fi

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

# Run the job command (args from spec: shlex-split + quoted)
set -- {job_args}
"$PYTHON_BIN" "$@" 1>&2
EXIT_CODE=$?
cat "$FLINK_LOG_DIR"/* 1>&2 || true
echo "FLINK FINISHED (exit code: $EXIT_CODE)" 1>&2
exit $EXIT_CODE
