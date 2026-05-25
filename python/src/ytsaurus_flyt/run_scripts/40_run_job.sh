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

# Stream Flink JM/TM log files to job stderr live (in addition to dumping them
# at the end). Files don't exist yet, so seed an empty one and tail -F follows
# any rotation/creation under FLINK_LOG_DIR.
mkdir -p "$FLINK_LOG_DIR"
: > "$FLINK_LOG_DIR/.flyt-tail-seed"
( tail -F --quiet "$FLINK_LOG_DIR"/*.log "$FLINK_LOG_DIR"/.flyt-tail-seed 2>/dev/null 1>&2 ) &
_FLYT_LOG_TAIL_PID=$!
trap '[ -n "$_FLYT_LOG_TAIL_PID" ] && kill "$_FLYT_LOG_TAIL_PID" 2>/dev/null || true' EXIT

# Run the job command (args from spec: shlex-split + quoted)
set -- {job_args}
"$PYTHON_BIN" "$@" 1>&2
EXIT_CODE=$?
# Give the background tail a moment to flush the last lines, then dump anything
# it might have missed (rotated files, late writes).
sleep 1
kill "$_FLYT_LOG_TAIL_PID" 2>/dev/null || true
cat "$FLINK_LOG_DIR"/* 1>&2 || true
echo "FLINK FINISHED (exit code: $EXIT_CODE)" 1>&2
exit $EXIT_CODE
