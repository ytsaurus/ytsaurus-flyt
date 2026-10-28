echo "START FLINK MINICLUSTER" 1>&2

# Detect slot IP for Flink Web UI
SLOT_IP=$(get_slot_ip)
if [ -n "$SLOT_IP" ]; then
    UI_HOST=$([[ "$SLOT_IP" == *:* ]] && echo "[$SLOT_IP]" || echo "$SLOT_IP")
    echo "Flink Web UI (cluster-internal only): http://${{UI_HOST}}:{rest_port}" 1>&2
    echo "From your client this URL likely will not open (Pod IPs are not routed on the host)." 1>&2
    echo "Port-forward the exec pod (Kind default is end-0), then use localhost:" 1>&2
    echo "  kubectl port-forward -n default pod/end-0 {rest_port}:{rest_port}" 1>&2
    echo "  # If your exec StatefulSet has another name/index, adjust pod/<name>." 1>&2
    echo "Pipeline must set rest.port={rest_port} and rest.bind-address=0.0.0.0 (see simple_wordcount/pipeline.py) or forward fails." 1>&2
    echo "Then open: http://localhost:{rest_port}" 1>&2
fi

# Stream the MiniCluster's Flink log files to the YT job stderr live (in addition to dumping them
# at the end). Files don't exist yet, so seed an empty one and tail -F follows
# any rotation/creation under FLINK_LOG_DIR.
mkdir -p "$FLINK_LOG_DIR"
: > "$FLINK_LOG_DIR/.flyt-tail-seed"
( tail -F --quiet "$FLINK_LOG_DIR"/*.log "$FLINK_LOG_DIR"/.flyt-tail-seed 2>/dev/null 1>&2 ) &
_FLYT_LOG_TAIL_PID=$!
trap '[ -n "$_FLYT_LOG_TAIL_PID" ] && kill "$_FLYT_LOG_TAIL_PID" 2>/dev/null || true' EXIT

# Run the pipeline script; PyFlink starts the MiniCluster inside it (args from spec: shlex-split + quoted)
set -- {job_args}
"$PYTHON_BIN" "$@" 1>&2
EXIT_CODE=$?
# Give the background tail a moment to flush the last lines, then dump anything
# it might have missed (rotated files, late writes).
sleep 1
kill "$_FLYT_LOG_TAIL_PID" 2>/dev/null || true
cat "$FLINK_LOG_DIR"/* 1>&2 || true
echo "FLINK MINICLUSTER FINISHED (exit code: $EXIT_CODE)" 1>&2
exit $EXIT_CODE
