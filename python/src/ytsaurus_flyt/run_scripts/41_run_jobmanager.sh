echo "START FLINK JOBMANAGER (application mode)" 1>&2

JM_HOST="$CLUSTER_IP"
REST_HOST="${{SLOT_IP:-$CLUSTER_IP}}"
UI_HOST=$([[ "$REST_HOST" == *:* ]] && echo "[$REST_HOST]" || echo "$REST_HOST")
echo "Flink Web UI (cluster-internal only): http://${{UI_HOST}}:{rest_port}" 1>&2

set -- {job_args}
JOB_SCRIPT="$1"
shift

_FLYT_HB_PID=""
_flyt_stop_heartbeat() {{
    if [ -n "$_FLYT_HB_PID" ]; then
        kill "$_FLYT_HB_PID" 2>/dev/null || true
        _FLYT_HB_PID=""
    fi
}}
trap _flyt_stop_heartbeat EXIT

while true; do
    "${{FLYT_HELPER[@]}}" publish --path "$FLYT_DISCOVERY_NODE" --host "$JM_HOST" --ui-host "$REST_HOST" \
        --rpc-port {rpc_port} --rest-port {rest_port} 1>&2
    "${{FLYT_HELPER[@]}}" heartbeat --path "$FLYT_DISCOVERY_NODE" --parent-pid $$ 1>&2 &
    _FLYT_HB_PID=$!

    # Cluster options first, then the driver's own arguments (PythonDriver parses -py and the rest).
    "$FLINK_HOME/bin/standalone-job.sh" start-foreground \
        "${{FLYT_FLINK_ARGS[@]}}" \
        "-Djobmanager.rpc.address=$JM_HOST" "-Drest.address=$REST_HOST" \
        --job-classname org.apache.flink.client.python.PythonDriver \
        -pyclientexec "$PYTHON_BIN" -pyexec "$PYTHON_BIN" -py "$JOB_SCRIPT" "$@" 1>&2 \
        && EXIT_CODE=0 || EXIT_CODE=$?
    _flyt_stop_heartbeat
    echo "FLINK JOBMANAGER FINISHED (exit code: $EXIT_CODE)" 1>&2

    if [ "$EXIT_CODE" -ne 0 ]; then
        # A failed gang job: YT restarts the whole cluster with a new incarnation.
        exit "$EXIT_CODE"
    fi
    if [ "{restart_completed_jobs}" = "1" ]; then
        echo "restart_completed_jobs: re-running the pipeline in place" 1>&2
        sleep 5
        continue
    fi
    "${{FLYT_HELPER[@]}}" remove --path "$FLYT_DISCOVERY_NODE" 1>&2 || true
    # TaskManagers never exit on their own; completing the operation aborts them.
    "${{FLYT_HELPER[@]}}" complete-operation --operation-id "$YT_OPERATION_ID" 1>&2
    exit 0
done
