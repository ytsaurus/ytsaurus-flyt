echo "START FLINK JOBMANAGER (application mode)" 1>&2

JM_HOST="$CLUSTER_IP"
REST_HOST="${{SLOT_IP:-$CLUSTER_IP}}"
UI_HOST=$([[ "$REST_HOST" == *:* ]] && echo "[$REST_HOST]" || echo "$REST_HOST")
echo "Flink Web UI (cluster-internal only): http://${{UI_HOST}}:{rest_port}" 1>&2

set -- {job_args}
JOB_SCRIPT="$1"
shift

# TaskManagers discover this JobManager through the YT API (list_jobs -> job addresses ->
# /jobmanager/config), so jobmanager.rpc.address must be the address they can reach.
# Per-container values first (the rest is in config.yaml): addresses, and the YT job id that
# TaskManagers check in /jobmanager/config when they probe a shared node address. Then the
# driver's own arguments (PythonDriver parses -py and the rest; the Python executables come from config.yaml).
"$FLINK_HOME/bin/standalone-job.sh" start-foreground \
    "-Djobmanager.rpc.address=$JM_HOST" "-Drest.address=$REST_HOST" \
    "-Dflyt.yt.job-id=$YT_JOB_ID" \
    --job-classname org.apache.flink.client.python.PythonDriver \
    -py "$JOB_SCRIPT" "$@" 1>&2 \
    && EXIT_CODE=0 || EXIT_CODE=$?
echo "FLINK JOBMANAGER FINISHED (exit code: $EXIT_CODE)" 1>&2

if [ "$EXIT_CODE" -ne 0 ]; then
    # A failed gang job: YT restarts the whole cluster with a new incarnation.
    exit "$EXIT_CODE"
fi
# The driver finished (bounded sources, cancel or stop from the Web UI). TaskManagers never exit
# on their own, so completing the operation is what ends them.
"${{FLYT_HELPER[@]}}" complete-operation --operation-id "$YT_OPERATION_ID" 1>&2
exit 0
