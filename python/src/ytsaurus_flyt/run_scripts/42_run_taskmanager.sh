echo "START FLINK TASKMANAGER (application mode)" 1>&2

# Block until the running JobManager job of this incarnation advertises its RPC endpoint.
# Prints "<job_id> <host> <port>"; a timeout fails this job (YT restarts it, counted in max_failed_job_count).
JM_ENDPOINT=$("${{FLYT_HELPER[@]}}" wait-jobmanager --operation-id "$YT_OPERATION_ID" \
    --incarnation "${{YT_OPERATION_INCARNATION:-}}" --rest-port {rest_port} --timeout {discovery_timeout})
read -r JM_JOB_ID JM_HOST JM_RPC_PORT <<< "$JM_ENDPOINT"
echo "JobManager job $JM_JOB_ID discovered at $JM_HOST:$JM_RPC_PORT" 1>&2

# Flink derives the default ResourceID from taskmanager.host, and an IPv6 literal puts brackets into
# the MetricQueryService actor name (InvalidActorNameException): no TaskManager metrics in the Web UI.
# The YT job id is a valid actor name and matches the job in the YT UI.
TM_ARGS=("-Djobmanager.rpc.address=$JM_HOST" "-Djobmanager.rpc.port=$JM_RPC_PORT" "-Dtaskmanager.host=$CLUSTER_IP"
    "-Dtaskmanager.resource-id=${{YT_JOB_ID:-tm-$(hostname)-$$}}")

# Not exec'ed so the EXIT trap can stop the sidecar when the TaskManager goes down.
"$FLINK_HOME/bin/taskmanager.sh" start-foreground "${{FLYT_FLINK_ARGS[@]}}" "${{TM_ARGS[@]}}" 1>&2 \
    && EXIT_CODE=0 || EXIT_CODE=$?
echo "FLINK TASKMANAGER FINISHED (exit code: $EXIT_CODE)" 1>&2
exit "$EXIT_CODE"
