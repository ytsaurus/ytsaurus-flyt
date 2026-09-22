echo "START FLINK TASKMANAGER (application mode)" 1>&2

# Block until the JobManager of this incarnation is published and answers on its REST port.
JM_ENDPOINT=$("${{FLYT_HELPER[@]}}" wait-jobmanager --path "$FLYT_DISCOVERY_NODE" \
    --incarnation "${{YT_OPERATION_INCARNATION:-}}" --timeout {discovery_timeout})
JM_HOST="${{JM_ENDPOINT%% *}}"
JM_RPC_PORT="${{JM_ENDPOINT##* }}"
echo "JobManager discovered at $JM_HOST:$JM_RPC_PORT" 1>&2

TM_ARGS=("-Djobmanager.rpc.address=$JM_HOST" "-Djobmanager.rpc.port=$JM_RPC_PORT" "-Dtaskmanager.host=$CLUSTER_IP")

exec "$FLINK_HOME/bin/taskmanager.sh" start-foreground "${{FLYT_FLINK_ARGS[@]}}" "${{TM_ARGS[@]}}" 1>&2
