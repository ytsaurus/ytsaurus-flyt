# In-job helper: JobManager discovery through the YT API and operation completion (stdlib only).
cat > "$ROOT_DIR/{job_helper_filename}" <<'FLYT_JOB_HELPER_EOF'
{job_helper}
FLYT_JOB_HELPER_EOF
FLYT_HELPER=("$PYTHON_BIN" "$ROOT_DIR/{job_helper_filename}")

if [ -z "${{YT_OPERATION_ID:-}}" ]; then
    echo "ERROR: YT_OPERATION_ID must be set for application mode" 1>&2
    exit 1
fi

# PythonEnvUtils logs the driver's full environment, secure_vault secrets included, at INFO.
cat >> "$FLINK_CONF_DIR/log4j-console.properties" <<'FLYT_LOG4J_EOF'

logger.flytPythonEnv.name = org.apache.flink.client.python.PythonEnvUtils
logger.flytPythonEnv.level = WARN
FLYT_LOG4J_EOF
# SLOT_IP is the address reachable by clients (Web UI). Job-to-job traffic must use the fastbone
# address where YT provides one: the default (backbone) address is filtered between containers.
SLOT_IP=$(get_slot_ip)
CLUSTER_IP="${{YT_IP_ADDRESS_FASTBONE:-$SLOT_IP}}"
if [ -z "$CLUSTER_IP" ]; then
    CLUSTER_IP="$(hostname -f 2>/dev/null || hostname)"
fi
echo "Slot address: ${{SLOT_IP:-<none>}}; cluster address: $CLUSTER_IP" 1>&2
