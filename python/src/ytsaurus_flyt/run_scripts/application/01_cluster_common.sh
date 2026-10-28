# In-job helper: JobManager discovery through the YT API and operation completion (stdlib only).
cat > "$ROOT_DIR/{job_helper_filename}" <<'FLYT_JOB_HELPER_EOF'
{job_helper}
FLYT_JOB_HELPER_EOF
FLYT_HELPER=("$PYTHON_BIN" "$ROOT_DIR/{job_helper_filename}")

if [ -z "${{YT_OPERATION_ID:-}}" ]; then
    echo "ERROR: YT_OPERATION_ID must be set for application mode" 1>&2
    exit 1
fi

# Cluster configuration assembled by the launcher (flyt defaults, then base_flink_conf_path, then
# flink_config_override and -D), identical for the JobManager and every TaskManager, like a ConfigMap on Kubernetes.
# It replaces the distribution's config.yaml; only per-container addresses stay on the command line.
mkdir -p "$FLINK_CONF_DIR"
# Flink >= 1.19 falls back to the legacy flink-conf.yaml when it exists, which would hide config.yaml.
rm -f "$FLINK_CONF_DIR/flink-conf.yaml"
cat > "$FLINK_CONF_DIR/config.yaml" <<'FLYT_FLINK_CONF_EOF'
{flink_conf_yaml}
FLYT_FLINK_CONF_EOF
echo "Flink cluster configuration ($FLINK_CONF_DIR/config.yaml):" 1>&2
cat "$FLINK_CONF_DIR/config.yaml" 1>&2
# SLOT_IP is the address reachable by clients (Web UI). Job-to-job traffic must use the fastbone
# address where YT provides one: the default (backbone) address is filtered between containers.
SLOT_IP=$(get_slot_ip)
CLUSTER_IP="${{YT_IP_ADDRESS_FASTBONE:-$SLOT_IP}}"
if [ -z "$CLUSTER_IP" ]; then
    CLUSTER_IP="$(hostname -f 2>/dev/null || hostname)"
fi
echo "Slot address: ${{SLOT_IP:-<none>}}; cluster address: $CLUSTER_IP" 1>&2
