# Optional per-container helper (FlytConfig.sidecar_command), e.g. a metrics agent that every
# Flink JVM pushes to on localhost. Runs in the background from $SERVICE_DIR with secrets exported.
FLYT_SIDECAR_COMMAND={sidecar_command}
_FLYT_SIDECAR_PID=""
if [ -n "$FLYT_SIDECAR_COMMAND" ]; then
    echo "Starting sidecar: $FLYT_SIDECAR_COMMAND" 1>&2
    bash -c "$FLYT_SIDECAR_COMMAND" 1>&2 &
    _FLYT_SIDECAR_PID=$!
fi
_flyt_stop_sidecar() {{
    if [ -n "$_FLYT_SIDECAR_PID" ]; then
        kill "$_FLYT_SIDECAR_PID" 2>/dev/null || true
        _FLYT_SIDECAR_PID=""
    fi
}}
trap _flyt_stop_sidecar EXIT
