export PYTHON_BIN={python_bin}
export PYFLINK_CLIENT_EXECUTABLE={python_bin}
export ROOT_DIR="$(pwd -P)"
export FLINK_HOME="$ROOT_DIR/flink"
export FLINK_LIB_DIR="$FLINK_HOME/lib"
export FLINK_OPT_DIR="$FLINK_HOME/opt"
export FLINK_PLUGINS_DIR="$FLINK_HOME/plugins"
export FLINK_LOG_DIR="$FLINK_HOME/log"
export FLINK_CONF_DIR="$FLINK_HOME/conf"
export SERVICE_DIR="$ROOT_DIR/python"

# Directory of the pyflink package in the layer's Python; fails when pyflink cannot be imported.
pyflink_home() {{
    "$PYTHON_BIN" -c "import os, pyflink; print(os.path.dirname(os.path.abspath(pyflink.__file__)))"
}}

# Print where the Flink Web UI of this YT job listens (both modes), with the Kind port-forward hint.
announce_web_ui() {{
    local host="$1"
    [ -n "$host" ] || return 0
    [[ "$host" == *:* ]] && host="[$host]"
    echo "Flink Web UI (cluster-internal only): http://${{host}}:{rest_port}" 1>&2
    echo "From your client this URL likely will not open (Pod IPs are not routed on the host)." 1>&2
    echo "Port-forward the exec pod (Kind default is end-0), then use localhost:" 1>&2
    echo "  kubectl port-forward -n default pod/end-0 {rest_port}:{rest_port}" 1>&2
    echo "  # If your exec StatefulSet has another name/index, adjust pod/<name>." 1>&2
    echo "Then open: http://localhost:{rest_port}" 1>&2
}}

# Address of this YT job for the Flink Web UI; both run modes report it.
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
