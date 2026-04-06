# SquashFS layout: python-runtime/ in sandbox root
if [ -d "$ROOT_DIR/python-runtime" ]; then
    export PYTHONPATH="$ROOT_DIR/python-runtime:${{PYTHONPATH:-}}"
fi

# Set up flink/ directories (discover from PyFlink if not in layer)
if [ ! -d "$FLINK_LIB_DIR" ]; then
    PYFLINK_HOME=$("$PYTHON_BIN" -c "import os, pyflink; print(os.path.dirname(os.path.abspath(pyflink.__file__)))" 2>/dev/null || true)
    if [ -n "$PYFLINK_HOME" ] && [ -d "$PYFLINK_HOME/lib" ]; then
        echo "Discovering Flink from PyFlink at $PYFLINK_HOME" 1>&2
        mkdir -p "$FLINK_LIB_DIR" && cp -r "$PYFLINK_HOME/lib/"* "$FLINK_LIB_DIR"
        mkdir -p "$FLINK_OPT_DIR" && cp -r "$PYFLINK_HOME/opt/"* "$FLINK_OPT_DIR"
        mkdir -p "$FLINK_PLUGINS_DIR" && cp -r "$PYFLINK_HOME/plugins/"* "$FLINK_PLUGINS_DIR"
        mkdir -p "$FLINK_LOG_DIR" && cp -r "$PYFLINK_HOME/log/"* "$FLINK_LOG_DIR" 2>/dev/null || true
        mkdir -p "$FLINK_CONF_DIR" && cp -r "$PYFLINK_HOME/conf/"* "$FLINK_CONF_DIR" 2>/dev/null || true
    else
        echo "ERROR: flink/lib not found and PyFlink not available via $PYTHON_BIN" 1>&2
        exit 1
    fi
fi
