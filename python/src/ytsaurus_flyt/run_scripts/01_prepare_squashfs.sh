# SquashFS layout: python-runtime/ in sandbox root.
# If the directory is a full virtualenv (has lib/pythonX.Y/site-packages),
# add its site-packages to PYTHONPATH.  Otherwise treat it as a flat
# pip-install --target directory (legacy layout).
if [ -d "$ROOT_DIR/python-runtime" ]; then
    _sp=$(find "$ROOT_DIR/python-runtime/lib" -maxdepth 2 -type d -name site-packages 2>/dev/null | head -1)
    if [ -n "$_sp" ]; then
        export PYTHONPATH="${{_sp}}:${{PYTHONPATH:-}}"
    else
        export PYTHONPATH="$ROOT_DIR/python-runtime:${{PYTHONPATH:-}}"
    fi
    unset _sp
fi

# Set up flink/ directories (discover from PyFlink if not in layer)
if [ ! -d "$FLINK_LIB_DIR" ]; then
    echo "flink/lib not present in layer, trying PyFlink discovery..." 1>&2
    echo "  PYTHON_BIN=$PYTHON_BIN" 1>&2
    echo "  PYTHONPATH=$PYTHONPATH" 1>&2
    echo "  ROOT_DIR=$ROOT_DIR" 1>&2

    # Check that PYTHON_BIN actually exists
    if [ ! -f "$PYTHON_BIN" ]; then
        echo "  WARNING: $PYTHON_BIN does not exist" 1>&2
        echo "  Checking what python is available:" 1>&2
        which python3 1>&2 2>&1 || echo "    python3: not found" 1>&2
        which python3.9 1>&2 2>&1 || echo "    python3.9: not found" 1>&2
        which python3.11 1>&2 2>&1 || echo "    python3.11: not found" 1>&2
        ls -la /usr/bin/python* 1>&2 2>&1 || echo "    no /usr/bin/python*" 1>&2
        ls -la "$ROOT_DIR/python-runtime/bin/"python* 1>&2 2>&1 || echo "    no python-runtime/bin/python*" 1>&2
    else
        echo "  $PYTHON_BIN exists: $(file "$PYTHON_BIN" 2>&1)" 1>&2
    fi

    # Show python-runtime layout for debugging
    if [ -d "$ROOT_DIR/python-runtime" ]; then
        echo "  python-runtime/ top-level entries:" 1>&2
        ls "$ROOT_DIR/python-runtime/" 1>&2 2>&1
        if [ -d "$ROOT_DIR/python-runtime/lib" ]; then
            echo "  python-runtime/lib/ entries:" 1>&2
            ls "$ROOT_DIR/python-runtime/lib/" 1>&2 2>&1
        fi
    else
        echo "  WARNING: $ROOT_DIR/python-runtime/ does not exist" 1>&2
        echo "  Sandbox contents:" 1>&2
        ls "$ROOT_DIR/" 1>&2 2>&1
    fi

    _pyflink_err=$("$PYTHON_BIN" -c "import os, pyflink; print(os.path.dirname(os.path.abspath(pyflink.__file__)))" 2>&1)
    _pyflink_rc=$?
    if [ $_pyflink_rc -eq 0 ]; then
        PYFLINK_HOME="$_pyflink_err"
    else
        PYFLINK_HOME=""
        echo "  pyflink import failed (rc=$_pyflink_rc): $_pyflink_err" 1>&2
    fi
    unset _pyflink_err _pyflink_rc

    if [ -n "$PYFLINK_HOME" ] && [ -d "$PYFLINK_HOME/lib" ]; then
        echo "Discovering Flink from PyFlink at $PYFLINK_HOME" 1>&2
        mkdir -p "$FLINK_LIB_DIR" && cp -r "$PYFLINK_HOME/lib/"* "$FLINK_LIB_DIR"
        mkdir -p "$FLINK_OPT_DIR" && cp -r "$PYFLINK_HOME/opt/"* "$FLINK_OPT_DIR"
        mkdir -p "$FLINK_PLUGINS_DIR" && cp -r "$PYFLINK_HOME/plugins/"* "$FLINK_PLUGINS_DIR"
        mkdir -p "$FLINK_LOG_DIR" && cp -r "$PYFLINK_HOME/log/"* "$FLINK_LOG_DIR" 2>/dev/null || true
        mkdir -p "$FLINK_CONF_DIR" && cp -r "$PYFLINK_HOME/conf/"* "$FLINK_CONF_DIR" 2>/dev/null || true
    else
        echo "ERROR: flink/lib not found and PyFlink not available via $PYTHON_BIN" 1>&2
        echo "  Ensure the SquashFS layer contains pyflink in python-runtime/, or" 1>&2
        echo "  pre-populate flink/lib/ in your layer." 1>&2
        exit 1
    fi
fi
