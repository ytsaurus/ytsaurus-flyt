# SquashFS layout: python-runtime/ may appear either at the filesystem
# root (Porto layer_paths mount at /) or inside the sandbox directory
# (sandbox_unpack extracts into $ROOT_DIR).  Check both locations.
_VENV_DIR=""
if [ -d "$ROOT_DIR/python-runtime" ]; then
    _VENV_DIR="$ROOT_DIR/python-runtime"
elif [ -d "/python-runtime" ]; then
    _VENV_DIR="/python-runtime"
fi

if [ -n "$_VENV_DIR" ]; then
    # If the directory is a full virtualenv (has lib/pythonX.Y/site-packages),
    # add its site-packages to PYTHONPATH.  Otherwise treat it as a flat
    # pip-install --target directory (legacy layout).
    _sp=$(find "$_VENV_DIR/lib" -maxdepth 2 -type d -name site-packages 2>/dev/null | head -1)
    if [ -n "$_sp" ]; then
        export PYTHONPATH="${{_sp}}:${{PYTHONPATH:-}}"
    else
        export PYTHONPATH="${{_VENV_DIR}}:${{PYTHONPATH:-}}"
    fi
    unset _sp
fi
unset _VENV_DIR

# Set up flink/ directories (discover from PyFlink if not in layer)
if [ ! -d "$FLINK_LIB_DIR" ]; then
    _pyflink_err=$("$PYTHON_BIN" -c "import os, pyflink; print(os.path.dirname(os.path.abspath(pyflink.__file__)))" 2>&1)
    _pyflink_rc=$?
    if [ $_pyflink_rc -eq 0 ]; then
        PYFLINK_HOME="$_pyflink_err"
    else
        PYFLINK_HOME=""
    fi

    if [ -n "$PYFLINK_HOME" ] && [ -d "$PYFLINK_HOME/lib" ]; then
        echo "Discovering Flink from PyFlink at $PYFLINK_HOME" 1>&2
        mkdir -p "$FLINK_LIB_DIR" && cp -r "$PYFLINK_HOME/lib/"* "$FLINK_LIB_DIR"
        mkdir -p "$FLINK_OPT_DIR" && cp -r "$PYFLINK_HOME/opt/"* "$FLINK_OPT_DIR"
        mkdir -p "$FLINK_PLUGINS_DIR" && cp -r "$PYFLINK_HOME/plugins/"* "$FLINK_PLUGINS_DIR"
        mkdir -p "$FLINK_LOG_DIR" && cp -r "$PYFLINK_HOME/log/"* "$FLINK_LOG_DIR" 2>/dev/null || true
        mkdir -p "$FLINK_CONF_DIR" && cp -r "$PYFLINK_HOME/conf/"* "$FLINK_CONF_DIR" 2>/dev/null || true
    else
        # Discovery failed — print diagnostics to help the user debug.
        echo "ERROR: flink/lib not found and PyFlink discovery failed." 1>&2
        echo "  PYTHON_BIN=$PYTHON_BIN" 1>&2
        echo "  PYTHONPATH=$PYTHONPATH" 1>&2
        echo "  ROOT_DIR=$ROOT_DIR" 1>&2
        if [ $_pyflink_rc -ne 0 ]; then
            echo "  pyflink import failed (rc=$_pyflink_rc): $_pyflink_err" 1>&2
        fi
        if [ ! -f "$PYTHON_BIN" ]; then
            echo "  $PYTHON_BIN does not exist." 1>&2
            ls -la /usr/bin/python* 1>&2 2>&1 || echo "  no /usr/bin/python*" 1>&2
        fi
        if [ ! -d "$ROOT_DIR/python-runtime" ] && [ ! -d "/python-runtime" ]; then
            echo "  python-runtime/ not found at $ROOT_DIR or /." 1>&2
            echo "  Sandbox contents:" 1>&2
            ls "$ROOT_DIR/" 1>&2 2>&1
        fi
        echo "  Ensure your layer contains pyflink in python-runtime/, or" 1>&2
        echo "  pre-populate flink/lib/ in your layer." 1>&2
        exit 1
    fi
    unset _pyflink_err _pyflink_rc
fi
