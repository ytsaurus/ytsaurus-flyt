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

# Set up flink/ directories: from the layer mounted at / (layer_paths), else from PyFlink.
# The sandbox may be tmpfs, which counts against the memory limit: link the distribution, don't copy it.
if [ ! -d "$FLINK_LIB_DIR" ]; then
    _pyflink_err=""
    _pyflink_rc=0
    if [ -d "/flink/lib" ]; then
        _flink_dist="/flink"
    else
        _pyflink_err=$(pyflink_home 2>&1)
        _pyflink_rc=$?
        if [ $_pyflink_rc -eq 0 ]; then
            _flink_dist="$_pyflink_err"
        else
            _flink_dist=""
        fi
    fi

    if [ -n "$_flink_dist" ] && [ -d "$_flink_dist/lib" ]; then
        echo "Linking Flink from $_flink_dist" 1>&2
        # lib/, conf/ and log/ stay writable: jars and config.yaml are added there. opt/ is a real
        # directory too: Flink finds flink-python there with a walk that does not follow a linked root.
        mkdir -p "$FLINK_LIB_DIR" "$FLINK_OPT_DIR" "$FLINK_LOG_DIR" "$FLINK_CONF_DIR"
        ln -s "$_flink_dist/lib/"* "$FLINK_LIB_DIR"/
        ln -s "$_flink_dist/opt/"* "$FLINK_OPT_DIR"/ 2>/dev/null || true
        [ -d "$_flink_dist/plugins" ] && ln -s "$_flink_dist/plugins" "$FLINK_PLUGINS_DIR"
        cp -r "$_flink_dist/conf/"* "$FLINK_CONF_DIR" 2>/dev/null || true
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
            ls -la /usr/bin/python* 1>&2 || echo "  no /usr/bin/python*" 1>&2
        fi
        if [ ! -d "$ROOT_DIR/python-runtime" ] && [ ! -d "/python-runtime" ]; then
            echo "  python-runtime/ not found at $ROOT_DIR or /." 1>&2
            echo "  Sandbox contents:" 1>&2
            ls "$ROOT_DIR/" 1>&2
        fi
        echo "  Ensure your layer contains pyflink in python-runtime/, or" 1>&2
        echo "  pre-populate flink/lib/ in your layer." 1>&2
        exit 1
    fi
    unset _pyflink_err _pyflink_rc _flink_dist
fi
