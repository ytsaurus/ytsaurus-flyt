# After layer mount/unpack: python-runtime + flink/ under $ROOT_DIR.
export PYTHONPATH="$ROOT_DIR/python-runtime:${{PYTHONPATH:-}}"

if [ ! -d "$FLINK_LIB_DIR" ]; then
    echo "SquashFS layer missing flink/lib at $FLINK_LIB_DIR" 1>&2
    exit 1
fi
