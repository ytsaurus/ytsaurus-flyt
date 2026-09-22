echo "JARs in sandbox:" 1>&2
ls *.jar 2>/dev/null 1>&2 || echo "No *.jar found in $(pwd)" 1>&2

for jar in *.jar; do
    if [ "$jar" != "*.jar" ]; then
        mv "$jar" "$FLINK_LIB_DIR"
    fi
done

# Files shipped as plugins/<name>/*.jar become Flink plugins (own classloader), not shared libs.
if [ -d plugins ]; then
    mkdir -p "$FLINK_PLUGINS_DIR"
    cp -r plugins/. "$FLINK_PLUGINS_DIR"/
    echo "Plugins in FLINK_PLUGINS_DIR:" 1>&2
    ls -R "$FLINK_PLUGINS_DIR" 2>/dev/null | head -20 1>&2
fi

echo "JARs in FLINK_LIB_DIR:" 1>&2
ls "$FLINK_LIB_DIR"/*.jar 2>/dev/null | head -20 1>&2
