echo "JARs in sandbox:" 1>&2
ls *.jar 2>/dev/null 1>&2 || echo "No *.jar found in $(pwd)" 1>&2

for jar in *.jar; do
    if [ "$jar" != "*.jar" ]; then
        mv "$jar" "$FLINK_LIB_DIR"
    fi
done

echo "JARs in FLINK_LIB_DIR:" 1>&2
ls "$FLINK_LIB_DIR"/*.jar 2>/dev/null | head -20 1>&2
