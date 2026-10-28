# Application mode needs the Flink launcher scripts and the flink-python jar, which
# common/02_prepare_squashfs.sh does not stage (the MiniCluster never uses them).
if [ ! -f "$FLINK_HOME/bin/standalone-job.sh" ] || [ ! -f "$FLINK_HOME/bin/taskmanager.sh" ]; then
    _pyflink_home=$(pyflink_home)
    if [ ! -d "$_pyflink_home/bin" ]; then
        echo "ERROR: $_pyflink_home/bin not found; the PyFlink distribution must ship the Flink bin/ scripts." 1>&2
        exit 1
    fi
    mkdir -p "$FLINK_HOME/bin" && cp -r "$_pyflink_home/bin/"* "$FLINK_HOME/bin"
    unset _pyflink_home
fi
chmod +x "$FLINK_HOME"/bin/*.sh 2>/dev/null || true

# The JobManager's PythonDriver and the TaskManager's Python UDF runner need flink-python on the
# classpath; the distribution keeps it in opt/.
for _jar in "$FLINK_OPT_DIR"/flink-python*.jar; do
    if [ -f "$_jar" ] && [ ! -f "$FLINK_LIB_DIR/$(basename "$_jar")" ]; then
        cp "$_jar" "$FLINK_LIB_DIR"/
    fi
done
unset _jar
mkdir -p "$FLINK_CONF_DIR" "$FLINK_LOG_DIR"
