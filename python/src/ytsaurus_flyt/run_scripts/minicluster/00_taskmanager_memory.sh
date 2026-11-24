# The job is one TaskManager of taskmanager.memory.process.size, but local execution ignores Flink's
# TaskManager memory model: take the JVM flags and the TaskManager's resolved options from Flink's own
# BashJavaUtils, as taskmanager.sh does; Flink decides which of them apply locally.
_flink_bin="$(pyflink_home)/bin"
FLINK_BIN_DIR="$_flink_bin"
source "$_flink_bin/bash-java-utils.sh"
setJavaRun "$FLINK_CONF_DIR"
parseResourceParamsAndExportLogs GET_TM_RESOURCE_PARAMS
echo "TaskManager memory of the MiniCluster JVM: $JVM_ARGS" 1>&2
_conf="$FLINK_CONF_DIR/config.yaml"
sed -i "s|^env.java.default-opts.all: |&${{JVM_ARGS# }} |" "$_conf"
# The MiniCluster takes its slots from the job graph unless the key is set: keep the user's value or none.
for _kv in $DYNAMIC_PARAMETERS; do
    case "$_kv" in
        -D|taskmanager.numberOfTaskSlots=*) ;;
        *=*)
            sed -i "/^${{_kv%%=*}}:/d" "$_conf"
            echo "${{_kv%%=*}}: ${{_kv#*=}}" >> "$_conf"
            ;;
    esac
done
unset _flink_bin _conf _kv
