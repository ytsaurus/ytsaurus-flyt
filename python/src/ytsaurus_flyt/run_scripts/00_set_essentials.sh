export ROOT_DIR="$(pwd -P)"

# The layer bundles a relocatable CPython (python-dist/) and a Temurin JRE (java/).
# It mounts at / for porto layer_paths, or unpacks under $ROOT_DIR for sandbox_unpack.
if [ -x "/python-dist/bin/python3" ]; then
    FLYT_LAYER_ROOT=""
else
    FLYT_LAYER_ROOT="$ROOT_DIR"
fi
export PYTHON_BIN="$FLYT_LAYER_ROOT/python-dist/bin/python3"
export PYFLINK_CLIENT_EXECUTABLE="$PYTHON_BIN"
export JAVA_HOME="$FLYT_LAYER_ROOT/java"
export PATH="$JAVA_HOME/bin:$FLYT_LAYER_ROOT/python-dist/bin:$PATH"
export FLINK_HOME="$ROOT_DIR/flink"
export FLINK_LIB_DIR="$FLINK_HOME/lib"
export FLINK_OPT_DIR="$FLINK_HOME/opt"
export FLINK_PLUGINS_DIR="$FLINK_HOME/plugins"
export FLINK_LOG_DIR="$FLINK_HOME/log"
export FLINK_CONF_DIR="$FLINK_HOME/conf"
export SERVICE_DIR="$ROOT_DIR/python"
