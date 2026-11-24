"""Flink ``config.yaml`` of a flyt job, the same mechanism in both cluster modes.

Layers, later wins: flyt's defaults (partly per mode), the user's Flink configuration
(``base_flink_conf_path``, ``flink_config_override``, ``flyt run -D``), then the keys flyt owns.
This module holds what both modes share; :mod:`ytsaurus_flyt.application` and :mod:`ytsaurus_flyt.minicluster`
build their ``config.yaml`` from it.
The result is written to ``flink/conf/config.yaml`` of every YT job: the JobManager and TaskManagers
read it in ``application`` mode, the MiniCluster gets it as the script's context configuration.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import yaml

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import JVM_OVERHEAD_MIN, MIB, parse_memory

if TYPE_CHECKING:
    from yt.wrapper import YtClient


class FlinkConfigError(ValueError):
    """The user's Flink configuration or the cluster sized from it is invalid."""


TASK_SLOTS_KEY = "taskmanager.numberOfTaskSlots"
DEFAULT_PARALLELISM_KEY = "parallelism.default"
TM_PROCESS_SIZE_KEY = "taskmanager.memory.process.size"
JM_PROCESS_SIZE_KEY = "jobmanager.memory.process.size"
TM_TASK_OFF_HEAP_KEY = "taskmanager.memory.task.off-heap.size"
TM_MANAGED_FRACTION_KEY = "taskmanager.memory.managed.fraction"
TM_MEMORY_PREFIX = "taskmanager.memory."
JM_MEMORY_PREFIX = "jobmanager.memory."
TM_CPU_CORES_KEY = "taskmanager.cpu.cores"
# flyt's own options in the Flink configuration: the YT resources of the cluster.
FLYT_JM_CPU_KEY = "flyt.jobmanager.cpu.cores"
FLYT_TM_CPU_KEY = "flyt.taskmanager.cpu.cores"
FLYT_TM_REPLICAS_KEY = "flyt.taskmanager.replicas"
# jvm-overhead.min/max/fraction under each role's prefix: flyt sets min unless the user bounds the overhead.
_JVM_OVERHEAD_KEYS = ("jvm-overhead.min", "jvm-overhead.max", "jvm-overhead.fraction")
# Keys flyt sets itself plus the overhead; any other memory key of the role turns flyt's memory defaults off.
TM_MEMORY_DEFAULT_KEYS = frozenset(
    {TM_PROCESS_SIZE_KEY, TM_TASK_OFF_HEAP_KEY, TM_MANAGED_FRACTION_KEY}
    | {TM_MEMORY_PREFIX + k for k in _JVM_OVERHEAD_KEYS}
)
JM_MEMORY_DEFAULT_KEYS = frozenset({JM_PROCESS_SIZE_KEY} | {JM_MEMORY_PREFIX + k for k in _JVM_OVERHEAD_KEYS})

# JobManager ports are fixed (one JobManager per operation, own slot IP); TaskManager ports are
# random so several TaskManagers can share an exec node without porto network isolation.
FLINK_REST_PORT = 27050
FLINK_JOBMANAGER_RPC_PORT = 27051
FLINK_BLOB_SERVER_PORT = 27052

# Default task off-heap share of the TaskManager JVM (Flink's own default is 0): Flink pins
# -XX:MaxDirectMemorySize to framework + task off-heap, and the YT connectors' netty and the UDF
# workers' gRPC need direct buffers.
TASKMANAGER_TASK_OFF_HEAP_FRACTION = 0.125
# Flink reserves 40% for managed memory (RocksDB, batch operators, Python UDF workers). flyt jobs keep
# state on the heap and their Python UDFs are light, so managed memory is only the Python workers'
# budget; 10% leaves the rest to task heap.
TASKMANAGER_MANAGED_MEMORY_FRACTION = "0.1"
# A JVM that survives an OutOfMemoryError hangs in GC, misses heartbeats and is never restarted by YT.
JVM_EXIT_ON_OOM_OPT = "-XX:+ExitOnOutOfMemoryError"
# JVM flags of every Flink process: YT exec nodes are IPv6-first; a container's locale may be POSIX;
# Python UDFs might need netty's reflective direct-buffer access on Java 9+.
JVM_COMMON_OPTS = (
    "-Djava.net.preferIPv6Addresses=true",
    "-Dfile.encoding=UTF-8",
    "-Dio.netty.tryReflectionSetAccessible=true",
)

# Set per container by the run scripts (slot address, discovered JobManager, YT job id); never
# taken from the user's configuration.
RUNTIME_FLINK_KEYS = frozenset(
    {"jobmanager.rpc.address", "rest.address", "taskmanager.host", "taskmanager.resource-id"}
)
# Dropped: the REST server binds on rest.port, where TaskManagers and `flyt ui` look for it.
REST_BIND_PORT_KEY = "rest.bind-port"
# How YT jobs and `flyt ui` reach the Flink processes; a user value is replaced with a warning.
# Bind addresses must be every interface; fixed TaskManager ports collide on a shared exec node.
FLYT_NETWORK_OPTIONS: Mapping[str, str] = {
    "rest.port": str(FLINK_REST_PORT),
    "jobmanager.rpc.port": str(FLINK_JOBMANAGER_RPC_PORT),
    "blob.server.port": str(FLINK_BLOB_SERVER_PORT),
    "rest.bind-address": "0.0.0.0",
    "jobmanager.bind-host": "0.0.0.0",
    "taskmanager.bind-host": "0.0.0.0",
    "taskmanager.rpc.port": "0",
    "taskmanager.data.port": "0",
}
# A launcher that submits a ready spec (the manifest of ``flyt install``) passes the Flink options of a run as a
# JSON object in this environment variable of every task; the job renders its config.yaml again with them.
FLINK_CONF_ENV = "FLYT_FLINK_CONF"
# flyt's platform JVM flags. Flink (and the PyFlink gateway of a MiniCluster) puts env.java.opts.*
# after them and the JVM takes the last occurrence of a flag, so the user overrides any of them there.
JVM_DEFAULT_OPTS_KEYS = (
    "env.java.default-opts.all",
    "env.java.default-opts.jobmanager",
    "env.java.default-opts.taskmanager",
)
# The PyFlink gateway that runs a MiniCluster is one JVM: per-role JVM options do not apply to it.
_ROLE_JVM_OPTS_KEYS = ("env.java.opts.jobmanager", "env.java.opts.taskmanager")


def flatten_flink_config(data: Mapping[str, Any], prefix: str = "") -> Dict[str, str]:
    """Flatten a (possibly nested) Flink YAML mapping into dotted keys with string values."""
    out: Dict[str, str] = {}
    for key, value in data.items():
        full = f"{prefix}{key}"
        if isinstance(value, Mapping):
            out.update(flatten_flink_config(value, f"{full}."))
        elif isinstance(value, bool):
            out[full] = "true" if value else "false"
        elif value is None:
            out[full] = ""
        else:
            out[full] = str(value)
    return out


def load_flink_conf_file(path: str, yt_client: Optional[YtClient] = None) -> Dict[str, str]:
    """Read a Flink ``config.yaml`` (local path or Cypress ``//`` file) as flat dotted keys."""
    if path.startswith("//"):
        if yt_client is None:
            raise ValueError(f"reading {path} from Cypress needs a YT client")
        raw = yt_client.read_file(path).read()
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    else:
        try:
            with open(os.path.expanduser(path), "r", encoding="utf-8") as f:
                text = f.read()
        except OSError as e:
            raise FlinkConfigError(f"base_flink_conf_path {path!r}: {e.strerror or e}") from e
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        raise FlinkConfigError(f"base_flink_conf_path {path!r} is not valid YAML: {e}") from e
    if not isinstance(data, Mapping):
        raise FlinkConfigError(f"base_flink_conf_path {path!r} must contain a YAML mapping, got {type(data).__name__}")
    return flatten_flink_config(data)


def user_flink_config(config: FlytConfig, yt_client: Optional[YtClient] = None) -> Dict[str, str]:
    """The user's Flink options as one mapping: ``flink_config_override`` over the ``base_flink_conf_path``
    file. Everything that reads Flink keys goes through this merge."""
    merged = load_flink_conf_file(config.base_flink_conf_path, yt_client) if config.base_flink_conf_path else {}
    merged.update(config.flink_config_override)
    return merged


def custom_memory_keys(user: Mapping[str, str], prefix: str, exclude_keys: frozenset) -> List[str]:
    return sorted(k for k in user if k.startswith(prefix) and k not in exclude_keys)


def mebibytes(size_bytes: int) -> str:
    return f"{max(int(size_bytes) // MIB, 1)}m"


def common_defaults(config: FlytConfig) -> Dict[str, str]:
    """flyt's defaults that do not depend on the mode; the user's configuration overrides any of them."""
    return {
        # Without checkpointing Flink defaults to no restarts: a failure would end the script and turn
        # into a YT job restart (a full gang restart in application mode) instead of a Flink job restart.
        "restart-strategy.type": "exponential-delay",
        # Task-thread OOMs kill the JVM (Flink option); ExitOnOutOfMemoryError covers every other thread.
        # Either way YT restarts the job and Flink recovers instead of hanging.
        "taskmanager.jvm-exit-on-oom": "true",
        # Flink's 50 s heartbeat timeout loses TaskManagers on shared exec nodes to GC pauses and
        # network hiccups; a TaskManager is still declared dead after 9 failed heartbeat RPCs.
        "heartbeat.timeout": "120000",
        "heartbeat.interval": "5000",
        "heartbeat.rpc-failure-threshold": "9",
        # The REST API has no authentication: no jar upload through it; cancel only leaves the web UI.
        "web.submit.enable": "false",
        "web.cancel.enable": "false",
        "python.executable": config.python_bin,
        "python.client.executable": config.python_bin,
    }


def apply_user_and_owned(props: Dict[str, str], user: Mapping[str, str], jvm_opts: Mapping[str, str]) -> None:
    props.update(user)
    for key in RUNTIME_FLINK_KEYS | {REST_BIND_PORT_KEY}:
        props.pop(key, None)
    props.update(FLYT_NETWORK_OPTIONS)
    for key in JVM_DEFAULT_OPTS_KEYS:
        props.pop(key, None)
    props.update(jvm_opts)


def memory_option(props: Mapping[str, str], key: str) -> int:
    raw = str(props[key]).strip().replace(" ", "")
    try:
        return parse_memory(raw)
    except ValueError as e:
        raise FlinkConfigError(f"{key} is not a memory size: {raw!r}") from e


def process_memory_option(
    user: Mapping[str, str], process_key: str, prefix: str, default_keys: frozenset, default: int
) -> int:
    """A role's ``*.memory.process.size``, ``default`` unless set; the role's other memory keys need it as well."""
    if str(user.get(process_key) or "").strip():
        return memory_option(user, process_key)
    custom = custom_memory_keys(user, prefix, default_keys)
    if custom:
        raise FlinkConfigError(
            f"{', '.join(custom)} configure Flink's memory model, so flyt adds no memory defaults; "
            f"set {process_key} as well: it sizes the YT container"
        )
    return default


def int_option(user: Mapping[str, str], key: str) -> Optional[int]:
    raw = str(user.get(key) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as e:
        raise FlinkConfigError(f"{key} must be an integer, got {raw!r}") from e
    if value < 1:
        raise FlinkConfigError(f"{key} must be >= 1, got {raw!r}")
    return value


def jvm_overhead_default(props: Dict[str, str], user: Mapping[str, str], prefix: str) -> None:
    """``jvm-overhead.min`` of :data:`JVM_OVERHEAD_MIN` unless the user sets the overhead's min or max."""
    min_key, max_key = prefix + "jvm-overhead.min", prefix + "jvm-overhead.max"
    if min_key not in user and max_key not in user:
        props[min_key] = mebibytes(JVM_OVERHEAD_MIN)


def taskmanager_memory_defaults(props: Dict[str, str], user: Mapping[str, str], process_size: int) -> None:
    if not custom_memory_keys(user, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS):
        props[TM_PROCESS_SIZE_KEY] = mebibytes(process_size)
        props[TM_TASK_OFF_HEAP_KEY] = mebibytes(int(process_size * TASKMANAGER_TASK_OFF_HEAP_FRACTION))
        props[TM_MANAGED_FRACTION_KEY] = TASKMANAGER_MANAGED_MEMORY_FRACTION
        jvm_overhead_default(props, user, TM_MEMORY_PREFIX)


def flink_config_warnings(config: FlytConfig, user: Mapping[str, str]) -> List[str]:
    """Warnings about the user's Flink options: keys flyt owns and ignores, keys the mode ignores."""
    out = [
        f"{key} is set per YT job by flyt; the value from the Flink configuration is ignored"
        for key in sorted(RUNTIME_FLINK_KEYS & set(user))
    ]
    if REST_BIND_PORT_KEY in user:
        out.append(f"{REST_BIND_PORT_KEY} is ignored: the REST server binds on rest.port, where flyt finds it")
    out += [
        f"{key} is always {value!r} on YT; {user[key]!r} from the Flink configuration is ignored"
        for key, value in sorted(FLYT_NETWORK_OPTIONS.items())
        if key in user and str(user[key]).strip() != value
    ]
    if TM_CPU_CORES_KEY in user:
        out.append(
            f"{TM_CPU_CORES_KEY} is the TaskManager's container cpu, set by flyt from {FLYT_TM_CPU_KEY}; "
            f"{user[TM_CPU_CORES_KEY]!r} from the Flink configuration is ignored"
        )
    out += [
        f"{key} carries flyt's platform JVM flags and is ignored; put JVM options into "
        f"{key.replace('default-opts', 'opts')}, where a later flag overrides flyt's"
        for key in JVM_DEFAULT_OPTS_KEYS
        if key in user
    ]
    if not config.is_application_cluster:
        out += [
            f"{key} has no effect in minicluster mode: the job is one TaskManager, sized by {TM_PROCESS_SIZE_KEY}"
            for key in sorted(user)
            if key.startswith(JM_MEMORY_PREFIX)
        ]
        out += [
            f"{key} has no effect in minicluster mode: the job is one JVM" for key in _ROLE_JVM_OPTS_KEYS if key in user
        ]
        out += [
            f"{key} has no effect in minicluster mode: the job is one TaskManager"
            for key in (FLYT_JM_CPU_KEY, FLYT_TM_REPLICAS_KEY)
            if key in user
        ]
        if TASK_SLOTS_KEY in user:
            out.append(
                f"{TASK_SLOTS_KEY} is not needed in minicluster mode: the MiniCluster gets as many slots as the "
                f"job graph's max parallelism; fewer slots fail the job, set {DEFAULT_PARALLELISM_KEY} instead"
            )
    return out


def render_flink_config_yaml(props: Mapping[str, str]) -> str:
    """``config.yaml`` text: one flat dotted key per line, values quoted where YAML would retype them."""
    # No line folding: Flink's bash scripts read some keys (env.java.*) line by line.
    return yaml.safe_dump(
        {k: str(v) for k, v in sorted(props.items())}, default_flow_style=False, sort_keys=True, width=float("inf")
    )
