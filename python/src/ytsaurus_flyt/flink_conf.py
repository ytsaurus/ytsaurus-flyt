"""Flink ``config.yaml`` of a flyt job, the same mechanism in both cluster modes.

Layers, later wins: flyt's defaults (partly per mode), the user's Flink configuration
(``base_flink_conf_path``, ``flink_config_override``, ``flyt run -D``), then the keys flyt owns.
The result is written to ``flink/conf/config.yaml`` of every YT job: the JobManager and TaskManagers
read it in ``application`` mode, the MiniCluster gets it as the script's context configuration.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

import yaml

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import JVM_OVERHEAD_MIN, MIB, ClusterParams, parse_memory

if TYPE_CHECKING:
    from yt.wrapper import YtClient

    from ytsaurus_flyt.application import ApplicationResources

TASK_SLOTS_KEY = "taskmanager.numberOfTaskSlots"
DEFAULT_PARALLELISM_KEY = "parallelism.default"
TM_PROCESS_SIZE_KEY = "taskmanager.memory.process.size"
JM_PROCESS_SIZE_KEY = "jobmanager.memory.process.size"
TM_TASK_OFF_HEAP_KEY = "taskmanager.memory.task.off-heap.size"
TM_MANAGED_FRACTION_KEY = "taskmanager.memory.managed.fraction"
TM_MEMORY_PREFIX = "taskmanager.memory."
JM_MEMORY_PREFIX = "jobmanager.memory."
# jvm-overhead.min/max/fraction under each role's prefix: flyt keeps min (and max) at JVM_OVERHEAD_MIN or above.
_JVM_OVERHEAD_KEYS = ("jvm-overhead.min", "jvm-overhead.max", "jvm-overhead.fraction")
# The memory keys flyt sets itself, plus the overhead that only carves process.size. Any other key under the
# role's prefix means the user configures Flink's memory model: flyt then adds none of these defaults
# instead of second-guessing Flink.
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
# taken from the user's configuration, the way Flink on Kubernetes strips them from its ConfigMap.
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
        with open(os.path.expanduser(path), "r", encoding="utf-8") as f:
            text = f.read()
    data = yaml.safe_load(text) or {}
    if not isinstance(data, Mapping):
        raise ValueError(f"base_flink_conf_path {path!r} must contain a YAML mapping, got {type(data).__name__}")
    return flatten_flink_config(data)


def user_flink_config(config: FlytConfig, yt_client: Optional[YtClient] = None) -> Dict[str, str]:
    """The user's Flink options as one mapping: ``flink_config_override`` over the ``base_flink_conf_path``
    file. Everything that reads Flink keys goes through this merge."""
    merged = load_flink_conf_file(config.base_flink_conf_path, yt_client) if config.base_flink_conf_path else {}
    merged.update(config.flink_config_override)
    return merged


def custom_memory_keys(user: Mapping[str, str], prefix: str, flyt_keys: frozenset) -> List[str]:
    return sorted(k for k in user if k.startswith(prefix) and k not in flyt_keys)


def _mebibytes(size_bytes: int) -> str:
    return f"{max(int(size_bytes) // MIB, 1)}m"


def _common_defaults(config: FlytConfig) -> Dict[str, str]:
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
        "python.executable": config.python_bin,
        "python.client.executable": config.python_bin,
    }


def _apply_user_and_owned(props: Dict[str, str], user: Mapping[str, str], jvm_opts: Mapping[str, str]) -> None:
    props.update(user)
    for key in RUNTIME_FLINK_KEYS | {REST_BIND_PORT_KEY}:
        props.pop(key, None)
    props.update(FLYT_NETWORK_OPTIONS)
    for key in JVM_DEFAULT_OPTS_KEYS:
        props.pop(key, None)
    props.update(jvm_opts)


def _jvm_overhead_floor(
    props: Mapping[str, str], prefixes: Tuple[str, ...] = (JM_MEMORY_PREFIX, TM_MEMORY_PREFIX)
) -> Dict[str, str]:
    """``*.memory.jvm-overhead.min``/``.max`` raised to :data:`JVM_OVERHEAD_MIN` where ``props`` has less;
    an unset max stays Flink's 1g."""
    out: Dict[str, str] = {}
    for prefix in prefixes:
        min_key, max_key = prefix + "jvm-overhead.min", prefix + "jvm-overhead.max"
        if min_key not in props or _memory_option(props, min_key) < JVM_OVERHEAD_MIN:
            out[min_key] = _mebibytes(JVM_OVERHEAD_MIN)
        if max_key in props and _memory_option(props, max_key) < JVM_OVERHEAD_MIN:
            out[max_key] = _mebibytes(JVM_OVERHEAD_MIN)
    return out


def _memory_option(props: Mapping[str, str], key: str) -> int:
    raw = str(props[key]).strip().replace(" ", "")
    try:
        return parse_memory(raw)
    except ValueError as e:
        raise ValueError(f"{key} is not a memory size: {raw!r}") from e


def _taskmanager_memory_defaults(props: Dict[str, str], user: Mapping[str, str], process_size: int) -> None:
    if not custom_memory_keys(user, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS):
        props[TM_PROCESS_SIZE_KEY] = _mebibytes(process_size)
        props[TM_TASK_OFF_HEAP_KEY] = _mebibytes(int(process_size * TASKMANAGER_TASK_OFF_HEAP_FRACTION))
        props[TM_MANAGED_FRACTION_KEY] = TASKMANAGER_MANAGED_MEMORY_FRACTION


def cluster_flink_config(
    config: FlytConfig, resources: ApplicationResources, user: Mapping[str, str]
) -> Dict[str, str]:
    """The ``config.yaml`` shared by the JobManager and TaskManagers in application mode.

    Per-container keys (``jobmanager.rpc.address``, ``rest.address``, ``taskmanager.host``,
    ``taskmanager.resource-id``) are not here: the run scripts pass them on the command line.
    """
    props = _common_defaults(config)
    props[TASK_SLOTS_KEY] = str(resources.slots)
    props[DEFAULT_PARALLELISM_KEY] = str(resources.default_parallelism)
    if not custom_memory_keys(user, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS):
        props[JM_PROCESS_SIZE_KEY] = _mebibytes(resources.jobmanager_process_memory)
    _taskmanager_memory_defaults(props, user, resources.taskmanager_process_memory)
    all_opts, jm_opts, tm_opts = JVM_DEFAULT_OPTS_KEYS
    _apply_user_and_owned(
        props,
        user,
        {
            all_opts: " ".join(JVM_COMMON_OPTS),
            # The processor count is the container cpu.
            jm_opts: f"-XX:ActiveProcessorCount={resources.jobmanager_cpu} {JVM_EXIT_ON_OOM_OPT}",
            tm_opts: f"-XX:ActiveProcessorCount={resources.taskmanager_cpu} {JVM_EXIT_ON_OOM_OPT}",
        },
    )
    props.update(_jvm_overhead_floor(props))
    return props


def minicluster_flink_config(
    config: FlytConfig,
    preset: Optional[ClusterParams],
    user: Mapping[str, str],
    *,
    cpu: Optional[int] = None,
    process_memory: Optional[int] = None,
) -> Dict[str, str]:
    """The ``config.yaml`` of a MiniCluster job: the same layers as :func:`cluster_flink_config`. The preset
    sizes the job's JVM, which the PyFlink gateway starts with ``env.java.*``; without one, ``cpu`` and
    ``process_memory`` (``taskmanager.memory.process.size``) make the job one TaskManager, and the run script
    adds the JVM flags Flink derives for it."""
    if preset is None and (cpu is None or process_memory is None):
        raise ValueError("a MiniCluster without a preset needs cpu and process_memory")
    cpu = cpu or preset.cpu
    props = _common_defaults(config)
    # Every cpu of the job, like every slot of an application cluster (and like PyFlink's local default).
    props[DEFAULT_PARALLELISM_KEY] = str(cpu)
    jvm_opts: List[str] = []
    if preset is not None:
        jvm_opts.append(f"-Xmx{preset.max_heap_size}")
        if preset.off_heap_size:
            # Bounds NIO/netty direct buffers (gRPC connectors); otherwise the JVM
            # defaults MaxDirectMemorySize to ~the max heap size.
            jvm_opts.append(f"-XX:MaxDirectMemorySize={preset.off_heap_size}")
    else:
        assert process_memory is not None
        _taskmanager_memory_defaults(props, user, process_memory)
    jvm_opts += [f"-XX:ActiveProcessorCount={cpu}", JVM_EXIT_ON_OOM_OPT, *JVM_COMMON_OPTS]
    _apply_user_and_owned(props, user, {JVM_DEFAULT_OPTS_KEYS[0]: " ".join(jvm_opts)})
    if preset is None:
        props.update(_jvm_overhead_floor(props, (TM_MEMORY_PREFIX,)))
    return props


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
    out += [
        f"{key} carries flyt's platform JVM flags and is ignored; put JVM options into "
        f"{key.replace('default-opts', 'opts')}, where a later flag overrides flyt's"
        for key in JVM_DEFAULT_OPTS_KEYS
        if key in user
    ]
    if config.is_application_cluster or TM_PROCESS_SIZE_KEY in user:
        prefixes = (JM_MEMORY_PREFIX, TM_MEMORY_PREFIX) if config.is_application_cluster else (TM_MEMORY_PREFIX,)
        out += [
            f"{key} is raised to {value}: the YT container is process.size, and the JVM overhead holds "
            f"the processes outside the JVM as well"
            for key, value in sorted(_jvm_overhead_floor(user, prefixes).items())
            if key in user
        ]
    if not config.is_application_cluster:
        out += [
            f"{key} has no effect in minicluster mode: the job is one TaskManager, sized by the preset or "
            f"{TM_PROCESS_SIZE_KEY}"
            for key in sorted(user)
            if key.startswith(JM_MEMORY_PREFIX)
        ]
        out += [
            f"{key} has no effect in minicluster mode: the job is one JVM" for key in _ROLE_JVM_OPTS_KEYS if key in user
        ]
        if TASK_SLOTS_KEY in user:
            out.append(
                f"{TASK_SLOTS_KEY} is not needed in minicluster mode: the MiniCluster gets one slot per "
                f"subtask of the widest operator; fewer slots fail the job, set {DEFAULT_PARALLELISM_KEY} instead"
            )
    return out


def render_flink_config_yaml(props: Mapping[str, str]) -> str:
    """``config.yaml`` text: one flat dotted key per line, values quoted where YAML would retype them."""
    # No line folding: Flink's bash scripts read some keys (env.java.*) line by line.
    return yaml.safe_dump(
        {k: str(v) for k, v in sorted(props.items())}, default_flow_style=False, sort_keys=True, width=float("inf")
    )
