"""Flink ``config.yaml`` of an application cluster: the user's layers, the Flink keys flyt relies on,
flyt's defaults and the warnings about risky options. A MiniCluster job gets none: its script
configures the MiniCluster in code.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import yaml
from yt.wrapper import YtClient

from ytsaurus_flyt.config import FlytConfig

if TYPE_CHECKING:
    from ytsaurus_flyt.application import ApplicationResources

TASK_SLOTS_KEY = "taskmanager.numberOfTaskSlots"
TM_PROCESS_SIZE_KEY = "taskmanager.memory.process.size"
JM_PROCESS_SIZE_KEY = "jobmanager.memory.process.size"
DEFAULT_PARALLELISM_KEY = "parallelism.default"
REST_PORT_KEY = "rest.port"
TM_TASK_OFF_HEAP_KEY = "taskmanager.memory.task.off-heap.size"
TM_MANAGED_FRACTION_KEY = "taskmanager.memory.managed.fraction"
TM_MEMORY_PREFIX = "taskmanager.memory."
JM_MEMORY_PREFIX = "jobmanager.memory."
# The memory keys flyt sets itself. Any other key under the role's prefix means the user configures
# Flink's memory model: flyt then adds none of these defaults instead of second-guessing Flink.
TM_MEMORY_DEFAULT_KEYS = frozenset({TM_PROCESS_SIZE_KEY, TM_TASK_OFF_HEAP_KEY, TM_MANAGED_FRACTION_KEY})
JM_MEMORY_DEFAULT_KEYS = frozenset({JM_PROCESS_SIZE_KEY})

# JobManager ports are fixed (one JobManager per operation, own slot IP); TaskManager ports are
# random so several TaskManagers can share an exec node without porto network isolation.
FLINK_REST_PORT = 27050
FLINK_JOBMANAGER_RPC_PORT = 27051
FLINK_BLOB_SERVER_PORT = 27052

# Default task off-heap share of the TaskManager JVM (Flink's own default is 0): Flink pins
# -XX:MaxDirectMemorySize to framework + task off-heap, and the YT connectors' netty and the UDF
# workers' gRPC need direct buffers. The Flink key in the user's options overrides it.
TASKMANAGER_TASK_OFF_HEAP_FRACTION = 0.125
# Flink reserves 40% for managed memory (RocksDB, batch operators, Python UDF workers). flyt jobs keep
# state on the heap and their Python UDFs are light, so managed memory is only the Python workers'
# budget; 10% leaves the rest to task heap. RocksDB or heavy UDFs raise it in the Flink configuration.
TASKMANAGER_MANAGED_MEMORY_FRACTION = "0.1"
# A JVM that survives an OutOfMemoryError hangs in GC, misses heartbeats and is never restarted by YT.
JVM_EXIT_ON_OOM_OPT = "-XX:+ExitOnOutOfMemoryError"

# Set per container by the run scripts (slot address, discovered JobManager, YT job id); never
# taken from the cluster config, the way Flink on Kubernetes strips bind/host keys from its ConfigMap.
RUNTIME_FLINK_KEYS = frozenset(
    {"jobmanager.rpc.address", "rest.address", "taskmanager.host", "taskmanager.resource-id"}
)

# How the containers reach each other on YT: owned by flyt, a user value is replaced with a warning.
# Bind addresses must be every interface; fixed TaskManager ports collide on a shared exec node.
FLYT_NETWORK_OPTIONS: Mapping[str, str] = {
    "rest.bind-address": "0.0.0.0",
    "jobmanager.bind-host": "0.0.0.0",
    "taskmanager.bind-host": "0.0.0.0",
    "taskmanager.rpc.port": "0",
    "taskmanager.data.port": "0",
}
# Dropped: the REST server binds on rest.port, where TaskManagers and `flyt ui` look for it.
REST_BIND_PORT_KEY = "rest.bind-port"
# Platform JVM flags, owned by flyt. Flink puts env.java.opts.* after them and the JVM takes the last
# occurrence of a flag, so the user overrides any of them there.
JVM_DEFAULT_OPTS_KEYS = (
    "env.java.default-opts.all",
    "env.java.default-opts.jobmanager",
    "env.java.default-opts.taskmanager",
)

# Restarts without a stable run (restart-strategy.exponential-delay.reset-backoff-threshold, 1h) before
# the Flink job fails; then the driver exits non-zero and YT restarts the cluster.
FLINK_RESTART_ATTEMPTS = 10

_MIB = 1024 * 1024


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


def load_flink_conf_file(yt_client: YtClient, path: str) -> Dict[str, str]:
    """Read a Flink ``config.yaml`` (local path or Cypress ``//`` file) as flat dotted keys."""
    if path.startswith("//"):
        raw = yt_client.read_file(path).read()
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    else:
        with open(os.path.expanduser(path), "r", encoding="utf-8") as f:
            text = f.read()
    data = yaml.safe_load(text) or {}
    if not isinstance(data, Mapping):
        raise ValueError(f"base_flink_conf_path {path!r} must contain a YAML mapping, got {type(data).__name__}")
    return flatten_flink_config(data)


def user_flink_config(config: FlytConfig, conf_file: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """The user's Flink options as one mapping: the flattened ``base_flink_conf_path`` file (``conf_file``)
    under ``flink_config_override``. Everything that reads Flink keys goes through this merge.
    """
    merged: Dict[str, str] = {str(k): str(v) for k, v in (conf_file or {}).items()}
    merged.update(config.flink_config_override)
    return merged


def custom_memory_keys(user: Mapping[str, str], prefix: str, flyt_keys: frozenset) -> List[str]:
    return sorted(k for k in user if k.startswith(prefix) and k not in flyt_keys)


def _mebibytes(size_bytes: int) -> str:
    return f"{max(int(size_bytes) // _MIB, 1)}m"


def cluster_flink_config(
    config: FlytConfig, resources: ApplicationResources, user: Mapping[str, str]
) -> Dict[str, str]:
    """The cluster ``config.yaml`` shared by the JobManager and TaskManagers in application mode.

    Layers, later wins: flyt defaults (ports, memory, slots, restart strategy), then ``user``
    (:func:`user_flink_config`). Keys flyt owns are never taken from ``user``: per-container keys
    (:data:`RUNTIME_FLINK_KEYS`, set by the run scripts) and ``rest.bind-port`` are dropped, the network
    options (:data:`FLYT_NETWORK_OPTIONS`) and platform JVM flags (:data:`JVM_DEFAULT_OPTS_KEYS`) are
    always flyt's.
    """
    props: Dict[str, str] = {
        REST_PORT_KEY: str(FLINK_REST_PORT),
        "jobmanager.rpc.port": str(FLINK_JOBMANAGER_RPC_PORT),
        "blob.server.port": str(FLINK_BLOB_SERVER_PORT),
        TASK_SLOTS_KEY: str(resources.slots),
        DEFAULT_PARALLELISM_KEY: str(resources.default_parallelism),
        # Without checkpointing Flink defaults to no restarts: a lost TaskManager would fail the
        # driver and turn into a full gang restart instead of an in-cluster job restart.
        "restart-strategy.type": "exponential-delay",
        # Flink retries forever by default; a job that keeps failing must fail the operation instead.
        "restart-strategy.exponential-delay.attempts-before-reset-backoff": str(FLINK_RESTART_ATTEMPTS),
        # Task-thread OOMs kill the TaskManager (Flink option); ExitOnOutOfMemoryError covers every
        # other thread. Either way YT restarts the job and Flink recovers instead of hanging.
        "taskmanager.jvm-exit-on-oom": "true",
        "python.executable": config.python_bin,
        "python.client.executable": config.python_bin,
    }
    if not custom_memory_keys(user, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS):
        props[JM_PROCESS_SIZE_KEY] = _mebibytes(resources.jobmanager_process_memory)
    if not custom_memory_keys(user, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS):
        tm_process_size = resources.taskmanager_process_memory
        props[TM_PROCESS_SIZE_KEY] = _mebibytes(tm_process_size)
        props[TM_TASK_OFF_HEAP_KEY] = _mebibytes(int(tm_process_size * TASKMANAGER_TASK_OFF_HEAP_FRACTION))
        props[TM_MANAGED_FRACTION_KEY] = TASKMANAGER_MANAGED_MEMORY_FRACTION
    props.update(user)
    for key in RUNTIME_FLINK_KEYS | {REST_BIND_PORT_KEY}:
        props.pop(key, None)
    props.update(FLYT_NETWORK_OPTIONS)
    # YT exec nodes are IPv6-first; the processor count is the container cpu.
    all_opts, jm_opts, tm_opts = JVM_DEFAULT_OPTS_KEYS
    props[all_opts] = "-Djava.net.preferIPv6Addresses=true"
    props[jm_opts] = f"-XX:ActiveProcessorCount={resources.jobmanager_cpu} {JVM_EXIT_ON_OOM_OPT}"
    props[tm_opts] = f"-XX:ActiveProcessorCount={resources.taskmanager_cpu} {JVM_EXIT_ON_OOM_OPT}"
    return props


def flink_config_warnings(user: Mapping[str, str]) -> List[str]:
    """Warnings about the user's Flink options: keys flyt owns and ignores, missing sizing keys."""
    out = [
        f"{key} is set per YT job by flyt; the value from the Flink configuration is ignored"
        for key in sorted(RUNTIME_FLINK_KEYS & set(user))
    ]
    if REST_BIND_PORT_KEY in user:
        out.append(f"{REST_BIND_PORT_KEY} is ignored: the REST server binds on {REST_PORT_KEY}, where flyt finds it")
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
    if TASK_SLOTS_KEY not in user:
        out.append(
            f"{TASK_SLOTS_KEY} is not set: every TaskManager gets 1 slot and, unless taskmanager_cpu is set, 1 cpu; "
            "set it to size the TaskManagers"
        )
    return out


def rest_port(flink_config: Mapping[str, str]) -> int:
    """The REST (Web UI) port a merged Flink configuration listens on."""
    return int(flink_config.get(REST_PORT_KEY) or FLINK_REST_PORT)


def minicluster_ignored_options_warning(config: FlytConfig) -> Optional[str]:
    """Warning when the profile or the run carries Flink options that a MiniCluster job ignores."""
    ignored = []
    if config.base_flink_conf_path:
        ignored.append(f"base_flink_conf_path {config.base_flink_conf_path}")
    if config.flink_config_override:
        ignored.append(f"{len(config.flink_config_override)} flink_config_override/-D key(s)")
    if not ignored:
        return None
    return f"{' and '.join(ignored)} apply to application mode only; in minicluster set Flink options in the script"


def render_flink_config_yaml(props: Mapping[str, str]) -> str:
    """``config.yaml`` text: one flat dotted key per line, values quoted where YAML would retype them."""
    return yaml.safe_dump({k: str(v) for k, v in sorted(props.items())}, default_flow_style=False, sort_keys=True)
