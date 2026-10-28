"""Flink ``config.yaml`` of an application cluster: the options flyt sets for the JobManager and
TaskManagers on YT. Users do not configure it; the script sets job options in code, in both modes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Mapping

import yaml

from ytsaurus_flyt.config import FlytConfig

if TYPE_CHECKING:
    from ytsaurus_flyt.application import ApplicationResources

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

# Restarts without a stable run (restart-strategy.exponential-delay.reset-backoff-threshold, 1h) before
# the Flink job fails; then the driver exits non-zero and YT restarts the cluster.
FLINK_RESTART_ATTEMPTS = 10

_MIB = 1024 * 1024


def _mebibytes(size_bytes: int) -> str:
    return f"{max(int(size_bytes) // _MIB, 1)}m"


def cluster_flink_config(config: FlytConfig, resources: ApplicationResources) -> Dict[str, str]:
    """The cluster ``config.yaml`` shared by the JobManager and TaskManagers in application mode.

    Per-container keys (``jobmanager.rpc.address``, ``rest.address``, ``taskmanager.host``,
    ``taskmanager.resource-id``) are not here: the run scripts pass them on the command line.
    """
    tm_process_size = resources.taskmanager_process_memory
    return {
        "rest.port": str(FLINK_REST_PORT),
        "jobmanager.rpc.port": str(FLINK_JOBMANAGER_RPC_PORT),
        "blob.server.port": str(FLINK_BLOB_SERVER_PORT),
        # Every interface: other YT jobs reach the processes on any of the job's addresses.
        "rest.bind-address": "0.0.0.0",
        "jobmanager.bind-host": "0.0.0.0",
        "taskmanager.bind-host": "0.0.0.0",
        "taskmanager.rpc.port": "0",
        "taskmanager.data.port": "0",
        "taskmanager.numberOfTaskSlots": str(resources.slots),
        "parallelism.default": str(resources.default_parallelism),
        "jobmanager.memory.process.size": _mebibytes(resources.jobmanager_process_memory),
        "taskmanager.memory.process.size": _mebibytes(tm_process_size),
        "taskmanager.memory.task.off-heap.size": _mebibytes(int(tm_process_size * TASKMANAGER_TASK_OFF_HEAP_FRACTION)),
        "taskmanager.memory.managed.fraction": TASKMANAGER_MANAGED_MEMORY_FRACTION,
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
        # YT exec nodes are IPv6-first; pin the JVM processor count to the container CPU limit.
        "env.java.opts.all": "-Djava.net.preferIPv6Addresses=true",
        "env.java.opts.jobmanager": f"-XX:ActiveProcessorCount={resources.jobmanager_cpu} {JVM_EXIT_ON_OOM_OPT}",
        "env.java.opts.taskmanager": f"-XX:ActiveProcessorCount={resources.taskmanager_cpu} {JVM_EXIT_ON_OOM_OPT}",
    }


def render_flink_config_yaml(props: Mapping[str, str]) -> str:
    """``config.yaml`` text: one flat dotted key per line, values quoted where YAML would retype them."""
    return yaml.safe_dump({k: str(v) for k, v in sorted(props.items())}, default_flow_style=False, sort_keys=True)
