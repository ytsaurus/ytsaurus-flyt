"""The application cluster of a run: the YT resources of its JobManager and TaskManagers and the
``config.yaml`` they all share. Each TaskManager is sized by :mod:`ytsaurus_flyt.sizing`; the JobManager by
flyt's defaults, ``flyt.jobmanager.cpu.cores`` and ``jobmanager.memory.*``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    DEFAULT_PARALLELISM_KEY,
    FLYT_JM_CPU_KEY,
    FLYT_TM_REPLICAS_KEY,
    JM_MEMORY_DEFAULT_KEYS,
    JM_MEMORY_PREFIX,
    JM_PROCESS_SIZE_KEY,
    JVM_COMMON_OPTS,
    JVM_DEFAULT_OPTS_KEYS,
    JVM_EXIT_ON_OOM_OPT,
    TASK_SLOTS_KEY,
    TM_CPU_CORES_KEY,
    FlinkConfigError,
    apply_user_and_owned,
    common_defaults,
    custom_memory_keys,
    int_option,
    jvm_overhead_default,
    mebibytes,
    process_memory_option,
    taskmanager_memory_defaults,
)
from ytsaurus_flyt.models import (
    DEFAULT_JOBMANAGER_CPU,
    DEFAULT_JOBMANAGER_PROCESS_MEMORY,
    MIB,
)
from ytsaurus_flyt.sizing import WorkerSize, resolve_taskmanager_size


@dataclass(frozen=True)
class ApplicationResources:
    """What an application cluster asks YT for, resolved by :func:`resolve_application_resources`."""

    taskmanager_count: int
    slots: int
    jobmanager_cpu: int
    jobmanager_process_memory: int
    taskmanager_size: WorkerSize
    default_parallelism: int

    def summary(self) -> str:
        """One line for logs and ``flyt validate``."""
        return (
            f"1 JobManager ({self.jobmanager_cpu} cpu, {self.jobmanager_process_memory // MIB}m JVM) + "
            f"{self.taskmanager_count} TaskManager(s) x {self.slots} slot(s) "
            f"({self.taskmanager_size.cpu} cpu, {self.taskmanager_size.memory // MIB}m JVM each), "
            f"parallelism.default {self.default_parallelism}"
        )


def resolve_application_resources(user: Optional[Mapping[str, str]] = None) -> ApplicationResources:
    """Every TaskManager is sized by :func:`resolve_taskmanager_size`, with one slot per cpu; the JobManager
    gets flyt's defaults (cpu from ``flyt.jobmanager.cpu.cores``). The user's Flink options
    (:func:`~ytsaurus_flyt.flink_conf.user_flink_config`) override slots, JVM sizes and the default parallelism."""
    user = user or {}
    taskmanager_size = resolve_taskmanager_size(user)
    slots = int_option(user, TASK_SLOTS_KEY) or taskmanager_size.cpu
    jobmanager_process_memory = process_memory_option(
        user, JM_PROCESS_SIZE_KEY, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS, DEFAULT_JOBMANAGER_PROCESS_MEMORY
    )
    taskmanager_count = int_option(user, FLYT_TM_REPLICAS_KEY) or 1
    capacity = taskmanager_count * slots
    default_parallelism = int_option(user, DEFAULT_PARALLELISM_KEY) or capacity
    if default_parallelism > capacity:
        # A standalone cluster cannot grow: the job would wait for slots and fail after slot.request.timeout.
        raise FlinkConfigError(
            f"{DEFAULT_PARALLELISM_KEY} {default_parallelism} does not fit into {FLYT_TM_REPLICAS_KEY} * slots = "
            f"{capacity}; raise {FLYT_TM_REPLICAS_KEY} or {TASK_SLOTS_KEY}"
        )
    return ApplicationResources(
        taskmanager_count=taskmanager_count,
        slots=slots,
        jobmanager_cpu=int_option(user, FLYT_JM_CPU_KEY) or DEFAULT_JOBMANAGER_CPU,
        jobmanager_process_memory=jobmanager_process_memory,
        taskmanager_size=taskmanager_size,
        default_parallelism=default_parallelism,
    )


def application_flink_config(
    config: FlytConfig, resources: ApplicationResources, user: Mapping[str, str]
) -> Dict[str, str]:
    """The ``config.yaml`` shared by the JobManager and TaskManagers in application mode.

    Per-container keys (``jobmanager.rpc.address``, ``rest.address``, ``taskmanager.host``,
    ``taskmanager.resource-id``) are not here: the run scripts pass them on the command line.
    """
    props = common_defaults(config)
    props[TASK_SLOTS_KEY] = str(resources.slots)
    props[DEFAULT_PARALLELISM_KEY] = str(resources.default_parallelism)
    if not custom_memory_keys(user, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS):
        props[JM_PROCESS_SIZE_KEY] = mebibytes(resources.jobmanager_process_memory)
        jvm_overhead_default(props, user, JM_MEMORY_PREFIX)
    taskmanager_memory_defaults(props, user, resources.taskmanager_size.memory)
    all_opts, jm_opts, tm_opts = JVM_DEFAULT_OPTS_KEYS
    apply_user_and_owned(
        props,
        user,
        {
            all_opts: " ".join(JVM_COMMON_OPTS),
            # The processor count is the container cpu.
            jm_opts: f"-XX:ActiveProcessorCount={resources.jobmanager_cpu} {JVM_EXIT_ON_OOM_OPT}",
            tm_opts: f"-XX:ActiveProcessorCount={resources.taskmanager_size.cpu} {JVM_EXIT_ON_OOM_OPT}",
        },
    )
    props[TM_CPU_CORES_KEY] = str(resources.taskmanager_size.cpu)
    return props


@dataclass(frozen=True)
class ApplicationCluster:
    """An application cluster ready for the spec: its YT resources and the ``config.yaml`` of every job, with the
    user's Flink options they come from."""

    resources: ApplicationResources
    flink_config: Dict[str, str]
    flink_options: Dict[str, str]


def application_cluster(config: FlytConfig, user: Optional[Mapping[str, str]] = None) -> ApplicationCluster:
    """Resolve the application cluster from the profile and the user's Flink options."""
    user = user or {}
    resources = resolve_application_resources(user)
    return ApplicationCluster(
        resources=resources,
        flink_config=application_flink_config(config, resources, user),
        flink_options=dict(user),
    )
