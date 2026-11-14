"""The YT resources of a run and the ``config.yaml`` of its jobs. A TaskManager, and the MiniCluster job,
which is one TaskManager, is sized either by a preset or by ``taskmanager_cpu`` and the user's
``taskmanager.memory.*`` keys, never by both; the JobManager by flyt's defaults and ``jobmanager.memory.*``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    DEFAULT_PARALLELISM_KEY,
    JM_MEMORY_DEFAULT_KEYS,
    JM_MEMORY_PREFIX,
    JM_PROCESS_SIZE_KEY,
    TASK_SLOTS_KEY,
    TM_MEMORY_DEFAULT_KEYS,
    TM_MEMORY_PREFIX,
    TM_PROCESS_SIZE_KEY,
    cluster_flink_config,
    custom_memory_keys,
    minicluster_flink_config,
)
from ytsaurus_flyt.models import (
    DEFAULT_JOBMANAGER_CPU,
    DEFAULT_JOBMANAGER_PROCESS_MEMORY,
    JVM_OVERHEAD_MIN,
    MIB,
    ClusterParams,
    ClusterPreset,
    JobmanagerParams,
    TaskmanagerParams,
    parse_memory,
)


def _int_option(user: Mapping[str, str], key: str) -> Optional[int]:
    raw = str(user.get(key) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as e:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from e
    if value < 1:
        raise ValueError(f"{key} must be >= 1, got {raw!r}")
    return value


def _process_memory(user: Mapping[str, str], key: str, prefix: str, flyt_keys: frozenset) -> Optional[int]:
    """The JVM size from ``key``, None for flyt's default; other memory keys need ``key`` as well."""
    raw = str(user.get(key) or "").strip()
    if raw:
        try:
            return parse_memory(raw)
        except ValueError as e:
            raise ValueError(f"{key} is not a memory size: {raw!r}") from e
    custom = custom_memory_keys(user, prefix, flyt_keys)
    if custom:
        raise ValueError(
            f"{', '.join(custom)} configure Flink's memory model, so flyt adds no memory defaults; "
            f"set {key} as well: it sizes the YT container"
        )
    return None


@dataclass(frozen=True)
class TaskmanagerSize:
    """One TaskManager, or the MiniCluster job: its cpu, its container (= ``process.size``) and the preset
    that sized it, None when ``taskmanager_cpu`` and ``taskmanager.memory.process.size`` did."""

    cpu: int
    memory: int
    preset: Optional[ClusterParams]


def resolve_taskmanager_size(
    config: FlytConfig, preset: Optional[ClusterParams], user: Mapping[str, str]
) -> TaskmanagerSize:
    """A preset (micro when none is given) or the user's own sizing; a preset is a starting point, so
    combining it with ``taskmanager_cpu`` or any ``taskmanager.memory.*`` key is an error."""
    own = (["taskmanager_cpu"] if config.taskmanager_cpu is not None else []) + sorted(
        k for k in user if k.startswith(TM_MEMORY_PREFIX)
    )
    if not own:
        preset = preset or ClusterPreset.MICRO.params
        _check_room_for_jvm("TaskManager", preset.memory_bytes())
        return TaskmanagerSize(cpu=preset.cpu, memory=preset.memory_bytes(), preset=preset)
    if preset is not None:
        raise ValueError(
            f"{', '.join(own)} size the TaskManager without a preset; remove the preset (--preset, "
            f"preset in the profile) or these settings"
        )
    missing = (["taskmanager_cpu (--tm-cpu)"] if config.taskmanager_cpu is None else []) + (
        [TM_PROCESS_SIZE_KEY] if TM_PROCESS_SIZE_KEY not in user else []
    )
    if missing:
        raise ValueError(f"a TaskManager sized without a preset needs {' and '.join(missing)}")
    process_memory = _process_memory(user, TM_PROCESS_SIZE_KEY, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS)
    assert process_memory is not None
    _check_room_for_jvm("TaskManager", process_memory)
    return TaskmanagerSize(cpu=config.taskmanager_cpu, memory=process_memory, preset=None)


def _check_room_for_jvm(role: str, process_memory: int) -> None:
    if process_memory <= JVM_OVERHEAD_MIN:
        raise ValueError(
            f"{role} memory {process_memory // MIB}m leaves no JVM next to the {JVM_OVERHEAD_MIN // MIB}m JVM overhead"
        )


@dataclass(frozen=True)
class ApplicationResources:
    """What an application cluster asks YT for, resolved by :func:`resolve_application_resources`."""

    taskmanager_count: int
    slots: int
    jobmanager_cpu: int
    jobmanager_process_memory: int
    taskmanager_cpu: int
    taskmanager_process_memory: int
    default_parallelism: int

    @property
    def capacity(self) -> int:
        """Task slots in the whole cluster."""
        return self.taskmanager_count * self.slots

    def summary(self) -> str:
        """One line for logs and ``flyt validate``."""
        return (
            f"1 JobManager ({self.jobmanager_cpu} cpu, {self.jobmanager_process_memory // MIB}m JVM) + "
            f"{self.taskmanager_count} TaskManager(s) x {self.slots} slot(s) "
            f"({self.taskmanager_cpu} cpu, {self.taskmanager_process_memory // MIB}m JVM each), "
            f"parallelism.default {self.default_parallelism}"
        )

    def jobmanager_params(self) -> JobmanagerParams:
        return JobmanagerParams(cpu=self.jobmanager_cpu, process_memory=self.jobmanager_process_memory)

    def taskmanager_params(self) -> TaskmanagerParams:
        return TaskmanagerParams(
            cpu=self.taskmanager_cpu,
            process_memory=self.taskmanager_process_memory,
            count=self.taskmanager_count,
            slots=self.slots,
        )


def resolve_application_resources(
    config: FlytConfig, preset: Optional[ClusterParams] = None, user: Optional[Mapping[str, str]] = None
) -> ApplicationResources:
    """Every TaskManager is sized by :func:`resolve_taskmanager_size`, with one slot per cpu; the JobManager
    gets flyt's defaults (cpu from ``jobmanager_cpu``). The user's Flink options
    (:func:`~ytsaurus_flyt.flink_conf.user_flink_config`) override slots, JVM sizes and the default parallelism."""
    user = user or {}
    taskmanager = resolve_taskmanager_size(config, preset, user)
    taskmanager_cpu = taskmanager.cpu
    slots = _int_option(user, TASK_SLOTS_KEY) or taskmanager_cpu
    taskmanager_process_memory = taskmanager.memory
    jobmanager_process_memory = (
        _process_memory(user, JM_PROCESS_SIZE_KEY, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS)
        or DEFAULT_JOBMANAGER_PROCESS_MEMORY
    )
    _check_room_for_jvm("JobManager", jobmanager_process_memory)
    capacity = config.taskmanager_count * slots
    default_parallelism = _int_option(user, DEFAULT_PARALLELISM_KEY) or capacity
    if default_parallelism > capacity:
        # A standalone cluster cannot grow: the job would wait for slots and fail after slot.request.timeout.
        raise ValueError(
            f"{DEFAULT_PARALLELISM_KEY} {default_parallelism} does not fit into taskmanager_count * slots = "
            f"{capacity}; raise taskmanager_count or {TASK_SLOTS_KEY}"
        )
    return ApplicationResources(
        taskmanager_count=config.taskmanager_count,
        slots=slots,
        jobmanager_cpu=config.jobmanager_cpu if config.jobmanager_cpu is not None else DEFAULT_JOBMANAGER_CPU,
        jobmanager_process_memory=jobmanager_process_memory,
        taskmanager_cpu=taskmanager_cpu,
        taskmanager_process_memory=taskmanager_process_memory,
        default_parallelism=default_parallelism,
    )


@dataclass(frozen=True)
class ApplicationCluster:
    """An application cluster ready for the spec: its YT resources and the ``config.yaml`` of every job."""

    resources: ApplicationResources
    flink_config: Dict[str, str]


def application_cluster(
    config: FlytConfig, preset: Optional[ClusterParams] = None, user: Optional[Mapping[str, str]] = None
) -> ApplicationCluster:
    """Resolve the application cluster from the profile, the preset that sizes each TaskManager and the
    user's Flink options."""
    user = user or {}
    resources = resolve_application_resources(config, preset, user)
    return ApplicationCluster(resources=resources, flink_config=cluster_flink_config(config, resources, user))


@dataclass(frozen=True)
class MiniClusterJob:
    """The MiniCluster YT job: its cpu, its container (``memory``) and its ``config.yaml``.
    ``flink_memory`` means the run script lays out the JVM by Flink's TaskManager memory model."""

    cpu: int
    memory: int
    flink_memory: bool
    flink_config: Dict[str, str]


def minicluster_job(
    config: FlytConfig, preset: Optional[ClusterParams] = None, user: Optional[Mapping[str, str]] = None
) -> MiniClusterJob:
    """The MiniCluster job is one TaskManager, sized as every TaskManager of application mode
    (:func:`resolve_taskmanager_size`)."""
    user = user or {}
    size = resolve_taskmanager_size(config, preset, user)
    process_memory = None if size.preset is not None else size.memory
    return MiniClusterJob(
        cpu=size.cpu,
        memory=size.memory,
        flink_memory=size.preset is None,
        flink_config=minicluster_flink_config(config, size.preset, user, cpu=size.cpu, process_memory=process_memory),
    )
