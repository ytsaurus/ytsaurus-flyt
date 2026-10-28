"""The application cluster of a run: the YT resources of its JobManager and TaskManagers, resolved from
the flyt fields and the Flink options, and the ``config.yaml`` they all share.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple

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
    flink_config_warnings,
    rest_port,
    user_flink_config,
)
from ytsaurus_flyt.models import (
    DEFAULT_JOBMANAGER_CPU,
    DEFAULT_JOBMANAGER_PROCESS_MEMORY,
    DEFAULT_TASKMANAGER_PROCESS_MEMORY,
    JobmanagerParams,
    TaskmanagerParams,
    parse_memory,
)

_MIB = 1024 * 1024


def _int_option(user: Mapping[str, str], key: str, default: int) -> int:
    raw = user.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as e:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from e
    if value < 1:
        raise ValueError(f"{key} must be >= 1, got {raw!r}")
    return value


def _memory_option(user: Mapping[str, str], key: str) -> Optional[int]:
    raw = user.get(key)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return parse_memory(str(raw).strip())
    except ValueError as e:
        raise ValueError(f"{key} is not a memory size: {raw!r}") from e


def _process_memory(user: Mapping[str, str], key: str, prefix: str, flyt_keys: frozenset, default: int) -> int:
    size = _memory_option(user, key)
    if size is not None:
        return size
    custom = custom_memory_keys(user, prefix, flyt_keys)
    if custom:
        raise ValueError(
            f"{', '.join(custom)} configure Flink's memory model, so flyt adds no memory defaults; "
            f"set {key} as well: it sizes the YT container"
        )
    return default


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
            f"1 JobManager ({self.jobmanager_cpu} cpu, {self.jobmanager_process_memory // _MIB}m JVM) + "
            f"{self.taskmanager_count} TaskManager(s) x {self.slots} slot(s) "
            f"({self.taskmanager_cpu} cpu, {self.taskmanager_process_memory // _MIB}m JVM each), "
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

    def check_parallelism_fits(self) -> None:
        """A standalone cluster cannot grow: a default parallelism above the capacity would wait for
        slots and fail after ``slot.request.timeout``."""
        if self.default_parallelism > self.capacity:
            raise ValueError(
                f"parallelism.default {self.default_parallelism} does not fit into taskmanager_count * slots = "
                f"{self.capacity}; raise taskmanager_count or {TASK_SLOTS_KEY}"
            )


def resolve_application_resources(config: FlytConfig, user: Mapping[str, str]) -> ApplicationResources:
    """Application-mode sizes with flyt's defaults applied, from the flyt fields and the user's Flink
    options (:func:`user_flink_config`). The single place where those defaults live.
    """
    slots = _int_option(user, TASK_SLOTS_KEY, default=1)
    return ApplicationResources(
        taskmanager_count=config.taskmanager_count,
        slots=slots,
        jobmanager_cpu=config.jobmanager_cpu if config.jobmanager_cpu is not None else DEFAULT_JOBMANAGER_CPU,
        jobmanager_process_memory=_process_memory(
            user, JM_PROCESS_SIZE_KEY, JM_MEMORY_PREFIX, JM_MEMORY_DEFAULT_KEYS, DEFAULT_JOBMANAGER_PROCESS_MEMORY
        ),
        # One cpu per slot unless set, like kubernetes.taskmanager.cpu.
        taskmanager_cpu=config.taskmanager_cpu if config.taskmanager_cpu is not None else slots,
        taskmanager_process_memory=_process_memory(
            user, TM_PROCESS_SIZE_KEY, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS, DEFAULT_TASKMANAGER_PROCESS_MEMORY
        ),
        # Every slot of every TaskManager unless the Flink options say otherwise.
        default_parallelism=_int_option(user, DEFAULT_PARALLELISM_KEY, default=config.taskmanager_count * slots),
    )


@dataclass(frozen=True)
class ApplicationCluster:
    """An application cluster ready for the spec: its YT resources and the ``config.yaml`` of every job."""

    resources: ApplicationResources
    flink_config: Dict[str, str]
    warnings: Tuple[str, ...] = ()

    @property
    def rest_port(self) -> int:
        """Port of the JobManager's REST endpoint: discovery and the Web UI."""
        return rest_port(self.flink_config)


def application_cluster(config: FlytConfig, conf_file: Optional[Mapping[str, str]] = None) -> ApplicationCluster:
    """Resolve the application cluster from the profile and the flattened ``base_flink_conf_path`` file."""
    user = user_flink_config(config, conf_file)
    resources = resolve_application_resources(config, user)
    return ApplicationCluster(
        resources=resources,
        flink_config=cluster_flink_config(config, resources, user),
        warnings=tuple(flink_config_warnings(user)),
    )
