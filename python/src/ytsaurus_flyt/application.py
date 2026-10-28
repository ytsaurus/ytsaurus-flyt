"""The application cluster of a run: the YT resources of its JobManager and TaskManagers and the
``config.yaml`` they all share. TaskManagers are sized by the preset, the JobManager by flyt's defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import cluster_flink_config
from ytsaurus_flyt.models import (
    DEFAULT_JOBMANAGER_CPU,
    DEFAULT_JOBMANAGER_PROCESS_MEMORY,
    TASKMANAGER_CONTAINER_OVERHEAD,
    ClusterParams,
    JobmanagerParams,
    TaskmanagerParams,
)

_MIB = 1024 * 1024


@dataclass(frozen=True)
class ApplicationResources:
    """What an application cluster asks YT for, resolved by :func:`resolve_application_resources`."""

    taskmanager_count: int
    slots: int
    jobmanager_cpu: int
    jobmanager_process_memory: int
    taskmanager_cpu: int
    taskmanager_process_memory: int

    @property
    def default_parallelism(self) -> int:
        """Every slot of every TaskManager; the script may set its own parallelism in code."""
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


def resolve_application_resources(config: FlytConfig, taskmanager_size: ClusterParams) -> ApplicationResources:
    """Every TaskManager gets the preset's cpu (or ``taskmanager_cpu``) and container memory, with one slot
    per cpu; the JobManager gets flyt's defaults (cpu from ``jobmanager_cpu``)."""
    taskmanager_cpu = config.taskmanager_cpu if config.taskmanager_cpu is not None else taskmanager_size.cpu
    # The container is the preset memory: the Flink JVM plus the fixed overhead for bash, the helper and a sidecar.
    taskmanager_process_memory = taskmanager_size.memory_bytes() - TASKMANAGER_CONTAINER_OVERHEAD
    if taskmanager_process_memory < 1:
        raise ValueError(
            f"preset memory {taskmanager_size.memory} leaves no TaskManager JVM next to the "
            f"{TASKMANAGER_CONTAINER_OVERHEAD // _MIB}m container overhead"
        )
    return ApplicationResources(
        taskmanager_count=config.taskmanager_count,
        slots=taskmanager_cpu,
        jobmanager_cpu=config.jobmanager_cpu if config.jobmanager_cpu is not None else DEFAULT_JOBMANAGER_CPU,
        jobmanager_process_memory=DEFAULT_JOBMANAGER_PROCESS_MEMORY,
        taskmanager_cpu=taskmanager_cpu,
        taskmanager_process_memory=taskmanager_process_memory,
    )


@dataclass(frozen=True)
class ApplicationCluster:
    """An application cluster ready for the spec: its YT resources and the ``config.yaml`` of every job."""

    resources: ApplicationResources
    flink_config: Dict[str, str]


def application_cluster(config: FlytConfig, taskmanager_size: ClusterParams) -> ApplicationCluster:
    """Resolve the application cluster from the profile and the preset that sizes each TaskManager."""
    resources = resolve_application_resources(config, taskmanager_size)
    return ApplicationCluster(resources=resources, flink_config=cluster_flink_config(config, resources))
