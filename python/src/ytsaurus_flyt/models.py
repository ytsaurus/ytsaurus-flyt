"""Data models for ytsaurus-flyt Vanilla operations."""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


def parse_memory(memory: Union[str, int]) -> int:
    """Parse a human-readable memory string (e.g. '4G', '512M') into bytes.

    Supported suffixes: G/GB, M/MB, K/KB, B (case-insensitive).
    If an integer is passed, it is returned as-is (assumed to be bytes).
    """
    if isinstance(memory, int):
        return memory
    units = {
        "gb": 1024 * 1024 * 1024,
        "mb": 1024 * 1024,
        "kb": 1024,
        "bb": 1,
        "b": 1,
    }
    match = re.match(r"(\d+)(.*)", memory)
    if match is None:
        raise ValueError(f"Cannot parse memory value: {memory!r}")
    value = int(match.group(1))
    unit = match.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    if unit not in units:
        raise ValueError(f"Unknown memory unit: {unit!r} in {memory!r}")
    return value * units[unit]


# Application-mode JobManager defaults: its load barely depends on the job (dispatcher, REST, blob
# server, plan compilation). TaskManagers are sized by the preset.
DEFAULT_JOBMANAGER_CPU = 2
DEFAULT_JOBMANAGER_PROCESS_MEMORY = parse_memory("2G")
# Memory of the JobManager container outside the Flink JVM: the PyFlink driver (the user's script and
# its imports), bash, the discovery helper and a sidecar.
JOBMANAGER_CONTAINER_OVERHEAD = parse_memory("512M")
# Memory of a TaskManager container outside the Flink JVM: bash, the discovery helper and a sidecar.
# Python UDF workers are not here: Flink reserves managed memory for them inside process.size.
TASKMANAGER_CONTAINER_OVERHEAD = parse_memory("512M")


@dataclass(frozen=True)
class MiniClusterParams:
    """The MiniCluster YT job (``cluster_mode: minicluster``): container cpu and memory, the JVM heap
    (``-Xmx``) and optionally its direct memory (``-XX:MaxDirectMemorySize``)."""

    cpu: int
    memory: str
    max_heap_size: str
    data_size_per_job: str
    disk_request_space: str = "10G"
    disk_request_medium_name: str = "default"
    off_heap_size: Optional[str] = None
    """``data_size_per_job`` and ``disk_request_*`` are for launchers that run the same MiniCluster
    inside YT Map operations (input bytes per job, sandbox disk quota); flyt's Vanilla operations
    do not use them."""

    def data_size_per_job_bytes(self) -> int:
        return parse_memory(self.data_size_per_job)

    def max_heap_size_bytes(self) -> int:
        return parse_memory(self.max_heap_size)

    def off_heap_size_bytes(self) -> Optional[int]:
        return parse_memory(self.off_heap_size) if self.off_heap_size else None

    def disk_request_space_bytes(self) -> int:
        return parse_memory(self.disk_request_space)

    def memory_bytes(self) -> int:
        return parse_memory(self.memory)


class MiniClusterPreset(Enum):
    """Pre-defined sizes: the MiniCluster YT job, or every TaskManager YT job in application mode."""

    MICRO = MiniClusterParams(
        cpu=2,
        memory="4G",
        max_heap_size="2324M",
        data_size_per_job="500M",
    )
    SMALL = MiniClusterParams(
        cpu=2,
        memory="8G",
        max_heap_size="4648M",
        data_size_per_job="1G",
    )
    LARGE = MiniClusterParams(
        cpu=4,
        memory="16G",
        max_heap_size="9296M",
        data_size_per_job="2G",
    )
    XLARGE = MiniClusterParams(
        cpu=8,
        memory="32G",
        max_heap_size="24G",
        data_size_per_job="2G",
    )

    @property
    def params(self) -> MiniClusterParams:
        return self.value


@dataclass
class OperationParams:
    """Parameters for a YT Vanilla operation."""

    pool: Optional[str] = None
    file_paths: List[str] = field(default_factory=list)
    layer_paths: List[str] = field(default_factory=list)
    description: Optional[str] = None
    max_failed_job_count: Optional[int] = None
    acl: Optional[List[Dict[str, Any]]] = None


@dataclass
class JobmanagerParams:
    """The JobManager YT job of ``cluster_mode: application``: ``process_memory`` is the Flink JVM
    (``jobmanager.memory.process.size``), the container gets it plus :data:`JOBMANAGER_CONTAINER_OVERHEAD`."""

    cpu: int
    process_memory: int

    def __post_init__(self) -> None:
        if self.process_memory < 1:
            raise ValueError(f"JobmanagerParams.process_memory must be positive, got {self.process_memory}")

    @property
    def memory(self) -> int:
        """YT ``memory_limit`` of the JobManager job."""
        return self.process_memory + JOBMANAGER_CONTAINER_OVERHEAD


@dataclass
class TaskmanagerParams:
    """Parameters for the Flink TaskManager task (``cluster_mode: application`` only).

    ``count`` TaskManager YT jobs are started, each with ``slots`` task slots; the default
    job parallelism becomes ``count * slots``. ``process_memory`` is the Flink JVM
    (``taskmanager.memory.process.size``); the YT container gets it plus
    :data:`TASKMANAGER_CONTAINER_OVERHEAD`, the way a Kubernetes pod gets ``process.size``.
    """

    cpu: int
    process_memory: int
    count: int
    slots: int

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError(f"TaskmanagerParams.count must be >= 1, got {self.count}")
        if self.slots < 1:
            raise ValueError(f"TaskmanagerParams.slots must be >= 1, got {self.slots}")
        if self.process_memory < 1:
            raise ValueError(f"TaskmanagerParams.process_memory must be positive, got {self.process_memory}")

    @property
    def memory(self) -> int:
        """YT ``memory_limit`` of one TaskManager YT job."""
        return self.process_memory + TASKMANAGER_CONTAINER_OVERHEAD
