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
        "b": 1,
    }
    match = re.match(r"(\d+)(.*)", memory)
    if match is None:
        raise ValueError(f"Cannot parse memory value: {memory!r}")
    value = int(match.group(1))
    unit = match.group(2).lower().strip()
    if not unit:
        unit = "b"
    elif len(unit) == 1:
        if unit != "b":
            unit = unit + "b"
    if unit not in units:
        raise ValueError(f"Unknown memory unit: {unit!r} in {memory!r}")
    return value * units[unit]


@dataclass(frozen=True)
class ClusterParams:
    """Resource parameters for a Flink cluster running in a YT Vanilla operation."""

    cpu: int
    memory: str
    max_heap_size: str
    data_size_per_job: str
    disk_request_space: str = "10G"
    disk_request_medium_name: str = "default"

    def data_size_per_job_bytes(self) -> int:
        return parse_memory(self.data_size_per_job)

    def max_heap_size_bytes(self) -> int:
        return parse_memory(self.max_heap_size)

    def disk_request_space_bytes(self) -> int:
        return parse_memory(self.disk_request_space)

    def memory_bytes(self) -> int:
        return parse_memory(self.memory)


class ClusterPreset(Enum):
    """Pre-defined resource presets for Flink clusters."""

    MICRO = ClusterParams(
        cpu=2,
        memory="4G",
        max_heap_size="2324M",
        data_size_per_job="500M",
    )
    SMALL = ClusterParams(
        cpu=2,
        memory="8G",
        max_heap_size="4648M",
        data_size_per_job="1G",
    )
    LARGE = ClusterParams(
        cpu=4,
        memory="16G",
        max_heap_size="9296M",
        data_size_per_job="2G",
    )
    XLARGE = ClusterParams(
        cpu=8,
        memory="32G",
        max_heap_size="24G",
        data_size_per_job="2G",
    )

    @property
    def params(self) -> ClusterParams:
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
    """Parameters for the Flink JobManager task in a Vanilla operation."""

    cpu: int = 2
    memory: int = parse_memory("4G")
