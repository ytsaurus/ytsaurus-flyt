"""Data models for ytsaurus-flyt Vanilla operations."""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

MIB = 1024 * 1024


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
# server, plan compilation). Its options are jobmanager.memory.process.size and flyt.jobmanager.cpu.cores.
DEFAULT_JOBMANAGER_CPU = 2
DEFAULT_JOBMANAGER_PROCESS_MEMORY = parse_memory("2560M")
# The YT container is process.size, so Flink's jvm-overhead holds the JVM's native memory and every process
# outside the JVM; measured ~360m on a TaskManager, ~240m on a JobManager.
JVM_OVERHEAD_MIN = parse_memory("512M")
# A TaskManager, or the MiniCluster job, the Flink configuration does not size.
DEFAULT_TASKMANAGER_CPU = 2
DEFAULT_TASKMANAGER_PROCESS_MEMORY = parse_memory("4G")


@dataclass
class OperationParams:
    """Parameters for a YT Vanilla operation."""

    pool: Optional[str] = None
    file_paths: List[str] = field(default_factory=list)
    layer_paths: List[str] = field(default_factory=list)
    description: Optional[str] = None
    max_failed_job_count: Optional[int] = None
    acl: Optional[List[Dict[str, Any]]] = None
