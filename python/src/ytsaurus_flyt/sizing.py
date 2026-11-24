"""The size of one TaskManager, which is also the MiniCluster job: ``flyt.taskmanager.cpu.cores`` and
``taskmanager.memory.process.size``, flyt's defaults unless set."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ytsaurus_flyt.flink_conf import (
    FLYT_TM_CPU_KEY,
    TASK_SLOTS_KEY,
    TM_MEMORY_DEFAULT_KEYS,
    TM_MEMORY_PREFIX,
    TM_PROCESS_SIZE_KEY,
    int_option,
    process_memory_option,
)
from ytsaurus_flyt.models import DEFAULT_TASKMANAGER_CPU, DEFAULT_TASKMANAGER_PROCESS_MEMORY


@dataclass(frozen=True)
class WorkerSize:
    """A TaskManager, or the MiniCluster job: its cpu and ``taskmanager.memory.process.size``, its YT container."""

    cpu: int
    memory: int


def resolve_taskmanager_size(user: Mapping[str, str]) -> WorkerSize:
    """The cpu defaults to the task slots when they are set."""
    return WorkerSize(
        cpu=int_option(user, FLYT_TM_CPU_KEY) or int_option(user, TASK_SLOTS_KEY) or DEFAULT_TASKMANAGER_CPU,
        memory=process_memory_option(
            user, TM_PROCESS_SIZE_KEY, TM_MEMORY_PREFIX, TM_MEMORY_DEFAULT_KEYS, DEFAULT_TASKMANAGER_PROCESS_MEMORY
        ),
    )
