"""The size of one TaskManager, which is also the MiniCluster job: either a preset or the user's own
sizing by ``flyt.taskmanager.cpu.cores`` and ``taskmanager.memory.*`` keys, never both."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Union

from ytsaurus_flyt.flink_conf import (
    FLYT_TM_CPU_KEY,
    TASK_SLOTS_KEY,
    TM_MEMORY_PREFIX,
    TM_PROCESS_SIZE_KEY,
    FlinkConfigError,
    int_option,
    memory_option,
)
from ytsaurus_flyt.models import JVM_OVERHEAD_MIN, MIB, ClusterParams, ClusterPreset


@dataclass(frozen=True)
class PresetSize:
    """A TaskManager, or the MiniCluster job, sized by a preset."""

    preset: ClusterParams

    @property
    def cpu(self) -> int:
        return self.preset.cpu

    @property
    def memory(self) -> int:
        """The YT container."""
        return self.preset.memory_bytes()


@dataclass(frozen=True)
class CustomSize:
    """A TaskManager, or the MiniCluster job, sized by ``flyt.taskmanager.cpu.cores`` and
    ``taskmanager.memory.process.size``, which is also the YT container."""

    cpu: int
    memory: int


WorkerSize = Union[PresetSize, CustomSize]


def resolve_taskmanager_size(preset: Optional[ClusterParams], user: Mapping[str, str]) -> WorkerSize:
    """A preset (micro when none is given) or the user's own sizing; a preset is a starting point, so
    combining it with ``flyt.taskmanager.cpu.cores`` or any ``taskmanager.memory.*`` key is an error.
    Without a preset the cpu defaults to the task slots, 1 unless set."""
    cpu = int_option(user, FLYT_TM_CPU_KEY)
    own = ([FLYT_TM_CPU_KEY] if cpu is not None else []) + sorted(k for k in user if k.startswith(TM_MEMORY_PREFIX))
    if not own:
        preset = preset or ClusterPreset.MICRO.params
        check_room_for_jvm("TaskManager", preset.memory_bytes())
        return PresetSize(preset)
    if preset is not None:
        raise FlinkConfigError(
            f"{', '.join(own)} size the TaskManager without a preset; remove the preset (--preset, "
            f"preset in the profile) or these settings"
        )
    if not str(user.get(TM_PROCESS_SIZE_KEY) or "").strip():
        raise FlinkConfigError(f"a TaskManager sized without a preset needs {TM_PROCESS_SIZE_KEY}")
    process_memory = memory_option(user, TM_PROCESS_SIZE_KEY)
    check_room_for_jvm("TaskManager", process_memory)
    return CustomSize(cpu=cpu or int_option(user, TASK_SLOTS_KEY) or 1, memory=process_memory)


def check_room_for_jvm(role: str, process_memory: int) -> None:
    if process_memory <= JVM_OVERHEAD_MIN:
        raise FlinkConfigError(
            f"{role} memory {process_memory // MIB}m leaves no JVM next to the {JVM_OVERHEAD_MIN // MIB}m JVM overhead"
        )
