"""The MiniCluster YT job of a run: one TaskManager (:mod:`ytsaurus_flyt.sizing`) and its ``config.yaml``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    DEFAULT_PARALLELISM_KEY,
    JVM_COMMON_OPTS,
    JVM_DEFAULT_OPTS_KEYS,
    JVM_EXIT_ON_OOM_OPT,
    TM_CPU_CORES_KEY,
    TM_MEMORY_PREFIX,
    apply_user_and_owned,
    common_defaults,
    jvm_overhead_floor,
    taskmanager_memory_defaults,
)
from ytsaurus_flyt.models import ClusterParams
from ytsaurus_flyt.sizing import CustomSize, PresetSize, WorkerSize, resolve_taskmanager_size


def minicluster_flink_config(config: FlytConfig, size: WorkerSize, user: Mapping[str, str]) -> Dict[str, str]:
    """The ``config.yaml`` of a MiniCluster job: the same layers as
    :func:`~ytsaurus_flyt.application.application_flink_config`. A preset sizes the job's JVM, which the PyFlink
    gateway starts with ``env.java.*``; without one, the job is one TaskManager of
    ``taskmanager.memory.process.size``, and the run script adds the JVM flags Flink derives for it."""
    props = common_defaults(config)
    # Every cpu of the job, like every slot of an application cluster (and like PyFlink's local default).
    props[DEFAULT_PARALLELISM_KEY] = str(size.cpu)
    jvm_opts: List[str] = []
    if isinstance(size, PresetSize):
        jvm_opts.append(f"-Xmx{size.preset.max_heap_size}")
        if size.preset.off_heap_size:
            # Bounds NIO/netty direct buffers (gRPC connectors); otherwise the JVM
            # defaults MaxDirectMemorySize to ~the max heap size.
            jvm_opts.append(f"-XX:MaxDirectMemorySize={size.preset.off_heap_size}")
    else:
        taskmanager_memory_defaults(props, user, size.memory)
    jvm_opts += [f"-XX:ActiveProcessorCount={size.cpu}", JVM_EXIT_ON_OOM_OPT, *JVM_COMMON_OPTS]
    apply_user_and_owned(props, user, {JVM_DEFAULT_OPTS_KEYS[0]: " ".join(jvm_opts)})
    props[TM_CPU_CORES_KEY] = str(size.cpu)
    if isinstance(size, CustomSize):
        props.update(jvm_overhead_floor(props, (TM_MEMORY_PREFIX,)))
    return props


@dataclass(frozen=True)
class MiniCluster:
    """The MiniCluster YT job: its cpu, its container (``memory``) and its ``config.yaml``.
    ``flink_memory`` means the run script lays out the JVM by Flink's TaskManager memory model."""

    cpu: int
    memory: int
    flink_memory: bool
    flink_config: Dict[str, str]


def minicluster(
    config: FlytConfig, preset: Optional[ClusterParams] = None, user: Optional[Mapping[str, str]] = None
) -> MiniCluster:
    """The MiniCluster job is one TaskManager, sized as every TaskManager of application mode
    (:func:`resolve_taskmanager_size`)."""
    user = user or {}
    size = resolve_taskmanager_size(preset, user)
    return MiniCluster(
        cpu=size.cpu,
        memory=size.memory,
        flink_memory=isinstance(size, CustomSize),
        flink_config=minicluster_flink_config(config, size, user),
    )
