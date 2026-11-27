"""The MiniCluster YT job of a run: one TaskManager (:mod:`ytsaurus_flyt.sizing`) and its ``config.yaml``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    DEFAULT_PARALLELISM_KEY,
    JVM_COMMON_OPTS,
    JVM_DEFAULT_OPTS_KEYS,
    JVM_EXIT_ON_OOM_OPT,
    TM_CPU_CORES_KEY,
    apply_user_and_owned,
    common_defaults,
    taskmanager_memory_defaults,
)
from ytsaurus_flyt.sizing import WorkerSize, resolve_taskmanager_size


def minicluster_flink_config(config: FlytConfig, size: WorkerSize, user: Mapping[str, str]) -> Dict[str, str]:
    """The ``config.yaml`` of a MiniCluster job: the same layers as
    :func:`~ytsaurus_flyt.application.application_flink_config`. The job is one TaskManager of
    ``taskmanager.memory.process.size``; the run script adds the JVM flags Flink derives for it."""
    props = common_defaults(config)
    # Every cpu of the job, like every slot of an application cluster (and like PyFlink's local default).
    props[DEFAULT_PARALLELISM_KEY] = str(size.cpu)
    taskmanager_memory_defaults(props, user, size.memory)
    jvm_opts = [f"-XX:ActiveProcessorCount={size.cpu}", JVM_EXIT_ON_OOM_OPT, *JVM_COMMON_OPTS]
    apply_user_and_owned(props, user, {JVM_DEFAULT_OPTS_KEYS[0]: " ".join(jvm_opts)})
    props[TM_CPU_CORES_KEY] = str(size.cpu)
    return props


@dataclass(frozen=True)
class MiniCluster:
    """The MiniCluster YT job: its cpu, its container (``memory``) and its ``config.yaml``, with the user's Flink
    options they come from."""

    cpu: int
    memory: int
    flink_config: Dict[str, str]
    flink_options: Dict[str, str]


def minicluster(config: FlytConfig, user: Optional[Mapping[str, str]] = None) -> MiniCluster:
    """The MiniCluster job is one TaskManager, sized as every TaskManager of application mode
    (:func:`resolve_taskmanager_size`)."""
    user = user or {}
    size = resolve_taskmanager_size(user)
    return MiniCluster(
        cpu=size.cpu,
        memory=size.memory,
        flink_config=minicluster_flink_config(config, size, user),
        flink_options=dict(user),
    )
