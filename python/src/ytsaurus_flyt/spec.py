"""Build YTsaurus Vanilla operation specs for PyFlink jobs."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any, Dict, List, Optional

from yt.wrapper.spec_builders import VanillaSpecBuilder

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import JobmanagerParams, OperationParams, TaskmanagerParams, parse_memory

FLINK_STANDALONE_FLAG = "FLINK_STANDALONE"

# Environment read by the application-mode run scripts and the in-job helper.
FLYT_CLUSTER_MODE_ENV = "FLYT_CLUSTER_MODE"
FLYT_YT_PROXY_ENV = "FLYT_YT_PROXY"

# Vanilla task names in application mode; ``flink`` stays the MiniCluster task name.
MINICLUSTER_TASK = "flink"
JOBMANAGER_TASK = "jobmanager"
TASKMANAGER_TASK = "taskmanager"

# JobManager ports are fixed (one JobManager per operation, own slot IP); TaskManager ports are
# random so several TaskManagers can share an exec node without porto network isolation.
FLINK_REST_PORT = 27050
FLINK_JOBMANAGER_RPC_PORT = 27051
FLINK_BLOB_SERVER_PORT = 27052

# Share of the TaskManager container left to the Flink JVM; the rest goes to Python UDF workers.
TASKMANAGER_PROCESS_MEMORY_FRACTION = 0.75
# Default task off-heap share of the TaskManager JVM when no off_heap is configured: Flink pins
# -XX:MaxDirectMemorySize to framework + task off-heap, and connectors (gRPC, netty) need direct buffers.
TASKMANAGER_TASK_OFF_HEAP_FRACTION = 0.125
# Flink reserves 40% of a TaskManager for managed memory, which only RocksDB, batch operators and
# Python UDF workers use; flyt jobs are stateless with the heap state backend, so most of it would
# sit idle while task heap starves. 10% keeps a slice for Python UDF workers.
TASKMANAGER_MANAGED_MEMORY_FRACTION = "0.1"
# A JVM that survives an OutOfMemoryError hangs in GC, misses heartbeats and is never restarted by YT.
JVM_EXIT_ON_OOM_OPT = "-XX:+ExitOnOutOfMemoryError"
_RUN_SCRIPTS_DIR = Path(__file__).parent / "run_scripts"
JOB_HELPER_FILENAME = "flyt_job_helper.py"


def _split_job_command_tokens_posix(job_command: str) -> list[str]:
    """POSIX shell tokenization for job command strings (exec runs on Linux)."""
    s = (job_command or "").strip()
    if not s:
        return []
    return shlex.split(s, posix=True)


def _job_args_for_shell(job_command: str) -> str:
    """Shell word list for ``set --`` (shlex-split + quote each token)."""
    return " ".join(shlex.quote(p) for p in _split_job_command_tokens_posix(job_command))


def _extract_service_name(job_command: str) -> str:
    parts = _split_job_command_tokens_posix(job_command)
    for part in parts:
        if "/" in part and not part.startswith("-"):
            segments = part.split("/")
            if len(segments) >= 2 and segments[0] == "services":
                return segments[1]
            return segments[0]
    return "unknown"


def _script_fragments(*, use_squashfs_sandbox_unpack: bool, role: Optional[str]) -> List[str]:
    """Ordered run-script fragments for a MiniCluster job or an application-mode role."""
    ordered = ["00_set_essentials.sh"]
    if use_squashfs_sandbox_unpack:
        ordered.append("00_unpack_runtime_squashfs.sh")
    ordered.append("01_prepare_squashfs.sh")
    if role is not None:
        ordered.append("02_prepare_cluster_bins.sh")
    ordered += ["21_prepare_libs.sh", "30_prepare_service.sh"]
    if role is None:
        ordered.append("40_run_job.sh")
    elif role == JOBMANAGER_TASK:
        ordered += ["35_cluster_common.sh", "36_start_sidecar.sh", "41_run_jobmanager.sh"]
    elif role == TASKMANAGER_TASK:
        ordered += ["35_cluster_common.sh", "36_start_sidecar.sh", "42_run_taskmanager.sh"]
    else:
        raise ValueError(f"Unknown application-mode role: {role!r}")
    return ordered


def _build_run_script(
    job_command: str,
    config: FlytConfig,
    service_name: str,
    *,
    use_squashfs_sandbox_unpack: bool = False,
    role: Optional[str] = None,
    flink_args: Optional[List[str]] = None,
) -> str:
    result_script = ["#!/bin/bash", "set -e"]
    fmt: Dict[str, Any] = {
        "job_args": _job_args_for_shell(job_command),
        "service_name": service_name,
        "python_bin": config.python_bin,
    }
    if role is not None:
        # Values are substituted into the templates, not re-formatted, so braces in the helper are safe.
        fmt.update(
            {
                "flink_args": " ".join(shlex.quote(a) for a in (flink_args or [])),
                "job_helper": (_RUN_SCRIPTS_DIR / JOB_HELPER_FILENAME).read_text(encoding="utf-8"),
                "job_helper_filename": JOB_HELPER_FILENAME,
                "restart_completed_jobs": "1" if config.restart_completed_jobs else "0",
                "sidecar_command": shlex.quote(config.sidecar_command) if config.sidecar_command else "''",
                "rest_port": FLINK_REST_PORT,
                "rpc_port": FLINK_JOBMANAGER_RPC_PORT,
                "discovery_timeout": config.discovery_timeout,
            }
        )

    for name in _script_fragments(use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack, role=role):
        script_path = _RUN_SCRIPTS_DIR / name
        result_script.append(f"echo 'Running {script_path.name}...' 1>&2")
        with open(script_path, encoding="utf-8") as script_file:
            result_script.append(script_file.read().format(**fmt))

    return "\n".join(result_script)


DEFAULT_TASK_SPEC = {
    "restart_completed_jobs": True,
    "memory_reserve_factor": 1.0,
    "max_speculative_job_count_per_task": 0,
}

DEFAULT_ENVIRONMENT = {
    "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
}


_MIB = 1024 * 1024
# Flink JobManager memory model defaults (off-heap, metaspace, JVM overhead bounds and fraction).
_JM_DEFAULT_OFF_HEAP = 128 * _MIB
_JM_METASPACE = 256 * _MIB
_JM_OVERHEAD_MIN = 192 * _MIB
_JM_OVERHEAD_MAX = 1024 * _MIB
_JM_OVERHEAD_FRACTION = 0.1


def _mebibytes(size_bytes: int) -> str:
    return f"{max(int(size_bytes) // _MIB, 1)}m"


def jobmanager_process_size(max_heap_size_str: str, off_heap_size_str: Optional[str]) -> str:
    """``jobmanager.memory.process.size`` that makes Flink derive a heap of ``max_heap_size_str``.

    The distribution's config.yaml ships a small ``process.size``, so it must be overridden as a whole:
    heap + off-heap + metaspace + JVM overhead (a fraction of the total, clamped by Flink's bounds).
    """
    heap = parse_memory(max_heap_size_str)
    off_heap = parse_memory(off_heap_size_str) if off_heap_size_str else _JM_DEFAULT_OFF_HEAP
    base = heap + off_heap + _JM_METASPACE
    overhead = base * _JM_OVERHEAD_FRACTION / (1 - _JM_OVERHEAD_FRACTION)
    overhead = min(max(overhead, _JM_OVERHEAD_MIN), _JM_OVERHEAD_MAX)
    return _mebibytes(int(base + overhead))


def flink_dynamic_properties(
    config: FlytConfig,
    jobmanager_params: JobmanagerParams,
    taskmanager_params: TaskmanagerParams,
    max_heap_size_str: str,
    off_heap_size_str: Optional[str],
) -> Dict[str, str]:
    """Flink options shared by the JobManager and TaskManagers in application mode.

    Addresses are appended at runtime by the run scripts (slot IP, discovered JobManager).
    ``config.flink_config`` is applied last and overrides any generated value.
    """
    tm_process_size = int(taskmanager_params.memory * TASKMANAGER_PROCESS_MEMORY_FRACTION)
    props: Dict[str, str] = {
        "rest.port": str(FLINK_REST_PORT),
        "rest.bind-address": "0.0.0.0",
        "jobmanager.rpc.port": str(FLINK_JOBMANAGER_RPC_PORT),
        "jobmanager.bind-host": "0.0.0.0",
        "blob.server.port": str(FLINK_BLOB_SERVER_PORT),
        "taskmanager.bind-host": "0.0.0.0",
        "taskmanager.rpc.port": "0",
        "taskmanager.data.port": "0",
        "taskmanager.numberOfTaskSlots": str(taskmanager_params.slots),
        "parallelism.default": str(
            config.parallelism
            if config.parallelism is not None
            else taskmanager_params.count * taskmanager_params.slots
        ),
        # Without checkpointing Flink defaults to no restarts: a lost TaskManager would fail the
        # driver and turn into a full gang restart instead of an in-cluster job restart.
        "restart-strategy.type": "exponential-delay",
        "jobmanager.memory.process.size": jobmanager_process_size(max_heap_size_str, off_heap_size_str),
        "taskmanager.memory.process.size": _mebibytes(tm_process_size),
        "taskmanager.memory.task.off-heap.size": _mebibytes(
            taskmanager_params.off_heap
            if taskmanager_params.off_heap is not None
            else int(tm_process_size * TASKMANAGER_TASK_OFF_HEAP_FRACTION)
        ),
        "taskmanager.memory.managed.fraction": TASKMANAGER_MANAGED_MEMORY_FRACTION,
        # Task-thread OOMs kill the TaskManager (Flink option); ExitOnOutOfMemoryError covers every
        # other thread. Either way YT restarts the job and Flink recovers instead of hanging.
        "taskmanager.jvm-exit-on-oom": "true",
        "python.executable": config.python_bin,
        "python.client.executable": config.python_bin,
        # YT exec nodes are IPv6-first; pin the JVM processor count to the container CPU limit.
        "env.java.opts.all": "-Djava.net.preferIPv6Addresses=true",
        "env.java.opts.jobmanager": f"-XX:ActiveProcessorCount={jobmanager_params.cpu} {JVM_EXIT_ON_OOM_OPT}",
        "env.java.opts.taskmanager": f"-XX:ActiveProcessorCount={taskmanager_params.cpu} {JVM_EXIT_ON_OOM_OPT}",
    }
    if off_heap_size_str:
        props["jobmanager.memory.off-heap.size"] = off_heap_size_str
    props.update(config.flink_config)
    return props


def _dynamic_property_args(props: Dict[str, str]) -> List[str]:
    return [f"-D{k}={v}" for k, v in props.items()]


def _begin_task(
    builder: VanillaSpecBuilder,
    name: str,
    *,
    command: str,
    job_count: int,
    cpu: int,
    memory: int,
    task_spec: Dict[str, Any],
    layer_paths: List[str],
) -> None:
    task = builder.begin_task(name).command(command).job_count(job_count).cpu_limit(cpu).memory_limit(memory)
    if layer_paths:
        task = task.tmpfs_path(".")
    task.copy_files(True).spec(task_spec).end_task()


def build_vanilla_operation_spec(
    title: str,
    job_command: str,
    config: FlytConfig,
    operation_params: OperationParams,
    jobmanager_params: JobmanagerParams,
    secure_vault: dict[str, str],
    max_heap_size_str: str = "2324M",
    *,
    off_heap_size_str: str | None = None,
    use_squashfs_sandbox_unpack: bool = False,
    taskmanager_params: Optional[TaskmanagerParams] = None,
    yt_proxy: Optional[str] = None,
) -> VanillaSpecBuilder:
    """Build a Vanilla operation spec to launch a Flink job (SquashFS runtime only).

    ``cluster_mode: minicluster`` yields a single ``flink`` task. ``cluster_mode: application``
    yields a gang ``jobmanager`` task and a ``taskmanager`` task; ``taskmanager_params`` and
    ``yt_proxy`` (HTTP proxy the TaskManagers query to discover the JobManager) are required then.
    """
    service_name = config.service_name or _extract_service_name(job_command)
    application = config.is_application_cluster
    if application:
        if taskmanager_params is None:
            raise ValueError("taskmanager_params is required for cluster_mode: application")
        if not (yt_proxy or "").strip():
            raise ValueError("yt_proxy is required for cluster_mode: application")

    environment: Dict[str, str] = {
        **DEFAULT_ENVIRONMENT,
        "JAVA_HOME": config.java_home,
        FLINK_STANDALONE_FLAG: "True",
    }
    if application:
        # Memory and JVM flags go through Flink's own memory model (-D options), never -Xmx.
        environment[FLYT_CLUSTER_MODE_ENV] = "application"
        environment[FLYT_YT_PROXY_ENV] = str(yt_proxy).strip()
    else:
        java_opts = f"-Xmx{max_heap_size_str}"
        if off_heap_size_str:
            # Bounds NIO/netty direct buffers (gRPC connectors); otherwise the JVM
            # defaults MaxDirectMemorySize to ~the max heap size.
            java_opts += f" -XX:MaxDirectMemorySize={off_heap_size_str}"
        if jobmanager_params.cpu:
            # Pin the JVM's processor count to the container CPU limit.
            java_opts += f" -XX:ActiveProcessorCount={jobmanager_params.cpu}"
        environment["FLINK_ENV_JAVA_OPTS"] = java_opts
    environment.update(config.extra_environment)

    common_task_spec: Dict[str, Any] = {
        **DEFAULT_TASK_SPEC,
        "file_paths": operation_params.file_paths,
        "environment": environment,
    }
    if application:
        # Gang tasks reject restart_completed_jobs; the JobManager script re-runs the driver instead.
        common_task_spec.pop("restart_completed_jobs")
    else:
        common_task_spec["restart_completed_jobs"] = config.restart_completed_jobs
    if operation_params.layer_paths:
        common_task_spec["layer_paths"] = operation_params.layer_paths

    if config.network_project:
        common_task_spec["network_project"] = config.network_project

    builder = VanillaSpecBuilder()
    if application:
        assert taskmanager_params is not None
        flink_args = _dynamic_property_args(
            flink_dynamic_properties(
                config, jobmanager_params, taskmanager_params, max_heap_size_str, off_heap_size_str
            )
        )
        _begin_task(
            builder,
            JOBMANAGER_TASK,
            command=_build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
                role=JOBMANAGER_TASK,
                flink_args=flink_args,
            ),
            job_count=1,
            cpu=jobmanager_params.cpu,
            memory=jobmanager_params.memory,
            # Any JobManager failure restarts the whole cluster with a new incarnation.
            task_spec={**common_task_spec, "gang_options": {}},
            layer_paths=operation_params.layer_paths,
        )
        _begin_task(
            builder,
            TASKMANAGER_TASK,
            command=_build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
                role=TASKMANAGER_TASK,
                flink_args=flink_args,
            ),
            job_count=taskmanager_params.count,
            cpu=taskmanager_params.cpu,
            memory=taskmanager_params.memory,
            # Not a gang task: a lost TaskManager is restarted alone and Flink restarts the job.
            task_spec=common_task_spec,
            layer_paths=operation_params.layer_paths,
        )
    else:
        _begin_task(
            builder,
            MINICLUSTER_TASK,
            command=_build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
            ),
            job_count=1,
            cpu=jobmanager_params.cpu,
            memory=jobmanager_params.memory,
            task_spec=common_task_spec,
            layer_paths=operation_params.layer_paths,
        )

    spec_dict = {}
    if title:
        spec_dict["title"] = title
    if operation_params.description:
        spec_dict["description"] = operation_params.description
    if operation_params.pool:
        builder.pool(operation_params.pool)
    if operation_params.max_failed_job_count is not None:
        builder.max_failed_job_count(operation_params.max_failed_job_count)
    if operation_params.acl is not None:
        builder.acl(operation_params.acl)

    if spec_dict:
        builder.spec(spec_dict)

    builder.secure_vault(secure_vault)

    return builder
