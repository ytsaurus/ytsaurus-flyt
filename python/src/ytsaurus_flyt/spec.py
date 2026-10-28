"""Build YTsaurus Vanilla operation specs for PyFlink jobs."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from yt.wrapper.spec_builders import VanillaSpecBuilder

from ytsaurus_flyt.application import ApplicationCluster
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import render_flink_config_yaml, rest_port
from ytsaurus_flyt.models import MiniClusterParams, OperationParams

# Environment read by the application-mode run scripts and the in-job helper.
FLYT_YT_PROXY_ENV = "FLYT_YT_PROXY"

# Operation annotation with the JobManager REST port (the Web UI).
FLINK_REST_PORT_ANNOTATION = "flyt_flink_rest_port"

# Vanilla task names: one task per role in application mode, one for the MiniCluster job.
MINICLUSTER_TASK = "flink_minicluster"
JOBMANAGER_TASK = "jobmanager"
TASKMANAGER_TASK = "taskmanager"

_RUN_SCRIPTS_DIR = Path(__file__).parent / "run_scripts"
JOB_HELPER_FILENAME = "flyt_job_helper.py"
_JOB_HELPER_PATH = _RUN_SCRIPTS_DIR / "application" / JOB_HELPER_FILENAME


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
    ordered = ["common/00_set_essentials.sh"]
    if use_squashfs_sandbox_unpack:
        ordered.append("common/01_unpack_runtime_squashfs.sh")
    ordered += ["common/02_prepare_squashfs.sh"]
    if role is not None:
        ordered.append("application/00_prepare_cluster_bins.sh")
    ordered += [
        "common/03_prepare_libs.sh",
        "common/04_prepare_service.sh",
        "common/05_prepare_job_env.sh",
    ]
    if role is None:
        ordered.append("minicluster/run.sh")
    elif role == JOBMANAGER_TASK:
        ordered += ["application/01_cluster_common.sh", "application/02_start_sidecar.sh", "application/jobmanager.sh"]
    elif role == TASKMANAGER_TASK:
        ordered += ["application/01_cluster_common.sh", "application/02_start_sidecar.sh", "application/taskmanager.sh"]
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
    flink_config: Optional[Mapping[str, str]] = None,
) -> str:
    result_script = ["#!/bin/bash", "set -e"]
    # Values are substituted into the templates, not re-formatted, so braces in them are safe.
    fmt: Dict[str, Any] = {
        "job_args": _job_args_for_shell(job_command),
        "service_name": service_name,
        "python_bin": config.python_bin,
        # A MiniCluster pipeline sets the Web UI port in code; flyt ui looks for it on flyt's default.
        "rest_port": rest_port(flink_config or {}),
    }
    if role is not None:
        assert flink_config is not None
        fmt.update(
            {
                "flink_conf_yaml": render_flink_config_yaml(flink_config).rstrip("\n"),
                "job_helper": _JOB_HELPER_PATH.read_text(encoding="utf-8"),
                "job_helper_filename": JOB_HELPER_FILENAME,
                "sidecar_command": shlex.quote(config.sidecar_command) if config.sidecar_command else "''",
                "discovery_timeout": config.discovery_timeout,
            }
        )

    for name in _script_fragments(use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack, role=role):
        script_path = _RUN_SCRIPTS_DIR / name
        result_script.append(f"echo 'Running {name}...' 1>&2")
        with open(script_path, encoding="utf-8") as script_file:
            result_script.append(script_file.read().format(**fmt))

    return "\n".join(result_script)


DEFAULT_TASK_SPEC = {
    "memory_reserve_factor": 1.0,
    "max_speculative_job_count_per_task": 0,
}

DEFAULT_ENVIRONMENT = {
    "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
}


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
    secure_vault: dict[str, str],
    *,
    minicluster_params: Optional[MiniClusterParams] = None,
    application_cluster: Optional[ApplicationCluster] = None,
    yt_proxy: Optional[str] = None,
    use_squashfs_sandbox_unpack: bool = False,
) -> VanillaSpecBuilder:
    """Build a Vanilla operation spec to launch a Flink job (SquashFS runtime only).

    ``cluster_mode: minicluster`` yields a single task sized by ``minicluster_params``; the script runs as
    ``python <script>`` and configures its MiniCluster in code.
    ``cluster_mode: application`` yields a gang ``jobmanager`` task and a ``taskmanager`` task sized and
    configured by ``application_cluster`` (:func:`ytsaurus_flyt.application.application_cluster`); ``yt_proxy`` is
    the HTTP proxy the TaskManagers query to discover the JobManager.
    """
    service_name = config.service_name or _extract_service_name(job_command)
    is_application = config.is_application_cluster
    if is_application:
        if application_cluster is None:
            raise ValueError("application_cluster is required for cluster_mode: application")
        if not (yt_proxy or "").strip():
            raise ValueError("yt_proxy is required for cluster_mode: application")
    elif minicluster_params is None:
        raise ValueError("minicluster_params is required for cluster_mode: minicluster")

    environment: Dict[str, str] = {
        **DEFAULT_ENVIRONMENT,
        "JAVA_HOME": config.java_home,
    }
    if is_application:
        # Memory and JVM flags live in the cluster config.yaml (Flink's memory model), never in -Xmx.
        environment[FLYT_YT_PROXY_ENV] = str(yt_proxy).strip()
    else:
        assert minicluster_params is not None
        java_opts = f"-Xmx{minicluster_params.max_heap_size}"
        if minicluster_params.off_heap_size:
            # Bounds NIO/netty direct buffers (gRPC connectors); otherwise the JVM
            # defaults MaxDirectMemorySize to ~the max heap size.
            java_opts += f" -XX:MaxDirectMemorySize={minicluster_params.off_heap_size}"
        # Pin the JVM's processor count to the container CPU limit.
        java_opts += f" -XX:ActiveProcessorCount={minicluster_params.cpu}"
        environment["FLINK_ENV_JAVA_OPTS"] = java_opts
    environment.update(config.extra_environment)

    common_task_spec: Dict[str, Any] = {
        **DEFAULT_TASK_SPEC,
        "file_paths": operation_params.file_paths,
        "environment": environment,
    }
    if operation_params.layer_paths:
        common_task_spec["layer_paths"] = operation_params.layer_paths

    if config.network_project:
        common_task_spec["network_project"] = config.network_project

    builder = VanillaSpecBuilder()
    if is_application:
        assert application_cluster is not None
        jobmanager_params = application_cluster.resources.jobmanager_params()
        taskmanager_params = application_cluster.resources.taskmanager_params()
        _begin_task(
            builder,
            JOBMANAGER_TASK,
            command=_build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
                role=JOBMANAGER_TASK,
                flink_config=application_cluster.flink_config,
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
                flink_config=application_cluster.flink_config,
            ),
            job_count=taskmanager_params.count,
            cpu=taskmanager_params.cpu,
            memory=taskmanager_params.memory,
            # Not a gang task: a lost TaskManager is restarted alone and Flink restarts the job.
            task_spec=common_task_spec,
            layer_paths=operation_params.layer_paths,
        )
    else:
        assert minicluster_params is not None
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
            cpu=minicluster_params.cpu,
            memory=minicluster_params.memory_bytes(),
            task_spec=common_task_spec,
            layer_paths=operation_params.layer_paths,
        )

    spec_dict: Dict[str, Any] = {}
    if title:
        spec_dict["title"] = title
    if application_cluster is not None:
        # Read back by `flyt ui`, which cannot know the Flink configuration of this run otherwise.
        spec_dict["annotations"] = {FLINK_REST_PORT_ANNOTATION: application_cluster.rest_port}
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
