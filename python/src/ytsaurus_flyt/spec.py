"""Build YTsaurus Vanilla operation specs for PyFlink jobs."""

from __future__ import annotations

import logging
import shlex
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from yt.wrapper import YtClient
from yt.wrapper.spec_builders import VanillaSpecBuilder

from ytsaurus_flyt.application import application_cluster, minicluster_job
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    FLINK_REST_PORT,
    render_flink_config_yaml,
    user_flink_config,
)
from ytsaurus_flyt.job_proxy import job_yt_proxy
from ytsaurus_flyt.models import MIB, ClusterParams, OperationParams
from ytsaurus_flyt.yt_client import proxy_url_from_client

logger = logging.getLogger(__name__)

# Environment read by the application-mode run scripts and the in-job helper.
FLYT_YT_PROXY_ENV = "FLYT_YT_PROXY"

# Vanilla task names: one task per role in application mode, one for the MiniCluster job.
MINICLUSTER_TASK = "flink_minicluster"
JOBMANAGER_TASK = "jobmanager"
TASKMANAGER_TASK = "taskmanager"

_RUN_SCRIPTS_DIR = Path(__file__).parent / "run_scripts"
JOB_HELPER_FILENAME = "flyt_job_helper.py"
_JOB_HELPER_PATH = _RUN_SCRIPTS_DIR / "application" / JOB_HELPER_FILENAME
MINICLUSTER_RUNNER_FILENAME = "flyt_minicluster.py"
_MINICLUSTER_RUNNER_PATH = _RUN_SCRIPTS_DIR / "minicluster" / MINICLUSTER_RUNNER_FILENAME


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


def _script_fragments(
    *, use_squashfs_sandbox_unpack: bool, role: Optional[str], flink_memory: bool = False
) -> List[str]:
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
        "common/06_write_flink_conf.sh",
    ]
    if role is None:
        if flink_memory:
            ordered.append("minicluster/00_taskmanager_memory.sh")
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
    flink_config: Mapping[str, str],
    flink_memory: bool = False,
) -> str:
    result_script = ["#!/bin/bash", "set -e"]
    # Values are substituted into the templates, not re-formatted, so braces in them are safe.
    fmt: Dict[str, Any] = {
        "job_args": _job_args_for_shell(job_command),
        "service_name": service_name,
        "python_bin": config.python_bin,
        "rest_port": FLINK_REST_PORT,
        "flink_conf_yaml": render_flink_config_yaml(flink_config).rstrip("\n"),
    }
    if role is None:
        fmt.update(
            {
                "minicluster_runner": _MINICLUSTER_RUNNER_PATH.read_text(encoding="utf-8"),
                "minicluster_runner_filename": MINICLUSTER_RUNNER_FILENAME,
            }
        )
    else:
        fmt.update(
            {
                "job_helper": _JOB_HELPER_PATH.read_text(encoding="utf-8"),
                "job_helper_filename": JOB_HELPER_FILENAME,
                "sidecar_command": shlex.quote(config.sidecar_command) if config.sidecar_command else "''",
                "discovery_timeout": config.discovery_timeout,
            }
        )

    for name in _script_fragments(
        use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack, role=role, flink_memory=flink_memory
    ):
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
    preset_params: Optional[ClusterParams] = None,
    yt_client: Optional[YtClient] = None,
    use_squashfs_sandbox_unpack: bool = False,
    flink_options: Optional[Mapping[str, str]] = None,
) -> VanillaSpecBuilder:
    """Build a Vanilla operation spec to launch a Flink job (SquashFS runtime only).

    ``cluster_mode: minicluster`` yields a single task sized like one TaskManager; the script's MiniCluster
    gets the generated ``config.yaml`` as its context configuration.
    ``cluster_mode: application`` yields a gang ``jobmanager`` task and a ``taskmanager`` task; ``preset_params``
    (or the user's own sizing, :func:`ytsaurus_flyt.application.resolve_taskmanager_size`) sizes every TaskManager. ``yt_client`` is then required:
    its proxy (or ``config.job_yt_proxy``) is what the TaskManagers query to discover the JobManager.
    ``flink_options``, the user's Flink options, go over flyt's defaults in both modes; by default they are read from ``config``
    (:func:`ytsaurus_flyt.flink_conf.user_flink_config`, a Cypress file through ``yt_client``).
    """
    service_name = config.service_name or _extract_service_name(job_command)
    is_application = config.is_application_cluster
    if flink_options is None:
        flink_options = user_flink_config(config, yt_client)

    environment: Dict[str, str] = {
        **DEFAULT_ENVIRONMENT,
        "JAVA_HOME": config.java_home,
    }
    if is_application:
        if yt_client is None:
            raise ValueError("yt_client is required for cluster_mode: application")
        proxy_url = proxy_url_from_client(yt_client)
        if proxy_url is None:
            raise ValueError(
                "Cannot determine the YT proxy URL from yt_client; application mode needs it for discovery"
            )
        yt_proxy = job_yt_proxy(yt_client, proxy_url, config.job_yt_proxy)
        if yt_proxy != proxy_url:
            logger.info("Jobs will reach YT through %s", yt_proxy)
        cluster = application_cluster(config, preset_params, flink_options)
        logger.info("Application cluster: %s", cluster.resources.summary())
        environment[FLYT_YT_PROXY_ENV] = yt_proxy
        flink_config = cluster.flink_config
    else:
        # JVM flags go to config.yaml (env.java.*): FLINK_ENV_JAVA_OPTS would make the gateway ignore them.
        minicluster = minicluster_job(config, preset_params, flink_options)
        logger.info("MiniCluster job: %d cpu, %dm", minicluster.cpu, minicluster.memory // MIB)
        flink_config = minicluster.flink_config
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
        jobmanager_params = cluster.resources.jobmanager_params()
        taskmanager_params = cluster.resources.taskmanager_params()
        _begin_task(
            builder,
            JOBMANAGER_TASK,
            command=_build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
                role=JOBMANAGER_TASK,
                flink_config=flink_config,
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
                flink_config=flink_config,
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
                flink_config=flink_config,
                flink_memory=minicluster.flink_memory,
            ),
            job_count=1,
            cpu=minicluster.cpu,
            memory=minicluster.memory,
            task_spec=common_task_spec,
            layer_paths=operation_params.layer_paths,
        )

    spec_dict: Dict[str, Any] = {}
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
