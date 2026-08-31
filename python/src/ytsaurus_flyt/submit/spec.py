"""Build YTsaurus Vanilla operation specs for PyFlink jobs."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import List, Optional

from yt.wrapper.spec_builders import VanillaSpecBuilder

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.models import JobmanagerParams, OperationParams

FLINK_STANDALONE_FLAG = "FLINK_STANDALONE"


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


def _build_run_script(
    job_command: str,
    config: FlytConfig,
    service_name: str,
    *,
    use_squashfs_sandbox_unpack: bool = False,
    sandbox_unpack_layers: Optional[List[str]] = None,
    unsquashfs_basename: str = "",
) -> str:
    # run_scripts/ ships at the package root (ytsaurus_flyt/), one level up from submit/.
    scripts_dir = Path(__file__).parent.parent / "run_scripts"
    result_script = ["#!/bin/bash", "set -e"]

    if use_squashfs_sandbox_unpack:
        ordered = [
            "00_set_essentials.sh",
            "00_unpack_runtime_squashfs.sh",
            "01_prepare_squashfs.sh",
            "21_prepare_libs.sh",
            "30_prepare_service.sh",
            "40_run_job.sh",
        ]
    else:
        ordered = [
            "00_set_essentials.sh",
            "01_prepare_squashfs.sh",
            "21_prepare_libs.sh",
            "30_prepare_service.sh",
            "40_run_job.sh",
        ]
    script_paths = [scripts_dir / name for name in ordered]
    # Ordered, space-separated layer basenames for sandbox_unpack; empty falls back to the glob.
    unpack_basenames = " ".join(sandbox_unpack_layers or [])

    for script_path in script_paths:
        result_script.append(f"echo 'Running {script_path.name}...' 1>&2")
        with open(script_path, encoding="utf-8") as script_file:
            script_text = script_file.read()
            result_script.append(
                script_text.format(
                    job_args=_job_args_for_shell(job_command),
                    service_name=service_name,
                    squashfs_unpack_basenames=unpack_basenames,
                    unsquashfs_basename=unsquashfs_basename,
                )
            )

    return "\n".join(result_script)


DEFAULT_TASK_SPEC = {
    "restart_completed_jobs": True,
    "memory_reserve_factor": 1.0,
    "max_speculative_job_count_per_task": 0,
}

DEFAULT_ENVIRONMENT = {
    "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
}


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
    sandbox_unpack_layers: Optional[List[str]] = None,
    unsquashfs_basename: str = "",
) -> VanillaSpecBuilder:
    """Build a Vanilla operation spec to launch a Flink job (SquashFS runtime only)."""
    service_name = config.service_name or _extract_service_name(job_command)

    java_opts = f"-Xmx{max_heap_size_str}"
    if off_heap_size_str:
        # Bounds NIO/netty direct buffers (gRPC connectors); otherwise the JVM
        # defaults MaxDirectMemorySize to ~the max heap size.
        java_opts += f" -XX:MaxDirectMemorySize={off_heap_size_str}"
    if jobmanager_params.cpu:
        # Pin the JVM's processor count to the container CPU limit.
        java_opts += f" -XX:ActiveProcessorCount={jobmanager_params.cpu}"

    # JAVA_HOME / PYTHON_BIN are set in the run script (layer-relative to the bundled JRE/CPython),
    # not here, since the layer root isn't known until runtime.
    environment = {
        **DEFAULT_ENVIRONMENT,
        "FLINK_ENV_JAVA_OPTS": java_opts,
        FLINK_STANDALONE_FLAG: "True",
        **config.extra_environment,
    }

    common_task_spec = {
        **DEFAULT_TASK_SPEC,
        "file_paths": operation_params.file_paths,
        "environment": environment,
    }
    if operation_params.layer_paths:
        common_task_spec["layer_paths"] = operation_params.layer_paths

    if config.network_project:
        common_task_spec["network_project"] = config.network_project

    builder = VanillaSpecBuilder()
    task = (
        builder.begin_task("flink")
        .command(
            _build_run_script(
                job_command,
                config,
                service_name,
                use_squashfs_sandbox_unpack=use_squashfs_sandbox_unpack,
                sandbox_unpack_layers=sandbox_unpack_layers,
                unsquashfs_basename=unsquashfs_basename,
            )
        )
        .job_count(1)
        .cpu_limit(jobmanager_params.cpu)
        .memory_limit(jobmanager_params.memory)
    )
    if operation_params.layer_paths:
        task = task.tmpfs_path(".")
    task.copy_files(True).spec(common_task_spec).end_task()

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
