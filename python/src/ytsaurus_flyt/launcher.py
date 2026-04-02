"""Main launcher for PyFlink jobs on YTsaurus Vanilla operations."""

import logging
import os
from contextlib import ExitStack
from typing import Any, Dict, Optional, Union

from yt.wrapper import YtClient
from yt.wrapper.run_operation_commands import run_operation

from ytsaurus_flyt.config import FlytConfig, require_squashfs_runtime_config
from ytsaurus_flyt.credentials import get_secure_credentials
from ytsaurus_flyt.flink_lib_jars import (
    partition_flink_lib_jars_for_delivery,
    resolve_flink_lib_jars,
)
from ytsaurus_flyt.jobshell_resolve import operation_title_for_profile
from ytsaurus_flyt.layer_builder import ensure_runtime_layer, ensure_unsquashfs_tool_remote
from ytsaurus_flyt.models import (
    ClusterParams,
    ClusterPreset,
    JobmanagerParams,
    OperationParams,
)
from ytsaurus_flyt.spec import build_vanilla_operation_spec
from ytsaurus_flyt.wheel_utils import dedupe_file_paths_by_basename, upload_service_wheel
from ytsaurus_flyt.yt_client import proxy_url_from_client

logger = logging.getLogger(__name__)


def _make_jobmanager_params(preset_params: ClusterParams) -> JobmanagerParams:
    return JobmanagerParams(
        cpu=preset_params.cpu,
        memory=preset_params.memory_bytes(),
    )


def _make_operation_params(
    config: FlytConfig,
    preset_params: ClusterParams,
    pool: str,
) -> OperationParams:
    return OperationParams(
        max_failed_job_count=config.max_failed_job_count,
        file_paths=list(config.extra_file_paths),
        layer_paths=[],
        pool=pool,
    )


def launch_vanilla_job(
    config: FlytConfig,
    yt_client: YtClient,
    job_command: str,
    pool: str,
    preset: Union[ClusterPreset, ClusterParams] = ClusterPreset.MICRO,
    extra_secrets: Optional[Dict[str, str]] = None,
    wheel_path: Optional[str] = None,
    source_dir: Optional[str] = None,
    cache_wheel: bool = False,
    sync: bool = True,
    force_rebuild_layer: bool = False,
    profile_name: Optional[str] = None,
) -> Any:
    """Submit a PyFlink job as a Vanilla operation (application mode, ``execute.wait()``)."""
    if isinstance(preset, ClusterPreset):
        preset_params = preset.params
        preset_name = preset.name
    else:
        preset_params = preset
        preset_name = "custom"
    proxy_url = proxy_url_from_client(yt_client)
    logger.info(
        "Launching Vanilla Flink job on %s in pool %s (preset=%s)",
        proxy_url,
        pool,
        preset_name,
    )

    require_squashfs_runtime_config(config)
    rv = (config.runtime_python_version or "").strip()
    logger.info(
        "SquashFS layer targets Python %s; exec node python_bin must be that "
        "interpreter (same ABI). If python_bin is an older 3.x, expect grpc/pyflink import errors (e.g. cygrpc).",
        rv,
    )

    logger.info("Fetching credentials...")
    secure_vault = get_secure_credentials(yt_client, extra_secrets=extra_secrets)

    cache_prefix = None
    if cache_wheel and config.wheel_cache_prefix:
        svc = config.service_name or "default"
        cache_prefix = f"{config.wheel_cache_prefix}/{svc}"

    if wheel_path:
        logger.info("Using pre-built wheel: %s", wheel_path)
    elif source_dir:
        if cache_wheel:
            logger.info("Resolving service wheel (cache enabled)...")
        else:
            logger.info("Building and uploading service wheel (may take 1-2 min)...")
    else:
        raise ValueError("Either wheel_path or source_dir must be provided to launch_vanilla_job")

    with ExitStack() as stack:
        wheel_remote_path = stack.enter_context(
            upload_service_wheel(
                yt_client=yt_client,
                wheel_path=wheel_path,
                source_dir=source_dir,
                cache_prefix=cache_prefix,
            )
        )
        operation_params = _make_operation_params(config, preset_params, pool)

        logger.info("Resolving Flink lib JARs...")
        flink_jars = resolve_flink_lib_jars(yt_client, config)
        all_flink_lib_jar_yt_paths = flink_jars.yt_paths
        jar_yt_for_squashfs, jar_yt_for_files = partition_flink_lib_jars_for_delivery(
            config,
            all_flink_lib_jar_yt_paths,
            extra_runtime_basenames=flink_jars.extra_runtime_basenames,
        )
        if jar_yt_for_files:
            logger.info(
                "Flink lib JARs attached as file_paths (not in SquashFS): %s",
                sorted(os.path.basename(p.rstrip("/")) for p in jar_yt_for_files),
            )
        squashfs_remote = ensure_runtime_layer(
            yt_client,
            config,
            jar_yt_for_squashfs,
            force_rebuild=force_rebuild_layer,
        )
        if config.squashfs_layer_delivery == "sandbox_unpack":
            operation_params.file_paths.append(squashfs_remote)
            operation_params.file_paths.append(ensure_unsquashfs_tool_remote(yt_client, config))
        else:
            operation_params.layer_paths = [squashfs_remote]
        operation_params.file_paths.extend(jar_yt_for_files)

        operation_params.file_paths.append(wheel_remote_path)
        operation_params.file_paths = dedupe_file_paths_by_basename(operation_params.file_paths)

        jobmanager_params = _make_jobmanager_params(preset_params)

        spec = build_vanilla_operation_spec(
            title=operation_title_for_profile(job_command, profile_name),
            job_command=job_command,
            config=config,
            operation_params=operation_params,
            jobmanager_params=jobmanager_params,
            secure_vault=secure_vault,
            max_heap_size_str=preset_params.max_heap_size,
            use_squashfs_sandbox_unpack=config.squashfs_layer_delivery == "sandbox_unpack",
        )

        logger.info("Starting YT Vanilla operation...")
        op = run_operation(spec, sync=False, client=yt_client)  # type: ignore[union-attr]

        op_id = getattr(op, "id", "unknown")
        base = str(proxy_url).rstrip("/")
        tracking_url = f"{base}/?page=operation&mode=detail&id={op_id}&tab=details"
        logger.info(
            "Operation %s started. Track: %s (Flink :27050 is in-cluster; see stderr for port-forward).",
            op_id,
            tracking_url,
        )
        ui_base = (config.yt_ui_base_url or "").strip()
        if ui_base:
            logger.info("Web UI base: %s", ui_base.rstrip("/"))

        if sync and op is not None:
            logger.info("Waiting for completion...")
            op.wait()
            logger.info("Operation %s finished successfully.", op_id)

        return op
