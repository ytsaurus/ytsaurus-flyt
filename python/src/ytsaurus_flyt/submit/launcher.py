"""Main launcher for PyFlink jobs on YTsaurus Vanilla operations."""

import logging
import os
import time
from contextlib import ExitStack
from typing import Any, Dict, List, Optional, Union

from yt.wrapper import YtClient
from yt.wrapper.run_operation_commands import run_operation

from ytsaurus_flyt.config.config import FlytConfig, require_squashfs_runtime_config
from ytsaurus_flyt.config.models import (
    ClusterParams,
    ClusterPreset,
    JobmanagerParams,
    OperationParams,
)
from ytsaurus_flyt.progress import LoggingReporter, Reporter
from ytsaurus_flyt.runtime.flink_lib_jars import resolve_flink_lib_jars
from ytsaurus_flyt.runtime.wheel_utils import dedupe_file_paths_by_basename, upload_service_wheel
from ytsaurus_flyt.submit.credentials import get_secure_credentials
from ytsaurus_flyt.submit.spec import build_vanilla_operation_spec
from ytsaurus_flyt.submit.yt_client import proxy_url_from_client
from ytsaurus_flyt.tracking.jobshell_resolve import operation_title_for_profile

logger = logging.getLogger(__name__)

_MATERIALIZE_POLL_S = 2.0
_MATERIALIZE_TIMEOUT_S = 300.0


def _wait_operation_materialized(op: Any, timeout: float = _MATERIALIZE_TIMEOUT_S) -> str:
    """Poll until the operation is running (materialized) or finished; return the last state."""
    deadline = time.monotonic() + timeout
    state = op.get_state()
    while not (state.is_running() or state.is_finished()) and time.monotonic() < deadline:
        time.sleep(_MATERIALIZE_POLL_S)
        state = op.get_state()
    return str(state)


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
    profile_name: Optional[str] = None,
    reporter: Optional[Reporter] = None,
) -> Any:
    """Submit a PyFlink job as a Vanilla operation (application mode, ``execute.wait()``).

    ``reporter`` receives phase progress; defaults to logging so callers keep old output.
    The runtime layer must already exist on Cypress (built with ``flyt build layer``) and be
    referenced via ``squashfs_layer_paths`` (or ``pre_built_layer_paths``); flyt never builds it here.
    """
    rep: Reporter = reporter or LoggingReporter(logger)
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

    with rep.step("Fetching credentials"):
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
        with rep.step("Uploading service wheel"):
            wheel_remote_path = stack.enter_context(
                upload_service_wheel(
                    yt_client=yt_client,
                    wheel_path=wheel_path,
                    source_dir=source_dir,
                    cache_prefix=cache_prefix,
                )
            )
        operation_params = _make_operation_params(config, preset_params, pool)

        with rep.step("Resolving Flink lib JARs"):
            flink_jars = resolve_flink_lib_jars(yt_client, config)
        # JARs always ship as file_paths (staged into flink/lib at runtime), never inside the layer.
        operation_params.file_paths.extend(flink_jars.yt_paths)
        if flink_jars.yt_paths:
            logger.info(
                "Flink lib JARs attached as file_paths: %s",
                sorted(os.path.basename(p.rstrip("/")) for p in flink_jars.yt_paths),
            )

        sandbox_unpack_layers: List[str] = []  # ordered layer basenames to unpack (sandbox_unpack only)
        unsquashfs_basename = ""
        if config.pre_built_layer_paths:
            logger.info("Using pre-built layer paths: %s", config.pre_built_layer_paths)
            operation_params.layer_paths = list(config.pre_built_layer_paths)
        else:
            layers = [p.strip() for p in config.squashfs_layer_paths if p and p.strip()]
            if not layers:
                raise RuntimeError(
                    "No runtime layer configured. Build one with `flyt build layer --upload <//cypress/path>` "
                    "and set squashfs_layer_paths in the profile."
                )
            with rep.step("Resolving runtime layers"):
                for p in layers:
                    if not yt_client.exists(p):
                        raise RuntimeError(f"squashfs_layer_paths entry {p} does not exist on the cluster.")

            if config.squashfs_layer_delivery == "sandbox_unpack":
                operation_params.file_paths.extend(layers)
                sandbox_unpack_layers = [os.path.basename(p.rstrip("/")) for p in layers]
                unsquashfs = (config.unsquashfs_path or "").strip()
                if unsquashfs:
                    if not yt_client.exists(unsquashfs):
                        raise RuntimeError(f"unsquashfs_path {unsquashfs} does not exist on the cluster.")
                    operation_params.file_paths.append(unsquashfs)
                    unsquashfs_basename = os.path.basename(unsquashfs.rstrip("/"))
                # else: the exec image must provide `unsquashfs` on PATH.
            else:
                operation_params.layer_paths = list(layers)

        operation_params.file_paths.append(wheel_remote_path)
        operation_params.file_paths = dedupe_file_paths_by_basename(operation_params.file_paths)

        jobmanager_params = _make_jobmanager_params(preset_params)

        with rep.step("Building operation spec"):
            spec = build_vanilla_operation_spec(
                title=operation_title_for_profile(job_command, profile_name),
                job_command=job_command,
                config=config,
                operation_params=operation_params,
                jobmanager_params=jobmanager_params,
                secure_vault=secure_vault,
                max_heap_size_str=preset_params.max_heap_size,
                off_heap_size_str=preset_params.off_heap_size,
                use_squashfs_sandbox_unpack=config.squashfs_layer_delivery == "sandbox_unpack",
                sandbox_unpack_layers=sandbox_unpack_layers,
                unsquashfs_basename=unsquashfs_basename,
            )

        with rep.step("Submitting operation"):
            op = run_operation(spec, sync=False, client=yt_client)  # type: ignore[union-attr]

        op_id = getattr(op, "id", "unknown")
        ui_base = (config.yt_ui_base_url or "").strip().rstrip("/")
        base = ui_base or (str(proxy_url).rstrip("/") if proxy_url != "unknown" else "")
        tracking_url = f"{base}/operations/{op_id}/details" if base else ""
        rows = [("operation", str(op_id))]
        if tracking_url:
            rows.append(("track", tracking_url))
        if ui_base:
            rows.append(("web ui", ui_base))
        rep.result(rows)
        # Flink's job UI (:27050) is in-cluster; the watcher surfaces it once the job runs.

        if sync and op is not None:
            rep.line("Running job (Ctrl-C to stop)...")
            op.wait()
            rep.line(f"Operation {op_id} finished successfully.")
        elif op is not None:
            # Must stay inside the ExitStack: holds the temp wheel until YT
            # snapshot-locks file_paths, so detached jobs survive its cleanup.
            rep.line("Waiting for the operation to materialize (detach mode)...")
            state = _wait_operation_materialized(op)
            rep.line(f"Operation {op_id} is {state}. Detaching.")

        return op
