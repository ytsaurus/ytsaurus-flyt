"""Pre-flight validation for flyt run / install (no job submission)."""

from __future__ import annotations

import shutil
from typing import Dict, List, Optional, Tuple

from yt.wrapper import YtClient

from ytsaurus_flyt.application import application_cluster, minicluster_job
from ytsaurus_flyt.config import (
    SQUASHFS_VALIDATE_RUNTIME_PACKAGES_MSG,
    SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG,
    FlytConfig,
)
from ytsaurus_flyt.container_runtime import get_container_runtime_command
from ytsaurus_flyt.flink_conf import flink_config_warnings, user_flink_config
from ytsaurus_flyt.models import MIB, ClusterParams, ClusterPreset


def _check_mksquashfs() -> Tuple[bool, str]:
    if shutil.which("mksquashfs"):
        return True, "mksquashfs found"
    return False, "mksquashfs not found (install squashfs-tools)"


def _check_unzip() -> Tuple[bool, str]:
    if shutil.which("unzip"):
        return True, "unzip found"
    return False, "unzip not found (needed to unpack service wheel in sandbox)"


def _check_container_runtime() -> Tuple[bool, str]:
    try:
        get_container_runtime_command()
        return True, "docker or podman found"
    except RuntimeError as e:
        return False, str(e)


# ok None: a warning, which does not fail validation.
_Row = Tuple[str, Optional[bool], str]


def _flink_config_rows(
    config: FlytConfig, yt_client: Optional[YtClient]
) -> Tuple[List[_Row], Optional[Dict[str, str]]]:
    """The user's Flink options as ``flyt run`` reads them, with their warnings (both modes)."""
    try:
        user = user_flink_config(config, yt_client)
    except (OSError, ValueError) as e:
        return [("flink config.yaml", False, f"{config.base_flink_conf_path}: {e}")], None
    source = config.base_flink_conf_path or "no file"
    rows: List[_Row] = [("flink config.yaml", True, f"{source}, {len(user)} user option(s)")]
    rows.extend(("flink config.yaml", None, warning) for warning in flink_config_warnings(config, user))
    return rows, user


def _application_rows(config: FlytConfig, preset: ClusterParams, user: Dict[str, str]) -> List[_Row]:
    """The application cluster exactly as ``flyt run`` resolves it."""
    try:
        resources = application_cluster(config, preset, user).resources
    except ValueError as e:
        return [("cluster_mode", False, f"application: {e}")]
    return [
        ("cluster_mode", True, f"application: {resources.summary()}"),
        ("discovery_timeout", True, f"{config.discovery_timeout}s for TaskManagers to find the JobManager"),
    ]


def _minicluster_row(config: FlytConfig, preset: ClusterParams, user: Dict[str, str]) -> _Row:
    """The MiniCluster job exactly as ``flyt run`` sizes it."""
    try:
        job = minicluster_job(config, preset, user)
    except ValueError as e:
        return ("cluster_mode", False, f"minicluster: {e}")
    if not job.flink_memory:
        return ("cluster_mode", True, "minicluster (single job, in-JVM MiniCluster)")
    return ("cluster_mode", True, f"minicluster: one TaskManager ({job.cpu} cpu, {job.memory // MIB}m)")


def validate_flyt_config(
    config: FlytConfig,
    *,
    proxy: Optional[str] = None,
    yt_client: Optional[YtClient] = None,
    preset: Optional[ClusterParams] = None,
) -> List[_Row]:
    """Return list of (name, ok, message); ok None is a warning. ``preset`` sizes each TaskManager in
    application mode (default micro). ``yt_client`` reads a Cypress ``base_flink_conf_path``."""
    rows: List[_Row] = []

    rows.append(("Runtime mode", True, "SquashFS"))
    flink_rows, user = _flink_config_rows(config, yt_client)
    if config.is_application_cluster:
        if user is not None:
            rows.extend(_application_rows(config, preset or ClusterPreset.MICRO.params, user))
    elif user is not None:
        rows.append(_minicluster_row(config, preset or ClusterPreset.MICRO.params, user))
    rows.extend(flink_rows)

    ok_pkgs = bool(config.runtime_python_packages)
    rows.append(
        (
            "runtime_python_packages",
            ok_pkgs,
            "set" if ok_pkgs else SQUASHFS_VALIDATE_RUNTIME_PACKAGES_MSG,
        )
    )
    rv = (config.runtime_python_version or "").strip()
    if ok_pkgs:
        rows.append(
            (
                "runtime_python_version",
                bool(rv),
                "set" if rv else SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG,
            )
        )
        if rv:
            ok, msg = _check_container_runtime()
            rows.append((f"Container runtime (Python {rv})", ok, msg))
    ok_sq, msg_sq = _check_mksquashfs()
    rows.append(("mksquashfs", ok_sq, msg_sq))
    ok_uz, msg_uz = _check_unzip()
    rows.append(("unzip", ok_uz, msg_uz))

    py = (config.runtime_python_version or "").strip()
    pbin = (config.python_bin or "").strip()
    if py and pbin:
        rows.append(
            (
                "python ABI hint",
                True,
                f"layer targets Python {py}; python_bin must match on exec nodes ({pbin})",
            )
        )

    if proxy and yt_client is not None:
        try:
            _ = yt_client.exists("//home")
            rows.append(("YT connectivity", True, f"proxy {proxy} reachable"))
        except Exception as e:
            rows.append(("YT connectivity", False, str(e)))

    return rows
