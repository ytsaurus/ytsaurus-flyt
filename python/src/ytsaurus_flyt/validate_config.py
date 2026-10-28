"""Pre-flight validation for flyt run / install (no job submission)."""

from __future__ import annotations

import shutil
from typing import List, Optional, Tuple

from yt.wrapper import YtClient

from ytsaurus_flyt.application import application_cluster
from ytsaurus_flyt.config import (
    SQUASHFS_VALIDATE_RUNTIME_PACKAGES_MSG,
    SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG,
    FlytConfig,
)
from ytsaurus_flyt.container_runtime import get_container_runtime_command
from ytsaurus_flyt.flink_conf import load_flink_conf_file, minicluster_ignored_options_warning


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


_Row = Tuple[str, Optional[bool], str]


def _flink_conf_file(config: FlytConfig, yt_client: Optional[YtClient], rows: List[_Row]):
    """``(conf_file, layers description, ok)`` for ``base_flink_conf_path``, read the way ``flyt run`` reads it."""
    conf_file = None
    desc = "flyt defaults"
    path = config.base_flink_conf_path
    if path:
        if path.startswith("//") and yt_client is None:
            rows.append(("base_flink_conf_path", None, f"{path} not read: no YT connection, results below ignore it"))
        else:
            try:
                conf_file = load_flink_conf_file(yt_client, path)  # type: ignore[arg-type]
            except Exception as e:  # noqa: BLE001 - reported as a failed row
                rows.append(("base_flink_conf_path", False, f"{path}: {e}"))
                return None, desc, False
            desc += f" + {path} ({len(conf_file)} key(s))"
    if config.flink_config_override:
        desc += f" + {len(config.flink_config_override)} flink_config_override key(s)"
    return conf_file, desc, True


def _application_rows(config: FlytConfig, yt_client: Optional[YtClient]) -> List[_Row]:
    """The application cluster exactly as ``flyt run`` resolves it, including ``base_flink_conf_path``."""
    rows: List[_Row] = []
    conf_file, conf_desc, ok = _flink_conf_file(config, yt_client, rows)
    if not ok:
        return rows
    try:
        cluster = application_cluster(config, conf_file)
    except ValueError as e:
        rows.append(("cluster_mode", False, f"application: {e}"))
        return rows
    res = cluster.resources
    rows.append(("cluster_mode", True, f"application: {res.summary()}"))
    try:
        res.check_parallelism_fits()
    except ValueError as e:
        rows.append(("parallelism.default", False, str(e)))
    rows.append(("discovery_timeout", True, f"{config.discovery_timeout}s for TaskManagers to find the JobManager"))
    rows.append(("flink config.yaml", True, conf_desc))
    rows.extend(("flink config.yaml", None, warning) for warning in cluster.warnings)
    return rows


def _minicluster_rows(config: FlytConfig) -> List[_Row]:
    rows: List[_Row] = [("cluster_mode", True, "minicluster (single job, in-JVM MiniCluster)")]
    ignored = minicluster_ignored_options_warning(config)
    if ignored:
        rows.append(("flink config.yaml", None, ignored))
    return rows


def validate_flyt_config(
    config: FlytConfig,
    *,
    proxy: Optional[str] = None,
    yt_client: Optional[YtClient] = None,
) -> List[Tuple[str, bool, str]]:
    """Return list of (name, ok, message); ``ok`` is None for a warning that does not fail validation."""
    rows: List[_Row] = []

    rows.append(("Runtime mode", True, "SquashFS"))
    if config.is_application_cluster:
        rows.extend(_application_rows(config, yt_client))
    else:
        rows.extend(_minicluster_rows(config))

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
