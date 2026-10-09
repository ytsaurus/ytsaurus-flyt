"""Pre-flight validation for flyt run / install (no job submission)."""

from __future__ import annotations

import shutil
from typing import List, Optional, Tuple

from yt.wrapper import YtClient

from ytsaurus_flyt.config.config import (
    SQUASHFS_VALIDATE_FLINK_VERSION_MSG,
    SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG,
    FlytConfig,
)
from ytsaurus_flyt.runtime.container_runtime import get_container_runtime_command


def _check_mksquashfs() -> Tuple[bool, str]:
    if shutil.which("mksquashfs"):
        return True, "mksquashfs found"
    if _has_container_runtime():
        return True, "mksquashfs not on host; will build SquashFS in a container"
    return False, "mksquashfs not found (install squashfs-tools, or docker/podman to build in a container)"


def _has_container_runtime() -> bool:
    try:
        get_container_runtime_command()
        return True
    except RuntimeError:
        return False


def _check_container_runtime() -> Tuple[bool, str]:
    try:
        get_container_runtime_command()
        return True, "docker or podman found"
    except RuntimeError as e:
        return False, str(e)


def validate_flyt_config(
    config: FlytConfig,
    *,
    proxy: Optional[str] = None,
    yt_client: Optional[YtClient] = None,
) -> List[Tuple[str, bool, str]]:
    """Return list of (name, ok, message)."""
    rows: List[Tuple[str, bool, str]] = []

    rows.append(("Runtime mode", True, "SquashFS"))

    ok_pkgs = bool((config.flink_version or "").strip())
    rows.append(
        (
            "flink_version",
            ok_pkgs,
            config.flink_version if ok_pkgs else SQUASHFS_VALIDATE_FLINK_VERSION_MSG,
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

    py = (config.runtime_python_version or "").strip()
    if py:
        rows.append(
            (
                "bundled runtime",
                True,
                f"layer bundles CPython {py} + Temurin JRE {config.java_version} (node-independent)",
            )
        )

    if proxy and yt_client is not None:
        try:
            _ = yt_client.exists("//home")
            rows.append(("YT connectivity", True, f"proxy {proxy} reachable"))
        except Exception as e:
            rows.append(("YT connectivity", False, str(e)))

    return rows
