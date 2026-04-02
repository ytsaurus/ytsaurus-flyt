"""Pre-flight validation for flyt run / install (no job submission)."""

from __future__ import annotations

import shutil
from typing import List, Optional, Tuple

from yt.wrapper import YtClient

from ytsaurus_flyt.config import (
    SQUASHFS_VALIDATE_RUNTIME_PACKAGES_MSG,
    SQUASHFS_VALIDATE_RUNTIME_VERSION_MSG,
    FlytConfig,
)
from ytsaurus_flyt.container_runtime import get_container_runtime_command


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


def validate_flyt_config(
    config: FlytConfig,
    *,
    proxy: Optional[str] = None,
    yt_client: Optional[YtClient] = None,
) -> List[Tuple[str, bool, str]]:
    """Return list of (name, ok, message)."""
    rows: List[Tuple[str, bool, str]] = []

    rows.append(("Runtime mode", True, "SquashFS"))

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
