"""Build and upload SquashFS runtime layers for Vanilla operations."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING, List

from ytsaurus_flyt.config import FlytConfig, require_squashfs_runtime_config
from ytsaurus_flyt.container_runtime import get_container_runtime_command, python_slim_image, run_expect_zero
from ytsaurus_flyt.flink_lib_jars import download_flink_lib_jars
from ytsaurus_flyt.wheel_utils import download_runtime_wheels

if TYPE_CHECKING:
    from yt.wrapper import YtClient

logger = logging.getLogger(__name__)

LAYER_SCHEMA_VERSION = "1"

_PYFLINK_HOME_PY = (
    "import os, pyflink\nfrom pathlib import Path\nprint(Path(os.path.dirname(os.path.abspath(pyflink.__file__))))"
)


def compute_layer_hash(
    config: FlytConfig,
    flink_lib_jar_yt_paths: List[str],
) -> str:
    payload = {
        "v": LAYER_SCHEMA_VERSION,
        "runtime_python_packages": sorted(config.runtime_python_packages),
        "runtime_python_version": config.runtime_python_version,
        "java_home": config.java_home,
        "python_bin": config.python_bin,
        "squashfs_compression": config.squashfs_compression,
        "flink_lib_jar_yt_paths": sorted(flink_lib_jar_yt_paths),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def _run_mksquashfs(
    rootfs_dir: str,
    output_path: str,
    compression: str,
) -> None:
    comp = compression.lower()
    try:
        subprocess.run(
            ["mksquashfs", rootfs_dir, output_path, "-noappend", "-comp", comp],
            check=True,
            capture_output=True,
            text=True,
            timeout=3600,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("mksquashfs not found. Install squashfs-tools (e.g. apt install squashfs-tools).") from exc
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or "") + (exc.stdout or "")
        raise RuntimeError(
            f"mksquashfs failed (compression={comp!r}). "
            "Install a squashfs-tools build that supports this compressor, or set squashfs_compression to one your mksquashfs supports.\n"
            f"{err}"
        ) from exc


def _pip_install_target_in_container(
    python_version: str,
    wheel_dir: str,
    requirements: List[str],
    target_dir: str,
) -> None:
    wheel_dir = os.path.abspath(wheel_dir)
    target_dir = os.path.abspath(target_dir)
    os.makedirs(target_dir, exist_ok=True)
    image = python_slim_image(python_version)
    cmd = get_container_runtime_command() + [
        "run",
        "--rm",
        "-v",
        f"{wheel_dir}:/wheels:ro",
        "-v",
        f"{target_dir}:/target",
        image,
        "python",
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-input",
        "--no-compile",
        "--no-index",
        "--find-links",
        "/wheels",
        "--target",
        "/target",
        *requirements,
    ]
    run_expect_zero(cmd, timeout=3600, err_prefix="pip install into layer (container) failed")


def _pyflink_home_in_container(python_version: str, pythonpath: str) -> str:
    pythonpath = os.path.abspath(pythonpath)
    image = python_slim_image(python_version)
    cmd = get_container_runtime_command() + [
        "run",
        "--rm",
        "-e",
        "PYTHONPATH=/py",
        "-v",
        f"{pythonpath}:/py",
        image,
        "python",
        "-c",
        _PYFLINK_HOME_PY,
    ]
    proc = run_expect_zero(
        cmd,
        timeout=120,
        err_prefix="Could not locate pyflink in layer (container)",
    )
    return proc.stdout.strip()


def build_runtime_squashfs(
    config: FlytConfig,
    local_jar_paths: List[str],
    output_squashfs: str,
) -> None:
    """Build rootfs with ``python-runtime/``, ``flink/``, embedded JARs; pack to SquashFS."""
    require_squashfs_runtime_config(config)
    py_ver = (config.runtime_python_version or "").strip()

    with tempfile.TemporaryDirectory(prefix="flyt_rootfs_") as tmp:
        root = os.path.join(tmp, "rootfs")
        os.makedirs(root, exist_ok=True)
        rt_target = os.path.join(root, "python-runtime")
        wheels_dir = os.path.join(tmp, "wheels")
        os.makedirs(wheels_dir, exist_ok=True)

        download_runtime_wheels(
            config.runtime_python_packages,
            output_dir=wheels_dir,
            python_version=py_ver,
        )
        _pip_install_target_in_container(
            py_ver,
            wheels_dir,
            config.runtime_python_packages,
            rt_target,
        )
        container_pyflink = _pyflink_home_in_container(py_ver, rt_target)
        pyflink_home = os.path.join(rt_target, os.path.relpath(container_pyflink, "/py"))
        logger.info("PyFlink home (host): %s", pyflink_home)
        for sub in ("lib", "opt", "plugins", "log", "conf"):
            src = os.path.join(pyflink_home, sub)
            dst = os.path.join(root, "flink", sub)
            if os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)
                logger.info("Copied %s to %s", src, dst)
            else:
                logger.debug("Skipped %s (not a directory)", src)

        flink_lib = os.path.join(root, "flink", "lib")
        os.makedirs(flink_lib, exist_ok=True)
        for jar in local_jar_paths:
            base = os.path.basename(jar)
            shutil.copy2(jar, os.path.join(flink_lib, base))

        os.makedirs(os.path.dirname(output_squashfs) or ".", exist_ok=True)
        _run_mksquashfs(root, output_squashfs, config.squashfs_compression)


def _ensure_cypress_parent(yt_client: YtClient, cypress_path: str) -> None:
    parent = os.path.dirname(cypress_path.rstrip("/"))
    if parent and not yt_client.exists(parent):
        yt_client.mkdir(parent, recursive=True)


def upload_squashfs_layer(
    yt_client: YtClient,
    local_path: str,
    cypress_path: str,
    *,
    set_filesystem_attribute: bool = True,
) -> None:
    """Write SquashFS to Cypress; optionally set ``@filesystem`` = ``squashfs``."""
    _ensure_cypress_parent(yt_client, cypress_path)

    with open(local_path, "rb") as f:
        yt_client.write_file(cypress_path, f)

    if set_filesystem_attribute:
        _set_filesystem_squashfs(yt_client, cypress_path)


def _build_unsquashfs_binary_docker(output_path: str) -> None:
    """Static ``unsquashfs`` via Alpine container (squashfs-tools from source)."""
    rt = get_container_runtime_command()
    out_path = os.path.abspath(output_path)
    out_dir = os.path.dirname(out_path)
    name = os.path.basename(out_path)
    os.makedirs(out_dir, exist_ok=True)

    build_script = (
        "set -e && "
        "apk add --no-cache build-base zlib-dev zlib-static lzo-dev lz4-dev xz-dev zstd-dev git >/dev/null 2>&1 && "
        "cd /tmp && "
        "git clone --depth 1 --branch 4.6.1 https://github.com/plougher/squashfs-tools.git && "
        "cd squashfs-tools/squashfs-tools && "
        "make LDFLAGS=-static unsquashfs -j$(nproc) && "
        f"cp unsquashfs /out/{name} && chmod +x /out/{name}"
    )
    cmd = rt + [
        "run",
        "--rm",
        "-v",
        f"{out_dir}:/out",
        "docker.io/library/alpine:3.19",
        "sh",
        "-c",
        build_script,
    ]
    run_expect_zero(
        cmd,
        timeout=1800,
        err_prefix="Could not build flyt_unsquashfs helper (docker/podman + network required)",
    )
    if not os.path.isfile(out_path):
        raise RuntimeError("Could not build flyt_unsquashfs helper: output binary missing after container build")


def ensure_unsquashfs_tool_remote(yt_client: YtClient, config: FlytConfig) -> str:
    prefix = (config.squashfs_tools_cache_prefix or "").strip() or "//tmp/flyt_squashfs_tools"
    prefix = prefix.rstrip("/")
    remote_path = f"{prefix}/flyt_unsquashfs"

    if yt_client.exists(remote_path):
        logger.info("Using cached unsquashfs helper at %s", remote_path)
        return remote_path

    logger.info("Building and uploading unsquashfs helper to %s", remote_path)
    with tempfile.TemporaryDirectory(prefix="flyt_unsquashfs_") as tmp:
        local_path = os.path.join(tmp, "flyt_unsquashfs")
        _build_unsquashfs_binary_docker(local_path)
        _ensure_cypress_parent(yt_client, remote_path)
        with open(local_path, "rb") as f:
            yt_client.write_file(remote_path, f)

    logger.info("Uploaded unsquashfs helper to %s", remote_path)
    return remote_path


def _set_filesystem_squashfs(yt_client: YtClient, cypress_path: str) -> None:
    yt_client.set(cypress_path + "/@filesystem", "squashfs")


def ensure_runtime_layer(
    yt_client: YtClient,
    config: FlytConfig,
    flink_lib_jar_yt_paths: List[str],
    *,
    force_rebuild: bool = False,
) -> str:
    """Return Cypress path to the runtime layer (build and upload if missing)."""
    h = compute_layer_hash(config, flink_lib_jar_yt_paths)
    prefix = config.squashfs_layer_cache_prefix.strip() or "//tmp/flyt_squashfs_layers"
    prefix = prefix.rstrip("/")
    remote_path = f"{prefix}/runtime-{h}.squashfs"

    if yt_client.exists(remote_path):
        if not force_rebuild:
            logger.info("Using existing SquashFS layer at %s", remote_path)
            return remote_path
        logger.info("Removing cached SquashFS layer at %s (force rebuild)", remote_path)
        yt_client.remove(remote_path, force=True)

    logger.info("Building SquashFS runtime layer (hash=%s)...", h)
    with tempfile.TemporaryDirectory(prefix="flyt_squashfs_") as tmp:
        jar_dir = os.path.join(tmp, "jars")
        local_jars = download_flink_lib_jars(yt_client, flink_lib_jar_yt_paths, jar_dir)
        out_local = os.path.join(tmp, "runtime.squashfs")
        build_runtime_squashfs(config, local_jars, out_local)
        set_fs = config.squashfs_layer_delivery == "layer_paths"
        upload_squashfs_layer(
            yt_client,
            out_local,
            remote_path,
            set_filesystem_attribute=set_fs,
        )

    logger.info("Uploaded SquashFS layer to %s", remote_path)
    return remote_path
