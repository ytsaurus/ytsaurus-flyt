"""Build and upload SquashFS runtime layers for Vanilla operations."""

from __future__ import annotations

import logging
import os
import posixpath
import shlex
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING, List, Optional

from ytsaurus_flyt.config.config import FlytConfig, require_squashfs_runtime_config
from ytsaurus_flyt.progress import Reporter
from ytsaurus_flyt.runtime.container_runtime import (
    flyt_cache_dir,
    get_container_runtime_command,
    python_slim_image,
    run_expect_zero,
)
from ytsaurus_flyt.runtime.wheel_utils import download_runtime_wheels

if TYPE_CHECKING:
    from yt.wrapper import YtClient

logger = logging.getLogger(__name__)

_PYFLINK_HOME_PY = (
    "import os, pyflink\nfrom pathlib import Path\nprint(Path(os.path.dirname(os.path.abspath(pyflink.__file__))))"
)

_MKSQUASHFS_IMAGE = "docker.io/library/alpine:3.19"
# Glibc, shell + coreutils + CA certs (the distroless ``:latest`` has only the uv binary).
_UV_IMAGE = "ghcr.io/astral-sh/uv:bookworm-slim"


def _run_mksquashfs(
    rootfs_dir: str,
    output_path: str,
    compression: str,
) -> None:
    """Pack ``rootfs_dir`` to SquashFS. Uses host ``mksquashfs`` if present, else a container."""
    comp = compression.lower()
    if shutil.which("mksquashfs"):
        _run_mksquashfs_host(rootfs_dir, output_path, comp)
    else:
        logger.info("mksquashfs not found on host; building SquashFS in a container")
        _run_mksquashfs_container(rootfs_dir, output_path, comp)


def _run_mksquashfs_host(rootfs_dir: str, output_path: str, comp: str) -> None:
    try:
        subprocess.run(
            ["mksquashfs", rootfs_dir, output_path, "-noappend", "-comp", comp],
            check=True,
            capture_output=True,
            text=True,
            timeout=3600,
        )
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or "") + (exc.stdout or "")
        raise RuntimeError(
            f"mksquashfs failed (compression={comp!r}). "
            "Install a squashfs-tools build that supports this compressor, or set squashfs_compression to one your mksquashfs supports.\n"
            f"{err}"
        ) from exc


def _run_mksquashfs_container(rootfs_dir: str, output_path: str, comp: str) -> None:
    rootfs_dir = os.path.abspath(rootfs_dir)
    out_path = os.path.abspath(output_path)
    out_dir = os.path.dirname(out_path)
    name = os.path.basename(out_path)
    os.makedirs(out_dir, exist_ok=True)
    script = (
        "set -e && "
        "apk add --no-cache squashfs-tools >/dev/null 2>&1 && "
        f"mksquashfs /rootfs /out/{name} -noappend -comp {comp}"
    )
    cmd = get_container_runtime_command() + [
        "run",
        "--rm",
        "-v",
        f"{rootfs_dir}:/rootfs:ro",
        "-v",
        f"{out_dir}:/out",
        _MKSQUASHFS_IMAGE,
        "sh",
        "-c",
        script,
    ]
    run_expect_zero(
        cmd,
        timeout=3600,
        err_prefix=f"mksquashfs in container failed (compression={comp!r}, docker/podman + network required)",
    )
    if not os.path.isfile(out_path):
        raise RuntimeError("mksquashfs container build produced no output file")


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
    output_squashfs: str,
    reporter: Optional[Reporter] = None,
) -> None:
    """Build rootfs with ``python-runtime/`` and ``flink/`` (from PyFlink); pack to SquashFS.

    JARs are not embedded — connector JARs ship as operation ``file_paths`` instead, keeping the
    layer a canonical Flink runtime. This is a purely local build (no cluster access)."""
    require_squashfs_runtime_config(config)
    rep = reporter or Reporter()
    py_ver = (config.runtime_python_version or "").strip()
    image = python_slim_image(py_ver).rsplit("/", 1)[-1]  # e.g. python:3.9-slim
    pack_mode = "host" if shutil.which("mksquashfs") else "container"

    with tempfile.TemporaryDirectory(prefix="flyt_rootfs_") as tmp:
        root = os.path.join(tmp, "rootfs")
        os.makedirs(root, exist_ok=True)
        rt_target = os.path.join(root, "python-runtime")
        wheels_dir = os.path.join(tmp, "wheels")
        os.makedirs(wheels_dir, exist_ok=True)

        flink_requirements = config.flink_requirements
        with rep.step(f"Downloading Flink runtime wheels ({image})"):
            download_runtime_wheels(
                flink_requirements,
                output_dir=wheels_dir,
                python_version=py_ver,
            )
        with rep.step(f"Installing Flink runtime into layer ({image})"):
            _pip_install_target_in_container(py_ver, wheels_dir, flink_requirements, rt_target)

        with rep.step("Assembling Flink distribution"):
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
            os.makedirs(os.path.join(root, "flink", "lib"), exist_ok=True)

        with rep.step(f"Bundling CPython {py_ver} (uv) + Temurin JRE {config.java_version}"):
            _install_python_via_uv(py_ver, os.path.join(root, "python-dist"))
            _copy_jre_from_temurin(config.java_version, os.path.join(root, "java"))

        with rep.step(f"Packing SquashFS image ({pack_mode}, {config.squashfs_compression})"):
            os.makedirs(os.path.dirname(output_squashfs) or ".", exist_ok=True)
            _run_mksquashfs(root, output_squashfs, config.squashfs_compression)


def _install_python_via_uv(py_ver: str, dest: str) -> None:
    """Install a relocatable CPython (python-build-standalone) into ``dest`` via uv (checksummed
    manifest, no scraping). uv's download cache is host-mounted so repeat builds skip the download."""
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)
    uv_cache = flyt_cache_dir("uv")
    # Copy only bin/include/lib, skip share/: its terminfo tree has symlinks and case-colliding names
    # (terminfo/e vs terminfo/E) a Windows bind mount can't hold, and it isn't needed to run PyFlink.
    # cp -RL dereferences symlinks to real files so bin/lib survive the mount (the stdlib is case-safe).
    script = (
        f"set -e; uv python install --install-dir /tmp/u {shlex.quote(py_ver)}; "
        'src="$(echo /tmp/u/*)"; '
        'for d in bin include lib; do if [ -e "$src/$d" ]; then cp -RL "$src/$d" /dist/; fi; done; '
        "chmod -R u+rwX /dist || true"
    )
    cmd = get_container_runtime_command() + [
        "run",
        "--rm",
        "-e",
        "UV_CACHE_DIR=/uvcache",
        "-v",
        f"{uv_cache}:/uvcache",
        "-v",
        f"{dest}:/dist",
        _UV_IMAGE,
        "sh",
        "-c",
        script,
    ]
    run_expect_zero(cmd, timeout=900, err_prefix=f"Installing CPython {py_ver} via uv failed")
    if not os.path.isfile(os.path.join(dest, "bin", "python3")):
        raise RuntimeError("Bundled CPython missing (python-dist/bin/python3 not found after `uv python install`)")


def _copy_jre_from_temurin(java_version: str, dest: str) -> None:
    """Copy a Temurin JRE out of the official ``eclipse-temurin:<N>-jre`` image into ``dest``."""
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)
    image = f"docker.io/library/eclipse-temurin:{java_version}-jre"
    # cp -RL (dereference) not -a: /out may be a Windows-backed bind mount that can't hold symlinks.
    script = "set -e && cp -RL /opt/java/openjdk/. /out/ && chmod -R u+rwX /out"
    cmd = get_container_runtime_command() + ["run", "--rm", "-v", f"{dest}:/out", image, "sh", "-c", script]
    run_expect_zero(cmd, timeout=600, err_prefix=f"Copying Temurin JRE {java_version} from its image failed")
    if not os.path.isfile(os.path.join(dest, "bin", "java")):
        raise RuntimeError("Bundled JRE missing (java/bin/java not found after copy from eclipse-temurin)")


def _ensure_cypress_parent(yt_client: YtClient, cypress_path: str) -> None:
    # Cypress paths are POSIX-style; os.path mangles "//tmp/..." into a UNC path on Windows.
    parent = posixpath.dirname(cypress_path.rstrip("/"))
    if parent and not yt_client.exists(parent):
        yt_client.mkdir(parent, recursive=True)


def upload_local_file(yt_client: YtClient, local_path: str, cypress_path: str) -> None:
    """Upload a local file to an explicit Cypress path (creating the parent dir)."""
    _ensure_cypress_parent(yt_client, cypress_path)
    with open(local_path, "rb") as f:
        yt_client.write_file(cypress_path, f)


def upload_squashfs_layer(
    yt_client: YtClient,
    local_path: str,
    cypress_path: str,
    *,
    set_filesystem_attribute: bool = True,
) -> None:
    """Write SquashFS to Cypress; optionally set ``@filesystem`` = ``squashfs`` (for layer_paths)."""
    upload_local_file(yt_client, local_path, cypress_path)
    if set_filesystem_attribute:
        yt_client.set(cypress_path + "/@filesystem", "squashfs")


def build_unsquashfs_binary(output_path: str) -> None:
    """Build a static ``unsquashfs`` (squashfs-tools from source) into ``output_path`` via a container."""
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
    cmd = rt + ["run", "--rm", "-v", f"{out_dir}:/out", _MKSQUASHFS_IMAGE, "sh", "-c", build_script]
    run_expect_zero(
        cmd,
        timeout=1800,
        err_prefix="Could not build unsquashfs helper (docker/podman + network required)",
    )
    if not os.path.isfile(out_path):
        raise RuntimeError("Could not build unsquashfs helper: output binary missing after container build")
