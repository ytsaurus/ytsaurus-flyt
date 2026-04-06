"""Utilities for building and uploading Python wheels to YTsaurus."""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import posixpath
import subprocess
import sys
import tempfile
import uuid
from typing import Iterator, List, Optional, Set

from yt.wrapper import YtClient

from ytsaurus_flyt.container_runtime import get_container_runtime_command, python_slim_image

logger = logging.getLogger(__name__)

SERVICE_WHEEL_NAME = "service.whl"


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dedupe_file_paths_by_basename(paths: List[str]) -> List[str]:
    """First wins per basename (Vanilla rejects duplicate file names in ``file_paths``)."""
    seen: Set[str] = set()
    out: List[str] = []
    for p in paths:
        base = posixpath.basename(p.rstrip("/"))
        if base in seen:
            logger.warning(
                "Skipping duplicate file basename %r (path %s)",
                base,
                p,
            )
            continue
        seen.add(base)
        out.append(p)
    return out


def _run_command(command: List[str], timeout: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Command failed: {cmd}\nstdout:\n{stdout}\nstderr:\n{stderr}".format(
                cmd=" ".join(command),
                stdout=exc.stdout or "<empty>",
                stderr=exc.stderr or "<empty>",
            )
        ) from exc


def _build_wheels_in_container(
    requirements: List[str],
    output_dir: str,
    python_version: str,
) -> None:
    image = python_slim_image(python_version)
    _run_command(
        get_container_runtime_command()
        + [
            "run",
            "--rm",
            "-v",
            f"{output_dir}:/out",
            image,
            "python",
            "-m",
            "pip",
            "wheel",
            "--wheel-dir",
            "/out",
            *requirements,
        ],
        timeout=1800,
    )


def build_wheel(source_dir: str, output_dir: str) -> str:
    """Build a wheel with ``pip wheel``; return path to the single ``.whl``."""
    logger.info("Building wheel from %s into %s", source_dir, output_dir)
    result = _run_command(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", output_dir, source_dir],
        timeout=1800,
    )
    logger.debug("pip wheel stdout: %s", result.stdout)

    wheels = [f for f in os.listdir(output_dir) if f.endswith(".whl")]
    if len(wheels) != 1:
        raise RuntimeError(
            "Expected exactly one .whl in %s after building from %s, got %r. stderr: %s"
            % (output_dir, source_dir, wheels, result.stderr or "")
        )
    wheel_path = os.path.join(output_dir, wheels[0])
    logger.info("Built wheel: %s", wheel_path)
    return wheel_path


def _yt_mkdir(yt_client: YtClient, path: str) -> None:
    yt_client.mkdir(path, recursive=True)


def _yt_write_file(yt_client: YtClient, path: str, local_path: str) -> None:
    with open(local_path, "rb") as f:
        yt_client.write_file(path, f)


def download_runtime_wheels(
    requirements: List[str],
    output_dir: Optional[str] = None,
    python_version: str = "",
) -> List[str]:
    if not requirements:
        return []

    pv = (python_version or "").strip()
    if not pv:
        raise ValueError("python_version is required to build runtime wheels (e.g. same as runtime_python_version).")

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="flyt_runtime_wheels_")
    output_dir = os.path.abspath(output_dir)

    logger.info("Building runtime wheelhouse for %s", ", ".join(requirements))
    logger.info("Using python:%s-slim to prebuild runtime wheels", pv)
    _build_wheels_in_container(requirements, output_dir, pv)

    wheel_paths = sorted(os.path.join(output_dir, fname) for fname in os.listdir(output_dir) if fname.endswith(".whl"))
    wheel_paths = dedupe_file_paths_by_basename(wheel_paths)
    if not wheel_paths:
        raise RuntimeError("No wheels were downloaded for runtime requirements: %r" % (requirements,))
    return wheel_paths


@contextlib.contextmanager
def upload_wheel(
    yt_client: YtClient,
    wheel_path: str,
    cache_prefix: Optional[str] = None,
) -> Iterator[str]:
    if cache_prefix:
        digest = _sha256_file(wheel_path)
        cache_path = f"{cache_prefix.rstrip('/')}/wheels/{digest}/{SERVICE_WHEEL_NAME}"
        if yt_client.exists(cache_path):
            logger.info("Using cached wheel at %s", cache_path)
            yield cache_path
            return

        parent = cache_path.rsplit("/", 1)[0]
        _yt_mkdir(yt_client, parent)
        _yt_write_file(yt_client, cache_path, wheel_path)
        logger.info("Cached wheel at %s", cache_path)
        yield cache_path
    else:
        tmp_dir = f"//tmp/flyt_wheel_{uuid.uuid4().hex[:12]}"
        _yt_mkdir(yt_client, tmp_dir)
        remote_path = f"{tmp_dir}/{SERVICE_WHEEL_NAME}"
        try:
            _yt_write_file(yt_client, remote_path, wheel_path)
            logger.info("Uploaded wheel to %s", remote_path)
            yield remote_path
        finally:
            try:
                yt_client.remove(tmp_dir, recursive=True, force=True)
                logger.debug("Cleaned up temp dir %s", tmp_dir)
            except Exception:
                logger.warning("Failed to clean up temp dir %s", tmp_dir, exc_info=True)


@contextlib.contextmanager
def upload_service_wheel(
    yt_client: YtClient,
    wheel_path: Optional[str] = None,
    source_dir: Optional[str] = None,
    cache_prefix: Optional[str] = None,
) -> Iterator[str]:
    if wheel_path is None and source_dir is None:
        raise ValueError("Either wheel_path or source_dir must be provided")

    if wheel_path is not None:
        with upload_wheel(yt_client, wheel_path, cache_prefix) as remote_path:
            yield remote_path
    else:
        if not source_dir:
            raise ValueError("source_dir must be non-empty when wheel_path is None")
        with tempfile.TemporaryDirectory(prefix="flyt_wheel_") as tmp:
            local_wheel = build_wheel(source_dir, output_dir=tmp)
            with upload_wheel(yt_client, local_wheel, cache_prefix) as remote_path:
                yield remote_path
