"""Docker/Podman resolution for wheel and layer builds."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Sequence


def flyt_cache_dir(*sub: str) -> str:
    """Persistent build cache (``FLYT_CACHE_DIR`` or ``~/.cache/flyt``); mounted into build
    containers to reuse wheel/uv downloads across ``flyt build layer`` runs."""
    base = os.environ.get("FLYT_CACHE_DIR", "").strip()
    root = Path(base).expanduser() if base else Path.home() / ".cache" / "flyt"
    d = root.joinpath(*sub)
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


def cache_owner_run_args() -> List[str]:
    """``run`` flags so a build container can write the host-mounted pip/uv cache.

    pip silently disables caching on a dir it doesn't own; rootless podman / Docker Desktop mounts
    are owned by a non-root uid (1000 in the podman machine on Windows). Running as that uid makes
    pip's check a plain writability check; ``HOME=/tmp`` gives the passwd-less uid a writable home.
    """
    uid = os.getuid() if hasattr(os, "getuid") else 1000
    gid = os.getgid() if hasattr(os, "getgid") else 1000
    return ["--user", f"{uid}:{gid}", "-e", "HOME=/tmp"]


def get_container_runtime_command() -> List[str]:
    """Return ``[podman|docker]`` (podman wins if both exist)."""
    for name in ("podman", "docker"):
        candidate = shutil.which(name)
        if candidate:
            return [candidate]

    raise RuntimeError(
        "Could not find podman or docker. Install one of them for "
        "runtime_python_version / SquashFS layer builds that use containerized pip."
    )


def python_slim_image(python_major_minor: str) -> str:
    """Official image used for wheel builds and pip install into the SquashFS layer."""
    v = (python_major_minor or "").strip()
    if not v:
        raise ValueError("python_major_minor must be non-empty")
    return f"docker.io/library/python:{v}-slim"


def run_expect_zero(
    cmd: Sequence[str],
    *,
    timeout: int,
    err_prefix: str,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess; raise RuntimeError with stdout/stderr if exit code is non-zero."""
    proc = subprocess.run(
        list(cmd),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{err_prefix}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return proc
