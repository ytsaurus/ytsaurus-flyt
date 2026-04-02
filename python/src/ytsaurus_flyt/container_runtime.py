"""Docker/Podman resolution for wheel and layer builds."""

from __future__ import annotations

import shutil
import subprocess
from typing import List, Sequence


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
