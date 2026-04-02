"""Helpers for flyt CLI: wheel auto-build and connection resolution."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple


def first_script_path_from_job_command(job_command: str) -> Optional[str]:
    """First token that looks like a path to a .py file."""
    parts = job_command.strip().split()
    if not parts:
        return None
    first = parts[0]
    if first.startswith("-") or not first.endswith(".py"):
        return None
    return first


def resolve_script_path(job_command: str, cwd: Optional[str] = None) -> Optional[str]:
    """Absolute path to the entry script if it exists on disk."""
    rel = first_script_path_from_job_command(job_command)
    if not rel:
        return None
    base = cwd or os.getcwd()
    p = Path(rel)
    if not p.is_absolute():
        p = Path(base) / p
    p = p.resolve()
    if p.is_file():
        return str(p)
    return None


def find_pyproject_for_job(job_command: str, cwd: Optional[str] = None) -> Optional[str]:
    """Directory containing pyproject.toml next to the job script, if any."""
    sp = resolve_script_path(job_command, cwd=cwd)
    if not sp:
        return None
    parent = Path(sp).parent
    pp = parent / "pyproject.toml"
    if pp.is_file():
        return str(parent)
    return None


def job_command_relative_to_project(job_command: str, project_dir: str) -> str:
    """Rewrite the first ``.py`` path to be relative to ``project_dir`` (matches wheel unpack layout)."""
    parts = job_command.strip().split()
    if not parts:
        return job_command
    if parts[0].startswith("-") or not parts[0].endswith(".py"):
        return job_command
    sp = resolve_script_path(job_command)
    if not sp:
        return job_command
    try:
        rel = Path(sp).resolve().relative_to(Path(project_dir).resolve())
    except ValueError:
        return job_command
    parts[0] = rel.as_posix()
    return " ".join(parts)


def resolve_proxy_pool(
    profile_proxy: Optional[str],
    profile_pool: Optional[str],
    cli_proxy: Optional[str],
    cli_pool: Optional[str],
) -> Tuple[str, str]:
    """Merge profile, CLI, env (FLYT_* / YT_*)."""
    proxy = (cli_proxy or "").strip() or (profile_proxy or "").strip()
    proxy = proxy or (os.environ.get("FLYT_PROXY") or os.environ.get("YT_PROXY") or "").strip()
    pool = (cli_pool or "").strip() or (profile_pool or "").strip()
    pool = pool or (os.environ.get("FLYT_POOL") or os.environ.get("YT_POOL") or "").strip()
    if not proxy:
        raise ValueError(
            "No YT HTTP proxy configured. Set proxy in a flyt profile, pass --proxy, or set FLYT_PROXY / YT_PROXY."
        )
    if not pool:
        raise ValueError("No pool configured. Set pool in a flyt profile, pass --pool, or set FLYT_POOL / YT_POOL.")
    return proxy, pool
