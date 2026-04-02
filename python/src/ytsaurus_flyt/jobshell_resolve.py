"""Resolve default ``yt`` argv for ``flyt jobshell`` when no subcommand is given."""

from __future__ import annotations

import re
from typing import List, Optional

from yt.wrapper import YtClient

# Embedded in Vanilla operation titles and used as list_operations filter substring.
_FLYT_PROFILE_KEY = "flyt-profile"

# list_operations / list_jobs limits (API caps; enough for typical single-job flyt runs)
LIST_OPERATIONS_LIMIT = 40
LIST_JOBS_LIMIT = 200


def profile_title_tag(profile_name: str) -> str:
    """Stable ASCII token derived from profile name (for titles / filters)."""
    s = "".join(c if c.isalnum() or c in "-_" else "_" for c in (profile_name or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "default"


def flyt_profile_marker(profile_name: str) -> str:
    """Substring in operation titles for this profile, e.g. ``[flyt-profile:kind]``."""
    return f"[{_FLYT_PROFILE_KEY}:{profile_title_tag(profile_name)}]"


def operation_title_for_profile(job_command: str, profile_name: Optional[str]) -> str:
    """Vanilla operation title: includes profile marker when ``profile_name`` is set."""
    if profile_name and str(profile_name).strip():
        return f"FLYT {flyt_profile_marker(profile_name)} {job_command}"
    return f"FLYT: {job_command}"


def resolve_default_jobshell_argv(yt_client: YtClient, profile_name: str) -> Optional[List[str]]:
    """Return ``[\"run-job-shell\", job_id]`` for this profile's running operation, or ``None``.

    Matches **running** operations whose title contains the profile marker (newest first), then
    picks a **running** job (prefers jobmanager-like task).
    """
    marker = flyt_profile_marker(profile_name)
    resp = yt_client.list_operations(state="running", filter=marker, limit=LIST_OPERATIONS_LIMIT)
    ops = list(resp.get("operations") or [])
    ops.sort(key=lambda o: str(o.get("start_time") or ""), reverse=True)

    for op in ops:
        op_id = op.get("id")
        if not op_id:
            continue
        job_id = _pick_running_job_id(yt_client, str(op_id))
        if job_id:
            return ["run-job-shell", job_id]

    return None


def _pick_running_job_id(yt_client: YtClient, operation_id: str) -> Optional[str]:
    resp = yt_client.list_jobs(
        operation_id,
        job_state="running",
        limit=LIST_JOBS_LIMIT,
        sort_field="start_time",
        sort_order="ascending",
    )
    jobs = list(resp.get("jobs") or [])
    if not jobs:
        return None

    for j in jobs:
        tn = str(j.get("task_name") or "").lower()
        if "jobmanager" in tn or "job_manager" in tn:
            jid = j.get("id")
            if jid:
                return str(jid)

    first = jobs[0].get("id")
    return str(first) if first else None
