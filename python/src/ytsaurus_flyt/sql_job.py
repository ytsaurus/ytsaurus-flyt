"""Package the SQL runner as a flyt service project and ship a Flink SQL script for ``flyt run``.

The project is a tiny setuptools package ``flyt_sql_job`` holding a copy of
:mod:`ytsaurus_flyt.sql_runner` as ``run.py``, so the job does not need ``ytsaurus_flyt``.
The script is not part of the wheel: it is a separate file of the operation.
"""

from __future__ import annotations

import contextlib
import shutil
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from ytsaurus_flyt import sql_runner

if TYPE_CHECKING:
    from yt.wrapper import YtClient

SQL_JOB_PACKAGE = "flyt_sql_job"
RUNNER_FILENAME = "run.py"
SCRIPT_FILENAME = "script.sql"
# Job command (paths relative to the unpacked wheel); the runner's own arguments follow it.
# The script is an operation file in the sandbox root, one level above the unpacked wheel.
SQL_JOB_COMMAND = (f"{SQL_JOB_PACKAGE}/{RUNNER_FILENAME}", f"../{SCRIPT_FILENAME}")

_PYPROJECT = f"""[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "flyt-sql-job"
version = "0.0.1"
description = "Flink SQL script packaged by flyt run"
requires-python = ">=3.8"

[tool.setuptools]
packages = ["{SQL_JOB_PACKAGE}"]
"""


def build_sql_job_project(dest_dir: str) -> str:
    """Create the service project in ``dest_dir`` and return the directory."""
    dest = Path(dest_dir)
    pkg = dest / SQL_JOB_PACKAGE
    pkg.mkdir(parents=True, exist_ok=True)
    (dest / "pyproject.toml").write_text(_PYPROJECT, encoding="utf-8")
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    shutil.copyfile(sql_runner.__file__, pkg / RUNNER_FILENAME)
    return str(dest)


@contextlib.contextmanager
def upload_sql_script(yt_client: YtClient, sql_path: str) -> Iterator[str]:
    """Upload the script to a temporary Cypress file for the operation's ``file_paths``; removed on exit."""
    tmp_dir = f"//tmp/flyt_sql_{uuid.uuid4().hex[:12]}"
    yt_client.mkdir(tmp_dir, recursive=True)
    remote_path = f"{tmp_dir}/{SCRIPT_FILENAME}"
    try:
        with open(sql_path, "rb") as f:
            yt_client.write_file(remote_path, f)
        yield remote_path
    finally:
        yt_client.remove(tmp_dir, recursive=True, force=True)
