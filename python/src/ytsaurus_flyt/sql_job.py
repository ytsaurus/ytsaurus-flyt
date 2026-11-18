"""Package a Flink SQL script as a throwaway flyt service project for ``flyt run``.

The project is a tiny setuptools package ``flyt_sql_job`` holding a copy of
:mod:`ytsaurus_flyt.sql_runner` as ``run.py`` and the script as package data, so the
job needs neither ``ytsaurus_flyt`` nor the script on the exec node.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from ytsaurus_flyt import sql_runner

SQL_JOB_PACKAGE = "flyt_sql_job"
RUNNER_FILENAME = "run.py"
SCRIPT_FILENAME = "script.sql"
# Job command (paths relative to the unpacked wheel); the runner's own arguments follow it.
SQL_JOB_COMMAND = (f"{SQL_JOB_PACKAGE}/{RUNNER_FILENAME}", f"{SQL_JOB_PACKAGE}/{SCRIPT_FILENAME}")

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

[tool.setuptools.package-data]
{SQL_JOB_PACKAGE} = ["*.sql"]
"""


def build_sql_job_project(sql_path: str, dest_dir: str) -> str:
    """Create the service project in ``dest_dir`` and return the directory."""
    dest = Path(dest_dir)
    pkg = dest / SQL_JOB_PACKAGE
    pkg.mkdir(parents=True, exist_ok=True)
    (dest / "pyproject.toml").write_text(_PYPROJECT, encoding="utf-8")
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    shutil.copyfile(sql_runner.__file__, pkg / RUNNER_FILENAME)
    shutil.copyfile(sql_path, pkg / SCRIPT_FILENAME)
    return str(dest)
