"""Ship the bundled SQL runner and a Flink SQL script as files of the operation for ``flyt run``.

The job has no service wheel: :mod:`ytsaurus_flyt.sql_runner` is self-contained, so its file and the
script are all the job needs besides the runtime layer.
"""

from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING, Iterator, List

from ytsaurus_flyt import sql_runner

if TYPE_CHECKING:
    from yt.wrapper import YtClient

SQL_JOB_SERVICE_NAME = "flyt_sql_job"
RUNNER_FILENAME = "sql_runner.py"
SCRIPT_FILENAME = "script.sql"
# Operation files land in the sandbox root, one level above the job's working directory.
# The runner's own arguments follow the command.
SQL_JOB_COMMAND = (f"../{RUNNER_FILENAME}", f"../{SCRIPT_FILENAME}")


@contextlib.contextmanager
def upload_sql_job(yt_client: YtClient, sql_path: str) -> Iterator[List[str]]:
    """Upload the runner and the script to temporary Cypress files for ``file_paths``; removed on exit."""
    tmp_dir = f"//tmp/flyt_sql_{uuid.uuid4().hex[:12]}"
    yt_client.mkdir(tmp_dir, recursive=True)
    remote_paths = []
    try:
        for local_path, name in ((sql_runner.__file__, RUNNER_FILENAME), (sql_path, SCRIPT_FILENAME)):
            with open(local_path, "rb") as f:
                yt_client.write_file(f"{tmp_dir}/{name}", f)
            remote_paths.append(f"{tmp_dir}/{name}")
        yield remote_paths
    finally:
        yt_client.remove(tmp_dir, recursive=True, force=True)
