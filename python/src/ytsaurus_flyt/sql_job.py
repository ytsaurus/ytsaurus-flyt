"""Ship the bundled SQL runner and a Flink SQL script as files of the operation for ``flyt run``.

The job has no service wheel: :mod:`ytsaurus_flyt.sql_runner` is self-contained, so its file and the
script are all the job needs besides the runtime layer. ``flyt install`` publishes the same job as a
manifest: the operation spec of each cluster mode, for launchers that do not run flyt.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import logging
import shlex
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from ytsaurus_flyt import __version__, sql_runner
from ytsaurus_flyt.config import ClusterMode, FlytConfig
from ytsaurus_flyt.launcher import build_job_spec
from ytsaurus_flyt.layer_builder import layer_cache_prefix, tools_cache_prefix
from ytsaurus_flyt.spec import resolve_flink_cluster

if TYPE_CHECKING:
    from yt.wrapper import YtClient

logger = logging.getLogger(__name__)

SQL_JOB_SERVICE_NAME = "flyt_sql_job"
RUNNER_FILENAME = "sql_runner.py"
SCRIPT_FILENAME = "script.sql"
# Operation files land in the sandbox root, one level above the job's working directory.
# The runner's own arguments follow the command.
SQL_JOB_COMMAND = (f"../{RUNNER_FILENAME}", f"../{SCRIPT_FILENAME}")
MANIFEST_NAME = "manifest"
MANIFEST_VERSION = 1


def ensure_sql_runner_remote(yt_client: YtClient, config: FlytConfig) -> str:
    """Cypress path of the bundled runner, uploaded if missing; the directory is named by the file's hash."""
    with open(sql_runner.__file__, "rb") as f:
        content = f.read()
    remote_dir = f"{tools_cache_prefix(config)}/sql_runner/{hashlib.sha256(content).hexdigest()[:16]}"
    remote_path = f"{remote_dir}/{RUNNER_FILENAME}"
    if not yt_client.exists(remote_path):
        logger.info("Uploading the SQL runner to %s", remote_path)
        yt_client.mkdir(remote_dir, recursive=True)
        yt_client.write_file(remote_path, content)
    return remote_path


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


def sql_job_config(config: FlytConfig, job_files: List[str]) -> FlytConfig:
    """``config`` of a job that runs the bundled runner from ``job_files``."""
    return dataclasses.replace(
        config,
        extra_file_paths=[*config.extra_file_paths, *job_files],
        service_name=config.service_name or SQL_JOB_SERVICE_NAME,
    )


def publish_sql_manifest(yt_client: YtClient, config: FlytConfig, profile_name: Optional[str] = None) -> str:
    """Write the manifest of the SQL job as a Cypress document next to the runtime layers; return its path.

    Each spec is what ``flyt run script.sql`` submits in that cluster mode, minus what belongs to a run:
    the script file (``script_file_name`` in every task's ``file_paths``), the pool and ``secure_vault``.
    """
    config = sql_job_config(config, [ensure_sql_runner_remote(yt_client, config)])
    specs: Dict[str, Any] = {}
    for mode in ClusterMode:
        mode_config = dataclasses.replace(config, cluster_mode=mode)
        cluster = resolve_flink_cluster(mode_config, yt_client)
        builder = build_job_spec(
            mode_config, yt_client, shlex.join(SQL_JOB_COMMAND), None, cluster, {}, profile_name=profile_name
        )
        spec = builder.build(client=yt_client)
        # Facts of this install run, not of the job.
        for key in ("started_by", "secure_vault"):
            spec.pop(key, None)
        specs[mode.value] = spec
    manifest = {
        "version": MANIFEST_VERSION,
        "flyt_version": __version__,
        "sql": {"script_file_name": SCRIPT_FILENAME, "spec": specs},
    }
    remote_path = f"{layer_cache_prefix(config)}/{MANIFEST_NAME}"
    yt_client.create("document", remote_path, recursive=True, ignore_existing=True)
    yt_client.set(remote_path, manifest)
    return remote_path
