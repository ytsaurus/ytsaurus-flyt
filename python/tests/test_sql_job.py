"""Shipping of the SQL runner and the script as files of the operation, and the published manifest."""

from pathlib import Path
from unittest.mock import MagicMock

from yt.wrapper import YtClient

from ytsaurus_flyt import launcher, sql_runner
from ytsaurus_flyt import spec as spec_module
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.sql_job import ensure_sql_runner_remote, publish_sql_manifest, upload_sql_script


class FakeYt:
    def __init__(self):
        self.files, self.removed = {}, []

    def exists(self, path):
        return path in self.files

    def mkdir(self, path, recursive):
        pass

    def write_file(self, path, stream):
        self.files[path] = stream if isinstance(stream, bytes) else stream.read()

    def remove(self, path, recursive, force):
        self.removed.append(path)


def test_upload_sql_script_ships_a_temporary_operation_file(tmp_path: Path) -> None:
    sql = tmp_path / "q.sql"
    sql.write_text("SELECT 1;", encoding="utf-8")
    yt = FakeYt()
    with upload_sql_script(yt, str(sql)) as script:
        tmp_dir = script.rsplit("/", 1)[0]
        assert script == f"{tmp_dir}/script.sql"
        assert yt.files == {script: b"SELECT 1;"}
        assert yt.removed == []
    assert yt.removed == [tmp_dir]


def test_sql_runner_is_uploaded_once_per_content() -> None:
    yt = FakeYt()
    cfg = FlytConfig(squashfs_tools_cache_prefix="//home/flyt/tools/")
    runner = ensure_sql_runner_remote(yt, cfg)
    assert runner.startswith("//home/flyt/tools/sql_runner/") and runner.endswith("/sql_runner.py")
    assert yt.files == {runner: Path(sql_runner.__file__).read_bytes()}
    yt.write_file = None  # a second call must not upload
    assert ensure_sql_runner_remote(yt, cfg) == runner


def test_manifest_holds_the_spec_of_each_cluster_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        launcher, "resolve_flink_lib_jars", lambda *a, **k: MagicMock(yt_paths=[], extra_runtime_basenames=[])
    )
    monkeypatch.setattr(launcher, "partition_flink_lib_jars_for_delivery", lambda *a, **k: ([], []))
    monkeypatch.setattr(launcher, "ensure_runtime_layer", lambda *a, **k: "//home/flyt/layers/runtime-abc.squashfs")
    monkeypatch.setattr(spec_module, "proxy_url_from_client", lambda _c: "http://proxy")
    monkeypatch.setattr(spec_module, "job_yt_proxy", lambda _c, url, _j: url)
    # a real client object: the spec builder reads its config
    yt = YtClient(proxy="http://proxy.invalid", config={"apply_remote_patch_at_start": None})
    yt.exists, yt.create, yt.set = MagicMock(return_value=True), MagicMock(), MagicMock()
    cfg = FlytConfig(
        squashfs_layer_cache_prefix="//home/flyt/layers",
        squashfs_tools_cache_prefix="//home/flyt/tools",
        flink_config_override={"flyt.taskmanager.replicas": "3"},
    )

    assert publish_sql_manifest(yt, cfg, "prod") == "//home/flyt/layers/manifest"
    path, manifest = yt.set.call_args[0]
    assert path == "//home/flyt/layers/manifest"
    assert manifest["version"] == 1 and manifest["sql"]["script_file_name"] == "script.sql"
    specs = manifest["sql"]["spec"]
    assert set(specs) == {"minicluster", "application"}
    assert set(specs["minicluster"]["tasks"]) == {"flink_minicluster"}
    assert specs["application"]["tasks"]["taskmanager"]["job_count"] == 3
    for spec in specs.values():
        # what belongs to a run is left to the launcher
        assert "secure_vault" not in spec and "started_by" not in spec and "pool" not in spec
        for task in spec["tasks"].values():
            runner, layer = task["file_paths"][0], task["layer_paths"]
            assert runner.startswith("//home/flyt/tools/sql_runner/") and runner.endswith("/sql_runner.py")
            assert layer == ["//home/flyt/layers/runtime-abc.squashfs"]
    for task in (specs["minicluster"]["tasks"]["flink_minicluster"], specs["application"]["tasks"]["jobmanager"]):
        assert "set -- ../sql_runner.py ../script.sql\n" in task["command"]
