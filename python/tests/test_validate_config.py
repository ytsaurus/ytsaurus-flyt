"""flyt validate rows for application mode: the cluster as flyt run resolves it."""

from unittest.mock import MagicMock

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.validate_config import validate_flyt_config


def _rows(config: FlytConfig, yt_client=None):
    return [row for row in validate_flyt_config(config, yt_client=yt_client) if row[0] != "Runtime mode"]


def _named(rows, name):
    return [(ok, msg) for n, ok, msg in rows if n == name]


def _app(**kw) -> FlytConfig:
    return FlytConfig(cluster_mode="application", **kw)


def test_validate_reads_the_local_flink_conf_file(tmp_path):
    conf = tmp_path / "config.yaml"
    conf.write_text("taskmanager:\n  numberOfTaskSlots: 4\n")
    rows = _rows(_app(taskmanager_count=2, base_flink_conf_path=str(conf)))
    ((ok, msg),) = _named(rows, "cluster_mode")
    assert ok is True
    assert "2 TaskManager(s) x 4 slot(s) (4 cpu, 4096m JVM each), parallelism.default 8" in msg
    assert any("(1 key(s))" in m for _, m in _named(rows, "flink config.yaml"))
    # slots are set, so there is no warning about them
    assert all(ok is not None for ok, _ in _named(rows, "flink config.yaml"))


def test_validate_reads_a_cypress_flink_conf_file_through_the_client():
    client = MagicMock()
    client.read_file.return_value.read.return_value = b"taskmanager.numberOfTaskSlots: 3\n"
    rows = _rows(_app(base_flink_conf_path="//home/flyt/config.yaml"), yt_client=client)
    assert "x 3 slot(s)" in _named(rows, "cluster_mode")[0][1]


def test_validate_warns_when_a_cypress_file_cannot_be_read():
    rows = _rows(_app(base_flink_conf_path="//home/flyt/config.yaml"))
    assert _named(rows, "base_flink_conf_path")[0][0] is None


def test_validate_fails_on_parallelism_above_capacity_and_bad_files(tmp_path):
    rows = _rows(_app(flink_config_override={"parallelism.default": "5"}))
    assert _named(rows, "parallelism.default")[0][0] is False
    rows = _rows(_app(base_flink_conf_path=str(tmp_path / "missing.yaml")))
    assert _named(rows, "base_flink_conf_path")[0][0] is False
    rows = _rows(_app(flink_config_override={"taskmanager.memory.flink.size": "7g"}))
    assert _named(rows, "cluster_mode")[0][0] is False


def test_validate_reports_flink_config_warnings():
    rows = _rows(_app(flink_config_override={"taskmanager.host": "h"}))
    warnings = [msg for ok, msg in _named(rows, "flink config.yaml") if ok is None]
    assert any(m.startswith("taskmanager.host is set per YT job") for m in warnings)
    assert any(m.startswith("taskmanager.numberOfTaskSlots is not set") for m in warnings)


def test_validate_minicluster_warns_about_ignored_flink_options(tmp_path):
    rows = _rows(FlytConfig())
    assert _named(rows, "cluster_mode") == [(True, "minicluster (single job, in-JVM MiniCluster)")]
    assert not _named(rows, "flink config.yaml")
    rows = _rows(FlytConfig(base_flink_conf_path=str(tmp_path / "missing.yaml"), flink_config_override={"a.b": "1"}))
    ((ok, msg),) = _named(rows, "flink config.yaml")
    assert ok is None  # a warning: the file is not even read in this mode
    assert "apply to application mode only" in msg
    assert not _named(rows, "base_flink_conf_path")
