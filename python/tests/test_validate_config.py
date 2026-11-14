"""flyt validate rows for application mode: the cluster as flyt run resolves it."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import ClusterParams, ClusterPreset
from ytsaurus_flyt.validate_config import validate_flyt_config


def _named(rows, name):
    return [(ok, msg) for n, ok, msg in rows if n == name]


def test_validate_prints_the_application_cluster_sized_by_the_preset():
    rows = validate_flyt_config(
        FlytConfig(cluster_mode="application", taskmanager_count=2), preset=ClusterPreset.LARGE.params
    )
    ((ok, msg),) = _named(rows, "cluster_mode")
    assert ok is True
    assert "2 TaskManager(s) x 4 slot(s) (4 cpu, 16384m JVM each), parallelism.default 8" in msg
    assert _named(rows, "discovery_timeout")


def test_validate_defaults_to_the_micro_preset_and_fails_on_a_too_small_one():
    rows = validate_flyt_config(FlytConfig(cluster_mode="application"))
    assert "x 2 slot(s)" in _named(rows, "cluster_mode")[0][1]
    tiny = ClusterParams(cpu=1, memory="512M", max_heap_size="256M", data_size_per_job="1M")
    rows = validate_flyt_config(FlytConfig(cluster_mode="application"), preset=tiny)
    assert _named(rows, "cluster_mode")[0][0] is False


def test_validate_minicluster_has_no_application_rows():
    rows = validate_flyt_config(FlytConfig())
    assert _named(rows, "cluster_mode") == [(True, "minicluster (single job, in-JVM MiniCluster)")]
    assert not _named(rows, "discovery_timeout")


def test_validate_minicluster_sized_as_one_taskmanager():
    cfg = FlytConfig(taskmanager_cpu=3, flink_config_override={"taskmanager.memory.process.size": "6g"})
    assert _named(validate_flyt_config(cfg), "cluster_mode") == [(True, "minicluster: one TaskManager (3 cpu, 6144m)")]
    bad = FlytConfig(flink_config_override={"taskmanager.memory.task.heap.size": "1g"})
    ((ok, msg),) = _named(validate_flyt_config(bad), "cluster_mode")
    assert ok is False and "needs taskmanager_cpu (--tm-cpu) and taskmanager.memory.process.size" in msg


def test_validate_reads_the_flink_config_in_both_modes(tmp_path):
    conf = tmp_path / "flink.yaml"
    conf.write_text("taskmanager.numberOfTaskSlots: 3\nrest.port: 8081\n")
    for mode in ("minicluster", "application"):
        rows = validate_flyt_config(FlytConfig(cluster_mode=mode, base_flink_conf_path=str(conf)))
        flink_rows = _named(rows, "flink config.yaml")
        assert flink_rows[0] == (True, f"{conf}, 2 user option(s)")
        assert any(ok is None and "rest.port" in msg for ok, msg in flink_rows)  # a warning, not a failure
    rows = validate_flyt_config(FlytConfig(cluster_mode="application", base_flink_conf_path=str(conf)))
    assert "x 3 slot(s)" in _named(rows, "cluster_mode")[0][1]


def test_validate_fails_on_a_missing_flink_config_file(tmp_path):
    rows = validate_flyt_config(
        FlytConfig(cluster_mode="application", base_flink_conf_path=str(tmp_path / "nope.yaml"))
    )
    assert _named(rows, "flink config.yaml")[0][0] is False
    assert not _named(rows, "cluster_mode")
