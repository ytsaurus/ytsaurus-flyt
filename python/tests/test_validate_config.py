"""flyt validate rows for application mode: the cluster as flyt run resolves it."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import MiniClusterParams, MiniClusterPreset
from ytsaurus_flyt.validate_config import validate_flyt_config


def _named(rows, name):
    return [(ok, msg) for n, ok, msg in rows if n == name]


def test_validate_prints_the_application_cluster_sized_by_the_preset():
    rows = validate_flyt_config(
        FlytConfig(cluster_mode="application", taskmanager_count=2), preset=MiniClusterPreset.LARGE.params
    )
    ((ok, msg),) = _named(rows, "cluster_mode")
    assert ok is True
    assert "2 TaskManager(s) x 4 slot(s) (4 cpu, 15872m JVM each), parallelism.default 8" in msg
    assert _named(rows, "discovery_timeout")


def test_validate_defaults_to_the_micro_preset_and_fails_on_a_too_small_one():
    rows = validate_flyt_config(FlytConfig(cluster_mode="application"))
    assert "x 2 slot(s)" in _named(rows, "cluster_mode")[0][1]
    tiny = MiniClusterParams(cpu=1, memory="512M", max_heap_size="256M", data_size_per_job="1M")
    rows = validate_flyt_config(FlytConfig(cluster_mode="application"), preset=tiny)
    assert _named(rows, "cluster_mode")[0][0] is False


def test_validate_minicluster_has_no_application_rows():
    rows = validate_flyt_config(FlytConfig())
    assert _named(rows, "cluster_mode") == [(True, "minicluster (single job, in-JVM MiniCluster)")]
    assert not _named(rows, "discovery_timeout")
