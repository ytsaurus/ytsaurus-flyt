"""Tests for launcher helpers."""

import contextlib
from unittest.mock import MagicMock

import pytest

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import FlinkConfigError
from ytsaurus_flyt.launcher import launch_vanilla_job


@pytest.fixture
def mock_yt_client():
    return MagicMock()


def test_launch_rejects_squashfs_without_runtime_packages(mock_yt_client):
    cfg = FlytConfig(runtime_python_packages=[])
    with pytest.raises(ValueError, match="runtime_python_packages"):
        launch_vanilla_job(
            cfg,
            mock_yt_client,
            "p.py",
            "pool",
            wheel_path="/tmp/w.whl",
        )


def test_launch_rejects_squashfs_without_runtime_python_version(mock_yt_client):
    cfg = FlytConfig(runtime_python_packages=["apache-flink==1.20.1"], runtime_python_version="")
    with pytest.raises(ValueError, match="runtime_python_version"):
        launch_vanilla_job(
            cfg,
            mock_yt_client,
            "p.py",
            "pool",
            wheel_path="/tmp/w.whl",
        )


def test_launch_passes_the_cluster_and_the_client_to_the_spec(monkeypatch):
    from ytsaurus_flyt import launcher as launcher_module

    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        raise RuntimeError("stop here")

    monkeypatch.setattr(launcher_module, "proxy_url_from_client", lambda _c: "http://proxy")
    monkeypatch.setattr(launcher_module, "require_squashfs_runtime_config", lambda _c: None)
    monkeypatch.setattr(launcher_module, "get_secure_credentials", lambda *a, **k: {})
    monkeypatch.setattr(launcher_module, "upload_service_wheel", lambda *a, **k: contextlib.nullcontext("//tmp/w.whl"))
    monkeypatch.setattr(
        launcher_module, "resolve_flink_lib_jars", lambda *a, **k: MagicMock(yt_paths=[], extra_runtime_basenames=[])
    )
    monkeypatch.setattr(launcher_module, "partition_flink_lib_jars_for_delivery", lambda *a, **k: ([], []))
    monkeypatch.setattr(launcher_module, "ensure_runtime_layer", lambda *a, **k: "//home/l.squashfs")
    monkeypatch.setattr(launcher_module, "ensure_unsquashfs_tool_remote", lambda *a, **k: "//home/unsquashfs")
    monkeypatch.setattr(launcher_module, "build_vanilla_operation_spec", fake_build)
    client = MagicMock()
    cfg = FlytConfig(
        service_name="svc",
        cluster_mode="application",
        squashfs_layer_delivery="sandbox_unpack",
        flink_config_override={"flyt.taskmanager.cpu.cores": "4"},
    )
    with pytest.raises(RuntimeError, match="stop here"):
        launch_vanilla_job(cfg, client, "pipeline.py", "pool", wheel_path="w.whl")
    # the cluster is resolved once, before anything is built; the client resolves the jobs' proxy
    assert captured["cluster"].resources.taskmanager_size.cpu == 4
    assert captured["yt_client"] is client


def test_launch_without_a_service_wheel_uses_only_operation_files(monkeypatch):
    from ytsaurus_flyt import launcher as launcher_module

    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        raise RuntimeError("stop here")

    def no_upload(*a, **k):
        raise AssertionError("no wheel to upload")

    monkeypatch.setattr(launcher_module, "proxy_url_from_client", lambda _c: "http://proxy")
    monkeypatch.setattr(launcher_module, "require_squashfs_runtime_config", lambda _c: None)
    monkeypatch.setattr(launcher_module, "get_secure_credentials", lambda *a, **k: {})
    monkeypatch.setattr(launcher_module, "upload_service_wheel", no_upload)
    monkeypatch.setattr(
        launcher_module, "resolve_flink_lib_jars", lambda *a, **k: MagicMock(yt_paths=[], extra_runtime_basenames=[])
    )
    monkeypatch.setattr(launcher_module, "partition_flink_lib_jars_for_delivery", lambda *a, **k: ([], []))
    monkeypatch.setattr(launcher_module, "ensure_runtime_layer", lambda *a, **k: "//home/l.squashfs")
    monkeypatch.setattr(launcher_module, "build_vanilla_operation_spec", fake_build)
    cfg = FlytConfig(service_name="svc", extra_file_paths=["//tmp/s/job.py"])
    with pytest.raises(RuntimeError, match="stop here"):
        launch_vanilla_job(cfg, MagicMock(), "../job.py", "pool")
    assert captured["operation_params"].file_paths == ["//tmp/s/job.py"]


def test_launch_reports_an_invalid_flink_configuration_before_building(monkeypatch):
    from ytsaurus_flyt import launcher as launcher_module

    monkeypatch.setattr(launcher_module, "proxy_url_from_client", lambda _c: "http://proxy")
    monkeypatch.setattr(launcher_module, "require_squashfs_runtime_config", lambda _c: None)
    monkeypatch.setattr(launcher_module, "get_secure_credentials", lambda *a, **k: pytest.fail("went on"))
    cfg = FlytConfig(service_name="svc", flink_config_override={"taskmanager.memory.process.size": "lots"})
    with pytest.raises(FlinkConfigError, match="not a memory size"):
        launch_vanilla_job(cfg, MagicMock(), "pipeline.py", "pool", wheel_path="w.whl")
    # the JobManager's size is parsed only in application mode
    override = {"jobmanager.memory.process.size": "abc"}
    cfg = FlytConfig(service_name="svc", cluster_mode="application", flink_config_override=override)
    with pytest.raises(FlinkConfigError, match="not a memory size"):
        launch_vanilla_job(cfg, MagicMock(), "pipeline.py", "pool", wheel_path="w.whl")


def test_failed_job_budget_scales_with_taskmanagers():
    from ytsaurus_flyt.launcher import _make_operation_params
    from ytsaurus_flyt.spec import resolve_flink_cluster

    def budget(replicas=None, **kw):
        config = FlytConfig(
            **kw, flink_config_override={"flyt.taskmanager.replicas": str(replicas)} if replicas else {}
        )
        return _make_operation_params(config, "pool", resolve_flink_cluster(config)).max_failed_job_count

    assert budget(4) == 10  # minicluster
    assert budget(cluster_mode="application") == 10
    assert budget(2, cluster_mode="application") == 10
    assert budget(4, cluster_mode="application") == 20
    assert budget(10, cluster_mode="application") == 50
    # an explicit value is the operation's limit as is, in both modes
    assert budget(10, cluster_mode="application", max_failed_job_count=3) == 3
    assert budget(max_failed_job_count=3) == 3


def test_invalid_max_failed_job_count():
    with pytest.raises(ValueError, match="max_failed_job_count"):
        FlytConfig(max_failed_job_count=0)
