"""Tests for launcher helpers."""

import contextlib
from typing import get_type_hints
from unittest.mock import MagicMock

import pytest

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.launcher import launch_vanilla_job
from ytsaurus_flyt.models import ClusterParams


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


def test_launch_vanilla_job_accepts_cluster_params_type():
    hints = get_type_hints(launch_vanilla_job)
    assert ClusterParams in hints["preset"].__args__


def test_launch_passes_the_preset_and_the_client_to_the_spec(monkeypatch):
    from ytsaurus_flyt import launcher as launcher_module
    from ytsaurus_flyt.models import ClusterPreset

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
    cfg = FlytConfig(service_name="svc", cluster_mode="application", squashfs_layer_delivery="sandbox_unpack")
    with pytest.raises(RuntimeError, match="stop here"):
        launch_vanilla_job(cfg, client, "pipeline.py", "pool", preset=ClusterPreset.LARGE, wheel_path="w.whl")
    # the spec builder sizes the cluster and resolves the jobs' proxy from these two
    assert captured["preset_params"] == ClusterPreset.LARGE.params
    assert captured["yt_client"] is client


def test_failed_job_budget_scales_with_taskmanagers():
    from ytsaurus_flyt.launcher import _make_operation_params

    def budget(**kw):
        return _make_operation_params(FlytConfig(**kw), "pool").max_failed_job_count

    assert budget(taskmanager_count=4) == 10  # minicluster
    assert budget(cluster_mode="application", taskmanager_count=1) == 10
    assert budget(cluster_mode="application", taskmanager_count=2) == 10
    assert budget(cluster_mode="application", taskmanager_count=4) == 20
    assert budget(cluster_mode="application", taskmanager_count=10) == 50
    # an explicit value is the operation's limit as is, in both modes
    assert budget(cluster_mode="application", taskmanager_count=10, max_failed_job_count=3) == 3
    assert budget(max_failed_job_count=3) == 3


def test_invalid_max_failed_job_count():
    with pytest.raises(ValueError, match="max_failed_job_count"):
        FlytConfig(max_failed_job_count=0)
