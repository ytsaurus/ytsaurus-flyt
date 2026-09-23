"""Tests for launcher helpers."""

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


def test_make_taskmanager_params_from_config():
    from ytsaurus_flyt.launcher import taskmanager_params_from_config as _make_taskmanager_params
    from ytsaurus_flyt.models import ClusterPreset

    micro = ClusterPreset.MICRO.params
    same = _make_taskmanager_params(FlytConfig(taskmanager_count=3, taskmanager_slots=2), micro)
    assert (same.cpu, same.memory, same.count, same.slots) == (micro.cpu, micro.memory_bytes(), 3, 2)
    derived = _make_taskmanager_params(FlytConfig(parallelism=7, taskmanager_slots=2), micro)
    assert (derived.count, derived.slots) == (4, 2)
    large = _make_taskmanager_params(FlytConfig(taskmanager_preset="large"), micro)
    assert (large.cpu, large.memory) == (ClusterPreset.LARGE.params.cpu, ClusterPreset.LARGE.params.memory_bytes())
