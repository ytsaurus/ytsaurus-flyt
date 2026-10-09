"""Tests for launcher helpers."""

from typing import get_type_hints
from unittest.mock import MagicMock

import pytest

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.models import ClusterParams
from ytsaurus_flyt.submit.launcher import launch_vanilla_job


@pytest.fixture
def mock_yt_client():
    return MagicMock()


def test_launch_rejects_squashfs_without_flink_version(mock_yt_client):
    cfg = FlytConfig(flink_version="")
    with pytest.raises(ValueError, match="flink_version"):
        launch_vanilla_job(
            cfg,
            mock_yt_client,
            "p.py",
            "pool",
            wheel_path="/tmp/w.whl",
        )


def test_launch_rejects_squashfs_without_runtime_python_version(mock_yt_client):
    cfg = FlytConfig(flink_version="1.20.1", runtime_python_version="")
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
