"""Tests for launcher helpers."""

from typing import get_type_hints
from unittest.mock import MagicMock

import pytest

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.launcher import _normalize_job_command_for_wheel_sandbox, launch_vanilla_job
from ytsaurus_flyt.models import ClusterParams


def test_normalize_strips_repo_path_to_basename():
    assert _normalize_job_command_for_wheel_sandbox("examples/simple_wordcount/pipeline.py") == "pipeline.py"


def test_normalize_preserves_plain_script():
    assert _normalize_job_command_for_wheel_sandbox("pipeline.py") == "pipeline.py"


def test_normalize_preserves_module_run():
    assert _normalize_job_command_for_wheel_sandbox("-m pipeline") == "-m pipeline"


def test_normalize_extra_args_after_path():
    assert _normalize_job_command_for_wheel_sandbox("pkg/sub/main.py --foo") == "main.py --foo"


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
