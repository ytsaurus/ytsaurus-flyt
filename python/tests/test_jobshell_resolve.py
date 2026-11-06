"""Tests for default jobshell argv resolution."""

from unittest.mock import MagicMock

from ytsaurus_flyt.jobshell_resolve import (
    flyt_profile_marker,
    operation_title_for_profile,
    resolve_default_jobshell_argv,
    runs_driver,
)


def test_operation_title_includes_profile_marker():
    assert operation_title_for_profile("pipeline.py", "kind-dev") == ("FLYT [flyt-profile:kind-dev] pipeline.py")
    assert operation_title_for_profile("pipeline.py", None) == "FLYT: pipeline.py"


def test_profile_title_tag_sanitizes():
    assert "[flyt-profile:my_prof]" in operation_title_for_profile("x.py", "my prof")


def test_resolve_returns_none_when_no_operations():
    yt = MagicMock()
    yt.list_operations.return_value = {"operations": []}
    assert resolve_default_jobshell_argv(yt, "kind") is None
    yt.list_operations.assert_called_once()
    call_kw = yt.list_operations.call_args
    assert call_kw[1]["filter"] == flyt_profile_marker("kind")


def test_resolve_prefers_jobmanager_task():
    yt = MagicMock()
    yt.list_operations.return_value = {
        "operations": [{"id": "op-1", "start_time": "2025-01-01T00:00:00Z"}],
    }
    yt.list_jobs.return_value = {
        "jobs": [
            {"id": "j-tm", "state": "running", "task_name": "taskmanager"},
            {"id": "j-jm", "state": "running", "task_name": "jobmanager"},
        ],
    }
    out = resolve_default_jobshell_argv(yt, "kind")
    assert out == ["run-job-shell", "j-jm"]


def test_resolve_first_running_job():
    yt = MagicMock()
    yt.list_operations.return_value = {
        "operations": [{"id": "op-1", "start_time": "2025-01-01T00:00:00Z"}],
    }
    yt.list_jobs.return_value = {
        "jobs": [
            {"id": "j1", "state": "running", "task_name": "flink"},
        ],
    }
    assert resolve_default_jobshell_argv(yt, "kind") == ["run-job-shell", "j1"]


def test_resolve_skips_op_without_job():
    yt = MagicMock()
    yt.list_operations.return_value = {
        "operations": [
            {"id": "op-empty", "start_time": "2025-01-02T00:00:00Z"},
            {"id": "op-ok", "start_time": "2025-01-01T00:00:00Z"},
        ],
    }

    def list_jobs_side_effect(op_id, **kwargs):
        if op_id == "op-empty":
            return {"jobs": []}
        return {"jobs": [{"id": "j1", "state": "running", "task_name": "flink"}]}

    yt.list_jobs.side_effect = list_jobs_side_effect
    assert resolve_default_jobshell_argv(yt, "kind") == ["run-job-shell", "j1"]


def test_runs_driver_is_every_job_but_a_taskmanager():
    assert runs_driver({"task_name": "jobmanager"})
    assert runs_driver({"task_name": "flink_minicluster"})
    assert runs_driver({"task_name": "flink"})  # operations from older flyt
    assert not runs_driver({"task_name": "TaskManager"})
