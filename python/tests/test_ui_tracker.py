"""Tests for synchronous Flink UI discovery helpers."""

from unittest.mock import MagicMock, patch

from ytsaurus_flyt.ui_tracker import (
    FLINK_UI_PORT,
    find_ui_url_for_operation,
    wait_ui_url_for_operation,
)


def _yt_with_job(ipv6: str = "2a02:6b8::1") -> MagicMock:
    yt = MagicMock()
    yt.list_jobs.return_value = {"jobs": [{"id": "j1", "state": "running"}]}
    yt.get_job.return_value = {"exec_attributes": {"ip_addresses": [ipv6]}}
    return yt


def test_find_ui_url_returns_none_without_jobs() -> None:
    yt = MagicMock()
    yt.list_jobs.return_value = {"jobs": []}
    assert find_ui_url_for_operation(yt, "op-1") is None


def test_find_ui_url_probes_port() -> None:
    yt = _yt_with_job()
    with patch("ytsaurus_flyt.ui_tracker._probe_tcp", return_value=True) as probe:
        url = find_ui_url_for_operation(yt, "op-1")
    assert url == f"http://[2a02:6b8::1]:{FLINK_UI_PORT}"
    probe.assert_called_once_with("2a02:6b8::1", FLINK_UI_PORT)


def test_find_ui_url_none_when_port_closed() -> None:
    yt = _yt_with_job()
    with patch("ytsaurus_flyt.ui_tracker._probe_tcp", return_value=False):
        assert find_ui_url_for_operation(yt, "op-1") is None


def test_find_ui_url_no_probe() -> None:
    yt = _yt_with_job()
    url = find_ui_url_for_operation(yt, "op-1", probe=False)
    assert url == f"http://[2a02:6b8::1]:{FLINK_UI_PORT}"


def test_wait_ui_url_returns_when_up() -> None:
    yt = _yt_with_job()
    yt.get_operation_state.return_value = "running"
    with patch("ytsaurus_flyt.ui_tracker._probe_tcp", return_value=True):
        url, state = wait_ui_url_for_operation(yt, "op-1")
    assert url == f"http://[2a02:6b8::1]:{FLINK_UI_PORT}"
    assert state == "running"


def test_wait_ui_url_stops_on_terminal_state() -> None:
    yt = MagicMock()
    yt.list_jobs.return_value = {"jobs": []}
    yt.get_operation_state.return_value = "failed"
    url, state = wait_ui_url_for_operation(yt, "op-1")
    assert url is None
    assert state == "failed"


def test_wait_ui_url_polls_until_up() -> None:
    yt = _yt_with_job()
    yt.get_operation_state.return_value = "running"
    probes = iter([False, True])
    with patch("ytsaurus_flyt.ui_tracker._probe_tcp", side_effect=lambda *_: next(probes)):
        url, state = wait_ui_url_for_operation(yt, "op-1", poll_interval_s=0.01)
    assert url == f"http://[2a02:6b8::1]:{FLINK_UI_PORT}"
    assert state == "running"
