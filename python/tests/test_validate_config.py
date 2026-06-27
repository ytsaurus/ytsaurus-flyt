"""Tests for pre-flight validation (tool checks, no job submission)."""

from unittest import mock

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.config.validate_config import validate_flyt_config


def _row(rows, name):
    return next((r for r in rows if r[0] == name), None)


def test_mksquashfs_ok_via_host_tool():
    cfg = FlytConfig(flink_version="1.20.1", runtime_python_version="3.9")
    with mock.patch("ytsaurus_flyt.config.validate_config.shutil.which", return_value="/usr/bin/mksquashfs"):
        rows = validate_flyt_config(cfg)
    ok, _, msg = _row(rows, "mksquashfs")[0:3]
    assert ok and "found" in msg


def test_mksquashfs_ok_via_container_fallback():
    cfg = FlytConfig(flink_version="1.20.1", runtime_python_version="3.9")
    with (
        mock.patch("ytsaurus_flyt.config.validate_config.shutil.which", return_value=None),
        mock.patch("ytsaurus_flyt.config.validate_config.get_container_runtime_command", return_value=["docker"]),
    ):
        rows = validate_flyt_config(cfg)
    ok, _, msg = _row(rows, "mksquashfs")[0:3]
    assert ok and "container" in msg


def test_mksquashfs_fails_without_host_tool_or_container():
    cfg = FlytConfig(flink_version="1.20.1", runtime_python_version="3.9")
    with (
        mock.patch("ytsaurus_flyt.config.validate_config.shutil.which", return_value=None),
        mock.patch(
            "ytsaurus_flyt.config.validate_config.get_container_runtime_command",
            side_effect=RuntimeError("no runtime"),
        ),
    ):
        rows = validate_flyt_config(cfg)
    assert _row(rows, "mksquashfs")[1] is False


def test_no_host_unzip_check_row():
    cfg = FlytConfig(flink_version="1.20.1", runtime_python_version="3.9")
    rows = validate_flyt_config(cfg)
    assert _row(rows, "unzip") is None
