"""Tests for SquashFS layer build helpers."""

from unittest import mock

from ytsaurus_flyt.runtime.layer_builder import (
    _copy_jre_from_temurin,
    _install_python_via_uv,
    _run_mksquashfs,
    upload_local_file,
)


def test_install_python_via_uv_runs_uv_image(tmp_path):
    captured = {}
    with (
        mock.patch("ytsaurus_flyt.runtime.layer_builder.get_container_runtime_command", return_value=["docker"]),
        mock.patch(
            "ytsaurus_flyt.runtime.layer_builder.run_expect_zero", side_effect=lambda cmd, **k: captured.update(cmd=cmd)
        ),
        mock.patch("os.path.isfile", return_value=True),
    ):
        _install_python_via_uv("3.10", str(tmp_path / "python-dist"))
    cmd = " ".join(captured["cmd"])
    assert "ghcr.io/astral-sh/uv:bookworm-slim" in cmd  # glibc, has a shell (not the distroless :latest)
    assert "uv python install" in cmd and "3.10" in cmd
    assert "UV_CACHE_DIR=/uvcache" in cmd  # cache mounted from host


def test_copy_jre_from_temurin_runs_temurin_image(tmp_path):
    captured = {}
    with (
        mock.patch("ytsaurus_flyt.runtime.layer_builder.get_container_runtime_command", return_value=["docker"]),
        mock.patch(
            "ytsaurus_flyt.runtime.layer_builder.run_expect_zero", side_effect=lambda cmd, **k: captured.update(cmd=cmd)
        ),
        mock.patch("os.path.isfile", return_value=True),
    ):
        _copy_jre_from_temurin("11", str(tmp_path / "java"))
    cmd = " ".join(captured["cmd"])
    assert "eclipse-temurin:11-jre" in cmd
    assert "/opt/java/openjdk" in cmd


def test_run_mksquashfs_uses_host_when_available():
    with (
        mock.patch("ytsaurus_flyt.runtime.layer_builder.shutil.which", return_value="/usr/bin/mksquashfs"),
        mock.patch("ytsaurus_flyt.runtime.layer_builder._run_mksquashfs_host") as host,
        mock.patch("ytsaurus_flyt.runtime.layer_builder._run_mksquashfs_container") as container,
    ):
        _run_mksquashfs("/root", "/out/runtime.squashfs", "GZIP")
    host.assert_called_once_with("/root", "/out/runtime.squashfs", "gzip")
    container.assert_not_called()


def test_run_mksquashfs_falls_back_to_container_without_host_tool():
    with (
        mock.patch("ytsaurus_flyt.runtime.layer_builder.shutil.which", return_value=None),
        mock.patch("ytsaurus_flyt.runtime.layer_builder._run_mksquashfs_host") as host,
        mock.patch("ytsaurus_flyt.runtime.layer_builder._run_mksquashfs_container") as container,
    ):
        _run_mksquashfs("/root", "/out/runtime.squashfs", "zstd")
    container.assert_called_once_with("/root", "/out/runtime.squashfs", "zstd")
    host.assert_not_called()


def test_upload_local_file_creates_parent_and_writes(tmp_path):
    local = tmp_path / "artifact.bin"
    local.write_bytes(b"data")
    yt = mock.MagicMock()
    yt.exists.return_value = False
    upload_local_file(yt, str(local), "//sys/flink/artifact.bin")
    yt.mkdir.assert_called_once_with("//sys/flink", recursive=True)
    assert yt.write_file.call_args[0][0] == "//sys/flink/artifact.bin"
