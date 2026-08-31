"""Tests for Flink lib JAR resolution (all JARs ship as file_paths, never in the layer)."""

from unittest.mock import MagicMock

from ytsaurus_flyt.config.config import FlytConfig
from ytsaurus_flyt.runtime.flink_lib_jars import (
    download_flink_lib_jars,
    resolve_flink_lib_jars,
)


def test_resolve_includes_runtime_jar_basenames():
    yt = MagicMock()
    yt.list.return_value = [
        "flink-connector-ytsaurus-1.0.0.jar",
        "flink-yson-2.1.0.jar",
    ]
    cfg = FlytConfig(
        jar_scan_folder="//sys/flink/libraries",
        runtime_jar_basenames=["flink-connector-ytsaurus.jar", "flink-yson"],
    )
    result = resolve_flink_lib_jars(yt, cfg)
    assert set(result.yt_paths) == {
        "//sys/flink/libraries/flink-connector-ytsaurus-1.0.0.jar",
        "//sys/flink/libraries/flink-yson-2.1.0.jar",
    }
    assert result.extra_runtime_basenames == frozenset()


def test_resolve_missing_scan_folder_returns_empty_without_listing():
    """A missing jar_scan_folder warns and resolves nothing instead of raising."""
    yt = MagicMock()
    yt.exists.return_value = False
    cfg = FlytConfig(
        jar_scan_folder="//sys/flink/libraries",
        runtime_jar_basenames=["my-udf"],
    )
    result = resolve_flink_lib_jars(yt, cfg)
    assert result.yt_paths == []
    yt.list.assert_not_called()


def test_resolve_extra_basenames_tracked():
    yt = MagicMock()
    yt.list.return_value = [
        "flink-connector-ytsaurus-1.0.0.jar",
        "extra-lib-1.0.0.jar",
    ]
    cfg = FlytConfig(
        jar_scan_folder="//sys/flink/libraries",
        runtime_jar_basenames=["flink-connector-ytsaurus"],
    )
    result = resolve_flink_lib_jars(yt, cfg, extra_basenames=["extra-lib"])
    assert result.extra_runtime_basenames == frozenset({"extra-lib"})
    assert set(result.yt_paths) == {
        "//sys/flink/libraries/flink-connector-ytsaurus-1.0.0.jar",
        "//sys/flink/libraries/extra-lib-1.0.0.jar",
    }


def test_download_flink_lib_jars_reads_chunked_stream(tmp_path):
    yt = MagicMock()
    yt.read_file.return_value = [b"jar", b"blob"]
    out = download_flink_lib_jars(yt, ["//sys/flink/libraries/foo-1.0.0.jar"], str(tmp_path))
    assert out == [str(tmp_path / "foo-1.0.0.jar")]
    assert (tmp_path / "foo-1.0.0.jar").read_bytes() == b"jarblob"


def test_download_flink_lib_jars_reads_bytes(tmp_path):
    yt = MagicMock()
    yt.read_file.return_value = b"whole"
    out = download_flink_lib_jars(yt, ["//x/p-1.0.0.jar"], str(tmp_path))
    assert (tmp_path / "p-1.0.0.jar").read_bytes() == b"whole"
    assert out == [str(tmp_path / "p-1.0.0.jar")]
