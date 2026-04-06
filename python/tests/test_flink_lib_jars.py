"""Tests for Flink lib JAR resolution and SquashFS vs file_paths split."""

from unittest.mock import MagicMock

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_lib_jars import (
    download_flink_lib_jars,
    partition_flink_lib_jars_for_delivery,
    resolve_flink_lib_jars,
)


def test_partition_all_squashfs_when_lists_empty():
    cfg = FlytConfig()
    paths = ["//h/a-1.0.0.jar", "//h/b-2.0.0.jar"]
    sq, fp = partition_flink_lib_jars_for_delivery(cfg, paths)
    assert sq == paths
    assert fp == []


def test_partition_embed_only_list():
    cfg = FlytConfig(embed_squashfs_layer_jar_basenames=["flink-connector-ytsaurus"])
    paths = [
        "//lib/flink-connector-ytsaurus-1.0.0.jar",
        "//lib/my-udf-0.1.0.jar",
    ]
    sq, fp = partition_flink_lib_jars_for_delivery(cfg, paths)
    assert sq == ["//lib/flink-connector-ytsaurus-1.0.0.jar"]
    assert fp == ["//lib/my-udf-0.1.0.jar"]


def test_partition_runtime_only_list():
    cfg = FlytConfig(runtime_jar_basenames=["my-udf"])
    paths = [
        "//lib/flink-connector-ytsaurus-1.0.0.jar",
        "//lib/my-udf-0.1.0.jar",
    ]
    sq, fp = partition_flink_lib_jars_for_delivery(cfg, paths)
    assert fp == ["//lib/my-udf-0.1.0.jar"]
    assert sq == ["//lib/flink-connector-ytsaurus-1.0.0.jar"]


def test_partition_both_lists_disjoint():
    cfg = FlytConfig(
        embed_squashfs_layer_jar_basenames=["flink-connector-ytsaurus"],
        runtime_jar_basenames=["my-udf"],
    )
    paths = [
        "//lib/flink-connector-ytsaurus-1.0.0.jar",
        "//lib/my-udf-0.1.0.jar",
        "//lib/other-1.0.0.jar",
    ]
    sq, fp = partition_flink_lib_jars_for_delivery(cfg, paths)
    assert sq == ["//lib/flink-connector-ytsaurus-1.0.0.jar"]
    assert set(fp) == {
        "//lib/my-udf-0.1.0.jar",
        "//lib/other-1.0.0.jar",
    }


def test_partition_extra_runtime_basenames_when_no_embed_runtime_lists():
    """extra_runtime_basenames always use file_paths; remaining JARs go to the layer."""
    cfg = FlytConfig()
    paths = [
        "//lib/in-layer-1.0.0.jar",
        "//lib/job-only-2.0.0.jar",
    ]
    sq, fp = partition_flink_lib_jars_for_delivery(
        cfg,
        paths,
        extra_runtime_basenames={"job-only"},
    )
    assert sq == ["//lib/in-layer-1.0.0.jar"]
    assert fp == ["//lib/job-only-2.0.0.jar"]


def test_partition_extra_runtime_basenames_override_embed():
    """Programmatic extra basenames use file_paths only, not embedded in SquashFS."""
    cfg = FlytConfig(embed_squashfs_layer_jar_basenames=["flink-connector-ytsaurus"])
    paths = ["//lib/flink-connector-ytsaurus-1.0.0.jar"]
    sq, fp = partition_flink_lib_jars_for_delivery(
        cfg,
        paths,
        extra_runtime_basenames={"flink-connector-ytsaurus"},
    )
    assert sq == []
    assert fp == paths


def test_resolve_includes_only_runtime_jar_basenames():
    """Only runtime_jar_basenames (no embed list) must still resolve from jar_scan_folder."""
    yt = MagicMock()
    yt.list.return_value = [
        "flink-connector-ytsaurus-1.0.0.jar",
        "flink-yson-2.1.0.jar",
    ]
    cfg = FlytConfig(
        jar_scan_folder="//home/flyt/libraries",
        runtime_jar_basenames=["flink-connector-ytsaurus.jar", "flink-yson"],
    )
    result = resolve_flink_lib_jars(yt, cfg)
    assert set(result.yt_paths) == {
        "//home/flyt/libraries/flink-connector-ytsaurus-1.0.0.jar",
        "//home/flyt/libraries/flink-yson-2.1.0.jar",
    }
    assert result.extra_runtime_basenames == frozenset()


def test_resolve_extra_basenames_tracked_for_partition():
    yt = MagicMock()
    yt.list.return_value = [
        "flink-connector-ytsaurus-1.0.0.jar",
        "extra-lib-1.0.0.jar",
    ]
    cfg = FlytConfig(
        jar_scan_folder="//home/flyt/libraries",
        embed_squashfs_layer_jar_basenames=["flink-connector-ytsaurus"],
    )
    result = resolve_flink_lib_jars(yt, cfg, extra_basenames=["extra-lib"])
    assert result.extra_runtime_basenames == frozenset({"extra-lib"})
    sq, fp = partition_flink_lib_jars_for_delivery(
        cfg,
        result.yt_paths,
        extra_runtime_basenames=result.extra_runtime_basenames,
    )
    assert set(sq) == {"//home/flyt/libraries/flink-connector-ytsaurus-1.0.0.jar"}
    assert set(fp) == {"//home/flyt/libraries/extra-lib-1.0.0.jar"}


def test_download_flink_lib_jars_reads_chunked_stream(tmp_path):
    yt = MagicMock()
    yt.read_file.return_value = [b"jar", b"blob"]
    out = download_flink_lib_jars(yt, ["//home/flyt/libraries/foo-1.0.0.jar"], str(tmp_path))
    assert out == [str(tmp_path / "foo-1.0.0.jar")]
    assert (tmp_path / "foo-1.0.0.jar").read_bytes() == b"jarblob"


def test_download_flink_lib_jars_reads_bytes(tmp_path):
    yt = MagicMock()
    yt.read_file.return_value = b"whole"
    out = download_flink_lib_jars(yt, ["//x/p-1.0.0.jar"], str(tmp_path))
    assert (tmp_path / "p-1.0.0.jar").read_bytes() == b"whole"
    assert out == [str(tmp_path / "p-1.0.0.jar")]
