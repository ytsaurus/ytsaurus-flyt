"""Tests for wheel_utils helpers."""

from ytsaurus_flyt.wheel_utils import dedupe_file_paths_by_basename


def test_dedupe_keeps_first_basename():
    paths = ["//a/foo.whl", "//b/foo.whl", "//c/bar.whl"]
    out = dedupe_file_paths_by_basename(paths)
    assert out == ["//a/foo.whl", "//c/bar.whl"]


def test_dedupe_preserves_order_of_unique_basenames():
    paths = ["//x/a.whl", "//y/b.whl"]
    assert dedupe_file_paths_by_basename(paths) == paths


def test_dedupe_strips_trailing_slash_for_basename_compare():
    paths = ["//tmp/wheel.whl", "//tmp/other/wheel.whl/"]
    out = dedupe_file_paths_by_basename(paths)
    assert len(out) == 1
