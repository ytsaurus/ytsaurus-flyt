"""Tests for wheel_utils helpers."""

import hashlib
from unittest.mock import MagicMock

from ytsaurus_flyt.wheel_utils import (
    dedupe_file_paths_by_basename,
    upload_wheel,
)


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


def test_upload_wheel_cache_key_includes_sha256(tmp_path):
    """Cached path must include content hash so code changes invalidate stale wheels."""
    body = b"wheel-bytes-for-hash"
    wheel = tmp_path / "w.whl"
    wheel.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()

    yt = MagicMock()
    yt.exists.return_value = False

    with upload_wheel(yt, str(wheel), "//home/cache/mysvc") as remote:
        assert remote == f"//home/cache/mysvc/wheels/{digest}/service.whl"

    yt.mkdir.assert_called_once()
    yt.write_file.assert_called_once()


def test_upload_wheel_cache_hit_skips_write(tmp_path):
    wheel = tmp_path / "w.whl"
    wheel.write_bytes(b"x")
    yt = MagicMock()
    yt.exists.return_value = True
    with upload_wheel(yt, str(wheel), "//cache/p") as remote:
        assert "wheels/" in remote
    yt.write_file.assert_not_called()
