"""Tests for container_runtime helpers."""

import sys

import pytest

from ytsaurus_flyt.runtime.container_runtime import (
    cache_owner_run_args,
    python_slim_image,
    run_expect_zero,
)


def test_python_slim_image():
    assert python_slim_image("3.10") == "docker.io/library/python:3.10-slim"
    assert python_slim_image(" 3.11 ") == "docker.io/library/python:3.11-slim"


def test_python_slim_image_rejects_empty():
    with pytest.raises(ValueError, match="non-empty"):
        python_slim_image("")


def test_run_expect_zero_ok():
    run_expect_zero([sys.executable, "-c", "pass"], timeout=30, err_prefix="should not fail")


def test_run_expect_zero_raises():
    with pytest.raises(RuntimeError, match="expected failure"):
        run_expect_zero(
            [sys.executable, "-c", "import sys; sys.exit(2)"],
            timeout=30,
            err_prefix="expected failure",
        )


def test_cache_owner_run_args_runs_as_uid_with_home():
    """Build containers must run as the mount owner (uid) so pip doesn't disable its cache."""
    args = cache_owner_run_args()
    assert args[0] == "--user"
    uid, _, gid = args[1].partition(":")
    assert uid.isdigit() and gid.isdigit()
    assert args[2:] == ["-e", "HOME=/tmp"]


def test_cache_owner_run_args_defaults_to_1000_without_getuid(monkeypatch):
    """On Windows (no os.getuid) the podman machine maps the host user to uid 1000."""
    import ytsaurus_flyt.runtime.container_runtime as cr

    monkeypatch.delattr(cr.os, "getuid", raising=False)
    monkeypatch.delattr(cr.os, "getgid", raising=False)
    assert cache_owner_run_args() == ["--user", "1000:1000", "-e", "HOME=/tmp"]
