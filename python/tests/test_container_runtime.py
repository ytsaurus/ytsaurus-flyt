"""Tests for container_runtime helpers."""

import sys

import pytest

from ytsaurus_flyt.container_runtime import python_slim_image, run_expect_zero


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
