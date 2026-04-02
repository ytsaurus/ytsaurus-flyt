"""Tests for SquashFS layer hashing and helpers."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.layer_builder import compute_layer_hash


def test_compute_layer_hash_stable():
    c1 = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.1"],
        runtime_python_version="3.10",
        squashfs_compression="gzip",
    )
    c2 = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.1"],
        runtime_python_version="3.10",
        squashfs_compression="gzip",
    )
    h1 = compute_layer_hash(c1, ["//x/a.jar"])
    h2 = compute_layer_hash(c2, ["//x/a.jar"])
    assert h1 == h2
    assert len(h1) == 16


def test_compute_layer_hash_order_independent():
    c = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.1"],
        squashfs_compression="gzip",
    )
    h1 = compute_layer_hash(c, ["//a.jar", "//b.jar"])
    h2 = compute_layer_hash(c, ["//b.jar", "//a.jar"])
    assert h1 == h2


def test_compute_layer_hash_changes_when_config_changes():
    c1 = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.1"],
        squashfs_compression="gzip",
    )
    c2 = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.2"],
        squashfs_compression="gzip",
    )
    assert compute_layer_hash(c1, []) != compute_layer_hash(c2, [])
