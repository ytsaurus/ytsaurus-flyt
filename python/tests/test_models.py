"""Tests for ytsaurus_flyt.models."""

import pytest

from ytsaurus_flyt.models import (
    ClusterParams,
    ClusterPreset,
    JobmanagerParams,
    OperationParams,
    parse_memory,
)


class TestParseMemory:
    def test_integer_passthrough(self):
        assert parse_memory(1024) == 1024

    def test_gigabytes(self):
        assert parse_memory("4G") == 4 * 1024 * 1024 * 1024

    def test_megabytes(self):
        assert parse_memory("512M") == 512 * 1024 * 1024

    def test_kilobytes(self):
        assert parse_memory("100K") == 100 * 1024

    def test_bytes_explicit(self):
        assert parse_memory("1024B") == 1024

    def test_bytes_lowercase_b(self):
        assert parse_memory("4b") == 4

    def test_case_insensitive(self):
        assert parse_memory("4g") == parse_memory("4G")
        assert parse_memory("512m") == parse_memory("512M")

    def test_invalid_unit(self):
        with pytest.raises(ValueError, match="Unknown memory unit"):
            parse_memory("4X")

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_memory("abc")


class TestClusterParams:
    def test_memory_bytes(self):
        params = ClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.memory_bytes() == 4 * 1024 * 1024 * 1024

    def test_max_heap_size_bytes(self):
        params = ClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.max_heap_size_bytes() == 2324 * 1024 * 1024

    def test_frozen(self):
        params = ClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        with pytest.raises(AttributeError):
            params.cpu = 4  # type: ignore[misc]


class TestClusterPreset:
    def test_micro_preset(self):
        assert ClusterPreset.MICRO.params.cpu == 2
        assert ClusterPreset.MICRO.params.memory == "4G"

    def test_small_preset(self):
        assert ClusterPreset.SMALL.params.cpu == 2
        assert ClusterPreset.SMALL.params.memory == "8G"

    def test_large_preset(self):
        assert ClusterPreset.LARGE.params.cpu == 4

    def test_xlarge_preset(self):
        assert ClusterPreset.XLARGE.params.cpu == 8


class TestOperationParams:
    def test_defaults(self):
        params = OperationParams()
        assert params.pool is None
        assert params.file_paths == []
        assert params.layer_paths == []

    def test_custom_values(self):
        params = OperationParams(pool="my-pool", file_paths=["//path/to/jar"])
        assert params.pool == "my-pool"
        assert params.file_paths == ["//path/to/jar"]


class TestJobmanagerParams:
    def test_defaults(self):
        params = JobmanagerParams()
        assert params.cpu == 2
        assert params.memory == parse_memory("4G")
