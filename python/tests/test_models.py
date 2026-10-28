"""Tests for ytsaurus_flyt.models."""

import pytest

from ytsaurus_flyt.models import (
    JobmanagerParams,
    MiniClusterParams,
    MiniClusterPreset,
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


class TestMiniClusterParams:
    def test_memory_bytes(self):
        params = MiniClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.memory_bytes() == 4 * 1024 * 1024 * 1024

    def test_max_heap_size_bytes(self):
        params = MiniClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.max_heap_size_bytes() == 2324 * 1024 * 1024

    def test_off_heap_size_bytes_none_by_default(self):
        params = MiniClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.off_heap_size is None
        assert params.off_heap_size_bytes() is None

    def test_off_heap_size_bytes(self):
        params = MiniClusterParams(
            cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M", off_heap_size="8G"
        )
        assert params.off_heap_size_bytes() == 8 * 1024 * 1024 * 1024

    def test_map_operation_fields_keep_their_bytes_helpers(self):
        params = MiniClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        assert params.data_size_per_job_bytes() == 500 * 1024 * 1024
        assert params.disk_request_space_bytes() == 10 * 1024**3
        assert params.disk_request_medium_name == "default"
        assert MiniClusterPreset.MICRO.params.data_size_per_job == "500M"

    def test_frozen(self):
        params = MiniClusterParams(cpu=2, memory="4G", max_heap_size="2324M", data_size_per_job="500M")
        with pytest.raises(AttributeError):
            params.cpu = 4  # type: ignore[misc]


class TestMiniClusterPreset:
    def test_micro_preset(self):
        assert MiniClusterPreset.MICRO.params.cpu == 2
        assert MiniClusterPreset.MICRO.params.memory == "4G"

    def test_small_preset(self):
        assert MiniClusterPreset.SMALL.params.cpu == 2
        assert MiniClusterPreset.SMALL.params.memory == "8G"

    def test_large_preset(self):
        assert MiniClusterPreset.LARGE.params.cpu == 4

    def test_xlarge_preset(self):
        assert MiniClusterPreset.XLARGE.params.cpu == 8


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


class TestTaskmanagerParams:
    def test_defaults(self):
        from ytsaurus_flyt.models import TaskmanagerParams

        tm = TaskmanagerParams(cpu=2, process_memory=parse_memory("4G"), count=1, slots=1)
        assert (tm.cpu, tm.process_memory, tm.count, tm.slots) == (2, parse_memory("4G"), 1, 1)
        assert tm.memory == parse_memory("4G") + parse_memory("512M")  # container = JVM + fixed overhead

    def test_rejects_zero_count_or_slots(self):
        from ytsaurus_flyt.models import TaskmanagerParams

        with pytest.raises(ValueError, match="count"):
            TaskmanagerParams(cpu=2, process_memory=parse_memory("4G"), count=0, slots=1)
        with pytest.raises(ValueError, match="slots"):
            TaskmanagerParams(cpu=2, process_memory=parse_memory("4G"), count=1, slots=0)


class TestJobmanagerParams:
    def test_container_is_jvm_plus_driver_overhead(self):
        jm = JobmanagerParams(cpu=2, process_memory=parse_memory("2G"))
        assert (jm.cpu, jm.process_memory, jm.memory) == (2, parse_memory("2G"), parse_memory("2560M"))
        with pytest.raises(ValueError, match="process_memory"):
            JobmanagerParams(cpu=2, process_memory=0)
