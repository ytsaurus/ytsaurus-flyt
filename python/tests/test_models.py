"""Tests for ytsaurus_flyt.models."""

import pytest

from ytsaurus_flyt.models import OperationParams, parse_memory


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
