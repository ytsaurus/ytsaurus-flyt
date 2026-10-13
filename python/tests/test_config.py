"""Tests for ytsaurus_flyt.config."""

import os
import tempfile

import pytest

from ytsaurus_flyt.config.config import FlytConfig, require_squashfs_runtime_config


class TestFlytConfig:
    def test_defaults(self):
        config = FlytConfig()
        assert config.jar_scan_folder == ""
        assert config.runtime_jar_basenames == []
        assert config.java_version == "11"
        assert config.flink_version == "1.20.1"
        assert config.runtime_python_version == ""
        assert config.squashfs_layer_paths == []
        assert config.squashfs_compression == "gzip"
        assert config.squashfs_layer_delivery == "layer_paths"
        assert config.unsquashfs_path == ""
        assert config.network_project is None
        assert config.max_failed_job_count == 10

    def test_construct_with_known_fields(self):
        config = FlytConfig(
            jar_scan_folder="//home/flink/libs",
            service_name="my_service",
        )
        assert config.jar_scan_folder == "//home/flink/libs"
        assert config.service_name == "my_service"

    def test_from_yaml(self):
        yaml_content = """
jar_scan_folder: "//home/flink/libs"
service_name: "my_service"
runtime_jar_basenames:
  - flink-connector-yt
  - flink-yson
flink_version: "1.20.1"
runtime_python_version: "3.10"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config = FlytConfig.from_yaml(f.name)

        os.unlink(f.name)

        assert config.jar_scan_folder == "//home/flink/libs"
        assert config.service_name == "my_service"
        assert config.runtime_jar_basenames == ["flink-connector-yt", "flink-yson"]
        assert config.flink_version == "1.20.1"
        assert config.flink_requirements == ["apache-flink==1.20.1"]
        assert config.runtime_python_version == "3.10"

    def test_extra_environment(self):
        config = FlytConfig(extra_environment={"MY_VAR": "value"})
        assert config.extra_environment == {"MY_VAR": "value"}

    def test_squashfs_delivery_and_unsquashfs_path(self):
        config = FlytConfig(
            squashfs_layer_delivery="sandbox_unpack",
            unsquashfs_path="//sys/flink/unsquashfs",
        )
        assert config.squashfs_layer_delivery == "sandbox_unpack"
        assert config.unsquashfs_path == "//sys/flink/unsquashfs"

    def test_invalid_squashfs_delivery(self):
        with pytest.raises(ValueError, match="squashfs_layer_delivery"):
            FlytConfig(squashfs_layer_delivery="layer_path")

    def test_invalid_squashfs_compression(self):
        with pytest.raises(ValueError, match="squashfs_compression"):
            FlytConfig(squashfs_compression="brotli")

    def test_normalize_runtime_jar_list_from_string(self):
        cfg = FlytConfig(runtime_jar_basenames="flink-connector-yt")  # type: ignore[arg-type]
        assert cfg.runtime_jar_basenames == ["flink-connector-yt"]

    def test_to_dict_roundtrip_fields(self):
        cfg = FlytConfig(service_name="s", max_failed_job_count=3)
        d = cfg.to_dict()
        assert d["service_name"] == "s"
        assert d["max_failed_job_count"] == 3
        assert "flink_version" in d

    def test_from_yaml_warns_unknown_keys(self):
        yaml_content = """
unknown_key: 1
jar_scan_folder: "//x"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            path = f.name
        try:
            with pytest.warns(UserWarning, match="unknown"):
                c = FlytConfig.from_yaml(path)
            assert c.jar_scan_folder == "//x"
        finally:
            os.unlink(path)

    def test_from_yaml_squashfs_fields(self):
        yaml_content = """
squashfs_layer_delivery: sandbox_unpack
squashfs_layer_paths:
  - "//sys/flink/runtime.squashfs"
unsquashfs_path: "//sys/flink/unsquashfs"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config = FlytConfig.from_yaml(f.name)
        os.unlink(f.name)
        assert config.squashfs_layer_delivery == "sandbox_unpack"
        assert config.squashfs_layer_paths == ["//sys/flink/runtime.squashfs"]
        assert config.unsquashfs_path == "//sys/flink/unsquashfs"


def test_require_squashfs_runtime_config_ok():
    cfg = FlytConfig(
        flink_version="1.20.1",
        runtime_python_version="3.10",
    )
    require_squashfs_runtime_config(cfg)


def test_require_squashfs_runtime_config_missing_flink_version():
    with pytest.raises(ValueError, match="flink_version"):
        require_squashfs_runtime_config(FlytConfig(flink_version=""))


def test_require_squashfs_runtime_config_missing_version():
    with pytest.raises(ValueError, match="runtime_python_version"):
        require_squashfs_runtime_config(FlytConfig(flink_version="1.20.1", runtime_python_version=""))
