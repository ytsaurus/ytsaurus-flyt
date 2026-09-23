"""Tests for ytsaurus_flyt.config."""

import os
import tempfile

import pytest

from ytsaurus_flyt.config import DEFAULT_JAVA_HOME, FlytConfig, require_squashfs_runtime_config


class TestFlytConfig:
    def test_defaults(self):
        config = FlytConfig()
        assert config.jar_scan_folder == ""
        assert config.embed_squashfs_layer_jar_basenames == []
        assert config.runtime_jar_basenames == []
        assert config.java_home == DEFAULT_JAVA_HOME
        assert config.python_bin == "/usr/bin/python3"
        assert config.runtime_python_packages == []
        assert config.runtime_python_version == ""
        assert config.squashfs_layer_cache_prefix == ""
        assert config.squashfs_compression == "gzip"
        assert config.squashfs_layer_delivery == "layer_paths"
        assert config.squashfs_tools_cache_prefix == ""
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
embed_squashfs_layer_jar_basenames:
  - flink-connector-yt
  - flink-yson
runtime_python_packages:
  - "apache-flink==1.20.1"
runtime_python_version: "3.10"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config = FlytConfig.from_yaml(f.name)

        os.unlink(f.name)

        assert config.jar_scan_folder == "//home/flink/libs"
        assert config.service_name == "my_service"
        assert config.embed_squashfs_layer_jar_basenames == ["flink-connector-yt", "flink-yson"]
        assert config.runtime_python_packages == ["apache-flink==1.20.1"]
        assert config.runtime_python_version == "3.10"

    def test_extra_environment(self):
        config = FlytConfig(extra_environment={"MY_VAR": "value"})
        assert config.extra_environment == {"MY_VAR": "value"}

    def test_squashfs_delivery_and_tools_prefix(self):
        config = FlytConfig(
            squashfs_layer_delivery="sandbox_unpack",
            squashfs_tools_cache_prefix="//home/flyt/tools",
        )
        assert config.squashfs_layer_delivery == "sandbox_unpack"
        assert config.squashfs_tools_cache_prefix == "//home/flyt/tools"

    def test_invalid_squashfs_delivery(self):
        with pytest.raises(ValueError, match="squashfs_layer_delivery"):
            FlytConfig(squashfs_layer_delivery="layer_path")

    def test_invalid_squashfs_compression(self):
        with pytest.raises(ValueError, match="squashfs_compression"):
            FlytConfig(squashfs_compression="brotli")

    def test_overlapping_embed_and_runtime_jar_basenames(self):
        with pytest.raises(ValueError, match="embed_squashfs_layer_jar_basenames"):
            FlytConfig(
                embed_squashfs_layer_jar_basenames=["same"],
                runtime_jar_basenames=["same"],
            )

    def test_normalize_embed_jar_list_from_string(self):
        cfg = FlytConfig(embed_squashfs_layer_jar_basenames="flink-connector-yt")  # type: ignore[arg-type]
        assert cfg.embed_squashfs_layer_jar_basenames == ["flink-connector-yt"]

    def test_to_dict_roundtrip_fields(self):
        cfg = FlytConfig(service_name="s", max_failed_job_count=3)
        d = cfg.to_dict()
        assert d["service_name"] == "s"
        assert d["max_failed_job_count"] == 3
        assert "runtime_python_packages" in d

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
squashfs_layer_delivery: layer_paths
squashfs_tools_cache_prefix: "//tmp/tools"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config = FlytConfig.from_yaml(f.name)
        os.unlink(f.name)
        assert config.squashfs_layer_delivery == "layer_paths"
        assert config.squashfs_tools_cache_prefix == "//tmp/tools"


def test_require_squashfs_runtime_config_ok():
    cfg = FlytConfig(
        runtime_python_packages=["apache-flink==1.20.1"],
        runtime_python_version="3.10",
    )
    require_squashfs_runtime_config(cfg)


def test_require_squashfs_runtime_config_missing_packages():
    with pytest.raises(ValueError, match="runtime_python_packages"):
        require_squashfs_runtime_config(FlytConfig(runtime_python_packages=[]))


def test_require_squashfs_runtime_config_missing_version():
    with pytest.raises(ValueError, match="runtime_python_version"):
        require_squashfs_runtime_config(
            FlytConfig(runtime_python_packages=["apache-flink==1.20.1"], runtime_python_version="")
        )


class TestClusterModeConfig:
    def test_defaults_are_minicluster(self):
        config = FlytConfig()
        assert config.cluster_mode == "minicluster"
        assert config.is_application_cluster is False
        assert config.taskmanager_count is None
        assert config.parallelism is None
        assert config.effective_taskmanager_count == 1
        assert config.effective_parallelism == 1
        assert config.taskmanager_preset == ""
        assert config.taskmanager_slots == 1
        assert config.restart_completed_jobs is True
        assert config.discovery_timeout == 600
        assert config.flink_config == {}

    def test_application_mode_normalizes(self):
        config = FlytConfig(cluster_mode=" Application ", taskmanager_preset="Small", flink_config={"a.b": 1})
        assert config.cluster_mode == "application"
        assert config.is_application_cluster is True
        assert config.taskmanager_preset == "small"
        assert config.flink_config == {"a.b": "1"}

    def test_invalid_cluster_mode(self):
        with pytest.raises(ValueError, match="cluster_mode"):
            FlytConfig(cluster_mode="session")

    def test_invalid_taskmanager_count_and_slots(self):
        with pytest.raises(ValueError, match="taskmanager_count"):
            FlytConfig(taskmanager_count=0)
        with pytest.raises(ValueError, match="taskmanager_slots"):
            FlytConfig(taskmanager_slots=0)

    def test_invalid_taskmanager_preset(self):
        with pytest.raises(ValueError, match="taskmanager_preset"):
            FlytConfig(taskmanager_preset="huge")

    def test_from_yaml_application_fields(self):
        yaml_content = """
cluster_mode: application
taskmanager_count: 3
taskmanager_preset: large
taskmanager_slots: 2
restart_completed_jobs: false
discovery_timeout: 120
flink_config:
  restart-strategy.type: fixed-delay
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config = FlytConfig.from_yaml(f.name)
        os.unlink(f.name)
        assert config.is_application_cluster
        assert config.taskmanager_count == 3
        assert config.taskmanager_preset == "large"
        assert config.taskmanager_slots == 2
        assert config.restart_completed_jobs is False
        assert config.discovery_timeout == 120
        assert config.flink_config == {"restart-strategy.type": "fixed-delay"}

    def test_taskmanager_count_derived_from_parallelism(self):
        cfg = FlytConfig(parallelism=8, taskmanager_slots=3)
        assert cfg.effective_taskmanager_count == 3  # ceil(8 / 3)
        assert cfg.effective_parallelism == 8
        explicit = FlytConfig(parallelism=4, taskmanager_count=4, taskmanager_slots=2)
        assert explicit.effective_taskmanager_count == 4
        assert explicit.effective_parallelism == 4
        no_parallelism = FlytConfig(taskmanager_count=2, taskmanager_slots=2)
        assert no_parallelism.effective_parallelism == 4

    def test_parallelism_must_fit_into_explicit_cluster(self):
        with pytest.raises(ValueError, match="does not fit"):
            FlytConfig(parallelism=5, taskmanager_count=2, taskmanager_slots=2)
        with pytest.raises(ValueError, match="parallelism"):
            FlytConfig(parallelism=0)

    def test_sidecar_command_is_kept_in_any_mode(self):
        assert FlytConfig(cluster_mode="application", sidecar_command=" agent ").sidecar_command == "agent"
        # a profile may carry it while --mode picks the topology per run; minicluster just ignores it
        assert FlytConfig(sidecar_command="agent").sidecar_command == "agent"

    def test_invalid_discovery_timeout(self):
        with pytest.raises(ValueError, match="discovery_timeout"):
            FlytConfig(discovery_timeout=0)
