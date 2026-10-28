"""Tests for ytsaurus_flyt.flink_conf: the Flink configuration of both cluster modes."""

from unittest.mock import MagicMock

import pytest
import yaml

from ytsaurus_flyt.application import resolve_application_resources
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    cluster_flink_config,
    flatten_flink_config,
    flink_config_warnings,
    load_flink_conf_file,
    render_flink_config_yaml,
    rest_port,
    user_flink_config,
)


def _app_config(**overrides) -> FlytConfig:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return FlytConfig(**base)


def test_flink_user_config_merges_the_file_under_flink_config():
    cfg = FlytConfig(flink_config_override={"custom.key": "from-config", "taskmanager.numberOfTaskSlots": 3})
    merged = user_flink_config(cfg, {"custom.key": "from-file", "file.only": "x", "taskmanager.numberOfTaskSlots": "4"})
    assert merged == {"custom.key": "from-config", "file.only": "x", "taskmanager.numberOfTaskSlots": "3"}
    assert user_flink_config(FlytConfig()) == {}


def testflatten_flink_config_handles_nested_mappings_and_scalars():
    flat = flatten_flink_config(
        {
            "taskmanager": {"memory": {"process": {"size": "4g"}}, "numberOfTaskSlots": 3},
            "state.backend": {"incremental": True},
            "pipeline.name": None,
        }
    )
    assert flat == {
        "taskmanager.memory.process.size": "4g",
        "taskmanager.numberOfTaskSlots": "3",
        "state.backend.incremental": "true",
        "pipeline.name": "",
    }


def test_load_flink_conf_file_reads_local_nested_yaml(tmp_path):
    conf = tmp_path / "config.yaml"
    conf.write_text("taskmanager:\n  numberOfTaskSlots: 4\nstate.backend.type: hashmap\n")
    assert load_flink_conf_file(MagicMock(), str(conf)) == {
        "taskmanager.numberOfTaskSlots": "4",
        "state.backend.type": "hashmap",
    }


def test_load_flink_conf_file_reads_cypress_file():
    client = MagicMock()
    client.read_file.return_value.read.return_value = b"rest.port: 8081\n"
    assert load_flink_conf_file(client, "//home/flyt/config.yaml") == {"rest.port": "8081"}
    client.read_file.assert_called_once_with("//home/flyt/config.yaml")


def test_load_flink_conf_file_rejects_non_mapping(tmp_path):
    conf = tmp_path / "config.yaml"
    conf.write_text("- a\n- b\n")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_flink_conf_file(MagicMock(), str(conf))


def _cluster_props(cfg: FlytConfig, conf_file=None) -> dict:
    user = user_flink_config(cfg, conf_file)
    return cluster_flink_config(cfg, resolve_application_resources(cfg, user), user)


def test_cluster_flink_config_derives_memory_slots_and_applies_overrides():
    cfg = _app_config(
        taskmanager_count=3,
        taskmanager_cpu=4,
        flink_config_override={
            "taskmanager.numberOfTaskSlots": 2,
            "restart-strategy.type": "fixed-delay",
            "custom.key": "v",
        },
    )
    props = _cluster_props(cfg)
    assert props["jobmanager.memory.process.size"] == "2048m"
    assert "jobmanager.memory.off-heap.size" not in props
    assert props["taskmanager.memory.process.size"] == "4096m"
    assert props["taskmanager.memory.task.off-heap.size"] == "512m"  # 1/8 of the TM JVM by default
    assert props["taskmanager.numberOfTaskSlots"] == "2"
    assert props["parallelism.default"] == "6"
    assert props["python.executable"] == "/py"
    assert props["env.java.default-opts.taskmanager"] == "-XX:ActiveProcessorCount=4 -XX:+ExitOnOutOfMemoryError"
    assert props["env.java.default-opts.jobmanager"] == "-XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError"
    assert props["taskmanager.jvm-exit-on-oom"] == "true"
    assert props["taskmanager.memory.managed.fraction"] == "0.1"  # the Python UDF workers' budget
    assert props["restart-strategy.type"] == "fixed-delay"
    assert props["custom.key"] == "v"


def test_cluster_flink_config_lets_configured_default_parallelism_win():
    cfg = _app_config(
        taskmanager_count=2, flink_config_override={"taskmanager.numberOfTaskSlots": 2, "parallelism.default": "3"}
    )
    assert _cluster_props(cfg)["parallelism.default"] == "3"


def test_cluster_flink_config_without_off_heap():
    props = _cluster_props(_app_config())
    assert "jobmanager.memory.off-heap.size" not in props
    assert props["restart-strategy.type"] == "exponential-delay"


def test_cluster_flink_config_lets_the_user_override_task_off_heap():
    cfg = _app_config(flink_config_override={"taskmanager.memory.task.off-heap.size": "2g"})
    assert _cluster_props(cfg)["taskmanager.memory.task.off-heap.size"] == "2g"


def test_cluster_flink_config_overlays_user_options_and_strips_runtime_keys():
    cfg = _app_config(flink_config_override={"custom.key": "from-config", "taskmanager.host": "ignored"})
    conf_file = {
        "custom.key": "from-file",
        "file.only": "x",
        "taskmanager.numberOfTaskSlots": "9",
        "jobmanager.rpc.address": "ignored",
        "rest.address": "ignored",
        "taskmanager.resource-id": "ignored",
    }
    props = _cluster_props(cfg, conf_file)
    assert props["custom.key"] == "from-config"  # flink_config_override wins over the file
    assert props["file.only"] == "x"
    assert props["taskmanager.numberOfTaskSlots"] == "9"  # the user options win over flyt defaults
    for key in ("jobmanager.rpc.address", "rest.address", "taskmanager.host", "taskmanager.resource-id"):
        assert key not in props


def test_cluster_flink_config_uses_application_jobmanager_memory():
    cfg = _app_config(jobmanager_cpu=4, flink_config_override={"jobmanager.memory.process.size": "6g"})
    props = _cluster_props(cfg)
    assert props["jobmanager.memory.process.size"] == "6g"  # the user value as written
    assert props["env.java.default-opts.jobmanager"].startswith("-XX:ActiveProcessorCount=4 ")


def test_render_flink_config_yaml_keeps_values_as_strings():
    text = render_flink_config_yaml({"b.key": "true", "a.key": "0.1", "c.key": "x y"})
    assert text.splitlines()[0].startswith("a.key:")  # sorted, flat dotted keys
    assert yaml.safe_load(text) == {"a.key": "0.1", "b.key": "true", "c.key": "x y"}


def test_cluster_flink_config_keeps_platform_jvm_flags_under_user_jvm_options():
    cfg = _app_config(
        flink_config_override={"env.java.opts.all": "-XX:+UseG1GC", "env.java.opts.taskmanager": "-Dfoo=1"}
    )
    props = _cluster_props(cfg)
    assert props["env.java.opts.all"] == "-XX:+UseG1GC"
    assert props["env.java.opts.taskmanager"] == "-Dfoo=1"
    # Flink prepends env.java.default-opts.*, so the user's options add to flyt's flags
    assert props["env.java.default-opts.all"] == "-Djava.net.preferIPv6Addresses=true"
    assert props["env.java.default-opts.taskmanager"] == "-XX:ActiveProcessorCount=1 -XX:+ExitOnOutOfMemoryError"


def test_cluster_flink_config_takes_ports_and_python_from_the_user():
    cfg = _app_config(flink_config_override={"rest.port": "8081", "python.executable": "/venv/bin/python"})
    props = _cluster_props(cfg)
    assert rest_port(props) == 8081
    assert props["python.executable"] == "/venv/bin/python"
    assert props["python.client.executable"] == "/py"
    assert rest_port(_cluster_props(_app_config())) == 27050


def test_process_size_alone_keeps_flyt_memory_defaults():
    props = _cluster_props(_app_config(flink_config_override={"taskmanager.memory.process.size": "8g"}))
    assert props["taskmanager.memory.process.size"] == "8g"
    assert props["taskmanager.memory.task.off-heap.size"] == "1024m"  # 1/8 of the user's size
    assert props["taskmanager.memory.managed.fraction"] == "0.1"
    tuned = _cluster_props(_app_config(flink_config_override={"taskmanager.memory.managed.fraction": "0.3"}))
    assert tuned["taskmanager.memory.managed.fraction"] == "0.3"
    assert tuned["taskmanager.memory.task.off-heap.size"] == "512m"


def test_other_memory_keys_hand_the_memory_model_to_the_user():
    cfg = _app_config(
        flink_config_override={"taskmanager.memory.process.size": "8g", "taskmanager.memory.flink.size": "7g"}
    )
    props = _cluster_props(cfg)
    assert props["taskmanager.memory.process.size"] == "8g"
    assert props["taskmanager.memory.flink.size"] == "7g"
    assert "taskmanager.memory.task.off-heap.size" not in props
    assert "taskmanager.memory.managed.fraction" not in props
    assert props["jobmanager.memory.process.size"] == "2048m"  # the other role keeps its defaults


def test_flink_config_warnings_name_ignored_flyt_owned_and_missing_keys():
    warnings = flink_config_warnings(
        {
            "taskmanager.host": "h",
            "rest.bind-address": "localhost",
            "rest.bind-port": "8081",
            "taskmanager.rpc.port": "6122",
            "env.java.default-opts.all": "-X",
        }
    )
    assert any(w.startswith("taskmanager.host is set per YT job by flyt") for w in warnings)
    assert any(w.startswith("rest.bind-address is always '0.0.0.0' on YT; 'localhost'") for w in warnings)
    assert any(w.startswith("taskmanager.rpc.port is always '0' on YT; '6122'") for w in warnings)
    assert any(w.startswith("rest.bind-port is ignored") for w in warnings)
    assert any(w.startswith("env.java.default-opts.all carries flyt's platform JVM flags") for w in warnings)
    assert any(w.startswith("taskmanager.numberOfTaskSlots is not set") for w in warnings)
    # a k8s-style config that repeats flyt's own values warns about nothing
    same = {"taskmanager.numberOfTaskSlots": "2", "rest.bind-address": "0.0.0.0", "taskmanager.rpc.port": "0"}
    assert flink_config_warnings(same) == []


def test_cluster_flink_config_never_takes_flyt_owned_keys_from_the_user():
    cfg = _app_config(
        flink_config_override={
            "rest.bind-address": "localhost",
            "jobmanager.bind-host": "127.0.0.1",
            "taskmanager.bind-host": "127.0.0.1",
            "taskmanager.rpc.port": "6122",
            "taskmanager.data.port": "6121",
            "rest.bind-port": "8081",
            "env.java.default-opts.taskmanager": "-Xss1m",
            "jobmanager.rpc.port": "6123",
            "blob.server.port": "6124",
        }
    )
    props = _cluster_props(cfg)
    for key in ("rest.bind-address", "jobmanager.bind-host", "taskmanager.bind-host"):
        assert props[key] == "0.0.0.0"
    assert props["taskmanager.rpc.port"] == props["taskmanager.data.port"] == "0"
    assert "rest.bind-port" not in props
    assert props["env.java.default-opts.taskmanager"] == "-XX:ActiveProcessorCount=1 -XX:+ExitOnOutOfMemoryError"
    # JobManager ports stay the user's: the JobManager has its own job address
    assert (props["jobmanager.rpc.port"], props["blob.server.port"]) == ("6123", "6124")


def test_cluster_flink_config_bounds_flink_restarts():
    props = _cluster_props(_app_config())
    assert props["restart-strategy.exponential-delay.attempts-before-reset-backoff"] == "10"
    cfg = _app_config(flink_config_override={"restart-strategy.exponential-delay.attempts-before-reset-backoff": "3"})
    assert _cluster_props(cfg)["restart-strategy.exponential-delay.attempts-before-reset-backoff"] == "3"


def test_minicluster_ignored_options_warning_names_what_is_ignored():
    from ytsaurus_flyt.flink_conf import minicluster_ignored_options_warning

    assert minicluster_ignored_options_warning(FlytConfig()) is None
    warning = minicluster_ignored_options_warning(
        FlytConfig(base_flink_conf_path="//home/conf.yaml", flink_config_override={"a": "1", "b": "2"})
    )
    assert warning.startswith("base_flink_conf_path //home/conf.yaml and 2 flink_config_override/-D key(s)")
    assert "application mode only" in warning
