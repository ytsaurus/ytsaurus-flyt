"""Tests for ytsaurus_flyt.flink_conf: the config.yaml flyt generates, the same layers in both modes."""

import pytest
import yaml

from ytsaurus_flyt.application import application_cluster
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    FlinkConfigError,
    flatten_flink_config,
    flink_config_warnings,
    load_flink_conf_file,
    render_flink_config_yaml,
    user_flink_config,
)
from ytsaurus_flyt.minicluster import minicluster
from ytsaurus_flyt.models import ClusterPreset


def _config(**overrides) -> FlytConfig:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return FlytConfig(**base)


def _props(user=None, **overrides) -> dict:
    if any(k.startswith("taskmanager.memory.") for k in user or {}):  # own sizing: no preset
        user = {"flyt.taskmanager.cpu.cores": "2", **user}
        return application_cluster(_config(**overrides), None, user).flink_config
    return application_cluster(_config(**overrides), ClusterPreset.MICRO.params, user).flink_config


def _mini_props(user=None, **overrides) -> dict:
    return minicluster(_config(cluster_mode="minicluster", **overrides), ClusterPreset.MICRO.params, user).flink_config


def test_application_flink_config_sets_ports_memory_slots_and_jvm_flags():
    props = _props({"flyt.taskmanager.replicas": "3", "flyt.jobmanager.cpu.cores": "4"})
    assert (props["rest.port"], props["jobmanager.rpc.port"], props["blob.server.port"]) == ("27050", "27051", "27052")
    for key in ("rest.bind-address", "jobmanager.bind-host", "taskmanager.bind-host"):
        assert props[key] == "0.0.0.0"
    assert props["taskmanager.rpc.port"] == props["taskmanager.data.port"] == "0"
    assert props["taskmanager.numberOfTaskSlots"] == "2"  # micro: 2 cpu
    assert props["taskmanager.cpu.cores"] == "2"
    assert props["parallelism.default"] == "6"
    assert props["jobmanager.memory.process.size"] == "2560m"
    assert props["taskmanager.memory.process.size"] == "4096m"  # the 4G container
    assert props["jobmanager.memory.jvm-overhead.min"] == props["taskmanager.memory.jvm-overhead.min"] == "512m"
    assert "taskmanager.memory.jvm-overhead.max" not in props  # Flink's 1g
    assert props["taskmanager.memory.task.off-heap.size"] == "512m"  # 1/8 of the TaskManager JVM
    assert props["taskmanager.memory.managed.fraction"] == "0.1"  # the Python UDF workers' budget
    assert props["python.executable"] == props["python.client.executable"] == "/py"
    # default-opts: env.java.opts.* of the user come after them and win
    assert props["env.java.default-opts.all"] == (
        "-Djava.net.preferIPv6Addresses=true -Dfile.encoding=UTF-8 -Dio.netty.tryReflectionSetAccessible=true"
    )
    assert props["env.java.default-opts.jobmanager"] == "-XX:ActiveProcessorCount=4 -XX:+ExitOnOutOfMemoryError"
    assert props["env.java.default-opts.taskmanager"] == "-XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError"
    assert props["taskmanager.jvm-exit-on-oom"] == "true"


def test_minicluster_config_sizes_the_job_jvm_by_the_preset():
    props = _mini_props()
    assert props["env.java.default-opts.all"] == (
        "-Xmx2324M -XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError -Djava.net.preferIPv6Addresses=true"
        " -Dfile.encoding=UTF-8 -Dio.netty.tryReflectionSetAccessible=true"
    )
    assert props["parallelism.default"] == props["taskmanager.cpu.cores"] == "2"  # micro: 2 cpu
    # the MiniCluster picks its slots from the job graph; its JVM is not sized by Flink's memory model
    assert "taskmanager.numberOfTaskSlots" not in props
    assert not [k for k in props if ".memory." in k]


def test_minicluster_sized_by_taskmanager_process_size_gets_taskmanager_memory_defaults():
    user = {"flyt.taskmanager.cpu.cores": "3", "taskmanager.memory.process.size": "6g"}
    props = minicluster(_config(cluster_mode="minicluster"), None, user).flink_config
    assert props["taskmanager.memory.task.off-heap.size"] == "768m"  # 1/8 of process.size
    assert props["taskmanager.memory.managed.fraction"] == "0.1"
    assert props["taskmanager.memory.jvm-overhead.min"] == "512m"
    assert "jobmanager.memory.jvm-overhead.min" not in props
    assert props["parallelism.default"] == props["taskmanager.cpu.cores"] == "3"
    # the run script prepends the JVM flags Flink derives; the preset's -Xmx is gone
    assert props["env.java.default-opts.all"].startswith("-XX:ActiveProcessorCount=3 ")


def test_both_modes_share_ports_restarts_and_python():
    shared = (
        "rest.port",
        "rest.bind-address",
        "restart-strategy.type",
        "taskmanager.jvm-exit-on-oom",
        "heartbeat.timeout",
        "heartbeat.interval",
        "heartbeat.rpc-failure-threshold",
        "python.executable",
        "python.client.executable",
    )
    app, mini = _props(), _mini_props()
    assert {k: app[k] for k in shared} == {k: mini[k] for k in shared}
    assert mini["restart-strategy.type"] == "exponential-delay"
    assert mini["heartbeat.timeout"] == "120000"


@pytest.mark.parametrize("props_of", [_props, _mini_props])
def test_user_options_override_flyt_defaults_in_both_modes(props_of):
    user = {
        "restart-strategy.type": "fixed-delay",
        "parallelism.default": "1",
        "pipeline.name": "wc",
        "env.java.opts.all": "-XX:+UseG1GC",
    }
    props = props_of(user)
    for key, value in user.items():
        assert props[key] == value


@pytest.mark.parametrize("props_of", [_props, _mini_props])
def test_flyt_owned_keys_win_over_the_user_in_both_modes(props_of):
    user = {
        "rest.port": "8081",
        "rest.bind-address": "localhost",
        "rest.bind-port": "8081-8090",
        "taskmanager.data.port": "6121",
        "jobmanager.rpc.address": "jm",
        "env.java.default-opts.all": "-Xmx1g",
    }
    props = props_of(user)
    assert props["rest.port"] == "27050"
    assert props["rest.bind-address"] == "0.0.0.0"
    assert props["taskmanager.data.port"] == "0"
    assert "rest.bind-port" not in props
    assert "jobmanager.rpc.address" not in props
    assert "-Xmx1g" not in props["env.java.default-opts.all"]


def test_application_flink_config_leaves_per_container_keys_to_the_run_scripts():
    props = _props()
    for key in ("jobmanager.rpc.address", "rest.address", "taskmanager.host", "taskmanager.resource-id"):
        assert key not in props


def test_custom_taskmanager_memory_model_drops_flyt_memory_defaults():
    props = _props({"taskmanager.memory.process.size": "6g", "taskmanager.memory.network.fraction": "0.2"})
    assert props["taskmanager.memory.process.size"] == "6g"
    assert "taskmanager.memory.managed.fraction" not in props
    assert "taskmanager.memory.task.off-heap.size" not in props
    assert props["jobmanager.memory.process.size"] == "2560m"


def test_jvm_overhead_is_never_below_flyt_minimum():
    user = {
        "taskmanager.memory.process.size": "4g",
        "taskmanager.memory.jvm-overhead.min": "256m",
        "taskmanager.memory.jvm-overhead.max": "300m",
        "jobmanager.memory.jvm-overhead.min": "1g",
        "jobmanager.memory.jvm-overhead.fraction": "0.2",
    }
    props = _props(user)
    assert props["taskmanager.memory.jvm-overhead.min"] == props["taskmanager.memory.jvm-overhead.max"] == "512m"
    assert props["jobmanager.memory.jvm-overhead.min"] == "1g"  # above the minimum: kept
    assert props["jobmanager.memory.jvm-overhead.fraction"] == "0.2"
    # overhead keys only carve process.size: flyt keeps its memory defaults
    assert props["taskmanager.memory.managed.fraction"] == "0.1"
    warnings = flink_config_warnings(_config(), user)
    assert len(warnings) == 2
    assert all(w.startswith("taskmanager.memory.jvm-overhead.") and "raised to 512m" in w for w in warnings)


def test_flink_config_warnings_cover_owned_and_mode_ignored_keys():
    user = {"rest.port": "8081", "rest.address": "x", "jobmanager.memory.process.size": "8g"}
    app = flink_config_warnings(_config(), user)
    mini = flink_config_warnings(_config(cluster_mode="minicluster"), user)
    assert any("rest.port" in w for w in app) and any("rest.address" in w for w in app)
    assert not any("jobmanager.memory.process.size" in w for w in app)
    assert any("jobmanager.memory.process.size has no effect in minicluster" in w for w in mini)
    overhead = {"taskmanager.memory.jvm-overhead.min": "256m"}
    sized = flink_config_warnings(
        _config(cluster_mode="minicluster"), {**overhead, "taskmanager.memory.process.size": "6g"}
    )
    assert sized == [
        "taskmanager.memory.jvm-overhead.min is raised to 512m: the YT container is process.size, and the JVM overhead holds the processes outside the JVM as well"
    ]
    assert flink_config_warnings(_config(), {"rest.port": "27050"}) == []  # flyt's own value is fine
    # taskmanager.cpu.cores is the container cpu: flyt.taskmanager.cpu.cores or the preset
    (cores,) = flink_config_warnings(_config(), {"taskmanager.cpu.cores": "8"})
    assert "set by flyt from flyt.taskmanager.cpu.cores" in cores
    assert _props({"taskmanager.cpu.cores": "8"})["taskmanager.cpu.cores"] == "2"


def test_minicluster_warns_about_slots_per_role_jvm_opts_and_cluster_options():
    user = {
        "taskmanager.numberOfTaskSlots": "4",
        "env.java.opts.taskmanager": "-XX:+UseG1GC",
        "env.java.opts.jobmanager": "-XX:+UseG1GC",
        "env.java.opts.all": "-XX:+UseG1GC",
        "flyt.taskmanager.replicas": "2",
        "flyt.jobmanager.cpu.cores": "4",
    }
    mini = flink_config_warnings(_config(cluster_mode="minicluster"), user)
    assert len(mini) == 5
    assert any("taskmanager.numberOfTaskSlots is not needed in minicluster" in w for w in mini)
    assert any("env.java.opts.taskmanager has no effect in minicluster" in w for w in mini)
    assert any("flyt.taskmanager.replicas has no effect in minicluster" in w for w in mini)
    assert flink_config_warnings(_config(), user) == []


def test_user_flink_config_layers_override_over_the_file(tmp_path):
    conf = tmp_path / "flink.yaml"
    conf.write_text("taskmanager:\n  numberOfTaskSlots: 4\npipeline.name: from-file\nexecution.attached: true\n")
    cfg = _config(base_flink_conf_path=str(conf), flink_config_override={"pipeline.name": "override"})
    assert user_flink_config(cfg) == {
        "taskmanager.numberOfTaskSlots": "4",
        "pipeline.name": "override",
        "execution.attached": "true",
    }
    assert user_flink_config(_config()) == {}


def test_load_flink_conf_file_rejects_a_non_mapping(tmp_path):
    conf = tmp_path / "flink.yaml"
    conf.write_text("- a\n- b\n")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_flink_conf_file(str(conf))


def test_load_flink_conf_file_reports_a_missing_file_and_broken_yaml_as_config_errors(tmp_path):
    with pytest.raises(FlinkConfigError, match="nope.yaml.*No such file"):
        load_flink_conf_file(str(tmp_path / "nope.yaml"))
    conf = tmp_path / "flink.yaml"
    conf.write_text("a: [1\n")
    with pytest.raises(FlinkConfigError, match="is not valid YAML"):
        load_flink_conf_file(str(conf))


def test_flatten_flink_config_keeps_scalars_as_flink_strings():
    assert flatten_flink_config({"a": {"b": True, "c": None}, "d": 1.5}) == {"a.b": "true", "a.c": "", "d": "1.5"}


def test_render_flink_config_yaml_keeps_values_as_strings_on_one_line():
    long_opts = " ".join(["-XX:+UseG1GC"] * 20)
    text = render_flink_config_yaml({"b.key": "true", "a.key": "0.1", "c.key": "x y", "env.java.opts.all": long_opts})
    assert text.splitlines()[0].startswith("a.key:")  # sorted, flat dotted keys
    # Flink's bash scripts grep env.java.* line by line: a folded value would be cut
    assert f"env.java.opts.all: {long_opts}" in text.splitlines()
    assert yaml.safe_load(text) == {"a.key": "0.1", "b.key": "true", "c.key": "x y", "env.java.opts.all": long_opts}
