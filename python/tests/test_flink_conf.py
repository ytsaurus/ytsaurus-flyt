"""Tests for ytsaurus_flyt.flink_conf: the cluster config.yaml flyt generates in application mode."""

import yaml

from ytsaurus_flyt.application import application_cluster
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import render_flink_config_yaml
from ytsaurus_flyt.models import MiniClusterPreset


def _props(**overrides) -> dict:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return application_cluster(FlytConfig(**base), MiniClusterPreset.MICRO.params).flink_config


def test_cluster_flink_config_sets_ports_memory_slots_and_jvm_flags():
    props = _props(taskmanager_count=3, jobmanager_cpu=4)
    assert (props["rest.port"], props["jobmanager.rpc.port"], props["blob.server.port"]) == ("27050", "27051", "27052")
    for key in ("rest.bind-address", "jobmanager.bind-host", "taskmanager.bind-host"):
        assert props[key] == "0.0.0.0"
    assert props["taskmanager.rpc.port"] == props["taskmanager.data.port"] == "0"
    assert props["taskmanager.numberOfTaskSlots"] == "2"  # micro: 2 cpu
    assert props["parallelism.default"] == "6"
    assert props["jobmanager.memory.process.size"] == "2048m"
    assert props["taskmanager.memory.process.size"] == "3584m"  # 4G container - 512m
    assert props["taskmanager.memory.task.off-heap.size"] == "448m"  # 1/8 of the TaskManager JVM
    assert props["taskmanager.memory.managed.fraction"] == "0.1"
    assert props["python.executable"] == props["python.client.executable"] == "/py"
    assert props["env.java.opts.all"] == "-Djava.net.preferIPv6Addresses=true"
    assert props["env.java.opts.jobmanager"] == "-XX:ActiveProcessorCount=4 -XX:+ExitOnOutOfMemoryError"
    assert props["env.java.opts.taskmanager"] == "-XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError"
    assert props["taskmanager.jvm-exit-on-oom"] == "true"


def test_cluster_flink_config_bounds_flink_restarts():
    props = _props()
    assert props["restart-strategy.type"] == "exponential-delay"
    assert props["restart-strategy.exponential-delay.attempts-before-reset-backoff"] == "10"


def test_cluster_flink_config_leaves_per_container_keys_to_the_run_scripts():
    props = _props()
    for key in ("jobmanager.rpc.address", "rest.address", "taskmanager.host", "taskmanager.resource-id"):
        assert key not in props


def test_render_flink_config_yaml_keeps_values_as_strings():
    text = render_flink_config_yaml({"b.key": "true", "a.key": "0.1", "c.key": "x y"})
    assert text.splitlines()[0].startswith("a.key:")  # sorted, flat dotted keys
    assert yaml.safe_load(text) == {"a.key": "0.1", "b.key": "true", "c.key": "x y"}
