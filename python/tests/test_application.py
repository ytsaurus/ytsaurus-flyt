"""Tests for ytsaurus_flyt.application: the YT resources and assembly of an application cluster."""

import pytest

from ytsaurus_flyt.application import application_cluster, resolve_application_resources
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import MIB

_MIB = MIB
_TM_CPU, _JM_CPU, _REPLICAS = "flyt.taskmanager.cpu.cores", "flyt.jobmanager.cpu.cores", "flyt.taskmanager.replicas"


def _app_config(**overrides) -> FlytConfig:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return FlytConfig(**base)


def test_defaults_size_every_taskmanager_and_the_jobmanager():
    res = resolve_application_resources({_REPLICAS: "3"})
    assert (res.taskmanager_size.cpu, res.slots) == (2, 2)  # one slot per cpu
    assert res.taskmanager_size.memory == 4 * 1024 * _MIB  # container = process.size
    assert (res.jobmanager_cpu, res.jobmanager_process_memory) == (2, 2560 * _MIB)
    assert res.default_parallelism == 6


def test_cpu_and_process_size_size_every_taskmanager():
    user = {_REPLICAS: "2", _TM_CPU: "6", _JM_CPU: "4", "taskmanager.memory.process.size": "8g"}
    res = resolve_application_resources(user)
    assert (res.taskmanager_size.cpu, res.slots, res.default_parallelism) == (6, 6, 12)
    assert res.taskmanager_size.memory == 8 * 1024 * _MIB
    assert res.jobmanager_cpu == 4
    # either key alone keeps flyt's default for the other
    assert resolve_application_resources({_TM_CPU: "6"}).taskmanager_size.memory == 4 * 1024 * _MIB
    assert resolve_application_resources({"taskmanager.memory.process.size": "8g"}).taskmanager_size.cpu == 2


@pytest.mark.parametrize(
    "user, match",
    [
        ({"taskmanager.memory.flink.size": "4g"}, "set taskmanager.memory.process.size as well"),
        ({"taskmanager.memory.process.size": "lots"}, "not a memory size"),
    ],
)
def test_taskmanager_memory_model_needs_process_size(user, match):
    with pytest.raises(ValueError, match=match):
        resolve_application_resources(user)


def test_taskmanager_cpu_defaults_to_the_slots():
    res = resolve_application_resources({"taskmanager.numberOfTaskSlots": "4"})
    assert (res.taskmanager_size.cpu, res.slots) == (4, 4)
    res = resolve_application_resources({_TM_CPU: "2", "taskmanager.numberOfTaskSlots": "4"})
    assert (res.taskmanager_size.cpu, res.slots) == (2, 4)


def test_application_cluster_bundles_resources_and_config():
    cluster = application_cluster(_app_config(), {_REPLICAS: "2", "taskmanager.memory.process.size": "8g"})
    assert cluster.resources.slots == 2
    assert cluster.flink_config["taskmanager.numberOfTaskSlots"] == "2"
    assert cluster.flink_config["parallelism.default"] == "4"
    assert cluster.flink_config["taskmanager.memory.process.size"] == "8g"  # the user's value as is
    assert cluster.flink_config["taskmanager.cpu.cores"] == "2"


def test_flink_keys_override_slots_memory_and_parallelism():
    user = {
        "taskmanager.numberOfTaskSlots": "3",
        "taskmanager.memory.process.size": "6g",
        "jobmanager.memory.process.size": "1g",
        "parallelism.default": "4",
        _REPLICAS: "2",
        _TM_CPU: "2",
    }
    res = resolve_application_resources(user)
    assert (res.taskmanager_size.cpu, res.slots, res.default_parallelism) == (2, 3, 4)
    assert res.taskmanager_size.memory == 6 * 1024 * _MIB  # container = process.size
    assert res.jobmanager_process_memory == 1024 * _MIB


def test_default_parallelism_above_the_capacity_is_rejected():
    with pytest.raises(ValueError, match=f"does not fit into {_REPLICAS} \\* slots = 4"):
        resolve_application_resources({_REPLICAS: "2", "parallelism.default": "5"})


@pytest.mark.parametrize(
    "user, match",
    [
        ({"taskmanager.numberOfTaskSlots": "two"}, "must be an integer"),
        ({_REPLICAS: "0"}, f"{_REPLICAS} must be >= 1"),
        ({"jobmanager.memory.heap.size": "1g"}, "set jobmanager.memory.process.size as well"),
    ],
)
def test_bad_flink_sizing_keys_are_rejected(user, match):
    with pytest.raises(ValueError, match=match):
        resolve_application_resources(user)
