"""Tests for ytsaurus_flyt.application: the YT resources and assembly of an application cluster."""

import pytest

from ytsaurus_flyt.application import application_cluster, resolve_application_resources
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import MIB, ClusterParams, ClusterPreset

_MIB = MIB


def _app_config(**overrides) -> FlytConfig:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return FlytConfig(**base)


def test_preset_sizes_every_taskmanager_and_jobmanager_keeps_defaults():
    res = resolve_application_resources(_app_config(taskmanager_count=3), ClusterPreset.LARGE.params)
    assert (res.taskmanager_cpu, res.slots) == (4, 4)  # one slot per cpu
    assert res.taskmanager_process_memory == 16 * 1024 * _MIB  # container = process.size = preset memory
    assert res.taskmanager_params().memory == 16 * 1024 * _MIB
    assert (res.jobmanager_cpu, res.jobmanager_process_memory) == (2, 2560 * _MIB)
    assert res.jobmanager_params().memory == 2560 * _MIB
    assert res.default_parallelism == 12


def test_cpu_fields_override_the_preset_and_the_jobmanager_default():
    cfg = _app_config(taskmanager_count=2, taskmanager_cpu=6, jobmanager_cpu=4)
    res = resolve_application_resources(cfg, ClusterPreset.SMALL.params)
    assert (res.taskmanager_cpu, res.slots, res.default_parallelism) == (6, 6, 12)
    assert res.taskmanager_process_memory == 8 * 1024 * _MIB
    assert res.jobmanager_cpu == 4


def test_preset_too_small_for_a_taskmanager_jvm_is_rejected():
    tiny = ClusterParams(cpu=1, memory="512M", max_heap_size="256M", data_size_per_job="1M")
    with pytest.raises(ValueError, match="TaskManager memory 512m leaves no JVM"):
        resolve_application_resources(_app_config(), tiny)
    with pytest.raises(ValueError, match="JobManager memory 512m leaves no JVM"):
        resolve_application_resources(
            _app_config(), ClusterPreset.MICRO.params, {"jobmanager.memory.process.size": "512m"}
        )


def test_application_cluster_bundles_resources_and_config():
    cluster = application_cluster(_app_config(taskmanager_count=2), ClusterPreset.SMALL.params)
    assert cluster.resources.slots == 2
    assert cluster.flink_config["taskmanager.numberOfTaskSlots"] == "2"
    assert cluster.flink_config["parallelism.default"] == "4"
    assert cluster.flink_config["taskmanager.memory.process.size"] == f"{8 * 1024}m"


def test_flink_keys_override_slots_memory_and_parallelism():
    user = {
        "taskmanager.numberOfTaskSlots": "3",
        "taskmanager.memory.process.size": "6g",
        "jobmanager.memory.process.size": "1g",
        "parallelism.default": "4",
    }
    res = resolve_application_resources(_app_config(taskmanager_count=2), ClusterPreset.SMALL.params, user)
    assert (res.taskmanager_cpu, res.slots, res.default_parallelism) == (2, 3, 4)  # cpu stays the preset's
    assert res.taskmanager_params().memory == 6 * 1024 * _MIB  # container = process.size
    assert res.jobmanager_params().memory == 1024 * _MIB


def test_default_parallelism_above_the_capacity_is_rejected():
    with pytest.raises(ValueError, match="does not fit into taskmanager_count \\* slots = 4"):
        resolve_application_resources(
            _app_config(taskmanager_count=2), ClusterPreset.SMALL.params, {"parallelism.default": "5"}
        )


@pytest.mark.parametrize(
    "user, match",
    [
        ({"taskmanager.numberOfTaskSlots": "two"}, "must be an integer"),
        ({"taskmanager.memory.process.size": "lots"}, "not a memory size"),
        ({"taskmanager.memory.flink.size": "4g"}, "set taskmanager.memory.process.size as well"),
        ({"jobmanager.memory.heap.size": "1g"}, "set jobmanager.memory.process.size as well"),
    ],
)
def test_bad_flink_sizing_keys_are_rejected(user, match):
    with pytest.raises(ValueError, match=match):
        resolve_application_resources(_app_config(), ClusterPreset.SMALL.params, user)
