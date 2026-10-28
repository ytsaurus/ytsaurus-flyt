"""Tests for ytsaurus_flyt.application: the YT resources and assembly of an application cluster."""

from typing import Mapping, Optional

import pytest

from ytsaurus_flyt.application import ApplicationResources, application_cluster, resolve_application_resources
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import flink_config_warnings, user_flink_config


def _res(cfg: FlytConfig, conf_file: Optional[Mapping[str, str]] = None) -> ApplicationResources:
    return resolve_application_resources(cfg, user_flink_config(cfg, conf_file))


def _app_config(**overrides) -> FlytConfig:
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py", cluster_mode="application")
    base.update(overrides)
    return FlytConfig(**base)


def test_invalid_slots_and_process_size():
    with pytest.raises(ValueError, match="process.size"):
        _res(FlytConfig(flink_config_override={"taskmanager.memory.process.size": "big"}))
    with pytest.raises(ValueError, match="numberOfTaskSlots"):
        _res(FlytConfig(flink_config_override={"taskmanager.numberOfTaskSlots": "0"}))
    with pytest.raises(ValueError, match="numberOfTaskSlots"):
        _res(FlytConfig(flink_config_override={"taskmanager.numberOfTaskSlots": "many"}))


def test_application_resources_apply_flyt_defaults_once():
    res = _res(FlytConfig())
    assert (res.jobmanager_cpu, res.jobmanager_process_memory) == (2, 2 * 1024**3)
    assert (res.taskmanager_cpu, res.taskmanager_process_memory) == (1, 4 * 1024**3)  # cpu = slots
    explicit = _res(
        FlytConfig(
            taskmanager_count=3,
            jobmanager_cpu=4,
            taskmanager_cpu=12,
            flink_config_override={"jobmanager.memory.process.size": "3g", "taskmanager.numberOfTaskSlots": 7},
        ),
        {"taskmanager.memory.process.size": "14500m", "parallelism.default": "5"},
    )
    assert (explicit.jobmanager_cpu, explicit.jobmanager_process_memory) == (4, 3 * 1024**3)
    assert (explicit.taskmanager_cpu, explicit.taskmanager_process_memory) == (12, 14500 * 1024**2)
    assert (explicit.slots, explicit.capacity, explicit.default_parallelism) == (7, 21, 5)


def test_default_parallelism_is_capacity_unless_configured():
    slots2 = FlytConfig(taskmanager_count=3, flink_config_override={"taskmanager.numberOfTaskSlots": 2})
    assert _res(slots2).default_parallelism == 6
    cfg = FlytConfig(taskmanager_count=3, flink_config_override={"parallelism.default": 4})
    assert _res(cfg).default_parallelism == 4
    from_file = _res(FlytConfig(taskmanager_count=3), {"parallelism.default": "5"})
    assert from_file.default_parallelism == 5
    with pytest.raises(ValueError, match="does not fit"):
        from_file.check_parallelism_fits()
    _res(FlytConfig(taskmanager_count=5), {"parallelism.default": "5"}).check_parallelism_fits()


def test_application_resources_build_task_and_jobmanager_params():
    slots2 = {"taskmanager.numberOfTaskSlots": "2"}
    same = _res(FlytConfig(taskmanager_count=3, flink_config_override=slots2)).taskmanager_params()
    assert (same.cpu, same.process_memory, same.count, same.slots) == (2, 4 * 1024**3, 3, 2)  # cpu = slots
    from_file = _res(FlytConfig(taskmanager_count=4), {"taskmanager.numberOfTaskSlots": "7"})
    assert (from_file.taskmanager_params().count, from_file.taskmanager_params().cpu) == (4, 7)
    assert _res(FlytConfig()).taskmanager_params().count == 1
    sized = FlytConfig(flink_config_override={"taskmanager.memory.process.size": "14500m"}, taskmanager_cpu=20)
    tm = _res(sized).taskmanager_params()
    assert (tm.cpu, tm.process_memory, tm.memory) == (20, 14500 * 1024**2, (14500 + 512) * 1024**2)

    jm = _res(FlytConfig()).jobmanager_params()
    assert (jm.cpu, jm.process_memory, jm.memory) == (2, 2 * 1024**3, 2560 * 1024**2)
    jm = _res(FlytConfig(jobmanager_cpu=4), {"jobmanager.memory.process.size": "6g"}).jobmanager_params()
    assert (jm.cpu, jm.process_memory, jm.memory) == (4, 6 * 1024**3, (6 * 1024 + 512) * 1024**2)


def test_other_memory_keys_require_process_size_to_size_the_container():
    with pytest.raises(ValueError, match="taskmanager.memory.flink.size .*set taskmanager.memory.process.size"):
        _res(_app_config(flink_config_override={"taskmanager.memory.flink.size": "7g"}))
    with pytest.raises(ValueError, match="jobmanager.memory.heap.size .*set jobmanager.memory.process.size"):
        _res(_app_config(flink_config_override={"jobmanager.memory.heap.size": "1g"}))
    props = application_cluster(
        _app_config(flink_config_override={"jobmanager.memory.heap.size": "1g", "jobmanager.memory.process.size": "3g"})
    ).flink_config
    assert props["jobmanager.memory.process.size"] == "3g"


def test_custom_memory_keys_with_process_size_size_the_container_from_it():
    cfg = _app_config(
        flink_config_override={"taskmanager.memory.process.size": "8g", "taskmanager.memory.flink.size": "7g"}
    )
    assert _res(cfg).taskmanager_process_memory == 8 * 1024**3


def test_application_cluster_bundles_resources_config_warnings_and_rest_port():
    cfg = _app_config(taskmanager_count=2, flink_config_override={"rest.port": "8081"})
    cluster = application_cluster(cfg, {"pipeline.name": "x"})
    assert cluster.resources == _res(cfg)
    assert cluster.flink_config["pipeline.name"] == "x"
    assert cluster.flink_config["parallelism.default"] == "2"
    assert cluster.rest_port == 8081
    assert cluster.warnings == tuple(flink_config_warnings(user_flink_config(cfg, {"pipeline.name": "x"})))
    assert application_cluster(_app_config()).rest_port == 27050
