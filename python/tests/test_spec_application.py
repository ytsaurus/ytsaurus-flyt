"""Vanilla spec assembly for ``cluster_mode: application`` (and MiniCluster parity)."""

import pytest

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import ClusterPreset, JobmanagerParams, OperationParams, TaskmanagerParams
from ytsaurus_flyt.spec import (
    JOBMANAGER_TASK,
    MINICLUSTER_TASK,
    TASKMANAGER_TASK,
    build_vanilla_operation_spec,
    flink_dynamic_properties,
    jobmanager_process_size,
)

_MICRO = ClusterPreset.MICRO.params


def _jm() -> JobmanagerParams:
    return JobmanagerParams(cpu=_MICRO.cpu, memory=_MICRO.memory_bytes())


def _tm(count: int = 2, slots: int = 2) -> TaskmanagerParams:
    return TaskmanagerParams(cpu=4, memory=_MICRO.memory_bytes(), count=count, slots=slots)


def _op() -> OperationParams:
    return OperationParams(file_paths=["//tmp/wheel.whl"], layer_paths=["//home/layers/runtime.squashfs"], pool="pool")


def _app_config(**overrides) -> FlytConfig:
    base = dict(
        service_name="svc",
        java_home="/jdk",
        python_bin="/py",
        cluster_mode="application",
    )
    base.update(overrides)
    return FlytConfig(**base)


def _build(config: FlytConfig, **kwargs):
    kwargs.setdefault("taskmanager_params", _tm())
    kwargs.setdefault("yt_proxy", "http://proxy.example:80")
    return build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py --x 1",
        config=config,
        operation_params=_op(),
        jobmanager_params=_jm(),
        secure_vault={},
        max_heap_size_str=_MICRO.max_heap_size,
        **kwargs,
    ).build()


def test_application_spec_has_gang_jobmanager_and_plain_taskmanager_tasks():
    spec = _build(_app_config())
    assert set(spec["tasks"]) == {JOBMANAGER_TASK, TASKMANAGER_TASK}
    jm = spec["tasks"][JOBMANAGER_TASK]
    tm = spec["tasks"][TASKMANAGER_TASK]
    assert jm["job_count"] == 1
    assert jm["gang_options"] == {}
    assert "gang_options" not in tm
    assert tm["job_count"] == 2
    assert tm["cpu_limit"] == 4
    # gang tasks reject restart_completed_jobs; the JobManager script handles it instead
    assert "restart_completed_jobs" not in jm
    assert "restart_completed_jobs" not in tm
    for task in (jm, tm):
        assert task["layer_paths"] == ["//home/layers/runtime.squashfs"]
        assert "//tmp/wheel.whl" in task["file_paths"]
        assert task["tmpfs_path"] == "."
        env = task["environment"]
        assert env["FLYT_CLUSTER_MODE"] == "application"
        assert env["FLYT_YT_PROXY"] == "http://proxy.example:80"
        assert "FLYT_DISCOVERY_PREFIX" not in env
        assert env["FLINK_STANDALONE"] == "True"
        # memory goes through Flink's memory model, never -Xmx
        assert "FLINK_ENV_JAVA_OPTS" not in env


def test_application_spec_requires_taskmanager_params_and_proxy():
    with pytest.raises(ValueError, match="taskmanager_params"):
        _build(_app_config(), taskmanager_params=None)
    with pytest.raises(ValueError, match="yt_proxy"):
        _build(_app_config(), yt_proxy=None)


def test_application_run_scripts_launch_standalone_job_and_taskmanager():
    spec = _build(_app_config())
    jm_cmd = spec["tasks"][JOBMANAGER_TASK]["command"]
    tm_cmd = spec["tasks"][TASKMANAGER_TASK]["command"]
    for cmd in (jm_cmd, tm_cmd):
        assert "02_prepare_cluster_bins.sh" in cmd
        assert "35_cluster_common.sh" in cmd
        assert "30_prepare_service.sh" in cmd
        assert "40_run_job.sh" not in cmd
        assert "FLYT_JOB_HELPER_EOF" in cmd
        assert "def cmd_wait_jobmanager" in cmd
        assert "-Djobmanager.rpc.port=27051" in cmd
        assert "-Dparallelism.default=4" in cmd
    assert 'standalone-job.sh" start-foreground' in jm_cmd
    assert "--job-classname org.apache.flink.client.python.PythonDriver" in jm_cmd
    assert "set -- pipeline.py --x 1" in jm_cmd
    assert 'CLUSTER_IP="${YT_IP_ADDRESS_FASTBONE:-$SLOT_IP}"' in jm_cmd
    assert '"-Djobmanager.rpc.address=$JM_HOST" "-Drest.address=$REST_HOST"' in jm_cmd
    assert '"-Dtaskmanager.host=$CLUSTER_IP"' in tm_cmd
    assert "publish" not in jm_cmd and "heartbeat" not in jm_cmd
    assert "complete-operation" in jm_cmd
    assert "42_run_taskmanager.sh" not in jm_cmd
    assert 'taskmanager.sh" start-foreground' in tm_cmd
    assert 'wait-jobmanager --operation-id "$YT_OPERATION_ID"' in tm_cmd
    assert "--rest-port 27050 --timeout 600" in tm_cmd
    assert "41_run_jobmanager.sh" not in tm_cmd


def test_application_sidecar_command_runs_on_both_roles():
    spec = _build(_app_config(sidecar_command="python -m svc.agent --port 4400"))
    for task in (JOBMANAGER_TASK, TASKMANAGER_TASK):
        cmd = spec["tasks"][task]["command"]
        assert "36_start_sidecar.sh" in cmd
        assert "FLYT_SIDECAR_COMMAND='python -m svc.agent --port 4400'" in cmd
        assert cmd.index("35_cluster_common.sh") < cmd.index("36_start_sidecar.sh")
    assert cmd.index("36_start_sidecar.sh") < cmd.index("42_run_taskmanager.sh")
    without = _build(_app_config())["tasks"][TASKMANAGER_TASK]["command"]
    assert "FLYT_SIDECAR_COMMAND=''" in without


def test_application_taskmanager_script_uses_configured_discovery_timeout():
    tm_cmd = _build(_app_config(discovery_timeout=45))["tasks"][TASKMANAGER_TASK]["command"]
    assert "--timeout 45" in tm_cmd


def test_application_jobmanager_script_honours_restart_completed_jobs():
    jm_restart = _build(_app_config())["tasks"][JOBMANAGER_TASK]["command"]
    jm_batch = _build(_app_config(restart_completed_jobs=False))["tasks"][JOBMANAGER_TASK]["command"]
    assert 'if [ "1" = "1" ]; then' in jm_restart
    assert 'if [ "0" = "1" ]; then' in jm_batch


def test_application_sandbox_unpack_keeps_unpack_fragment_and_drops_layer_paths():
    cfg = _app_config(squashfs_layer_delivery="sandbox_unpack")
    spec = build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py",
        config=cfg,
        operation_params=OperationParams(file_paths=["//tmp/runtime.squashfs", "//tmp/wheel.whl"], pool="pool"),
        jobmanager_params=_jm(),
        secure_vault={},
        use_squashfs_sandbox_unpack=True,
        taskmanager_params=_tm(),
        yt_proxy="proxy.example",
    ).build()
    for task in spec["tasks"].values():
        assert "00_unpack_runtime_squashfs.sh" in task["command"]
        assert "layer_paths" not in task
        assert "tmpfs_path" not in task


def test_flink_dynamic_properties_derive_memory_slots_and_apply_overrides():
    cfg = _app_config(flink_config={"restart-strategy.type": "fixed-delay", "custom.key": "v"})
    props = flink_dynamic_properties(cfg, _jm(), _tm(count=3, slots=2), "2324M", "512M")
    # heap 2324 + off-heap 512 + metaspace 256 = 3092, plus 10% overhead of the total (343)
    assert props["jobmanager.memory.process.size"] == "3435m"
    assert props["jobmanager.memory.off-heap.size"] == "512M"
    assert props["taskmanager.memory.process.size"] == "3072m"  # 75% of 4G
    assert props["taskmanager.memory.task.off-heap.size"] == "384m"  # 1/8 of the TM JVM by default
    assert props["taskmanager.numberOfTaskSlots"] == "2"
    assert props["parallelism.default"] == "6"
    assert props["python.executable"] == "/py"
    assert props["env.java.opts.taskmanager"] == "-XX:ActiveProcessorCount=4 -XX:+ExitOnOutOfMemoryError"
    assert props["env.java.opts.jobmanager"] == "-XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError"
    assert props["taskmanager.jvm-exit-on-oom"] == "true"
    assert props["taskmanager.memory.managed.fraction"] == "0.1"
    assert props["restart-strategy.type"] == "fixed-delay"
    assert props["custom.key"] == "v"


def test_flink_dynamic_properties_use_explicit_parallelism():
    props = flink_dynamic_properties(_app_config(parallelism=3), _jm(), _tm(count=2, slots=2), "2324M", None)
    assert props["parallelism.default"] == "3"


def test_flink_dynamic_properties_without_off_heap():
    props = flink_dynamic_properties(_app_config(), _jm(), _tm(), "2324M", None)
    assert "jobmanager.memory.off-heap.size" not in props
    # heap 2324 + default off-heap 128 + metaspace 256 = 2708, plus overhead 300
    assert props["jobmanager.memory.process.size"] == "3008m"
    assert props["restart-strategy.type"] == "exponential-delay"


def test_flink_dynamic_properties_use_explicit_taskmanager_off_heap():
    tm = TaskmanagerParams(cpu=2, memory=_MICRO.memory_bytes(), count=1, slots=1, off_heap=2 * 1024**3)
    props = flink_dynamic_properties(_app_config(), _jm(), tm, "2324M", None)
    assert props["taskmanager.memory.task.off-heap.size"] == "2048m"


def test_jobmanager_process_size_clamps_overhead():
    assert jobmanager_process_size("1G", None) == "1600m"  # overhead floor 192m
    assert jobmanager_process_size("24G", None) == "25984m"  # overhead cap 1g


def test_minicluster_spec_is_unchanged_by_application_fields():
    cfg = FlytConfig(service_name="svc", java_home="/jdk", python_bin="/py")
    spec = build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py",
        config=cfg,
        operation_params=_op(),
        jobmanager_params=_jm(),
        secure_vault={},
        max_heap_size_str="2324M",
    ).build()
    assert set(spec["tasks"]) == {MINICLUSTER_TASK}
    task = spec["tasks"][MINICLUSTER_TASK]
    assert task["restart_completed_jobs"] is True
    assert "gang_options" not in task
    env = task["environment"]
    assert env["FLINK_ENV_JAVA_OPTS"].startswith("-Xmx2324M")
    assert "FLYT_CLUSTER_MODE" not in env
    assert "FLYT_YT_PROXY" not in env
    assert "35_cluster_common.sh" not in task["command"]
    assert "40_run_job.sh" in task["command"]


def test_minicluster_spec_passes_restart_completed_jobs_false_through():
    cfg = FlytConfig(service_name="svc", restart_completed_jobs=False)
    spec = build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py",
        config=cfg,
        operation_params=_op(),
        jobmanager_params=_jm(),
        secure_vault={},
    ).build()
    assert spec["tasks"][MINICLUSTER_TASK]["restart_completed_jobs"] is False
