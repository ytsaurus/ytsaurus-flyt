"""Vanilla spec assembly for ``cluster_mode: application`` (and MiniCluster parity)."""

import shutil
import subprocess

import pytest

from ytsaurus_flyt.application import application_cluster
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import MiniClusterPreset, OperationParams
from ytsaurus_flyt.spec import (
    JOBMANAGER_TASK,
    MINICLUSTER_TASK,
    TASKMANAGER_TASK,
    build_vanilla_operation_spec,
)

_MICRO = MiniClusterPreset.MICRO.params


def _op() -> OperationParams:
    return OperationParams(file_paths=["//tmp/wheel.whl"], layer_paths=["//home/layers/runtime.squashfs"], pool="pool")


def _app_config(**overrides) -> FlytConfig:
    base = dict(
        service_name="svc",
        java_home="/jdk",
        python_bin="/py",
        cluster_mode="application",
        taskmanager_count=2,
        taskmanager_cpu=4,
    )
    base.update(overrides)
    base.setdefault("flink_config_override", {"taskmanager.numberOfTaskSlots": 2})
    return FlytConfig(**base)


def _build(config: FlytConfig, conf_file=None, **kwargs):
    kwargs.setdefault("application_cluster", application_cluster(config, conf_file))
    kwargs.setdefault("yt_proxy", "http://proxy.example:80")
    return build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py --x 1",
        config=config,
        operation_params=_op(),
        secure_vault={},
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
    assert tm["memory_limit"] == _MICRO.memory_bytes() + 512 * 1024**2  # JVM + container overhead
    # exit 0 of the driver completes the operation from inside the JobManager job, YT restarts nothing
    assert "restart_completed_jobs" not in jm
    assert "restart_completed_jobs" not in tm
    for task in (jm, tm):
        assert task["layer_paths"] == ["//home/layers/runtime.squashfs"]
        assert "//tmp/wheel.whl" in task["file_paths"]
        assert task["tmpfs_path"] == "."
        env = task["environment"]
        assert env["FLYT_YT_PROXY"] == "http://proxy.example:80"
        assert "FLYT_DISCOVERY_PREFIX" not in env
        # memory goes through Flink's memory model, never -Xmx
        assert "FLINK_ENV_JAVA_OPTS" not in env


def test_application_spec_requires_the_cluster_and_proxy():
    with pytest.raises(ValueError, match="application_cluster is required"):
        _build(_app_config(), application_cluster=None)
    with pytest.raises(ValueError, match="yt_proxy"):
        _build(_app_config(), yt_proxy=None)


def test_application_run_scripts_launch_standalone_job_and_taskmanager():
    spec = _build(_app_config())
    jm_cmd = spec["tasks"][JOBMANAGER_TASK]["command"]
    tm_cmd = spec["tasks"][TASKMANAGER_TASK]["command"]
    for cmd in (jm_cmd, tm_cmd):
        assert "application/00_prepare_cluster_bins.sh" in cmd
        assert "application/01_cluster_common.sh" in cmd
        assert "common/04_prepare_service.sh" in cmd
        assert "minicluster/run.sh" not in cmd
        assert "FLYT_JOB_HELPER_EOF" in cmd
        assert "def cmd_wait_jobmanager" in cmd
        # cluster options live in config.yaml, written by application/01_cluster_common.sh before the role script
        assert "FLYT_FLINK_CONF_EOF" in cmd
        assert "jobmanager.rpc.port: '27051'" in cmd
        assert "parallelism.default: '4'" in cmd
        assert 'rm -f "$FLINK_CONF_DIR/flink-conf.yaml"' in cmd
        assert "-Djobmanager.rpc.port=27051" not in cmd  # TMs still pass the discovered port
        assert "-Dparallelism.default" not in cmd
        assert "FLYT_FLINK_ARGS" not in cmd
    assert 'standalone-job.sh" start-foreground' in jm_cmd
    assert "--job-classname org.apache.flink.client.python.PythonDriver" in jm_cmd
    assert "set -- pipeline.py --x 1" in jm_cmd
    assert 'CLUSTER_IP="${YT_IP_ADDRESS_FASTBONE:-$SLOT_IP}"' in jm_cmd
    assert '"-Djobmanager.rpc.address=$JM_HOST" "-Drest.address=$REST_HOST"' in jm_cmd
    assert '"-Dflyt.yt.job-id=$YT_JOB_ID"' in jm_cmd
    assert '"-Dtaskmanager.host=$CLUSTER_IP"' in tm_cmd
    assert '"-Dtaskmanager.resource-id=$YT_JOB_ID"' in tm_cmd
    assert "publish" not in jm_cmd and "heartbeat" not in jm_cmd
    assert "complete-operation" in jm_cmd
    assert "application/taskmanager.sh" not in jm_cmd
    assert 'taskmanager.sh" start-foreground' in tm_cmd
    assert 'wait-jobmanager --operation-id "$YT_OPERATION_ID"' in tm_cmd
    assert "--rest-port 27050 --timeout 300" in tm_cmd
    assert "application/jobmanager.sh" not in tm_cmd


def test_application_sidecar_command_runs_on_both_roles():
    spec = _build(_app_config(sidecar_command="python -m svc.agent --port 4400"))
    for task in (JOBMANAGER_TASK, TASKMANAGER_TASK):
        cmd = spec["tasks"][task]["command"]
        assert "application/02_start_sidecar.sh" in cmd
        assert "FLYT_SIDECAR_COMMAND='python -m svc.agent --port 4400'" in cmd
        assert cmd.index("application/01_cluster_common.sh") < cmd.index("application/02_start_sidecar.sh")
    assert cmd.index("application/02_start_sidecar.sh") < cmd.index("application/taskmanager.sh")
    without = _build(_app_config())["tasks"][TASKMANAGER_TASK]["command"]
    assert "FLYT_SIDECAR_COMMAND=''" in without


def test_application_taskmanager_script_uses_configured_discovery_timeout():
    tm_cmd = _build(_app_config(discovery_timeout=45))["tasks"][TASKMANAGER_TASK]["command"]
    assert "--timeout 45" in tm_cmd


def test_application_jobmanager_script_completes_the_operation_after_exit_zero():
    jm_cmd = _build(_app_config())["tasks"][JOBMANAGER_TASK]["command"]
    assert 'complete-operation --operation-id "$YT_OPERATION_ID"' in jm_cmd
    assert "while true" not in jm_cmd
    assert "re-running the pipeline" not in jm_cmd


def test_application_sandbox_unpack_keeps_unpack_fragment_and_drops_layer_paths():
    cfg = _app_config(squashfs_layer_delivery="sandbox_unpack")
    spec = build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py",
        config=cfg,
        operation_params=OperationParams(file_paths=["//tmp/runtime.squashfs", "//tmp/wheel.whl"], pool="pool"),
        secure_vault={},
        use_squashfs_sandbox_unpack=True,
        application_cluster=application_cluster(cfg),
        yt_proxy="proxy.example",
    ).build()
    for task in spec["tasks"].values():
        assert "common/01_unpack_runtime_squashfs.sh" in task["command"]
        assert "layer_paths" not in task
        assert "tmpfs_path" not in task


def test_application_spec_renders_conf_file_into_both_roles():
    spec = _build(_app_config(), conf_file={"file.only": "x"})
    for task in (JOBMANAGER_TASK, TASKMANAGER_TASK):
        cmd = spec["tasks"][task]["command"]
        assert "file.only: x" in cmd
        assert cmd.index("FLYT_FLINK_CONF_EOF") < cmd.index("application/02_start_sidecar.sh")


def test_application_spec_sizes_the_jobmanager_from_the_flink_memory():
    cfg = _app_config(jobmanager_cpu=4, flink_config_override={"jobmanager.memory.process.size": "6g"})
    spec = _build(cfg)
    assert spec["tasks"][JOBMANAGER_TASK]["memory_limit"] == (6 * 1024 + 512) * 1024**2  # JVM + 512m for the driver
    assert spec["tasks"][JOBMANAGER_TASK]["cpu_limit"] == 4


def _minicluster_spec(cfg: FlytConfig) -> dict:
    return build_vanilla_operation_spec(
        title="t",
        job_command="pipeline.py",
        config=cfg,
        operation_params=_op(),
        secure_vault={},
        minicluster_params=_MICRO,
    ).build()


def test_minicluster_spec_is_unchanged_by_application_fields():
    base = dict(service_name="svc", java_home="/jdk", python_bin="/py")
    plain = _minicluster_spec(FlytConfig(**base))
    with_app_fields = _minicluster_spec(
        FlytConfig(
            **base,
            taskmanager_count=5,
            jobmanager_cpu=4,
            taskmanager_cpu=8,
            discovery_timeout=60,
            sidecar_command="agent",
            job_yt_proxy="http://jobs-proxy",
        )
    )
    assert with_app_fields == plain
    assert set(plain["tasks"]) == {MINICLUSTER_TASK}
    task = plain["tasks"][MINICLUSTER_TASK]
    assert "restart_completed_jobs" not in task  # exit 0 ends the operation in every mode
    assert "gang_options" not in task
    assert task["environment"]["FLINK_ENV_JAVA_OPTS"].startswith("-Xmx2324M")
    assert "FLYT_YT_PROXY" not in task["environment"]
    assert "application/01_cluster_common.sh" not in task["command"]
    assert "minicluster/run.sh" in task["command"]
    assert "annotations" not in plain


def test_application_scripts_and_annotation_follow_the_user_rest_port():
    spec = _build(_app_config(flink_config_override={"rest.port": "8081"}))
    assert "--rest-port 8081 " in spec["tasks"][TASKMANAGER_TASK]["command"]
    assert ":8081" in spec["tasks"][JOBMANAGER_TASK]["command"]
    assert spec["annotations"] == {"flyt_flink_rest_port": 8081}


def test_application_jobmanager_takes_python_executables_from_the_config():
    jm_cmd = _build(_app_config())["tasks"][JOBMANAGER_TASK]["command"]
    assert "-pyexec" not in jm_cmd and "-pyclientexec" not in jm_cmd
    assert "python.client.executable: /py" in jm_cmd


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("delivery", ["layer_paths", "sandbox_unpack"])
@pytest.mark.parametrize("mode", ["minicluster", "application"])
def test_rendered_run_scripts_are_valid_bash(mode, delivery, tmp_path):
    cfg = _app_config(cluster_mode=mode, squashfs_layer_delivery=delivery, sidecar_command="echo 'a {b}' && sleep 1")
    unpack = delivery == "sandbox_unpack"
    if mode == "application":
        spec = _build(cfg, use_squashfs_sandbox_unpack=unpack)
    else:
        spec = build_vanilla_operation_spec(
            title="t",
            job_command="pipeline.py --x 1",
            config=cfg,
            operation_params=_op(),
            secure_vault={},
            minicluster_params=_MICRO,
            use_squashfs_sandbox_unpack=unpack,
        ).build()
    for name, task in spec["tasks"].items():
        script = tmp_path / f"{name}.sh"
        script.write_text(task["command"])
        result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert result.returncode == 0, f"{name}: {result.stderr}"


def test_minicluster_job_runs_the_script_directly_without_flyt_flink_config():
    cfg = FlytConfig(
        service_name="svc",
        java_home="/jdk",
        python_bin="/py",
        base_flink_conf_path="//home/conf.yaml",
        flink_config_override={"rest.port": "8082", "pipeline.name": "ignored"},
    )
    spec = _minicluster_spec(cfg)
    cmd = spec["tasks"][MINICLUSTER_TASK]["command"]
    assert "FLYT_FLINK_CONF_EOF" not in cmd and "pipeline.name" not in cmd
    assert '"$PYTHON_BIN" "$@" 1>&2' in cmd
    assert "http://${UI_HOST}:27050" in cmd
    assert "Pipeline must set rest.port=27050 and rest.bind-address=0.0.0.0" in cmd
    assert "annotations" not in spec
