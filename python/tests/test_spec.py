"""Tests for Vanilla spec assembly and run-script selection."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import ClusterPreset, JobmanagerParams, OperationParams
from ytsaurus_flyt.spec import _build_run_script, build_vanilla_operation_spec


def _jobmanager_from_preset() -> JobmanagerParams:
    cp = ClusterPreset.MICRO.params
    return JobmanagerParams(
        cpu=cp.cpu,
        memory=cp.memory_bytes(),
    )


def test_build_run_script_squashfs_unpack_includes_unpack_fragment():
    cfg = FlytConfig(python_bin="/usr/bin/python3")
    script = _build_run_script(
        "pipeline.py",
        cfg,
        "svc",
        use_squashfs_sandbox_unpack=True,
    )
    assert "00_unpack_runtime_squashfs.sh" in script
    assert script.index("00_set_essentials.sh") < script.index("00_unpack_runtime_squashfs.sh")
    assert "21_prepare_libs.sh" in script
    assert script.index("01_prepare_squashfs.sh") < script.index("21_prepare_libs.sh")


def test_build_run_script_uses_set_for_job_args():
    cfg = FlytConfig(python_bin="/usr/bin/python3")
    script = _build_run_script(
        "pipeline.py --foo",
        cfg,
        "svc",
        use_squashfs_sandbox_unpack=False,
    )
    assert "set --" in script
    assert "pipeline.py" in script


def test_build_run_script_squashfs_layer_paths_skips_unpack():
    cfg = FlytConfig(python_bin="/usr/bin/python3")
    script = _build_run_script(
        "pipeline.py",
        cfg,
        "svc",
        use_squashfs_sandbox_unpack=False,
    )
    assert "00_unpack_runtime_squashfs.sh" not in script
    assert "01_prepare_squashfs.sh" in script
    assert "21_prepare_libs.sh" in script
    assert script.index("01_prepare_squashfs.sh") < script.index("21_prepare_libs.sh")


def test_build_vanilla_operation_spec_layer_paths_and_tmpfs():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(
        file_paths=["//tmp/wheel.whl"],
        layer_paths=["//home/layers/runtime.squashfs"],
        pool="pool",
    )
    jm = _jobmanager_from_preset()
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        jobmanager_params=jm,
        secure_vault={},
        use_squashfs_sandbox_unpack=False,
    )
    spec = builder.build()
    task = spec["tasks"]["flink"]
    assert task["layer_paths"] == ["//home/layers/runtime.squashfs"]
    assert "//tmp/wheel.whl" in task["file_paths"]
    assert "tmpfs_path" in task


def test_build_vanilla_operation_spec_sandbox_unpack_no_layer_paths_in_task():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(
        file_paths=["//layer.squashfs", "//tool", "//wheel.whl"],
        layer_paths=[],
        pool="pool",
    )
    jm = _jobmanager_from_preset()
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        jobmanager_params=jm,
        secure_vault={},
        use_squashfs_sandbox_unpack=True,
    )
    spec = builder.build()
    task = spec["tasks"]["flink"]
    assert "layer_paths" not in task
    assert task["file_paths"] == ["//layer.squashfs", "//tool", "//wheel.whl"]


def test_build_vanilla_operation_spec_environment_no_hardcoded_flink_paths():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(file_paths=[], pool="pool")
    jm = _jobmanager_from_preset()
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        jobmanager_params=jm,
        secure_vault={},
    )
    spec = builder.build()
    env = spec["tasks"]["flink"]["environment"]
    assert env["JAVA_HOME"] == "/jdk"
    assert "FLINK_HOME" not in env
    assert "FLINK_LIB_DIR" not in env
    assert env["YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB"] == "1"
