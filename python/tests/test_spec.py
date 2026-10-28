"""Tests for Vanilla spec assembly and run-script selection."""

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import ClusterParams, OperationParams
from ytsaurus_flyt.spec import MINICLUSTER_TASK, _build_run_script, build_vanilla_operation_spec


def _minicluster(max_heap: str = "2324M", off_heap=None) -> ClusterParams:
    return ClusterParams(cpu=2, memory="4G", max_heap_size=max_heap, data_size_per_job="0B", off_heap_size=off_heap)


def test_build_run_script_squashfs_unpack_includes_unpack_fragment():
    cfg = FlytConfig(python_bin="/usr/bin/python3")
    script = _build_run_script(
        "pipeline.py",
        cfg,
        "svc",
        use_squashfs_sandbox_unpack=True,
        flink_config={},
    )
    assert "common/01_unpack_runtime_squashfs.sh" in script
    assert script.index("common/00_set_essentials.sh") < script.index("common/01_unpack_runtime_squashfs.sh")
    assert "common/03_prepare_libs.sh" in script
    assert script.index("common/02_prepare_squashfs.sh") < script.index("common/03_prepare_libs.sh")


def test_build_run_script_uses_set_for_job_args():
    cfg = FlytConfig(python_bin="/usr/bin/python3")
    script = _build_run_script(
        "pipeline.py --foo",
        cfg,
        "svc",
        use_squashfs_sandbox_unpack=False,
        flink_config={},
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
        flink_config={},
    )
    assert "common/01_unpack_runtime_squashfs.sh" not in script
    assert "common/02_prepare_squashfs.sh" in script
    assert "common/03_prepare_libs.sh" in script
    assert script.index("common/02_prepare_squashfs.sh") < script.index("common/03_prepare_libs.sh")


def test_build_vanilla_operation_spec_layer_paths_and_tmpfs():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(
        file_paths=["//tmp/wheel.whl"],
        layer_paths=["//home/layers/runtime.squashfs"],
        pool="pool",
    )
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        minicluster_params=_minicluster(),
        secure_vault={},
        use_squashfs_sandbox_unpack=False,
    )
    spec = builder.build()
    task = spec["tasks"][MINICLUSTER_TASK]
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
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        minicluster_params=_minicluster(),
        secure_vault={},
        use_squashfs_sandbox_unpack=True,
    )
    spec = builder.build()
    task = spec["tasks"][MINICLUSTER_TASK]
    assert "layer_paths" not in task
    assert task["file_paths"] == ["//layer.squashfs", "//tool", "//wheel.whl"]


def test_build_vanilla_operation_spec_environment_no_hardcoded_flink_paths():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(file_paths=[], pool="pool")
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        minicluster_params=_minicluster(),
        secure_vault={},
    )
    spec = builder.build()
    env = spec["tasks"][MINICLUSTER_TASK]["environment"]
    assert env["JAVA_HOME"] == "/jdk"
    assert "FLINK_HOME" not in env
    assert "FLINK_LIB_DIR" not in env
    assert env["YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB"] == "1"


def test_build_vanilla_operation_spec_java_opts_heap_only():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(file_paths=[], pool="pool")
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        secure_vault={},
        minicluster_params=_minicluster("14G"),
    )
    env = builder.build()["tasks"][MINICLUSTER_TASK]["environment"]
    # MICRO preset -> cpu=2, so ActiveProcessorCount is pinned to 2.
    assert env["FLINK_ENV_JAVA_OPTS"] == "-Xmx14G -XX:ActiveProcessorCount=2"


def test_build_vanilla_operation_spec_java_opts_with_off_heap():
    config = FlytConfig(service_name="t", java_home="/jdk", python_bin="/py")
    op = OperationParams(file_paths=[], pool="pool")
    builder = build_vanilla_operation_spec(
        title="x",
        job_command="p.py",
        config=config,
        operation_params=op,
        secure_vault={},
        minicluster_params=_minicluster("14G", "8G"),
    )
    env = builder.build()["tasks"][MINICLUSTER_TASK]["environment"]
    assert env["FLINK_ENV_JAVA_OPTS"] == ("-Xmx14G -XX:MaxDirectMemorySize=8G -XX:ActiveProcessorCount=2")
