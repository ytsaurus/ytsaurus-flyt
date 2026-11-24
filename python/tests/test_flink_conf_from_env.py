"""The Flink options of a run (FLYT_FLINK_CONF) in a job: config.yaml rendered again, and the tasks they size."""

import json
import subprocess
import sys

import pytest
import yaml
from yt.wrapper import YtClient

from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.models import OperationParams, parse_memory
from ytsaurus_flyt.spec import TASK_RESOURCE_OPTIONS, _build_run_script, build_vanilla_operation_spec, flink_cluster
from ytsaurus_flyt.sql_job import flink_conf_tool

_PROFILE = {"pipeline.name": "from profile", "taskmanager.numberOfTaskSlots": "3"}
_RUN = {
    "pipeline.name": 'q: "1"',
    "execution.checkpointing": {"unaligned": {"enabled": True}},
    "taskmanager.memory.process.size": "8g",
    "flyt.taskmanager.replicas": 3,
    "jobmanager.memory.process.size": "3g",
}
_MERGED = {
    **_PROFILE,
    "pipeline.name": 'q: "1"',
    "execution.checkpointing.unaligned.enabled": "true",
    "taskmanager.memory.process.size": "8g",
    "flyt.taskmanager.replicas": "3",
    "jobmanager.memory.process.size": "3g",
}


def _render(tmp_path, mode, run_options):
    tool, conf = tmp_path / "flyt_flink_conf.zip", tmp_path / "config.yaml"
    tool.write_bytes(flink_conf_tool())
    conf.write_text("stale: 'true'\n", encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(tool), str(conf), "FLYT_FLINK_CONF", mode, "/py", json.dumps(_PROFILE)],
        env={"FLYT_FLINK_CONF": json.dumps(run_options)},
        capture_output=True,
        text=True,
    )
    return proc, yaml.safe_load(conf.read_text(encoding="utf-8"))


@pytest.mark.parametrize("mode", ["minicluster", "application"])
def test_job_renders_the_configuration_of_the_merged_options_as_flyt_run_does(tmp_path, mode):
    proc, rendered = _render(tmp_path, mode, _RUN)
    assert proc.returncode == 0, proc.stderr
    assert rendered == flink_cluster(FlytConfig(cluster_mode=mode, python_bin="/py"), _MERGED).flink_config
    # what follows from the resized TaskManager is derived again
    assert rendered["taskmanager.memory.task.off-heap.size"] == "1024m"
    if mode == "minicluster":
        assert "flyt.taskmanager.replicas has no effect in minicluster mode" in proc.stderr


def test_job_fails_on_an_invalid_configuration_of_the_run(tmp_path):
    proc, rendered = _render(tmp_path, "application", {"parallelism.default": 100})
    assert proc.returncode != 0
    assert "Flink configuration: parallelism.default 100 does not fit" in proc.stderr
    assert rendered == {"stale": "true"}


@pytest.mark.parametrize("mode", ["minicluster", "application"])
def test_every_job_renders_before_flink_reads_the_configuration(mode):
    config = FlytConfig(cluster_mode=mode)
    roles = ["jobmanager", "taskmanager"] if mode == "application" else [None]
    for role in roles:
        cmd = _build_run_script("job.py", config, "svc", role=role, flink_config={}, flink_options=_PROFILE)
        call = (
            '"$PYTHON_BIN" "$ROOT_DIR/flyt_flink_conf.zip" "$FLINK_CONF_DIR/config.yaml" FLYT_FLINK_CONF '
            f"{mode} \"$PYTHON_BIN\" '{json.dumps(_PROFILE, sort_keys=True)}'"
        )
        last = "minicluster/00_taskmanager_memory.sh" if role is None else "application/01_cluster_common.sh"
        assert 'if [ -n "${FLYT_FLINK_CONF:-}" ]; then' in cmd
        assert cmd.index(call) < cmd.index(last)


@pytest.mark.parametrize("mode", ["minicluster", "application"])
@pytest.mark.parametrize(
    "options",
    [
        {},
        {"taskmanager.numberOfTaskSlots": "6"},
        {
            "flyt.taskmanager.cpu.cores": "5",
            "taskmanager.numberOfTaskSlots": "6",
            "taskmanager.memory.process.size": "8g",
            "flyt.taskmanager.replicas": "4",
            "flyt.jobmanager.cpu.cores": "3",
            "jobmanager.memory.process.size": "3072m",
        },
    ],
)
def test_resource_options_size_the_tasks_as_the_spec_does(mode, options):
    config = FlytConfig(cluster_mode=mode, service_name="svc", flink_config_override=options)
    default = FlytConfig(cluster_mode=mode, service_name="svc")

    def tasks(cfg):
        return build_vanilla_operation_spec(
            title="t",
            job_command="job.py",
            config=cfg,
            operation_params=OperationParams(file_paths=[], layer_paths=[], pool="pool"),
            secure_vault={},
            cluster=flink_cluster(cfg, cfg.flink_config_override),
            yt_client=YtClient(proxy="proxy.example:80", token="t"),
        ).build()["tasks"]

    # a launcher's side: the template of the profile's defaults, resized by the table
    resized = {
        name: {f: task[f] for f in ("cpu_limit", "memory_limit", "job_count")} for name, task in tasks(default).items()
    }
    for task, fields in TASK_RESOURCE_OPTIONS[mode].items():
        for field, keys in fields.items():
            value = next((options[key] for key in keys if key in options), None)
            if value is not None:
                resized[task][field] = parse_memory(value) if field == "memory_limit" else int(value)

    assert set(TASK_RESOURCE_OPTIONS[mode]) == set(resized)
    for name, task in tasks(config).items():
        assert {f: task[f] for f in resized[name]} == resized[name]
