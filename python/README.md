# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla): one operation per pipeline, either as an in-JVM MiniCluster or as a Flink application cluster with separate TaskManagers.
See [docs/application-mode.md](docs/application-mode.md) for the application cluster, [docs/flink-configuration.md](docs/flink-configuration.md) for Flink options and [ARCHITECTURE.md](ARCHITECTURE.md) for internals.

## Install

```bash
pip install ytsaurus-flyt
```

## Supported Flink Versions

flyt is tested with Apache Flink `1.20.X` and needs `1.19` or later. The version is the `apache-flink` package in `runtime_python_packages`, or the Flink inside a pre-built layer.

## Quick start

```bash
export YT_TOKEN=...
flyt profile add kind-dev --proxy http://localhost:50005
flyt install
flyt run examples/simple_wordcount/pipeline.py
```

## Profiles

A profile stores proxy, pool, and `FlytConfig` defaults under `~/.config/flyt/profiles/<name>.yaml`.
You can keep several profiles for different clusters or environments.

```bash
flyt profile add <name> --proxy <url>
flyt profile import <file.yaml>
```

Commands use the active profile unless you pass `--profile` or set `FLYT_PROFILE`.
For proxy/pool, precedence: CLI flags > env vars (`FLYT_PROXY` / `YT_PROXY` / `FLYT_POOL` / `YT_POOL`) > profile.

### Example YAML

`flyt profile add` writes a starter; extend with any `FlytConfig` field:

```yaml
proxy: http://localhost:50005
pool: default
cypress_base_path: //home/flyt/clusters/my-dev
service_name: "my_service"
squashfs_layer_delivery: layer_paths
runtime_python_packages: ["apache-flink==1.20.1"]
runtime_python_version: "3.8"
java_home: "/usr/lib/jvm/java-11-openjdk-amd64"
python_bin: "/usr/bin/python3"
# application mode: a JobManager plus TaskManager YT jobs (default: minicluster, one YT job)
cluster_mode: application
flink_config_override:
  flyt.taskmanager.replicas: "2"            # TaskManager YT jobs
  flyt.taskmanager.cpu.cores: "1"           # cpu (and task slots) of each TaskManager
  taskmanager.memory.process.size: 2g       # container of each TaskManager
```

`runtime_python_version` must match the Python ABI of `python_bin` on exec nodes.

Default `squashfs_layer_delivery` is `layer_paths`. For Kind local dev, use `sandbox_unpack` (see [examples/kind/README.md](examples/kind/README.md)).

## Cluster modes

`cluster_mode` in the profile (or `flyt run --mode`) picks the topology of the Vanilla operation:

| Mode | Operation layout | Use it for |
|---|---|---|
| `minicluster` (default) | One `flink_minicluster` YT job runs the script; PyFlink starts JobManager and TaskManager in the same JVM. | Small pipelines, local Kind clusters. |
| `application` | A `jobmanager` job runs the script through Flink's `PythonDriver` plus `flyt.taskmanager.replicas` TaskManager YT jobs. | Pipelines that need more than one container of CPU/RAM. |

In application mode every TaskManager is sized like the MiniCluster job. Sizing, options, lifecycle and troubleshooting: [docs/application-mode.md](docs/application-mode.md).

Conceptually, application mode is a standalone Flink cluster on Kubernetes in application mode, and flyt takes its approaches from there: a fixed number of TaskManagers, one generated `config.yaml` for every container, the container sized by `*.memory.process.size`, and the cluster's resources as Flink options (`flyt.*`, like `kubernetes.*`).

## Commands

| Command | Description |
|---------|-------------|
| `flyt run <script>` | Build wheel, upload runtime, submit [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) job |
| `flyt install` | Upload SquashFS runtime for the active profile |
| `flyt validate` | Check profile, tools (`mksquashfs`, `unzip`), optional connectivity |
| `flyt build-layer` | Build local `.squashfs`; `--upload` to push to Cypress |
| `flyt jobshell` | Interactive shell into a running job (needs `tornado`) |
| `flyt ui` | Print the Flink Web UI URL of a running flyt operation (`--wait`, `--open`) |

Run `flyt <cmd> --help` for all flags.

### `flyt run` details

Without `--wheel` / `--source-dir`, `flyt run` finds `pyproject.toml` next to the script, builds a wheel in a temp dir, and rewrites the script path to match the wheel layout.
With `--wheel` only, pass the script path as it appears inside the unpacked wheel (e.g. `pipeline.py`).

`--force-rebuild` ignores a cached SquashFS on Cypress and rebuilds the layer.

`--mode application --taskmanagers N` runs the pipeline as an application cluster of N TaskManagers ([docs/application-mode.md](docs/application-mode.md)).

`--flink-conf FILE` and `-D KEY=VALUE` pass Flink options for this run, in both modes; with the profile's `base_flink_conf_path` and `flink_config_override` they make up the job's `config.yaml` ([docs/flink-configuration.md](docs/flink-configuration.md)).

`-d` / `--detach` submits the operation, waits until it materializes, prints the tracking link and exits. Use `flyt ui --wait` afterwards to find the Flink Web UI. Add `--cache-wheel` to reuse the uploaded wheel across runs (needs `wheel_cache_prefix` or `cypress_base_path`).

## JARs

JAR filenames follow `<basename>-<version>.jar`. Place them under `jar_scan_folder`; resolution picks the latest semver per basename.

| Profile field | Where the JAR ends up |
|---|---|
| `embed_squashfs_layer_jar_basenames` | Baked into the SquashFS layer |
| `runtime_jar_basenames` | Staged as [`file_paths`](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#user-script-options) into `flink/lib` at runtime |

Do not put the same basename in both lists.

## Credentials

- `YT_TOKEN` / `FLYT_YT_TOKEN`.
- Kind demo ([examples/kind/README.md](examples/kind/README.md)): UI password works as `YT_TOKEN`; set `YT_USER=admin` if needed.
- Do not set `YT_TOKEN` to an empty string — it blocks the YTsaurus client from using `~/.yt/token`. Unset or use a real token.
- Optional `FLYT_SECURE_<KEY>` for extra [`secure_vault`](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#general-options-for-all-operation-types) keys.
- Programmatic: `extra_secrets` on `get_secure_credentials` / `launch_vanilla_job` if secrets come from outside env.

## Sizing

The Flink configuration sizes the MiniCluster job, or every TaskManager in application mode: `flyt.taskmanager.cpu.cores` (default 2) and `taskmanager.memory.process.size` (default `4g`), which is also the YT container. See [Sizing](docs/flink-configuration.md#sizing).

## API

`FlytConfig`, `FlytConfig.from_yaml()`, `launch_vanilla_job()` — see the package and [ARCHITECTURE.md](ARCHITECTURE.md).

## Development

This repo uses [uv](https://docs.astral.sh/uv/):

```bash
cd python
uv sync --extra dev
uv run pytest
uv run ruff check src tests
```

## License

Apache-2.0
