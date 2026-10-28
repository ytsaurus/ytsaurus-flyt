# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla): one operation per pipeline, either as an in-JVM MiniCluster or as a Flink application cluster with separate TaskManagers.
See [docs/application-mode.md](docs/application-mode.md) for the application cluster and [ARCHITECTURE.md](ARCHITECTURE.md) for internals.

## Install

```bash
pip install ytsaurus-flyt
```

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
```

`runtime_python_version` must match the Python ABI of `python_bin` on exec nodes.

Default `squashfs_layer_delivery` is `layer_paths`. For Kind local dev, use `sandbox_unpack` (see [examples/kind/README.md](examples/kind/README.md)).

## Cluster modes

`cluster_mode` in the profile (or `flyt run --mode`) picks the topology of the Vanilla operation:

| Mode | Operation layout | Use it for |
|---|---|---|
| `minicluster` (default) | One `flink_minicluster` YT job runs the script; PyFlink starts JobManager and TaskManager in the same JVM. | Small pipelines, local Kind clusters. |
| `application` | A `jobmanager` job runs the script through Flink's `PythonDriver` plus `taskmanager_count` TaskManager YT jobs. | Pipelines that need more than one container of CPU/RAM. |

Application mode in a profile:

```yaml
cluster_mode: application
taskmanager_count: 3          # --taskmanagers; capacity, like TaskManager replicas on Kubernetes
preset: large                 # --preset; every TaskManager: 4 cpu = 4 task slots, 16G container
```

The preset that sizes the MiniCluster job sizes every TaskManager in application mode (its cpu is also the slot count, its memory the container); the JobManager gets 2 cpu and a 2g JVM. The cluster must hold the pipeline's highest operator parallelism in `taskmanager_count * slots`. Sizing, lifecycle and troubleshooting: [docs/application-mode.md](docs/application-mode.md).

## Flink options

flyt has no setting for Flink options yet, in either mode: the pipeline sets them in code (`StreamExecutionEnvironment.get_execution_environment(config)`, `t_env.get_config().set(...)`). In `minicluster` this includes the Web UI that `flyt ui` and port-forwards look for, `rest.port` 27050 and `rest.bind-address` 0.0.0.0 (see [examples/simple_wordcount/pipeline.py](examples/simple_wordcount/pipeline.py)). In `application` mode the same code configures the Flink job (parallelism, checkpointing, restart strategy, `pipeline.*`, `table.*`); the cluster itself (memory, slots, ports, JVM flags) is configured by flyt ([docs/application-mode.md](docs/application-mode.md#flink-configuration)).

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

`--mode application --taskmanagers N [--jm-cpu C --tm-cpu C]` runs the pipeline as an application cluster ([docs/application-mode.md](docs/application-mode.md)); flags override the profile for this run, `--preset` sizes every TaskManager.

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

## Presets

`minicluster`: the one YT job's CPU, container memory and `-Xmx`. `application`: every TaskManager's CPU (= task slots) and container memory, the heap columns do not apply; the JobManager has fixed defaults ([docs/application-mode.md](docs/application-mode.md#sizing)).

| Preset | CPU | RAM | Heap |
|--------|-----|-----|------|
| MICRO | 2 | 4G | 2324M |
| SMALL | 2 | 8G | 4648M |
| LARGE | 4 | 16G | 9296M |
| XLARGE | 8 | 32G | 24G |

## API

`FlytConfig`, `FlytConfig.from_yaml()`, `launch_vanilla_job()`, `MiniClusterPreset` — see the package and [ARCHITECTURE.md](ARCHITECTURE.md).

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
