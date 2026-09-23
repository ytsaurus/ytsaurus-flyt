# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla): one operation per pipeline, either as an in-JVM MiniCluster or as a Flink application cluster with separate TaskManagers.
See [ARCHITECTURE.md](ARCHITECTURE.md) for internals.

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
| `minicluster` (default) | One `flink` job runs the script; PyFlink starts JobManager and TaskManager in the same JVM. | Small pipelines, local Kind clusters. |
| `application` | A `jobmanager` job runs the script through Flink's `PythonDriver` (application mode) plus `taskmanager_count` TaskManager jobs. | Pipelines that need more than one container of CPU/RAM. |

Application mode fields (profile or flags):

```yaml
cluster_mode: application
parallelism: 8                # --parallelism; sizes the cluster: ceil(parallelism / slots) TaskManagers
taskmanager_slots: 2          # --slots
taskmanager_count: 4          # --taskmanagers; optional, must cover parallelism when both are set
taskmanager_preset: small     # --tm-preset; empty = same preset as the JobManager
taskmanager_cpu: 20           # --tm-cpu; overrides the preset's cpu per TaskManager
taskmanager_memory: 24G       # --tm-mem; overrides the preset's memory per TaskManager
taskmanager_off_heap: 2G      # --tm-off-heap; direct memory for connectors (default: preset off_heap, else 1/8 of the TM JVM)
restart_completed_jobs: true  # false: complete the operation when the pipeline finishes (batch)
discovery_timeout: 600        # seconds a TaskManager waits for the JobManager before failing
sidecar_command: ""           # optional helper started in every JM/TM container before Flink (e.g. a metrics agent); ignored in minicluster
flink_config:                 # optional Flink overrides, applied last
  restart-strategy.type: fixed-delay
```

How it works: the JobManager is a [gang](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla#gang-operations) job, so any JobManager failure restarts the whole cluster with a new incarnation. TaskManagers find it through the YT API: they list the running `jobmanager` job of their own operation and incarnation, read its addresses from the job's `exec_attributes`, fetch `/jobmanager/config` from its REST port and connect to the `jobmanager.rpc.address` it advertises (no Cypress registry). A TaskManager that finds no JobManager within `discovery_timeout` fails and is restarted by YT, so a JobManager that never gets scheduled eventually fails the operation through `max_failed_job_count`. Job-to-job traffic uses `YT_IP_ADDRESS_FASTBONE` when the exec node provides it (the default address is filtered between containers on some clusters); the Web UI stays on the default address. A failed TaskManager is restarted by YT on its own and the job recovers through Flink's restart strategy (`exponential-delay` by default). The Web UI stays on port 27050 of the JobManager job.

Memory: the JobManager JVM heap is the preset `max_heap`, the rest of the container is left to the Python driver; TaskManagers give 75% of their container to `taskmanager.memory.process.size` and the rest to Python UDF workers. Unlike the MiniCluster, where direct memory defaults to the heap size, Flink caps a TaskManager's `-XX:MaxDirectMemorySize` at `framework.off-heap` (128m) plus `taskmanager.memory.task.off-heap.size`, so connectors with netty/gRPC buffers need the latter: flyt sets it from `taskmanager_off_heap`, else the preset `off_heap`, else 1/8 of the TaskManager JVM. Override any of it via `flink_config`.

Sizing: a standalone Flink cluster cannot ask YT for more TaskManagers, so the job's parallelism must fit into `taskmanager_count * taskmanager_slots` (otherwise it waits for slots and fails after `slot.request.timeout`). Set `parallelism` and let flyt derive the count, or set both and flyt checks they fit. `parallelism.default` is `parallelism` when set, else `count * slots`; a pipeline's own `set_parallelism` still overrides it.

The MiniCluster script and Flink configuration are untouched by these fields.

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

`--mode application --parallelism N [--slots K --tm-preset P]` (or `--taskmanagers N`) runs the pipeline as an application cluster (see [Cluster modes](#cluster-modes)); flags override the profile for this run.

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

| Preset | CPU | RAM | Heap |
|--------|-----|-----|------|
| MICRO | 2 | 4G | 2324M |
| SMALL | 2 | 8G | 4648M |
| LARGE | 4 | 16G | 9296M |
| XLARGE | 8 | 32G | 24G |

## API

`FlytConfig`, `FlytConfig.from_yaml()`, `launch_vanilla_job()`, `ClusterPreset` — see the package and [ARCHITECTURE.md](ARCHITECTURE.md).

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
