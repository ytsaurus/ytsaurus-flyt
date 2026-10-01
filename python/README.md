# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) in application mode.
See [ARCHITECTURE.md](ARCHITECTURE.md) for internals.

## Install

```bash
pip install ytsaurus-flyt
```

## Quick start

Three steps: build the runtime layer and upload it once, point a profile at it, then launch jobs.

```bash
export YT_TOKEN=...

# 1. Build the self-contained Flink runtime layer (python + JRE + pyflink) and upload it to YT.
#    Do this once per (flink, python) combo; the path below is the default convention.
flyt build layer --upload //sys/flink/flyt-flink120-py310.squashfs

# 2. Import a profile that references that layer (squashfs_layer_paths must list the path above),
#    or create one with `flyt profile add` and edit it.
flyt profile import examples/simple_wordcount/profile.yaml --as my-cluster

# 3. Launch a PyFlink job. `flyt run` references the layer — it never builds one.
flyt run examples/simple_wordcount/pipeline.py
```

## Profiles

A profile stores proxy, pool, and `FlytConfig` defaults under `~/.config/flyt/profiles/<name>.yaml`.
You can keep several profiles for different clusters or environments.

```bash
flyt profile add <name> --proxy <url>
flyt profile import <file.yaml> --as <name>
```

Commands use the active profile unless you pass `--profile` or set `FLYT_PROFILE`.
For proxy/pool, precedence: CLI flags > env vars (`YT_PROXY` / `YT_POOL`, the standard YTsaurus vars) > profile.

### Example YAML

`flyt profile add` writes a starter; extend with any `FlytConfig` field:

```yaml
proxy: http://localhost:50005
pool: default
service_name: "my_service"
squashfs_layer_delivery: layer_paths
squashfs_layer_paths: ["//sys/flink/flyt-flink120-py310.squashfs"]
flink_version: "1.20.1"
runtime_python_version: "3.10"
java_version: "11"
```

The layer is **self-contained**: `flyt build layer` bundles a relocatable CPython (`runtime_python_version`) and a Temurin JRE (`java_version`) into it, so jobs don't depend on the exec node's Python or JDK.

For cluster quirks you can pass raw `YtClient` config under `yt_client_config` (deep-merged over flyt's defaults). For example, if the proxy advertises internal hosts unreachable from your machine, disable proxy discovery:

```yaml
yt_client_config:
  proxy:
    enable_proxy_discovery: false
```

Default `squashfs_layer_delivery` is `layer_paths`. For Kind local dev, use `sandbox_unpack` (see [examples/kind/README.md](examples/kind/README.md)).

## Commands

| Command | Description |
|---------|-------------|
| `flyt run <script>` | Build the job wheel and submit the [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) job (references a pre-built layer; never builds it) |
| `flyt build layer` | Build the runtime `.squashfs` locally; `--output FILE` and/or `--upload //path` (explicit) |
| `flyt build unsquashfs` | Build a static `unsquashfs` helper for `sandbox_unpack`; `--output`/`--upload` |
| `flyt validate` | Check profile, container runtime / `mksquashfs`, optional connectivity |
| `flyt jobshell` | Attach to a running job's sandbox (needs the `yt` CLI: `pip install ytsaurus-client`) |
| `flyt ui` | Print the Flink Web UI URL of a running flyt operation (`--wait`, `--open`) |

Run `flyt <cmd> --help` for all flags.

### `flyt run` details

Without `--wheel` / `--source-dir`, `flyt run` finds `pyproject.toml` next to the script, builds a wheel in a temp dir, and rewrites the script path to match the wheel layout.
With `--wheel` only, pass the script path as it appears inside the unpacked wheel (e.g. `pipeline.py`).

By default `flyt run` shows a terse step view (credentials → wheel → layer → submit) with the job ID and tracking URL. Pass `-v` / `--verbose` for the full launcher logs, or `--debug` to also turn on yt client debug logging (HTTP requests/responses). yt's own `YT_LOG_LEVEL` / `YT_LOG_PATTERN` / `YT_LOG_PATH` env vars are honored too.

`-d` / `--detach` submits the operation, waits until it materializes, prints the tracking link and exits. Use `flyt ui --wait` afterwards to find the Flink Web UI. Add `--cache-wheel` to reuse the uploaded wheel across runs (needs `wheel_cache_prefix`).

## JARs

JAR filenames follow `<basename>-<version>.jar`. Place them under `jar_scan_folder`; resolution picks the latest semver per basename.

List the basenames you need in `runtime_jar_basenames`. They are resolved from `jar_scan_folder` and staged as operation [`file_paths`](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/operations-options#user-script-options) into `flink/lib` at runtime. JARs are never baked into the runtime layer — the layer holds only the Flink runtime, so it stays a canonical `flink × python` artifact (the same layer is reusable across profiles).

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
