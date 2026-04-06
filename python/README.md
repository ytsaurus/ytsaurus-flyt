# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) in application mode.

Runtime is a SquashFS image (PyFlink + optional JARs for `flink/lib`), cached on Cypress. See [ARCHITECTURE.md](ARCHITECTURE.md). `squashfs_layer_delivery` is `layer_paths` or `sandbox_unpack`.

## Install

```bash
pip install ytsaurus-flyt
# dev: cd python && pip install -e .
```

## Typical flow

A profile stores proxy, pool, and `FlytConfig` defaults (under `~/.config/flyt/profiles/<name>.yaml`). You can keep several profiles for the same cluster.

```bash
export YT_TOKEN=...
flyt profile add kind-dev --proxy http://localhost:50005 --pool default
flyt install
flyt run examples/simple_wordcount/pipeline.py
```

- `flyt install` uploads the SquashFS runtime for the active profile.
- `flyt run`, `validate`, and `build-layer` use the active profile unless you pass `--profile` or set `FLYT_PROFILE`. For proxy/pool, precedence is: `--proxy` / `--pool`, then `FLYT_PROXY` / `YT_PROXY` / `FLYT_POOL` / `YT_POOL`, then the profile.
- Without `--wheel` / `--source-dir`, `flyt run` finds `pyproject.toml` next to the script and builds a wheel in a temp dir. The script path you pass is rewritten relative to that project (or `--source-dir`) so it matches the wheel layout.
- With `--wheel` only, pass the script path as it appears inside the unpacked wheel (e.g. `pipeline.py`).

Other commands: `flyt profile import`, `flyt validate` (profile, `mksquashfs`, `unzip`, optional connectivity check), `flyt jobshell` (needs `tornado`), `flyt build-layer` (local `.squashfs`, optional `--upload`). See `flyt <cmd> --help` for flags.

```bash
flyt build-layer --output runtime.squashfs
```

`--force-rebuild` on `flyt run` ignores a cached SquashFS on Cypress and rebuilds the layer.

If `squashfs_layer_cache_prefix` / `squashfs_tools_cache_prefix` are unset, defaults under `//tmp/` on Cypress may be GC’d. New profiles default `cypress_base_path` to `//home/flyt/clusters/<profile_name>/`. Put shared JARs under something like `//home/flyt/libraries/` and set `jar_scan_folder`.

JAR filenames follow `<basename>-<version>.jar`. List basenames in `jar_scan_folder`; use `embed_squashfs_layer_jar_basenames` for the layer and `runtime_jar_basenames` for `file_paths` into `flink/lib` only. Do not put the same basename in both lists.

## Minimal profile YAML (SquashFS)

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

Default `squashfs_layer_delivery` from `flyt profile add` is `layer_paths`. For Kind local dev, use `sandbox_unpack` (see [examples/kind/README.md](examples/kind/README.md)).

## API

`FlytConfig`, `FlytConfig.from_yaml()`, `launch_vanilla_job()`, `ClusterPreset` — see the package and [ARCHITECTURE.md](ARCHITECTURE.md).

## Credentials

- `YT_TOKEN` / `FLYT_YT_TOKEN`.
- Kind demo ([examples/kind/README.md](examples/kind/README.md)): UI password works as `YT_TOKEN`; set `YT_USER=admin` if needed.
- Do not set `YT_TOKEN` to an empty string (it blocks the YTsaurus Python client from using the default token file at `~/.yt/token`). Unset or use a real token.
- Optional `FLYT_SECURE_<KEY>` for extra secure_vault keys.
- Programmatic: `extra_secrets` on `get_secure_credentials` / `launch_vanilla_job` if secrets come from outside env.

## Presets

| Preset | CPU | RAM | Heap |
|--------|-----|-----|------|
| MICRO | 2 | 4G | 2324M |
| SMALL | 2 | 8G | 4648M |
| LARGE | 4 | 16G | 9296M |
| XLARGE | 8 | 32G | 24G |

## License

Apache-2.0
