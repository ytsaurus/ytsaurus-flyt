# ytsaurus-flyt

PyFlink on [YTsaurus](https://ytsaurus.tech/) [Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) in application mode.

**Runtime:** a **SquashFS** image with PyFlink + optional extra JARs for `flink/lib`, cached on Cypress by hash. See [ARCHITECTURE.md](ARCHITECTURE.md). `squashfs_layer_delivery` is `layer_paths` or `sandbox_unpack`.

## Install

```bash
pip install ytsaurus-flyt
# dev: cd python && pip install -e .
```

## Typical flow (profiles)

One **profile** is a named config: YT proxy, pool, and `FlytConfig` defaults (including a Cypress base path for cached layers). Several profiles can target the same YTsaurus cluster with different pools or runtimes.

```bash
export YT_TOKEN=...
flyt profile add kind-dev --proxy http://localhost:50005 --pool default
flyt install          # build and upload SquashFS layer for the runtime
flyt run examples/simple_wordcount/pipeline.py
```

- `flyt install` is for SquashFS mode: prepares the remote runtime under your profile paths.
- `flyt run`, `validate`, and `build-layer` use the **active profile** (or `--profile` / `FLYT_PROFILE`). Defaults live in `~/.config/flyt/profiles/<name>.yaml` (see `flyt profile show`).
- `--proxy` / `--pool` on `flyt run` override the profile (or set `FLYT_PROXY` / `YT_PROXY` and `FLYT_POOL` / `YT_POOL` when the profile omits them).

**Auto wheel:** if you omit `--wheel` and `--source-dir`, `flyt run` looks for `pyproject.toml` next to the job script and runs `pip wheel` into a temp directory.

**`--profile` / `FLYT_PROFILE`:** choose a profile when the active one is not what you want (e.g. in CI).

## Other commands

- `flyt profile import <file> --as <name> [--proxy URL] [--pool POOL] [--preset PRESET]`: copy a YAML file into `~/.config/flyt/profiles/<name>.yaml` with optional overrides (same defaults as `profile add` when `cypress_base_path` is missing).
- `flyt validate`: check profile, `mksquashfs`, optional YT connectivity.
- `flyt jobshell` / `flyt jobshell --profile <name>`: Attach to the sandbox of the running `flyt run` job for that profile. Needs `tornado` (dependency) for the interactive shell.
- `flyt build-layer`: build `.squashfs` locally; `--upload` pushes to Cypress (same layout as `flyt install`).

Commands that use profiles print `Profile: '...'` (or `Profile: (none)`) at startup.

```bash
flyt build-layer --output runtime.squashfs
```

- **`--force-rebuild`** on `flyt run`: ignore cached SquashFS on Cypress and rebuild the runtime layer.

If `squashfs_layer_cache_prefix` / `squashfs_tools_cache_prefix` are unset, defaults under `//tmp/` on Cypress may be garbage-collected. New profiles default `cypress_base_path` to `//home/flyt/clusters/<profile_name>/` so per-cluster caches (`layers/`, `tools/`) sit under **clusters**. Put shared files (e.g. `flink-connector-ytsaurus.jar`) under **`//home/flyt/libraries/`** and point `jar_scan_folder` there.

**Flink `lib/` JARs:** filenames follow ``<basename>-<version>.jar``. List basenames under **`jar_scan_folder`**. **`embed_squashfs_layer_jar_basenames`**: pack into the SquashFS layer. **`runtime_jar_basenames`**: add as operation `file_paths` (staged to `flink/lib`). Do not list the same basename in both.

## Minimal profile YAML (SquashFS)

`flyt profile add` writes a starter file; extend it with any `FlytConfig` field, for example:

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

Default `squashfs_layer_delivery` from `flyt profile add` is **`layer_paths`** (Cypress mounts the SquashFS on exec nodes). For **Kind** local dev, set **`sandbox_unpack`** in the profile instead (ship `.squashfs` as `file_paths` and unpack in the job sandbox); see [examples/kind/README.md](examples/kind/README.md).

## API

`FlytConfig`, `FlytConfig.from_yaml()` (for tests or custom tooling), `launch_vanilla_job()`, `ClusterPreset`. See the package and [ARCHITECTURE.md](ARCHITECTURE.md).

## Credentials

- `YT_TOKEN` / `FLYT_YT_TOKEN`.
- Local Kind ([examples/kind/README.md](examples/kind/README.md)): default UI password is also valid as `YT_TOKEN` (`password` in the demo); set `YT_USER=admin` when required.
- Do not set `YT_TOKEN` to an empty string: that blocks the Python client from using `~/.yt/token` (the `yt` CLI would still work). Unset the variable or use a real token.
- Optional: `FLYT_SECURE_<KEY>` for extra secure_vault keys (e.g. `FLYT_SECURE_MY_SECRET`).
- Programmatic use: pass `extra_secrets={...}` to `get_secure_credentials()` or `launch_vanilla_job()` when integrating from another layer that resolves secrets outside env vars.

## Presets

| Preset | CPU | RAM | Heap |
|--------|-----|-----|------|
| MICRO | 2 | 4G | 2324M |
| SMALL | 2 | 8G | 4648M |
| LARGE | 4 | 16G | 9296M |
| XLARGE | 8 | 32G | 24G |

## License

Apache-2.0
