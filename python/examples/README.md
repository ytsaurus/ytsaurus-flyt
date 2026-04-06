# Examples

Run `flyt` from the repo `python/` directory; paths such as `examples/simple_wordcount/pipeline.py` are relative to that root. Install the package first so `flyt` is on `PATH` (`pip install ytsaurus-flyt`, or `pip install -e .` from `python/` when developing).

Then use `flyt profile` / `flyt install` / `flyt run` as in [../README.md](../README.md). That flow (profile + SquashFS runtime on the cluster) is the same for every job below; the table only describes what each pipeline does.

Example [profile.yaml](simple_wordcount/profile.yaml) files under `examples/` default **`squashfs_layer_delivery: sandbox_unpack`** so local and Kind-style runs work without extra edits.

**Production:** Prefer **`layer_paths`** (`squashfs_layer_delivery`) for real clusters: the SquashFS is mounted from Cypress on exec nodes, startup is much faster, and layers are cached on nodes between jobs. **`sandbox_unpack`** ships the archive as job files and unpacks in the sandbox—fine for try-out, not what you want at scale. Set `layer_paths` in the profile you import before running serious workloads (see [../README.md](../README.md)).

| Path | Notes |
|------|--------|
| [simple_wordcount/](simple_wordcount/) | Datagen and print; [profile.yaml](simple_wordcount/profile.yaml) |
| [simple_java_udf/](simple_java_udf/) | PyFlink + Java `ScalarFunction` JAR (Gradle); [profile.yaml](simple_java_udf/profile.yaml) |
| [kind/](kind/) | Kind cluster. See [kind/README.md](kind/README.md) |

With `flyt` on `PATH`, absolute paths to job scripts also work from any cwd.
