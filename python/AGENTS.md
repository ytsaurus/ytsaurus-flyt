# AGENTS.md — `ytsaurus-flyt` (Python launcher)

Guidance for AI agents and human contributors working in the `python/` subproject. This covers the `ytsaurus-flyt` PyPI package only; the Gradle/Java connectors at the repo root are out of scope here.

## What this is

`flyt` is a CLI and library that launches PyFlink jobs as [YTsaurus Vanilla](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/vanilla) operations (application mode). It builds an operation spec, uploads the job wheel + a cached SquashFS runtime + flink/lib JARs to Cypress, and submits via the YTsaurus client. See [ARCHITECTURE.md](ARCHITECTURE.md) for the full picture and [README.md](README.md) for user-facing docs.

## Layout

```
python/
├── src/ytsaurus_flyt/
│   ├── __init__.py        # public API facade — the ONLY supported import surface
│   ├── __main__.py        # Click CLI (`flyt`); tests patch symbols here
│   ├── cli_helpers.py     # CLI-only glue
│   ├── config/            # FlytConfig, models/presets, profiles, validate_config
│   ├── runtime/           # container_runtime, wheel_utils, jar_utils, flink_lib_jars, layer_builder
│   ├── submit/            # yt_client, credentials, spec, launcher
│   ├── tracking/          # ui_tracker, jobshell_resolve
│   └── run_scripts/       # *.sh job bootstrap, concatenated at submit time (package data)
├── tests/                 # pytest; one test_<module>.py per module
├── examples/              # runnable PyFlink jobs + Kind local-cluster setup
├── pyproject.toml         # setuptools build, ruff + pytest config
└── uv.lock                # pinned; CI runs `uv lock --check`
```

Dependency direction is one-way: `config` is the leaf, `runtime`/`submit` build on it, `__main__` sits on top of everything. Don't introduce cycles or make `config` import from `runtime`/`submit`/`tracking`.

> The subpackage is named `runtime/`, not `build/`, on purpose — `build/` is ignored by `.gitignore` (build-artifacts convention), so a package by that name wouldn't commit.

## Dev workflow

This subproject uses [uv](https://docs.astral.sh/uv/). Run everything from `python/`:

```bash
uv sync --extra dev          # install deps (incl. ruff, pytest)
uv run pytest -q             # tests
uv run ruff check src tests examples
uv run ruff format src tests examples   # drop --check to apply
uv build --wheel             # build the package
uv run flyt --help           # exercise the CLI
```

CI (`.github/workflows/flyt-python-build.yml`) runs the lockfile check, ruff `check` **and** `format --check`, pytest, and a wheel build across Python 3.9–3.12. Match it locally before pushing.

## Conventions

- **Target Python 3.9+.** No 3.10+-only syntax (e.g. `match`, `X | Y` in runtime annotations). `from __future__ import annotations` is used where helpful.
- **ruff** is the linter and formatter (line length 120; rules `E`, `F`, `I`, `W`; `E501` ignored). Keep imports sorted (`ruff check --fix` does this).
- **Public API stability.** External users import from the top-level `ytsaurus_flyt` package (see `__all__` in `__init__.py`). Treat those names as a contract — keep them re-exported even when internals move. Deep imports (`ytsaurus_flyt.runtime.layer_builder`, `ytsaurus_flyt.submit.launcher`) are internal and may change.
- **Lazy imports:** `__init__.py.__getattr__` defers modules that pull in the YTsaurus client so `import ytsaurus_flyt` stays cheap. If you add a public symbol from such a module, register it there, not as a top-level import.
- **Tests mirror modules** 1:1 (`test_<module>.py`). Add/extend the matching test file. CLI tests use Click's `CliRunner` and patch symbols on `ytsaurus_flyt.__main__`, so keep CLI dependencies imported into that module's namespace.
- **`run_scripts/*.sh`** ship as package data and are resolved relative to the package root. If you move modules that read them, keep the path anchored to `ytsaurus_flyt/run_scripts` (see `submit/spec.py`).

## Commits & PRs

Conventional Commits with a scope, matching repo history: `fix(python): …`, `feat(python): …`. PRs that touch `python/**` trigger the Python CI workflow. Discuss non-trivial changes in a GitHub issue first (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

## Gotchas

- Don't set `YT_TOKEN=""` — an empty string blocks the YTsaurus client from falling back to `~/.yt/token`. Unset it or use a real token.
- The layer is self-contained: `flyt build layer` bundles a relocatable CPython (`runtime_python_version`, installed with `uv python install` in the `ghcr.io/astral-sh/uv` image) and a Temurin JRE (`java_version`, copied out of the official `eclipse-temurin:<N>-jre` image), so jobs don't use the exec node's Python/JDK. The run script sets `PYTHON_BIN`/`JAVA_HOME` layer-relative. No download-URL scraping — uv and the Temurin image are the sources of truth.
- The SquashFS runtime build needs Docker/Podman on the machine running `flyt`. Host `mksquashfs` is used if present; otherwise the layer is packed inside a container, so `squashfs-tools` is not required on the host. Layer delivery is `layer_paths` (production) or `sandbox_unpack` (Kind/local where porto is unavailable).
- JARs are never baked into the runtime layer — list them in `runtime_jar_basenames` and they ship as operation `file_paths`. The layer is a canonical `flink × python` artifact; build it explicitly with `flyt build layer --upload <//yt/path>` (local build, creds only for `--upload`) and reference it from `squashfs_layer_paths`. There is no `install` command and no implicit/hash-named layer paths — `flyt run` never builds a layer, it only references one.
- **Build-cache containers run as the host uid, not root** (`cache_owner_run_args` in `runtime/container_runtime.py`). pip silently *disables* its cache when the cache dir isn't owned by the running user, and rootless podman / Docker Desktop bind mounts are owned by a non-root uid (uid 1000 inside the podman machine on Windows). Root-in-container can't chown them either — the WSL/virtiofs mount pins ownership. So the wheel-build container runs `--user <uid> -e HOME=/tmp`; don't "simplify" it back to root or caching breaks. (The `mksquashfs` container stays root — it `apk add`s squashfs-tools.)
- **When bundling CPython, copy only `bin/`/`include/`/`lib/`, never `share/`.** python-build-standalone's `share/terminfo` has unix symlinks and case-colliding names (`terminfo/e` vs `terminfo/E/Eterm`) that a Windows-backed bind mount can't create — the `cp` aborts. The CPython stdlib itself is case-safe; `share/` (terminfo, man) isn't needed to run PyFlink. Copies use `cp -RL` (dereference symlinks → real files) for the same Windows-mount reason.
