# YTsaurus on Kind (FLYT)

A **minimal** YTsaurus cluster on Kind for local FLYT development: [manifests/cluster_v1_flyt_local.yaml](manifests/cluster_v1_flyt_local.yaml) omits a **query tracker** (and other query-oriented components) to keep the footprint small. **YQL / the query UI** are not part of this setup; use the full operator docs if you need them.

Install **Kind**, **kubectl**, and **helm** on the host; see the [official YTsaurus Kind guide](https://ytsaurus.tech/docs/en/overview/try-yt) for background. You must **`kind create cluster`** (or use an existing cluster) and point **`kubectl`** at it; **`up.sh` / `bootstrap.sh` do not create the Kind cluster.**

**Layout**

| Path | Purpose |
|------|---------|
| `scripts/up.sh` | Runs `bootstrap.sh` (cert-manager if missing, operator, cluster CR), then port-forward UI / HTTP / RPC (foreground). |
| `scripts/bootstrap.sh` | Same stack without long-running forwards; then run `scripts/port-forward-ui.sh` separately. |
| `scripts/port-forward-ui.sh` | Local ports only (defaults 50004 / 50005 / 50006). |
| `scripts/ensure-podman-pids-for-kind.sh` | Before `kind create`: raise Podman `pids_limit` as pods spawn a lot of processes. |
| `scripts/kind-common.sh` | Shared helpers (sourced by other scripts). |
| `manifests/` | Cluster CR and Helm values for the operator chart. |
| `sysctl-flyt-kind.conf` | Optional host inotify limits. |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common failures and fixes. |

**Quick path**

1. Prepare the host (Docker or Podman, `kubectl`, `kind`, `helm`). Roughly ~4 CPUs, 8 GiB RAM.
2. On Linux with Podman/Kind you often need higher **inotify** limits on the host (`fs.inotify.max_user_instances` ≥ 512). The script will complain if not. You can install `sysctl-flyt-kind.conf` under `/etc/sysctl.d/` (see `scripts/bootstrap.sh` output or TROUBLESHOOTING).
3. **Create the Kind cluster** (e.g. `kind create cluster --name flyt-local`) and use that context. Optional: run `scripts/ensure-podman-pids-for-kind.sh` before `kind create` when using Podman.
4. From `python/examples/kind` run **`./scripts/up.sh`**. It runs `bootstrap.sh` (operator Helm install, cert-manager if missing, YTsaurus CR), then **port-forward** UI / HTTP / RPC in the foreground.

UI login: **admin** / **password**. For `flyt` and `yt`, use the same HTTP proxy as the forwards (often `localhost:50005`).

**Credentials for CLI:** set **`YT_TOKEN` to the same string as the UI password** (default **`password`**). Set **`YT_USER=admin`** if a tool needs a username (matches the UI login). This is specific to the bundled Kind demo; production clusters use real tokens.

**FLYT example** (from the repo `python/` directory):

```bash
pip install -e .
export YT_TOKEN=password
export YT_USER=admin
flyt profile add kind-dev --proxy http://localhost:50005
```

Edit `~/.config/flyt/profiles/kind-dev.yaml` and set **`squashfs_layer_delivery: sandbox_unpack`** (default `flyt profile add` is **`layer_paths`**, which fits production clusters; Kind needs unpack-in-sandbox).

```bash
flyt install
flyt run "examples/simple_wordcount/pipeline.py"
```

You can also override proxy/pool for a single run: `flyt run ... --proxy http://localhost:50005 --pool default` (and `--wheel` / `--source-dir` - of the job - as needed).

SquashFS, pools, and incident handling: [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
