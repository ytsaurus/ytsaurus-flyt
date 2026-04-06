# Troubleshooting YTsaurus on Kind

Notes from bringing FLYT up on a single-node Kind cluster.

## Quick triage

From the repo root:

```bash
kubectl get pods -n default
curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:50005/ping
```

This example CR has no query tracker, so there is no `qt-0`. A healthy baseline looks like:

- Pods `hp-0`, `ms-0`, `sch-0`, `ca-0`, `dnd-0`, `tnd-0` are Running
- `http://localhost:50005/ping` returns 200
- `http://localhost:50004/api/cluster-info/minisaurus` responds without hanging

If port-forwards died, restart:

```bash
cd python/examples/kind
./scripts/up.sh
```

## FLYT SquashFS: `cannot import name 'cygrpc'` (grpc)

SquashFS is built with wheels for `runtime_python_version` (e.g. cp310). The job still runs `$PYTHON_BIN` from your profile (`python_bin`), usually `/usr/bin/python3` on the exec pod. That interpreter must be the same major.minor as the layer. Kind images often ship Python 3.8 at `/usr/bin/python3`. If you set `runtime_python_version: "3.10"` but `python_bin` stays 3.8, native modules (grpc, PyFlink bits) break with errors like `cygrpc` import failure.

Fix: either set `runtime_python_version` to match what `python_bin` actually is (often `3.8` on Kind), or install Python 3.10 on the exec image and set `python_bin` to that binary (e.g. `/usr/bin/python3.10`). Rebuild the SquashFS layer after changing `runtime_python_version` (`--force-rebuild` or bump cache).

## UI: ECONNREFUSED, ECONNABORTED, XSRF 503

You may see `Failed to get cluster version`, `ECONNABORTED` in the browser, `connect ECONNREFUSED` to a ClusterIP, or XSRF failing with 503.

The UI pod talks to `http-proxies-lb` via ClusterIP. When `hp-0` is NotReady, Kubernetes drops it from ready endpoints and kube-proxy stops routing to that ClusterIP even if the pod is reachable by other means.

Patch the HTTP LB once (re-apply if the operator reverts it):

```bash
kubectl patch svc http-proxies-lb -n default --type merge -p '{"spec":{"publishNotReadyAddresses":true}}'
```

Use the same hostname style everywhere: open the UI at `http://localhost:50004`, and set `spec.ui.externalProxy` to `localhost:50005` (host:port only, no `http://` prefix; the UI builds paths like `//<externalProxy>/api/...`). Do not mix `localhost` and `127.0.0.1`.

If `hp-0` is wedged:

```bash
kubectl delete pod -n default hp-0
cd python/examples/kind && ./scripts/up.sh
```

## Dynamic tables: no healthy tablet cells in bundle "default"

Mounting `//home/sample` fails, the UI shows no healthy cells, and `//sys/tablet_cell_bundles/default/@health` is `"failed"`. Often `tnd-0` / `dnd-0` still look Running in Kubernetes while YT marks the node offline under `//sys/cluster_nodes/...`, peers sit in `none` or elections, and `//sys/lost_chunks` may grow.

Check:

```bash
export PATH="$PWD/.venv/bin:$PATH"
yt --proxy http://localhost:50005 get '//sys/cluster_nodes/tnd-0.tablet-nodes.default.svc.cluster.local:9022/@state'
yt --proxy http://localhost:50005 get '//sys/cluster_nodes/dnd-0.data-nodes.default.svc.cluster.local:9012/@state'
yt --proxy http://localhost:50005 get //sys/tablet_cell_bundles/default/@health
```

Fast reset:

```bash
kubectl delete pod -n default tnd-0 dnd-0
```

When they are back:

```bash
yt --proxy http://localhost:50005 get //sys/tablet_cell_bundles/default/@health
yt --proxy http://localhost:50005 get //sys/tablet_cell_bundles/sys/@health
```

Both should report `"good"`. Remount if needed:

```bash
yt --proxy http://localhost:50005 mount-table //home/sample
```

## Query tracker / YQL

[manifests/cluster_v1_flyt_local.yaml](manifests/cluster_v1_flyt_local.yaml) does not deploy a query tracker. Do not expect `qt-0`, `yt start-query`, or the YQL editor here. After upgrading from an older CR, stale `qt-*` objects may remain; see `kind_cleanup_removed_query_components` in `scripts/bootstrap.sh`. For YQL you need a fuller cluster spec: [YTsaurus docs](https://ytsaurus.tech/docs/en/).

## Scheduler / controller-agent: missing transaction

Operations fail with `Prerequisite check failed: transaction ... is missing` and stacks mentioning `StartOperation` or controller-agent. Often `sch-0` and/or `ca-0` kept stale state across a master restart.

```bash
kubectl delete pod -n default sch-0 ca-0
```

Then verify:

```bash
export PATH="$PWD/.venv/bin:$PATH"
yt --proxy http://localhost:50005 get //sys/scheduler/orchid/scheduler/service
yt --proxy http://localhost:50005 get //sys/controller_agents/instances/'ca-0.controller-agents.default.svc.cluster.local:9014'/orchid/controller_agent/service
```

`last_connection_time` should be recent.

## Jobs pending, empty Jobs tab

`brief_progress.jobs.pending` stays 1 but nothing runs. Check the exec pod jobs sidecar:

```bash
kubectl logs -n default end-0 -c jobs --tail=80
```

If you see `no network config found in /etc/cni/net.d`, `RunPodSandbox`, or rootfs mount errors, containerd inside the sidecar cannot start sandboxes. That is a host CNI/CRI issue, not FLYT.

Compare CR volumes vs StatefulSet:

```bash
kubectl get ytsaurus minisaurus -o jsonpath='{.spec.execNodes[0].volumes[*].name}'; echo
kubectl get sts end -n default -o jsonpath='{.spec.template.spec.volumes[*].name}'; echo
```

If the CR lists volumes the StatefulSet lacks, the operator build may be too old.

## not recognized as a valid master address

Usually `useShortNames` no longer matches DNS names already stored in cluster state. Pick one naming mode and wipe storage:

```bash
kubectl delete ytsaurus minisaurus
kubectl delete pvc -n default --all
kubectl apply -f manifests/cluster_v1_flyt_local.yaml
cd python/examples/kind && ./scripts/up.sh
```

## Not enough data nodes to write chunk

Default tablet bundle stuck in election, or logs say `Not enough target nodes to write blob chunk`. Common cause: only two data nodes while replication factor is 3 (YTsaurus default). RF=3 needs three distinct data nodes; two nodes cannot place three replicas.

What to do:

1. Use [manifests/cluster_v1_flyt_local.yaml](manifests/cluster_v1_flyt_local.yaml) with three data nodes (`instanceCount: 3`), aligned with [upstream cluster_v1_local.yaml](https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/cluster-config/cluster_v1_local.yaml).
2. On Podman, raise `pids_limit` (see below) and recreate the Kind node if the limit was still ~2048, or data nodes may hit `failed to create thread`.
3. Apply the CR and wait until `dnd-0` through `dnd-2` are Running:

```bash
kubectl apply -f manifests/cluster_v1_flyt_local.yaml
kubectl get pods -n default -l app.kubernetes.io/component=yt-data-node -w
```

Chunk writes and the default bundle should recover after all three nodes are healthy (can take a few minutes).

## ca-0 CrashLoop: failed to create thread

Podman Kind nodes with a low PID limit: `ca-0` loops, logs show `failed to create thread`.

Persistent on Podman (delete cluster):

```ini
[containers]
pids_limit = 16384
```

in `~/.config/containers/containers.conf`, then recreate the Kind node or cluster.

## When in doubt

This blunt reset fixed most partial failures on this setup:

```bash
kubectl delete pod -n default hp-0 sch-0 ca-0 tnd-0 dnd-0
cd python/examples/kind
./scripts/up.sh
```
