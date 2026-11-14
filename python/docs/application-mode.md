# Application mode

`cluster_mode: application` runs a pipeline as a Flink application cluster inside one YTsaurus Vanilla operation: a `jobmanager` job that executes the script through Flink's `PythonDriver`, plus `taskmanager_count` TaskManager YT jobs. Use it when a pipeline needs more CPU or memory than one container gives; `minicluster` (the default, one YT job with an in-JVM MiniCluster) stays the simpler choice for small pipelines and Kind clusters. Internals are in [ARCHITECTURE.md](../ARCHITECTURE.md).

flyt is the YTsaurus counterpart of a standalone Flink cluster on Kubernetes in application mode: a fixed number of TaskManagers and one `config.yaml` that flyt generates for every container. Flink options come from the same places in both modes ([flink-configuration.md](flink-configuration.md)).

## Fields and flags

Profile fields; the flag overrides the profile for one `flyt run`.

| Field | Flag | Default | Meaning |
|---|---|---|---|
| `cluster_mode` | `--mode` | `minicluster` | `application` enables everything below |
| `taskmanager_count` | `--taskmanagers` | 1 | TaskManager YT jobs: the cluster capacity, like TaskManager `replicas` on Kubernetes |
| `preset` | `--preset` | `micro`, unless the TaskManager is sized without a preset | size of every TaskManager YT job: its cpu (and task slots) and container memory |
| `taskmanager_cpu` | `--tm-cpu` | | with `taskmanager.memory.process.size`, sizes every TaskManager (and the `minicluster` job) without a preset: its CPU, and task slots unless `taskmanager.numberOfTaskSlots` is set |
| `jobmanager_cpu` | `--jm-cpu` | 2 | CPU of the JobManager container |
| `discovery_timeout` | | 300 | seconds a TaskManager waits for the JobManager before failing, like Flink's `taskmanager.registration.timeout` |
| `sidecar_command` | | | helper started in every JobManager/TaskManager container before Flink, e.g. a metrics agent |
| `job_yt_proxy` | | | YT HTTP proxy as seen from inside the YT jobs; empty = the client proxy, or the cluster's `/hosts` when the client proxy is `localhost`. A proxy without a scheme gets the client's one (`http`/`https`) |
| `max_failed_job_count` | | `max(10, 5 * taskmanager_count)` | failed YT jobs before the operation fails; an explicit value is used as is (minicluster default: 10) |

`flyt validate` resolves the cluster the way `flyt run` does and prints its sizes.

```yaml
cluster_mode: application
taskmanager_count: 3
preset: large        # every TaskManager: 4 cpu (4 slots), 16G container
```

## Sizing

A TaskManager, and the single MiniCluster job in `minicluster` mode, is sized one of two ways, never both:

- a preset (`--preset`, `preset` in the profile; `micro` when nothing else sizes it), to try a pipeline out;
- your own sizing: `taskmanager_cpu` (`--tm-cpu`) and `taskmanager.memory.process.size`, both required, plus any other `taskmanager.memory.*` key.

A preset together with `taskmanager_cpu` or any `taskmanager.memory.*` key is an error. Either way the same size applies in both modes, so a pipeline can be compared between them with one flag: `--taskmanagers 1` gives about the resources of the MiniCluster job, plus a small JobManager.

```bash
flyt run --mode application --taskmanagers 3 --tm-cpu 4 -D taskmanager.memory.process.size=12g pipeline.py
```

| | cpu | Task slots | Container | Flink JVM (`*.memory.process.size`) |
|---|---|---|---|---|
| TaskManager | preset cpu, or `taskmanager_cpu` | = cpu, or `taskmanager.numberOfTaskSlots` | preset memory, or `taskmanager.memory.process.size` | = container |
| JobManager | 2, or `jobmanager_cpu` | | 2560m, or `jobmanager.memory.process.size` | = container |

The preset's `-Xmx`/direct memory apply to the MiniCluster only; a TaskManager's JVM is laid out by Flink's memory model.

A standalone Flink cluster cannot ask YT for more TaskManagers, so the capacity is fixed at submit time: `taskmanager_count * slots`. `parallelism.default` is all of it unless the Flink configuration sets a smaller one. The capacity must cover the highest operator parallelism of the pipeline, including explicit `set_parallelism` calls: an operator that needs more slots waits for them and fails after `slot.request.timeout` (5 minutes by default). This is the same contract as TaskManager `replicas` in a standalone Kubernetes deployment; Flink's native Kubernetes mode adds TaskManagers for the job graph instead, which a fixed-size YT operation cannot do.

## Memory

The YT container is `*.memory.process.size`, as a pod on Kubernetes. Flink's JVM overhead (`*.memory.jvm-overhead.*`, 10% of `process.size`) is the part of it no JVM flag caps: the JVM's native memory and, on YT, everything else in the container (bash, the discovery helper, a sidecar, the PyFlink driver on the JobManager, Python UDF workers above their managed memory budget). flyt keeps `jvm-overhead.min` (and `jvm-overhead.max`, if set) at 512m or above for both roles; a lower value is raised with a warning.

Python UDF workers live inside the TaskManager JVM's `process.size`: PyFlink reserves managed memory for them (`python.fn-execution.memory.managed`). flyt jobs keep state on the heap, so managed memory is only the Python workers' budget: flyt sets `taskmanager.memory.managed.fraction` to 0.1 instead of Flink's 0.4, which leaves the rest to task heap.

Direct memory: unlike the MiniCluster, where it defaults to the heap size, Flink caps a TaskManager's `-XX:MaxDirectMemorySize` at `framework.off-heap` (128m) plus `taskmanager.memory.task.off-heap.size`. Connectors with netty/gRPC buffers (including the YTsaurus connectors) need the latter; flyt sets it to 1/8 of the TaskManager JVM.

Each of flyt's memory keys (`*.memory.process.size`, `taskmanager.memory.task.off-heap.size`, `taskmanager.memory.managed.fraction`) and the JVM overhead (`*.memory.jvm-overhead.*`, above the 512m minimum) can be overridden in the Flink configuration; for a TaskManager only without a preset ([Sizing](#sizing)). Any other `taskmanager.memory.*` key (`flink.size`, `task.heap.size`, `network.*`, ...) means you lay out Flink's memory model yourself: flyt then adds none of its TaskManager memory defaults and requires `taskmanager.memory.process.size`, which sizes the YT container. The same rule holds for `jobmanager.memory.*`.

Every JVM runs with `-XX:+ExitOnOutOfMemoryError`, and TaskManagers with `taskmanager.jvm-exit-on-oom`, so an out-of-memory TaskManager dies and is restarted by YT instead of hanging in GC.

## Flink configuration

The JobManager and every TaskManager start with the `config.yaml` that flyt assembles from its defaults and the user's Flink configuration ([flink-configuration.md](flink-configuration.md)), like a ConfigMap shared by every pod on Kubernetes. Besides the defaults of both modes, application mode adds `taskmanager.numberOfTaskSlots`, `parallelism.default = taskmanager_count * slots` and the sizes from [Sizing](#sizing) and [Memory](#memory); a `parallelism.default` above `taskmanager_count * slots` is refused.

Per-container values are passed on the command line by the run scripts:

| Container | Command-line `-D` |
|---|---|
| JobManager | `jobmanager.rpc.address`, `rest.address` (the YT job's cluster/default address), `flyt.yt.job-id=$YT_JOB_ID` |
| TaskManager | `taskmanager.host`, `taskmanager.resource-id=$YT_JOB_ID`, the discovered `jobmanager.rpc.address`/`jobmanager.rpc.port` |

## Discovery and lifecycle

How TaskManagers find the JobManager and how the gang operation restarts are described in [ARCHITECTURE.md](../ARCHITECTURE.md); what matters when operating a job:

- A TaskManager polls for the JobManager up to `discovery_timeout` (default 300 s, Flink's own registration timeout), then fails and is restarted by YT; every such failure counts toward the operation's failed-job budget (`max_failed_job_count`, by default `max(10, 5 * taskmanager_count)`: about five failures per TaskManager, never fewer than minicluster's 10), so raise both on a pool where scheduling the JobManager takes long; a JobManager that never gets scheduled therefore fails the operation through that budget.
- Flink restarts a failed job in the cluster (`exponential-delay`, Flink's defaults) without an attempt limit, so a job that keeps failing keeps retrying with a growing backoff instead of failing the operation. Set another restart strategy in the Flink configuration or in code (e.g. `restart-strategy.exponential-delay.attempts-before-reset-backoff`).
- Inside the YT jobs the proxy is the client proxy, the cluster's `/hosts` when the client proxy is a loopback address, or `job_yt_proxy`.
- When the driver exits 0 (bounded sources finished, or the Flink job was cancelled or stopped from the Web UI) the operation completes: the JobManager calls `complete_operation` (retried for up to 2 minutes) and exits 0 whatever the answer, and every TaskManager also watches the JobManager job and exits 0 once it is completed, so the operation completes even when YT refuses the call (e.g. the token in `secure_vault` may not manage the operation). flyt never re-runs a finished pipeline, restarting operations is the orchestrator's job. A non-zero exit restarts the whole cluster with a new incarnation.
- The Flink Web UI listens on port 27050 of the JobManager YT job: `flyt ui --wait` prints the URL.

## Kind and other port-forwarded clusters

Jobs reach YT from inside the cluster, where the client's `localhost:<port>` is not the proxy. flyt detects a loopback client proxy and hands YT jobs the hosts from `GET /hosts`; set `job_yt_proxy` to override. Exec nodes without per-YT-job network namespaces (the `simple` job environment, Kind) report no YT job IPs; the TaskManager then probes the node address and checks `flyt.yt.job-id` in `/jobmanager/config` so it never registers with another operation's JobManager. See [examples/kind/README.md](../examples/kind/README.md) for the resource limits a Kind node needs.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoResourceAvailableException` after 5 minutes | parallelism above `taskmanager_count * slots` | raise `taskmanager_count`, or the TaskManager cpu (`--preset`, or `--tm-cpu` with `taskmanager.memory.process.size`) |
| `OutOfMemoryError: Direct buffer memory` or OOM-killed TaskManagers | the TaskManager JVM is too small for the job | a larger `--preset`, or a larger `taskmanager.memory.process.size` |
| `pthread_create failed (EAGAIN)` | container or node PID limit | see [examples/kind/TROUBLESHOOTING.md](../examples/kind/TROUBLESHOOTING.md) |
| TaskManagers keep restarting with `no JobManager discovered` | jobs cannot reach the YT proxy or the JobManager REST port | check `job_yt_proxy` and the JobManager stderr |
