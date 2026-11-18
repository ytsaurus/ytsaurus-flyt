# Application mode

`cluster_mode: application` runs a pipeline as a Flink application cluster inside one YTsaurus Vanilla operation: a `jobmanager` job that executes the script through Flink's `PythonDriver`, plus `flyt.taskmanager.replicas` TaskManager YT jobs. Use it when a pipeline needs more CPU or memory than one container gives; `minicluster` (the default, one YT job with an in-JVM MiniCluster) stays the simpler choice for small pipelines and Kind clusters. Internals are in [ARCHITECTURE.md](../ARCHITECTURE.md).

An application cluster has a fixed number of TaskManagers and one `config.yaml` that flyt generates for every container. Flink options come from the same places in both modes ([flink-configuration.md](flink-configuration.md)).

## Fields and options

Profile fields; the flag overrides the profile for one `flyt run`.

| Field | Flag | Default | Meaning |
|---|---|---|---|
| `cluster_mode` | `--mode` | `minicluster` | `application` enables everything below |
| `discovery_timeout` | | 300 | seconds a TaskManager waits for the JobManager before failing, like Flink's `taskmanager.registration.timeout` |
| `sidecar_command` | | | helper started in every JobManager/TaskManager container before Flink, e.g. a metrics agent |
| `job_yt_proxy` | | | YT HTTP proxy as seen from inside the YT jobs; empty = the client proxy, or the cluster's `/hosts` for a loopback one ([Kind](#kind-and-other-port-forwarded-clusters)). A proxy without a scheme gets the client's one (`http`/`https`) |
| `max_failed_job_count` | | `max(10, 5 * flyt.taskmanager.replicas)` | failed YT jobs before the operation fails; an explicit value is used as is (minicluster default: 10) |

The cluster's YT resources are Flink options, set like any Flink key: `flyt.taskmanager.replicas` (TaskManager YT jobs; `flyt run --taskmanagers N` for one run), `flyt.taskmanager.cpu.cores`, `flyt.jobmanager.cpu.cores` and the `*.memory.process.size` of each role ([defaults](flink-configuration.md#resources)).

`flyt validate` resolves the cluster the way `flyt run` does and prints its sizes.

```yaml
cluster_mode: application
flink_config_override:
  flyt.taskmanager.replicas: "3"
  flyt.taskmanager.cpu.cores: "4"           # every TaskManager: 4 cpu (4 slots)
  taskmanager.memory.process.size: 16g      # and a 16g container
```

## Capacity

Every TaskManager is sized like the MiniCluster job ([Sizing](flink-configuration.md#sizing)). A standalone Flink cluster cannot ask YT for more TaskManagers, so the capacity is fixed at submit time: `flyt.taskmanager.replicas * slots`. `parallelism.default` is all of it unless the Flink configuration sets a smaller one. The capacity must cover the highest operator parallelism of the pipeline, including explicit `set_parallelism` calls: an operator that needs more slots waits for them and fails after `slot.request.timeout` (5 minutes by default).

## Memory

flyt's memory defaults, and when they apply, are in [Resources](flink-configuration.md#resources). Why they look the way they do:

- The YT container is `*.memory.process.size`, so Flink's JVM overhead holds everything no JVM flag caps: the JVM's native memory and the rest of the container (bash, the discovery helper, a sidecar, the PyFlink driver on the JobManager, Python UDF workers above their managed budget).
- Python UDF workers take managed memory (`python.fn-execution.memory.managed`); flyt jobs keep state on the heap, so managed memory is only their budget.
- Flink caps a TaskManager's direct memory, the MiniCluster job's included, at `framework.off-heap` (128m) plus `task.off-heap.size`, 0 by default in Flink; netty and gRPC buffers of the connectors need more, so flyt sets it to 1/8 of `process.size`.

## Flink configuration

The JobManager and every TaskManager start with the `config.yaml` that flyt assembles ([flink-configuration.md](flink-configuration.md)); per-container values are passed on the command line by the run scripts:

| Container | Command-line `-D` |
|---|---|
| JobManager | `jobmanager.rpc.address`, `rest.address` (the YT job's cluster/default address), `flyt.yt.job-id=$YT_JOB_ID` |
| TaskManager | `taskmanager.host`, `taskmanager.resource-id=$YT_JOB_ID`, the discovered `jobmanager.rpc.address`/`jobmanager.rpc.port` |

## Discovery and lifecycle

How TaskManagers find the JobManager and how the gang operation restarts are described in [ARCHITECTURE.md](../ARCHITECTURE.md); what matters when operating a job:

- A TaskManager polls for the JobManager up to `discovery_timeout`, then fails and is restarted by YT; every such failure counts toward `max_failed_job_count`, so raise both on a pool where scheduling the JobManager takes long; a JobManager that never gets scheduled fails the operation through that budget.
- Flink restarts a failed job in the cluster without an attempt limit ([flyt's defaults](flink-configuration.md#flyts-defaults)), so a job that keeps failing keeps retrying with a growing backoff instead of failing the operation; set another restart strategy to change that.
- When the driver exits 0 (bounded sources finished, or the Flink job was cancelled or stopped through the REST API) the operation completes: the JobManager calls `complete_operation` (retried for up to 2 minutes) and exits 0 whatever the answer, and every TaskManager also watches the JobManager job and exits 0 once it is completed, so the operation completes even when YT refuses the call (e.g. the token in `secure_vault` may not manage the operation). flyt never re-runs a finished pipeline, restarting operations is the orchestrator's job. A non-zero exit restarts the whole cluster with a new incarnation.
- The Flink Web UI listens on port 27050 of the JobManager YT job: `flyt ui --wait` prints the URL.

## Kind and other port-forwarded clusters

Jobs reach YT from inside the cluster, where the client's `localhost:<port>` is not the proxy. flyt detects a loopback client proxy and hands YT jobs the hosts from `GET /hosts`; set `job_yt_proxy` to override. Exec nodes without per-YT-job network namespaces (the `simple` job environment, Kind) report no YT job IPs; the TaskManager then probes the node address and checks `flyt.yt.job-id` in `/jobmanager/config` so it never registers with another operation's JobManager. See [examples/kind/README.md](../examples/kind/README.md) for the resource limits a Kind node needs.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoResourceAvailableException` after 5 minutes | parallelism above `flyt.taskmanager.replicas * slots` | raise `flyt.taskmanager.replicas`, or the slots of a TaskManager ([Resources](flink-configuration.md#resources)) |
| `OutOfMemoryError: Direct buffer memory` or OOM-killed TaskManagers | the TaskManager JVM is too small for the job | a larger `taskmanager.memory.process.size` |
| `pthread_create failed (EAGAIN)` | container or node PID limit | see [examples/kind/TROUBLESHOOTING.md](../examples/kind/TROUBLESHOOTING.md) |
| TaskManagers keep restarting with `no JobManager discovered` | jobs cannot reach the YT proxy or the JobManager REST port | check `job_yt_proxy` and the JobManager stderr |
