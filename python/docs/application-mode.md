# Application mode

`cluster_mode: application` runs a pipeline as a Flink application cluster inside one YTsaurus Vanilla operation: a `jobmanager` job that executes the script through Flink's `PythonDriver`, plus `taskmanager_count` TaskManager YT jobs. Use it when a pipeline needs more CPU or memory than one container gives; `minicluster` (the default, one YT job with an in-JVM MiniCluster) stays the simpler choice for small pipelines and Kind clusters. Internals are in [ARCHITECTURE.md](../ARCHITECTURE.md).

flyt is the YTsaurus counterpart of a standalone Flink cluster on Kubernetes in application mode: a fixed number of TaskManagers and one `config.yaml` that flyt generates for every container. The pipeline configures its Flink job in code, the same way in both modes (see [Flink options](#flink-options)).

## Fields and flags

Profile fields; the flag overrides the profile for one `flyt run`.

| Field | Flag | Default | Meaning |
|---|---|---|---|
| `cluster_mode` | `--mode` | `minicluster` | `application` enables everything below |
| `taskmanager_count` | `--taskmanagers` | 1 | TaskManager YT jobs: the cluster capacity, like TaskManager `replicas` on Kubernetes |
| `preset` | `--preset` | `micro` | size of every TaskManager YT job: its cpu (and task slots) and container memory |
| `taskmanager_cpu` | `--tm-cpu` | the preset's cpu | CPU, and task slots, of each TaskManager |
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

The same `preset` sizes the single MiniCluster job in `minicluster` mode and every TaskManager in `application` mode, so a pipeline can be compared between the modes with one flag: `--taskmanagers 1` gives about the resources of the MiniCluster job, plus a small JobManager.

| | cpu | Task slots | Container | Flink JVM (`*.memory.process.size`) |
|---|---|---|---|---|
| TaskManager | preset cpu, or `taskmanager_cpu` | = cpu | preset memory | container − 512m (bash, the discovery helper, a sidecar) |
| JobManager | 2, or `jobmanager_cpu` | | 2g + 512m | 2g; the 512m is the PyFlink driver (the script and its imports) next to the JobManager JVM |

The preset's `-Xmx`/direct memory apply to the MiniCluster only; a TaskManager's JVM is laid out by Flink's memory model.

A standalone Flink cluster cannot ask YT for more TaskManagers, so the capacity is fixed at submit time: `taskmanager_count * slots`. `parallelism.default` is all of it. The capacity must cover the highest operator parallelism of the pipeline, including explicit `set_parallelism` calls: an operator that needs more slots waits for them and fails after `slot.request.timeout` (5 minutes by default). This is the same contract as TaskManager `replicas` in a standalone Kubernetes deployment; Flink's native Kubernetes mode adds TaskManagers for the job graph instead, which a fixed-size YT operation cannot do.

## Memory

Python UDF workers live inside the TaskManager JVM's `process.size`: PyFlink reserves managed memory for them (`python.fn-execution.memory.managed`). flyt jobs keep state on the heap, so managed memory is only the Python workers' budget: flyt sets `taskmanager.memory.managed.fraction` to 0.1 instead of Flink's 0.4, which leaves the rest to task heap.

Direct memory: unlike the MiniCluster, where it defaults to the heap size, Flink caps a TaskManager's `-XX:MaxDirectMemorySize` at `framework.off-heap` (128m) plus `taskmanager.memory.task.off-heap.size`. Connectors with netty/gRPC buffers (including the YTsaurus connectors) need the latter; flyt sets it to 1/8 of the TaskManager JVM.

Every JVM runs with `-XX:+ExitOnOutOfMemoryError`, and TaskManagers with `taskmanager.jvm-exit-on-oom`, so an out-of-memory TaskManager dies and is restarted by YT instead of hanging in GC.

## Flink configuration

flyt generates one `config.yaml` at submit time and writes it to `flink/conf/config.yaml` in every JobManager and TaskManager YT job, like a ConfigMap shared by every pod on Kubernetes; the effective file is printed at the top of every YT job's stderr. The distribution's `config.yaml`/`flink-conf.yaml` is replaced, so Flink >= 1.19 is required. It holds:

- ports: `rest.port` 27050 (the Web UI), `jobmanager.rpc.port` 27051, `blob.server.port` 27052, random TaskManager RPC/data ports so TaskManagers can share an exec node; bind addresses `0.0.0.0`;
- the sizes from [Sizing](#sizing) and [Memory](#memory), `parallelism.default = taskmanager_count * slots`;
- `restart-strategy.type: exponential-delay` with at most 10 attempts in a row (see [Discovery and lifecycle](#discovery-and-lifecycle));
- `python.executable`/`python.client.executable` = `python_bin`; JVM flags `-Djava.net.preferIPv6Addresses=true`, `-XX:ActiveProcessorCount=<container cpu>`, `-XX:+ExitOnOutOfMemoryError`.

Per-container values are passed on the command line by the run scripts:

| Container | Command-line `-D` |
|---|---|
| JobManager | `jobmanager.rpc.address`, `rest.address` (the YT job's cluster/default address), `flyt.yt.job-id=$YT_JOB_ID` |
| TaskManager | `taskmanager.host`, `taskmanager.resource-id=$YT_JOB_ID`, the discovered `jobmanager.rpc.address`/`jobmanager.rpc.port` |

### Flink options

The pipeline sets its Flink options in code, as in `minicluster` (`StreamExecutionEnvironment.get_execution_environment(config)`, `t_env.get_config().set(...)`), and the same code works in both modes for job options: parallelism, checkpointing, restart strategy, `pipeline.*`, `table.*`, `python.*`. Cluster options (memory, slots, ports, metrics reporters, JVM flags) are read by the JobManager and TaskManagers when they start, before the script runs, so setting them in code has no effect in `application` mode; flyt's values above apply.

## Discovery and lifecycle

How TaskManagers find the JobManager and how the gang operation restarts are described in [ARCHITECTURE.md](../ARCHITECTURE.md); what matters when operating a job:

- A TaskManager polls for the JobManager up to `discovery_timeout` (default 300 s, Flink's own registration timeout), then fails and is restarted by YT; every such failure counts toward the operation's failed-job budget (`max_failed_job_count`, by default `max(10, 5 * taskmanager_count)`: about five failures per TaskManager, never fewer than minicluster's 10), so raise both on a pool where scheduling the JobManager takes long; a JobManager that never gets scheduled therefore fails the operation through that budget.
- Flink restarts a failed job in the cluster (`exponential-delay`) at most 10 times in a row (`restart-strategy.exponential-delay.attempts-before-reset-backoff`; the count resets after an hour of stable run, `reset-backoff-threshold`). Then the Flink job fails, the driver exits non-zero and YT restarts the whole cluster, which also counts toward the budget: a job with a deterministic bug fails the operation instead of retrying forever. The pipeline can set its own restart strategy in code.
- Inside the YT jobs the proxy is the client proxy, the cluster's `/hosts` when the client proxy is a loopback address, or `job_yt_proxy`.
- When the driver exits 0 (bounded sources finished, or the Flink job was cancelled or stopped from the Web UI) the operation completes: the JobManager calls `complete_operation` (retried for up to 2 minutes) and exits 0 whatever the answer, and every TaskManager also watches the JobManager job and exits 0 once it is completed, so the operation completes even when YT refuses the call (e.g. the token in `secure_vault` may not manage the operation). flyt never re-runs a finished pipeline, restarting operations is the orchestrator's job. A non-zero exit restarts the whole cluster with a new incarnation.
- The Flink Web UI listens on port 27050 of the JobManager YT job: `flyt ui --wait` prints the URL.

## Kind and other port-forwarded clusters

Jobs reach YT from inside the cluster, where the client's `localhost:<port>` is not the proxy. flyt detects a loopback client proxy and hands YT jobs the hosts from `GET /hosts`; set `job_yt_proxy` to override. Exec nodes without per-YT-job network namespaces (the `simple` job environment, Kind) report no YT job IPs; the TaskManager then probes the node address and checks `flyt.yt.job-id` in `/jobmanager/config` so it never registers with another operation's JobManager. See [examples/kind/README.md](../examples/kind/README.md) for the resource limits a Kind node needs.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoResourceAvailableException` after 5 minutes | parallelism above `taskmanager_count * slots` | raise `taskmanager_count`, or the TaskManager cpu (`--preset`, `--tm-cpu`) |
| `OutOfMemoryError: Direct buffer memory` or OOM-killed TaskManagers | the TaskManager JVM is too small for the job | a larger `--preset` |
| `pthread_create failed (EAGAIN)` | container or node PID limit | see [examples/kind/TROUBLESHOOTING.md](../examples/kind/TROUBLESHOOTING.md) |
| TaskManagers keep restarting with `no JobManager discovered` | jobs cannot reach the YT proxy or the JobManager REST port | check `job_yt_proxy` and the JobManager stderr |
