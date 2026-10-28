# Application mode

`cluster_mode: application` runs a pipeline as a Flink application cluster inside one YTsaurus Vanilla operation: a `jobmanager` job that executes the script through Flink's `PythonDriver`, plus `taskmanager_count` TaskManager YT jobs. Use it when a pipeline needs more CPU or memory than one container gives; `minicluster` (the default, one YT job with an in-JVM MiniCluster) stays the simpler choice for small pipelines and Kind clusters. Internals are in [ARCHITECTURE.md](../ARCHITECTURE.md).

flyt is the YTsaurus counterpart of a standalone Flink cluster on Kubernetes in application mode: one `config.yaml` for every container, a fixed number of TaskManagers, Flink's own options kept as Flink options. flyt-specific fields cover only what Flink cannot express in a standalone cluster: how many TaskManager YT jobs YT starts and the CPU of each container.

## Fields and flags

Profile fields; the flag overrides the profile for one `flyt run`.

| Field | Flag | Default | Meaning |
|---|---|---|---|
| `cluster_mode` | `--mode` | `minicluster` | `application` enables everything below |
| `taskmanager_count` | `--taskmanagers` | 1 | TaskManager YT jobs: the cluster capacity, like TaskManager `replicas` on Kubernetes |
| `jobmanager_cpu` | `--jm-cpu` | 2 | CPU of the JobManager container |
| `taskmanager_cpu` | `--tm-cpu` | one per task slot | CPU of each TaskManager container (like `kubernetes.taskmanager.cpu`) |
| `base_flink_conf_path` | `--flink-conf` | | Flink `config.yaml`, local file or `//cypress/path`, nested or flat keys. The flag replaces the profile's whole Flink configuration (`base_flink_conf_path` and `flink_config_override`) for that run |
| `flink_config_override` | | `{}` | Flink options applied over the file |
| | `-D KEY=VALUE` | | Flink option for one run, repeatable; the top layer, over everything else |
| `discovery_timeout` | | 300 | seconds a TaskManager waits for the JobManager before failing, like Flink's `taskmanager.registration.timeout` |
| `sidecar_command` | | | helper started in every JobManager/TaskManager container before Flink, e.g. a metrics agent |
| `job_yt_proxy` | | | YT HTTP proxy as seen from inside the YT jobs; empty = the client proxy, or the cluster's `/hosts` when the client proxy is `localhost` |
| `max_failed_job_count` | | `max(10, 5 * taskmanager_count)` | failed YT jobs before the operation fails; an explicit value is used as is (minicluster default: 10) |

`preset` / `--preset` applies to `minicluster` only.

`flyt validate` resolves the cluster the way `flyt run` does: it reads `base_flink_conf_path`, prints the sizes, fails when `parallelism.default` does not fit and lists the configuration warnings as `[WARN]` rows, which do not fail validation.

```yaml
cluster_mode: application
taskmanager_count: 3
taskmanager_cpu: 12
flink_config_override:
  taskmanager.numberOfTaskSlots: 7
  taskmanager.memory.process.size: 14500m
  jobmanager.memory.process.size: 2g
```

## Sizing

A standalone Flink cluster cannot ask YT for more TaskManagers, so the capacity is fixed at submit time: `taskmanager_count * taskmanager.numberOfTaskSlots` slots. It must cover the highest operator parallelism of the pipeline, including explicit `set_parallelism` calls, which only you know before the job graph exists. An operator that needs more slots waits for them and fails after `slot.request.timeout` (5 minutes by default). This is the same contract as TaskManager `replicas` in a standalone Kubernetes deployment; Flink's native Kubernetes mode sizes pods from the job graph instead, which a fixed-size YT operation cannot do.

`parallelism.default` (operators without an explicit parallelism) is whatever the Flink configuration says, else every slot of every TaskManager, i.e. `taskmanager_count * slots`. flyt refuses a configured `parallelism.default` above the capacity. There is no `-p` flag: `standalone-job.sh` has none either, set the key in `flink_config_override` or `base_flink_conf_path`.

`jobmanager.scheduler: adaptive` (streaming only) makes a Flink job start with the slots it gets instead of failing; it rescales the whole Flink job, so prefer sizing the cluster right.

## Memory

Every container is a Flink JVM plus a fixed overhead for what Flink does not model, the way a Kubernetes pod gets `process.size`:

| | JVM | Container | Default JVM |
|---|---|---|---|
| JobManager | `jobmanager.memory.process.size` | JVM + 512m for the PyFlink driver (the user's script and its imports, which runs next to the JobManager JVM) | 2g |
| TaskManager | `taskmanager.memory.process.size` | JVM + 512m for bash, the discovery helper and a sidecar | 4g |

The JobManager's load barely depends on the job, so its defaults rarely need changing. TaskManagers are sized per YT job through the Flink keys.

Python UDF workers live inside the TaskManager's `process.size`: PyFlink reserves managed memory for them (`python.fn-execution.memory.managed`), so size them with `taskmanager.memory.managed.fraction`, not with the container. flyt sets the fraction to 0.1 instead of Flink's 0.4: flyt jobs keep state on the heap, so managed memory is only the Python workers' budget, and one worker per slot with light `map`-style UDFs fits (a PyFlink worker takes 250–350 MB before the first row, mostly imports). Raise it to 0.2–0.3 for several Python operators per slot or heavy imports, and back to 0.4 for RocksDB.

Direct memory: unlike the MiniCluster, where it defaults to the heap size, Flink caps a TaskManager's `-XX:MaxDirectMemorySize` at `framework.off-heap` (128m) plus `taskmanager.memory.task.off-heap.size`. Connectors with netty/gRPC buffers (including the YTsaurus connectors) need the latter; flyt defaults it to 1/8 of the TaskManager JVM, override the key to change it.

flyt's memory defaults are `*.memory.process.size` (both roles), `taskmanager.memory.task.off-heap.size` and `taskmanager.memory.managed.fraction`; each of them can be overridden on its own. Any other `taskmanager.memory.*` key (`flink.size`, `task.heap.size`, `network.*`, `jvm-overhead.*`, ...) means you configure Flink's memory model yourself: flyt then adds none of its TaskManager memory defaults, so Flink resolves your keys alone, and requires `taskmanager.memory.process.size` next to them, because that is what sizes the YT container. The same rule applies to `jobmanager.memory.*` and `jobmanager.memory.process.size`.

Every JVM runs with `-XX:+ExitOnOutOfMemoryError`, and TaskManagers with `taskmanager.jvm-exit-on-oom`, so an out-of-memory TaskManager dies and is restarted by YT instead of hanging in GC.

## Flink configuration

flyt assembles one `config.yaml` at submit time and writes it to `flink/conf/config.yaml` in every JobManager and TaskManager YT job, like a ConfigMap shared by every pod on Kubernetes. The distribution's `config.yaml`/`flink-conf.yaml` is replaced, so Flink >= 1.19 is required.

Layers, later wins:

1. flyt's generated options: `rest.port` 27050, `jobmanager.rpc.port` 27051, `blob.server.port` 27052, `taskmanager.numberOfTaskSlots`, `parallelism.default = taskmanager_count * slots`, `jobmanager.memory.process.size`, `taskmanager.memory.process.size`, `taskmanager.memory.task.off-heap.size` (1/8 of the TaskManager JVM), `taskmanager.memory.managed.fraction` (see [Memory](#memory)), `restart-strategy.type: exponential-delay`, `taskmanager.jvm-exit-on-oom`, `python.executable`/`python.client.executable` = `python_bin`;
2. the file from `base_flink_conf_path` (`launcher` reads it locally or from Cypress and flattens nested keys);
3. `flink_config_override`;
4. `-D KEY=VALUE` of `flyt run`, applied last: it overrides every key of the layers above.

For example, a profile file plus a few per-run options (the flags go before the script; a value may contain `=`):

```bash
flyt run --mode application --flink-conf ./flink-config.yaml \
  -D taskmanager.numberOfTaskSlots=4 \
  -D taskmanager.memory.process.size=8g \
  -D pipeline.name=wordcount-backfill \
  -D env.java.opts.taskmanager="-XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
  examples/simple_wordcount/pipeline.py
```

Here `config.yaml` gets `./flink-config.yaml` with those four keys set over it; the profile's `flink_config_override` is not used because of `--flink-conf`.

Keys merge across the three layers: a later layer overrides only the keys it sets, everything else keeps its effect. Between the profile and `flyt run` there is no merge: `--flink-conf` makes its file the whole Flink configuration for that run, dropping both the profile's `base_flink_conf_path` and its `flink_config_override`; only flyt's defaults remain underneath. The one exception is `-D` of the same command: explicit, and applied over the file.

flyt reads every value it relies on back from the merged configuration, never from its own defaults: the TaskManagers discover the JobManager on the configured `rest.port`, `flyt ui` probes it (the port is stored in the operation's `flyt_flink_rest_port` annotation), and the JobManager starts the driver with the configured Python executables.

Some keys belong to flyt: a value from any layer is ignored, and `flyt run`/`flyt validate` warn about it unless it equals flyt's own (a config copied from Kubernetes usually repeats them):

| Keys | flyt's value | Why |
|---|---|---|
| `rest.bind-address`, `jobmanager.bind-host`, `taskmanager.bind-host` | `0.0.0.0` | other YT jobs and `flyt ui` reach the processes on any of the job's addresses |
| `taskmanager.rpc.port`, `taskmanager.data.port` | `0` (random) | fixed ports collide when TaskManagers share an exec node |
| `rest.bind-port` | not set | the REST server binds on `rest.port`, where TaskManagers and `flyt ui` look for it |
| `env.java.default-opts.all` / `.jobmanager` / `.taskmanager` | `-Djava.net.preferIPv6Addresses=true` / `-XX:ActiveProcessorCount=<container cpu> -XX:+ExitOnOutOfMemoryError` | YT exec nodes are IPv6-first; the JVM must see the container cpu and exit on an OOM |

The platform JVM flags still yield to yours: Flink puts `env.java.opts.*` after `env.java.default-opts.*` (in the order `all`, then the role), and the JVM takes the last occurrence of a flag, so `env.java.opts.taskmanager: -XX:ActiveProcessorCount=8` overrides flyt's processor count. Override a role flag in the role's key: `env.java.opts.all` comes before `env.java.default-opts.taskmanager`.

`flyt run` and `flyt validate` also warn when `taskmanager.numberOfTaskSlots` is not set: every TaskManager then gets one slot and, unless `taskmanager_cpu` is set, one cpu.

Per-container keys are dropped from all layers, with a warning, and set by the run scripts instead:

| Container | Command-line `-D` |
|---|---|
| JobManager | `jobmanager.rpc.address`, `rest.address` (the YT job's cluster/default address), `flyt.yt.job-id=$YT_JOB_ID` |
| TaskManager | `taskmanager.host`, `taskmanager.resource-id=$YT_JOB_ID`, the discovered `jobmanager.rpc.address`/`jobmanager.rpc.port` |

`application/01_cluster_common.sh` writes the rendered file to `flink/conf/config.yaml` in every YT job and removes the distribution's legacy `flink-conf.yaml`; the effective file is printed at the top of every YT job's stderr.

## Discovery and lifecycle

How TaskManagers find the JobManager and how the gang operation restarts are described in [ARCHITECTURE.md](../ARCHITECTURE.md); what matters when operating a job:

- A TaskManager polls for the JobManager up to `discovery_timeout` (default 300 s, Flink's own registration timeout), then fails and is restarted by YT; every such failure counts toward the operation's failed-job budget (`max_failed_job_count`, by default `max(10, 5 * taskmanager_count)`: about five failures per TaskManager, never fewer than minicluster's 10), so raise both on a pool where scheduling the JobManager takes long; a JobManager that never gets scheduled therefore fails the operation through that budget.
- Flink restarts a failed job in the cluster (`exponential-delay`) at most 10 times in a row (`restart-strategy.exponential-delay.attempts-before-reset-backoff`; the count resets after an hour of stable run, `reset-backoff-threshold`). Then the Flink job fails, the driver exits non-zero and YT restarts the whole cluster, which also counts toward the budget: a job with a deterministic bug fails the operation instead of retrying forever. Override the keys to change it.
- Inside the YT jobs the proxy is the client proxy, the cluster's `/hosts` when the client proxy is a loopback address, or `job_yt_proxy`.
- When the driver exits 0 (bounded sources finished, or the Flink job was cancelled or stopped from the Web UI) the operation completes (the JobManager retries `complete_operation` for up to 10 minutes, since a failure there would restart the finished pipeline); flyt never re-runs a finished pipeline, restarting operations is the orchestrator's job. A non-zero exit restarts the whole cluster with a new incarnation.
- The Flink Web UI listens on port 27050 of the JobManager YT job: `flyt ui --wait` prints the URL.

## Kind and other port-forwarded clusters

Jobs reach YT from inside the cluster, where the client's `localhost:<port>` is not the proxy. flyt detects a loopback client proxy and hands YT jobs the hosts from `GET /hosts`; set `job_yt_proxy` to override. Exec nodes without per-YT-job network namespaces (the `simple` job environment, Kind) report no YT job IPs; the TaskManager then probes the node address and checks `flyt.yt.job-id` in `/jobmanager/config` so it never registers with another operation's JobManager. See [examples/kind/README.md](../examples/kind/README.md) for the resource limits a Kind node needs.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoResourceAvailableException` after 5 minutes | parallelism above `taskmanager_count * slots` | raise `taskmanager_count` or slots |
| `OutOfMemoryError: Direct buffer memory` on TaskManagers | connectors exceed task off-heap | raise `taskmanager.memory.task.off-heap.size` |
| TaskManagers OOM-killed by YT | JVM + Python workers above `process.size` + 512m | raise `taskmanager.memory.process.size`; size Python workers via `taskmanager.memory.managed.fraction` |
| `pthread_create failed (EAGAIN)` | container or node PID limit | see [examples/kind/TROUBLESHOOTING.md](../examples/kind/TROUBLESHOOTING.md) |
| TaskManagers keep restarting with `no JobManager discovered` | jobs cannot reach the YT proxy or the JobManager REST port | check `job_yt_proxy` and the JobManager stderr |
| No TaskManager metrics in the Web UI | `taskmanager.resource-id` contained an IPv6 literal | fixed by flyt (resource id = YT job id, a user value is ignored) |
