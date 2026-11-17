# Flink configuration

A flyt job is configured the same way in both cluster modes: the same profile fields, the same `flyt run` flags and the same Flink keys, so a pipeline moves between `minicluster` and `application` with `--mode` alone.

## Sources

Layers, later wins:

1. flyt's defaults (below);
2. `base_flink_conf_path`: a Flink `config.yaml`, local file or `//cypress/path`, nested or flat keys;
3. `flink_config_override`: Flink keys in the profile;
4. `flyt run -D KEY=VALUE`, repeatable;
5. what the pipeline sets in code (`t_env.get_config().set(...)`), for the options Flink reads when the job is submitted: parallelism, checkpointing, `pipeline.*`, `table.*`. Cluster options (memory, slots, metrics reporters, JVM flags) are read when the processes start, before the script runs, and belong in the Flink configuration.

`flyt run --flink-conf FILE` replaces the profile's whole Flink configuration (`base_flink_conf_path` and `flink_config_override`) for one run; `-D` of the same command still applies over the file. The flags go before the script:

```bash
flyt run --flink-conf ./flink-config.yaml \
  -D pipeline.name=wordcount-backfill \
  -D execution.checkpointing.interval=1min \
  examples/simple_wordcount/pipeline.py
```

```yaml
# profile
base_flink_conf_path: //home/team/flink/config.yaml
flink_config_override:
  restart-strategy.type: fixed-delay
  restart-strategy.fixed-delay.attempts: "20"
```

flyt renders the result into `flink/conf/config.yaml` of every YT job; the effective file is printed at the top of the job's stderr. In `application` mode the JobManager and TaskManagers start with it; in `minicluster` mode flyt runs the script with it as the MiniCluster's configuration (PyFlink's local mode ignores `config.yaml`, so flyt installs it as the context environment, the way `flink run` does).

`flyt validate` reads the configuration the way `flyt run` does and lists warnings as `[WARN]` rows; `flyt run` logs them.

## flyt's defaults

Both modes, overridable:

| Key | Value | Why |
|---|---|---|
| `restart-strategy.type` | `exponential-delay` (no attempt limit) | without checkpointing Flink would not restart a failed job at all: the script would fail and YT would restart the whole job |
| `taskmanager.jvm-exit-on-oom` | `true` | an out-of-memory JVM dies and is restarted by YT instead of hanging in GC |
| `heartbeat.timeout`, `heartbeat.interval`, `heartbeat.rpc-failure-threshold` | `120000`, `5000`, `9` | Flink's 50 s timeout loses TaskManagers on shared exec nodes to GC pauses and network hiccups |
| `web.submit.enable`, `web.cancel.enable` | `false` | the REST API has no authentication and is reachable from the YT cluster network: it accepts no jar upload, and the web UI has no Cancel button (the REST API still cancels a job); stop a run by aborting the YT operation |
| `python.executable`, `python.client.executable` | `python_bin` | the layer's interpreter |

## Sizing

The Flink configuration is the only way to size a job. A TaskManager, and the single MiniCluster job in `minicluster` mode, is sized by `flyt.taskmanager.cpu.cores` and `taskmanager.memory.process.size`, plus any other `taskmanager.memory.*` key; without them it gets flyt's defaults ([Resources](#resources)). The same size applies in both modes, so a pipeline can be compared between them with one flag: `--mode application` with one TaskManager gives about the resources of the MiniCluster job, plus a small JobManager. Every container is its Flink JVM's `*.memory.process.size`.

```bash
flyt run --mode application --taskmanagers 3 \
  -D flyt.taskmanager.cpu.cores=4 -D taskmanager.memory.process.size=12g pipeline.py
```

### Resources

Overridable. The memory keys apply where Flink's memory model lays out a TaskManager: every TaskManager in `application`, and the MiniCluster job ([below](#minicluster-memory)); `jobmanager.memory.*` has no effect in `minicluster` (warning). Any other `taskmanager.memory.*` key (`flink.size`, `task.heap.size`, `network.*`, ...) turns off flyt's `task.off-heap.size`, `managed.fraction` and `jvm-overhead.min` and needs `process.size` set too, since it sizes the YT container; the same rule holds for `jobmanager.memory.*` ([why](application-mode.md#memory)):

| Key | Default | Meaning |
|---|---|---|
| `flyt.taskmanager.replicas` | 1 | TaskManager YT jobs in `application`; `flyt run --taskmanagers N` sets it for one run |
| `flyt.taskmanager.cpu.cores` | `taskmanager.numberOfTaskSlots` when set, else 2 | cpu of every TaskManager YT job and of the MiniCluster job |
| `flyt.jobmanager.cpu.cores` | 2 | cpu of the JobManager YT job in `application` |
| `taskmanager.numberOfTaskSlots` | one per TaskManager cpu in `application`; not set in `minicluster` | task slots of a TaskManager; without it Flink gives the MiniCluster as many slots as the job graph's max parallelism |
| `parallelism.default` | `minicluster`: the TaskManager cpu, `flyt.taskmanager.cpu.cores`; `application`: every slot of the cluster, `flyt.taskmanager.replicas * slots` | parallelism of operators that set none |
| `taskmanager.memory.process.size` | `4g` | a TaskManager's Flink JVM, and its YT container |
| `taskmanager.memory.task.off-heap.size` | 1/8 of `process.size` | direct memory of the tasks: netty of the YTsaurus connectors, gRPC of Python UDFs |
| `taskmanager.memory.managed.fraction` | `0.1` (Flink: 0.4) | share of Flink memory managed by Flink; with state on the heap, only the Python UDF workers' budget |
| `taskmanager.memory.jvm-overhead.min` | `512m`; not set when you set `jvm-overhead.max` | lower bound of Flink's JVM overhead: the JVM's native memory and everything else in the container (bash, the discovery helper, a sidecar, Python workers above their managed budget) |
| `jobmanager.memory.process.size` | `2560m` | the JobManager's Flink JVM, and its YT container |
| `jobmanager.memory.jvm-overhead.min` | `512m`; not set when you set `jvm-overhead.max` | the same for the JobManager, whose container also holds the PyFlink driver |

### Minicluster memory

The MiniCluster runs inside the PyFlink gateway's JVM, which Flink's local execution does not size. `taskmanager.memory.process.size` and `flyt.taskmanager.cpu.cores` size the job as one TaskManager of application mode ([Resources](#resources)), and the job runs Flink's own `BashJavaUtils`, as `taskmanager.sh` does, to turn the TaskManager memory keys into the JVM's `-Xmx`, `-XX:MaxDirectMemorySize` and `-XX:MaxMetaspaceSize` and the MiniCluster's managed and network memory. The memory defaults above apply, so `--mode application` with one TaskManager gets a TaskManager laid out like the MiniCluster job.

```bash
flyt run -D flyt.taskmanager.cpu.cores=4 -D taskmanager.memory.process.size=12g pipeline.py
```

## flyt options

The `flyt.*` keys are flyt's own options for the YT resources of the cluster: `flyt.taskmanager.replicas`, `flyt.taskmanager.cpu.cores`, `flyt.jobmanager.cpu.cores`. They are set in any layer above and stay in `config.yaml`, where Flink ignores them; defaults are in [Resources](#resources).

## Keys flyt owns

A value from any layer is ignored, with a warning unless it equals flyt's own:

| Keys | flyt's value | Why |
|---|---|---|
| `rest.port`, `jobmanager.rpc.port`, `blob.server.port` | 27050, 27051, 27052 | TaskManagers and `flyt ui` find the JobManager there |
| `rest.bind-address`, `jobmanager.bind-host`, `taskmanager.bind-host` | `0.0.0.0` | other YT jobs and `flyt ui` reach the processes on any of the job's addresses |
| `taskmanager.rpc.port`, `taskmanager.data.port` | `0` (random) | fixed ports collide when TaskManagers share an exec node |
| `taskmanager.cpu.cores` | the TaskManager cpu | the container cpu: `flyt.taskmanager.cpu.cores` |
| `rest.bind-port` | not set | the REST server binds on `rest.port` |
| `jobmanager.rpc.address`, `rest.address`, `taskmanager.host`, `taskmanager.resource-id` | per container | set by the run scripts ([application mode](application-mode.md#flink-configuration)) |
| `env.java.default-opts.all` / `.jobmanager` / `.taskmanager` | `-Djava.net.preferIPv6Addresses=true`, `-Dfile.encoding=UTF-8`, `-Dio.netty.tryReflectionSetAccessible=true`, `-XX:ActiveProcessorCount=<container cpu>`, `-XX:+ExitOnOutOfMemoryError`; in `minicluster` also the JVM flags Flink derives from `taskmanager.memory.*` | YT exec nodes are IPv6-first; a container's locale may be POSIX; Python UDFs might need netty's direct buffers on Java 9+; the JVM must see the container cpu and exit on an OOM |

The platform JVM flags still yield to yours: Flink (and PyFlink's gateway, which runs the MiniCluster) puts `env.java.opts.*` after `env.java.default-opts.*` and the JVM takes the last occurrence of a flag, so `env.java.opts.taskmanager: -XX:ActiveProcessorCount=8` changes a TaskManager's processor count. A MiniCluster is one JVM: `env.java.opts.jobmanager`/`.taskmanager` have no effect there (warning).
