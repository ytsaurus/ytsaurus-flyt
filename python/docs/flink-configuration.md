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
| `python.executable`, `python.client.executable` | `python_bin` | the layer's interpreter |
| `parallelism.default` | every cpu: the job's cpu in `minicluster`, `flyt.taskmanager.replicas * slots` in `application` | |

Per mode, overridable through Flink keys:

| | `minicluster` | `application` |
|---|---|---|
| Task slots | not set: Flink gives the MiniCluster as many as the job graph's max parallelism | one per TaskManager cpu, or `taskmanager.numberOfTaskSlots` |
| Memory | the preset sizes the YT job and its JVM (`-Xmx`, direct memory), or, without a preset, the job is one TaskManager of `taskmanager.memory.process.size` ([below](#minicluster-memory)); `jobmanager.memory.*` has no effect (warning) | the preset, or `flyt.taskmanager.cpu.cores` with the Flink keys, sizes every TaskManager (`taskmanager.memory.process.size` = container), the JobManager gets 2560m or `jobmanager.memory.process.size`; `jvm-overhead.min` is at least 512m ([details](application-mode.md#sizing)) |

### Minicluster memory

The MiniCluster runs inside the PyFlink gateway's JVM, which Flink's local execution does not size. By default the YT container is the preset's memory and the JVM gets the preset's `-Xmx` (and `-XX:MaxDirectMemorySize` where the preset has one).

Without a preset, `taskmanager.memory.process.size` (and `flyt.taskmanager.cpu.cores`, 1 by default) sizes the job as one TaskManager of application mode ([Sizing](application-mode.md#sizing)): the container is `process.size`, the cpu is `flyt.taskmanager.cpu.cores`, and the job runs Flink's own `BashJavaUtils`, as `taskmanager.sh` does, to turn the TaskManager memory keys into the JVM's `-Xmx`, `-XX:MaxDirectMemorySize` and `-XX:MaxMetaspaceSize` and the MiniCluster's managed and network memory. The same keys and flyt's defaults apply as in [application mode](application-mode.md#memory), so `--mode application` with one TaskManager gets a TaskManager laid out like the MiniCluster job.

```bash
flyt run -D flyt.taskmanager.cpu.cores=4 -D taskmanager.memory.process.size=12g pipeline.py
```

## flyt options

flyt reads the YT resources of the cluster from the Flink configuration; they are set in any layer above and stay in `config.yaml`, where Flink ignores them:

| Key | Default | Meaning |
|---|---|---|
| `flyt.taskmanager.replicas` | 1 | TaskManager YT jobs in `application` mode; `flyt run --taskmanagers N` sets it for one run |
| `flyt.taskmanager.cpu.cores` | the preset's cpu, else 1 | CPU of every TaskManager YT job, and of the `minicluster` job; not combinable with a preset ([Sizing](application-mode.md#sizing)) |
| `flyt.jobmanager.cpu.cores` | 2 | CPU of the JobManager YT job in `application` mode |

## Keys flyt owns

A value from any layer is ignored, with a warning unless it equals flyt's own:

| Keys | flyt's value | Why |
|---|---|---|
| `rest.port`, `jobmanager.rpc.port`, `blob.server.port` | 27050, 27051, 27052 | TaskManagers and `flyt ui` find the JobManager there |
| `rest.bind-address`, `jobmanager.bind-host`, `taskmanager.bind-host` | `0.0.0.0` | other YT jobs and `flyt ui` reach the processes on any of the job's addresses |
| `taskmanager.rpc.port`, `taskmanager.data.port` | `0` (random) | fixed ports collide when TaskManagers share an exec node |
| `taskmanager.cpu.cores` | the TaskManager cpu | the container cpu: `flyt.taskmanager.cpu.cores` or the preset |
| `rest.bind-port` | not set | the REST server binds on `rest.port` |
| `jobmanager.rpc.address`, `rest.address`, `taskmanager.host`, `taskmanager.resource-id` | per container | set by the run scripts ([application mode](application-mode.md#flink-configuration)) |
| `env.java.default-opts.all` / `.jobmanager` / `.taskmanager` | `-Djava.net.preferIPv6Addresses=true`, `-Dfile.encoding=UTF-8`, `-Dio.netty.tryReflectionSetAccessible=true`, `-XX:ActiveProcessorCount=<container cpu>`, `-XX:+ExitOnOutOfMemoryError`; in `minicluster` also the preset's `-Xmx` and `-XX:MaxDirectMemorySize` | YT exec nodes are IPv6-first; a container's locale may be POSIX; Python UDFs might need netty's direct buffers on Java 9+; the JVM must see the container cpu and exit on an OOM |

The platform JVM flags still yield to yours: Flink (and PyFlink's gateway, which runs the MiniCluster) puts `env.java.opts.*` after `env.java.default-opts.*` and the JVM takes the last occurrence of a flag, so `env.java.opts.taskmanager: -XX:ActiveProcessorCount=8` changes a TaskManager's processor count. A MiniCluster is one JVM: `env.java.opts.jobmanager`/`.taskmanager` have no effect there (warning).
