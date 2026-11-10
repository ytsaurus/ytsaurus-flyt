# Static table sink

The `ytsaurus-static` connector appends an insert-only Flink stream to **one exact,
unsorted static table path**. It supports Flink 1.20.1 and Java 11. It uses the same
Sink V2 implementation for SQL and Java.

Enable checkpointing before submitting the job. The connector rejects a topology
without checkpoints before creating a table or opening an upload.

## SQL

```sql
SET 'execution.checkpointing.interval' = '30 s';

CREATE TABLE output_events (
    id BIGINT,
    payload STRING
) WITH (
    'connector' = 'ytsaurus-static',
    'proxy' = 'my-cluster',
    'path' = '//home/my-project/events',
    'schema' = '[{name=id;type=int64};{name=payload;type=string}]',
    'credentials-source' = 'env',
    'sink.parallelism' = '4',
    'sink.roll-interval' = '60 s',
    'sink.write-timeout' = '5 min'
);

INSERT INTO output_events SELECT id, payload FROM input_events;
```

`//home/my-project/events` is the table itself. Unlike the dynamic-table connector,
the sink does not append another path component. UPDATE, DELETE, upserts, sorted
schemas and partitioned destinations are unsupported.

The first record creates a missing table with `dynamic=false`, the configured schema,
`optimize_for=scan` and `chunk_merger_mode=auto`. The parent directory must already
exist. Concurrent subtasks use `ignoreExisting` and then verify the table type,
sortedness and full schema, including nullability. An incompatible table fails the
job. The sink never alters or replaces an existing table. An empty input creates
neither a table nor an empty upload.

Custom creation attributes use YSON values, for example
`'properties.attributes.compression_codec' = 'lz4'`. Table kind and schema attributes
are reserved. Attributes apply only to a newly created table. The YT account must
allow the configured chunk merger mode; on an existing table, configure chunk merging
separately. Legacy aliases such as `clusterName`, `credentialsSource`, `proxyRole`
and `timestampFormat` are supported.

| Option | Default | Meaning |
| --- | --- | --- |
| `sink.roll-interval` | `60 s` | Close a nonempty upload after this processing-time interval, starting with its first accepted row. |
| `sink.write-timeout` | `5 min` | Bound each open, readiness and close wait; also set streaming RPC inactivity timeouts. Must exceed the roll interval. |
| `sink.parallelism` | Flink default | Number of concurrent append writers. |

Checkpointing can close portions more frequently than the roll interval. YT creates
chunks within each streaming upload; there is no client-side upload-size threshold.
The sink maintains at most one active and one closing upload per subtask, and applies
backpressure before creating another closing upload. It passes individual detached
rows into the SDK stream without an unbounded row queue. SDK flow-control buffers,
serialization overhead and the largest individual row still contribute to memory use.

Native temporal columns need both the converter's `type` tag and a `type_v3`
definition. For example, a nullable native timestamp uses
`{name=event_time;type=timestamp;type_v3={type_name=optional;item=timestamp}}`.
When `type_v3` is present, it determines nullability; a separate `required=false`
does not make `type_v3=timestamp` nullable.

## Java

```java
env.enableCheckpointing(30_000);

records.sinkTo(YtStaticSink.forRowData(rowType, ysonSchema)
        .setProxy(cluster)
        .setPath("//home/my-project/events")
        .setCredentialsProvider(credentialsProvider)
        .setRollInterval(Duration.ofSeconds(60))
        .setWriteTimeout(Duration.ofMinutes(5))
        .build());
```

For other input types, use `YtStaticSink.<T>builder()`, `setYsonSchema(...)` and
`setSerializer(...)`. The serializable `YtStaticRowSerializer<T>` returns a detached
`YTreeMapNode`. `YtStaticRowSerializer.forRowData(rowType, schema, timestampFormat)`
exposes the shared type mapping for adapters. RowData converters copy nested buffers
and reject non-INSERT row kinds. Serialization occurs on the task thread before
`write` returns.

## Delivery and visibility

Each closed upload becomes visible independently. There is no global row ordering
across writers or atomic visibility across a checkpoint. A checkpoint succeeds only
after all preceding uploads have successfully closed and committed in YT. A failure,
timeout or unknown commit result fails the task; Flink replays from the last completed
checkpoint. Already published rows can therefore appear again: delivery is
**at-least-once**.

The SDK uses `writeTable` with `append=true` and `needRetries=false`; Flink owns replay.
A rejected SDK write is retried only after readiness. Readiness means available
buffer capacity, not durability. Successful close waits for the YT upload commit.
The sink has no external parent transaction, committer or writer state. End of input
flushes uploads. Task cleanup cancels unfinished streams and never publishes an active
upload as a side effect of cleanup.

Frequent checkpoints and high parallelism create more small chunks. Monitor chunk
count and YT master load; automatic chunk merging is asynchronous and does not remove
the cost of creating uploads.

## Metrics

Metrics under the existing `<proxy>.<exact-path>` group include `publishedRows`,
`publishedYsonBytes`, `sumCommittedRows`, `sumFailedRows`, `writeErrors`, `pendingRows`,
`pendingCloses`, `lastCloseDurationMillis` and `lastCommitTimestamp`.
`publishedYsonBytes` counts uncompressed binary YSON bytes, excluding RPC framing and
YT chunk compression. `sumFailedRows` counts rows affected by a terminal failure once,
including a current rejected row; it is suitable for the existing failed-row alert.

`trackable-field` uses the existing `trackedField` and `lastTrackedField` gauges.
`properties.data_metrics.table_alias` and
`properties.data_metrics.timestamp.<metric-name>.column` use the shared data-metrics
contract. Timestamp aggregates are held separately for each upload and published only
after its successful close. Generic serializers without RowData metadata cannot enable
these timestamp metrics.

## Validation

Unit tests cover readiness/retry semantics, checkpoint fencing, stale timers, failed
and timed-out close, cancellation and late open, schema validation, serialization,
factory discovery and insert-only negotiation.

The optional local integration tests run a four-subtask Flink job, read back the
static table and inject a restart after a completed checkpoint. They require a
disposable local YT instance with RPC discovery and chunk-merger permissions:

```sh
./gradlew :flink-connector-ytsaurus:test --tests '*YtStaticSinkLocalIT' \
    -PytStaticTestProxy=localhost:8000
```

These tests accept only a loopback proxy, use local test credentials, create unique
tables under `//tmp`, and remove only those tables. Without the explicit property
they are skipped.
