package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.Atomicity;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtBufferedTransactionWriter;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

public class YtQueueSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final String TABLET_INDEX = "$tablet_index";

    private final String proxy;
    private final String queuePath;
    private final CredentialsProvider credentialsProvider;
    private final SerializationSchema<RowData> serializationSchema;
    private final List<String> fieldNames;
    private final YtQueueWriteMode writeMode;
    private final String valueColumn;
    private final YtQueueWriterOptions writerOptions;

    private transient YTsaurusClient client;
    private transient YtBufferedTransactionWriter writer;
    private transient Object lock;
    private transient int rowsSinceFlush;
    private transient volatile boolean closed;
    private transient IOException failure;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtQueueSinkFunction(
            String proxy,
            String queuePath,
            CredentialsProvider credentialsProvider,
            SerializationSchema<RowData> serializationSchema,
            RowType rowType,
            YtQueueWriteMode writeMode,
            String valueColumn,
            YtQueueWriterOptions writerOptions) {
        this.proxy = Objects.requireNonNull(proxy, "proxy");
        this.queuePath = Objects.requireNonNull(queuePath, "queuePath");
        this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
        this.serializationSchema = Objects.requireNonNull(serializationSchema, "serializationSchema");
        this.fieldNames = new ArrayList<>(Objects.requireNonNull(rowType, "rowType").getFieldNames());
        this.writeMode = Objects.requireNonNull(writeMode, "writeMode");
        this.valueColumn = Objects.requireNonNull(valueColumn, "valueColumn");
        this.writerOptions = Objects.requireNonNull(writerOptions, "writerOptions");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lock = new Object();
        closed = false;
        failure = null;
        rowsSinceFlush = 0;
        try {
            serializationSchema.open(
                    RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()));
            client = createClient();
            writer = YtBufferedTransactionWriter.builder()
                    .client(client)
                    .path(queuePath)
                    .schema(loadWriteSchema())
                    .rowsInModificationLimit(writerOptions.getBatchSize())
                    .rowsInTransactionLimit(writerOptions.getBatchSize())
                    .commitTransactionPeriod(writerOptions.getFlushInterval())
                    .flushModificationPeriod(writerOptions.getFlushInterval())
                    .transactionTimeout(writerOptions.getRequestTimeout())
                    .atomicity(Atomicity.Full)
                    .retryStrategy(new FixedRetryStrategy(0, Duration.ZERO))
                    .onCommitSuccess(() -> { })
                    .onTransactionCommitted(() -> { })
                    .build();
            MetricGroup metrics = getRuntimeContext().getMetricGroup();
            metrics.gauge("sumCommittedRows", writer::getCommittedRowCount);
            metrics.gauge("sumFailedRows", writer::getFailedRowCount);
            metrics.gauge("lastCommitTimestamp", writer::getLastCommitTimestamp);
            if (!writerOptions.getFlushInterval().isZero()) {
                writer.open();
            }
        } catch (Exception e) {
            try {
                close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    protected YTsaurusClient createClient() {
        YTsaurusClient ytClient = YtUtils.makeYtClient(proxy, credentialsProvider.getCredentials(proxy));
        ytClient.getConfig().getRpcOptions().setGlobalTimeout(writerOptions.getRequestTimeout());
        return ytClient;
    }

    private TableSchema loadWriteSchema() throws Exception {
        YTreeNode queue = await(client.getNode(GetNode.builder()
                .setPath(YPath.simple(queuePath))
                .setAttributes(List.of("dynamic", "sorted", "schema", "tablet_count"))
                .setTimeout(writerOptions.getRequestTimeout())
                .build()));
        if (!queue.getAttributeOrThrow("dynamic").boolValue() ||
                queue.getAttributeOrThrow("sorted").boolValue()) {
            throw new IllegalArgumentException("Queue '" + queuePath + "' must be an ordered dynamic table");
        }
        TableSchema schema = TableSchema.fromYTree(queue.getAttributeOrThrow("schema"));
        List<String> columns = writeMode == YtQueueWriteMode.ROW ? fieldNames : List.of(valueColumn);
        TableSchema.Builder builder = TableSchema.builder();
        for (String name : columns) {
            int index = schema.findColumn(name);
            if (name.startsWith("$") || index < 0) {
                throw new IllegalArgumentException("Queue has no writable column '" + name + "'");
            }
            ColumnSchema column = schema.getColumnSchema(index);
            if (column.getExpression() != null) {
                throw new IllegalArgumentException("Queue column '" + name + "' is computed");
            }
            if (writeMode == YtQueueWriteMode.COLUMN && column.getWireType() != ColumnValueType.STRING) {
                throw new IllegalArgumentException("Queue payload column '" + name + "' must be string-like");
            }
            builder.add(column);
        }
        Integer partitionIndex = writerOptions.getPartitionIndex();
        if (partitionIndex != null) {
            long tabletCount = queue.getAttributeOrThrow("tablet_count").longValue();
            if (partitionIndex >= tabletCount) {
                throw new IllegalArgumentException("'sink.partition-index' must be less than queue tablet count " +
                        tabletCount);
            }
            builder.addValue(TABLET_INDEX, ColumnValueType.INT64);
        }
        return builder.build().toWrite();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        synchronized (lock) {
            checkFailure();
            if (closed) {
                throw new IllegalStateException("Queue sink is closed");
            }
            if (value == null || value.getRowKind() != RowKind.INSERT) {
                throw new IllegalArgumentException("The 'ytsaurus-queue' sink accepts only INSERT rows");
            }
            Map<String, ?> row = serialize(value);
            try {
                writer.write(() -> row);
                rowsSinceFlush++;
                if (rowsSinceFlush >= writerOptions.getBatchSize()) {
                    writer.flush();
                    rowsSinceFlush = 0;
                }
            } catch (Exception e) {
                throw recordFailure(e);
            }
        }
    }

    private Map<String, ?> serialize(RowData value) {
        byte[] payload = Objects.requireNonNull(serializationSchema.serialize(value),
                "Queue serializer returned a null payload");
        Map<String, Object> row = new HashMap<>();
        if (writeMode == YtQueueWriteMode.COLUMN) {
            row.put(valueColumn, Arrays.copyOf(payload, payload.length));
        } else {
            List<YTreeNode> nodes = YTreeBinarySerializer.deserializeAll(new ByteArrayInputStream(payload));
            if (nodes.size() != 1 || !nodes.get(0).isMapNode()) {
                throw new IllegalArgumentException("Queue ROW format must serialize exactly one YSON map");
            }
            for (Map.Entry<String, YTreeNode> entry : nodes.get(0).asMap().entrySet()) {
                if (!fieldNames.contains(entry.getKey())) {
                    throw new IllegalArgumentException("Serialized queue row has undeclared column '" +
                            entry.getKey() + "'");
                }
                row.put(entry.getKey(), entry.getValue());
            }
        }
        if (writerOptions.getPartitionIndex() != null) {
            row.put(TABLET_INDEX, writerOptions.getPartitionIndex().longValue());
        }
        return row;
    }

    private void flush() throws Exception {
        synchronized (lock) {
            checkFailure();
            if (!closed && writer.isBusy()) {
                try {
                    writer.flush();
                    rowsSinceFlush = 0;
                } catch (Exception e) {
                    throw recordFailure(e);
                }
            }
        }
    }

    private IOException recordFailure(Exception cause) {
        if (failure == null) {
            failure = new IOException("Failed to write YTsaurus queue '" + queuePath + "'", cause);
            writer.recordError(failure);
            writer.closeAsyncTasks().forEach(failure::addSuppressed);
        }
        return failure;
    }

    private void checkFailure() throws IOException {
        if (failure != null) {
            throw new IOException("YTsaurus queue sink previously failed for '" + queuePath + "'", failure);
        }
        if (writer != null) {
            try {
                writer.checkError();
            } catch (RuntimeException e) {
                throw recordFailure(e);
            }
        }
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get(writerOptions.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw new IOException("Failed to write YTsaurus queue '" + queuePath + "'", e.getCause());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void finish() throws Exception {
        flush();
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (lock == null) {
            super.close();
            return;
        }
        synchronized (lock) {
            List<Exception> errors = writer == null ? new ArrayList<>() : writer.closeAsyncTasks();
            try {
                if (client != null) {
                    YTsaurusClient currentClient = client;
                    client = null;
                    currentClient.close();
                }
            } catch (Exception e) {
                errors.add(e);
            } finally {
                if (writer != null) {
                    writer.clearMetrics();
                    writer = null;
                }
                super.close();
            }
            if (!errors.isEmpty()) {
                Exception first = errors.get(0);
                for (int i = 1; i < errors.size(); i++) {
                    first.addSuppressed(errors.get(i));
                }
                throw first;
            }
        }
    }
}
