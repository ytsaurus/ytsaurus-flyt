package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.DefaultSerializationResolver;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor.OptionsCredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtBufferedTransactionWriter;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;
import tech.ytsaurus.flyt.formats.yson.YsonRowDataSerializationSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.RetryStrategy.NO_RETRY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.RetryUtils.getRetryStrategy;

class YtQueueSinkWriterTest {
    private static final String PROXY = "localhost:18000";
    private static final String QUEUE_PATH = "//tmp/test_queue";
    private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING())).getLogicalType();
    private static final TableSchema QUEUE_SCHEMA = TableSchema.builder()
            .addValue("id", ColumnValueType.INT64)
            .addValue("name", ColumnValueType.STRING)
            .addValue("value", ColumnValueType.STRING)
            .build();

    private final List<YtQueueSinkWriter> sinks = new ArrayList<>();
    private YTsaurusClient client;
    private ApiServiceTransaction transaction;
    private ScheduledExecutorService committer;
    private ScheduledExecutorService flusher;
    private MockedStatic<Executors> executors;
    private SinkWriterMetricGroup metrics;
    private SerializationSchema.InitializationContext serializationContext;
    private Map<String, Gauge<?>> gauges;

    @BeforeEach
    void setUp() throws Exception {
        client = mock(YTsaurusClient.class);
        when(client.getConfig()).thenReturn(YTsaurusClientConfig.builder().build());
        transaction = mock(ApiServiceTransaction.class);
        committer = mock(ScheduledExecutorService.class);
        flusher = mock(ScheduledExecutorService.class);
        executors = mockStatic(Executors.class);
        executors.when(Executors::newSingleThreadScheduledExecutor).thenReturn(committer, flusher);
        metrics = mock(SinkWriterMetricGroup.class);
        serializationContext = mock(SerializationSchema.InitializationContext.class);
        gauges = new HashMap<>();
        doAnswer(invocation -> {
            Gauge<?> gauge = invocation.getArgument(1);
            gauges.put(invocation.getArgument(0), gauge);
            return gauge;
        }).when(metrics).gauge(anyString(), any());
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(QUEUE_SCHEMA, true, false)));
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterEach
    void closeSinks() throws Exception {
        try {
            for (YtQueueSinkWriter sink : sinks) {
                sink.close();
            }
        } finally {
            executors.close();
        }
    }

    @Test
    void schedulesFlushAndCommitUsingTheirOwnPeriods() throws Exception {
        openWriter(YtQueueWriteMode.ROW, ysonSerializer(),
                new YtQueueWriterOptions(100, 1000, Duration.ofMillis(10), Duration.ofMillis(20),
                        Duration.ofSeconds(5), null));

        verify(flusher).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(10L), eq(TimeUnit.MILLISECONDS));
        verify(committer).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(20L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void flushCopiesReusedRowsAndRoutesToFixedPartition() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 0, 1));
        GenericRowData row = row(1, "first");
        sink.write(row, null);
        row.setField(0, 2L);
        row.setField(1, StringData.fromString("second"));
        sink.write(row, null);
        verify(transaction, never()).commit();
        sink.flush(true);

        ModifyRowsRequest request = writtenRequest();
        List<YTreeMapNode> records = records(request);
        assertThat(request.getPath()).isEqualTo(QUEUE_PATH);
        assertThat(records).hasSize(2);
        assertThat(records.get(0).getLong("id")).isEqualTo(1);
        assertThat(records.get(0).getString("name")).isEqualTo("first");
        assertThat(records.get(1).getLong("id")).isEqualTo(2);
        assertThat(records.get(1).getLong("$tablet_index")).isEqualTo(1);
        verify(transaction).commit();
        sink.flush(true);
        verify(transaction).commit();
    }

    @Test
    void columnModeCopiesReusedSerializerBytes() throws Exception {
        byte[] reused = new byte[1];
        SerializationSchema<RowData> serializer = value -> {
            reused[0] = (byte) value.getLong(0);
            return reused;
        };
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.COLUMN, serializer, options(2, 0, null));

        sink.write(row(1, "unused"), null);
        sink.write(row(2, "unused"), null);
        sink.flush(true);

        ModifyRowsRequest request = writtenRequest();
        List<YTreeMapNode> records = records(request);
        assertThat(records.get(0).getOrThrow("value").bytesValue()).containsExactly((byte) 1);
        assertThat(records.get(1).getOrThrow("value").bytesValue()).containsExactly((byte) 2);
        assertThat(request.getSchema().findColumn("$tablet_index")).isEqualTo(-1);
    }

    @Test
    void sharedWriterUsesIndependentModificationAndTransactionLimits() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 5, 0, null));
        sink.write(row(1, "first"), null);
        sink.write(row(2, "second"), null);
        verify(client, never()).startTransaction(any(StartTransaction.class));

        for (int id = 3; id <= 6; id++) {
            sink.write(row(id, "buffered"), null);
        }
        verify(transaction, times(2)).modifyRows(any(ModifyRowsRequest.Builder.class));
        verify(transaction, never()).commit();
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(0L);

        sink.write(row(7, "next transaction"), null);

        ArgumentCaptor<ModifyRowsRequest.Builder> requests = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(transaction, times(3)).modifyRows(requests.capture());
        List<Long> writtenIds = new ArrayList<>();
        for (ModifyRowsRequest.Builder request : requests.getAllValues()) {
            List<YTreeMapNode> rows = records(request.build());
            assertThat(rows).hasSize(2);
            rows.forEach(value -> writtenIds.add(value.getLong("id")));
        }
        assertThat(writtenIds).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        verify(transaction).commit();
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(6L);

        sink.flush(true);

        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(transaction, times(2)).commit();
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(7L);
    }

    @Test
    void checkpointAndFinishCommitPartialBatches() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 0, null));
        sink.write(row(1, "checkpoint"), null);
        verify(transaction, never()).commit();

        sink.flush(false);
        sink.write(row(2, "finish"), null);
        sink.flush(true);

        verify(transaction, times(2)).commit();
    }

    @Test
    void failedCommitReplaysAllRowsInNewTransactionBeforeCompletingCheckpoint() throws Exception {
        ApiServiceTransaction retryTransaction = mock(ApiServiceTransaction.class);
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction),
                        CompletableFuture.completedFuture(retryTransaction));
        when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("unknown commit")));
        when(retryTransaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(retryTransaction.commit()).thenReturn(CompletableFuture.completedFuture(null));
        YtQueueSinkWriter sink = newWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 10, 0, 1),
                () -> new FixedRetryStrategy(1, Duration.ZERO));
        sink.open(serializationContext, metrics);

        sink.write(row(1, "first"), null);
        sink.write(row(2, "second"), null);
        sink.write(row(3, "third"), null);
        sink.flush(false);

        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(transaction, times(2)).modifyRows(any(ModifyRowsRequest.Builder.class));
        verify(transaction).commit();
        ArgumentCaptor<ModifyRowsRequest.Builder> replay = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(retryTransaction).modifyRows(replay.capture());
        List<YTreeMapNode> replayedRows = records(replay.getValue().build());
        assertThat(replayedRows).extracting(value -> value.getLong("id")).containsExactly(1L, 2L, 3L);
        assertThat(replayedRows).extracting(value -> value.getString("name"))
                .containsExactly("first", "second", "third");
        assertThat(replayedRows).allSatisfy(value -> assertThat(value.getLong("$tablet_index")).isEqualTo(1L));
        assertThat(gauges.get("sumFailedRows").getValue()).isEqualTo(3L);
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(3L);

        sink.flush(false);
        verify(retryTransaction).commit();
    }

    @Test
    void interruptedCheckpointFailsWithoutCommittingPendingRows() throws Exception {
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class))).thenReturn(new CompletableFuture<>());
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 0, null));
        sink.write(row(1, "pending"), null);

        try {
            Thread.currentThread().interrupt();
            IOException failure = assertThrows(IOException.class, () -> sink.flush(false));
            assertThat(failure).hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }

        assertThrows(IOException.class, () -> sink.flush(false));
        verify(transaction, never()).commit();
    }

    @Test
    void periodicFlushCommitsLowTrafficBatch() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.write(row(1, "timer"), null);
        verify(transaction, never()).commit();

        flushAndCommitFromTimers(sink);

        verify(transaction).commit();
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(1L);
        sink.flush(true);
        verify(transaction).commit();
    }

    @Test
    void closeDropsUncheckpointedBatchWithoutPublishingIt() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 0, null));
        sink.write(row(1, "uncheckpointed"), null);

        sink.close();

        verify(client, never()).startTransaction(any(StartTransaction.class));
        verify(client).close();
        assertThrows(IllegalStateException.class, () -> sink.write(row(2, "closed"), null));
    }

    @Test
    void closeStopsSharedSchedulersWithoutPublishingBufferedRows() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.write(row(1, "pending"), null);

        sink.close();

        verify(committer).shutdownNow();
        verify(flusher).shutdownNow();
        verify(committer, never()).awaitTermination(anyLong(), any(TimeUnit.class));
        verify(flusher, never()).awaitTermination(anyLong(), any(TimeUnit.class));
        verify(transaction, never()).commit();
        verify(client).close();
        assertThat(gauges.get("lastCommitTimestamp").getValue()).isEqualTo(-1L);
    }

    @Test
    void writeFailurePreventsCheckpointAndDoesNotRetryUncertainWrites() throws Exception {
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("write failed")));
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));

        sink.write(row(1, "pending failure"), null);
        assertThrows(IOException.class, () -> sink.write(row(2, "not written"), null));
        assertThrows(IOException.class, () -> sink.flush(false));
        assertThrows(IOException.class, () -> sink.flush(true));

        verify(client).startTransaction(any(StartTransaction.class));
        verify(transaction, never()).commit();
        sink.close();
        verify(client).close();
    }

    @Test
    void noRetryStrategyExposesMetricsAndDoesNotRetryFailedCommit() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 1, null));
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(0L);
        assertThat(gauges.get("lastCommitTimestamp").getValue()).isEqualTo(0L);

        sink.write(row(1, "first"), null);
        sink.write(row(2, "second"), null);
        sink.write(row(3, "third"), null);
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(2L);
        long committedAt = (Long) gauges.get("lastCommitTimestamp").getValue();
        assertThat(committedAt).isPositive();

        when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("unknown commit")));
        sink.write(row(4, "fourth"), null);
        IOException failure = assertThrows(IOException.class, () -> sink.write(row(5, "failure"), null));

        flushAndCommitFromTimers(sink);

        IOException repeated = assertThrows(IOException.class, () -> sink.write(row(6, "not written"), null));
        assertThat(repeated.getCause()).isSameAs(failure);

        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(2L);
        assertThat(gauges.get("sumFailedRows").getValue()).isEqualTo(2L);
        assertThat(gauges.get("lastCommitTimestamp").getValue()).isEqualTo(committedAt);
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(transaction, times(2)).commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void periodicFailurePreventsFurtherWritesAndCheckpoints(boolean failCommit) throws Exception {
        if (failCommit) {
            when(transaction.commit())
                    .thenReturn(CompletableFuture.failedFuture(new IOException("periodic commit failed")));
        } else {
            when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                    .thenReturn(CompletableFuture.failedFuture(new IOException("periodic write failed")));
        }
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.write(row(1, "timer failure"), null);

        flushAndCommitFromTimers(sink);

        assertThrows(IOException.class, () -> sink.write(row(2, "not written"), null));
        assertThrows(IOException.class, () -> sink.flush(false));
        assertThrows(IOException.class, () -> sink.flush(true));
        verify(transaction, times(failCommit ? 1 : 0)).commit();
    }

    @ParameterizedTest
    @EnumSource(value = RowKind.class, names = {"UPDATE_BEFORE", "UPDATE_AFTER", "DELETE"})
    void rejectsNonInsertRows(RowKind rowKind) throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
        GenericRowData value = row(1, "changelog");
        value.setRowKind(rowKind);

        assertThrows(IllegalArgumentException.class, () -> sink.write(value, null));
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsNullPayloadInsteadOfLosingRecord() throws Exception {
        YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.COLUMN, value -> null, options(1, 0, null));

        assertThrows(NullPointerException.class, () -> sink.write(row(1, "null"), null));
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsNonMapOrMultipleYsonRecords() throws Exception {
        for (String encoded : List.of("42;", "{id=1;};{id=2;};", "{undeclared=1;};")) {
            YtQueueSinkWriter sink = openWriter(YtQueueWriteMode.ROW,
                    value -> encoded.getBytes(StandardCharsets.UTF_8), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.write(row(1, "invalid"), null));
        }
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsStaticAndSortedTablesAndClosesClient() throws Exception {
        for (boolean dynamic : new boolean[]{false, true}) {
            when(client.getNode(any(GetNode.class)))
                    .thenReturn(CompletableFuture.completedFuture(queueMetadata(QUEUE_SCHEMA, dynamic, true)));
            YtQueueSinkWriter sink = newWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.open(serializationContext, metrics));
        }
        verify(client, times(2)).close();
    }

    @Test
    void rejectsMissingOrNonStringPayloadColumn() {
        for (TableSchema schema : List.of(
                TableSchema.builder().addValue("other", ColumnValueType.STRING).build(),
                TableSchema.builder().addValue("value", ColumnValueType.INT64).build())) {
            when(client.getNode(any(GetNode.class)))
                    .thenReturn(CompletableFuture.completedFuture(queueMetadata(schema, true, false)));
            YtQueueSinkWriter sink = newWriter(YtQueueWriteMode.COLUMN, ysonSerializer(), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.open(serializationContext, metrics));
        }
    }

    @Test
    void rejectsUnknownRowFieldsAndOutOfRangePartitions() {
        YtQueueSinkWriter wrongPartition = newWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> wrongPartition.open(serializationContext, metrics));

        TableSchema schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build();
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(schema, true, false)));
        YtQueueSinkWriter missingField = newWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
        assertThrows(IllegalArgumentException.class, () -> missingField.open(serializationContext, metrics));
    }

    @Test
    void metadataRequestFailureClosesClient() {
        TimeoutException timeout = new TimeoutException("metadata request timed out");
        when(client.getNode(any(GetNode.class))).thenReturn(CompletableFuture.failedFuture(timeout));
        YtQueueSinkWriter sink = newWriter(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));

        IOException failure = assertThrows(IOException.class, () -> sink.open(serializationContext, metrics));
        assertThat(failure).hasCause(timeout);
        verify(client).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resolvesCredentialsOnlyWhenOpeningRuntimeAndAppliesTls(boolean useTls) throws Exception {
        CredentialsProvider credentials = mock(CredentialsProvider.class);
        OAuthCredentialsConfig config = new OAuthCredentialsConfig("user", "test-token");
        when(credentials.getCredentials(PROXY)).thenReturn(config);
        SerializationSchema<RowData> serializer = spy(ysonSerializer());
        YtQueueSink sink = new YtQueueSink(PROXY, QUEUE_PATH, credentials,
                serializer, ROW_TYPE, YtQueueWriteMode.ROW, "value", options(1, 0, null), useTls,
                getRetryStrategy(NO_RETRY));
        WriterInitContext context = mock(WriterInitContext.class);
        when(context.metricGroup()).thenReturn(metrics);
        when(context.asSerializationSchemaInitializationContext()).thenReturn(serializationContext);
        verify(credentials, never()).getCredentials(any());
        Duration rpcTimeout = client.getConfig().getRpcOptions().getGlobalTimeout();

        try (MockedStatic<YtUtils> utils = mockStatic(YtUtils.class)) {
            utils.when(() -> YtUtils.makeYtClient(PROXY, config, useTls)).thenReturn(client);
            sinks.add((YtQueueSinkWriter) sink.createWriter(context));
            utils.verify(() -> YtUtils.makeYtClient(PROXY, config, useTls));
        }

        verify(serializer).open(serializationContext);
        verify(credentials).getCredentials(PROXY);
        assertThat(client.getConfig().getRpcOptions().getGlobalTimeout()).isEqualTo(rpcTimeout);
    }

    @Test
    void runtimeConfigurationCanBeSerializedForFlink() throws Exception {
        YtQueueSink sink = new YtQueueSink(PROXY, QUEUE_PATH, new OptionsCredentialsProvider(),
                ysonSerializer(), ROW_TYPE, YtQueueWriteMode.ROW, "value", options(1, 0, null), true,
                getRetryStrategy(NO_RETRY));

        YtQueueSink restored = InstantiationUtil.clone(sink, getClass().getClassLoader());

        assertThat(restored).isNotSameAs(sink);
        assertThat(restored).extracting("useTls").isEqualTo(true);
        assertThat(restored).extracting("retryStrategy").satisfies(value ->
                assertThat(((RetryStrategy) ((SerializableSupplier<?>) value).get()).getNumRemainingRetries())
                        .isZero());
    }

    private YtQueueSinkWriter newWriter(YtQueueWriteMode mode, SerializationSchema<RowData> serializer,
                                        YtQueueWriterOptions options) {
        return newWriter(mode, serializer, options, getRetryStrategy(NO_RETRY));
    }

    private YtQueueSinkWriter newWriter(YtQueueWriteMode mode, SerializationSchema<RowData> serializer,
                                        YtQueueWriterOptions options, SerializableSupplier<RetryStrategy> retryStrategy) {
        YtQueueSinkWriter sink = spy(new YtQueueSinkWriter(PROXY, QUEUE_PATH,
                new OptionsCredentialsProvider(), serializer, ROW_TYPE, mode, "value", options, false, retryStrategy));
        doReturn(client).when(sink).createClient();
        sinks.add(sink);
        return sink;
    }

    private YtQueueSinkWriter openWriter(YtQueueWriteMode mode, SerializationSchema<RowData> serializer,
                                         YtQueueWriterOptions options) throws Exception {
        YtQueueSinkWriter sink = newWriter(mode, serializer, options);
        sink.open(serializationContext, metrics);
        return sink;
    }

    private ModifyRowsRequest writtenRequest() {
        ArgumentCaptor<ModifyRowsRequest.Builder> captor = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(transaction).modifyRows(captor.capture());
        return captor.getValue().build();
    }

    private void flushAndCommitFromTimers(YtQueueSinkWriter sink) throws ReflectiveOperationException {
        Field writerField = YtQueueSinkWriter.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        YtBufferedTransactionWriter writer = (YtBufferedTransactionWriter) writerField.get(sink);
        for (String name : List.of("lastModificationFlush", "lastTransactionCommit")) {
            Field timestamp = YtBufferedTransactionWriter.class.getDeclaredField(name);
            timestamp.setAccessible(true);
            ((AtomicLong) timestamp.get(writer)).set(0);
        }
        scheduledCallback(flusher).run();
        scheduledCallback(committer).run();
    }

    private static Runnable scheduledCallback(ScheduledExecutorService executor) {
        ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleAtFixedRate(callback.capture(), eq(0L), eq(1L), eq(TimeUnit.MILLISECONDS));
        return callback.getValue();
    }

    private static List<YTreeMapNode> records(ModifyRowsRequest request) {
        request.convertValues(DefaultSerializationResolver.getInstance());
        List<YTreeMapNode> result = new ArrayList<>();
        request.getRows().forEach(row -> result.add(row.toYTreeMap(request.getSchema(), false)));
        return result;
    }

    private static GenericRowData row(long id, String name) {
        return GenericRowData.of(id, StringData.fromString(name));
    }

    private static SerializationSchema<RowData> ysonSerializer() {
        return new YsonRowDataSerializationSchema(ROW_TYPE, TimestampFormat.SQL);
    }

    private static YtQueueWriterOptions options(int rowLimit, long intervalMillis, Integer partition) {
        return options(rowLimit, rowLimit, intervalMillis, partition);
    }

    private static YtQueueWriterOptions options(
            int modificationLimit, int transactionLimit, long intervalMillis, Integer partition) {
        Duration interval = Duration.ofMillis(intervalMillis);
        return new YtQueueWriterOptions(
                modificationLimit, transactionLimit, interval, interval, Duration.ofSeconds(5), partition);
    }

    private static YTreeNode queueMetadata(TableSchema schema, boolean dynamic, boolean sorted) {
        return YTree.builder().beginAttributes()
                .key("dynamic").value(dynamic)
                .key("sorted").value(sorted)
                .key("schema").value(schema.toYTree())
                .key("tablet_count").value(2)
                .endAttributes().entity().build();
    }
}
