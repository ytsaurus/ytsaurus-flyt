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
import java.util.stream.Stream;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
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
import tech.ytsaurus.ysontree.YTreeTextSerializer;

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

class YtQueueSinkFunctionTest {
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

    private final List<YtQueueSinkFunction> sinks = new ArrayList<>();
    private YTsaurusClient client;
    private ApiServiceTransaction transaction;
    private ScheduledExecutorService committer;
    private ScheduledExecutorService flusher;
    private MockedStatic<Executors> executors;
    private OperatorMetricGroup metrics;
    private Map<String, Gauge<?>> gauges;

    @BeforeEach
    void setUp() throws Exception {
        client = mock(YTsaurusClient.class);
        when(client.getConfig()).thenReturn(YTsaurusClientConfig.builder().build());
        transaction = mock(ApiServiceTransaction.class);
        committer = mock(ScheduledExecutorService.class);
        flusher = mock(ScheduledExecutorService.class);
        when(committer.awaitTermination(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        when(flusher.awaitTermination(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        executors = mockStatic(Executors.class);
        executors.when(Executors::newSingleThreadScheduledExecutor).thenReturn(committer, flusher);
        metrics = mock(OperatorMetricGroup.class);
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
            for (YtQueueSinkFunction sink : sinks) {
                sink.close();
            }
        } finally {
            executors.close();
        }
    }

    @Test
    void batchFlushCopiesReusedRowsAndRoutesToFixedPartition() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 0, 1));
        GenericRowData row = row(1, "first");
        sink.invoke(row, null);
        row.setField(0, 2L);
        row.setField(1, StringData.fromString("second"));
        sink.invoke(row, null);

        ModifyRowsRequest request = writtenRequest();
        List<YTreeMapNode> records = records(request);
        assertThat(request.getPath()).isEqualTo(QUEUE_PATH);
        assertThat(records).hasSize(2);
        assertThat(records.get(0).getLong("id")).isEqualTo(1);
        assertThat(records.get(0).getString("name")).isEqualTo("first");
        assertThat(records.get(1).getLong("id")).isEqualTo(2);
        assertThat(records.get(1).getLong("$tablet_index")).isEqualTo(1);
        verify(transaction).commit();
        sink.finish();
        verify(transaction).commit();
    }

    @Test
    void rowModeMatchesTargetColumnsByNameWhenTheirOrderDiffers() throws Exception {
        TableSchema schema = TableSchema.builder()
                .addValue("name", ColumnValueType.STRING)
                .addValue("id", ColumnValueType.INT64)
                .build();
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(schema, true, false)));
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));

        sink.invoke(row(1234, "reordered"), null);

        YTreeMapNode record = records(writtenRequest()).get(0);
        assertThat(record.getLong("id")).isEqualTo(1234);
        assertThat(record.getString("name")).isEqualTo("reordered");
        verify(transaction).commit();
    }

    @Test
    void columnModeCopiesReusedSerializerBytes() throws Exception {
        byte[] reused = new byte[1];
        SerializationSchema<RowData> serializer = value -> {
            reused[0] = (byte) value.getLong(0);
            return reused;
        };
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.COLUMN, serializer, options(2, 0, null));

        sink.invoke(row(1, "unused"), null);
        sink.invoke(row(2, "unused"), null);

        ModifyRowsRequest request = writtenRequest();
        List<YTreeMapNode> records = records(request);
        assertThat(records.get(0).getOrThrow("value").bytesValue()).containsExactly((byte) 1);
        assertThat(records.get(1).getOrThrow("value").bytesValue()).containsExactly((byte) 2);
        assertThat(request.getSchema().findColumn("$tablet_index")).isEqualTo(-1);
    }

    @Test
    void checkpointAndFinishCommitPartialBatches() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 0, null));
        sink.invoke(row(1, "checkpoint"), null);
        verify(transaction, never()).commit();

        sink.snapshotState(null);
        sink.invoke(row(2, "finish"), null);
        sink.finish();

        verify(transaction, times(2)).commit();
    }

    @Test
    void periodicFlushCommitsLowTrafficBatch() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.invoke(row(1, "timer"), null);
        verify(transaction, never()).commit();

        flushAndCommitFromTimers(sink);

        verify(transaction).commit();
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(1L);
        sink.finish();
        verify(transaction).commit();
    }

    @Test
    void closeDropsUncheckpointedBatchWithoutPublishingIt() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 0, null));
        sink.invoke(row(1, "uncheckpointed"), null);

        sink.close();

        verify(client, never()).startTransaction(any(StartTransaction.class));
        verify(client).close();
        assertThrows(IllegalStateException.class, () -> sink.invoke(row(2, "closed"), null));
    }

    @Test
    void closeStopsSharedSchedulersWithoutPublishingBufferedRows() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.invoke(row(1, "pending"), null);

        sink.close();

        verify(committer).shutdown();
        verify(flusher).shutdown();
        verify(committer).awaitTermination(5000, TimeUnit.MILLISECONDS);
        verify(flusher).awaitTermination(5000, TimeUnit.MILLISECONDS);
        verify(transaction, never()).commit();
        verify(client).close();
        assertThat(gauges.get("lastCommitTimestamp").getValue()).isEqualTo(-1L);
    }

    @Test
    void writeFailurePreventsCheckpointAndDoesNotRetryUncertainWrites() throws Exception {
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("write failed")));
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));

        assertThrows(IOException.class, () -> sink.invoke(row(1, "failure"), null));
        assertThrows(IOException.class, () -> sink.snapshotState(null));
        assertThrows(IOException.class, sink::finish);

        verify(client).startTransaction(any(StartTransaction.class));
        verify(transaction, never()).commit();
        sink.close();
        verify(client).close();
    }

    @Test
    void exposesSharedWriterMetricsAndDoesNotRetryFailedCommit() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(2, 1, null));
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(0L);
        assertThat(gauges.get("lastCommitTimestamp").getValue()).isEqualTo(0L);

        sink.invoke(row(1, "first"), null);
        sink.invoke(row(2, "second"), null);
        assertThat(gauges.get("sumCommittedRows").getValue()).isEqualTo(2L);
        long committedAt = (Long) gauges.get("lastCommitTimestamp").getValue();
        assertThat(committedAt).isPositive();

        when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("unknown commit")));
        sink.invoke(row(3, "third"), null);
        IOException failure = assertThrows(IOException.class, () -> sink.invoke(row(4, "failure"), null));

        flushAndCommitFromTimers(sink);

        IOException repeated = assertThrows(IOException.class, () -> sink.invoke(row(5, "not written"), null));
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
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(10, 1, null));
        sink.invoke(row(1, "timer failure"), null);

        flushAndCommitFromTimers(sink);

        assertThrows(IOException.class, () -> sink.invoke(row(2, "not written"), null));
        assertThrows(IOException.class, () -> sink.snapshotState(null));
        assertThrows(IOException.class, sink::finish);
        verify(transaction, times(failCommit ? 1 : 0)).commit();
    }

    @ParameterizedTest
    @EnumSource(value = RowKind.class, names = {"UPDATE_BEFORE", "UPDATE_AFTER", "DELETE"})
    void rejectsNonInsertRows(RowKind rowKind) throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
        GenericRowData value = row(1, "changelog");
        value.setRowKind(rowKind);

        assertThrows(IllegalArgumentException.class, () -> sink.invoke(value, null));
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsNullPayloadInsteadOfLosingRecord() throws Exception {
        YtQueueSinkFunction sink = openSink(YtQueueWriteMode.COLUMN, value -> null, options(1, 0, null));

        assertThrows(NullPointerException.class, () -> sink.invoke(row(1, "null"), null));
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsNonMapOrMultipleYsonRecords() throws Exception {
        for (String encoded : List.of("42;", "{id=1;};{id=2;};", "{undeclared=1;};")) {
            YtQueueSinkFunction sink = openSink(YtQueueWriteMode.ROW,
                    value -> encoded.getBytes(StandardCharsets.UTF_8), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.invoke(row(1, "invalid"), null));
        }
        verify(client, never()).startTransaction(any(StartTransaction.class));
    }

    @Test
    void rejectsStaticAndSortedTablesAndClosesClient() throws Exception {
        for (boolean dynamic : new boolean[]{false, true}) {
            when(client.getNode(any(GetNode.class)))
                    .thenReturn(CompletableFuture.completedFuture(queueMetadata(QUEUE_SCHEMA, dynamic, true)));
            YtQueueSinkFunction sink = newSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.open(new Configuration()));
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
            YtQueueSinkFunction sink = newSink(YtQueueWriteMode.COLUMN, ysonSerializer(), options(1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> sink.open(new Configuration()));
        }
    }

    @Test
    void rejectsUnknownRowFieldsAndOutOfRangePartitions() {
        YtQueueSinkFunction wrongPartition = newSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> wrongPartition.open(new Configuration()));

        TableSchema schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build();
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(schema, true, false)));
        YtQueueSinkFunction missingField = newSink(YtQueueWriteMode.ROW, ysonSerializer(), options(1, 0, null));
        assertThrows(IllegalArgumentException.class, () -> missingField.open(new Configuration()));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("compatibleRowSchemas")
    void writesCompatibleRowSchemasThroughSdk(DataType fieldType, String ytType, Object value,
                                               String expectedYson) throws Exception {
        RowType rowType = singleFieldType(fieldType);
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(singleColumnSchema(ytType), true, false)));
        YtQueueSinkFunction sink = newSink(rowType, YtQueueWriteMode.ROW,
                new YsonRowDataSerializationSchema(rowType, TimestampFormat.SQL), options(1, 0, null));

        sink.open(new Configuration());
        sink.invoke(GenericRowData.of(value), null);

        assertThat(records(writtenRequest()).get(0).getOrThrow("payload"))
                .isEqualTo(YTreeTextSerializer.deserialize(expectedYson));
        verify(transaction).commit();
    }

    private static Stream<Arguments> compatibleRowSchemas() {
        return Stream.of(
                Arguments.of(DataTypes.BIGINT(), "type=int64;required=%false", 1234L, "1234"),
                Arguments.of(DataTypes.BIGINT().notNull(), "type=int64;required=%true", 1234L, "1234"),
                Arguments.of(DataTypes.INT(), "type_v3={type_name=optional;item=int64}", null, "#"),
                Arguments.of(DataTypes.INT().notNull(), "type_v3={type_name=optional;item=int64}", 1234, "1234"),
                Arguments.of(DataTypes.TINYINT().notNull(), "type_v3=int8", (byte) -128, "-128"),
                Arguments.of(DataTypes.TINYINT().notNull(), "type_v3=int64", (byte) 127, "127"),
                Arguments.of(DataTypes.SMALLINT().notNull(), "type_v3=int16", (short) -32768, "-32768"),
                Arguments.of(DataTypes.INT().notNull(), "type_v3=int32", Integer.MAX_VALUE, "2147483647"),
                Arguments.of(DataTypes.BIGINT().notNull(), "type_v3=int64", Long.MAX_VALUE, "9223372036854775807"),
                Arguments.of(DataTypes.FLOAT().notNull(), "type_v3=float", 1.25F, "1.25"),
                Arguments.of(DataTypes.FLOAT().notNull(), "type_v3=double", 1.25F, "1.25"),
                Arguments.of(DataTypes.DOUBLE().notNull(), "type_v3=double", 1.25D, "1.25"),
                Arguments.of(DataTypes.BOOLEAN().notNull(), "type_v3=bool", true, "%true"),
                Arguments.of(DataTypes.STRING().notNull(), "type_v3=utf8", StringData.fromString("text"), "\"text\""),
                Arguments.of(DataTypes.STRING().notNull(), "type_v3=json", StringData.fromString("{}"), "\"{}\""),
                Arguments.of(DataTypes.DATE().notNull(), "type_v3=string", 1, "\"1970-01-02\""),
                Arguments.of(DataTypes.TIME(0).notNull(), "type_v3=utf8", 1000, "\"00:00:01\""),
                Arguments.of(DataTypes.TIMESTAMP(3).notNull(), "type_v3=string",
                        TimestampData.fromEpochMillis(1234), "\"1970-01-01 00:00:01.234\""),
                Arguments.of(DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3)).notNull(),
                        "type_v3=int64", 1234L, "1234"),
                Arguments.of(DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).notNull(),
                        "type_v3=int32", 14, "14"),
                Arguments.of(DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).notNull(),
                        "type_v3=int64", 14, "14"),
                Arguments.of(DataTypes.INT().notNull(), "type_v3={type_name=tagged;tag=counter;item=int64}",
                        1234, "1234"),
                Arguments.of(DataTypes.ARRAY(DataTypes.INT()).notNull(),
                        "type_v3={type_name=list;item={type_name=optional;item=int64}}",
                        new GenericArrayData(new Integer[]{1, null, 3}), "[1;#;3]"),
                Arguments.of(DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.INT())).notNull(),
                        "type_v3={type_name=optional;item=yson}", GenericRowData.of(42), "{nested=42}"),
                Arguments.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).notNull(),
                        "type_v3={type_name=optional;item=yson}",
                        new GenericMapData(Map.of(StringData.fromString("one"), 1)), "{one=1}"),
                Arguments.of(DataTypes.MULTISET(DataTypes.STRING()).notNull(), "type=any;required=%false",
                        new GenericMapData(Map.of(StringData.fromString("one"), 2)), "{one=2}"),
                // BYTES contains embedded YSON, so its static type and nullability do not describe the node.
                Arguments.of(DataTypes.BYTES(), "type_v3=int64", "1234".getBytes(StandardCharsets.UTF_8), "1234"),
                Arguments.of(DataTypes.BINARY(4), "type_v3=int64", "1234".getBytes(StandardCharsets.UTF_8), "1234")
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("incompatibleRowSchemas")
    void rejectsIncompatibleRowSchemasBeforeWriting(DataType fieldType, String ytType) throws Exception {
        RowType rowType = singleFieldType(fieldType);
        YTreeNode schema = singleColumnSchema(ytType);
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata(schema, true, false)));
        YtQueueSinkFunction sink = newSink(rowType, YtQueueWriteMode.ROW,
                new YsonRowDataSerializationSchema(rowType, TimestampFormat.SQL), options(1, 1, null));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> sink.open(new Configuration()));

        assertThat(failure).hasMessageContaining(QUEUE_PATH).hasMessageContaining("payload")
                .hasMessageContaining(fieldType.getLogicalType().asSummaryString())
                .hasMessageContaining(TableSchema.fromYTree(schema).getColumnSchema(0).getTypeV3().toString());
        verify(client, never()).startTransaction(any(StartTransaction.class));
        verify(transaction, never()).modifyRows(any(ModifyRowsRequest.Builder.class));
        verify(client).close();
        executors.verifyNoInteractions();
    }

    private static Stream<Arguments> incompatibleRowSchemas() {
        return Stream.of(
                Arguments.of(DataTypes.BIGINT().notNull(), "type=string;required=%false"),
                Arguments.of(DataTypes.STRING().notNull(), "type_v3=int64"),
                Arguments.of(DataTypes.BIGINT(), "type=int64;required=%true"),
                Arguments.of(DataTypes.BIGINT(), "type_v3=int64"),
                Arguments.of(DataTypes.SMALLINT().notNull(), "type_v3=int8"),
                Arguments.of(DataTypes.INT().notNull(), "type_v3=int16"),
                Arguments.of(DataTypes.BIGINT().notNull(), "type_v3=int32"),
                Arguments.of(DataTypes.TINYINT().notNull(), "type_v3=uint64"),
                Arguments.of(DataTypes.INT().notNull(), "type_v3=double"),
                Arguments.of(DataTypes.DOUBLE().notNull(), "type_v3=float"),
                Arguments.of(DataTypes.BOOLEAN().notNull(), "type_v3=int64"),
                Arguments.of(DataTypes.DATE().notNull(), "type_v3=date"),
                Arguments.of(DataTypes.TIMESTAMP(3).notNull(), "type_v3=timestamp"),
                Arguments.of(DataTypes.TIMESTAMP_LTZ(3).notNull(), "type_v3=datetime"),
                Arguments.of(DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3)).notNull(),
                        "type_v3=interval"),
                Arguments.of(DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).notNull(),
                        "type_v3=int16"),
                Arguments.of(DataTypes.INT().notNull(),
                        "type_v3={type_name=optional;item={type_name=optional;item=int64}}"),
                Arguments.of(DataTypes.ARRAY(DataTypes.INT()).notNull(), "type_v3={type_name=list;item=int64}"),
                Arguments.of(DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull(),
                        "type_v3={type_name=list;item=int64}"),
                Arguments.of(DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.INT())).notNull(),
                        "type_v3={type_name=struct;members=[{name=nested;type=int32}]}"),
                Arguments.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).notNull(),
                        "type_v3={type_name=dict;key=string;value=int32}"),
                Arguments.of(DataTypes.MULTISET(DataTypes.STRING()).notNull(),
                        "type_v3={type_name=dict;key=string;value=int32}")
        );
    }

    @Test
    void columnModeKeepsPayloadIndependentOfInputFieldTypes() throws Exception {
        RowType rowType = singleFieldType(DataTypes.DECIMAL(10, 2));
        byte[] payload = "{\"payload\":1.25}".getBytes(StandardCharsets.UTF_8);
        YtQueueSinkFunction sink = newSink(rowType, YtQueueWriteMode.COLUMN,
                ignored -> payload, options(1, 0, null));

        sink.open(new Configuration());
        sink.invoke(GenericRowData.of((Object) null), null);

        assertThat(records(writtenRequest()).get(0).getOrThrow("value").bytesValue()).containsExactly(payload);
    }

    @Test
    void metadataTimeoutIsBoundedAndClosesClient() {
        when(client.getNode(any(GetNode.class))).thenReturn(new CompletableFuture<>());
        YtQueueSinkFunction sink = newSink(YtQueueWriteMode.ROW, ysonSerializer(),
                new YtQueueWriterOptions(1, Duration.ZERO, Duration.ofSeconds(1), null));

        assertThrows(TimeoutException.class, () -> sink.open(new Configuration()));
        verify(client).close();
    }

    @Test
    void resolvesCredentialsOnlyWhenOpeningRuntime() throws Exception {
        CredentialsProvider credentials = mock(CredentialsProvider.class);
        OAuthCredentialsConfig config = new OAuthCredentialsConfig("user", "test-token");
        when(credentials.getCredentials(PROXY)).thenReturn(config);
        YtQueueSinkFunction sink = new YtQueueSinkFunction(PROXY, QUEUE_PATH, credentials,
                ysonSerializer(), ROW_TYPE, YtQueueWriteMode.ROW, "value", options(1, 0, null));
        sinks.add(sink);
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metrics);
        sink.setRuntimeContext(runtimeContext);
        verify(credentials, never()).getCredentials(any());

        try (MockedStatic<YtUtils> utils = mockStatic(YtUtils.class)) {
            utils.when(() -> YtUtils.makeYtClient(PROXY, config)).thenReturn(client);
            sink.open(new Configuration());
        }

        verify(credentials).getCredentials(PROXY);
        assertThat(client.getConfig().getRpcOptions().getGlobalTimeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void runtimeConfigurationCanBeSerializedForFlink() throws Exception {
        YtQueueSinkFunction sink = new YtQueueSinkFunction(PROXY, QUEUE_PATH, new OptionsCredentialsProvider(),
                ysonSerializer(), ROW_TYPE, YtQueueWriteMode.ROW, "value", options(1, 0, null));

        YtQueueSinkFunction restored = InstantiationUtil.clone(sink, getClass().getClassLoader());

        assertThat(restored).isNotSameAs(sink);
    }

    private YtQueueSinkFunction newSink(YtQueueWriteMode mode, SerializationSchema<RowData> serializer,
                                        YtQueueWriterOptions options) {
        return newSink(ROW_TYPE, mode, serializer, options);
    }

    private YtQueueSinkFunction newSink(RowType rowType, YtQueueWriteMode mode,
                                        SerializationSchema<RowData> serializer, YtQueueWriterOptions options) {
        YtQueueSinkFunction sink = spy(new YtQueueSinkFunction(PROXY, QUEUE_PATH,
                new OptionsCredentialsProvider(), serializer, rowType, mode, "value", options));
        doReturn(client).when(sink).createClient();
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metrics);
        sink.setRuntimeContext(runtimeContext);
        sinks.add(sink);
        return sink;
    }

    private YtQueueSinkFunction openSink(YtQueueWriteMode mode, SerializationSchema<RowData> serializer,
                                         YtQueueWriterOptions options) throws Exception {
        YtQueueSinkFunction sink = newSink(mode, serializer, options);
        sink.open(new Configuration());
        return sink;
    }

    private ModifyRowsRequest writtenRequest() {
        ArgumentCaptor<ModifyRowsRequest.Builder> captor = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(transaction).modifyRows(captor.capture());
        return captor.getValue().build();
    }

    private void flushAndCommitFromTimers(YtQueueSinkFunction sink) throws ReflectiveOperationException {
        Field writerField = YtQueueSinkFunction.class.getDeclaredField("writer");
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

    private static YtQueueWriterOptions options(int batchSize, long intervalMillis, Integer partition) {
        return new YtQueueWriterOptions(batchSize, Duration.ofMillis(intervalMillis), Duration.ofSeconds(5), partition);
    }

    private static YTreeNode queueMetadata(TableSchema schema, boolean dynamic, boolean sorted) {
        return queueMetadata(schema.toYTree(), dynamic, sorted);
    }

    private static RowType singleFieldType(DataType fieldType) {
        return (RowType) DataTypes.ROW(DataTypes.FIELD("payload", fieldType)).getLogicalType();
    }

    private static YTreeNode singleColumnSchema(String ytType) {
        return YTreeTextSerializer.deserialize("[{name=payload;" + ytType + ";}]");
    }

    private static YTreeNode queueMetadata(YTreeNode schema, boolean dynamic, boolean sorted) {
        return YTree.builder().beginAttributes()
                .key("dynamic").value(dynamic)
                .key("sorted").value(sorted)
                .key("schema").value(schema)
                .key("tablet_count").value(2)
                .endAttributes().entity().build();
    }
}
