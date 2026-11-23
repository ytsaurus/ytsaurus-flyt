package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;
import tech.ytsaurus.flyt.formats.yson.YsonRowDataSerializationSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.RetryStrategy.NO_RETRY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.RetryUtils.getRetryStrategy;

class YtQueueSinkLifecycleTest {
    private static final String PROXY = "localhost:18000";
    private static final String QUEUE_PATH = "//tmp/lifecycle_queue";
    private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING())).getLogicalType();
    private static final TableSchema QUEUE_SCHEMA = TableSchema.builder()
            .addValue("id", ColumnValueType.INT64)
            .addValue("name", ColumnValueType.STRING)
            .build();

    private YTsaurusClient client;
    private ApiServiceTransaction transaction;
    private MockedStatic<YtUtils> ytUtils;
    private OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> harness;

    @BeforeEach
    void setUp() throws Exception {
        client = mock(YTsaurusClient.class);
        when(client.getConfig()).thenReturn(YTsaurusClientConfig.builder().build());
        transaction = mock(ApiServiceTransaction.class);
        when(client.getNode(any(GetNode.class)))
                .thenReturn(CompletableFuture.completedFuture(queueMetadata()));
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));

        CredentialsProvider credentials = mock(CredentialsProvider.class);
        OAuthCredentialsConfig config = new OAuthCredentialsConfig("user", "test-token");
        when(credentials.getCredentials(PROXY)).thenReturn(config);
        ytUtils = mockStatic(YtUtils.class);
        ytUtils.when(() -> YtUtils.makeYtClient(PROXY, config, false)).thenReturn(client);

        YtQueueWriterOptions options = new YtQueueWriterOptions(
                2, 5, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(5), null);
        YtQueueSink sink = new YtQueueSink(PROXY, QUEUE_PATH, credentials,
                new YsonRowDataSerializationSchema(ROW_TYPE, TimestampFormat.SQL),
                ROW_TYPE, YtQueueWriteMode.ROW, "value", options, false, getRetryStrategy(NO_RETRY));
        harness = new OneInputStreamOperatorTestHarness<>(
                new SinkWriterOperatorFactory<RowData, Void>(sink), new RowDataSerializer(ROW_TYPE));
        harness.getStreamConfig().setCheckpointingEnabled(true);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            closeHarness();
        } finally {
            if (ytUtils != null) {
                ytUtils.close();
            }
        }
    }

    @Test
    void checkpointCommitsPartialBatchBeforeSnapshot() throws Exception {
        harness.processElement(row(1), 0L);
        verify(client, never()).startTransaction(any(StartTransaction.class));

        harness.prepareSnapshotPreBarrier(1L);

        assertWrittenRow(1);
        verify(transaction).commit();
        harness.snapshot(1L, 0L);
        verify(transaction).commit();
    }

    @Test
    void endOfInputCommitsPartialBatch() throws Exception {
        harness.processElement(row(1), 0L);
        verify(client, never()).startTransaction(any(StartTransaction.class));

        harness.endInput();

        assertWrittenRow(1);
        verify(transaction).commit();
    }

    @Test
    void closeWithoutEndOfInputDoesNotCommitPendingRows() throws Exception {
        harness.processElement(row(1), 0L);
        harness.processElement(row(2), 0L);
        harness.processElement(row(3), 0L);
        verify(transaction).modifyRows(any(ModifyRowsRequest.Builder.class));
        verify(transaction, never()).commit();

        closeHarness();

        verify(client).close();
        verify(transaction).modifyRows(any(ModifyRowsRequest.Builder.class));
        verify(transaction, never()).commit();
    }

    @Test
    void commitFailurePreventsCheckpointWithoutRetry() throws Exception {
        when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("commit failed")));
        harness.processElement(row(1), 0L);

        IOException failure = assertThrows(IOException.class, () -> harness.prepareSnapshotPreBarrier(1L));

        assertThat(failure).hasRootCauseInstanceOf(IOException.class).hasRootCauseMessage("commit failed");
        assertThrows(IOException.class, () -> harness.prepareSnapshotPreBarrier(2L));
        verify(client).startTransaction(any(StartTransaction.class));
        verify(transaction).commit();
    }

    private void closeHarness() throws Exception {
        if (harness != null) {
            OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> currentHarness = harness;
            harness = null;
            currentHarness.close();
        }
    }

    private void assertWrittenRow(long id) {
        ArgumentCaptor<ModifyRowsRequest.Builder> captor = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(transaction).modifyRows(captor.capture());
        ModifyRowsRequest request = captor.getValue().build();
        assertThat(request.getPath()).isEqualTo(QUEUE_PATH);
        request.convertValues(DefaultSerializationResolver.getInstance());
        assertThat(request.getRows()).hasSize(1);
        YTreeMapNode written = request.getRows().get(0).toYTreeMap(request.getSchema(), false);
        assertThat(written.getLong("id")).isEqualTo(id);
        assertThat(written.getString("name")).isEqualTo("row-" + id);
    }

    private static GenericRowData row(long id) {
        return GenericRowData.of(id, StringData.fromString("row-" + id));
    }

    private static YTreeNode queueMetadata() {
        return YTree.builder().beginAttributes()
                .key("dynamic").value(true)
                .key("sorted").value(false)
                .key("schema").value(QUEUE_SCHEMA.toYTree())
                .key("tablet_count").value(1)
                .endAttributes().entity().build();
    }
}
