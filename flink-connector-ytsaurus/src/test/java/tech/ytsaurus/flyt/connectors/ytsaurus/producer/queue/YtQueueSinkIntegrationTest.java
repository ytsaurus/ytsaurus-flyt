package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PullQueue;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.RowBatchReadOptions;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs against an explicitly selected local YTsaurus cluster with authentication disabled. */
@EnabledIfEnvironmentVariable(named = "YT_QUEUE_TEST_PROXY", matches = ".+")
@Timeout(120)
class YtQueueSinkIntegrationTest {
    // Two transactions of six rows each, followed by one row flushed at end of input.
    private static final int ROW_COUNT = 13;
    private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING())).getLogicalType();
    private static final TableSchema ROW_SCHEMA = TableSchema.builder()
            .addValue("id", ColumnValueType.INT64)
            .addValue("name", ColumnValueType.STRING)
            .build();
    private static final TableSchema COLUMN_SCHEMA = TableSchema.builder()
            .addValue("payload", ColumnValueType.STRING)
            .build();

    private String proxy;
    private String queuePath;
    private YTsaurusClient client;
    private boolean queueCreated;
    private boolean queueMounted;

    @BeforeEach
    void setUp() {
        proxy = System.getenv("YT_QUEUE_TEST_PROXY");
        queuePath = "//tmp/flyt_queue_sink_v2_" + UUID.randomUUID().toString().replace("-", "");
        client = YtUtils.makeYtClient(proxy, new OAuthCredentialsConfig("root", "local-test"), false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            try {
                if (queueMounted) {
                    client.unmountTableAndWaitTablets(UnmountTable.builder().setPath(queuePath).build())
                            .get(30, TimeUnit.SECONDS);
                }
                if (queueCreated) {
                    client.removeNode(RemoveNode.builder().setPath(YPath.simple(queuePath)).setForce(true).build())
                            .get(30, TimeUnit.SECONDS);
                }
            } finally {
                client.close();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(YtQueueWriteMode.class)
    void boundedFlinkJobPreservesOrderAcrossTransactions(YtQueueWriteMode mode) throws Exception {
        createQueue(mode);
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "0");
        configuration.setString("restart-strategy.type", "none");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        List<RowData> input = LongStream.rangeClosed(1, ROW_COUNT).mapToObj(YtQueueSinkIntegrationTest::row)
                .collect(Collectors.toList());
        environment.fromCollection(input, InternalTypeInfo.of(ROW_TYPE)).sinkTo(createSink(mode, true));

        environment.execute("YT queue Sink V2 integration: " + mode);

        List<YTreeMapNode> rows = readRows();
        assertThat(rows).hasSize(ROW_COUNT);
        ObjectMapper mapper = new ObjectMapper();
        List<Long> ids = new ArrayList<>();
        for (YTreeMapNode written : rows) {
            if (mode == YtQueueWriteMode.ROW) {
                long id = written.getLong("id");
                ids.add(id);
                assertThat(written.getString("name")).isEqualTo(name(id));
            } else {
                JsonNode payload = mapper.readTree(written.getString("payload"));
                long id = payload.get("id").longValue();
                ids.add(id);
                assertThat(payload.get("name").textValue()).isEqualTo(name(id));
            }
        }
        assertThat(ids).containsExactlyElementsOf(
                LongStream.rangeClosed(1, ROW_COUNT).boxed().collect(Collectors.toList()));
    }

    @Test
    void checkpointPublishesPartialBatch() throws Exception {
        createQueue(YtQueueWriteMode.ROW);
        try (OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> harness = createHarness()) {
            harness.open();
            harness.processElement(row(1), 0L);
            assertThat(readRows()).isEmpty();

            harness.prepareSnapshotPreBarrier(1L);
            harness.snapshot(1L, 0L);

            List<YTreeMapNode> rows = readRows();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1);
            assertThat(rows.get(0).getString("name")).isEqualTo(name(1));
        }
        assertThat(readRows()).hasSize(1);
    }

    @Test
    void closeWithoutCheckpointDoesNotPublishPendingTransactionOrBuffer() throws Exception {
        createQueue(YtQueueWriteMode.ROW);
        try (OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> harness = createHarness()) {
            harness.open();
            // The third row sends the first two rows to an uncommitted transaction and remains buffered.
            harness.processElement(row(1), 0L);
            harness.processElement(row(2), 0L);
            harness.processElement(row(3), 0L);
            assertThat(readRows()).isEmpty();
        }
        assertThat(readRows()).isEmpty();
    }

    private void createQueue(YtQueueWriteMode mode) throws Exception {
        TableSchema schema = mode == YtQueueWriteMode.ROW ? ROW_SCHEMA : COLUMN_SCHEMA;
        client.createNode(CreateNode.builder()
                .setPath(YPath.simple(queuePath))
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of("dynamic", YTree.booleanNode(true), "schema", schema.toYTree()))
                .build()).get(30, TimeUnit.SECONDS);
        queueCreated = true;
        client.mountTableAndWaitTablets(MountTable.builder().setPath(queuePath).build())
                .get(30, TimeUnit.SECONDS);
        queueMounted = true;
    }

    private Sink<RowData> createSink(YtQueueWriteMode mode, boolean bounded) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "ytsaurus-queue");
        options.put("proxy", proxy);
        options.put("path", queuePath);
        options.put("credentials-source", "options");
        options.put("username", "root");
        options.put("token", "local-test");
        options.put("format", mode == YtQueueWriteMode.ROW ? "yson" : "json");
        options.put("sink.write-mode", mode.name());
        if (mode == YtQueueWriteMode.COLUMN) {
            options.put("sink.value-column", "payload");
        }
        options.put("sink.partition-index", "0");
        options.put("rows-in-modification-limit", "2");
        options.put("rows-in-transaction-limit", "5");
        options.put("flush-modification-period", "0 ms");
        options.put("commit-transaction-period", "0 ms");
        options.put("retry-strategy", "NO_RETRY");
        ResolvedCatalogTable table = new ResolvedCatalogTable(
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.BIGINT())
                                .column("name", DataTypes.STRING()).build())
                        .options(options).build(),
                ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING())));
        DynamicTableSink sink = FactoryUtil.createTableSink(null,
                ObjectIdentifier.of("catalog", "database", "queue"), table,
                new Configuration(), getClass().getClassLoader(), false);
        return ((SinkV2Provider) sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(bounded))).createSink();
    }

    private OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> createHarness() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, CommittableMessage<Void>> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new SinkWriterOperatorFactory<RowData, Void>(createSink(YtQueueWriteMode.ROW, false)),
                        new RowDataSerializer(ROW_TYPE));
        harness.getStreamConfig().setCheckpointingEnabled(true);
        return harness;
    }

    private List<YTreeMapNode> readRows() throws Exception {
        List<YTreeMapNode> rows = new ArrayList<>();
        long offset = 0;
        while (true) {
            QueueRowset batch = client.pullQueue(PullQueue.builder()
                    .setQueuePath(YPath.simple(queuePath)).setPartitionIndex(0).setOffset(offset)
                    .setRowBatchReadOptions(RowBatchReadOptions.builder().setMaxRowCount(ROW_COUNT + 1).build())
                    .build()).get(30, TimeUnit.SECONDS);
            if (batch.getRows().isEmpty()) {
                return rows;
            }
            assertThat(batch.getStartOffset()).isEqualTo(offset);
            batch.getRows().forEach(row -> rows.add(row.toYTreeMap(batch.getSchema(), false)));
            assertThat(rows).hasSizeLessThanOrEqualTo(ROW_COUNT);
            offset = batch.getFinishOffset();
        }
    }

    private static RowData row(long id) {
        return GenericRowData.of(id, StringData.fromString(name(id)));
    }

    private static String name(long id) {
        return "row-" + id + "-тест";
    }
}
