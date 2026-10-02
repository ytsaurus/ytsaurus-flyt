package tech.ytsaurus.flyt.connectors.ytsaurus.producer.integration;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real sorted-table Flink SQL sink with an independent SDK reader. Run with
 * {@code YT_TEST_PROXY=localhost:18000 ./gradlew :flink-connector-ytsaurus:dynamicTableIntegrationTest}.
 * The cluster must already have a tablet cell and advertise RPC proxies reachable from the host.
 */
@Tag("integration")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class YtDynamicTableSinkIntegrationTest {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private YTsaurusClient client;
    private String proxy;
    private String basePath;

    @BeforeEach
    void connectToExplicitLocalCluster() {
        proxy = requireLocalProxy(System.getenv("YT_TEST_PROXY"));
        basePath = "//tmp/ytsaurus_flyt_dynamic_it_" + UUID.randomUUID().toString().replace("-", "");
        // Never inherit a developer's token or username from the environment.
        client = YTsaurusClient.builder()
                .setCluster(proxy)
                .setAuth(YTsaurusClientAuth.builder().setUser("root").setToken("local-test").build())
                .build();
    }

    @AfterEach
    void removeOwnPathAndCloseClient() throws Exception {
        if (client == null) {
            return;
        }
        try {
            client.removeNode(RemoveNode.builder()
                            .setPath(YPath.simple(basePath))
                            .setRecursive(true)
                            .setForce(true)
                            .setTimeout(REQUEST_TIMEOUT)
                            .build())
                    .get(30, TimeUnit.SECONDS);
        } finally {
            client.close();
        }
    }

    @Test
    void sortedSinkWritesMultipleModificationsPerTransactionAndFinishesPartialBatch() throws Exception {
        TableSchema schema = TableSchema.builder()
                .addKey("id", ColumnValueType.INT64)
                .addValue("message", ColumnValueType.STRING)
                .addValue("enabled", ColumnValueType.BOOLEAN)
                .addValue("score", ColumnValueType.DOUBLE)
                .setUniqueKeys(true)
                .build();
        // The non-partitioned writer currently appends the base path's last component.
        // Preserve that existing layout while testing the transaction writer refactoring.
        String tablePath = basePath + "/" + basePath.substring(basePath.lastIndexOf('/') + 1);
        createMountedTable(tablePath, schema);

        TableEnvironment tables = localTables();
        String ysonSchema = YTreeTextSerializer.serialize(schema.toYTree()).replace("'", "''");
        tables.executeSql("CREATE TABLE sorted_sink (" +
                "id BIGINT NOT NULL, message STRING, enabled BOOLEAN, score DOUBLE, " +
                "PRIMARY KEY (id) NOT ENFORCED) WITH (" +
                "'connector' = 'ytsaurus', 'proxy' = '" + proxy + "', 'path' = '" + basePath + "', " +
                "'credentials-source' = 'options', 'username' = 'root', 'token' = 'local-test', " +
                "'schema' = '" + ysonSchema + "', 'sink.parallelism' = '1', " +
                "'rows-in-modification-limit' = '2', 'rows-in-transaction-limit' = '5', " +
                "'flush-modification-period' = '1 h', 'commit-transaction-period' = '1 h', " +
                "'transaction-timeout' = '30 s', 'mount-mode' = 'ALWAYS', 'locks.provider' = 'noop')");
        // The original writer checks thresholds before adding a row: three requests of two rows
        // commit together before row seven. The last three need a full request; finish() flushes row nine.
        executeInsert(tables, "INSERT INTO sorted_sink VALUES " +
                "(30, 'Сортированная таблица 🌍', TRUE, 1.5), " +
                "(31, CAST(NULL AS STRING), FALSE, -2.25), " +
                "(32, '', TRUE, 0.0), " +
                "(33, '漢字', FALSE, 4.25), " +
                "(34, 'third modification', TRUE, 5.5), " +
                "(35, 'transaction boundary', FALSE, 6.75), " +
                "(36, 'second transaction', TRUE, 7.0), " +
                "(37, 'last full batch', FALSE, 8.5), " +
                "(38, 'final partial batch', TRUE, 9.75)");

        List<YTreeMapNode> rows = new ArrayList<>(client.selectRows(SelectRowsRequest.builder()
                        .setQuery("id, message, enabled, score FROM [" + tablePath + "]")
                        .setAllowFullScan(true)
                        .setInputRowsLimit(100L)
                        .setOutputRowsLimit(100L)
                        .setFailOnIncompleteResult(true)
                        .setTimeout(REQUEST_TIMEOUT)
                        .build())
                .get(30, TimeUnit.SECONDS)
                .getYTreeRows());
        rows.sort(Comparator.comparingLong(row -> row.getLong("id")));
        assertThat(rows).hasSize(9);
        assertThat(rows).extracting(row -> row.getLong("id"))
                .containsExactly(30L, 31L, 32L, 33L, 34L, 35L, 36L, 37L, 38L);
        assertThat(rows).extracting(row -> row.getBool("enabled"))
                .containsExactly(true, false, true, false, true, false, true, false, true);
        assertThat(rows).extracting(row -> row.getDouble("score"))
                .containsExactly(1.5, -2.25, 0.0, 4.25, 5.5, 6.75, 7.0, 8.5, 9.75);
        assertThat(rows.get(0).getString("message")).isEqualTo("Сортированная таблица 🌍");
        assertThat(rows.get(1).getOrThrow("message").isEntityNode()).isTrue();
        assertThat(rows.get(2).getString("message")).isEmpty();
        assertThat(rows.get(3).getString("message")).isEqualTo("漢字");
        assertThat(rows.get(4).getString("message")).isEqualTo("third modification");
        assertThat(rows.get(5).getString("message")).isEqualTo("transaction boundary");
        assertThat(rows.get(6).getString("message")).isEqualTo("second transaction");
        assertThat(rows.get(7).getString("message")).isEqualTo("last full batch");
        assertThat(rows.get(8).getString("message")).isEqualTo("final partial batch");
    }

    private void createMountedTable(String tablePath, TableSchema schema) throws Exception {
        client.createNode(CreateNode.builder()
                        .setPath(YPath.simple(tablePath))
                        .setType(CypressNodeType.TABLE)
                        .setRecursive(true)
                        .setAttributes(Map.of(
                                "dynamic", YTree.booleanNode(true),
                                "schema", schema.toYTree()))
                        .setTimeout(REQUEST_TIMEOUT)
                        .build())
                .get(30, TimeUnit.SECONDS);
        // Check the actual schema before any writes to the table.
        TableSchema actualSchema = TableSchema.fromYTree(client.getNode(GetNode.builder()
                        .setPath(YPath.simple(tablePath + "/@schema"))
                        .setTimeout(REQUEST_TIMEOUT)
                        .build())
                .get(30, TimeUnit.SECONDS));
        assertThat(actualSchema.getColumnNames()).containsExactlyElementsOf(schema.getColumnNames());
        assertThat(actualSchema.getKeyColumnsCount()).isEqualTo(schema.getKeyColumnsCount());
        client.mountTableAndWaitTablets(MountTable.builder()
                        .setPath(tablePath)
                        .setTimeout(REQUEST_TIMEOUT)
                        .build())
                .get(30, TimeUnit.SECONDS);
    }

    private static TableEnvironment localTables() {
        Configuration configuration = new Configuration();
        configuration.setString("parallelism.default", "1");
        configuration.setString("restart-strategy.type", "none");
        return TableEnvironment.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build());
    }

    private static void executeInsert(TableEnvironment tables, String statement) throws Exception {
        TableResult result = tables.executeSql(statement);
        try {
            result.await(90, TimeUnit.SECONDS);
        } catch (Exception error) {
            try {
                result.getJobClient().orElseThrow().cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception cancelError) {
                error.addSuppressed(cancelError);
            }
            throw error;
        }
    }

    private static String requireLocalProxy(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Set YT_TEST_PROXY, for example localhost:18000");
        }
        URI uri = URI.create(value.contains("://") ? value : "http://" + value);
        if (!"http".equals(uri.getScheme()) || uri.getHost() == null ||
                !Set.of("localhost", "127.0.0.1", "[::1]").contains(uri.getHost()) ||
                uri.getPort() <= 0 || uri.getRawUserInfo() != null || uri.getRawQuery() != null ||
                uri.getRawFragment() != null || !uri.getRawPath().isEmpty()) {
            throw new IllegalArgumentException("YT_TEST_PROXY must be an explicit local HTTP host:port");
        }
        return uri.getRawAuthority();
    }
}
