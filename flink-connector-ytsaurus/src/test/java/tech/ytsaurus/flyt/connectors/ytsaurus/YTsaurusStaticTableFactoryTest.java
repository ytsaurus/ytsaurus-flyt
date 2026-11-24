package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtStaticDynamicTableSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtStaticSink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YTsaurusStaticTableFactoryTest {
    @Test
    void discoversStaticFactoryAndCreatesSinkV2WithExactPathAndParallelism() {
        assertThat(FactoryUtil.discoverFactory(getClass().getClassLoader(), DynamicTableSinkFactory.class,
                "ytsaurus-static")).isInstanceOf(YTsaurusStaticTableFactory.class);
        Map<String, String> options = options();
        options.put("sink.parallelism", "4");
        DynamicTableSink tableSink = create(options);
        assertThat(tableSink).isInstanceOf(YtStaticDynamicTableSink.class);
        assertThat(tableSink.asSummaryString()).isEqualTo("Static YT append to //tmp/exact-table");
        SinkV2Provider provider = (SinkV2Provider) tableSink.getSinkRuntimeProvider(null);
        assertThat(provider.createSink()).isInstanceOf(YtStaticSink.class);
        assertThat(provider.getParallelism()).contains(4);
        assertThat(tableSink.copy().asSummaryString()).isEqualTo(tableSink.asSummaryString());
    }

    @Test
    void negotiatesInsertOnlyAndKeepsThisContractInCopies() {
        DynamicTableSink sink = create(options());
        ChangelogMode requested = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER).addContainedKind(RowKind.DELETE).build();
        assertThat(sink.getChangelogMode(requested)).isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink.copy().getChangelogMode(requested)).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void acceptsLegacyFlowAliasesAndProperties() {
        Map<String, String> options = options();
        options.put("clusterName", options.remove("proxy"));
        options.put("credentialsSource", options.remove("credentials-source"));
        options.put("timestampFormat", "ISO_8601");
        options.put("proxyRole", "default");
        options.put("sink.roll-interval", "60000 ms");
        options.put("sink.write-timeout", "300000 ms");
        options.put("properties.attributes.compression_codec", "lz4");
        options.put("properties.attributes.primary_medium", "default");
        assertThat(create(options)).isInstanceOf(YtStaticDynamicTableSink.class);
    }

    @ParameterizedTest
    @CsvSource({"sink.parallelism,0", "sink.roll-interval,0 ms", "sink.roll-interval,5 min",
            "sink.write-timeout,60 s", "sink.write-timeout,0 ms", "path,relative/path",
            "properties.attributes.dynamic,%true", "properties.attributes.schema,[]",
            "properties.attributes.sorted,%true", "sink.roll-size,128 mb"})
    void rejectsInvalidAndUnsupportedOptions(String key, String value) {
        Map<String, String> options = options();
        options.put(key, value);
        assertThrows(ValidationException.class, () -> create(options));
    }

    @Test
    void rejectsSortedSchema() {
        Map<String, String> options = options();
        options.put("schema", "[{name=id;type=int64;sort_order=ascending}]");
        assertThrows(ValidationException.class, () -> create(options));
    }

    private static DynamicTableSink create(Map<String, String> options) {
        CatalogTable table = CatalogTable.newBuilder().schema(Schema.newBuilder()
                .column("id", DataTypes.BIGINT()).build()).options(options).build();
        ResolvedCatalogTable resolved = new ResolvedCatalogTable(table,
                ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())));
        return FactoryUtil.createTableSink(null, ObjectIdentifier.of("catalog", "database", "static_table"),
                resolved, new Configuration(), YTsaurusStaticTableFactoryTest.class.getClassLoader(), false);
    }

    private static Map<String, String> options() {
        return new HashMap<>(Map.of("connector", "ytsaurus-static", "proxy", "localhost:9013",
                "path", "//tmp/exact-table", "schema", "[{name=id;type=int64}]",
                "credentials-source", "options", "username", "root", "token", "local-test"));
    }
}
