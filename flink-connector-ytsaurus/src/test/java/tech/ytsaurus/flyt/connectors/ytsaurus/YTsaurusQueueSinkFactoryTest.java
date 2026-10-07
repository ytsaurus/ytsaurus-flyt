package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.time.Duration;
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
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue.YtQueueDynamicTableSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue.YtQueueSinkFunction;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue.YtQueueWriteMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class YTsaurusQueueSinkFactoryTest {
    @Test
    void discoversSinkWithTheExistingQueueIdentifier() {
        assertThat(FactoryUtil.discoverFactory(
                getClass().getClassLoader(), DynamicTableSinkFactory.class, "ytsaurus-queue"))
                .isInstanceOf(YTsaurusQueueDynamicTableFactory.class);
    }

    @Test
    void createsInsertOnlySinkAndPreservesRuntimeOptionsOnCopy() {
        Map<String, String> options = validOptions();
        options.put("sink.buffer-flush.max-rows", "5");
        options.put("sink.buffer-flush.interval", "250 ms");
        options.put("sink.request-timeout", "30 s");
        options.put("sink.partition-index", "2");
        options.put("sink.parallelism", "3");
        DynamicTableSink sink = createSink(options);

        for (DynamicTableSink current : new DynamicTableSink[]{sink, sink.copy()}) {
            assertThat(current).isInstanceOf(YtQueueDynamicTableSink.class);
            assertThat(current.getChangelogMode(ChangelogMode.all())).isEqualTo(ChangelogMode.insertOnly());
            assertThat(current).extracting("writeMode").isEqualTo(YtQueueWriteMode.ROW);
            assertThat(current).extracting("writerOptions.batchSize").isEqualTo(5);
            assertThat(current).extracting("writerOptions.flushInterval").isEqualTo(Duration.ofMillis(250));
            assertThat(current).extracting("writerOptions.requestTimeout").isEqualTo(Duration.ofSeconds(30));
            assertThat(current).extracting("writerOptions.partitionIndex").isEqualTo(2);
            SinkFunctionProvider provider = (SinkFunctionProvider) current.getSinkRuntimeProvider(
                    mock(DynamicTableSink.Context.class));
            assertThat(provider.getParallelism()).contains(3);
            assertThat(provider.createSinkFunction()).isInstanceOf(YtQueueSinkFunction.class);
        }
    }

    @Test
    void supportsColumnPayloadsWithIndependentReadOptions() {
        Map<String, String> options = validOptions();
        options.put("sink.write-mode", "COLUMN");
        options.put("sink.value-column", "payload");
        // A table can be used as both a source and a sink with separate settings.
        options.put("scan.read-mode", "COLUMN");
        options.put("scan.value-column", "payload");
        DynamicTableSink sink = createSink(options);

        assertThat(sink).extracting("writeMode").isEqualTo(YtQueueWriteMode.COLUMN);
        assertThat(sink.copy()).extracting("valueColumn").isEqualTo("payload");
    }

    @Test
    void columnWriteModeAcceptsJsonButRowWriteModeDoesNot() {
        Map<String, String> options = validOptions();
        options.put("format", "json");
        assertThrows(ValidationException.class, () -> createSink(options));
        options.put("sink.write-mode", "COLUMN");
        DynamicTableSink sink = createSink(options);
        assertThat(sink).extracting("valueColumn").isEqualTo("value");
    }

    @ParameterizedTest(name = "{0}={1}")
    @CsvSource({
            "sink.buffer-flush.max-rows, 0",
            "sink.buffer-flush.max-rows, -1",
            "sink.buffer-flush.interval, -1 ms",
            "sink.buffer-flush.interval, 1 ns",
            "sink.request-timeout, 0 ms",
            "sink.request-timeout, 1 ns",
            "sink.partition-index, -1",
            "sink.parallelism, 0",
            "sink.parallelism, -1",
            "sink.write-mode, UNKNOWN",
            "sink.value-column, payload"
    })
    void rejectsInvalidSinkOptions(String key, String value) {
        Map<String, String> options = validOptions();
        options.put(key, value);
        assertThrows(ValidationException.class, () -> createSink(options));
    }

    @Test
    void allowsDisablingPeriodicFlush() {
        Map<String, String> options = validOptions();
        options.put("sink.buffer-flush.interval", "0 ms");
        assertThat(createSink(options)).extracting("writerOptions.flushInterval").isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsBlankOrSystemPayloadColumn() {
        for (String valueColumn : new String[]{" ", "$tablet_index"}) {
            Map<String, String> options = validOptions();
            options.put("sink.write-mode", "COLUMN");
            options.put("sink.value-column", valueColumn);
            assertThrows(ValidationException.class, () -> createSink(options));
        }
    }

    @Test
    void requiresExplicitCredentialsAndRejectsUnknownOptions() {
        Map<String, String> missingCredentials = validOptions();
        missingCredentials.remove("token");
        assertThrows(ValidationException.class, () -> createSink(missingCredentials));

        Map<String, String> unknownOptions = validOptions();
        unknownOptions.put("properties.format", "json");
        assertThrows(ValidationException.class, () -> createSink(unknownOptions));
    }

    @Test
    void rejectsYsonTypesThatWouldSilentlyLosePrecision() {
        for (DataType type : new DataType[]{
                DataTypes.DECIMAL(12, 2),
                DataTypes.TIME(3),
                DataTypes.ARRAY(DataTypes.DECIMAL(20, 0)),
                DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.TIME(6)))}) {
            Map<String, String> options = validOptions();
            assertThrows(ValidationException.class, () -> createSink(options, type));
            options.put("sink.write-mode", "COLUMN");
            assertThrows(ValidationException.class, () -> createSink(options, type));
        }
    }

    @Test
    void allowsDecimalInJsonPayloadsAndWholeSecondsInYson() {
        Map<String, String> options = validOptions();
        assertThat(createSink(options, DataTypes.TIME(0))).isInstanceOf(YtQueueDynamicTableSink.class);
        options.put("sink.write-mode", "COLUMN");
        options.put("format", "json");
        assertThat(createSink(options, DataTypes.DECIMAL(12, 2))).isInstanceOf(YtQueueDynamicTableSink.class);
    }

    private static DynamicTableSink createSink(Map<String, String> options) {
        return createSink(options, DataTypes.STRING());
    }

    private static DynamicTableSink createSink(Map<String, String> options, DataType fieldType) {
        Schema schema = Schema.newBuilder().column("message", fieldType).build();
        ResolvedCatalogTable table = new ResolvedCatalogTable(
                CatalogTable.newBuilder().schema(schema).options(options).build(),
                ResolvedSchema.of(Column.physical("message", fieldType)));
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("catalog", "database", "queue"),
                table,
                new Configuration(),
                YTsaurusQueueSinkFactoryTest.class.getClassLoader(),
                false);
    }

    private static Map<String, String> validOptions() {
        return new HashMap<>(Map.of(
                "connector", "ytsaurus-queue",
                "proxy", "localhost:18000",
                "path", "//tmp/queue",
                "credentials-source", "options",
                "username", "root",
                "token", "local-test",
                "format", "yson"));
    }
}
