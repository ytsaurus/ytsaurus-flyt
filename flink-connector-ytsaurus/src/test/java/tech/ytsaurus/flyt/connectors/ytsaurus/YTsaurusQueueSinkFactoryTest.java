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
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue.YtQueueDynamicTableSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue.YtQueueSink;
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

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"false", "true"})
    void createsInsertOnlySinkAndPreservesRuntimeOptionsOnCopy(String useTls) {
        Map<String, String> options = validOptions();
        if (useTls != null) {
            options.put("use-tls", useTls);
        }
        options.put("rows-in-modification-limit", "5");
        options.put("rows-in-transaction-limit", "30");
        options.put("flush-modification-period", "250 ms");
        options.put("commit-transaction-period", "2 s");
        options.put("transaction-timeout", "30 s");
        options.put("sink.partition-index", "2");
        options.put("sink.parallelism", "3");
        DynamicTableSink sink = createSink(options);

        for (DynamicTableSink current : new DynamicTableSink[]{sink, sink.copy()}) {
            assertThat(current).isInstanceOf(YtQueueDynamicTableSink.class);
            assertThat(current.getChangelogMode(ChangelogMode.all())).isEqualTo(ChangelogMode.insertOnly());
            assertThat(current).extracting("writeMode").isEqualTo(YtQueueWriteMode.ROW);
            assertThat(current).extracting("useTls").isEqualTo(Boolean.parseBoolean(useTls));
            assertThat(current).extracting("writerOptions.rowsInModificationLimit").isEqualTo(5);
            assertThat(current).extracting("writerOptions.rowsInTransactionLimit").isEqualTo(30);
            assertThat(current).extracting("writerOptions.flushModificationPeriod").isEqualTo(Duration.ofMillis(250));
            assertThat(current).extracting("writerOptions.commitTransactionPeriod").isEqualTo(Duration.ofSeconds(2));
            assertThat(current).extracting("writerOptions.transactionTimeout").isEqualTo(Duration.ofSeconds(30));
            assertThat(current).extracting("writerOptions.partitionIndex").isEqualTo(2);
            SinkV2Provider provider = (SinkV2Provider) current.getSinkRuntimeProvider(
                    mock(DynamicTableSink.Context.class));
            assertThat(provider.getParallelism()).contains(3);
            assertThat(provider.createSink()).isInstanceOf(YtQueueSink.class);
            assertThat(provider.createSink()).extracting("useTls").isEqualTo(Boolean.parseBoolean(useTls));
        }
    }

    @Test
    void usesSharedWriterDefaults() {
        DynamicTableSink sink = createSink(validOptions());

        assertThat(sink).extracting("writerOptions.rowsInModificationLimit").isEqualTo(1_000);
        assertThat(sink).extracting("writerOptions.rowsInTransactionLimit").isEqualTo(99_000);
        assertThat(sink).extracting("writerOptions.flushModificationPeriod").isEqualTo(Duration.ofSeconds(1));
        assertThat(sink).extracting("writerOptions.commitTransactionPeriod").isEqualTo(Duration.ofSeconds(10));
        assertThat(sink).extracting("writerOptions.transactionTimeout").isEqualTo(Duration.ofMinutes(5));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"EXPONENTIAL", "NO_RETRY"})
    void preservesRetryStrategyInCopyAndRuntime(String configuredStrategy) {
        Map<String, String> options = validOptions();
        if (configuredStrategy != null) {
            options.put("retry-strategy", configuredStrategy);
        }
        DynamicTableSink sink = createSink(options);
        boolean noRetry = "NO_RETRY".equals(configuredStrategy);
        int retries = noRetry ? 0 : 5;
        Duration delay = noRetry ? Duration.ZERO : Duration.ofSeconds(10);

        for (DynamicTableSink current : new DynamicTableSink[]{sink, sink.copy()}) {
            assertRetryStrategy(current, retries, delay);
            SinkV2Provider provider = (SinkV2Provider) current.getSinkRuntimeProvider(
                    mock(DynamicTableSink.Context.class));
            assertRetryStrategy(provider.createSink(), retries, delay);
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
            "rows-in-modification-limit, 0",
            "rows-in-modification-limit, -1",
            "rows-in-modification-limit, 1001",
            "rows-in-modification-limit, 2147483647",
            "rows-in-transaction-limit, 0",
            "rows-in-transaction-limit, -1",
            "rows-in-transaction-limit, 99001",
            "rows-in-transaction-limit, 2147483647",
            "flush-modification-period, -1 ms",
            "flush-modification-period, 1 ns",
            "commit-transaction-period, -1 ms",
            "commit-transaction-period, 1 ns",
            "transaction-timeout, -1 ms",
            "transaction-timeout, 0 ms",
            "transaction-timeout, 1 ns",
            "transaction-timeout, 999 ms",
            "sink.partition-index, -1",
            "sink.parallelism, 0",
            "sink.parallelism, -1",
            "retry-strategy, UNKNOWN",
            "sink.write-mode, UNKNOWN",
            "sink.value-column, payload"
    })
    void rejectsInvalidSinkOptions(String key, String value) {
        Map<String, String> options = validOptions();
        options.put(key, value);
        assertThrows(ValidationException.class, () -> createSink(options));
    }

    @ParameterizedTest
    @CsvSource({
            "flush-modification-period, 0, 10",
            "commit-transaction-period, 1, 0"
    })
    void allowsDisablingEachPeriodIndependently(String key, long flushSeconds, long commitSeconds) {
        Map<String, String> options = validOptions();
        options.put(key, "0 ms");
        DynamicTableSink sink = createSink(options);

        assertThat(sink).extracting("writerOptions.flushModificationPeriod")
                .isEqualTo(Duration.ofSeconds(flushSeconds));
        assertThat(sink).extracting("writerOptions.commitTransactionPeriod")
                .isEqualTo(Duration.ofSeconds(commitSeconds));
        assertThat(sink).extracting("writerOptions.transactionTimeout").isEqualTo(Duration.ofMinutes(5));
    }

    @ParameterizedTest
    @CsvSource({
            "sink.buffer-flush.interval, 1 s",
            "sink.request-timeout, 1 s",
            "sink.buffer-flush.max-rows, 5"
    })
    void rejectsRemovedQueueSpecificOptions(String key, String value) {
        Map<String, String> options = validOptions();
        options.put(key, value);

        assertThat(assertThrows(ValidationException.class, () -> createSink(options)))
                .hasStackTraceContaining("Unsupported options")
                .hasStackTraceContaining(key);
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

    private static void assertRetryStrategy(Object sink, int retries, Duration delay) {
        assertThat(sink).extracting("retryStrategy").satisfies(value -> {
            RetryStrategy strategy = (RetryStrategy) ((SerializableSupplier<?>) value).get();
            assertThat(strategy.getNumRemainingRetries()).isEqualTo(retries);
            assertThat(strategy.getRetryDelay()).isEqualTo(delay);
        });
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
