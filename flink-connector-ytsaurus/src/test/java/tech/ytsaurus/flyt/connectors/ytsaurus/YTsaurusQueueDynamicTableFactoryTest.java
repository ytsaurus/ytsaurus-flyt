package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table.YtQueueDynamicTableSource;
import tech.ytsaurus.flyt.formats.yson.YsonFormatFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.PARTITION_DISCOVERY_INTERVAL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.SPECIFIC_OFFSETS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.STARTUP_MODE;

class YTsaurusQueueDynamicTableFactoryTest {
    @Test
    void discoversFactoryByQueueIdentifier() {
        DynamicTableSourceFactory factory = FactoryUtil.discoverFactory(
                getClass().getClassLoader(),
                DynamicTableSourceFactory.class,
                YTsaurusQueueDynamicTableFactory.IDENTIFIER);

        assertThat(factory).isInstanceOf(YTsaurusQueueDynamicTableFactory.class);
    }

    @Test
    void createsSourceFromSqlOptions() {
        Map<String, String> options = validSqlOptions();
        options.put("scan.startup.mode", "SPECIFIC");
        options.put("scan.startup.specific-offsets", "10;20;30");
        options.put("scan.async.worker-count", "2");
        options.put("scan.async.buffer-capacity", "4");
        options.put("scan.parallelism", "2");
        DynamicTableSource source = createSource(options);

        assertThat(source).isInstanceOf(YtQueueDynamicTableSource.class);
        assertThat(source).extracting("specificOffsets")
                .isEqualTo(List.of(10L, 20L, 30L));
    }

    @Test
    void createsSourceWithLatestStartupMode() {
        Map<String, String> options = validSqlOptions();
        options.put("scan.startup.mode", "LATEST");

        DynamicTableSource source = createSource(options);

        assertThat(source).extracting("startupMode")
                .isEqualTo(YtQueueStartupMode.LATEST);
    }

    @Test
    void rejectsPropertiesFormatInsteadOfApplyingDifferentEffectiveOptions() {
        Map<String, String> options = Map.of(
                "connector", YTsaurusQueueDynamicTableFactory.IDENTIFIER,
                "proxy", "localhost:9013",
                "path", "//tmp/queue",
                "credentials-source", "options",
                "username", "user",
                "token", "token",
                "format", YsonFormatFactory.IDENTIFIER,
                "properties.format", "json");

        assertThrows(ValidationException.class, () -> createSource(options));
    }

    @Test
    void validatesStartupModeAndOffsetsTogether() {
        Configuration options = new Configuration();
        options.set(STARTUP_MODE, YtQueueStartupMode.SPECIFIC);
        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateScanOptions(options));

        options.set(SPECIFIC_OFFSETS, List.of(0L, 10L));
        YTsaurusQueueDynamicTableFactory.validateScanOptions(options);

        options.set(STARTUP_MODE, YtQueueStartupMode.LATEST);
        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateScanOptions(options));
    }

    @Test
    void rejectsEmptyAndNegativeSpecificOffsets() {
        Configuration options = new Configuration();
        options.set(STARTUP_MODE, YtQueueStartupMode.SPECIFIC);
        options.set(SPECIFIC_OFFSETS, List.of());

        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateScanOptions(options));

        options.set(SPECIFIC_OFFSETS, List.of(0L, -1L));
        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateScanOptions(options));
    }

    @ParameterizedTest(name = "{0}={1}")
    @CsvSource({
            "scan.max-row-count, 0",
            "scan.max-data-weight, 0 bytes",
            "scan.poll-backoff, 0 ms",
            "scan.async.worker-count, 0",
            "scan.async.buffer-capacity, 0"
    })
    void rejectsInvalidReaderOptionsFromSql(String option, String value) {
        Map<String, String> options = validSqlOptions();
        options.put(option, value);

        ValidationException failure = assertThrows(
                ValidationException.class,
                () -> createSource(options));

        assertThat(failure).hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDiscoveryIntervalBelowSchedulerPrecision() {
        Configuration options = new Configuration();
        options.set(PARTITION_DISCOVERY_INTERVAL, Duration.ofNanos(1));

        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateScanOptions(options));
    }

    @Test
    void rejectsFormatsOtherThanYson() {
        Configuration options = new Configuration();
        options.set(FactoryUtil.FORMAT, "json");

        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateFormatIdentifier(options));
    }

    @Test
    void requiresExplicitCredentialsForOptionsProvider() {
        Configuration options = new Configuration();
        options.set(CREDENTIALS_SOURCE, "options");

        assertThrows(
                ValidationException.class,
                () -> new YTsaurusQueueDynamicTableFactory().validateCredentialsOptions(options));
    }

    @Test
    void rejectsBlankRequiredValuesAndNonPositiveParallelism() {
        Configuration options = new Configuration();
        options.setString("proxy", "hahn");
        options.setString("path", " ");
        options.setString("credentials-source", "env");

        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateRequiredOptions(options));

        options.setString("path", "//tmp/queue");
        options.set(FactoryUtil.SOURCE_PARALLELISM, 0);
        assertThrows(
                ValidationException.class,
                () -> YTsaurusQueueDynamicTableFactory.validateRequiredOptions(options));
    }

    private static DynamicTableSource createSource(Map<String, String> options) {
        Schema schema = Schema.newBuilder().column("payload", DataTypes.STRING()).build();
        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(schema)
                .options(options)
                .build();
        ResolvedCatalogTable resolvedTable = new ResolvedCatalogTable(
                catalogTable,
                ResolvedSchema.of(Column.physical("payload", DataTypes.STRING())));
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("catalog", "database", "queue"),
                resolvedTable,
                new Configuration(),
                YTsaurusQueueDynamicTableFactoryTest.class.getClassLoader(),
                false);
    }

    private static Map<String, String> validSqlOptions() {
        return new HashMap<>(Map.of(
                "connector", YTsaurusQueueDynamicTableFactory.IDENTIFIER,
                "proxy", "localhost:9013",
                "path", "//tmp/queue",
                "credentials-source", "options",
                "username", "user",
                "token", "token",
                "format", YsonFormatFactory.IDENTIFIER));
    }
}
