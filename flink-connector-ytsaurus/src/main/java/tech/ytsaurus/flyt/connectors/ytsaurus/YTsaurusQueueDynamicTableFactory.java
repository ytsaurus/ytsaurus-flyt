package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueColumnModeOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueReadMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table.YtQueueDynamicTableSource;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PROXY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_TOKEN_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_USERNAME_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_BUFFER_CAPACITY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_WORKER_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.CODEC_COLUMN;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_DATA_WEIGHT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_ROW_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.PARTITION_DISCOVERY_INTERVAL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.POLL_BACKOFF;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.READ_MODE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.SPECIFIC_OFFSETS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.STARTUP_MODE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.TRIMMED_OFFSET_POLICY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.VALUE_COLUMN;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table.YtQueueColumnValueDeserializer.DEFAULT_VALUE_COLUMN;

public class YTsaurusQueueDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "ytsaurus-queue";
    static final String SUPPORTED_FORMAT = "yson";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        validateFormatIdentifier(options);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();
        validateChangelogMode(decodingFormat);
        validateRequiredOptions(options);
        validateScanOptions(options);
        validateCredentialsOptions(options);
        CredentialsProvider credentialsProvider = getAndValidateCredentialsProvider(options);
        YtQueueReaderOptions readerOptions = createReaderOptions(options);
        YtQueueColumnModeOptions columnModeOptions = createColumnModeOptions(options);

        return YtQueueDynamicTableSource.builder()
                .proxy(options.get(PROXY))
                .queuePath(options.get(PATH))
                .credentialsProvider(credentialsProvider)
                .decodingFormat(decodingFormat)
                .physicalRowDataType(context.getPhysicalRowDataType())
                .startupMode(options.get(STARTUP_MODE))
                .specificOffsets(options.getOptional(SPECIFIC_OFFSETS).orElse(null))
                .trimmedOffsetPolicy(options.get(TRIMMED_OFFSET_POLICY))
                .readerOptions(readerOptions)
                .readMode(options.get(READ_MODE))
                .columnModeOptions(columnModeOptions)
                .partitionDiscoveryInterval(options.get(PARTITION_DISCOVERY_INTERVAL))
                .parallelism(options.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null))
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(PROXY, PATH, CREDENTIALS_SOURCE, FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                YT_USERNAME_OPTION,
                YT_TOKEN_OPTION,
                STARTUP_MODE,
                SPECIFIC_OFFSETS,
                TRIMMED_OFFSET_POLICY,
                MAX_ROW_COUNT,
                MAX_DATA_WEIGHT,
                POLL_BACKOFF,
                ASYNC_WORKER_COUNT,
                ASYNC_BUFFER_CAPACITY,
                PARTITION_DISCOVERY_INTERVAL,
                READ_MODE,
                VALUE_COLUMN,
                CODEC_COLUMN,
                FactoryUtil.SOURCE_PARALLELISM);
    }

    static void validateRequiredOptions(ReadableConfig options) {
        validateNonBlank(options.get(PROXY), "proxy");
        validateNonBlank(options.get(PATH), "path");
        options.getOptional(CREDENTIALS_SOURCE)
                .ifPresent(credentialsSource -> validateNonBlank(credentialsSource, "credentials-source"));
        options.getOptional(FactoryUtil.SOURCE_PARALLELISM).ifPresent(parallelism -> {
            if (parallelism <= 0) {
                throw new ValidationException("'scan.parallelism' must be greater than zero");
            }
        });
    }

    static void validateScanOptions(ReadableConfig options) {
        validateSpecificOffsets(
                options.get(STARTUP_MODE),
                options.getOptional(SPECIFIC_OFFSETS).orElse(null));

        validatePositiveMilliseconds(
                options.get(PARTITION_DISCOVERY_INTERVAL),
                "scan.partition-discovery.interval");
    }

    private static void validateSpecificOffsets(
            YtQueueStartupMode startupMode,
            @Nullable List<Long> specificOffsets) {
        if (startupMode == YtQueueStartupMode.SPECIFIC) {
            if (specificOffsets == null || specificOffsets.isEmpty()) {
                throw new ValidationException(
                        "'scan.startup.specific-offsets' must contain at least one offset " +
                                "for SPECIFIC startup mode");
            }
            for (int index = 0; index < specificOffsets.size(); index++) {
                Long offset = specificOffsets.get(index);
                if (offset == null || offset < 0) {
                    throw new ValidationException(
                            "'scan.startup.specific-offsets' offset at index " + index +
                                    " must be nonnegative");
                }
            }
        } else if (specificOffsets != null) {
            throw new ValidationException(
                    "'scan.startup.specific-offsets' is only valid for SPECIFIC startup mode");
        }
    }

    static YtQueueReaderOptions createReaderOptions(ReadableConfig options) {
        try {
            return new YtQueueReaderOptions(
                    options.get(MAX_ROW_COUNT),
                    options.get(MAX_DATA_WEIGHT).getBytes(),
                    options.get(POLL_BACKOFF),
                    options.get(ASYNC_WORKER_COUNT),
                    options.get(ASYNC_BUFFER_CAPACITY));
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid queue reader options", e);
        }
    }

    @Nullable
    static YtQueueColumnModeOptions createColumnModeOptions(ReadableConfig options) {
        if (options.get(READ_MODE) != YtQueueReadMode.COLUMN) {
            if (options.getOptional(VALUE_COLUMN).isPresent() || options.getOptional(CODEC_COLUMN).isPresent()) {
                throw new ValidationException(
                        "'scan.value-column' and 'scan.codec-column' are only valid " +
                                "for 'scan.read-mode' = 'COLUMN'");
            }
            return null;
        }
        String valueColumn = options.getOptional(VALUE_COLUMN).orElse(DEFAULT_VALUE_COLUMN);
        try {
            return new YtQueueColumnModeOptions(
                    valueColumn,
                    options.getOptional(CODEC_COLUMN).orElse(null));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new ValidationException("Invalid queue column mode options", e);
        }
    }

    static void validateFormatIdentifier(ReadableConfig options) {
        if (options.get(READ_MODE) == YtQueueReadMode.COLUMN) {
            return;
        }
        String format = options.get(FactoryUtil.FORMAT);
        if (!SUPPORTED_FORMAT.equals(format)) {
            throw new ValidationException(
                    "The 'ytsaurus-queue' connector supports only 'format' = 'yson' for " +
                            "'scan.read-mode' = 'ROW'; in 'COLUMN' mode the column payload is passed " +
                            "to the configured format as is");
        }
    }

    static void validateChangelogMode(DecodingFormat<?> decodingFormat) {
        if (!ChangelogMode.insertOnly().equals(decodingFormat.getChangelogMode())) {
            throw new ValidationException("The decoder for 'ytsaurus-queue' must be insert-only");
        }
    }

    protected void validateCredentialsOptions(ReadableConfig options) {
        if (options.getOptional(CREDENTIALS_SOURCE).isEmpty() ||
                !"options".equalsIgnoreCase(options.get(CREDENTIALS_SOURCE))) {
            return;
        }
        String username = options.getOptional(YT_USERNAME_OPTION).orElse(null);
        String token = options.getOptional(YT_TOKEN_OPTION).orElse(null);
        if (username == null || username.isBlank() || token == null || token.isBlank()) {
            throw new ValidationException(
                    "Non-blank 'username' and 'token' are required for 'credentials-source' = 'options'");
        }
    }

    protected CredentialsProvider getAndValidateCredentialsProvider(ReadableConfig options) {
        return YtConfigUtils.getAndValidateCredentialsProvider(options);
    }

    private static void validateNonBlank(String value, String key) {
        if (value.isBlank()) {
            throw new ValidationException("'" + key + "' must not be blank");
        }
    }

    private static void validatePositiveMilliseconds(Duration value, String key) {
        if (value.isZero() || value.isNegative()) {
            throw new ValidationException("'" + key + "' must be greater than zero");
        }
        try {
            if (value.toMillis() == 0) {
                throw new ValidationException("'" + key + "' must be at least one millisecond");
            }
        } catch (ArithmeticException e) {
            throw new ValidationException("'" + key + "' is too large", e);
        }
    }
}
