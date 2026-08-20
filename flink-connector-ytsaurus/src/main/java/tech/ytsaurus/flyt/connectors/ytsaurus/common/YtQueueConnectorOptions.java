package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.time.Duration;
import java.util.List;

import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueReadMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;

@UtilityClass
public class YtQueueConnectorOptions {

    public static final ConfigOption<YtQueueStartupMode> STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(YtQueueStartupMode.class)
                    .defaultValue(YtQueueStartupMode.EARLIEST);

    public static final ConfigOption<List<Long>> SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.startup.specific-offsets")
                    .longType()
                    .asList()
                    .noDefaultValue();

    public static final ConfigOption<YtQueueTrimmedOffsetPolicy> TRIMMED_OFFSET_POLICY =
            ConfigOptions.key("scan.trimmed-offset-policy")
                    .enumType(YtQueueTrimmedOffsetPolicy.class)
                    .defaultValue(YtQueueTrimmedOffsetPolicy.FAIL);

    public static final ConfigOption<Integer> MAX_ROW_COUNT =
            ConfigOptions.key("scan.max-row-count")
                    .intType()
                    .defaultValue(1000);

    public static final ConfigOption<MemorySize> MAX_DATA_WEIGHT =
            ConfigOptions.key("scan.max-data-weight")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(16));

    public static final ConfigOption<Duration> POLL_BACKOFF =
            ConfigOptions.key("scan.poll-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(250));

    public static final ConfigOption<Integer> ASYNC_WORKER_COUNT =
            ConfigOptions.key("scan.async.worker-count")
                    .intType()
                    .defaultValue(1);

    public static final ConfigOption<Integer> ASYNC_BUFFER_CAPACITY =
            ConfigOptions.key("scan.async.buffer-capacity")
                    .intType()
                    .defaultValue(2);

    public static final ConfigOption<Duration> PARTITION_DISCOVERY_INTERVAL =
            ConfigOptions.key("scan.partition-discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60));

    public static final ConfigOption<YtQueueReadMode> READ_MODE =
            ConfigOptions.key("scan.read-mode")
                    .enumType(YtQueueReadMode.class)
                    .defaultValue(YtQueueReadMode.ROW);

    public static final ConfigOption<String> VALUE_COLUMN =
            ConfigOptions.key("scan.value-column")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> CODEC_COLUMN =
            ConfigOptions.key("scan.codec-column")
                    .stringType()
                    .noDefaultValue();
}
