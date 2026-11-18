package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtStaticSink;

public final class YtStaticConnectorOptions {
    public static final ConfigOption<Duration> ROLL_INTERVAL = ConfigOptions.key("sink.roll-interval")
            .durationType().defaultValue(YtStaticSink.DEFAULT_ROLL_INTERVAL)
            .withDescription("Maximum processing time of a nonempty static-table upload before publication.");
    public static final ConfigOption<Duration> WRITE_TIMEOUT = ConfigOptions.key("sink.write-timeout")
            .durationType().defaultValue(YtStaticSink.DEFAULT_WRITE_TIMEOUT)
            .withDescription("Bounds open, readiness and close waits and streaming RPC inactivity.");

    private YtStaticConnectorOptions() { }
}
