package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CLUSTER_PICK_STRATEGY;

public abstract class SimpleClusterPickStrategy implements ClusterPickStrategy {
    protected static final String PERIOD_OPTION_NAME = "period";
    protected static final Duration DEFAULT_PERIOD = Duration.ofMinutes(10);

    private final String name;

    protected boolean open;
    protected ReadableConfig options;
    protected Duration period;

    public SimpleClusterPickStrategy(String name) {
        this.name = name;
    }

    @Override
    public void open(ReadableConfig options) {
        this.options = options;
        this.open = true;
        this.period = mandatory(makeOption(PERIOD_OPTION_NAME).durationType().defaultValue(DEFAULT_PERIOD));
    }

    @Override
    public Duration getPollingPeriod() {
        checkOpen();
        return period;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        this.open = false;
    }

    protected Map<String, ComplexYtPath> getPathMap() {
        checkOpen();
        return YtConfigUtils.getPathMap(options);
    }

    protected ConfigOptions.OptionBuilder makeOption(String key) {
        return ConfigOptions.key(CLUSTER_PICK_STRATEGY.key() + "." + name + "." + key);
    }

    protected <T> T mandatory(ConfigOption<T> option) {
        checkOpen();
        return options.get(option);
    }

    protected <T> T optional(ConfigOption<T> option) {
        checkOpen();
        return options.getOptional(option).orElse(null);
    }

    protected void checkOpen() {
        if (!open) {
            throw new IllegalStateException("ClusterPickStrategy is not open");
        }
    }
}
