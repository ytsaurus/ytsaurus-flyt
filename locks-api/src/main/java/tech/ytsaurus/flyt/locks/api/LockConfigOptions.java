package tech.ytsaurus.flyt.locks.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LockConfigOptions {

    public static final String LOCKS_OPTIONS_PREFIX = "locks.";

    public static final ConfigOption<String> LOCKS_PROVIDER =
            ConfigOptions.key("locks.provider")
                    .stringType()
                    .defaultValue("noop")
                    .withDescription("Type of locks provider: yt, noop, etc.");

    private LockConfigOptions() {
    }

}
