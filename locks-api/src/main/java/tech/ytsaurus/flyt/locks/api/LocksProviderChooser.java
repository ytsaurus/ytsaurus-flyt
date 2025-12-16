package tech.ytsaurus.flyt.locks.api;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;

@Slf4j
public class LocksProviderChooser {

    public static final String DEFAULT_NOOP_STRATEGY = "noop";

    private LocksProviderChooser() {
    }

    /**
     * @param strategyName — what the user specified in config (e.g., "yt", "noon"). Can be null.
     * @param config       — Flink config
     */
    public static LocksProvider chooseAndConfigureLocksProvider(String strategyName, ReadableConfig config) {
        ServiceLoader<LocksProvider> load = ServiceLoader.load(LocksProvider.class);
        Map<String, LocksProvider> strategyMap = new HashMap<>();
        load.forEach(strategy -> strategyMap.put(strategy.getStrategyName(), strategy));

        LocksProvider resultStrategy = strategyMap.containsKey(strategyName) ? strategyMap.get(strategyName) :
                strategyMap.get(DEFAULT_NOOP_STRATEGY);

        if (resultStrategy == null) {
            String msg = "No suitable LocksProvider found. Required: " + strategyName +
                    " : Available " + load;
            throw new RuntimeException(msg);
        }
        resultStrategy.configure(config);
        return resultStrategy;
    }
}
