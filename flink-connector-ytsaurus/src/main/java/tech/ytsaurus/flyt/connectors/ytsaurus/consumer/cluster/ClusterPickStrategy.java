package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.flink.configuration.ReadableConfig;

import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtClusterUtils;

public interface ClusterPickStrategy extends Closeable, Serializable {
    void open(ReadableConfig options);

    String pickCluster();

    default boolean isAvailable(String cluster) {
        return YtClusterUtils.isAvailable(cluster);
    }

    String getName();

    Duration getPollingPeriod();

    static Map<String, ClusterPickStrategy> loadAvailable() {
        return ServiceLoader.load(ClusterPickStrategy.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toMap(ClusterPickStrategy::getName, Function.identity()));
    }

    static ClusterPickStrategy loadOrThrow(String name) {
        return Optional.ofNullable(loadAvailable().getOrDefault(name, null))
                .orElseThrow(() -> new RuntimeException("Could not find cluster pick strategy '" + name + "'"));
    }
}
