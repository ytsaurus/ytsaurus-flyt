package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsSupplier {

    private final String groupName;

    private final Map<String, Supplier<Long>> metrics = new HashMap<>();

    public MetricsSupplier(String groupName) {
        this.groupName = groupName;
    }

    public Supplier<Long> getMetric(String metricName) {
        return metrics.get(metricName);
    }

    public void setMetric(String metricName, Supplier<Long> metric) {
        Supplier<Long> prev = metrics.put(metricName, metric);
        if (prev != null) {
            Long currentValue = metric.get();
            Long previousValue = prev.get();

            if (previousValue != -1) {
                log.warn("Non-closed metric value of {} for {} (group {}) ", previousValue, metricName, groupName);
            }

            log.info("New metric was set for {} " +
                    "(last value of previous: {}, first value of current: {}). " +
                    "Group name: {}", metricName, previousValue, currentValue, groupName);
        }
    }
}
