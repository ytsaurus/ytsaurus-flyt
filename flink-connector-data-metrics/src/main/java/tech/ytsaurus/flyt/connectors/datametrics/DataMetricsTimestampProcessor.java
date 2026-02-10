package tech.ytsaurus.flyt.connectors.datametrics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

/**
 * Processor for data metrics timestamp metrics.
 */
@Slf4j
public class DataMetricsTimestampProcessor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String metricPathAlias;
    private final List<DataMetricsMetricConfig> metrics;

    private transient Map<String, AtomicLong> gaugeValues;
    private transient AtomicLong lastCommitTimestamp;
    private transient MetricGroup dataMetricsGroup;

    public DataMetricsTimestampProcessor(String metricPathAlias, List<DataMetricsMetricConfig> metrics) {
        this.metricPathAlias = metricPathAlias;
        this.metrics = metrics;
    }

    public void initialize(MetricGroup parentMetricGroup) {
        gaugeValues = new HashMap<>();
        dataMetricsGroup = parentMetricGroup
                .addGroup("data_metrics")
                .addGroup(metricPathAlias);

        if (dataMetricsGroup instanceof AbstractMetricGroup && ((AbstractMetricGroup<?>) dataMetricsGroup).isClosed()) {
            log.error("Metric group is closed. This will lead to stale metric values! Alias: {}", metricPathAlias);
            return;
        }

        log.info("Data metrics group created: data_metrics.{}", metricPathAlias);

        for (DataMetricsMetricConfig metric : metrics) {
            AtomicLong value = new AtomicLong(0);
            gaugeValues.put(metric.getMetricName(), value);
            // Register gauge directly in dataMetricsGroup with metric name, without creating subgroup
            dataMetricsGroup.gauge(metric.getMetricName(), new GaugeLong(value::get));
            log.info("Registered data metric: data_metrics.{}.{}",
                    metricPathAlias, metric.getMetricName());
        }

        lastCommitTimestamp = new AtomicLong(0);
        dataMetricsGroup.gauge("lastCommitTimestamp", new GaugeLong(lastCommitTimestamp::get));
        log.info("Registered data metric: data_metrics.{}.lastCommitTimestamp",
                metricPathAlias);
    }

    public void updateMetric(String metricName, long value) {
        if (gaugeValues == null) {
            log.warn("Data metrics processor not initialized, cannot update metric: {}", metricName);
            return;
        }
        AtomicLong gauge = gaugeValues.get(metricName);
        if (gauge != null) {
            gauge.set(value);
        } else {
            log.warn("Unknown data metric name: {}", metricName);
        }
    }

    public void updateCommitTimestamp() {
        if (lastCommitTimestamp != null) {
            lastCommitTimestamp.set(System.currentTimeMillis());
        }
    }
}
