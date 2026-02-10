package tech.ytsaurus.flyt.connectors.datametrics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

/**
 * Configuration for a single data metrics timestamp metric.
 * Metric name already includes aggregation suffix from DSL (e.g. "event_ts.min").
 */
@Value
@Builder
@AllArgsConstructor
public class DataMetricsMetricConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    String columnName;
    String metricName;
}
