package tech.ytsaurus.flyt.connectors.datametrics;

import lombok.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

@Value
public class DataMetricsConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String DATA_METRICS_TABLE_ALIAS_KEY = "data_metrics.table_alias";
    private static final String DATA_METRICS_TIMESTAMP_PREFIX = "data_metrics.timestamp.";

    String metricTablePathAlias;
    List<DataMetricsMetricConfig> metrics;

    public DataMetricsConfig(String metricTablePathAlias, List<DataMetricsMetricConfig> metrics) {
        this.metricTablePathAlias = metricTablePathAlias;
        this.metrics = metrics != null ? metrics : new ArrayList<>();
    }

    public static DataMetricsConfig fromConfiguration(Configuration config) {
        String alias = config.getString(DATA_METRICS_TABLE_ALIAS_KEY, null);
        if (alias == null || alias.isEmpty()) {
            return null;
        }

        List<DataMetricsMetricConfig> metrics = new ArrayList<>();
        Map<String, String> properties = config.toMap();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(DATA_METRICS_TIMESTAMP_PREFIX) && key.endsWith(".column")) {
                String metricName = key.substring(
                        DATA_METRICS_TIMESTAMP_PREFIX.length(),
                        key.length() - ".column".length()
                );

                String columnName = entry.getValue();

                metrics.add(DataMetricsMetricConfig.builder()
                        .metricName(metricName)
                        .columnName(columnName)
                        .build());
            }
        }

        return new DataMetricsConfig(alias, metrics);
    }

    public static DataMetricsConfig fromConfiguration(ReadableConfig config) {
        if (config instanceof Configuration) {
            return fromConfiguration((Configuration) config);
        }

        String alias = config.getOptional(
                org.apache.flink.configuration.ConfigOptions.key(DATA_METRICS_TABLE_ALIAS_KEY)
                        .stringType()
                        .noDefaultValue()
        ).orElse(null);

        if (alias == null || alias.isEmpty()) {
            return null;
        }

        return new DataMetricsConfig(alias, new ArrayList<>());
    }

    public static DataMetricsConfig fromProperties(java.util.Properties properties) {
        String alias = properties.getProperty(DATA_METRICS_TABLE_ALIAS_KEY);
        if (alias == null || alias.isEmpty()) {
            return null;
        }

        List<DataMetricsMetricConfig> metrics = new ArrayList<>();

        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(DATA_METRICS_TIMESTAMP_PREFIX) && key.endsWith(".column")) {
                String metricName = key.substring(
                        DATA_METRICS_TIMESTAMP_PREFIX.length(),
                        key.length() - ".column".length()
                );

                String columnName = properties.getProperty(key);

                metrics.add(DataMetricsMetricConfig.builder()
                        .metricName(metricName)
                        .columnName(columnName)
                        .build());
            }
        }

        return new DataMetricsConfig(alias, metrics);
    }

    public boolean isEmpty() {
        return metrics.isEmpty();
    }
}
