package tech.ytsaurus.flyt.connectors.datametrics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Delegate for collecting timestamp-based metrics from records in Flink sink writers.
 *
 * <p>This component extracts timestamp values from specified columns in each record
 * and exposes them as Flink metrics. Metrics are updated only after successful commit
 * to ensure they reflect actually persisted data.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // In writer constructor or field initialization:
 * private final DataMetricsWriterDelegate dataMetrics =
 *     DataMetricsWriterDelegate.create(config, dataType);
 *
 * // In open():
 * dataMetrics.open(getRuntimeContext());
 *
 * // In write():
 * dataMetrics.onRecord(record);
 *
 * // In commit callback (metrics updated here!):
 * dataMetrics.onCommit();
 *
 * // Or for ack-driven sinks:
 * dataMetrics.onCommitExtracted(record);
 *
 * // In close():
 * dataMetrics.close();
 * }</pre>
 *
 * <p>The delegate handles null/empty config gracefully by returning a no-op instance,
 * so null checks are not required at call sites.
 *
 * <h2>Commit Semantics</h2>
 * <p>Timestamps are accumulated during {@link #onRecord(RowData)} calls but metrics
 * are only updated when {@link #onCommit()} is called. This ensures metrics reflect
 * only successfully committed data, not data that might be rolled back.
 *
 * <h2>Thread Safety</h2>
 * <p>This class is not thread-safe. Each writer instance should have its own delegate.
 *
 * @see DataMetricsConfig
 */
@Slf4j
public class DataMetricsWriterDelegate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataMetricsConfig config;
    private final DataType dataType;

    private transient DataMetricsTimestampProcessor processor;
    private transient Map<String, RowData.FieldGetter> fieldGetters;
    private transient Map<String, Long> pendingValues;
    private transient boolean initialized;

    protected DataMetricsWriterDelegate(DataMetricsConfig config, DataType dataType) {
        this.config = config;
        this.dataType = dataType;
    }

    /**
     * Create a delegate for the given configuration.
     *
     * <p>If config is null or empty, returns a no-op delegate that does nothing.
     * This allows callers to avoid null checks.
     *
     * @param config   data metrics configuration (may be null)
     * @param dataType physical row data type for column index resolution
     * @return delegate instance (never null)
     */
    public static DataMetricsWriterDelegate create(
            @Nullable DataMetricsConfig config,
            DataType dataType) {
        if (config == null || config.isEmpty()) {
            return NoopDataMetricsWriterDelegate.INSTANCE;
        }
        return new DataMetricsWriterDelegate(config, dataType);
    }

    /**
     * Initialize the delegate. Call this in writer's open() method.
     *
     * @param context Flink runtime context for metric registration
     */
    public void open(RuntimeContext context) {
        open(context.getMetricGroup());
    }

    /**
     * Initialize the delegate with a specific metric group.
     *
     * @param metricGroup metric group for registering gauges
     */
    public void open(MetricGroup metricGroup) {
        if (initialized || config == null || config.isEmpty()) {
            return;
        }

        fieldGetters = computeFieldGetters();
        pendingValues = new HashMap<>();
        processor = new DataMetricsTimestampProcessor(
                config.getMetricTablePathAlias(),
                config.getMetrics());
        processor.initialize(metricGroup);

        initialized = true;
        log.info("Data metrics initialized for alias '{}' with {} metrics, column mappings: {}",
                config.getMetricTablePathAlias(),
                config.getMetrics().size(),
                fieldGetters.keySet());
    }

    /**
     * Process a record, accumulating timestamp values.
     * Call this for each record in writer's write() method.
     *
     * <p>Note: This only accumulates values. Metrics are updated in {@link #onCommit()}.
     *
     * @param record the record to process
     */
    public void onRecord(RowData record) {
        if (!initialized || pendingValues == null) {
            return;
        }

        mergeValues(pendingValues, extractValues(record));
    }

    public Map<String, Long> extractValues(RowData record) {
        Map<String, Long> values = new HashMap<>();
        if (!initialized || record == null) {
            return values;
        }

        for (DataMetricsMetricConfig metric : config.getMetrics()) {
            RowData.FieldGetter getter = fieldGetters.get(metric.getMetricName());
            if (getter == null) {
                continue;
            }
            Long timestamp = extractTimestampValue(getter.getFieldOrNull(record));
            if (timestamp != null) {
                mergeAggregateMetric(values, metric.getMetricName(), timestamp);
            }
        }
        return values;
    }

    public void onCommit(Map<String, Long> values) {
        if (!initialized || processor == null || values == null) {
            return;
        }

        for (Map.Entry<String, Long> entry : values.entrySet()) {
            processor.updateMetric(entry.getKey(), entry.getValue());
        }
        processor.updateCommitTimestamp();
    }

    public void onCommitExtracted(RowData record) {
        onCommit(extractValues(record));
    }

    /**
     * Extract long timestamp value from field value.
     * Handles BIGINT (direct Long) and TIMESTAMP types (converts to milliseconds).
     */
    private Long extractTimestampValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof TimestampData) {
            return ((TimestampData) value).getMillisecond();
        }
        log.warn("Unexpected timestamp value type: {}", value.getClass().getName());
        return null;
    }

    /**
     * Notify successful commit. Call this in writer's commit callback.
     *
     * <p>This is where metrics are actually updated with accumulated values.
     * Only call this after data has been successfully committed/persisted.
     */
    public void onCommit() {
        if (!initialized || processor == null || pendingValues == null) {
            return;
        }

        onCommit(pendingValues);
        pendingValues.clear();
    }

    /**
     * Close the delegate and release resources.
     * Call this in writer's close() method.
     */
    public void close() {
        initialized = false;
        processor = null;
        fieldGetters = null;
        pendingValues = null;
    }

    @VisibleForTesting
    boolean isInitialized() {
        return initialized;
    }

    @VisibleForTesting
    Map<String, Long> getPendingValues() {
        return pendingValues;
    }

    /**
     * Merges one metric sample into {@code target}.
     * TODO: support aggregate (min / max / last); currently always max.
     */
    private static void mergeAggregateMetric(Map<String, Long> target, String metricName, long value) {
        target.merge(metricName, value, Long::max);
    }

    private Map<String, RowData.FieldGetter> computeFieldGetters() {
        Map<String, RowData.FieldGetter> getters = new HashMap<>();
        LogicalType logicalType = dataType.getLogicalType();

        if (!(logicalType instanceof RowType)) {
            log.warn("DataType is not RowType, cannot compute column indices: {}", logicalType);
            return getters;
        }

        RowType rowType = (RowType) logicalType;

        for (DataMetricsMetricConfig metric : config.getMetrics()) {
            String columnName = metric.getColumnName();
            int index = rowType.getFieldIndex(columnName);
            if (index >= 0) {
                LogicalType columnType = rowType.getTypeAt(index);
                RowData.FieldGetter getter = buildFieldGetter(columnType, index);
                if (getter != null) {
                    getters.put(metric.getMetricName(), getter);
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Column '%s' has unsupported type '%s' for metric '%s', expected BIGINT or TIMESTAMP",
                            columnName,
                            columnType,
                            metric.getMetricName()));
                }
            } else {
                throw new IllegalArgumentException(String.format(
                        "Column '%s' not found in schema for metric '%s'",
                        columnName, metric.getMetricName()));
            }
        }

        return getters;
    }

    private void mergeValues(Map<String, Long> target, Map<String, Long> values) {
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            mergeAggregateMetric(target, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Build a FieldGetter for the given column type.
     * Uses Flink's RowData.createFieldGetter for optimized field access.
     * Returns null if the type is not BIGINT or TIMESTAMP.
     */
    private RowData.FieldGetter buildFieldGetter(LogicalType columnType, int index) {
        switch (columnType.getTypeRoot()) {
            case BIGINT:
                log.debug("Column index={} mapped as BIGINT", index);
                return RowData.createFieldGetter(columnType, index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                log.debug("Column index={} mapped as TIMESTAMP", index);
                return RowData.createFieldGetter(columnType, index);
            default:
                return null;
        }
    }
}
