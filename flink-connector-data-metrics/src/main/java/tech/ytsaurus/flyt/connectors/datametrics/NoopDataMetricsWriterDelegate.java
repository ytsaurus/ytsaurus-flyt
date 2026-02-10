package tech.ytsaurus.flyt.connectors.datametrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;

/**
 * No-op implementation of {@link DataMetricsWriterDelegate} for when metrics are not configured.
 *
 * <p>This class provides a safe, functional delegate that does nothing, allowing callers
 * to avoid null checks.
 */
public final class NoopDataMetricsWriterDelegate extends DataMetricsWriterDelegate {
    private static final long serialVersionUID = 1L;

    public static final NoopDataMetricsWriterDelegate INSTANCE = new NoopDataMetricsWriterDelegate();

    private NoopDataMetricsWriterDelegate() {
        super(null, null);
    }

    @Override
    public void open(RuntimeContext context) {
        // no-op
    }

    @Override
    public void open(MetricGroup metricGroup) {
        // no-op
    }

    @Override
    public void onRecord(RowData record) {
        // no-op
    }

    @Override
    public void onCommit() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
