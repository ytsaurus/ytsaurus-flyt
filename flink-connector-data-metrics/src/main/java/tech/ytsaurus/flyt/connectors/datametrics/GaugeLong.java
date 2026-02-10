package tech.ytsaurus.flyt.connectors.datametrics;

import org.apache.flink.metrics.Gauge;

import java.util.function.LongSupplier;

/**
 * A simple Gauge implementation for long values.
 * This is a local copy to avoid dependency on flink-common-metrics.
 */
public class GaugeLong implements Gauge<Long> {
    private final LongSupplier supplier;

    public GaugeLong(LongSupplier supplier) {
        this.supplier = supplier;
    }

    @Override
    public Long getValue() {
        return supplier.getAsLong();
    }
}
