package tech.ytsaurus.flyt.connectors.ytsaurus.common.metrics;

import java.util.function.Supplier;

import org.apache.flink.metrics.Gauge;

public class GaugeLong implements Gauge<Long> {
    private final Supplier<Long> supplier;

    public GaugeLong(Supplier<Long> supplier) {
        this.supplier = supplier;
    }

    @Override
    public Long getValue() {
        return supplier.get();
    }
}
