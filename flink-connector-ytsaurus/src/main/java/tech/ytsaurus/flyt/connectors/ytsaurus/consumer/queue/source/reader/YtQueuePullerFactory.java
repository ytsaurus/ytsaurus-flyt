package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;
import java.util.function.Supplier;

@FunctionalInterface
public interface YtQueuePullerFactory extends Supplier<YtQueuePuller>, AutoCloseable {
    @Override
    YtQueuePuller get();

    @Override
    default void close() throws Exception {
    }

    static YtQueuePullerFactory fromSupplier(Supplier<? extends YtQueuePuller> supplier) {
        Objects.requireNonNull(supplier, "supplier");
        return () -> Objects.requireNonNull(
                supplier.get(),
                "supplier returned null puller");
    }
}
