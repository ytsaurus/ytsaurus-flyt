package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import javax.annotation.Nullable;

import lombok.Getter;

@Getter
public final class YtQueueWriterOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int batchSize;
    private final Duration flushInterval;
    private final Duration requestTimeout;
    @Nullable
    private final Integer partitionIndex;

    public YtQueueWriterOptions(
            int batchSize,
            Duration flushInterval,
            Duration requestTimeout,
            @Nullable Integer partitionIndex) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("'sink.buffer-flush.max-rows' must be greater than zero");
        }
        validateDuration(flushInterval, "sink.buffer-flush.interval", true);
        validateDuration(requestTimeout, "sink.request-timeout", false);
        if (requestTimeout.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException("'sink.request-timeout' must be at least one second");
        }
        if (partitionIndex != null && partitionIndex < 0) {
            throw new IllegalArgumentException("'sink.partition-index' must be nonnegative");
        }
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.requestTimeout = requestTimeout;
        this.partitionIndex = partitionIndex;
    }

    private static void validateDuration(Duration value, String name, boolean allowZero) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || (!allowZero && value.isZero())) {
            throw new IllegalArgumentException("'" + name + "' must be " +
                    (allowZero ? "nonnegative" : "greater than zero"));
        }
        try {
            if (!value.isZero() && value.toMillis() == 0) {
                throw new IllegalArgumentException("'" + name + "' must be at least one millisecond");
            }
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("'" + name + "' is too large", e);
        }
    }
}
