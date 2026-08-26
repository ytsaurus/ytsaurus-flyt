package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import lombok.Getter;

@Getter
public final class YtQueueReaderOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int maxRows;

    private final long maxDataWeightBytes;

    private final Duration emptyPollBackoff;

    private final int workerCount;

    private final int bufferCapacity;

    public YtQueueReaderOptions(
            int maxRows,
            long maxDataWeightBytes,
            Duration emptyPollBackoff,
            int workerCount,
            int bufferCapacity) {
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive");
        }
        if (maxDataWeightBytes <= 0) {
            throw new IllegalArgumentException("maxDataWeightBytes must be positive");
        }
        this.emptyPollBackoff = Objects.requireNonNull(emptyPollBackoff, "emptyPollBackoff");
        if (emptyPollBackoff.isNegative() || emptyPollBackoff.isZero()) {
            throw new IllegalArgumentException("emptyPollBackoff must be positive");
        }
        try {
            emptyPollBackoff.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("emptyPollBackoff is too large", e);
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("bufferCapacity must be positive");
        }
        this.maxRows = maxRows;
        this.maxDataWeightBytes = maxDataWeightBytes;
        this.workerCount = workerCount;
        this.bufferCapacity = bufferCapacity;
    }
}
