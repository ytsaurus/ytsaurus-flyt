package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import javax.annotation.Nullable;

import lombok.Getter;

import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtWriterOptions.ROWS_LIMIT;

@Getter
public final class YtQueueWriterOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Duration MIN_TRANSACTION_TIMEOUT = Duration.ofSeconds(1);

    private final int rowsInModificationLimit;
    private final int rowsInTransactionLimit;
    private final Duration flushModificationPeriod;
    private final Duration commitTransactionPeriod;
    private final Duration transactionTimeout;
    @Nullable
    private final Integer partitionIndex;

    public YtQueueWriterOptions(
            int rowsInModificationLimit,
            int rowsInTransactionLimit,
            Duration flushModificationPeriod,
            Duration commitTransactionPeriod,
            Duration transactionTimeout,
            @Nullable Integer partitionIndex) {
        if (rowsInModificationLimit <= 0) {
            throw new IllegalArgumentException("'rows-in-modification-limit' must be greater than zero");
        }
        if (rowsInTransactionLimit <= 0) {
            throw new IllegalArgumentException("'rows-in-transaction-limit' must be greater than zero");
        }
        if ((long) rowsInModificationLimit + rowsInTransactionLimit > ROWS_LIMIT) {
            throw new IllegalArgumentException(
                    "The sum of 'rows-in-modification-limit' and 'rows-in-transaction-limit' " +
                            "must not exceed " + ROWS_LIMIT);
        }
        validateDuration(flushModificationPeriod, "flush-modification-period", true);
        validateDuration(commitTransactionPeriod, "commit-transaction-period", true);
        validateDuration(transactionTimeout, "transaction-timeout", false);
        if (transactionTimeout.compareTo(MIN_TRANSACTION_TIMEOUT) < 0) {
            throw new IllegalArgumentException("'transaction-timeout' must be at least one second");
        }
        if (partitionIndex != null && partitionIndex < 0) {
            throw new IllegalArgumentException("'sink.partition-index' must be nonnegative");
        }
        this.rowsInModificationLimit = rowsInModificationLimit;
        this.rowsInTransactionLimit = rowsInTransactionLimit;
        this.flushModificationPeriod = flushModificationPeriod;
        this.commitTransactionPeriod = commitTransactionPeriod;
        this.transactionTimeout = transactionTimeout;
        this.partitionIndex = partitionIndex;
    }

    private static void validateDuration(Duration value, String name, boolean allowZero) {
        Objects.requireNonNull(value, "'" + name + "' must not be null");
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
