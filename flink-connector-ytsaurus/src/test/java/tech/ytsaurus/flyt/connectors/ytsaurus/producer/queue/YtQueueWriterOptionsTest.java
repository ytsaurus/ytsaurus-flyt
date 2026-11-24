package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.time.Duration;
import java.util.stream.Stream;

import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtQueueWriterOptionsTest {
    @ParameterizedTest
    @CsvSource({"0, 3", "2, 0", "2, 3", "0, 0"})
    void keepsLimitsPeriodsAndPartitionAfterSerialization(long flushSeconds, long commitSeconds) throws Exception {
        YtQueueWriterOptions options = new YtQueueWriterOptions(
                20, 80, Duration.ofSeconds(flushSeconds), Duration.ofSeconds(commitSeconds), Duration.ofSeconds(7), 2);

        YtQueueWriterOptions restored = InstantiationUtil.clone(options, getClass().getClassLoader());

        assertThat(restored.getRowsInModificationLimit()).isEqualTo(20);
        assertThat(restored.getRowsInTransactionLimit()).isEqualTo(80);
        assertThat(restored.getFlushModificationPeriod()).isEqualTo(Duration.ofSeconds(flushSeconds));
        assertThat(restored.getCommitTransactionPeriod()).isEqualTo(Duration.ofSeconds(commitSeconds));
        assertThat(restored.getTransactionTimeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(restored.getPartitionIndex()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 1",
            "-1, 1",
            "1, 0",
            "1, -1",
            "1000, 99001",
            "100001, 1",
            "1, 100001",
            "2147483647, 1",
            "1, 2147483647",
            "2147483647, 2147483647"
    })
    void rejectsInvalidRowLimits(int modificationLimit, int transactionLimit) {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(
                        modificationLimit, transactionLimit, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1), null));
    }

    @ParameterizedTest
    @CsvSource({"1, 1", "1, 99999", "99999, 1", "1000, 99000"})
    void acceptsPositiveRowLimitsWithinTotalBound(int modificationLimit, int transactionLimit) {
        YtQueueWriterOptions options = new YtQueueWriterOptions(
                modificationLimit, transactionLimit, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1), null);

        assertThat(options.getRowsInModificationLimit()).isEqualTo(modificationLimit);
        assertThat(options.getRowsInTransactionLimit()).isEqualTo(transactionLimit);
    }

    @Test
    void rejectsNegativePartition() {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, 1, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1), -1));
    }

    @Test
    void acceptsMinimumPeriodsAndTimeout() {
        YtQueueWriterOptions options = new YtQueueWriterOptions(
                1, 1, Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofSeconds(1), null);

        assertThat(options.getFlushModificationPeriod()).isEqualTo(Duration.ofMillis(1));
        assertThat(options.getCommitTransactionPeriod()).isEqualTo(Duration.ofMillis(1));
        assertThat(options.getTransactionTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.getPartitionIndex()).isNull();
    }

    @ParameterizedTest
    @MethodSource("invalidPeriods")
    void rejectsInvalidFlushModificationPeriods(Duration period) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, 1, period, Duration.ZERO, Duration.ofSeconds(1), null)))
                .hasMessageContaining("flush-modification-period");
    }

    @ParameterizedTest
    @MethodSource("invalidPeriods")
    void rejectsInvalidCommitTransactionPeriods(Duration period) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, 1, Duration.ZERO, period, Duration.ofSeconds(1), null)))
                .hasMessageContaining("commit-transaction-period");
    }

    @ParameterizedTest
    @MethodSource("invalidTimeouts")
    void rejectsInvalidTransactionTimeouts(Duration timeout) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, 1, Duration.ZERO, Duration.ZERO, timeout, null)))
                .hasMessageContaining("transaction-timeout");
    }

    private static Stream<Duration> invalidPeriods() {
        return Stream.of(Duration.ofMillis(-1), Duration.ofNanos(1), Duration.ofSeconds(Long.MAX_VALUE));
    }

    private static Stream<Duration> invalidTimeouts() {
        return Stream.of(Duration.ZERO, Duration.ofMillis(-1), Duration.ofNanos(1), Duration.ofMillis(1),
                Duration.ofMillis(999), Duration.ofSeconds(Long.MAX_VALUE));
    }
}
