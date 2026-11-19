package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.time.Duration;
import java.util.stream.Stream;

import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtQueueWriterOptionsTest {
    @ParameterizedTest
    @CsvSource({"0, 3", "2, 0", "2, 3", "0, 0"})
    void keepsIndependentPeriodsAndPartitionAfterSerialization(long flushSeconds, long commitSeconds) throws Exception {
        YtQueueWriterOptions options = new YtQueueWriterOptions(
                20, Duration.ofSeconds(flushSeconds), Duration.ofSeconds(commitSeconds), Duration.ofSeconds(7), 2);

        YtQueueWriterOptions restored = InstantiationUtil.clone(options, getClass().getClassLoader());

        assertThat(restored.getBatchSize()).isEqualTo(20);
        assertThat(restored.getFlushModificationPeriod()).isEqualTo(Duration.ofSeconds(flushSeconds));
        assertThat(restored.getCommitTransactionPeriod()).isEqualTo(Duration.ofSeconds(commitSeconds));
        assertThat(restored.getTransactionTimeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(restored.getPartitionIndex()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonpositiveBatchSize(int batchSize) {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(batchSize, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1), null));
    }

    @Test
    void rejectsNegativePartition() {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1), -1));
    }

    @Test
    void acceptsMinimumPeriodsAndTimeout() {
        YtQueueWriterOptions options = new YtQueueWriterOptions(
                1, Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofSeconds(1), null);

        assertThat(options.getFlushModificationPeriod()).isEqualTo(Duration.ofMillis(1));
        assertThat(options.getCommitTransactionPeriod()).isEqualTo(Duration.ofMillis(1));
        assertThat(options.getTransactionTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.getPartitionIndex()).isNull();
    }

    @ParameterizedTest
    @MethodSource("invalidPeriods")
    void rejectsInvalidFlushModificationPeriods(Duration period) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, period, Duration.ZERO, Duration.ofSeconds(1), null)))
                .hasMessageContaining("flush-modification-period");
    }

    @ParameterizedTest
    @MethodSource("invalidPeriods")
    void rejectsInvalidCommitTransactionPeriods(Duration period) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, Duration.ZERO, period, Duration.ofSeconds(1), null)))
                .hasMessageContaining("commit-transaction-period");
    }

    @ParameterizedTest
    @MethodSource("invalidTimeouts")
    void rejectsInvalidTransactionTimeouts(Duration timeout) {
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, Duration.ZERO, Duration.ZERO, timeout, null)))
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
