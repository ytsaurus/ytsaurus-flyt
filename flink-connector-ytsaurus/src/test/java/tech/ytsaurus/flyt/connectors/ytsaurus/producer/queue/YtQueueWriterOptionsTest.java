package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.time.Duration;

import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtQueueWriterOptionsTest {
    @Test
    void keepsDisabledTimerAndPartitionAfterSerialization() throws Exception {
        YtQueueWriterOptions options = new YtQueueWriterOptions(20, Duration.ZERO, Duration.ofSeconds(3), 2);

        YtQueueWriterOptions restored = InstantiationUtil.clone(options, getClass().getClassLoader());

        assertThat(restored.getBatchSize()).isEqualTo(20);
        assertThat(restored.getFlushInterval()).isZero();
        assertThat(restored.getRequestTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(restored.getPartitionIndex()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonpositiveBatchSize(int batchSize) {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(batchSize, Duration.ZERO, Duration.ofSeconds(1), null));
    }

    @Test
    void rejectsNegativePartition() {
        assertThrows(IllegalArgumentException.class,
                () -> new YtQueueWriterOptions(1, Duration.ZERO, Duration.ofSeconds(1), -1));
    }

    @Test
    void rejectsInvalidTimeoutsAndTimerPrecision() {
        for (Duration timeout : new Duration[]{Duration.ZERO, Duration.ofMillis(-1), Duration.ofNanos(1),
                Duration.ofMillis(1), Duration.ofMillis(999),
                Duration.ofSeconds(Long.MAX_VALUE)}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new YtQueueWriterOptions(1, Duration.ZERO, timeout, null));
        }
        for (Duration interval : new Duration[]{Duration.ofMillis(-1), Duration.ofNanos(1),
                Duration.ofSeconds(Long.MAX_VALUE)}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new YtQueueWriterOptions(1, interval, Duration.ofSeconds(1), null));
        }
    }
}
