package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import java.time.Duration;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class YtQueueReaderOptionsTest {
    @Test
    void exposesValidOptions() {
        YtQueueReaderOptions options =
                new YtQueueReaderOptions(100, 1024, Duration.ofMillis(250), 3, 5);

        assertThat(options.getMaxRows()).isEqualTo(100);
        assertThat(options.getMaxDataWeightBytes()).isEqualTo(1024);
        assertThat(options.getEmptyPollBackoff()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getWorkerCount()).isEqualTo(3);
        assertThat(options.getBufferCapacity()).isEqualTo(5);
    }

    @ParameterizedTest(name = "{5}")
    @MethodSource("invalidOptions")
    void rejectsInvalidOptions(
            int maxRows,
            long maxDataWeightBytes,
            Duration emptyPollBackoff,
            int workerCount,
            int bufferCapacity,
            String expectedMessage,
            Class<? extends Throwable> expectedException) {
        assertThatThrownBy(() -> new YtQueueReaderOptions(
                maxRows,
                maxDataWeightBytes,
                emptyPollBackoff,
                workerCount,
                bufferCapacity))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedMessage);
    }

    private static Stream<Arguments> invalidOptions() {
        return Stream.of(
                arguments(0, 1024, Duration.ofMillis(250), 1, 2,
                        "maxRows", IllegalArgumentException.class),
                arguments(100, 0, Duration.ofMillis(250), 1, 2,
                        "maxDataWeightBytes", IllegalArgumentException.class),
                arguments(100, 1024, null, 1, 2,
                        "emptyPollBackoff", NullPointerException.class),
                arguments(100, 1024, Duration.ZERO, 1, 2,
                        "emptyPollBackoff", IllegalArgumentException.class),
                arguments(100, 1024, Duration.ofNanos(-1), 1, 2,
                        "emptyPollBackoff", IllegalArgumentException.class),
                arguments(100, 1024, Duration.ofSeconds(Long.MAX_VALUE), 1, 2,
                        "emptyPollBackoff", IllegalArgumentException.class),
                arguments(100, 1024, Duration.ofMillis(250), 0, 2,
                        "workerCount", IllegalArgumentException.class),
                arguments(100, 1024, Duration.ofMillis(250), 1, 0,
                        "bufferCapacity", IllegalArgumentException.class));
    }
}
