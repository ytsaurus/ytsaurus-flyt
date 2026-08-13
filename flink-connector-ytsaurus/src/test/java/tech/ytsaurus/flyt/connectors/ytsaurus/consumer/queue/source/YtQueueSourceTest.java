package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor.ManualCredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueEnumeratorStateSerializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_BUFFER_CAPACITY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_WORKER_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_DATA_WEIGHT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_ROW_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.POLL_BACKOFF;

class YtQueueSourceTest {
    @Test
    void buildsWithDefaultsAndSurvivesFlinkSerialization() throws Exception {
        YtQueueSource<String> source = sourceBuilder().build();

        YtQueueSource<String> restored = InstantiationUtil.clone(
                source,
                getClass().getClassLoader());

        assertThat(restored.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(restored.getProducedType()).isEqualTo(Types.STRING);
        assertThat(restored.getSplitSerializer()).isInstanceOf(YtQueueSplitSerializer.class);
        assertThat(restored.getEnumeratorCheckpointSerializer())
                .isInstanceOf(YtQueueEnumeratorStateSerializer.class);
        assertReaderOptions(
                restored,
                MAX_ROW_COUNT.defaultValue(),
                MAX_DATA_WEIGHT.defaultValue().getBytes(),
                POLL_BACKOFF.defaultValue(),
                ASYNC_WORKER_COUNT.defaultValue(),
                ASYNC_BUFFER_CAPACITY.defaultValue());
    }

    @Test
    void validatesStartupModeAndOffsets() {
        assertThatThrownBy(() -> sourceBuilder()
                .startupMode(YtQueueStartupMode.SPECIFIC)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specificOffsets");

        assertThatThrownBy(() -> sourceBuilder()
                .startupMode(YtQueueStartupMode.LATEST)
                .specificOffsets(List.of(0L))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specificOffsets");

        assertThatThrownBy(() -> sourceBuilder()
                .startupMode(YtQueueStartupMode.SPECIFIC)
                .specificOffsets(List.of())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specificOffsets");

        assertThatThrownBy(() -> sourceBuilder()
                .startupMode(YtQueueStartupMode.SPECIFIC)
                .specificOffsets(List.of(0L, -1L))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specificOffsets");

        assertThatThrownBy(() -> sourceBuilder()
                .startupMode(YtQueueStartupMode.SPECIFIC)
                .specificOffsets(Arrays.asList(0L, null))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specificOffsets");
    }

    @Test
    void specificOffsetsSurviveFlinkSerialization() throws Exception {
        YtQueueSource<String> source = sourceBuilder()
                .startupMode(YtQueueStartupMode.SPECIFIC)
                .specificOffsets(List.of(10L, 20L, 30L))
                .build();

        YtQueueSource<String> restored = InstantiationUtil.clone(
                source,
                getClass().getClassLoader());

        assertThat(restored).extracting("specificOffsets")
                .isEqualTo(List.of(10L, 20L, 30L));
    }

    @Test
    void latestStartupModeSurvivesFlinkSerialization() throws Exception {
        YtQueueSource<String> source = sourceBuilder()
                .startupMode(YtQueueStartupMode.LATEST)
                .build();

        YtQueueSource<String> restored = InstantiationUtil.clone(
                source,
                getClass().getClassLoader());

        assertThat(restored).extracting("startupMode")
                .isEqualTo(YtQueueStartupMode.LATEST);
    }

    @Test
    void rejectsDiscoveryIntervalBelowOneMillisecond() {
        assertThatThrownBy(() -> sourceBuilder()
                .discoveryInterval(Duration.ofNanos(1))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one millisecond");
    }

    @Test
    void delegatesReaderOptionsAndSurvivesFlinkSerialization() throws Exception {
        YtQueueReaderOptions readerOptions =
                new YtQueueReaderOptions(101, 2048, Duration.ofSeconds(2), 3, 5);
        YtQueueSource<String> source = sourceBuilder()
                .readerOptions(readerOptions)
                .build();

        YtQueueSource<String> restored = InstantiationUtil.clone(
                source,
                getClass().getClassLoader());

        assertReaderOptions(restored, 101, 2048, Duration.ofSeconds(2), 3, 5);
    }

    private static void assertReaderOptions(
            YtQueueSource<?> source,
            int maxRows,
            long maxDataWeightBytes,
            Duration emptyPollBackoff,
            int workerCount,
            int bufferCapacity) {
        assertThat(source).extracting(
                        "readerOptions.maxRows",
                        "readerOptions.maxDataWeightBytes",
                        "readerOptions.emptyPollBackoff",
                        "readerOptions.workerCount",
                        "readerOptions.bufferCapacity")
                .containsExactly(
                        maxRows,
                        maxDataWeightBytes,
                        emptyPollBackoff,
                        workerCount,
                        bufferCapacity);
    }

    private static YtQueueSource.Builder<String> sourceBuilder() {
        return YtQueueSource.<String>builder()
                .proxy("localhost:9013")
                .queuePath("//tmp/queue")
                .credentialsProvider(new ManualCredentialsProvider("test-user", "test-token"))
                .recordDeserializer((row, schema) -> row.toString())
                .producedType(Types.STRING);
    }
}
