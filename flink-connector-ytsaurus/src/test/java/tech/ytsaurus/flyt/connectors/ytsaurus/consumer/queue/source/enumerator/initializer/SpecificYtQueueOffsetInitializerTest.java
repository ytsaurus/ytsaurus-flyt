package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpecificYtQueueOffsetInitializerTest {
    @Test
    void mapsOffsetsByPartitionIndex() throws Exception {
        SpecificYtQueueOffsetInitializer initializer =
                new SpecificYtQueueOffsetInitializer(List.of(10L, 20L, 30L, 40L));

        Map<Integer, Long> offsets = initializer.getInitialOffsets(
                new YtQueueMetadata("queue-object", 4),
                List.of(1, 2, 3));

        assertThat(offsets).containsExactly(
                Map.entry(1, 20L),
                Map.entry(2, 30L),
                Map.entry(3, 40L));
    }

    @Test
    void rejectsPartitionWithoutConfiguredOffset() {
        SpecificYtQueueOffsetInitializer initializer =
                new SpecificYtQueueOffsetInitializer(List.of(10L, 20L, 30L));

        assertThatThrownBy(() -> initializer.getInitialOffsets(
                new YtQueueMetadata("queue-object", 4),
                List.of(3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition 3")
                .hasMessageContaining("configured offsets count: 3");
    }
}
