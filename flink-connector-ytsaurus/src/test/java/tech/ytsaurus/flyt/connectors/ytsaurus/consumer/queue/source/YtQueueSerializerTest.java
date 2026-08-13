package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueEnumeratorState;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueEnumeratorStateSerializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueSerializerTest {
    @Test
    void splitRoundTrip() throws Exception {
        YtQueueSplitSerializer serializer = new YtQueueSplitSerializer();
        YtQueueSplit split = split("11-22-33-44", 5, 123456789L);

        byte[] bytes = serializer.serialize(split);

        assertThat(serializer.deserialize(serializer.getVersion(), bytes)).isEqualTo(split);
    }

    @Test
    void enumeratorStateRoundTripPreservesQueueObjectIdInitializedPartitionCountAndUnassignedSplits()
            throws Exception {
        YtQueueEnumeratorStateSerializer serializer = new YtQueueEnumeratorStateSerializer();
        YtQueueEnumeratorState state = new YtQueueEnumeratorState(
                "queue-object",
                3,
                List.of(
                        split("queue-object", 2, 22),
                        split("queue-object", 0, 10)));

        byte[] bytes = serializer.serialize(state);

        assertThat(serializer.deserialize(serializer.getVersion(), bytes)).isEqualTo(state);
    }

    @Test
    void emptyEnumeratorStateRoundTrip() throws Exception {
        YtQueueEnumeratorStateSerializer serializer = new YtQueueEnumeratorStateSerializer();
        YtQueueEnumeratorState state = YtQueueEnumeratorState.empty();

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void serializersRejectUnknownVersionsAndTrailingData() throws Exception {
        YtQueueSplitSerializer splitSerializer = new YtQueueSplitSerializer();
        YtQueueEnumeratorStateSerializer stateSerializer = new YtQueueEnumeratorStateSerializer();
        byte[] splitBytes = splitSerializer.serialize(split("queue-object", 0, 0));
        byte[] bytesWithTrailingData = Arrays.copyOf(splitBytes, splitBytes.length + 1);

        assertThatThrownBy(() -> splitSerializer.deserialize(99, splitBytes))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> stateSerializer.deserialize(99, stateSerializer.serialize(
                YtQueueEnumeratorState.empty())))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> splitSerializer.deserialize(splitSerializer.getVersion(), bytesWithTrailingData))
                .isInstanceOf(IOException.class);
    }
}
