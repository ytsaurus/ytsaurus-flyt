package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueSplitTest {
    @Test
    void splitIdentityDoesNotDependOnOffset() {
        YtQueueSplit first = split("queue-object", 7, 12);
        YtQueueSplit advanced = split("queue-object", 7, 99);

        assertThat(first.splitId()).isEqualTo("queue-object:7");
        assertThat(advanced.splitId()).isEqualTo(first.splitId());
    }

    @Test
    void mutableStateConvertsBackToSplit() {
        YtQueueSplitState state = new YtQueueSplitState(split("queue-object", 3, 10));

        state.setNextOffset(25);

        assertThat(state.getQueueObjectId()).isEqualTo("queue-object");
        assertThat(state.getPartitionIndex()).isEqualTo(3);
        assertThat(state.getNextOffset()).isEqualTo(25);
        assertThat(state.toSplit()).isEqualTo(split("queue-object", 3, 25));
    }

    @Test
    void offsetsMustRepresentUnreadRows() {
        YtQueueSplitState state = new YtQueueSplitState(split("queue-object", 0, 0));

        assertThatThrownBy(() -> new YtQueueSplit("queue-object", 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> state.setNextOffset(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
