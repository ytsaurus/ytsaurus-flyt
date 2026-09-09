package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

public final class YtQueueTestFixtures {
    private YtQueueTestFixtures() {
    }

    public static YtQueueMetadata metadata(String queueObjectId, int partitionCount) {
        return new YtQueueMetadata(queueObjectId, partitionCount);
    }

    public static YtQueueSplit split(String queueObjectId, int partitionIndex, long nextOffset) {
        return new YtQueueSplit(queueObjectId, partitionIndex, nextOffset);
    }
}
