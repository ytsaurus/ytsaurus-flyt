package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata;

import java.util.Objects;

import lombok.Value;

@Value
public class YtQueueMetadata {
    String queueObjectId;
    int partitionCount;

    public YtQueueMetadata(String queueObjectId, int partitionCount) {
        this.queueObjectId = Objects.requireNonNull(queueObjectId, "queueObjectId");
        if (queueObjectId.isEmpty()) {
            throw new IllegalArgumentException("queueObjectId must not be empty");
        }
        if (partitionCount < 0) {
            throw new IllegalArgumentException("partitionCount must not be negative");
        }
        this.partitionCount = partitionCount;
    }
}
