package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split;

import java.io.Serializable;
import java.util.Objects;

import lombok.Value;
import org.apache.flink.api.connector.source.SourceSplit;

@Value
public class YtQueueSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    String queueObjectId;
    int partitionIndex;
    long nextOffset;

    public YtQueueSplit(String queueObjectId, int partitionIndex, long nextOffset) {
        this.queueObjectId = Objects.requireNonNull(queueObjectId, "queueObjectId");
        if (queueObjectId.isEmpty()) {
            throw new IllegalArgumentException("queueObjectId must not be empty");
        }
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must not be negative");
        }
        if (nextOffset < 0) {
            throw new IllegalArgumentException("nextOffset must not be negative");
        }
        this.partitionIndex = partitionIndex;
        this.nextOffset = nextOffset;
    }

    @Override
    public String splitId() {
        return queueObjectId + ':' + partitionIndex;
    }
}
