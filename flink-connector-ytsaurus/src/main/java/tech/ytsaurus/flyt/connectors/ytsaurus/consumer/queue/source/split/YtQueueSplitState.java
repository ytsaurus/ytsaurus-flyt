package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split;

import java.util.Objects;

import lombok.Getter;

@Getter
public final class YtQueueSplitState {
    private final String queueObjectId;
    private final int partitionIndex;
    private long nextOffset;

    public YtQueueSplitState(YtQueueSplit split) {
        Objects.requireNonNull(split, "split");
        this.queueObjectId = split.getQueueObjectId();
        this.partitionIndex = split.getPartitionIndex();
        this.nextOffset = split.getNextOffset();
    }

    public void setNextOffset(long nextOffset) {
        if (nextOffset < 0) {
            throw new IllegalArgumentException("nextOffset must not be negative");
        }
        this.nextOffset = nextOffset;
    }

    public YtQueueSplit toSplit() {
        return new YtQueueSplit(queueObjectId, partitionIndex, nextOffset);
    }
}
