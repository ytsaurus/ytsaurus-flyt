package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import lombok.Value;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

@Value
public class YtQueueEnumeratorState {
    @Nullable
    String queueObjectId;
    int initializedPartitionCount;
    List<YtQueueSplit> unassignedSplits;

    public YtQueueEnumeratorState(
            @Nullable String queueObjectId,
            int initializedPartitionCount,
            List<YtQueueSplit> unassignedSplits) {
        if (initializedPartitionCount < 0) {
            throw new IllegalArgumentException("initializedPartitionCount must not be negative");
        }
        if (queueObjectId != null && queueObjectId.isEmpty()) {
            throw new IllegalArgumentException("queueObjectId must not be empty");
        }
        if (queueObjectId == null && initializedPartitionCount != 0) {
            throw new IllegalArgumentException("queueObjectId is required for initialized partitions");
        }

        this.queueObjectId = queueObjectId;
        this.initializedPartitionCount = initializedPartitionCount;

        List<YtQueueSplit> splits = new ArrayList<>(Objects.requireNonNull(
                unassignedSplits,
                "unassignedSplits"));
        splits.sort(Comparator.comparingInt(YtQueueSplit::getPartitionIndex));
        Set<Integer> partitions = new HashSet<>();
        for (YtQueueSplit split : splits) {
            Objects.requireNonNull(split, "unassignedSplits contains null");
            if (queueObjectId == null || !queueObjectId.equals(split.getQueueObjectId())) {
                throw new IllegalArgumentException("Unassigned split belongs to a different queue object");
            }
            if (split.getPartitionIndex() >= initializedPartitionCount) {
                throw new IllegalArgumentException("Unassigned split has not been initialized");
            }
            if (!partitions.add(split.getPartitionIndex())) {
                throw new IllegalArgumentException("Duplicate unassigned split partition");
            }
        }
        this.unassignedSplits = Collections.unmodifiableList(splits);
    }

    public static YtQueueEnumeratorState empty() {
        return new YtQueueEnumeratorState(null, 0, List.of());
    }

    @Nullable
    public String getQueueObjectId() {
        return queueObjectId;
    }
}
