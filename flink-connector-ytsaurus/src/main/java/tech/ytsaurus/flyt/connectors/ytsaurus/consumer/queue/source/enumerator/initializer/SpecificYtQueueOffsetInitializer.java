package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

public final class SpecificYtQueueOffsetInitializer implements YtQueueOffsetInitializer {
    private final List<Long> offsets;

    public SpecificYtQueueOffsetInitializer(List<Long> offsets) {
        this.offsets = offsets;
    }

    @Override
    public Map<Integer, Long> getInitialOffsets(
            YtQueueMetadata metadata,
            List<Integer> partitionIndexes) {
        Map<Integer, Long> initialOffsets = new LinkedHashMap<>(partitionIndexes.size());
        for (int partitionIndex : partitionIndexes) {
            if (partitionIndex < 0 || partitionIndex >= offsets.size()) {
                throw new IllegalArgumentException(
                        "No specific offset configured for partition " + partitionIndex +
                                "; configured offsets count: " + offsets.size());
            }
            initialOffsets.put(partitionIndex, offsets.get(partitionIndex));
        }
        return initialOffsets;
    }
}
