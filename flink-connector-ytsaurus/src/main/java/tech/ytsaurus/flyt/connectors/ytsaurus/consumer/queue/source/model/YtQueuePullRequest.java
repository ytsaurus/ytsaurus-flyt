package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public final class YtQueuePullRequest {
    private final int partitionIndex;
    private final long offset;
    private final int maxRows;
    private final long maxDataWeightBytes;
}
