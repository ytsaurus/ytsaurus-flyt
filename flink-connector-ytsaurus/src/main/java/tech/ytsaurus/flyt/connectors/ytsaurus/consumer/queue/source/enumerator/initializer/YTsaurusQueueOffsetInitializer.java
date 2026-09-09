package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

public final class YTsaurusQueueOffsetInitializer implements YtQueueOffsetInitializer {
    private final YTsaurusClient client;

    private final YPath queuePath;

    private final YtQueueStartupMode startupMode;

    public YTsaurusQueueOffsetInitializer(
            YTsaurusClient client,
            String queuePath,
            YtQueueStartupMode startupMode) {
        this.client = Objects.requireNonNull(client, "client");
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
        this.startupMode = Objects.requireNonNull(startupMode, "startupMode");
    }

    @Override
    public Map<Integer, Long> getInitialOffsets(
            YtQueueMetadata metadata,
            List<Integer> partitionIndexes) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(partitionIndexes, "partitionIndexes");
        for (int partitionIndex : partitionIndexes) {
            if (partitionIndex < 0 || partitionIndex >= metadata.getPartitionCount()) {
                throw new IllegalArgumentException(
                        "partitionIndex must be between zero and the queue partition count");
            }
        }
        if (partitionIndexes.isEmpty()) {
            return Map.of();
        }

        List<TabletInfo> tabletInfos = client.getTabletInfos(GetTabletInfos.builder()
                .setPath(queuePath.toString())
                .setTabletIndexes(partitionIndexes)
                .build()).join();
        if (tabletInfos.size() != partitionIndexes.size()) {
            throw new IllegalStateException(
                    "Expected " + partitionIndexes.size() +
                            " tablet infos, got " + tabletInfos.size());
        }

        Map<Integer, Long> offsets = new LinkedHashMap<>(partitionIndexes.size());
        for (int index = 0; index < partitionIndexes.size(); index++) {
            offsets.put(
                    partitionIndexes.get(index),
                    getInitialOffset(tabletInfos.get(index)));
        }
        return offsets;
    }

    private long getInitialOffset(TabletInfo tabletInfo) {
        switch (startupMode) {
            case EARLIEST:
                return tabletInfo.getTrimmedRowCount();
            case LATEST:
                return tabletInfo.getTotalRowCount();
            default:
                throw new IllegalArgumentException(
                        "Unsupported YTsaurus queue offset startup mode: " + startupMode);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
