package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata;

import java.util.List;
import java.util.Objects;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.MasterReadKind;
import tech.ytsaurus.client.request.MasterReadOptions;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeNode;

public final class YTsaurusQueueMetadataProvider implements YtQueueMetadataProvider {
    private static final String ID_ATTRIBUTE = "id";
    private static final String TABLET_COUNT_ATTRIBUTE = "tablet_count";

    private final YTsaurusClient client;

    private final YPath queuePath;

    public YTsaurusQueueMetadataProvider(YTsaurusClient client, String queuePath) {
        this.client = Objects.requireNonNull(client, "client");
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
    }

    @Override
    public YtQueueMetadata getMetadata() {
        GetNode request = GetNode.builder()
                .setPath(queuePath)
                .setAttributes(List.of(ID_ATTRIBUTE, TABLET_COUNT_ATTRIBUTE))
                .setMasterReadOptions(new MasterReadOptions().setReadFrom(MasterReadKind.Leader))
                .build();
        YTreeNode queueNode = client.getNode(request).join();
        String queueObjectId = queueNode.getAttributeOrThrow(ID_ATTRIBUTE).stringValue();
        long tabletCount = queueNode.getAttributeOrThrow(TABLET_COUNT_ATTRIBUTE).longValue();
        return new YtQueueMetadata(queueObjectId, Math.toIntExact(tabletCount));
    }

    @Override
    public void close() {
        client.close();
    }
}
