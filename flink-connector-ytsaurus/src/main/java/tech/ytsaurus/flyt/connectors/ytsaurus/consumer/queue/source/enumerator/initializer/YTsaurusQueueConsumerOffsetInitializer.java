package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

public final class YTsaurusQueueConsumerOffsetInitializer implements YtQueueOffsetInitializer {
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addKey("queue_cluster", ColumnValueType.STRING)
            .addKey("queue_path", ColumnValueType.STRING)
            .addKey("partition_index", ColumnValueType.UINT64)
            .build()
            .toLookup();

    private final YTsaurusClient client;

    private final YPath consumerPath;

    private final YPath queuePath;

    @Nullable
    private String queueCluster;

    public YTsaurusQueueConsumerOffsetInitializer(
            YTsaurusClient client,
            String consumerPath,
            String queuePath) {
        this.client = Objects.requireNonNull(client, "client");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "consumerPath"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
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

        if (queueCluster == null) {
            queueCluster = client.getNode(GetNode.builder()
                    .setPath(YPath.simple("//sys/@cluster_name"))
                    .build()).join().stringValue();
        }

        // PullConsumer can return the first available offset instead of the stored, trimmed offset.
        LookupRowsRequest.Builder request = LookupRowsRequest.builder()
                .setPath(consumerPath.toString())
                .setSchema(KEY_SCHEMA)
                .addLookupColumn("offset")
                .setKeepMissingRows(true);
        for (int partitionIndex : partitionIndexes) {
            request.addFilter(queueCluster, queuePath.toString(), (long) partitionIndex);
        }
        List<YTreeMapNode> rows = client.lookupRows(request.build()).join().getYTreeRows();
        if (rows.size() != partitionIndexes.size()) {
            throw new IllegalStateException(
                    "Expected " + partitionIndexes.size() +
                            " consumer offset rows, got " + rows.size());
        }

        Map<Integer, Long> offsets = new LinkedHashMap<>(partitionIndexes.size());
        for (int index = 0; index < partitionIndexes.size(); index++) {
            YTreeMapNode row = rows.get(index);
            long offset = row == null ? 0 : row.getLong("offset");
            if (offset < 0) {
                throw new IllegalStateException(
                        "Consumer offset must be nonnegative for partition " + partitionIndexes.get(index));
            }
            offsets.put(partitionIndexes.get(index), offset);
        }
        return offsets;
    }

    @Override
    public void close() {
        client.close();
    }
}
