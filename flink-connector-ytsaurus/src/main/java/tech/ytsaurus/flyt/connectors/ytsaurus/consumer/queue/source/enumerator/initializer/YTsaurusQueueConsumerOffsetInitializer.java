package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

public final class YTsaurusQueueConsumerOffsetInitializer implements YtQueueOffsetInitializer {
    private static final String QUEUE_CLUSTER_COLUMN = "queue_cluster";
    private static final String QUEUE_PATH_COLUMN = "queue_path";
    private static final String PARTITION_INDEX_COLUMN = "partition_index";
    private static final String OFFSET_COLUMN = "offset";
    private static final YPath CLUSTER_NAME_PATH = YPath.simple("//sys/@cluster_name");

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addKey(QUEUE_CLUSTER_COLUMN, ColumnValueType.STRING)
            .addKey(QUEUE_PATH_COLUMN, ColumnValueType.STRING)
            .addKey(PARTITION_INDEX_COLUMN, ColumnValueType.UINT64)
            .build()
            .toLookup();

    private final ApiServiceClient client;

    private final YPath consumerPath;

    private final YPath queuePath;

    @Nullable
    private String queueClusterName;

    /** Uses a shared client whose lifecycle is managed by the caller. */
    public YTsaurusQueueConsumerOffsetInitializer(
            ApiServiceClient client,
            String consumerPath,
            String queuePath) {
        this.client = Objects.requireNonNull(client, "YT client must not be null");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "Consumer path must not be null"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "Queue path must not be null"));
    }

    @Override
    public Map<Integer, Long> getInitialOffsets(
            YtQueueMetadata metadata,
            List<Integer> partitionIndexes) {
        Objects.requireNonNull(metadata, "Queue metadata must not be null");
        Objects.requireNonNull(partitionIndexes, "Queue partition indexes must not be null");
        for (int partitionIndex : partitionIndexes) {
            if (partitionIndex < 0 || partitionIndex >= metadata.getPartitionCount()) {
                throw new IllegalArgumentException(
                        "partitionIndex must be between zero and the queue partition count");
            }
        }
        if (partitionIndexes.isEmpty()) {
            return Map.of();
        }

        if (queueClusterName == null) {
            // Consumer keys use the cluster name, which may differ from the proxy endpoint.
            queueClusterName = client.getNode(GetNode.builder()
                    .setPath(CLUSTER_NAME_PATH)
                    .build()).join().stringValue();
        }

        // PullConsumer can return the first available offset instead of the stored, trimmed offset.
        LookupRowsRequest.Builder request = LookupRowsRequest.builder()
                .setPath(consumerPath.toString())
                .setSchema(KEY_SCHEMA)
                .addLookupColumn(OFFSET_COLUMN)
                // Keep missing rows as null slots so results remain aligned with partitionIndexes.
                .setKeepMissingRows(true);
        for (int partitionIndex : partitionIndexes) {
            request.addFilter(queueClusterName, queuePath.toString(), (long) partitionIndex);
        }
        List<YTreeMapNode> rows = client.lookupRows(request.build()).join().getYTreeRows();
        if (rows.size() != partitionIndexes.size()) {
            throw new IllegalStateException(
                    "Expected " + partitionIndexes.size() +
                            " consumer offset rows, got " + rows.size());
        }

        Map<Integer, Long> offsets = new LinkedHashMap<>(partitionIndexes.size());
        for (int index = 0; index < partitionIndexes.size(); index++) {
            long offset = Optional.ofNullable(rows.get(index))
                    .map(row -> row.getLong(OFFSET_COLUMN))
                    .orElse(0L);
            if (offset < 0) {
                throw new IllegalStateException(
                        "Consumer offset must be nonnegative for partition " + partitionIndexes.get(index));
            }
            offsets.put(partitionIndexes.get(index), offset);
        }
        return offsets;
    }
}
