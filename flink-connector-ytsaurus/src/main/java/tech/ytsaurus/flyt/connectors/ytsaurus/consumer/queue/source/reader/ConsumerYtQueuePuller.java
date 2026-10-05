package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.request.RowBatchReadOptions;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

public final class ConsumerYtQueuePuller extends AbstractYtQueuePuller {
    private final ApiServiceClient client;

    private final YPath consumerPath;

    private final YPath queuePath;

    public ConsumerYtQueuePuller(
            YTsaurusClient client,
            String consumerPath,
            String queuePath) {
        super(client);
        this.client = Objects.requireNonNull(client, "client");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "consumerPath"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
    }

    public ConsumerYtQueuePuller(
            ApiServiceClient client,
            String consumerPath,
            String queuePath) {
        super(null);
        this.client = Objects.requireNonNull(client, "client");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "consumerPath"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
    }

    @Override
    protected CompletableFuture<QueueRowset> pullRows(YtQueuePullRequest request) {
        PullConsumer pullConsumer = PullConsumer.builder()
                .setConsumerPath(consumerPath)
                .setQueuePath(queuePath)
                .setPartitionIndex(request.getPartitionIndex())
                .setOffset(request.getOffset())
                .setRowBatchReadOptions(RowBatchReadOptions.builder()
                        .setMaxRowCount(request.getMaxRows())
                        .setMaxDataWeight(DataSize.fromBytes(request.getMaxDataWeightBytes()))
                        .build())
                .build();
        return client.pullConsumer(pullConsumer);
    }
}
