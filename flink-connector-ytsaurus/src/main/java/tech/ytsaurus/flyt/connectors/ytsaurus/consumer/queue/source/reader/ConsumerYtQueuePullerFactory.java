package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;

public final class ConsumerYtQueuePullerFactory extends AbstractYtQueuePullerFactory {
    private final String consumerPath;

    private final String queuePath;

    public ConsumerYtQueuePullerFactory(
            YTsaurusClient client,
            String consumerPath,
            String queuePath) {
        super(client);
        this.consumerPath = Objects.requireNonNull(consumerPath, "consumerPath");
        this.queuePath = Objects.requireNonNull(queuePath, "queuePath");
    }

    @Override
    protected YtQueuePuller createPuller(ApiServiceClient client) {
        return new ConsumerYtQueuePuller(client, consumerPath, queuePath);
    }
}
