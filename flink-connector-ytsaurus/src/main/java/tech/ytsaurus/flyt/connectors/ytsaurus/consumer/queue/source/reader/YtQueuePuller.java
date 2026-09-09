package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

public interface YtQueuePuller extends AutoCloseable {
    CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request);

    void wakeUp();

    @Override
    void close() throws Exception;
}
