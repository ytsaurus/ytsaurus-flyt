package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.PullQueue;
import tech.ytsaurus.client.request.RowBatchReadOptions;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

public final class DirectYtQueuePuller implements YtQueuePuller {
    private final ApiServiceClient client;

    private final YPath queuePath;

    private final AutoCloseable ownedClient;

    private final Object lifecycleMonitor = new Object();

    private Operation inFlight;

    private boolean closed;

    public DirectYtQueuePuller(YTsaurusClient client, String queuePath) {
        this(client, queuePath, client);
    }

    public DirectYtQueuePuller(ApiServiceClient client, String queuePath) {
        this(client, queuePath, null);
    }

    private DirectYtQueuePuller(
            ApiServiceClient client,
            String queuePath,
            AutoCloseable ownedClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
        this.ownedClient = ownedClient;
    }

    @Override
    public CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request) {
        Objects.requireNonNull(request, "request");
        PullQueue pullQueue = pullQueueRequest(request);
        Operation operation = new Operation();
        CompletableFuture<QueueRowset> rpcFuture;

        synchronized (lifecycleMonitor) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Queue puller is closed"));
            }
            if (inFlight != null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Concurrent queue pulls are not supported"));
            }

            inFlight = operation;
            try {
                rpcFuture = Objects.requireNonNull(
                        client.pullQueue(pullQueue),
                        "client returned null future");
            } catch (RuntimeException e) {
                rpcFuture = CompletableFuture.failedFuture(e);
            }
            operation.rpcFuture = rpcFuture;
        }

        return rpcFuture
                .thenApply(rowset -> new YtQueueBatch(
                        rowset.getSchema(),
                        rowset.getStartOffset(),
                        rowset.getRows()))
                .whenComplete((ignored, error) -> clearInFlight(operation));
    }

    @Override
    public void wakeUp() {
        CompletableFuture<?> rpcFuture;
        synchronized (lifecycleMonitor) {
            if (inFlight == null) {
                return;
            }
            rpcFuture = inFlight.rpcFuture;
        }
        rpcFuture.cancel(true);
    }

    @Override
    public void close() throws Exception {
        CompletableFuture<?> rpcFuture = null;
        synchronized (lifecycleMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            if (inFlight != null) {
                rpcFuture = inFlight.rpcFuture;
            }
        }
        if (rpcFuture != null) {
            rpcFuture.cancel(true);
        }
        if (ownedClient != null) {
            ownedClient.close();
        }
    }

    private PullQueue pullQueueRequest(YtQueuePullRequest request) {
        return PullQueue.builder()
                .setQueuePath(queuePath)
                .setPartitionIndex(request.getPartitionIndex())
                .setOffset(request.getOffset())
                .setRowBatchReadOptions(RowBatchReadOptions.builder()
                        .setMaxRowCount(request.getMaxRows())
                        .setMaxDataWeight(DataSize.fromBytes(request.getMaxDataWeightBytes()))
                        .build())
                .build();
    }

    private void clearInFlight(Operation operation) {
        synchronized (lifecycleMonitor) {
            if (inFlight == operation) {
                inFlight = null;
            }
        }
    }

    private static final class Operation {
        private CompletableFuture<QueueRowset> rpcFuture;
    }
}
