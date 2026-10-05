package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rows.QueueRowset;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

abstract class AbstractYtQueuePuller implements YtQueuePuller {
    @Nullable
    private final AutoCloseable ownedClient;

    private final Object lifecycleMonitor = new Object();

    @Nullable
    private Operation inFlight;

    private boolean closed;

    AbstractYtQueuePuller(@Nullable AutoCloseable ownedClient) {
        this.ownedClient = ownedClient;
    }

    @Override
    public final CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request) {
        Objects.requireNonNull(request, "request");
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
                        pullRows(request),
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

    protected abstract CompletableFuture<QueueRowset> pullRows(YtQueuePullRequest request);

    @Override
    public final void wakeUp() {
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
    public final void close() throws Exception {
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
