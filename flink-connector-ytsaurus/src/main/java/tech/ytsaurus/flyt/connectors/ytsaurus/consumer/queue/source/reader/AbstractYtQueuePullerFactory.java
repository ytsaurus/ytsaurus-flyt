package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;

abstract class AbstractYtQueuePullerFactory implements YtQueuePullerFactory {
    private final YTsaurusClient client;

    private final Object lifecycleMonitor = new Object();

    private boolean closed;

    AbstractYtQueuePullerFactory(YTsaurusClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public final YtQueuePuller get() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("Queue puller factory is closed");
            }
            return Objects.requireNonNull(
                    createPuller((ApiServiceClient) client),
                    "createPuller returned null");
        }
    }

    protected abstract YtQueuePuller createPuller(ApiServiceClient client);

    @Override
    public final void close() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            client.close();
        }
    }
}
