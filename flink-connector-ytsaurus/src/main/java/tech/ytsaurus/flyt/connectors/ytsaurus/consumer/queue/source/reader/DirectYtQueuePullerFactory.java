package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;

import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;

public final class DirectYtQueuePullerFactory implements YtQueuePullerFactory {
    private final YTsaurusClient client;

    private final String queuePath;

    private final Object lifecycleMonitor = new Object();

    private boolean closed;

    public DirectYtQueuePullerFactory(YTsaurusClient client, String queuePath) {
        this.client = Objects.requireNonNull(client, "client");
        this.queuePath = Objects.requireNonNull(queuePath, "queuePath");
    }

    @Override
    public YtQueuePuller get() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("Queue puller factory is closed");
            }
            return new DirectYtQueuePuller(asNonOwningClient(client), queuePath);
        }
    }

    @Override
    public void close() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            client.close();
        }
    }

    private static ApiServiceClient asNonOwningClient(YTsaurusClient client) {
        return client;
    }
}
