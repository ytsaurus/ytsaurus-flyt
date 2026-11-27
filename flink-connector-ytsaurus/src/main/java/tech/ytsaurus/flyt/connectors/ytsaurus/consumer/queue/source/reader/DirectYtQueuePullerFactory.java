package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import tech.ytsaurus.client.YTsaurusClient;

import java.util.Objects;

import javax.annotation.Nullable;

public final class DirectYtQueuePullerFactory implements YtQueuePullerFactory {
    private final YTsaurusClient client;

    private final String queuePath;

    @Nullable
    private final String consumerPath;

    private final Object lifecycleMonitor = new Object();

    private boolean closed;

    public DirectYtQueuePullerFactory(YTsaurusClient client, String queuePath) {
        this(client, queuePath, null);
    }

    public DirectYtQueuePullerFactory(YTsaurusClient client, String queuePath, @Nullable String consumerPath) {
        this.client = Objects.requireNonNull(client, "YT client must not be null");
        this.queuePath = Objects.requireNonNull(queuePath, "Queue path must not be null");
        this.consumerPath = consumerPath;
    }

    @Override
    public YtQueuePuller get() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("Queue puller factory is closed");
            }
            return new DirectYtQueuePuller(client, queuePath, consumerPath, null);
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
}
