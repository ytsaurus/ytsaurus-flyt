package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata;

import java.io.IOException;

@FunctionalInterface
public interface YtQueueMetadataProvider extends AutoCloseable {
    YtQueueMetadata getMetadata() throws Exception;

    @Override
    default void close() throws IOException {
    }
}
