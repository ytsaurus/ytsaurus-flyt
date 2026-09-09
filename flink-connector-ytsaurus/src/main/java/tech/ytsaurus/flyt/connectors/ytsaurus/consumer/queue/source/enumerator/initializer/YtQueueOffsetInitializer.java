package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;

@FunctionalInterface
public interface YtQueueOffsetInitializer extends AutoCloseable {
    Map<Integer, Long> getInitialOffsets(
            YtQueueMetadata metadata,
            List<Integer> partitionIndexes) throws Exception;

    @Override
    default void close() throws IOException {
    }
}
