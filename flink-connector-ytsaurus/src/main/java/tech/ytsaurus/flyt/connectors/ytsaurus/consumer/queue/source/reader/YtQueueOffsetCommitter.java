package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.List;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

public interface YtQueueOffsetCommitter extends AutoCloseable {
    void snapshotState(long checkpointId, List<YtQueueSplit> splits);

    void notifyCheckpointComplete(long checkpointId) throws Exception;

    void notifyCheckpointAborted(long checkpointId) throws Exception;

    @Override
    void close() throws Exception;

    static YtQueueOffsetCommitter noOp() {
        return NoOpYtQueueOffsetCommitter.INSTANCE;
    }

    enum NoOpYtQueueOffsetCommitter implements YtQueueOffsetCommitter {
        INSTANCE;

        @Override
        public void snapshotState(long checkpointId, List<YtQueueSplit> splits) {
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
        }

        @Override
        public void close() {
        }
    }
}
