package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.Objects;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitState;

public final class YtQueueRecordEmitter<T>
        implements RecordEmitter<YtQueueRawRecord, T, YtQueueSplitState> {
    private final YtQueueRecordDeserializer<T> deserializer;

    public YtQueueRecordEmitter(YtQueueRecordDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    @Override
    public void emitRecord(
            YtQueueRawRecord element,
            SourceOutput<T> output,
            YtQueueSplitState splitState) throws Exception {
        if (element.getPartitionIndex() != splitState.getPartitionIndex()) {
            throw new IllegalArgumentException("Record partition does not match split state");
        }
        T record = deserializer.deserialize(element.getRow(), element.getSchema());
        if (record != null) {
            output.collect(record);
        }
        splitState.setNextOffset(element.getOffset() + 1);
    }
}
