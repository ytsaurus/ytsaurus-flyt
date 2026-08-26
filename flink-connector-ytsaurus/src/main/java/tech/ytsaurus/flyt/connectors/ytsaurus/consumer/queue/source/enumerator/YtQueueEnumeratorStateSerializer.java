package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitSerializer;

public final class YtQueueEnumeratorStateSerializer
        implements SimpleVersionedSerializer<YtQueueEnumeratorState> {
    private static final int VERSION = 1;

    private final YtQueueSplitSerializer splitSerializer = new YtQueueSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(YtQueueEnumeratorState state) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(byteStream)) {
            String queueObjectId = state.getQueueObjectId();
            output.writeBoolean(queueObjectId != null);
            if (queueObjectId != null) {
                YtQueueSplitSerializer.writeString(output, queueObjectId);
            }
            output.writeInt(state.getInitializedPartitionCount());
            output.writeInt(state.getUnassignedSplits().size());
            for (YtQueueSplit split : state.getUnassignedSplits()) {
                byte[] splitBytes = splitSerializer.serialize(split);
                output.writeInt(splitSerializer.getVersion());
                output.writeInt(splitBytes.length);
                output.write(splitBytes);
            }
        }
        return byteStream.toByteArray();
    }

    @Override
    public YtQueueEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported YT queue enumerator state serializer version: " + version);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String queueObjectId = input.readBoolean()
                    ? YtQueueSplitSerializer.readString(input, serialized.length)
                    : null;
            int initializedPartitionCount = input.readInt();
            int splitCount = input.readInt();
            if (splitCount < 0 || splitCount > serialized.length) {
                throw new IOException("Invalid unassigned split count: " + splitCount);
            }

            List<YtQueueSplit> splits = new ArrayList<>(splitCount);
            for (int index = 0; index < splitCount; index++) {
                int splitVersion = input.readInt();
                int splitLength = input.readInt();
                if (splitLength < 0 || splitLength > input.available()) {
                    throw new IOException("Invalid serialized split length: " + splitLength);
                }
                byte[] splitBytes = new byte[splitLength];
                input.readFully(splitBytes);
                splits.add(splitSerializer.deserialize(splitVersion, splitBytes));
            }
            YtQueueSplitSerializer.requireFullyConsumed(input);
            try {
                return new YtQueueEnumeratorState(
                        queueObjectId,
                        initializedPartitionCount,
                        splits);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid YT queue enumerator state", e);
            }
        }
    }
}
