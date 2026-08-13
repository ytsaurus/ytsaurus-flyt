package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public final class YtQueueSplitSerializer implements SimpleVersionedSerializer<YtQueueSplit> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(YtQueueSplit split) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(byteStream)) {
            writeString(output, split.getQueueObjectId());
            output.writeInt(split.getPartitionIndex());
            output.writeLong(split.getNextOffset());
        }
        return byteStream.toByteArray();
    }

    @Override
    public YtQueueSplit deserialize(int version, byte[] serialized) throws IOException {
        requireSupportedVersion(version);
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String queueObjectId = readString(input, serialized.length);
            int partitionIndex = input.readInt();
            long nextOffset = input.readLong();
            requireFullyConsumed(input);
            try {
                return new YtQueueSplit(queueObjectId, partitionIndex, nextOffset);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid YT queue split", e);
            }
        }
    }

    public static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    public static String readString(DataInputStream input, int maximumLength) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximumLength) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void requireFullyConsumed(DataInputStream input) throws IOException {
        if (input.available() != 0) {
            throw new IOException("Unexpected trailing serialized data");
        }
    }

    static void requireSupportedVersion(int version) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported YT queue split serializer version: " + version);
        }
    }
}
