package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.github.luben.zstd.ZstdInputStream;

/**
 * Decompresses values written by the YTsaurus {@code zstd_*} codec: an eight byte little endian
 * uncompressed size followed by the zstd stream itself.
 */
final class ZstdValueCodec implements YtValueCodec {
    static final ZstdValueCodec INSTANCE = new ZstdValueCodec();

    private static final int HEADER_SIZE = Long.BYTES;

    private static final long MAX_UNCOMPRESSED_SIZE = Integer.MAX_VALUE - 8L;

    private ZstdValueCodec() {
    }

    @Override
    public byte[] decompress(byte[] compressed) {
        if (compressed.length < HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "zstd value is shorter than the " + HEADER_SIZE + " byte size header: " + compressed.length);
        }
        long uncompressedSize = ByteBuffer.wrap(compressed, 0, HEADER_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
        if (uncompressedSize < 0 || uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
            throw new IllegalArgumentException("Invalid zstd uncompressed size: " + uncompressedSize);
        }

        byte[] uncompressed = new byte[(int) uncompressedSize];
        try (ZstdInputStream input = new ZstdInputStream(
                new ByteArrayInputStream(compressed, HEADER_SIZE, compressed.length - HEADER_SIZE))) {
            int read = input.readNBytes(uncompressed, 0, uncompressed.length);
            if (read != uncompressed.length || input.read() != -1) {
                throw new IllegalArgumentException(String.format(
                        "zstd value declares %d uncompressed bytes but the stream contains a different amount",
                        uncompressedSize));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decompress zstd value", e);
        }
        return uncompressed;
    }
}
