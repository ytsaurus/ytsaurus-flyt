package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;

/**
 * Decompresses values written by the YTsaurus {@code zstd_*} codec: an eight byte little endian
 * uncompressed size followed by the zstd stream itself.
 */
final class ZstdValueCodec implements YtValueCodec {
    private static final int HEADER_SIZE = Long.BYTES;

    private static final long MAX_UNCOMPRESSED_SIZE = Integer.MAX_VALUE - 8L;

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
        long decompressedSize;
        try {
            decompressedSize = Zstd.decompressByteArray(
                    uncompressed, 0, uncompressed.length,
                    compressed, HEADER_SIZE, compressed.length - HEADER_SIZE);
        } catch (ZstdException e) {
            throw new IllegalArgumentException(
                    "Failed to decompress zstd value into " + uncompressedSize + " uncompressed bytes", e);
        }
        if (decompressedSize != uncompressedSize) {
            throw new IllegalArgumentException(String.format(
                    "zstd value declares %d uncompressed bytes but the stream contains %d",
                    uncompressedSize, decompressedSize));
        }
        return uncompressed;
    }
}
