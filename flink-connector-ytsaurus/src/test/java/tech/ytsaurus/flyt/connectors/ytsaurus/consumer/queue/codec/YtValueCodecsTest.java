package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.rpc.Codec;
import tech.ytsaurus.client.rpc.Compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class YtValueCodecsTest {
    private static final byte[] PAYLOAD =
            "{message=\"hello\";count=42}".repeat(20).getBytes(StandardCharsets.UTF_8);

    private final YtValueCodecs codecs = new YtValueCodecs();

    @Test
    void passesUncompressedValueThroughForNoneAndMissingCodec() {
        assertThat(codecs.forName("none").decompress(PAYLOAD)).isSameAs(PAYLOAD);
        assertThat(codecs.forName(null).decompress(PAYLOAD)).isSameAs(PAYLOAD);
        assertThat(codecs.forName("  ").decompress(PAYLOAD)).isSameAs(PAYLOAD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zstd_1", "zstd_6", "zstd_21", " ZSTD_6 "})
    void decompressesZstdValues(String codecName) {
        byte[] compressed = compressZstd(PAYLOAD, 6);

        assertThat(codecs.forName(codecName).decompress(compressed)).isEqualTo(PAYLOAD);
    }

    @Test
    void decompressesEmptyZstdValue() {
        byte[] compressed = compressZstd(new byte[0], 6);

        assertThat(codecs.forName("zstd_6").decompress(compressed)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"lz4", "lz4_high_compression", "zlib_1", "zlib_6", "zlib_9"})
    void decompressesCodecsSupportedByTheYtsaurusClient(String codecName) {
        byte[] compressed = rpcCodec(codecName).compress(PAYLOAD);

        assertThat(codecs.forName(codecName).decompress(compressed)).isEqualTo(PAYLOAD);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "brotli_3", "snappy", "quick_lz", "zstd_0", "zstd_06", "zstd_22", "zlib_0", "zlib_01",
            "zlib_10", "zstd_x"
    })
    void rejectsUnsupportedCodecs(String codecName) {
        assertThatThrownBy(() -> codecs.forName(codecName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(codecName)
                .hasMessageContaining(YtValueCodecs.SUPPORTED_CODECS);
    }

    @Test
    void reusesResolvedCodecInstances() {
        assertThat(codecs.forName("zstd_6")).isSameAs(codecs.forName("zstd_6"));
        assertThat(codecs.forName("lz4")).isSameAs(codecs.forName("LZ4"));
    }

    @Test
    void doesNotShareCacheBetweenResolvers() {
        assertThat(codecs.forName("lz4"))
                .isNotSameAs(new YtValueCodecs().forName("lz4"));
    }

    @Test
    void rejectsZstdValueWithoutSizeHeader() {
        assertThatThrownBy(() -> codecs.forName("zstd_6").decompress(new byte[] {1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size header");
    }

    @Test
    void rejectsZstdValueWithMismatchedSizeHeader() {
        byte[] compressed = compressZstd(PAYLOAD, 6);
        byte[] corrupted = compressed.clone();
        ByteBuffer.wrap(corrupted, 0, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(PAYLOAD.length - 1L);

        assertThatThrownBy(() -> codecs.forName("zstd_6").decompress(corrupted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uncompressed bytes");
    }

    @Test
    void rejectsZstdValueWithBrokenStream() {
        byte[] compressed = compressZstd(PAYLOAD, 6);
        byte[] truncated = Arrays.copyOf(compressed, compressed.length - 5);

        assertThatThrownBy(() -> codecs.forName("zstd_6").decompress(truncated))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Codec rpcCodec(String codecName) {
        switch (codecName) {
            case "lz4":
                return Codec.codecFor(Compression.Lz4);
            case "lz4_high_compression":
                return Codec.codecFor(Compression.Lz4HighCompression);
            default:
                return Codec.codecFor(Compression.valueOf("Zlib_" + codecName.substring("zlib_".length())));
        }
    }

    /**
     * Mimics the YTsaurus zstd framing: an eight byte little endian uncompressed size, then the stream.
     */
    private static byte[] compressZstd(byte[] payload, int level) {
        byte[] frame = Zstd.compress(payload, level);
        byte[] compressed = new byte[Long.BYTES + frame.length];
        ByteBuffer.wrap(compressed, 0, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(payload.length);
        System.arraycopy(frame, 0, compressed, Long.BYTES, frame.length);
        return compressed;
    }
}
