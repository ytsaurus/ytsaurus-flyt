package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import lombok.experimental.UtilityClass;
import tech.ytsaurus.client.rpc.Codec;
import tech.ytsaurus.client.rpc.Compression;

@UtilityClass
public class YtValueCodecs {
    public static final String NONE = "none";

    static final String SUPPORTED_CODECS =
            "none, zstd_1..zstd_21, lz4, lz4_high_compression, zlib_1..zlib_9";

    private static final String ZSTD_PREFIX = "zstd_";

    private static final String ZLIB_PREFIX = "zlib_";

    private static final String LZ4 = "lz4";

    private static final String LZ4_HIGH_COMPRESSION = "lz4_high_compression";

    private static final int MAX_ZSTD_LEVEL = 21;

    private static final int MAX_ZLIB_LEVEL = 9;

    private static final YtValueCodec IDENTITY = compressed -> compressed;

    private static final ConcurrentMap<String, YtValueCodec> CACHE = new ConcurrentHashMap<>();

    /**
     * Resolves a YTsaurus compression codec by its name as it is stored in a queue row.
     *
     * @param codecName codec name; null or blank means that the value is not compressed
     * @return codec that decompresses values written with {@code codecName}
     */
    public static YtValueCodec forName(@Nullable String codecName) {
        if (codecName == null || codecName.isBlank()) {
            return IDENTITY;
        }
        return CACHE.computeIfAbsent(codecName.trim().toLowerCase(Locale.ROOT), YtValueCodecs::resolve);
    }

    private static YtValueCodec resolve(String codecName) {
        if (NONE.equals(codecName)) {
            return IDENTITY;
        }
        if (LZ4.equals(codecName)) {
            return rpcCodec(Compression.Lz4);
        }
        if (LZ4_HIGH_COMPRESSION.equals(codecName)) {
            return rpcCodec(Compression.Lz4HighCompression);
        }
        if (codecName.startsWith(ZSTD_PREFIX)) {
            parseLevel(codecName, ZSTD_PREFIX, MAX_ZSTD_LEVEL);
            return ZstdValueCodec.INSTANCE;
        }
        if (codecName.startsWith(ZLIB_PREFIX)) {
            int level = parseLevel(codecName, ZLIB_PREFIX, MAX_ZLIB_LEVEL);
            return rpcCodec(Compression.valueOf("Zlib_" + level));
        }
        throw new IllegalArgumentException(unsupported(codecName));
    }

    private static YtValueCodec rpcCodec(Compression compression) {
        Codec codec = Codec.codecFor(compression);
        return codec::decompress;
    }

    private static int parseLevel(String codecName, String prefix, int maxLevel) {
        int level;
        try {
            level = Integer.parseInt(codecName.substring(prefix.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(unsupported(codecName), e);
        }
        if (level < 1 || level > maxLevel) {
            throw new IllegalArgumentException(unsupported(codecName));
        }
        return level;
    }

    private static String unsupported(String codecName) {
        return "Unsupported YTsaurus value codec: '" + codecName + "'. Supported codecs: " + SUPPORTED_CODECS;
    }
}
