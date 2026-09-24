package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nullable;

public final class YtValueCodecs {
    public static final String NONE = "none";

    static final String SUPPORTED_CODECS = "none, zstd_1..zstd_21";

    private static final String ZSTD_PREFIX = "zstd_";

    private static final int MAX_ZSTD_LEVEL = 21;

    private static final YtValueCodec IDENTITY = compressed -> compressed;

    private final Map<String, YtValueCodec> cache = new HashMap<>();

    /**
     * Resolves a YTsaurus compression codec by its name as it is stored in a queue row.
     *
     * @param codecName codec name; null or blank means that the value is not compressed
     * @return codec that decompresses values written with {@code codecName}
     */
    public YtValueCodec forName(@Nullable String codecName) {
        if (codecName == null || codecName.isBlank()) {
            return IDENTITY;
        }
        String normalizedCodecName = codecName.trim().toLowerCase(Locale.ROOT);
        YtValueCodec cached = cache.get(normalizedCodecName);
        if (cached != null) {
            return cached;
        }
        YtValueCodec resolved = resolve(normalizedCodecName);
        cache.put(normalizedCodecName, resolved);
        return resolved;
    }

    private static YtValueCodec resolve(String codecName) {
        if (NONE.equals(codecName)) {
            return IDENTITY;
        }
        if (codecName.startsWith(ZSTD_PREFIX)) {
            validateZstdLevel(codecName);
            return new ZstdValueCodec();
        }
        throw new IllegalArgumentException(unsupported(codecName));
    }

    private static void validateZstdLevel(String codecName) {
        int level;
        try {
            level = Integer.parseInt(codecName.substring(ZSTD_PREFIX.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(unsupported(codecName), e);
        }
        if (level < 1 || level > MAX_ZSTD_LEVEL || !codecName.equals(ZSTD_PREFIX + level)) {
            throw new IllegalArgumentException(unsupported(codecName));
        }
    }

    private static String unsupported(String codecName) {
        return "Unsupported YTsaurus value codec: '" + codecName + "'. Supported codecs: " + SUPPORTED_CODECS;
    }
}
