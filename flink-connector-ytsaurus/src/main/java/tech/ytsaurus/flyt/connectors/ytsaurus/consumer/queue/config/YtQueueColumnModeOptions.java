package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

import lombok.Getter;

@Getter
public final class YtQueueColumnModeOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String valueColumn;

    /**
     * Column with the codec name; null means that the payload is never compressed.
     */
    @Nullable
    private final String codecColumn;

    public YtQueueColumnModeOptions(String valueColumn, @Nullable String codecColumn) {
        this.valueColumn = requireNonBlank(valueColumn, "valueColumn");
        this.codecColumn = codecColumn == null ? null : requireNonBlank(codecColumn, "codecColumn");
        if (this.valueColumn.equals(this.codecColumn)) {
            throw new IllegalArgumentException("valueColumn and codecColumn must be different");
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
