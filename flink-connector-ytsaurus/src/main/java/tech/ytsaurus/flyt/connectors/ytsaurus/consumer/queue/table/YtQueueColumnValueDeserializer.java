package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.codec.YtValueCodecs;

/**
 * Reads a record payload from a single queue column instead of the whole row. When a codec column is
 * configured, the payload is decompressed with the codec named in that column before it is passed to
 * the wrapped format deserializer; without a codec column the payload is read as is.
 */
public class YtQueueColumnValueDeserializer<T> extends YtQueueDeserializationSchemaAdapter<T> {
    public static final String DEFAULT_VALUE_COLUMN = "value";

    private static final long serialVersionUID = 1L;

    private final String valueColumn;

    @Nullable
    private final String codecColumn;

    public YtQueueColumnValueDeserializer(DeserializationSchema<T> deserializationSchema) {
        this(deserializationSchema, DEFAULT_VALUE_COLUMN, null);
    }

    public YtQueueColumnValueDeserializer(
            DeserializationSchema<T> deserializationSchema,
            String valueColumn) {
        this(deserializationSchema, valueColumn, null);
    }

    public YtQueueColumnValueDeserializer(
            DeserializationSchema<T> deserializationSchema,
            String valueColumn,
            @Nullable String codecColumn) {
        super(deserializationSchema);
        this.valueColumn = requireNonBlank(valueColumn, "valueColumn");
        this.codecColumn = codecColumn == null ? null : requireNonBlank(codecColumn, "codecColumn");
        if (this.valueColumn.equals(this.codecColumn)) {
            throw new IllegalArgumentException("valueColumn and codecColumn must be different");
        }
    }

    @Override
    @Nullable
    public T deserialize(UnversionedRow row, TableSchema schema) throws Exception {
        int valueIndex = columnIndex(schema, valueColumn);
        int codecIndex = codecColumn == null ? -1 : columnIndex(schema, codecColumn);

        byte[] value = null;
        String codecName = null;
        for (UnversionedValue rowValue : row.getValues()) {
            if (rowValue.getId() == valueIndex) {
                value = bytesValue(rowValue, valueColumn);
            } else if (rowValue.getId() == codecIndex) {
                codecName = stringValue(rowValue, codecColumn);
            }
        }

        if (value == null) {
            return null;
        }
        if (codecIndex < 0) {
            return deserializationSchema().deserialize(value);
        }
        return deserializationSchema().deserialize(YtValueCodecs.forName(codecName).decompress(value));
    }

    private static int columnIndex(TableSchema schema, String column) {
        int index = schema.findColumn(column);
        if (index < 0) {
            throw new IllegalStateException(String.format(
                    "Queue schema has no column '%s'; available columns: %s",
                    column,
                    schema.getColumnNames()));
        }
        return index;
    }

    @Nullable
    private static byte[] bytesValue(UnversionedValue rowValue, String column) {
        if (rowValue.getType() == ColumnValueType.NULL) {
            return null;
        }
        requireStringLike(rowValue, column);
        return rowValue.bytesValue();
    }

    @Nullable
    private static String stringValue(UnversionedValue rowValue, String column) {
        if (rowValue.getType() == ColumnValueType.NULL) {
            return null;
        }
        requireStringLike(rowValue, column);
        return rowValue.stringValue();
    }

    private static void requireStringLike(UnversionedValue rowValue, String column) {
        if (!rowValue.getType().isStringLikeType()) {
            throw new IllegalStateException(String.format(
                    "Queue column '%s' must be string-like, but it is %s",
                    column,
                    rowValue.getType().getName()));
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
