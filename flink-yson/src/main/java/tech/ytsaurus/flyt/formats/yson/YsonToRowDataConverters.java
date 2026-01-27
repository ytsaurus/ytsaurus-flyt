package tech.ytsaurus.flyt.formats.yson;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeListNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

/**
 * Base on JsonToRowDataConverters and a similar type conversion logic was applied to YSON
 */
public class YsonToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean failOnMissingField;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    public YsonToRowDataConverters(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @FunctionalInterface
    public interface YsonToRowDataConverter extends Serializable {
        Object convert(YTreeNode yTreeNode);
    }

    public YsonToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private YsonToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return yTreeNode -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return yTreeNode -> Byte.parseByte(yTreeNode.stringValue().trim());
            case SMALLINT:
                return yTreeNode -> Short.parseShort(yTreeNode.stringValue().trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private Boolean convertToBoolean(YTreeNode yTreeNode) {
        if (yTreeNode.isEntityNode()) {
            return null;
        } else if (yTreeNode.isBooleanNode()) {
            return yTreeNode.boolValue();
        } else {
            return Boolean.parseBoolean(yTreeNode.stringValue().trim());
        }
    }

    private int convertToInt(YTreeNode yTreeNode) {
        if (yTreeNode.isIntegerNode()) {
            return yTreeNode.intValue();
        } else {
            return Integer.parseInt(yTreeNode.stringValue().trim());
        }
    }

    private long convertToLong(YTreeNode yTreeNode) {
        if (yTreeNode.isIntegerNode()) {
            return yTreeNode.longValue();
        } else {
            return Long.parseLong(yTreeNode.stringValue().trim());
        }
    }

    private double convertToDouble(YTreeNode yTreeNode) {
        if (yTreeNode.isDoubleNode()) {
            return yTreeNode.doubleValue();
        } else if (yTreeNode.isIntegerNode()) {
            return yTreeNode.intValue();
        } else {
            return Double.parseDouble(yTreeNode.stringValue().trim());
        }
    }

    private float convertToFloat(YTreeNode yTreeNode) {
        if (yTreeNode.isDoubleNode()) {
            return yTreeNode.floatValue();
        } else {
            return Float.parseFloat(yTreeNode.stringValue().trim());
        }
    }

    private int convertToDate(YTreeNode yTreeNode) {
        LocalDate date = ISO_LOCAL_DATE.parse(yTreeNode.stringValue()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(YTreeNode yTreeNode) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(yTreeNode.stringValue());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(YTreeNode yTreeNode) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(yTreeNode.stringValue());
                break;
            case ISO_8601:
                parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(yTreeNode.stringValue());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private TimestampData convertToTimestampWithLocalZone(YTreeNode yTreeNode) {
        TemporalAccessor parsedTimestampWithLocalZone;
        switch (timestampFormat) {
            case SQL:
                parsedTimestampWithLocalZone =
                        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(yTreeNode.stringValue());
                break;
            case ISO_8601:
                parsedTimestampWithLocalZone =
                        ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(yTreeNode.stringValue());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(
                LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }

    private StringData convertToString(YTreeNode yTreeNode) {
        if (yTreeNode.isEntityNode()) {
            return null;
        } else if (yTreeNode.isMapNode() || yTreeNode.isListNode()) {
            return StringData.fromString(YTreeTextSerializer.serialize(yTreeNode));
        } else if (yTreeNode.isIntegerNode()) {
            return StringData.fromString(String.valueOf(yTreeNode.intValue()));
        } else if (yTreeNode.isDoubleNode()) {
            return StringData.fromString(String.valueOf(yTreeNode.doubleValue()));
        } else if (yTreeNode.isBooleanNode()) {
            return StringData.fromString(String.valueOf(yTreeNode.boolValue()));
        }
        return StringData.fromString(yTreeNode.stringValue());
    }

    private byte[] convertToBytes(YTreeNode yTreeNode) {
        if (yTreeNode.isMapNode()) {
            return yTreeNode.toBinary();
        }
        try {
            return yTreeNode.bytesValue();
        } catch (Exception e) {
            throw new YsonParseException("Unable to deserialize byte array.", e);
        }
    }

    private YsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return yTreeNode -> {
            BigDecimal bigDecimal;
            if (yTreeNode.isIntegerNode()) {
                bigDecimal = new BigDecimal(yTreeNode.longValue());
            } else if (yTreeNode.isDoubleNode()) {
                bigDecimal = BigDecimal.valueOf(yTreeNode.doubleValue());
            } else {
                bigDecimal = new BigDecimal(yTreeNode.stringValue());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private YsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        YsonToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return yTreeNode -> {
            final YTreeListNode node = yTreeNode.listNode();
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final YTreeNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }
            return new GenericArrayData(array);
        };
    }

    private YsonToRowDataConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "YSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final YsonToRowDataConverter keyConverter = createConverter(keyType);
        final YsonToRowDataConverter valueConverter = createConverter(valueType);

        return yTreeNode -> {
            Iterator<Map.Entry<String, YTreeNode>> fields = yTreeNode.asMap().entrySet().iterator();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, YTreeNode> entry = fields.next();
                Object key = keyConverter.convert(YTree.stringNode(entry.getKey()));
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    public YsonToRowDataConverter createRowConverter(RowType rowType) {
        final YsonToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(YsonToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return yTreeNode -> {
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                YTreeNode field = yTreeNode.asMap().get(fieldName);
                try {
                    Object convertedField = convertField(fieldConverters[i], fieldName, field);
                    row.setField(i, convertedField);
                } catch (Throwable t) {
                    throw new YsonParseException(
                            String.format("Fail to deserialize at field: %s.", fieldName), t);
                }
            }
            return row;
        };
    }

    private Object convertField(
            YsonToRowDataConverter fieldConverter, String fieldName, YTreeNode field) {
        if (field == null || field.isEntityNode()) {
            if (failOnMissingField) {
                throw new YsonParseException("Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(field);
        }
    }

    private YsonToRowDataConverter wrapIntoNullableConverter(YsonToRowDataConverter converter) {
        return yTreeNode -> {
            if (yTreeNode == null) {
                return null;
            }
            try {
                return converter.convert(yTreeNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    private static final class YsonParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        YsonParseException(String message) {
            super(message);
        }

        YsonParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
