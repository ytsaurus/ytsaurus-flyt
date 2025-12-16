package tech.ytsaurus.flyt.formats.yson;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.flyt.formats.yson.common.TimestampFormat;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static tech.ytsaurus.flyt.formats.yson.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static tech.ytsaurus.flyt.formats.yson.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static tech.ytsaurus.flyt.formats.yson.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static tech.ytsaurus.flyt.formats.yson.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static tech.ytsaurus.flyt.formats.yson.common.TimeFormats.SQL_TIME_FORMAT;

public class RowDataToYsonConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TimestampFormat timestampFormat;

    public RowDataToYsonConverters(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public interface RowDataToYsonConverter extends Serializable {
        YTreeNode convert(YTreeNode reuse, Object value);
    }

    public RowDataToYsonConverters.RowDataToYsonConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (reuse, value) -> YTree.nullNode();
            case BOOLEAN:
                return (reuse, value) ->
                        YTree.booleanNode((boolean) value);
            case TINYINT:
                return (reuse, value) -> YTree.integerNode((byte) value);
            case SMALLINT:
                return (reuse, value) -> YTree.integerNode((short) value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (reuse, value) -> YTree.integerNode((int) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (reuse, value) -> YTree.longNode((long) value);
            case FLOAT:
                return (reuse, value) -> YTree.doubleNode((float) value);
            case DOUBLE:
                return (reuse, value) -> YTree.doubleNode((double) value);
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (reuse, value) -> YTree.stringNode(value.toString());
            case BINARY:
            case VARBINARY:
                return createBytesConverter();
            case DATE:
                return createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampWithLocalZone();
            case DECIMAL:
                return createDecimalConverter();
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
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    protected RowDataToYsonConverters.RowDataToYsonConverter createBytesConverter() {
        return (reuse, value) -> YTreeBinarySerializer.deserialize(new ByteArrayInputStream((byte[]) value));
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createDecimalConverter() {
        return (reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return YTree.longNode(bd.longValue());
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createDateConverter() {
        return (reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return YTree.stringNode(ISO_LOCAL_DATE.format(date));
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createTimeConverter() {
        return (reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return YTree.stringNode(SQL_TIME_FORMAT.format(time));
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createTimestampConverter() {
        switch (timestampFormat) {
            case ISO_8601:
                return (reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return YTree
                            .stringNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            case SQL:
                return (reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return YTree
                            .stringNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createTimestampWithLocalZone() {
        switch (timestampFormat) {
            case ISO_8601:
                return (reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return YTree
                            .stringNode(
                                    ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            case SQL:
                return (reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return YTree
                            .stringNode(
                                    SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createArrayConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final RowDataToYsonConverters.RowDataToYsonConverter elementConverter = createConverter(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (reuse, value) -> {
            YTreeBuilder listBuilder = YTree.listBuilder();

            ArrayData array = (ArrayData) value;
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                listBuilder.value(elementConverter.convert(null, element));
            }

            return listBuilder.buildList();
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "YSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final RowDataToYsonConverters.RowDataToYsonConverter valueConverter = createConverter(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return (reuse, object) -> {
            YTreeBuilder mapBuilder = YTree.mapBuilder();

            MapData map = (MapData) object;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            int numElements = map.size();
            for (int i = 0; i < numElements; i++) {
                String fieldName = null;
                if (keyArray.isNullAt(i)) {
                    continue;
                } else {
                    fieldName = keyArray.getString(i).toString();
                }
                Object value = valueGetter.getElementOrNull(valueArray, i);
                try {
                    mapBuilder.key(fieldName).value(valueConverter.convert(null, value));
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Unable to convert field (%s). Error value is (%s)%n", fieldName, value), e);
                }
            }
            return mapBuilder.buildMap();
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter createRowConverter(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowDataToYsonConverters.RowDataToYsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToYsonConverters.RowDataToYsonConverter[]::new);
        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (reuse, data) -> {
            YTreeBuilder builder = YTree.mapBuilder();

            RowData row = (RowData) data;
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                Object key = fieldGetters[i].getFieldOrNull(row);
                try {
                    YTreeNode value = fieldConverters[i].convert(null, key);
                    builder.key(fieldName).value(value);
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Fail to serialize at field (%s) with value: %s", fieldName, key), t);
                }
            }
            return builder.buildMap();
        };
    }

    private RowDataToYsonConverters.RowDataToYsonConverter wrapIntoNullableConverter(
            RowDataToYsonConverters.RowDataToYsonConverter converter) {
        return (reuse, object) -> {
            if (object == null) {
                return YTree.nullNode();
            }

            return converter.convert(reuse, object);
        };
    }
}
