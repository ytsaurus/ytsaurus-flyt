package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.core.operations.YTreeBinarySerializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ChronoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ConverterUtils;
import tech.ytsaurus.typeinfo.TypeName;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.RowTypeUtils.getFieldNames;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.RowTypeUtils.getFieldTypes;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.ChronoUtils.MICROS_PER_MILLIS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.ConverterUtils.SCHEMA_TYPE_NAME;

@Slf4j
public class RowDataToYtListConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Set<String> EXPLICIT_YSON_TYPES = Set.of(TypeName.Yson.getWireName().toLowerCase(), "any");

    private final TimestampFormat timestampFormat;

    public RowDataToYtListConverters(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public interface RowDataToYtMapConverter extends Serializable {
        Object convert(Object reuse, Object value);
    }

    public RowDataToYtListConverters.RowDataToYtMapConverter createConverter(LogicalType type, YTreeNode schemaNode) {
        return wrapIntoNullableConverter(createNotNullConverter(type, schemaNode));
    }

    private RowDataToYtMapConverter createNotNullConverter(LogicalType type, YTreeNode fieldNode) {
        boolean nativeType = ConverterUtils.isOfNativeType(fieldNode);
        switch (type.getTypeRoot()) {
            case NULL:
                return (reuse, value) -> null;
            case BOOLEAN:
                return (reuse, value) -> (boolean) value;
            case TINYINT:
                return (reuse, value) -> (byte) value;
            case SMALLINT:
                return (reuse, value) -> (short) value;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (reuse, value) -> (int) value;
            case BIGINT:
                return (reuse, value) -> (long) value;
            case INTERVAL_DAY_TIME:
                return (reuse, value) -> ((long) value) * MICROS_PER_MILLIS;
            case FLOAT:
                return (reuse, value) -> (float) value;
            case DOUBLE:
                return (reuse, value) -> (double) value;
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (reuse, value) -> value.toString();
            case BINARY:
            case VARBINARY:
                if (fieldNode != null && EXPLICIT_YSON_TYPES.contains(getFieldTypeName(fieldNode).toLowerCase())) {
                    return (reuse, yson) -> YTreeBinarySerializer.deserialize(new ByteArrayInputStream((byte[]) yson));
                } else {
                    return (reuse, value) -> YTree.bytesNode((byte[]) value);
                }
            case DATE:
                return createDateConverter(nativeType);
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter(nativeType, fieldNode);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampWithLocalZone(nativeType, fieldNode);
            case DECIMAL:
                if (getFieldTypeName(fieldNode).equals(TypeName.Uint64.getWireName())) {
                    log.info("Using Decimal->Uint64 converter because of field type: {}", fieldNode);
                    return createDecimalToUint64Converter();
                }
                return createDecimalConverter();
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                return createMapConverter((MapType) type, fieldNode);
            case MULTISET:
                throw new RuntimeException("Unsupported type: MULTISET");
            case ROW:
                return createRowConverter((RowType) type, fieldNode);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToYtMapConverter createDecimalConverter() {
        return (reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return YTree.longNode(bd.longValue());
        };
    }

    private RowDataToYtMapConverter createDecimalToUint64Converter() {
        return (reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return YTree.unsignedIntegerNode(Long.parseUnsignedLong(bd.toString()));
        };
    }

    private RowDataToYtMapConverter createDateConverter(boolean nativeType) {
        if (nativeType) {
            return createNativeDateConverter();
        }
        return (reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return ISO_LOCAL_DATE.format(date);
        };
    }

    private RowDataToYtMapConverter createTimeConverter() {
        return (reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return SQL_TIME_FORMAT.format(time);
        };
    }

    private RowDataToYtMapConverter createTimestampConverter(boolean nativeType, YTreeNode fieldNode) {
        if (nativeType) {
            return createNativeTemporalConverter(fieldNode);
        }
        switch (timestampFormat) {
            case ISO_8601:
                return (reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
                };
            case SQL:
                return (reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToYtMapConverter createTimestampWithLocalZone(boolean nativeType, YTreeNode fieldNode) {
        if (nativeType) {
            return createNativeTemporalConverter(fieldNode);
        }
        switch (timestampFormat) {
            case ISO_8601:
                return (reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                            timestampWithLocalZone
                                    .toInstant()
                                    .atOffset(ZoneOffset.UTC));
                };
            case SQL:
                return (reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData) value;
                    return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                            timestampWithLocalZone
                                    .toInstant()
                                    .atOffset(ZoneOffset.UTC));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToYtMapConverter createNativeDateConverter() {
        // Returns Flink standard days since the epoch
        return (reuse, value) -> (int) value;
    }

    private RowDataToYtMapConverter createNativeTemporalConverter(YTreeNode fieldNode) {
        String type = ConverterUtils.getType(fieldNode);
        if (type == null) {
            throw new IllegalStateException("No type could be inferred for native timestamp-like field: " + fieldNode);
        }
        if (type.equals(ConverterUtils.TIMESTAMP_YT_TYPE)) {
            return createNativeTimestampConverter();
        } else if (type.equals(ConverterUtils.DATETIME_YT_TYPE)) {
            return createNativeDateTimeConverter();
        } else {
            throw new IllegalStateException("Unknown native timestamp-like type: " + type);
        }
    }

    private RowDataToYtMapConverter createNativeDateTimeConverter() {
        return (reuse, value) -> ((TimestampData) value)
                .toInstant()
                .getEpochSecond();
    }

    private RowDataToYtMapConverter createNativeTimestampConverter() {
        return (reuse, value) -> ChronoUtils.getMicrosSinceEpoch((TimestampData) value);
    }

    private RowDataToYtMapConverter createRowConverter(RowType type, YTreeNode treeNode) {
        final String[] fieldNames = getFieldNames(type);
        final LogicalType[] fieldTypes = getFieldTypes(type);
        final RowDataToYtMapConverter[] fieldConverters = getFieldConverters(fieldNames, fieldTypes, treeNode, type);

        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (reuse, data) -> {
            Map<String, Object> res = new HashMap<>();

            RowData row = (RowData) data;
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                Object key = fieldGetters[i].getFieldOrNull(row);
                try {
                    Object value = fieldConverters[i].convert(null, key);
                    res.put(fieldName, value);
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Fail to serialize at field (%s) with value: %s", fieldName, key), t);
                }
            }
            return res;
        };
    }

    private RowDataToYtMapConverter createArrayConverter(ArrayType arrayType) {
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(arrayType.getElementType());
        RowDataToYtMapConverter elementConvertor = createConverter(arrayType.getElementType(), null);
        return (reuse, data) -> {
            YTreeBuilder builder = YTree.listBuilder();
            ArrayData arrayData = (ArrayData) data;
            for (int i = 0; i < arrayData.size(); i++) {
                Object element = elementGetter.getElementOrNull(arrayData, i);
                Object convertedElement = elementConvertor.convert(null, element);
                builder.value(convertedElement);
            }
            return builder.buildList();
        };
    }

    private RowDataToYtMapConverter createMapConverter(MapType mapType, YTreeNode fieldNode) {
        LogicalType keyType = mapType.getKeyType();
        if (!keyType.is(LogicalTypeRoot.CHAR) && !keyType.is(LogicalTypeRoot.VARCHAR)) {
            throw new IllegalStateException(String.format(
                    "Keys for maps in YT must be of any string type only. Got: %s (%s)",
                    keyType, keyType.getTypeRoot()));
        }

        LogicalType valueType = mapType.getValueType();
        RowDataToYtMapConverter valueConvertor = createConverter(valueType, null);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        if (isDictField(fieldNode)) {
            log.info("Creating map converter as YT dict for field: {}", fieldNode);
            return createMapAsYtDictConverter(valueConvertor, valueGetter);
        } else {
            log.info("Creating map converter as YSON map for field: {}", fieldNode);
            return createYsonMapConverter(valueConvertor, valueGetter);
        }
    }

    private boolean isDictField(YTreeNode fieldNode) {
        return Optional.of(fieldNode)
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::asMap)
                .map(map -> map.get("type_v3"))
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::asMap)
                .map(map -> map.get("type_name"))
                .filter(YTreeNode::isStringNode)
                .map(YTreeNode::stringValue)
                .filter("dict"::equals)
                .isPresent();
    }



    private RowDataToYtListConverters.RowDataToYtMapConverter createYsonMapConverter(RowDataToYtMapConverter valueConvertor, ArrayData.ElementGetter valueGetter) {
        return (reuse, data) -> {
            YTreeBuilder builder = YTree.mapBuilder();
            MapData mapData = (MapData) data;
            ArrayData keys = mapData.keyArray();
            ArrayData values = mapData.valueArray();

            for (int i = 0; i < mapData.size(); i++) {
                String key = keys.getString(i).toString();
                Object rawValue = valueGetter.getElementOrNull(values, i);
                Object value = valueConvertor.convert(null, rawValue);

                builder.key(key).value(value);
            }
            return builder.buildMap();
        };
    }

    private RowDataToYtMapConverter createMapAsYtDictConverter(RowDataToYtMapConverter valueConvertor, ArrayData.ElementGetter valueGetter) {
        return (reuse, data) -> {
            YTreeBuilder builder = YTree.listBuilder(); // dict in YT is a list of pairs
            MapData mapData = (MapData) data;
            ArrayData keys = mapData.keyArray();
            ArrayData values = mapData.valueArray();

            for (int i = 0; i < mapData.size(); i++) {
                String key = keys.getString(i).toString();
                Object rawValue = valueGetter.getElementOrNull(values, i);
                Object value = valueConvertor.convert(null, rawValue);

                // Each pair is a [key, value]
                builder.value(YTree.listBuilder()
                        .value(key)
                        .value(value)
                        .buildList());
            }

            return builder.buildList();
        };
    }

    private RowDataToYtListConverters.RowDataToYtMapConverter wrapIntoNullableConverter(
            RowDataToYtListConverters.RowDataToYtMapConverter converter) {
        return (reuse, object) -> {
            if (object == null) {
                return YTree.nullNode();
            }

            return converter.convert(reuse, object);
        };
    }

    private RowDataToYtMapConverter[] getFieldConverters(String[] fieldNames,
                                                         LogicalType[] fieldTypes,
                                                         YTreeNode treeNode,
                                                         RowType rowType) {
        if (treeNode == null || !treeNode.isListNode()) {
            return Arrays.stream(fieldTypes)
                    .map(fieldType -> createConverter(fieldType, null))
                    .toArray(RowDataToYtMapConverter[]::new);
        }

        Map<String, YTreeNode> childNodes = new HashMap<>();
        for (YTreeNode childNode : treeNode.asList()) {
            YTreeMapNode fieldNode = childNode.mapNode();
            childNodes.put(fieldNode.getString("name"), fieldNode);
        }

        RowDataToYtMapConverter[] fieldConverters = new RowDataToYtMapConverter[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            YTreeNode fieldNode = childNodes.get(fieldNames[i]);
            if (fieldNode == null) {
                throw new IllegalStateException(String.format(
                        "No field '%s' found in YT schema (%s) but expected in Flink row (%s)",
                        fieldNames[i],
                        YTreeTextSerializer.serialize(treeNode),
                        rowType.asSummaryString()
                ));
            }
            fieldConverters[i] = createConverter(fieldTypes[i], fieldNode);
        }
        return fieldConverters;
    }

    private String getFieldTypeName(YTreeNode fieldNode) {
        return fieldNode.asMap().get(SCHEMA_TYPE_NAME).stringValue();
    }
}
