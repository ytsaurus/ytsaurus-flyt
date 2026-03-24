package tech.ytsaurus.flyt.formats.yson;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;

/**
 * Tests for {@link YsonRowDataDeserializationSchema} and {@link YsonRowDataSerializationSchema}.
 */
public class YsonRowDataSerDeSchemaTest {

    private static YsonRowDataDeserializationSchema createDeserializer(
            RowType schema,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        return new YsonRowDataDeserializationSchema(
                schema, InternalTypeInfo.of(schema),
                failOnMissingField, ignoreParseErrors, timestampFormat);
    }

    private static YsonRowDataDeserializationSchema createDeserializer(RowType schema) {
        return createDeserializer(schema, false, false, TimestampFormat.SQL);
    }

    private static YsonRowDataSerializationSchema createSerializer(
            RowType schema, TimestampFormat timestampFormat) {
        return new YsonRowDataSerializationSchema(schema, timestampFormat);
    }

    private static YsonRowDataSerializationSchema createSerializer(RowType schema) {
        return createSerializer(schema, TimestampFormat.SQL);
    }

    private static byte[] toYsonBytes(YTreeNode node) {
        return YTreeTextSerializer.serialize(node).getBytes(StandardCharsets.UTF_8);
    }

    // ===== BOOLEAN =====

    @Test
    public void testDeserializeBooleanFromBooleanNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(true)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BOOLEAN())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getBoolean(0)).isTrue();
    }

    @Test
    public void testDeserializeBooleanFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("true")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BOOLEAN())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getBoolean(0)).isTrue();
    }

    @Test
    public void testDeserializeBooleanFromEntity() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").entity()
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BOOLEAN())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    // ===== TINYINT / SMALLINT =====

    @Test
    public void testDeserializeTinyintFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("42")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", TINYINT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getByte(0)).isEqualTo((byte) 42);
    }

    @Test
    public void testDeserializeSmallintFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("1024")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", SMALLINT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getShort(0)).isEqualTo((short) 1024);
    }

    // ===== INTEGER =====

    @Test
    public void testDeserializeIntFromIntegerNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(100000)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getInt(0)).isEqualTo(100000);
    }

    @Test
    public void testDeserializeIntFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("100000")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getInt(0)).isEqualTo(100000);
    }

    // ===== BIGINT =====

    @Test
    public void testDeserializeBigintFromIntegerNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(9999999999L)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BIGINT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getLong(0)).isEqualTo(9999999999L);
    }

    @Test
    public void testDeserializeBigintFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("9999999999")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BIGINT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getLong(0)).isEqualTo(9999999999L);
    }

    // ===== FLOAT =====

    @Test
    public void testDeserializeFloatFromDoubleNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(3.14)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", FLOAT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getFloat(0)).isCloseTo(3.14f, Offset.offset(0.01f));
    }

    @Test
    public void testDeserializeFloatFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("3.14")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", FLOAT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getFloat(0)).isCloseTo(3.14f, Offset.offset(0.01f));
    }

    // ===== DOUBLE =====

    @Test
    public void testDeserializeDoubleFromDoubleNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(2.718)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DOUBLE())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDouble(0)).isCloseTo(2.718, Offset.offset(0.001));
    }

    @Test
    public void testDeserializeDoubleFromIntegerNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(42)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DOUBLE())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDouble(0)).isCloseTo(42.0, Offset.offset(0.001));
    }

    @Test
    public void testDeserializeDoubleFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2.718")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DOUBLE())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDouble(0)).isCloseTo(2.718, Offset.offset(0.001));
    }

    // ===== STRING =====

    @Test
    public void testDeserializeStringFromStringNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("hello")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("hello"));
    }

    @Test
    public void testDeserializeStringFromEntity() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").entity()
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void testDeserializeStringFromIntegerNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(42)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("42"));
    }

    @Test
    public void testDeserializeStringFromDoubleNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(3.14)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("3.14"));
    }

    @Test
    public void testDeserializeStringFromBooleanNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(true)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("true"));
    }

    @Test
    public void testDeserializeStringFromMapNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginMap()
                                .key("a").value(1)
                                .buildMap())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0).toString()).contains("a");
    }

    @Test
    public void testDeserializeStringFromListNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value(1).value(2)
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", STRING())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isNotNull();
    }

    // ===== BYTES =====

    @Test
    public void testDeserializeBytesFromStringNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("hello")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", BYTES())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getBinary(0)).isEqualTo("hello".getBytes());
    }

    // ===== DATE =====

    @Test
    public void testDeserializeDate() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2023-06-15")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DATE())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getInt(0))
                .isEqualTo((int) LocalDate.of(2023, 6, 15).toEpochDay());
    }

    // ===== TIME =====

    @Test
    public void testDeserializeTime() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("10:30:00")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", TIME())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getInt(0))
                .isEqualTo(LocalTime.of(10, 30, 0).toSecondOfDay() * 1000);
    }

    // ===== TIMESTAMP =====

    @Test
    public void testDeserializeTimestampSqlFormat() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2023-06-15 10:30:00")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", TIMESTAMP())).getLogicalType();
        RowData row = createDeserializer(schema, false, false, TimestampFormat.SQL)
                .deserialize(toYsonBytes(yson));

        LocalDateTime expected = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromLocalDateTime(expected));
    }

    @Test
    public void testDeserializeTimestampIso8601Format() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2023-06-15T10:30:00")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", TIMESTAMP())).getLogicalType();
        RowData row = createDeserializer(schema, false, false, TimestampFormat.ISO_8601)
                .deserialize(toYsonBytes(yson));

        LocalDateTime expected = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromLocalDateTime(expected));
    }

    // ===== TIMESTAMP WITH LOCAL TIME ZONE =====

    @Test
    public void testDeserializeTimestampWithLocalTzSqlFormat() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2023-06-15 10:30:00Z")
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("val", TIMESTAMP_WITH_LOCAL_TIME_ZONE())
        ).getLogicalType();
        RowData row = createDeserializer(schema, false, false, TimestampFormat.SQL)
                .deserialize(toYsonBytes(yson));

        LocalDateTime ldt = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));
    }

    @Test
    public void testDeserializeTimestampWithLocalTzIso8601Format() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("2023-06-15T10:30:00Z")
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("val", TIMESTAMP_WITH_LOCAL_TIME_ZONE())
        ).getLogicalType();
        RowData row = createDeserializer(schema, false, false, TimestampFormat.ISO_8601)
                .deserialize(toYsonBytes(yson));

        LocalDateTime ldt = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));
    }

    // ===== DECIMAL =====

    @Test
    public void testDeserializeDecimalFromIntegerNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(42L)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DECIMAL(10, 0))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDecimal(0, 10, 0))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(42), 10, 0));
    }

    @Test
    public void testDeserializeDecimalFromDoubleNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(99.99)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DECIMAL(10, 2))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDecimal(0, 10, 2)).isNotNull();
    }

    @Test
    public void testDeserializeDecimalFromString() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value("123.456")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", DECIMAL(10, 3))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getDecimal(0, 10, 3))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("123.456"), 10, 3));
    }

    // ===== ARRAY =====

    @Test
    public void testDeserializeArrayOfInts() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value(1).value(2).value(3)
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", ARRAY(INT()))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(3);
        Assertions.assertThat(row.getArray(0).getInt(0)).isEqualTo(1);
        Assertions.assertThat(row.getArray(0).getInt(1)).isEqualTo(2);
        Assertions.assertThat(row.getArray(0).getInt(2)).isEqualTo(3);
    }

    @Test
    public void testDeserializeArrayOfStrings() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value("a").value("b")
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", ARRAY(STRING()))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(2);
        Assertions.assertThat(row.getArray(0).getString(0)).isEqualTo(StringData.fromString("a"));
        Assertions.assertThat(row.getArray(0).getString(1)).isEqualTo(StringData.fromString("b"));
    }

    // ===== MAP =====

    @Test
    public void testDeserializeMapFromMapNode() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginMap()
                                .key("k1").value(1)
                                .key("k2").value(2)
                                .buildMap())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getMap(0).size()).isEqualTo(2);
    }

    @Test
    public void testDeserializeMapFromListOfPairs() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value(YTree.builder().beginList().value("k1").value(10).buildList())
                                .value(YTree.builder().beginList().value("k2").value(20).buildList())
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getMap(0).size()).isEqualTo(2);
    }

    @Test
    public void testDeserializeNestedMap() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginMap()
                                .key("outer").value(
                                        YTree.builder().beginMap()
                                                .key("inner").value(42)
                                                .buildMap())
                                .buildMap())
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("val", MAP(STRING(), MAP(STRING(), INT())))
        ).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getMap(0).size()).isEqualTo(1);
    }

    // ===== NESTED ROW =====

    @Test
    public void testDeserializeNestedRow() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("name").value("Bob")
                .key("address").value(
                        YTree.builder().beginMap()
                                .key("city").value("NYC")
                                .key("zip").value(10001)
                                .buildMap())
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("name", STRING()),
                FIELD("address", ROW(FIELD("city", STRING()), FIELD("zip", INT())))
        ).getLogicalType();

        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("Bob"));
        RowData address = row.getRow(1, 2);
        Assertions.assertThat(address.getString(0)).isEqualTo(StringData.fromString("NYC"));
        Assertions.assertThat(address.getInt(1)).isEqualTo(10001);
    }

    @Test
    public void testDeserializeDeeplyNestedRow() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("l1").value(
                        YTree.builder().beginMap()
                                .key("l2").value(
                                        YTree.builder().beginMap()
                                                .key("val").value(42)
                                                .buildMap())
                                .buildMap())
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("l1", ROW(
                        FIELD("l2", ROW(
                                FIELD("val", INT())))))
        ).getLogicalType();

        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getRow(0, 1).getRow(0, 1).getInt(0)).isEqualTo(42);
    }

    // ===== PARTIAL FIELDS =====

    @Test
    public void testDeserializePartialYson() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("name").value("Alice")
                .key("age").value(30)
                .key("extra1").value("ignored")
                .key("extra2").value(999)
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("name", STRING()), FIELD("age", INT())
        ).getLogicalType();

        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getArity()).isEqualTo(2);
        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("Alice"));
        Assertions.assertThat(row.getInt(1)).isEqualTo(30);
    }

    // ===== ENTITY AND MISSING FIELDS =====

    @Test
    public void testDeserializeEntityFieldAsNull() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").entity()
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void testDeserializeMissingFieldAsNull() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("other").value(1)
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void testDeserializeNullBytesReturnsNull() throws Exception {
        RowType schema = (RowType) ROW(FIELD("f0", INT())).getLogicalType();

        Assertions.assertThat(createDeserializer(schema).deserialize((byte[]) null)).isNull();
    }

    // ===== ERROR HANDLING =====

    @Test
    public void testFailOnMissingField() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("f0").value(0)
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("f0", INT()), FIELD("f1", INT())
        ).getLogicalType();

        YsonRowDataDeserializationSchema deserializer =
                createDeserializer(schema, true, false, TimestampFormat.SQL);

        Assertions.assertThatThrownBy(() -> deserializer.deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testFailOnEntityFieldWhenFailOnMissingEnabled() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").entity()
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();

        YsonRowDataDeserializationSchema deserializer =
                createDeserializer(schema, true, false, TimestampFormat.SQL);

        Assertions.assertThatThrownBy(() -> deserializer.deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testIgnoreParseErrorsReturnsNullForBadValue() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("num").value("not_a_number")
                .key("name").value("valid")
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("num", INT()), FIELD("name", STRING())
        ).getLogicalType();

        RowData row = createDeserializer(schema, false, true, TimestampFormat.SQL)
                .deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
        Assertions.assertThat(row.getString(1)).isEqualTo(StringData.fromString("valid"));
    }

    @Test
    public void testDoNotIgnoreParseErrorsThrows() {
        YTreeNode yson = YTree.builder().beginMap()
                .key("num").value("not_a_number")
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("num", INT())).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testFailOnMissingAndIgnoreErrorsBothEnabled() {
        RowType schema = (RowType) ROW(FIELD("f0", INT())).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema, true, true, TimestampFormat.SQL))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===== SERIALIZATION =====

    private static YTreeNode serializeAndParse(
            YsonRowDataSerializationSchema serializer, GenericRowData row) {
        byte[] bytes = serializer.serialize(row);
        String result = new String(bytes, StandardCharsets.UTF_8);
        Assertions.assertThat(result).endsWith(";");
        return YTreeTextSerializer.deserialize(result.substring(0, result.length() - 1));
    }

    private static YTreeNode serializeAndParse(RowType schema, GenericRowData row) {
        return serializeAndParse(createSerializer(schema), row);
    }

    @Test
    public void testSerializeAllPrimitiveTypes() {
        RowType schema = (RowType) ROW(
                FIELD("bool_val", BOOLEAN()),
                FIELD("tinyint_val", TINYINT()),
                FIELD("smallint_val", SMALLINT()),
                FIELD("int_val", INT()),
                FIELD("bigint_val", BIGINT()),
                FIELD("float_val", FLOAT()),
                FIELD("double_val", DOUBLE()),
                FIELD("string_val", STRING())
        ).getLogicalType();

        GenericRowData row = GenericRowData.of(
                true, (byte) 7, (short) 1024, 42, 9999999999L,
                3.14f, 2.718, StringData.fromString("hello"));

        YTreeNode parsed = serializeAndParse(schema, row);

        Assertions.assertThat(parsed.asMap().get("bool_val").boolValue()).isTrue();
        Assertions.assertThat(parsed.asMap().get("tinyint_val").intValue()).isEqualTo(7);
        Assertions.assertThat(parsed.asMap().get("smallint_val").intValue()).isEqualTo(1024);
        Assertions.assertThat(parsed.asMap().get("int_val").intValue()).isEqualTo(42);
        Assertions.assertThat(parsed.asMap().get("bigint_val").longValue()).isEqualTo(9999999999L);
        Assertions.assertThat(parsed.asMap().get("float_val").doubleValue())
                .isCloseTo(3.14, Offset.offset(0.01));
        Assertions.assertThat(parsed.asMap().get("double_val").doubleValue())
                .isCloseTo(2.718, Offset.offset(0.001));
        Assertions.assertThat(parsed.asMap().get("string_val").stringValue()).isEqualTo("hello");
    }

    @Test
    public void testSerializeNullFields() {
        RowType schema = (RowType) ROW(
                FIELD("name", STRING()),
                FIELD("age", INT()),
                FIELD("score", DOUBLE())
        ).getLogicalType();

        YTreeNode parsed = serializeAndParse(schema,
                GenericRowData.of(StringData.fromString("Bob"), null, null));

        Assertions.assertThat(parsed.asMap().get("name").stringValue()).isEqualTo("Bob");
        Assertions.assertThat(parsed.asMap().get("age").isEntityNode()).isTrue();
        Assertions.assertThat(parsed.asMap().get("score").isEntityNode()).isTrue();
    }

    @Test
    public void testSerializeDateTimeAndTimestamps() {
        RowType schema = (RowType) ROW(
                FIELD("date_val", DATE()),
                FIELD("time_val", TIME()),
                FIELD("ts", TIMESTAMP()),
                FIELD("ts_tz", TIMESTAMP_WITH_LOCAL_TIME_ZONE())
        ).getLogicalType();

        LocalDate date = LocalDate.of(2023, 6, 15);
        LocalTime time = LocalTime.of(10, 30, 0);
        LocalDateTime ldt = LocalDateTime.of(date, time);

        GenericRowData row = GenericRowData.of(
                (int) date.toEpochDay(),
                time.toSecondOfDay() * 1000,
                TimestampData.fromLocalDateTime(ldt),
                TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));

        YTreeNode parsed = serializeAndParse(schema, row);

        Assertions.assertThat(parsed.asMap().get("date_val").stringValue()).isEqualTo("2023-06-15");
        Assertions.assertThat(parsed.asMap().get("time_val").stringValue()).isEqualTo("10:30:00");
        Assertions.assertThat(parsed.asMap().get("ts").stringValue())
                .isEqualTo("2023-06-15 10:30:00");
        Assertions.assertThat(parsed.asMap().get("ts_tz").stringValue())
                .isEqualTo("2023-06-15 10:30:00Z");
    }

    @Test
    public void testSerializeTimestampsIso8601() {
        RowType schema = (RowType) ROW(
                FIELD("ts", TIMESTAMP()),
                FIELD("ts_tz", TIMESTAMP_WITH_LOCAL_TIME_ZONE())
        ).getLogicalType();

        LocalDateTime ldt = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        GenericRowData row = GenericRowData.of(
                TimestampData.fromLocalDateTime(ldt),
                TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));

        YTreeNode parsed = serializeAndParse(
                createSerializer(schema, TimestampFormat.ISO_8601), row);

        Assertions.assertThat(parsed.asMap().get("ts").stringValue())
                .isEqualTo("2023-06-15T10:30:00");
        Assertions.assertThat(parsed.asMap().get("ts_tz").stringValue())
                .isEqualTo("2023-06-15T10:30:00Z");
    }

    @Test
    public void testSerializeDecimal() {
        RowType schema = (RowType) ROW(FIELD("val", DECIMAL(10, 0))).getLogicalType();

        YTreeNode parsed = serializeAndParse(schema,
                GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("12345"), 10, 0)));

        Assertions.assertThat(parsed.asMap().get("val").longValue()).isEqualTo(12345L);
    }

    @Test
    public void testSerializeCollections() {
        RowType schema = (RowType) ROW(
                FIELD("ints", ARRAY(INT())),
                FIELD("props", MAP(STRING(), INT())),
                FIELD("nested", MAP(STRING(), MAP(STRING(), INT())))
        ).getLogicalType();

        Map<StringData, Integer> simpleMap = new HashMap<>();
        simpleMap.put(StringData.fromString("k1"), 1);
        simpleMap.put(StringData.fromString("k2"), 2);

        Map<StringData, Integer> innerMap = new HashMap<>();
        innerMap.put(StringData.fromString("key"), 42);
        Map<StringData, GenericMapData> outerMap = new HashMap<>();
        outerMap.put(StringData.fromString("inner"), new GenericMapData(innerMap));

        GenericRowData row = GenericRowData.of(
                new GenericArrayData(new int[]{1, 2, 3}),
                new GenericMapData(simpleMap),
                new GenericMapData(outerMap));

        YTreeNode parsed = serializeAndParse(schema, row);

        Assertions.assertThat(parsed.asMap().get("ints").listNode().size()).isEqualTo(3);
        Assertions.assertThat(parsed.asMap().get("ints").listNode().get(0).intValue()).isEqualTo(1);
        Assertions.assertThat(parsed.asMap().get("props").asMap().get("k1").intValue()).isEqualTo(1);
        Assertions.assertThat(parsed.asMap().get("props").asMap().get("k2").intValue()).isEqualTo(2);
        Assertions.assertThat(parsed.asMap().get("nested").asMap()
                .get("inner").asMap().get("key").intValue()).isEqualTo(42);
    }

    @Test
    public void testSerializeNestedRows() {
        RowType schema = (RowType) ROW(
                FIELD("name", STRING()),
                FIELD("l1", ROW(
                        FIELD("l2", ROW(
                                FIELD("val", INT()),
                                FIELD("label", STRING())))))
        ).getLogicalType();

        GenericRowData l2 = GenericRowData.of(42, StringData.fromString("deep"));
        GenericRowData row = GenericRowData.of(
                StringData.fromString("Alice"), GenericRowData.of(l2));

        YTreeNode parsed = serializeAndParse(schema, row);

        Assertions.assertThat(parsed.asMap().get("name").stringValue()).isEqualTo("Alice");
        YTreeNode l2Node = parsed.asMap().get("l1").asMap().get("l2");
        Assertions.assertThat(l2Node.asMap().get("val").intValue()).isEqualTo(42);
        Assertions.assertThat(l2Node.asMap().get("label").stringValue()).isEqualTo("deep");
    }
}
