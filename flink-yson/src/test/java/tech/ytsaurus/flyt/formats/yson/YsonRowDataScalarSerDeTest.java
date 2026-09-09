package tech.ytsaurus.flyt.formats.yson;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import lombok.SneakyThrows;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.createDeserializer;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.createSerializer;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.serializeAndParse;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.toYsonBytes;

/**
 * Scalar type conversion tests for {@link YsonRowDataDeserializationSchema}
 * and {@link YsonRowDataSerializationSchema}: primitives, decimal, date/time/timestamp.
 */
public class YsonRowDataScalarSerDeTest {

    @SneakyThrows
    private static RowData deserializeVal(DataType type, YTreeNode value) {
        YTreeNode yson = YTree.builder().beginMap().key("val").value(value).buildMap();
        RowType schema = (RowType) ROW(FIELD("val", type)).getLogicalType();
        return createDeserializer(schema).deserialize(toYsonBytes(yson));
    }

    @SneakyThrows
    private static RowData deserializeVal(
            DataType type, YTreeNode value, TimestampFormat format) {
        YTreeNode yson = YTree.builder().beginMap().key("val").value(value).buildMap();
        RowType schema = (RowType) ROW(FIELD("val", type)).getLogicalType();
        return createDeserializer(schema, false, false, format).deserialize(toYsonBytes(yson));
    }

    // ===== BOOLEAN =====

    @Test
    public void deserializeBooleanFromNode() {
        RowData row = deserializeVal(BOOLEAN(), YTree.booleanNode(true));
        Assertions.assertThat(row.getBoolean(0)).isTrue();
    }

    @Test
    public void deserializeBooleanFromString() {
        RowData row = deserializeVal(BOOLEAN(), YTree.stringNode("true"));
        Assertions.assertThat(row.getBoolean(0)).isTrue();
    }

    @Test
    public void deserializeBooleanFromEntityIsNull() {
        RowData row = deserializeVal(BOOLEAN(), YTree.entityNode());
        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    // ===== TINYINT / SMALLINT =====

    @Test
    public void deserializeTinyintFromString() {
        RowData row = deserializeVal(TINYINT(), YTree.stringNode("42"));
        Assertions.assertThat(row.getByte(0)).isEqualTo((byte) 42);
    }

    @Test
    public void deserializeSmallintFromString() {
        RowData row = deserializeVal(SMALLINT(), YTree.stringNode("1024"));
        Assertions.assertThat(row.getShort(0)).isEqualTo((short) 1024);
    }

    // ===== INT =====

    @Test
    public void deserializeIntFromNode() {
        RowData row = deserializeVal(INT(), YTree.integerNode(100000));
        Assertions.assertThat(row.getInt(0)).isEqualTo(100000);
    }

    @Test
    public void deserializeIntFromString() {
        RowData row = deserializeVal(INT(), YTree.stringNode("100000"));
        Assertions.assertThat(row.getInt(0)).isEqualTo(100000);
    }

    // ===== BIGINT =====

    @Test
    public void deserializeBigintFromNode() {
        RowData row = deserializeVal(BIGINT(), YTree.integerNode(9999999999L));
        Assertions.assertThat(row.getLong(0)).isEqualTo(9999999999L);
    }

    @Test
    public void deserializeBigintFromString() {
        RowData row = deserializeVal(BIGINT(), YTree.stringNode("9999999999"));
        Assertions.assertThat(row.getLong(0)).isEqualTo(9999999999L);
    }

    // ===== FLOAT =====

    @Test
    public void deserializeFloatFromNode() {
        RowData row = deserializeVal(FLOAT(), YTree.doubleNode(3.14));
        Assertions.assertThat(row.getFloat(0)).isCloseTo(3.14f, Offset.offset(0.01f));
    }

    @Test
    public void deserializeFloatFromString() {
        RowData row = deserializeVal(FLOAT(), YTree.stringNode("3.14"));
        Assertions.assertThat(row.getFloat(0)).isCloseTo(3.14f, Offset.offset(0.01f));
    }

    // ===== DOUBLE =====

    @Test
    public void deserializeDoubleFromNode() {
        RowData row = deserializeVal(DOUBLE(), YTree.doubleNode(2.718));
        Assertions.assertThat(row.getDouble(0)).isCloseTo(2.718, Offset.offset(0.001));
    }

    @Test
    public void deserializeDoubleFromIntegerNode() {
        RowData row = deserializeVal(DOUBLE(), YTree.integerNode(42));
        Assertions.assertThat(row.getDouble(0)).isCloseTo(42.0, Offset.offset(0.001));
    }

    @Test
    public void deserializeDoubleFromString() {
        RowData row = deserializeVal(DOUBLE(), YTree.stringNode("2.718"));
        Assertions.assertThat(row.getDouble(0)).isCloseTo(2.718, Offset.offset(0.001));
    }

    // ===== STRING =====

    @Test
    public void deserializeStringFromNode() {
        RowData row = deserializeVal(STRING(), YTree.stringNode("hello"));
        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("hello"));
    }

    @Test
    public void deserializeStringFromEntityIsNull() {
        RowData row = deserializeVal(STRING(), YTree.entityNode());
        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void deserializeStringFromIntegerNode() {
        RowData row = deserializeVal(STRING(), YTree.integerNode(42));
        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("42"));
    }

    @Test
    public void deserializeStringFromDoubleNode() {
        RowData row = deserializeVal(STRING(), YTree.doubleNode(3.14));
        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("3.14"));
    }

    @Test
    public void deserializeStringFromBooleanNode() {
        RowData row = deserializeVal(STRING(), YTree.booleanNode(true));
        Assertions.assertThat(row.getString(0)).isEqualTo(StringData.fromString("true"));
    }

    @Test
    public void deserializeStringFromMapNode() {
        RowData row = deserializeVal(STRING(),
                YTree.builder().beginMap().key("a").value(1).buildMap());
        Assertions.assertThat(row.getString(0).toString()).isEqualTo("{\"a\"=1;}");
    }

    @Test
    public void deserializeStringFromListNode() {
        RowData row = deserializeVal(STRING(),
                YTree.builder().beginList().value(1).value(2).buildList());
        Assertions.assertThat(row.getString(0).toString()).isEqualTo("[1;2;]");
    }

    // ===== BYTES =====

    @Test
    public void deserializeBytesFromStringNode() {
        RowData row = deserializeVal(BYTES(), YTree.stringNode("hello"));
        Assertions.assertThat(row.getBinary(0)).isEqualTo("hello".getBytes());
    }

    // ===== DATE / TIME =====

    @Test
    public void deserializeDate() {
        RowData row = deserializeVal(DATE(), YTree.stringNode("2023-06-15"));
        Assertions.assertThat(row.getInt(0))
                .isEqualTo((int) LocalDate.of(2023, 6, 15).toEpochDay());
    }

    @Test
    public void deserializeTime() {
        RowData row = deserializeVal(TIME(), YTree.stringNode("10:30:00"));
        Assertions.assertThat(row.getInt(0))
                .isEqualTo(LocalTime.of(10, 30, 0).toSecondOfDay() * 1000);
    }

    // ===== TIMESTAMP =====

    @Test
    public void deserializeTimestampSqlFormat() {
        RowData row = deserializeVal(
                TIMESTAMP(), YTree.stringNode("2023-06-15 10:30:00"), TimestampFormat.SQL);
        LocalDateTime expected = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromLocalDateTime(expected));
    }

    @Test
    public void deserializeTimestampIso8601Format() {
        RowData row = deserializeVal(
                TIMESTAMP(), YTree.stringNode("2023-06-15T10:30:00"), TimestampFormat.ISO_8601);
        LocalDateTime expected = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromLocalDateTime(expected));
    }

    // ===== TIMESTAMP WITH LOCAL TIME ZONE =====

    @Test
    public void deserializeTimestampWithLocalTzSqlFormat() {
        RowData row = deserializeVal(TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                YTree.stringNode("2023-06-15 10:30:00Z"), TimestampFormat.SQL);
        LocalDateTime ldt = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));
    }

    @Test
    public void deserializeTimestampWithLocalTzIso8601Format() {
        RowData row = deserializeVal(TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                YTree.stringNode("2023-06-15T10:30:00Z"), TimestampFormat.ISO_8601);
        LocalDateTime ldt = LocalDateTime.of(2023, 6, 15, 10, 30, 0);
        Assertions.assertThat(row.getTimestamp(0, 6))
                .isEqualTo(TimestampData.fromInstant(ldt.toInstant(ZoneOffset.UTC)));
    }

    // ===== DECIMAL =====

    @Test
    public void deserializeDecimalFromIntegerNode() {
        RowData row = deserializeVal(DECIMAL(10, 0), YTree.integerNode(42L));
        Assertions.assertThat(row.getDecimal(0, 10, 0))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(42), 10, 0));
    }

    @Test
    public void deserializeDecimalFromDoubleNode() {
        RowData row = deserializeVal(DECIMAL(10, 2), YTree.doubleNode(99.99));
        Assertions.assertThat(row.getDecimal(0, 10, 2).toBigDecimal().doubleValue())
                .isCloseTo(99.99, Offset.offset(0.001));
    }

    @Test
    public void deserializeDecimalFromString() {
        RowData row = deserializeVal(DECIMAL(10, 3), YTree.stringNode("123.456"));
        Assertions.assertThat(row.getDecimal(0, 10, 3))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("123.456"), 10, 3));
    }

    // ===== SERIALIZE =====

    @Test
    public void serializeAllPrimitiveTypes() {
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
    public void serializeDecimal() {
        RowType schema = (RowType) ROW(FIELD("val", DECIMAL(10, 0))).getLogicalType();
        YTreeNode parsed = serializeAndParse(schema,
                GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("12345"), 10, 0)));
        Assertions.assertThat(parsed.asMap().get("val").longValue()).isEqualTo(12345L);
    }

    @Test
    public void serializeDateTimeAndTimestamps() {
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
    public void serializeTimestampsIso8601() {
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
}
