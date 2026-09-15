package tech.ytsaurus.flyt.formats.yson;

import lombok.SneakyThrows;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.createDeserializer;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.serializeAndParse;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.toYsonBytes;

/**
 * Row-structure tests for {@link YsonRowDataDeserializationSchema}
 * and {@link YsonRowDataSerializationSchema}: nested rows, partial/missing/entity fields, nulls.
 */
public class YsonRowDataStructureSerDeTest {

    // ===== NESTED ROW =====

    @SneakyThrows
    @Test
    public void deserializeNestedRow() {
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

    @SneakyThrows
    @Test
    public void deserializeDeeplyNestedRow() {
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
                FIELD("l1", ROW(FIELD("l2", ROW(FIELD("val", INT())))))
        ).getLogicalType();

        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.getRow(0, 1).getRow(0, 1).getInt(0)).isEqualTo(42);
    }

    // ===== PARTIAL / MISSING / ENTITY =====

    @SneakyThrows
    @Test
    public void deserializePartialYson() {
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

    @SneakyThrows
    @Test
    public void deserializeEntityFieldAsNull() {
        YTreeNode yson = YTree.builder().beginMap().key("val").entity().buildMap();
        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));
        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @SneakyThrows
    @Test
    public void deserializeMissingFieldAsNull() {
        YTreeNode yson = YTree.builder().beginMap().key("other").value(1).buildMap();
        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();
        RowData row = createDeserializer(schema).deserialize(toYsonBytes(yson));
        Assertions.assertThat(row.isNullAt(0)).isTrue();
    }

    @SneakyThrows
    @Test
    public void deserializeNullBytesReturnsNull() {
        RowType schema = (RowType) ROW(FIELD("f0", INT())).getLogicalType();
        Assertions.assertThat(createDeserializer(schema).deserialize((byte[]) null)).isNull();
    }

    // ===== SERIALIZE =====

    @Test
    public void serializeNullFields() {
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
    public void serializeNestedRows() {
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
