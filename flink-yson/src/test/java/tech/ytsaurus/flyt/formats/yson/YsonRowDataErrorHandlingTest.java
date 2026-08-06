package tech.ytsaurus.flyt.formats.yson;

import java.io.IOException;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.createDeserializer;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.toYsonBytes;

/**
 * Error-handling tests for {@link YsonRowDataDeserializationSchema}:
 * fail-on-missing, ignore-parse-errors and malformed dict/map inputs.
 */
public class YsonRowDataErrorHandlingTest {

    @Test
    public void failOnMissingField() {
        YTreeNode yson = YTree.builder().beginMap().key("f0").value(0).buildMap();
        RowType schema = (RowType) ROW(FIELD("f0", INT()), FIELD("f1", INT())).getLogicalType();

        YsonRowDataDeserializationSchema deserializer =
                createDeserializer(schema, true, false, TimestampFormat.SQL);

        Assertions.assertThatThrownBy(() -> deserializer.deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void failOnEntityFieldWhenFailOnMissingEnabled() {
        YTreeNode yson = YTree.builder().beginMap().key("val").entity().buildMap();
        RowType schema = (RowType) ROW(FIELD("val", INT())).getLogicalType();

        YsonRowDataDeserializationSchema deserializer =
                createDeserializer(schema, true, false, TimestampFormat.SQL);

        Assertions.assertThatThrownBy(() -> deserializer.deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void ignoreParseErrorsReturnsNullForBadValue() throws Exception {
        YTreeNode yson = YTree.builder().beginMap()
                .key("num").value("not_a_number")
                .key("name").value("valid")
                .buildMap();

        RowType schema = (RowType) ROW(
                FIELD("num", INT()), FIELD("name", STRING())
        ).getLogicalType();

        var row = createDeserializer(schema, false, true, TimestampFormat.SQL)
                .deserialize(toYsonBytes(yson));

        Assertions.assertThat(row.isNullAt(0)).isTrue();
        Assertions.assertThat(row.getString(1)).isEqualTo(
                org.apache.flink.table.data.StringData.fromString("valid"));
    }

    @Test
    public void doNotIgnoreParseErrorsThrows() {
        YTreeNode yson = YTree.builder().beginMap().key("num").value("not_a_number").buildMap();
        RowType schema = (RowType) ROW(FIELD("num", INT())).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void failOnMissingAndIgnoreErrorsBothEnabled() {
        RowType schema = (RowType) ROW(FIELD("f0", INT())).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema, true, true, TimestampFormat.SQL))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===== malformed dict / map =====

    @Test
    public void deserializeDictFromBrokenPairShouldFail_singleElementPair() {
        // [["k1"]] — pair has only key, no value
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value(YTree.builder().beginList().value("k1").buildList())
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(YsonToRowDataConverters.YsonParseException.class);
    }

    @Test
    public void deserializeDictFromBrokenPairShouldFail_nonListElement() {
        // ["k1"] — element is scalar, not a pair
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(YTree.builder().beginList().value("k1").buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(YsonToRowDataConverters.YsonParseException.class);
    }

    @Test
    public void deserializeDictWithNullValueShouldFailExplicitly() {
        YTreeNode yson = YTree.builder().beginMap()
                .key("val").value(
                        YTree.builder().beginList()
                                .value(YTree.builder().beginList().value("k1").entity().buildList())
                                .buildList())
                .buildMap();

        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class)
                .hasRootCauseInstanceOf(YsonToRowDataConverters.YsonParseException.class)
                .rootCause()
                .hasMessageContaining("Null values in YT dict are not supported");
    }

    @Test
    public void deserializeMapFromScalarShouldFail() {
        YTreeNode yson = YTree.builder().beginMap().key("val").value("not_a_map").buildMap();
        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), INT()))).getLogicalType();

        Assertions.assertThatThrownBy(
                        () -> createDeserializer(schema).deserialize(toYsonBytes(yson)))
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(YsonToRowDataConverters.YsonParseException.class);
    }
}
