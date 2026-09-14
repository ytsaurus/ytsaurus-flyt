package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

public class RowDataToYtListConverterTest {
    @Test
    void testFlinkYtTypesConversion() {
        String schema = fieldDeclarationToSchema(
                "{name='targetDate'; type='date';}",
                "{name='targetDatetime'; type='datetime';}",
                "{name='targetTimestamp'; type='timestamp';}",
                "{name='targetBytes'; type='yson';}",
                "{name='targetInterval'; type='interval';}",
                "{name='nested'; type='yson';}"
        );

        LogicalType logicalType = new RowType(List.of(
                new RowType.RowField("targetDate", new DateType()),
                new RowType.RowField("targetDatetime", new TimestampType()),
                new RowType.RowField("targetTimestamp", new TimestampType()),
                new RowType.RowField("targetBytes", new VarBinaryType()),
                new RowType.RowField("targetInterval",
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND)),
                new RowType.RowField("nested",
                        new RowType(List.of(new RowType.RowField("nestedTarget", new VarBinaryType())))
                )
        ));

        YTreeNode targetNode = YTree.mapBuilder().key("sample").value("test").buildMap();

        GenericRowData nestedData = new GenericRowData(1);
        nestedData.setField(/* nestedTarget */ 0, new byte[]{1, 0, 1});

        GenericRowData rowData = new GenericRowData(6);
        rowData.setField(/* targetDate */ 0, /* Start of the Epoch */ 0);
        rowData.setField(/* targetDatetime */ 1, TimestampData.fromEpochMillis(1000));
        rowData.setField(/* targetTimestamp */ 2, TimestampData.fromEpochMillis(1000));
        rowData.setField(/* targetBytes */ 3, targetNode.toBinary());
        rowData.setField(/* targetInterval */ 4, /* Start of the Epoch */ 0L);
        rowData.setField(/* nested */ 5, nestedData);

        Map<String, Object> result = convert(schema, logicalType, rowData);
        // Right now we don't support nested native chrono conversions
        // because there's no way to provide enough data to determine
        // what fields to converse
        Assertions.assertEquals(0, result.get("targetDate"));
        Assertions.assertEquals(1L, result.get("targetDatetime"));
        Assertions.assertEquals(1000 * 1000L, result.get("targetTimestamp"));
        Assertions.assertEquals(targetNode, result.get("targetBytes"));
        Assertions.assertEquals(0L, result.get("targetInterval"));
        Assertions.assertEquals(
                Map.of("nestedTarget", YTree.bytesNode(new byte[]{1, 0, 1})),
                result.get("nested"));
    }


    @Test
    void testFlinkYtTypesConversionArrayWithNullableTypes() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("arrayWithNulls", new ArrayType(
                        new VarCharType()
                ))
        ));
        GenericRowData rowData = new GenericRowData(1);
        GenericArrayData genericArrayData = new GenericArrayData(new BinaryStringData[]{
                new BinaryStringData("abacaba"),
                null,
                new BinaryStringData("caba"),
                null,
                null
        });
        rowData.setField(/* arrayWithNulls */ 0, genericArrayData);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='arrayWithNulls'; type='yson';}"),
                rowType,
                rowData);

        Assertions.assertEquals(
                YTree.listBuilder()
                        .value("abacaba")
                        .value(YTree.nullNode())
                        .value("caba")
                        .value(YTree.nullNode())
                        .value(YTree.nullNode())
                        .buildList(),
                result.get("arrayWithNulls"));
    }

    @Test
    void testFlinkYtTypesConversionMapWithNullableTypes() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("mapWithNulls", new MapType(
                        new VarCharType(),
                        new VarCharType()
                ))
        ));
        GenericRowData rowData = new GenericRowData(1);
        Map<BinaryStringData, BinaryStringData> mapData = new HashMap<>();
        mapData.put(new BinaryStringData("nullKey"), null);
        mapData.put(new BinaryStringData("key"), new BinaryStringData("value"));
        GenericMapData genericArrayData = new GenericMapData(mapData);
        rowData.setField(/* mapWithNulls */ 0, genericArrayData);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='mapWithNulls'; type='yson';}"),
                rowType,
                rowData);

        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("nullKey")
                        .value(YTree.nullNode())
                        .key("key")
                        .value("value")
                        .buildMap(),
                result.get("mapWithNulls"));
    }

    @Test
    void tableRowConverter_ordersColumnsByWriteSchemaAndSkipsExpressionColumns() {
        // Flink row lists the fields in a different order than the YT schema.
        RowType rowType = new RowType(List.of(
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("id", new BigIntType())
        ));
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, new BinaryStringData("alice"));
        rowData.setField(1, 42L);

        Object[] result = convertTableRow(
                fieldDeclarationToSchema(
                        "{name='hash'; type='uint64'; sort_order='ascending'; expression='farm_hash(id)';}",
                        "{name='id'; type='int64'; sort_order='ascending';}",
                        "{name='name'; type='string';}"),
                rowType,
                rowData);

        Assertions.assertArrayEquals(new Object[]{42L, "alice"}, result);
    }

    @Test
    void tableRowConverter_leavesSchemaColumnsMissingInRowNull() {
        RowType rowType = new RowType(List.of(new RowType.RowField("id", new BigIntType())));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, 7L);

        Object[] result = convertTableRow(
                fieldDeclarationToSchema(
                        "{name='id'; type='int64'; sort_order='ascending';}",
                        "{name='extra'; type='string';}"),
                rowType,
                rowData);

        Assertions.assertArrayEquals(new Object[]{7L, null}, result);
    }

    @Test
    void tableRowConverter_convertsNullFieldToYtNull() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType())
        ));
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1L);
        rowData.setField(1, null);

        Object[] result = convertTableRow(
                fieldDeclarationToSchema(
                        "{name='id'; type='int64'; sort_order='ascending';}",
                        "{name='name'; type='string';}"),
                rowType,
                rowData);

        Assertions.assertArrayEquals(new Object[]{1L, YTree.nullNode()}, result);
    }

    @Test
    void tableRowConverter_rejectsRowFieldAbsentInSchema() {
        RowType rowType = new RowType(List.of(new RowType.RowField("unknown", new BigIntType())));
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        YTreeNode schemaNode = YTreeTextSerializer.deserialize(
                fieldDeclarationToSchema("{name='id'; type='int64'; sort_order='ascending';}"));

        Assertions.assertThrows(IllegalStateException.class,
                () -> converter.createTableRowConverter(rowType, schemaNode));
    }

    @Test
    void tableRowConverter_survivesJavaSerialization() throws Exception {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("id", new BigIntType())
        ));
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601).createTableRowConverter(
                rowType,
                YTreeTextSerializer.deserialize(fieldDeclarationToSchema(
                        "{name='id'; type='int64'; sort_order='ascending';}",
                        "{name='name'; type='string';}")));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(converter);
        }
        RowDataToYtListConverters.RowDataToYtMapConverter restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (RowDataToYtListConverters.RowDataToYtMapConverter) in.readObject();
        }

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, new BinaryStringData("bob"));
        rowData.setField(1, 3L);
        Assertions.assertArrayEquals(new Object[]{3L, "bob"}, (Object[]) restored.convert(null, rowData));
    }

    private Object[] convertTableRow(String ysonSchema, RowType rowType, GenericRowData rowData) {
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        YTreeNode schemaNode = YTreeTextSerializer.deserialize(ysonSchema);
        return (Object[]) converter.createTableRowConverter(rowType, schemaNode).convert(null, rowData);
    }

    private Map<String, Object> convert(String ysonSchema, LogicalType rowType, GenericRowData rowData) {
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        YTreeNode schemaNode = YTreeTextSerializer.deserialize(ysonSchema);
        //noinspection unchecked
        return (Map<String, Object>) converter
                .createConverter(rowType, schemaNode)
                .convert(null, rowData);
    }

    private String fieldDeclarationToSchema(String... fields) {
        return "<\"strict\"=%true;\"unique_keys\"=%true;>[" +
                Stream.of(fields)
                        .map(field -> field.replace("'", "\""))
                        .collect(Collectors.joining(";"))
                + ";]";
    }
}
