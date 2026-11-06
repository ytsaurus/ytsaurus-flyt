package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.io.ByteArrayInputStream;
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
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.operations.YTreeBinarySerializer;
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
        Assertions.assertEquals(targetNode, decodeYson(result.get("targetBytes")));
        Assertions.assertEquals(0L, result.get("targetInterval"));
        Assertions.assertEquals(
                YTree.mapBuilder().key("nestedTarget").value(YTree.bytesNode(new byte[]{1, 0, 1})).buildMap(),
                decodeYson(result.get("nested")));
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
                decodeYson(result.get("arrayWithNulls")));
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
                decodeYson(result.get("mapWithNulls")));
    }

    @Test
    void rowConverter_encodesYsonColumnsAsBinaryYson() {
        String schema = fieldDeclarationToSchema(
                "{name='id'; type='int64'; sort_order='ascending';}",
                "{name='payload'; type='any';}",
                "{name='raw'; type='yson';}",
                "{name='items'; type_v3={type_name='list'; item='int64';};}",
                "{name='empty'; type='any';}");
        RowType rowType = new RowType(List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("payload", new RowType(List.of(new RowType.RowField("k", new VarCharType())))),
                new RowType.RowField("raw", new VarBinaryType()),
                new RowType.RowField("items", new ArrayType(new BigIntType())),
                new RowType.RowField("empty", new VarBinaryType())
        ));
        GenericRowData payload = new GenericRowData(1);
        payload.setField(0, new BinaryStringData("v"));
        YTreeNode rawNode = YTree.mapBuilder().key("a").value(1).buildMap();
        GenericRowData rowData = new GenericRowData(5);
        rowData.setField(0, 1L);
        rowData.setField(1, payload);
        rowData.setField(2, rawNode.toBinary());
        rowData.setField(3, new GenericArrayData(new Long[]{2L, 3L}));
        rowData.setField(4, null);

        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601)
                .createConverter(rowType, YTreeTextSerializer.deserialize(schema));
        //noinspection unchecked
        Map<String, Object> result = (Map<String, Object>) converter.convert(null, rowData);

        Assertions.assertEquals(1L, result.get("id"));
        Assertions.assertArrayEquals(
                YTree.mapBuilder().key("k").value("v").buildMap().toBinary(), (byte[]) result.get("payload"));
        Assertions.assertArrayEquals(rawNode.toBinary(), (byte[]) result.get("raw"));
        Assertions.assertArrayEquals(
                YTree.listBuilder().value(2L).value(3L).buildList().toBinary(), (byte[]) result.get("items"));
        Assertions.assertArrayEquals(YTree.nullNode().toBinary(), (byte[]) result.get("empty"));
        // The encoder buffer is reused: a second row must not carry bytes of the first one.
        //noinspection unchecked
        Map<String, Object> again = (Map<String, Object>) converter.convert(null, rowData);
        Assertions.assertArrayEquals((byte[]) result.get("payload"), (byte[]) again.get("payload"));
    }

    @Test
    void rowConverter_isSerializableWithEncoder() throws Exception {
        String schema = fieldDeclarationToSchema("{name='payload'; type='any';}");
        RowType rowType = new RowType(List.of(new RowType.RowField("payload", new VarBinaryType())));
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601)
                .createConverter(rowType, YTreeTextSerializer.deserialize(schema));
        YTreeNode node = YTree.mapBuilder().key("a").value(1).buildMap();
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, node.toBinary());
        //noinspection unchecked
        Map<String, Object> before = (Map<String, Object>) converter.convert(null, rowData);

        var copy = InstantiationUtil.clone(converter);
        //noinspection unchecked
        Map<String, Object> after = (Map<String, Object>) copy.convert(null, rowData);
        Assertions.assertArrayEquals((byte[]) before.get("payload"), (byte[]) after.get("payload"));
    }

    private static YTreeNode decodeYson(Object value) {
        return YTreeBinarySerializer.deserialize(new ByteArrayInputStream((byte[]) value));
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
