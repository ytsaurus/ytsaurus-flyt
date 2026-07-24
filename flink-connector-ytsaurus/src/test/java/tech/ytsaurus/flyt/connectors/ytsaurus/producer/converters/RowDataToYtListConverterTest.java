package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.util.LinkedHashMap;
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

    // ===== scalar / native =====

    @Test
    void nativeDate() {
        Map<String, Object> result = convertSingleField(
                "targetDate", new DateType(),
                "{name='targetDate'; type='date';}",
                /* start of the epoch */ 0);

        // native date returns days since epoch as int
        Assertions.assertEquals(0, result.get("targetDate"));
    }

    @Test
    void nativeDatetime() {
        Map<String, Object> result = convertSingleField(
                "targetDatetime", new TimestampType(),
                "{name='targetDatetime'; type='datetime';}",
                TimestampData.fromEpochMillis(1000));

        // native datetime returns epoch seconds
        Assertions.assertEquals(1L, result.get("targetDatetime"));
    }

    @Test
    void nativeTimestamp() {
        Map<String, Object> result = convertSingleField(
                "targetTimestamp", new TimestampType(),
                "{name='targetTimestamp'; type='timestamp';}",
                TimestampData.fromEpochMillis(1000));

        // native timestamp returns epoch microseconds
        Assertions.assertEquals(1000 * 1000L, result.get("targetTimestamp"));
    }

    @Test
    void interval() {
        Map<String, Object> result = convertSingleField(
                "targetInterval",
                new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
                "{name='targetInterval'; type='interval';}",
                /* start of the epoch */ 0L);

        Assertions.assertEquals(0L, result.get("targetInterval"));
    }

    // ===== yson bytes / nested row =====

    @Test
    void ysonBytes() {
        YTreeNode targetNode = YTree.mapBuilder().key("sample").value("test").buildMap();

        Map<String, Object> result = convertSingleField(
                "targetBytes", new VarBinaryType(),
                "{name='targetBytes'; type='yson';}",
                targetNode.toBinary());

        Assertions.assertEquals(targetNode, result.get("targetBytes"));
    }

    @Test
    void nestedRow() {
        GenericRowData nestedData = new GenericRowData(1);
        nestedData.setField(0, new byte[]{1, 0, 1});

        Map<String, Object> result = convertSingleField(
                "nested",
                new RowType(List.of(new RowType.RowField("nestedTarget", new VarBinaryType()))),
                "{name='nested'; type='yson';}",
                nestedData);

        Assertions.assertEquals(
                Map.of("nestedTarget", YTree.bytesNode(new byte[]{1, 0, 1})),
                result.get("nested"));
    }

    // ===== dict (list of [key, value] pairs) =====

    @Test
    void dict() {
        Map<String, Object> result = convertSingleField(
                "dictField",
                new MapType(new VarCharType(), new VarCharType()),
                "{name='dictField'; type_v3={type_name='dict'; key='string'; value='string'};}",
                mapData("key1", str("value1")));

        Assertions.assertEquals(
                ytDict(ytPair("key1", "value1")),
                result.get("dictField"));
    }

    @Test
    void dictOfArrays() {
        Map<String, Object> result = convertSingleField(
                "dictOfArrays",
                new MapType(new VarCharType(), new ArrayType(new VarCharType())),
                "{name='dictOfArrays'; type_v3={type_name='dict'; key='string'; "
                        + "value={type_name='list'; item='string'}};}",
                mapData("fruits", arr(str("apple"), str("banana"))));

        Assertions.assertEquals(
                ytDict(ytPair("fruits", ytList("apple", "banana"))),
                result.get("dictOfArrays"));
    }

    @Test
    void arrayOfDicts() {
        Map<String, Object> result = convertSingleField(
                "arrayOfDicts",
                new ArrayType(new MapType(new VarCharType(), new VarCharType())),
                "{name='arrayOfDicts'; type_v3={type_name='list'; "
                        + "item={type_name='dict'; key='string'; value='string'}};}",
                arr(mapData("k1", str("v1")), mapData("k2", str("v2"))));

        Assertions.assertEquals(
                ytList(
                        ytDict(ytPair("k1", "v1")),
                        ytDict(ytPair("k2", "v2"))),
                result.get("arrayOfDicts"));
    }

    @Test
    void dictOfDicts() {
        Map<String, Object> result = convertSingleField(
                "dictOfDicts",
                new MapType(new VarCharType(),
                        new MapType(new VarCharType(), new VarCharType())),
                "{name='dictOfDicts'; type_v3={type_name='dict'; key='string'; "
                        + "value={type_name='dict'; key='string'; value='string'}};}",
                mapData("outerKey", mapData("innerKey", str("innerValue"))));

        Assertions.assertEquals(
                ytDict(ytPair("outerKey",
                        ytDict(ytPair("innerKey", "innerValue")))),
                result.get("dictOfDicts"));
    }

    @Test
    void dictOfDictsOfDicts() {
        Map<String, Object> result = convertSingleField(
                "dictOfDictsOfDicts",
                new MapType(new VarCharType(),
                        new MapType(new VarCharType(),
                                new MapType(new VarCharType(), new VarCharType()))),
                "{name='dictOfDictsOfDicts'; type_v3={type_name='dict'; key='string'; "
                        + "value={type_name='dict'; key='string'; "
                        + "value={type_name='dict'; key='string'; value='string'}}};}",
                mapData("outerKey",
                        mapData("midKey",
                                mapData("innerKey", str("innerValue")))));

        Assertions.assertEquals(
                ytDict(ytPair("outerKey",
                        ytDict(ytPair("midKey",
                                ytDict(ytPair("innerKey", "innerValue")))))),
                result.get("dictOfDictsOfDicts"));
    }

    @Test
    void dictOfArraysOfDicts() {
        Map<String, Object> result = convertSingleField(
                "dictOfArraysOfDicts",
                new MapType(new VarCharType(),
                        new ArrayType(new MapType(new VarCharType(), new VarCharType()))),
                "{name='dictOfArraysOfDicts'; type_v3={type_name='dict'; key='string'; "
                        + "value={type_name='list'; item={type_name='dict'; key='string'; value='string'}}};}",
                mapData("ok", arr(mapData("ik", str("iv")))));

        Assertions.assertEquals(
                ytDict(ytPair("ok",
                        ytList(ytDict(ytPair("ik", "iv"))))),
                result.get("dictOfArraysOfDicts"));
    }

    @Test
    void arrayOfDictsOfArrays() {
        Map<String, Object> result = convertSingleField(
                "arrayOfDictsOfArrays",
                new ArrayType(new MapType(new VarCharType(), new ArrayType(new VarCharType()))),
                "{name='arrayOfDictsOfArrays'; type_v3={type_name='list'; "
                        + "item={type_name='dict'; key='string'; value={type_name='list'; item='string'}}};}",
                arr(mapData("fruits", arr(str("apple"), str("banana")))));

        Assertions.assertEquals(
                ytList(ytDict(ytPair("fruits", ytList("apple", "banana")))),
                result.get("arrayOfDictsOfArrays"));
    }

    // ===== nullable collections =====

    @Test
    void arrayWithNullableTypes() {
        Map<String, Object> result = convertSingleField(
                "arrayWithNulls", new ArrayType(new VarCharType()),
                "{name='arrayWithNulls'; type='yson';}",
                arr(str("abacaba"), null, str("caba"), null, null));

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
    void mapWithNullableTypes() {
        Map<String, Object> result = convertSingleField(
                "mapWithNulls", new MapType(new VarCharType(), new VarCharType()),
                "{name='mapWithNulls'; type='yson';}",
                mapData("nullKey", null, "key", str("value")));

        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("nullKey").value(YTree.nullNode())
                        .key("key").value("value")
                        .buildMap(),
                result.get("mapWithNulls"));
    }

    // ===== yson map (not dict) =====

    @Test
    void ysonMap() {
        Map<String, Object> result = convertSingleField(
                "ysonMapField", new MapType(new VarCharType(), new VarCharType()),
                "{name='ysonMapField'; type='yson';}",
                mapData("ysonKey1", str("ysonValue1"), "ysonKey2", str("ysonValue2")));

        // yson map (not dict): type='yson' triggers the else branch in createMapConverter
        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("ysonKey1").value("ysonValue1")
                        .key("ysonKey2").value("ysonValue2")
                        .buildMap(),
                result.get("ysonMapField"));
    }

    @Test
    void ysonMapOfArrays() {
        Map<String, Object> result = convertSingleField(
                "ysonMapOfArrays", new MapType(new VarCharType(), new ArrayType(new VarCharType())),
                "{name='ysonMapOfArrays'; type='yson';}",
                mapData("colors", arr(str("red"), str("green"))));

        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("colors")
                        .value(YTree.listBuilder().value("red").value("green").buildList())
                        .buildMap(),
                result.get("ysonMapOfArrays"));
    }

    @Test
    void ysonMapOfMaps() {
        Map<String, Object> result = convertSingleField(
                "ysonMapOfMaps",
                new MapType(new VarCharType(), new MapType(new VarCharType(), new VarCharType())),
                "{name='ysonMapOfMaps'; type='yson';}",
                mapData("outer", mapData("nestedKey", str("nestedValue"))));

        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("outer")
                        .value(YTree.mapBuilder()
                                .key("nestedKey").value("nestedValue")
                                .buildMap())
                        .buildMap(),
                result.get("ysonMapOfMaps"));
    }

    // ===== helpers: input data builders =====

    private static BinaryStringData str(String s) {
        return new BinaryStringData(s);
    }

    private static GenericArrayData arr(Object... items) {
        return new GenericArrayData(items);
    }

    /** Builds a {@link GenericMapData} from key/value pairs. String keys are wrapped automatically. */
    private static GenericMapData mapData(Object... kv) {
        Map<Object, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            Object key = (kv[i] instanceof String) ? str((String) kv[i]) : kv[i];
            m.put(key, kv[i + 1]);
        }
        return new GenericMapData(m);
    }

    // ===== helpers: expected YT-value builders =====

    /** YT dict is a list of [key, value] pairs. */
    private static YTreeNode ytDict(YTreeNode... pairs) {
        var b = YTree.listBuilder();
        for (YTreeNode p : pairs) {
            b.value(p);
        }
        return b.buildList();
    }

    /** A single [key, value] pair; the value may be a String or a {@link YTreeNode}. */
    private static YTreeNode ytPair(String key, Object value) {
        return YTree.listBuilder()
                .value(key)
                .value(toNode(value))
                .buildList();
    }

    /** YT list; String elements are wrapped into string nodes. */
    private static YTreeNode ytList(Object... items) {
        var b = YTree.listBuilder();
        for (Object it : items) {
            b.value(toNode(it));
        }
        return b.buildList();
    }

    private static YTreeNode toNode(Object value) {
        return (value instanceof YTreeNode) ? (YTreeNode) value : YTree.stringNode((String) value);
    }


    // ===== helpers: conversion entry points =====

    private Map<String, Object> convertSingleField(
            String fieldName, LogicalType fieldType, String fieldSchema, Object value) {
        RowType rowType = new RowType(List.of(new RowType.RowField(fieldName, fieldType)));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, value);
        return convert(fieldDeclarationToSchema(fieldSchema), rowType, rowData);
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
