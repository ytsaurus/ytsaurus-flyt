package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.util.List;
import java.util.Map;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.arr;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.convertSingleField;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.mapData;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.str;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.ytDict;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.ytList;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.ytMap;
import static tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtTestUtil.ytPair;

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
    void dictWithNonStringKeyTypeFails() {
        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> convertSingleField(
                        "dictField",
                        new MapType(new VarCharType(), new VarCharType()),
                        "{name='dictField'; type_v3={type_name='dict'; key='int64'; value='string'};}",
                        mapData("key", str("value"))));

        Assertions.assertTrue(exception.getMessage().contains("Only YT dicts with string keys are supported"));
    }

    @Test
    void dictWithNativeDateValues() {
        int epochDay = 1;
        Map<String, Object> result = convertSingleField(
                "dates",
                new MapType(new VarCharType(), new DateType()),
                "{name='dates'; type_v3={type_name='dict'; key='string'; value='date'};}",
                mapData("tomorrow", epochDay));

        YTreeNode expected = YTree.listBuilder()
                .value(YTree.listBuilder()
                        .value("tomorrow")
                        .value(epochDay)
                        .buildList())
                .buildList();
        Assertions.assertEquals(expected, result.get("dates"));
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

    @Test
    void arrayOfArraysOfDicts() {
        // array<array<dict<string, string>>>
        Map<String, Object> result = convertSingleField(
                "arrayOfArraysOfDicts",
                new ArrayType(new ArrayType(
                        new MapType(new VarCharType(), new VarCharType()))),
                "{name='arrayOfArraysOfDicts'; type_v3={type_name='list'; "
                        + "item={type_name='list'; "
                        + "item={type_name='dict'; key='string'; value='string'}}};}",
                arr(arr(mapData("k1", str("v1")), mapData("k2", str("v2")))));

        // outer list -> inner list -> each element is a dict (list of pairs)
        Assertions.assertEquals(
                ytList(ytList(
                        ytDict(ytPair("k1", "v1")),
                        ytDict(ytPair("k2", "v2")))),
                result.get("arrayOfArraysOfDicts"));
    }

    @Test
    void dictOfDictsOfArrays() {
        // dict<string, dict<string, array<string>>>
        Map<String, Object> result = convertSingleField(
                "dictOfDictsOfArrays",
                new MapType(new VarCharType(),
                        new MapType(new VarCharType(), new ArrayType(new VarCharType()))),
                "{name='dictOfDictsOfArrays'; type_v3={type_name='dict'; key='string'; "
                        + "value={type_name='dict'; key='string'; "
                        + "value={type_name='list'; item='string'}}};}",
                mapData("outerKey",
                        mapData("innerKey", arr(str("apple"), str("banana")))));

        // outer dict -> inner dict -> array value
        Assertions.assertEquals(
                ytDict(ytPair("outerKey",
                        ytDict(ytPair("innerKey", ytList("apple", "banana"))))),
                result.get("dictOfDictsOfArrays"));
    }

    @Test
    void arrayOfDictsOfDicts() {
        // array<dict<string, dict<string, string>>>
        Map<String, Object> result = convertSingleField(
                "arrayOfDictsOfDicts",
                new ArrayType(new MapType(new VarCharType(),
                        new MapType(new VarCharType(), new VarCharType()))),
                "{name='arrayOfDictsOfDicts'; type_v3={type_name='list'; "
                        + "item={type_name='dict'; key='string'; "
                        + "value={type_name='dict'; key='string'; value='string'}}};}",
                arr(mapData("outerKey", mapData("innerKey", str("innerValue")))));

        // outer list -> each element is a dict (list of pairs) -> value is a nested dict
        Assertions.assertEquals(
                ytList(ytDict(ytPair("outerKey",
                        ytDict(ytPair("innerKey", "innerValue"))))),
                result.get("arrayOfDictsOfDicts"));
    }

    // ===== nullable collections =====

    @Test
    void arrayWithNullableTypes() {
        Map<String, Object> result = convertSingleField(
                "arrayWithNulls", new ArrayType(new VarCharType()),
                "{name='arrayWithNulls'; type='yson';}",
                arr(str("abacaba"), null, str("caba"), null, null));

        Assertions.assertEquals(
                ytList("abacaba", null, "caba", null, null),
                result.get("arrayWithNulls"));
    }

    @Test
    void mapWithNullableTypes() {
        Map<String, Object> result = convertSingleField(
                "mapWithNulls", new MapType(new VarCharType(), new VarCharType()),
                "{name='mapWithNulls'; type='yson';}",
                mapData("nullKey", null, "key", str("value")));

        Assertions.assertEquals(
                ytMap("nullKey", null, "key", "value"),
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
                ytMap("ysonKey1", "ysonValue1", "ysonKey2", "ysonValue2"),
                result.get("ysonMapField"));
    }

    @Test
    void ysonMapOfArrays() {
        Map<String, Object> result = convertSingleField(
                "ysonMapOfArrays", new MapType(new VarCharType(), new ArrayType(new VarCharType())),
                "{name='ysonMapOfArrays'; type='yson';}",
                mapData("colors", arr(str("red"), str("green"))));

        Assertions.assertEquals(
                ytMap("colors", ytList("red", "green")),
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
                ytMap("outer", ytMap("nestedKey", "nestedValue")),
                result.get("ysonMapOfMaps"));
    }

    @Test
    void ysonMapOfMapsOfMaps() {
        // yson map<string, map<string, map<string, string>>>
        Map<String, Object> result = convertSingleField(
                "ysonMapOfMapsOfMaps",
                new MapType(new VarCharType(),
                        new MapType(new VarCharType(),
                                new MapType(new VarCharType(), new VarCharType()))),
                "{name='ysonMapOfMapsOfMaps'; type='yson';}",
                mapData("outer",
                        mapData("mid",
                                mapData("innerKey", str("innerValue")))));

        Assertions.assertEquals(
                ytMap("outer", ytMap("mid", ytMap("innerKey", "innerValue"))),
                result.get("ysonMapOfMapsOfMaps"));
    }

    @Test
    void ysonMapOfArraysOfMaps() {
        // yson map<string, array<map<string, string>>>
        Map<String, Object> result = convertSingleField(
                "ysonMapOfArraysOfMaps",
                new MapType(new VarCharType(),
                        new ArrayType(new MapType(new VarCharType(), new VarCharType()))),
                "{name='ysonMapOfArraysOfMaps'; type='yson';}",
                mapData("outer", arr(
                        mapData("k1", str("v1")),
                        mapData("k2", str("v2")))));

        Assertions.assertEquals(
                ytMap("outer", ytList(
                        ytMap("k1", "v1"),
                        ytMap("k2", "v2"))),
                result.get("ysonMapOfArraysOfMaps"));
    }

    @Test
    void ysonMapOfMapsOfArrays() {
        // yson map<string, map<string, array<string>>>
        Map<String, Object> result = convertSingleField(
                "ysonMapOfMapsOfArrays",
                new MapType(new VarCharType(),
                        new MapType(new VarCharType(), new ArrayType(new VarCharType()))),
                "{name='ysonMapOfMapsOfArrays'; type='yson';}",
                mapData("outer",
                        mapData("colors", arr(str("red"), str("green")))));

        Assertions.assertEquals(
                ytMap("outer", ytMap("colors", ytList("red", "green"))),
                result.get("ysonMapOfMapsOfArrays"));
    }

}
