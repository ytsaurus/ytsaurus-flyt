package tech.ytsaurus.flyt.formats.yson;

import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.createDeserializer;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.serializeAndParse;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.toJavaStringIntegerMap;
import static tech.ytsaurus.flyt.formats.yson.YsonRowDataTestUtil.toYsonBytes;

/**
 * Collection conversion tests for {@link YsonRowDataDeserializationSchema}
 * and {@link YsonRowDataSerializationSchema}: arrays, maps, dicts and their nesting.
 */
public class YsonRowDataCollectionSerDeTest {

    @SneakyThrows
    private static RowData deserializeVal(org.apache.flink.table.types.DataType type,
                                          YTreeNode value) {
        YTreeNode yson = YTree.builder().beginMap().key("val").value(value).buildMap();
        RowType schema = (RowType) ROW(FIELD("val", type)).getLogicalType();
        return createDeserializer(schema).deserialize(toYsonBytes(yson));
    }

    // ===== ARRAY =====

    @Test
    public void deserializeArrayOfInts() {
        RowData row = deserializeVal(ARRAY(INT()),
                YTree.builder().beginList().value(1).value(2).value(3).buildList());

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(3);
        Assertions.assertThat(row.getArray(0).getInt(0)).isEqualTo(1);
        Assertions.assertThat(row.getArray(0).getInt(1)).isEqualTo(2);
        Assertions.assertThat(row.getArray(0).getInt(2)).isEqualTo(3);
    }

    @Test
    public void deserializeArrayOfStrings() {
        RowData row = deserializeVal(ARRAY(STRING()),
                YTree.builder().beginList().value("a").value("b").buildList());

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(2);
        Assertions.assertThat(row.getArray(0).getString(0)).isEqualTo(StringData.fromString("a"));
        Assertions.assertThat(row.getArray(0).getString(1)).isEqualTo(StringData.fromString("b"));
    }

    @Test
    public void deserializeArrayOfMaps() {
        // [{"k1":1}, {"k2":2}]
        RowData row = deserializeVal(ARRAY(MAP(STRING(), INT())),
                YTree.builder().beginList()
                        .value(YTree.builder().beginMap().key("k1").value(1).buildMap())
                        .value(YTree.builder().beginMap().key("k2").value(2).buildMap())
                        .buildList());

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(2);

        MapData first = row.getArray(0).getMap(0);
        Assertions.assertThat(first.keyArray().getString(0).toString()).isEqualTo("k1");
        Assertions.assertThat(first.valueArray().getInt(0)).isEqualTo(1);

        MapData second = row.getArray(0).getMap(1);
        Assertions.assertThat(second.keyArray().getString(0).toString()).isEqualTo("k2");
        Assertions.assertThat(second.valueArray().getInt(0)).isEqualTo(2);
    }

    @Test
    public void deserializeArrayOfDicts() {
        // [[["k1",1]], [["k2",2]]]
        RowData row = deserializeVal(ARRAY(MAP(STRING(), INT())),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value(YTree.builder().beginList()
                                        .value("k1").value(1).buildList())
                                .buildList())
                        .value(YTree.builder().beginList()
                                .value(YTree.builder().beginList()
                                        .value("k2").value(2).buildList())
                                .buildList())
                        .buildList());

        Assertions.assertThat(row.getArray(0).size()).isEqualTo(2);

        MapData first = row.getArray(0).getMap(0);
        Assertions.assertThat(first.keyArray().getString(0).toString()).isEqualTo("k1");
        Assertions.assertThat(first.valueArray().getInt(0)).isEqualTo(1);

        MapData second = row.getArray(0).getMap(1);
        Assertions.assertThat(second.keyArray().getString(0).toString()).isEqualTo("k2");
        Assertions.assertThat(second.valueArray().getInt(0)).isEqualTo(2);
    }

    // ===== MAP =====

    @Test
    public void deserializeMapFromMapNode() {
        // {"k1":1, "k2":2}
        RowData row = deserializeVal(MAP(STRING(), INT()),
                YTree.builder().beginMap().key("k1").value(1).key("k2").value(2).buildMap());

        Map<String, Integer> result = toJavaStringIntegerMap(row.getMap(0));
        Assertions.assertThat(result).containsEntry("k1", 1).containsEntry("k2", 2);
    }

    @Test
    public void deserializeDictFromListOfPairs() {
        // [["k1",10], ["k2",20]]
        RowData row = deserializeVal(MAP(STRING(), INT()),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList().value("k1").value(10).buildList())
                        .value(YTree.builder().beginList().value("k2").value(20).buildList())
                        .buildList());

        Map<String, Integer> result = toJavaStringIntegerMap(row.getMap(0));
        Assertions.assertThat(result).containsEntry("k1", 10).containsEntry("k2", 20);
    }

    // ===== NESTED MAP / DICT =====

    @Test
    public void deserializeMapOfMaps() {
        // {"outer": {"inner": 42}}
        RowData row = deserializeVal(MAP(STRING(), MAP(STRING(), INT())),
                YTree.builder().beginMap()
                        .key("outer").value(
                                YTree.builder().beginMap().key("inner").value(42).buildMap())
                        .buildMap());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("outer");
        MapData inner = outer.valueArray().getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("inner");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(42);
    }

    @Test
    public void deserializeDictOfDicts() {
        // [["outer", [["inner", 42]]]]
        RowData row = deserializeVal(MAP(STRING(), MAP(STRING(), INT())),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value("outer")
                                .value(YTree.builder().beginList()
                                        .value(YTree.builder().beginList()
                                                .value("inner").value(42).buildList())
                                        .buildList())
                                .buildList())
                        .buildList());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("outer");
        MapData inner = outer.valueArray().getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("inner");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(42);
    }

    @Test
    public void deserializeDictOfDictsOfDicts() {
        // [["outer", [["mid", [["inner", 42]]]]]]
        RowData row = deserializeVal(MAP(STRING(), MAP(STRING(), MAP(STRING(), INT()))),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value("outer")
                                .value(YTree.builder().beginList()
                                        .value(YTree.builder().beginList()
                                                .value("mid")
                                                .value(YTree.builder().beginList()
                                                        .value(YTree.builder().beginList()
                                                                .value("inner").value(42)
                                                                .buildList())
                                                        .buildList())
                                                .buildList())
                                        .buildList())
                                .buildList())
                        .buildList());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("outer");
        MapData mid = outer.valueArray().getMap(0);
        Assertions.assertThat(mid.keyArray().getString(0).toString()).isEqualTo("mid");
        MapData inner = mid.valueArray().getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("inner");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(42);
    }

    @Test
    public void deserializeMapOfDicts() {
        // {"outer": [["inner", 42]]}
        RowData row = deserializeVal(MAP(STRING(), MAP(STRING(), INT())),
                YTree.builder().beginMap()
                        .key("outer").value(
                                YTree.builder().beginList()
                                        .value(YTree.builder().beginList()
                                                .value("inner").value(42).buildList())
                                        .buildList())
                        .buildMap());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("outer");
        MapData inner = outer.valueArray().getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("inner");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(42);
    }

    @Test
    public void deserializeDictOfMaps() {
        // [["outer", {"inner": 42}]]
        RowData row = deserializeVal(MAP(STRING(), MAP(STRING(), INT())),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value("outer")
                                .value(YTree.builder().beginMap().key("inner").value(42).buildMap())
                                .buildList())
                        .buildList());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("outer");
        MapData inner = outer.valueArray().getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("inner");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(42);
    }

    // ===== MIXED NESTING =====

    @Test
    public void deserializeDictOfArraysOfMaps() {
        // [["ok", [ {"ik":7} ]]]
        RowData row = deserializeVal(MAP(STRING(), ARRAY(MAP(STRING(), INT()))),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value("ok")
                                .value(YTree.builder().beginList()
                                        .value(YTree.builder().beginMap()
                                                .key("ik").value(7).buildMap())
                                        .buildList())
                                .buildList())
                        .buildList());

        MapData outer = row.getMap(0);
        Assertions.assertThat(outer.keyArray().getString(0).toString()).isEqualTo("ok");
        ArrayData array = outer.valueArray().getArray(0);
        Assertions.assertThat(array.size()).isEqualTo(1);
        MapData inner = array.getMap(0);
        Assertions.assertThat(inner.keyArray().getString(0).toString()).isEqualTo("ik");
        Assertions.assertThat(inner.valueArray().getInt(0)).isEqualTo(7);
    }

    @Test
    public void deserializeArrayOfDictsOfArrays() {
        // [ [["fruits",[1,2]]] ]
        RowData row = deserializeVal(ARRAY(MAP(STRING(), ARRAY(INT()))),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value(YTree.builder().beginList()
                                        .value("fruits")
                                        .value(YTree.builder().beginList()
                                                .value(1).value(2).buildList())
                                        .buildList())
                                .buildList())
                        .buildList());

        ArrayData outer = row.getArray(0);
        Assertions.assertThat(outer.size()).isEqualTo(1);
        MapData dict = outer.getMap(0);
        Assertions.assertThat(dict.keyArray().getString(0).toString()).isEqualTo("fruits");
        ArrayData inner = dict.valueArray().getArray(0);
        Assertions.assertThat(inner.size()).isEqualTo(2);
        Assertions.assertThat(inner.getInt(0)).isEqualTo(1);
        Assertions.assertThat(inner.getInt(1)).isEqualTo(2);
    }

    @Test
    public void deserializeMapOfArrays() {
        // {"k1": [1,2], "k2": [3,4]}
        RowData row = deserializeVal(MAP(STRING(), ARRAY(INT())),
                YTree.builder().beginMap()
                        .key("k1").value(YTree.builder().beginList().value(1).value(2).buildList())
                        .key("k2").value(YTree.builder().beginList().value(3).value(4).buildList())
                        .buildMap());

        assertMapOfArrays(row.getMap(0));
    }

    @Test
    public void deserializeDictOfArrays() {
        // [["k1",[1,2]], ["k2",[3,4]]]
        RowData row = deserializeVal(MAP(STRING(), ARRAY(INT())),
                YTree.builder().beginList()
                        .value(YTree.builder().beginList()
                                .value("k1")
                                .value(YTree.builder().beginList().value(1).value(2).buildList())
                                .buildList())
                        .value(YTree.builder().beginList()
                                .value("k2")
                                .value(YTree.builder().beginList().value(3).value(4).buildList())
                                .buildList())
                        .buildList());

        assertMapOfArrays(row.getMap(0));
    }

    private static void assertMapOfArrays(MapData map) {
        Assertions.assertThat(map.size()).isEqualTo(2);

        Map<String, Integer> keyIndex = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            keyIndex.put(map.keyArray().getString(i).toString(), i);
        }

        int idx1 = keyIndex.get("k1");
        Assertions.assertThat(map.valueArray().getArray(idx1).getInt(0)).isEqualTo(1);
        Assertions.assertThat(map.valueArray().getArray(idx1).getInt(1)).isEqualTo(2);

        int idx2 = keyIndex.get("k2");
        Assertions.assertThat(map.valueArray().getArray(idx2).getInt(0)).isEqualTo(3);
        Assertions.assertThat(map.valueArray().getArray(idx2).getInt(1)).isEqualTo(4);
    }

    // ===== SERIALIZE =====

    @Test
    public void serializeCollections() {
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
    public void serializeArrayOfMaps() {
        RowType schema = (RowType) ROW(FIELD("val", ARRAY(MAP(STRING(), INT())))).getLogicalType();

        Map<StringData, Integer> map1 = new HashMap<>();
        map1.put(StringData.fromString("k1"), 1);
        Map<StringData, Integer> map2 = new HashMap<>();
        map2.put(StringData.fromString("k2"), 2);

        GenericRowData row = GenericRowData.of(new GenericArrayData(new Object[]{
                new GenericMapData(map1), new GenericMapData(map2)}));

        YTreeNode parsed = serializeAndParse(schema, row);

        Assertions.assertThat(parsed.asMap().get("val").listNode().size()).isEqualTo(2);
        Assertions.assertThat(parsed.asMap().get("val").listNode().get(0)
                .asMap().get("k1").intValue()).isEqualTo(1);
        Assertions.assertThat(parsed.asMap().get("val").listNode().get(1)
                .asMap().get("k2").intValue()).isEqualTo(2);
    }

    @Test
    public void serializeMapOfArrays() {
        RowType schema = (RowType) ROW(FIELD("val", MAP(STRING(), ARRAY(INT())))).getLogicalType();

        Map<StringData, GenericArrayData> map = new HashMap<>();
        map.put(StringData.fromString("k1"), new GenericArrayData(new int[]{1, 2}));
        map.put(StringData.fromString("k2"), new GenericArrayData(new int[]{3, 4}));

        YTreeNode parsed = serializeAndParse(schema, GenericRowData.of(new GenericMapData(map)));

        Assertions.assertThat(parsed.asMap().get("val").asMap()
                .get("k1").listNode().get(0).intValue()).isEqualTo(1);
        Assertions.assertThat(parsed.asMap().get("val").asMap()
                .get("k1").listNode().get(1).intValue()).isEqualTo(2);
        Assertions.assertThat(parsed.asMap().get("val").asMap()
                .get("k2").listNode().get(0).intValue()).isEqualTo(3);
        Assertions.assertThat(parsed.asMap().get("val").asMap()
                .get("k2").listNode().get(1).intValue()).isEqualTo(4);
    }
}
