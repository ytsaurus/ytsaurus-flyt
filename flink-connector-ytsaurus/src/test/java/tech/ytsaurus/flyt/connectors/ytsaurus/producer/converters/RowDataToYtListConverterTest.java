package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

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
    void testNativeDateConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("targetDate", new DateType())
        ));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, /* Start of the Epoch */ 0);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='targetDate'; type='date';}"),
                rowType,
                rowData);

        // Native date returns days since epoch as int
        Assertions.assertEquals(0, result.get("targetDate"));
    }

    @Test
    void testNativeDatetimeConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("targetDatetime", new TimestampType())
        ));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, TimestampData.fromEpochMillis(1000));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='targetDatetime'; type='datetime';}"),
                rowType,
                rowData);

        // Native datetime returns epoch seconds
        Assertions.assertEquals(1L, result.get("targetDatetime"));
    }

    @Test
    void testNativeTimestampConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("targetTimestamp", new TimestampType())
        ));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, TimestampData.fromEpochMillis(1000));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='targetTimestamp'; type='timestamp';}"),
                rowType,
                rowData);

        // Native timestamp returns epoch microseconds
        Assertions.assertEquals(1000 * 1000L, result.get("targetTimestamp"));
    }

    @Test
    void testYsonBytesConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("targetBytes", new VarBinaryType())
        ));
        YTreeNode targetNode = YTree.mapBuilder().key("sample").value("test").buildMap();
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, targetNode.toBinary());

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='targetBytes'; type='yson';}"),
                rowType,
                rowData);

        Assertions.assertEquals(targetNode, result.get("targetBytes"));
    }

    @Test
    void testIntervalConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("targetInterval",
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND))
        ));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, /* Start of the Epoch */ 0L);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='targetInterval'; type='interval';}"),
                rowType,
                rowData);

        Assertions.assertEquals(0L, result.get("targetInterval"));
    }

    @Test
    void testNestedRowConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("nested",
                        new RowType(List.of(new RowType.RowField("nestedTarget", new VarBinaryType())))
                )
        ));
        GenericRowData nestedData = new GenericRowData(1);
        nestedData.setField(0, new byte[]{1, 0, 1});

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, nestedData);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='nested'; type='yson';}"),
                rowType,
                rowData);

        Assertions.assertEquals(
                Map.of("nestedTarget", YTree.bytesNode(new byte[]{1, 0, 1})),
                result.get("nested"));
    }

    @Test
    void testDictFieldConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("dictField", new MapType(
                        new VarCharType(),
                        new VarCharType()
                ))
        ));
        Map<BinaryStringData, BinaryStringData> dictMapData = new HashMap<>();
        dictMapData.put(new BinaryStringData("key1"), new BinaryStringData("value1"));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(dictMapData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='dictField'; type_v3={type_name='dict'; key='string'; value='string'};}"),
                rowType,
                rowData);

        // dict in YT is a list of [key, value] pairs
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder().value("key1").value("value1").buildList())
                        .buildList(),
                result.get("dictField"));
    }

    @Test
    void testDictOfArraysConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("dictOfArrays", new MapType(
                        new VarCharType(),
                        new ArrayType(new VarCharType())
                ))
        ));
        Map<BinaryStringData, GenericArrayData> dictOfArraysData = new HashMap<>();
        dictOfArraysData.put(new BinaryStringData("fruits"), new GenericArrayData(new BinaryStringData[]{
                new BinaryStringData("apple"),
                new BinaryStringData("banana")
        }));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(dictOfArraysData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='dictOfArrays'; type_v3={type_name='dict'; key='string'; value={type_name='list'; item='string'}};}"),
                rowType,
                rowData);

        // dictOfArrays: dict in YT is a list of [key, value] pairs, values are YTree lists
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value("fruits")
                                .value(YTree.listBuilder().value("apple").value("banana").buildList())
                                .buildList())
                        .buildList(),
                result.get("dictOfArrays"));
    }

    @Test
    void testArrayOfDictsConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("arrayOfDicts", new ArrayType(
                        new MapType(new VarCharType(), new VarCharType())
                ))
        ));
        Map<BinaryStringData, BinaryStringData> innerMap1 = new HashMap<>();
        innerMap1.put(new BinaryStringData("k1"), new BinaryStringData("v1"));
        Map<BinaryStringData, BinaryStringData> innerMap2 = new HashMap<>();
        innerMap2.put(new BinaryStringData("k2"), new BinaryStringData("v2"));
        GenericArrayData arrayOfDictsData = new GenericArrayData(new Object[]{
                new GenericMapData(innerMap1),
                new GenericMapData(innerMap2)
        });

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, arrayOfDictsData);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='arrayOfDicts'; type_v3={type_name='list'; item={type_name='dict'; key='string'; value='string'}};}"),
                rowType,
                rowData);

        // arrayOfDicts: YTree list; inner dicts are also serialized as list of [key, value] pairs
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value(YTree.listBuilder().value("k1").value("v1").buildList())
                                .buildList())
                        .value(YTree.listBuilder()
                                .value(YTree.listBuilder().value("k2").value("v2").buildList())
                                .buildList())
                        .buildList(),
                result.get("arrayOfDicts"));
    }

    @Test
    void testDictOfDictsConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("dictOfDicts", new MapType(
                        new VarCharType(),
                        new MapType(new VarCharType(), new VarCharType())
                ))
        ));
        Map<BinaryStringData, BinaryStringData> nestedDictValue = new HashMap<>();
        nestedDictValue.put(new BinaryStringData("innerKey"), new BinaryStringData("innerValue"));
        Map<BinaryStringData, GenericMapData> dictOfDictsData = new HashMap<>();
        dictOfDictsData.put(new BinaryStringData("outerKey"), new GenericMapData(nestedDictValue));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(dictOfDictsData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='dictOfDicts'; type_v3={type_name='dict'; key='string'; value={type_name='dict'; key='string'; value='string'}};}"),
                rowType,
                rowData);

        // dictOfDicts: outer dict is list of [key, value] pairs; inner dicts are also list of [key, value] pairs
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value("outerKey")
                                .value(YTree.listBuilder()
                                        .value(YTree.listBuilder().value("innerKey").value("innerValue").buildList())
                                        .buildList())
                                .buildList())
                        .buildList(),
                result.get("dictOfDicts"));
    }

    @Test
    void testDictOfDictsOfDictsConversion() {
        // dict<string, dict<string, dict<string, string>>>
        RowType rowType = new RowType(List.of(
                new RowType.RowField("dictOfDictsOfDicts", new MapType(
                        new VarCharType(),
                        new MapType(
                                new VarCharType(),
                                new MapType(new VarCharType(), new VarCharType())
                        )
                ))
        ));
        Map<BinaryStringData, BinaryStringData> innermostDictValue = new HashMap<>();
        innermostDictValue.put(new BinaryStringData("innerKey"), new BinaryStringData("innerValue"));
        Map<BinaryStringData, GenericMapData> midDictValue = new HashMap<>();
        midDictValue.put(new BinaryStringData("midKey"), new GenericMapData(innermostDictValue));
        Map<BinaryStringData, GenericMapData> outerDictValue = new HashMap<>();
        outerDictValue.put(new BinaryStringData("outerKey"), new GenericMapData(midDictValue));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(outerDictValue));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='dictOfDictsOfDicts'; type_v3={type_name='dict'; key='string'; "
                                + "value={type_name='dict'; key='string'; "
                                + "value={type_name='dict'; key='string'; value='string'}}};}"),
                rowType,
                rowData);

        // Every level is a YT dict, i.e. a list of [key, value] pairs, nested three deep.
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value("outerKey")
                                .value(YTree.listBuilder()
                                        .value(YTree.listBuilder()
                                                .value("midKey")
                                                .value(YTree.listBuilder()
                                                        .value(YTree.listBuilder()
                                                                .value("innerKey").value("innerValue").buildList())
                                                        .buildList())
                                                .buildList())
                                        .buildList())
                                .buildList())
                        .buildList(),
                result.get("dictOfDictsOfDicts"));
    }

    @Test
    void testDictOfArraysOfDictsConversion() {
        // dict<string, array<dict<string, string>>>
        RowType rowType = new RowType(List.of(
                new RowType.RowField("dictOfArraysOfDicts", new MapType(
                        new VarCharType(),
                        new ArrayType(new MapType(new VarCharType(), new VarCharType()))
                ))
        ));
        Map<BinaryStringData, BinaryStringData> innerDict = new HashMap<>();
        innerDict.put(new BinaryStringData("ik"), new BinaryStringData("iv"));
        GenericArrayData arrayOfDicts = new GenericArrayData(new Object[]{new GenericMapData(innerDict)});
        Map<BinaryStringData, GenericArrayData> outerDict = new HashMap<>();
        outerDict.put(new BinaryStringData("ok"), arrayOfDicts);

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(outerDict));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='dictOfArraysOfDicts'; type_v3={type_name='dict'; key='string'; "
                                + "value={type_name='list'; item={type_name='dict'; key='string'; value='string'}}};}"),
                rowType,
                rowData);

        // outer dict -> list of pairs; value is a YTree list; each element is an inner dict (list of pairs)
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value("ok")
                                .value(YTree.listBuilder()
                                        .value(YTree.listBuilder()
                                                .value(YTree.listBuilder().value("ik").value("iv").buildList())
                                                .buildList())
                                        .buildList())
                                .buildList())
                        .buildList(),
                result.get("dictOfArraysOfDicts"));
    }

    @Test
    void testArrayOfDictsOfArraysConversion() {
        // array<dict<string, array<string>>>
        RowType rowType = new RowType(List.of(
                new RowType.RowField("arrayOfDictsOfArrays", new ArrayType(
                        new MapType(new VarCharType(), new ArrayType(new VarCharType()))
                ))
        ));
        Map<BinaryStringData, GenericArrayData> dict = new HashMap<>();
        dict.put(new BinaryStringData("fruits"), new GenericArrayData(new BinaryStringData[]{
                new BinaryStringData("apple"),
                new BinaryStringData("banana")
        }));
        GenericArrayData arrayOfDicts = new GenericArrayData(new Object[]{new GenericMapData(dict)});

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, arrayOfDicts);

        Map<String, Object> result = convert(
                fieldDeclarationToSchema(
                        "{name='arrayOfDictsOfArrays'; type_v3={type_name='list'; "
                                + "item={type_name='dict'; key='string'; value={type_name='list'; item='string'}}};}"),
                rowType,
                rowData);

        // outer array -> list; each element is a dict (list of pairs); each value is a YTree list
        Assertions.assertEquals(
                YTree.listBuilder()
                        .value(YTree.listBuilder()
                                .value(YTree.listBuilder()
                                        .value("fruits")
                                        .value(YTree.listBuilder().value("apple").value("banana").buildList())
                                        .buildList())
                                .buildList())
                        .buildList(),
                result.get("arrayOfDictsOfArrays"));
    }

    @Test
    void testArrayWithNullableTypes() {
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
        rowData.setField(0, genericArrayData);

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
    void testMapWithNullableTypes() {
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
        rowData.setField(0, genericArrayData);

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
    void testYsonMapFieldConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("ysonMapField", new MapType(
                        new VarCharType(),
                        new VarCharType()
                ))
        ));
        Map<BinaryStringData, BinaryStringData> ysonMapData = new HashMap<>();
        ysonMapData.put(new BinaryStringData("ysonKey1"), new BinaryStringData("ysonValue1"));
        ysonMapData.put(new BinaryStringData("ysonKey2"), new BinaryStringData("ysonValue2"));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(ysonMapData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='ysonMapField'; type='yson';}"),
                rowType,
                rowData);

        // YSON map (not dict), type='yson' triggers else branch in createMapConverter
        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("ysonKey1").value("ysonValue1")
                        .key("ysonKey2").value("ysonValue2")
                        .buildMap(),
                result.get("ysonMapField"));
    }

    @Test
    void testYsonMapOfArraysConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("ysonMapOfArrays", new MapType(
                        new VarCharType(),
                        new ArrayType(new VarCharType())
                ))
        ));
        Map<BinaryStringData, GenericArrayData> ysonMapOfArraysData = new HashMap<>();
        ysonMapOfArraysData.put(new BinaryStringData("colors"), new GenericArrayData(new BinaryStringData[]{
                new BinaryStringData("red"),
                new BinaryStringData("green")
        }));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(ysonMapOfArraysData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='ysonMapOfArrays'; type='yson';}"),
                rowType,
                rowData);

        // YSON map with array values (else branch with nested types)
        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("colors")
                        .value(YTree.listBuilder().value("red").value("green").buildList())
                        .buildMap(),
                result.get("ysonMapOfArrays"));
    }

    @Test
    void testYsonMapOfMapsConversion() {
        RowType rowType = new RowType(List.of(
                new RowType.RowField("ysonMapOfMaps", new MapType(
                        new VarCharType(),
                        new MapType(new VarCharType(), new VarCharType())
                ))
        ));
        Map<BinaryStringData, BinaryStringData> ysonInnerMapValue = new HashMap<>();
        ysonInnerMapValue.put(new BinaryStringData("nestedKey"), new BinaryStringData("nestedValue"));
        Map<BinaryStringData, GenericMapData> ysonMapOfMapsData = new HashMap<>();
        ysonMapOfMapsData.put(new BinaryStringData("outer"), new GenericMapData(ysonInnerMapValue));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericMapData(ysonMapOfMapsData));

        Map<String, Object> result = convert(
                fieldDeclarationToSchema("{name='ysonMapOfMaps'; type='yson';}"),
                rowType,
                rowData);

        // YSON map with nested YSON map values (else branch with nested maps)
        Assertions.assertEquals(
                YTree.mapBuilder()
                        .key("outer")
                        .value(YTree.mapBuilder()
                                .key("nestedKey").value("nestedValue")
                                .buildMap())
                        .buildMap(),
                result.get("ysonMapOfMaps"));
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
