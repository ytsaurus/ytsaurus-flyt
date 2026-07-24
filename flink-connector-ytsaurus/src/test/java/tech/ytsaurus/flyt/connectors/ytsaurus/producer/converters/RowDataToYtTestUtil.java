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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Shared helpers for {@link RowDataToYtListConverters} tests:
 * input-data builders, expected YT-value builders and conversion entry points.
 */
final class RowDataToYtTestUtil {

    private RowDataToYtTestUtil() {
    }

    // ===== input data builders =====

    static BinaryStringData str(String s) {
        return new BinaryStringData(s);
    }

    static GenericArrayData arr(Object... items) {
        return new GenericArrayData(items);
    }

    /**
     * Builds a {@link GenericMapData} from key/value pairs. String keys are wrapped automatically.
     */
    static GenericMapData mapData(Object... kv) {
        Map<Object, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            Object key = (kv[i] instanceof String) ? str((String) kv[i]) : kv[i];
            m.put(key, kv[i + 1]);
        }
        return new GenericMapData(m);
    }

    // ===== expected YT-value builders =====

    /**
     * YT dict is a list of [key, value] pairs.
     */
    static YTreeNode ytDict(YTreeNode... pairs) {
        var b = YTree.listBuilder();
        for (YTreeNode p : pairs) {
            b.value(p);
        }
        return b.buildList();
    }

    /**
     * A single [key, value] pair; the value may be a String or a {@link YTreeNode}.
     */
    static YTreeNode ytPair(String key, Object value) {
        return YTree.listBuilder()
                .value(key)
                .value(toNode(value))
                .buildList();
    }

    /**
     * YT list; String elements are wrapped into string nodes, {@code null} into a null node.
     */
    static YTreeNode ytList(Object... items) {
        var b = YTree.listBuilder();
        for (Object it : items) {
            b.value(toNode(it));
        }
        return b.buildList();
    }

    /**
     * YT map node from alternating key/value args. Values may be String / YTreeNode / null.
     */
    static YTreeNode ytMap(Object... kv) {
        var b = YTree.mapBuilder();
        for (int i = 0; i < kv.length; i += 2) {
            b.key((String) kv[i]).value(toNode(kv[i + 1]));
        }
        return b.buildMap();
    }

    private static YTreeNode toNode(Object value) {
        if (value == null) {
            return YTree.nullNode();
        }
        return (value instanceof YTreeNode) ? (YTreeNode) value : YTree.stringNode((String) value);
    }


    // ===== conversion entry points =====

    static Map<String, Object> convertSingleField(
            String fieldName, LogicalType fieldType, String fieldSchema, Object value) {
        RowType rowType = new RowType(List.of(new RowType.RowField(fieldName, fieldType)));
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, value);
        return convert(fieldDeclarationToSchema(fieldSchema), rowType, rowData);
    }

    static Map<String, Object> convert(String ysonSchema, LogicalType rowType, GenericRowData rowData) {
        var converter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        YTreeNode schemaNode = YTreeTextSerializer.deserialize(ysonSchema);
        //noinspection unchecked
        return (Map<String, Object>) converter
                .createConverter(rowType, schemaNode)
                .convert(null, rowData);
    }

    static String fieldDeclarationToSchema(String... fields) {
        return "<\"strict\"=%true;\"unique_keys\"=%true;>[" +
                Stream.of(fields)
                        .map(field -> field.replace("'", "\""))
                        .collect(Collectors.joining(";"))
                + ";]";
    }
}
