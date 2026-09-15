package tech.ytsaurus.flyt.formats.yson;

import java.nio.charset.StandardCharsets;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.assertj.core.api.Assertions;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Shared helpers for tests of {@link YsonRowDataDeserializationSchema}
 * and {@link YsonRowDataSerializationSchema}.
 */
final class YsonRowDataTestUtil {

    private YsonRowDataTestUtil() {
    }

    static YsonRowDataDeserializationSchema createDeserializer(
            RowType schema,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        return new YsonRowDataDeserializationSchema(
                schema, InternalTypeInfo.of(schema),
                failOnMissingField, ignoreParseErrors, timestampFormat);
    }

    static YsonRowDataDeserializationSchema createDeserializer(RowType schema) {
        return createDeserializer(schema, false, false, TimestampFormat.SQL);
    }

    static YsonRowDataSerializationSchema createSerializer(
            RowType schema, TimestampFormat timestampFormat) {
        return new YsonRowDataSerializationSchema(schema, timestampFormat);
    }

    static YsonRowDataSerializationSchema createSerializer(RowType schema) {
        return createSerializer(schema, TimestampFormat.SQL);
    }

    static byte[] toYsonBytes(YTreeNode node) {
        return YTreeTextSerializer.serialize(node).getBytes(StandardCharsets.UTF_8);
    }

    static YTreeNode serializeAndParse(
            YsonRowDataSerializationSchema serializer, GenericRowData row) {
        byte[] bytes = serializer.serialize(row);
        String result = new String(bytes, StandardCharsets.UTF_8);
        Assertions.assertThat(result).endsWith(";");
        return YTreeTextSerializer.deserialize(result.substring(0, result.length() - 1));
    }

    static YTreeNode serializeAndParse(RowType schema, GenericRowData row) {
        return serializeAndParse(createSerializer(schema), row);
    }

    @Nonnull
    static Map<String, Integer> toJavaStringIntegerMap(MapData map) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(map.keyArray().getString(i).toString(), map.valueArray().getInt(i));
        }
        return result;
    }
}
