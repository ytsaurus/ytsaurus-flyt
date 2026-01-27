package tech.ytsaurus.flyt.formats.yson;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

public class YsonRowDataSerializationSchema implements SerializationSchema<RowData> {

    private final RowType rowType;

    private final RowDataToYsonConverters.RowDataToYsonConverter runtimeConverter;

    public YsonRowDataSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
        this.rowType = rowType;
        this.runtimeConverter =
                new RowDataToYsonConverters(timestampFormat)
                        .createConverter(rowType);
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            YTreeNode node = runtimeConverter.convert(YTree.entityNode(), row);
            String serializedNode = YTreeTextSerializer.serialize(node) + ';';
            return serializedNode.getBytes(StandardCharsets.UTF_8);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Could not serialize row '%s'.", row), t);
        }
    }

    public RowDataToYsonConverters.RowDataToYsonConverter getRuntimeConverter() {
        return runtimeConverter;
    }
}
