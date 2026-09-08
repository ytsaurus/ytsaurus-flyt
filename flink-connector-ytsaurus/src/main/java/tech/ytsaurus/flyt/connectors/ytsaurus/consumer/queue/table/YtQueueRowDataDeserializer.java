package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;

public class YtQueueRowDataDeserializer extends YtQueueDeserializationSchemaAdapter<RowData> {
    private static final long serialVersionUID = 1L;

    public YtQueueRowDataDeserializer(DeserializationSchema<RowData> deserializationSchema) {
        super(deserializationSchema);
    }

    @Override
    public RowData deserialize(UnversionedRow row, TableSchema schema) throws Exception {
        DeserializationSchema<RowData> deserializationSchema = deserializationSchema();
        YTreeMapNode node = row.toYTreeMap(schema, true);
        if (deserializationSchema instanceof YTreeNodeDeserializationSchema) {
            return ((YTreeNodeDeserializationSchema) deserializationSchema).deserialize(node);
        }
        return deserializationSchema.deserialize(node.toBinary());
    }
}
