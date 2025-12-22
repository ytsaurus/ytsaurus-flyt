package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;

public class SerializableTableSchema implements Serializable {
    private final byte[] ysonSchema;

    public SerializableTableSchema(TableSchema schema) {
        var output = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(schema.toYTree(), output);
        this.ysonSchema = output.toByteArray();
    }

    public TableSchema toTableSchema() {
        var node = YTreeBinarySerializer.deserialize(new ByteArrayInputStream(ysonSchema));
        return TableSchema.fromYTree(node);
    }
}
