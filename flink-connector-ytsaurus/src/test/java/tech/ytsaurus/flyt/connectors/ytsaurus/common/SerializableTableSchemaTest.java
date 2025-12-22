package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.SerializationUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerializableTableSchemaTest {
    @Test
    public void testSerializable() throws Exception {
        var schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.INT64)
                .setUniqueKeys(true)
                .build();
        var serializableSchema = new SerializableTableSchema(schema);

        var bytes = SerializationUtils.serialize(serializableSchema);
        var result = (SerializableTableSchema) SerializationUtils.deserialize(bytes);

        assertTrue(result.toTableSchema().isUniqueKeys());
        assertThat(result.toTableSchema().getColumnNames()).containsExactlyInAnyOrder("key", "value");
    }
}
