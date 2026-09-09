package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.util.Objects;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;

public class YtQueueRowDataDeserializer implements YtQueueRecordDeserializer<RowData> {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> deserializationSchema;

    public YtQueueRowDataDeserializer(DeserializationSchema<RowData> deserializationSchema) {
        this.deserializationSchema = Objects.requireNonNull(deserializationSchema);
    }

    @Override
    public void open(SourceReaderContext context) throws Exception {
        Objects.requireNonNull(context, "context");
        deserializationSchema.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return context.metricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return context.getUserCodeClassLoader();
            }
        });
    }

    @Override
    public RowData deserialize(UnversionedRow row, TableSchema schema) throws Exception {
        YTreeMapNode node = row.toYTreeMap(schema, true);
        if (deserializationSchema instanceof YTreeNodeDeserializationSchema) {
            return ((YTreeNodeDeserializationSchema) deserializationSchema).deserialize(node);
        }
        return deserializationSchema.deserialize(node.toBinary());
    }
}
