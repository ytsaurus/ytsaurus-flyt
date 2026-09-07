package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.util.Objects;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;

abstract class YtQueueDeserializationSchemaAdapter<T> implements YtQueueRecordDeserializer<T> {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;

    YtQueueDeserializationSchemaAdapter(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = Objects.requireNonNull(deserializationSchema, "deserializationSchema");
    }

    protected final DeserializationSchema<T> deserializationSchema() {
        return deserializationSchema;
    }

    @Override
    public final void open(SourceReaderContext context) throws Exception {
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
}
