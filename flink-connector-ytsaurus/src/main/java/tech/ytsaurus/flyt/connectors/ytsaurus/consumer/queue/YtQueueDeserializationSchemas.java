package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue;

import java.util.Objects;

import lombok.experimental.UtilityClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

@UtilityClass
public class YtQueueDeserializationSchemas {
    public static void open(
            DeserializationSchema<?> deserializationSchema,
            SourceReaderContext context) throws Exception {
        Objects.requireNonNull(deserializationSchema, "deserializationSchema");
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
