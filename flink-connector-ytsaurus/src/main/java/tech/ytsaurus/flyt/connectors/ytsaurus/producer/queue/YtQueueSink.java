package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import java.io.IOException;
import java.util.Objects;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

public class YtQueueSink implements Sink<RowData> {
    private static final long serialVersionUID = 1L;

    private final String proxy;
    private final String queuePath;
    private final CredentialsProvider credentialsProvider;
    private final SerializationSchema<RowData> serializationSchema;
    private final RowType rowType;
    private final YtQueueWriteMode writeMode;
    private final String valueColumn;
    private final YtQueueWriterOptions writerOptions;
    private final boolean useTls;
    private final SerializableSupplier<RetryStrategy> retryStrategy;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtQueueSink(
            String proxy,
            String queuePath,
            CredentialsProvider credentialsProvider,
            SerializationSchema<RowData> serializationSchema,
            RowType rowType,
            YtQueueWriteMode writeMode,
            String valueColumn,
            YtQueueWriterOptions writerOptions,
            boolean useTls,
            SerializableSupplier<RetryStrategy> retryStrategy) {
        this.proxy = Objects.requireNonNull(proxy, "YTsaurus proxy must not be null");
        this.queuePath = Objects.requireNonNull(queuePath, "Queue path must not be null");
        this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "Credentials provider must not be null");
        this.serializationSchema = Objects.requireNonNull(serializationSchema, "Serialization schema must not be null");
        this.rowType = Objects.requireNonNull(rowType, "Row type must not be null");
        this.writeMode = Objects.requireNonNull(writeMode, "Queue write mode must not be null");
        this.valueColumn = Objects.requireNonNull(valueColumn, "Queue payload column must not be null");
        this.writerOptions = Objects.requireNonNull(writerOptions, "Queue writer options must not be null");
        this.useTls = useTls;
        this.retryStrategy = Objects.requireNonNull(retryStrategy, "Retry strategy supplier must not be null");
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        return createWriter(context.asSerializationSchemaInitializationContext(), context.metricGroup());
    }

    private SinkWriter<RowData> createWriter(
            SerializationSchema.InitializationContext serializationContext, MetricGroup metrics) throws IOException {
        YtQueueSinkWriter writer = new YtQueueSinkWriter(
                proxy, queuePath, credentialsProvider, serializationSchema, rowType, writeMode,
                valueColumn, writerOptions, useTls, retryStrategy);
        try {
            writer.open(serializationContext, metrics);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("Failed to initialize YTsaurus queue sink for '" + queuePath + "'", e);
        }
        return writer;
    }
}
