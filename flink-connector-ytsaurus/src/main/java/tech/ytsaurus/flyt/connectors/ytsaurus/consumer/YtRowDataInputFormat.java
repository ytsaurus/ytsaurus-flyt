package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

@Slf4j
public class YtRowDataInputFormat extends AbstractYtRowDataInputFormat {
    private static final long serialVersionUID = 1L;

    private final ComplexYtPath path;

    @Builder
    public YtRowDataInputFormat(
            ComplexYtPath path,
            String ysonSchemaString,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo,
            CredentialsProvider credentialsProvider,
            SerializableSupplier<RetryStrategy> retryStrategy) {

        super(ysonSchemaString, limit, deserializer, rowDataTypeInfo, credentialsProvider, retryStrategy);
        this.path = path;
    }

    @Override
    protected ComplexYtPath resolvePath() {
        log.info("Path is {}", path);
        return path;
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing here
    }
}
