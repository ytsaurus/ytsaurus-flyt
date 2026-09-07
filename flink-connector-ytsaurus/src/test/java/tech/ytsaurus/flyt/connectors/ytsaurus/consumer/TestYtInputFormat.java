package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Input format that talks to a {@link FakeYtCluster} instead of a real proxy.
 *
 * <p>Holds no reference to the fake so that it survives the serialization round trip
 * {@code InputFormatCacheLoader} performs on every reload: the cluster is resolved by table path.
 */
final class TestYtInputFormat extends YtRowDataInputFormat {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("checkstyle:parameternumber")
    private TestYtInputFormat(
            ComplexYtPath path,
            String ysonSchemaString,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo,
            CredentialsProvider credentialsProvider,
            SerializableSupplier<RetryStrategy> retryStrategy) {
        super(path, ysonSchemaString, limit, deserializer, rowDataTypeInfo, credentialsProvider,
                retryStrategy);
    }

    static TestYtInputFormat create(
            String basePath,
            String tableName,
            SerializableSupplier<RetryStrategy> retryStrategy) {
        return new TestYtInputFormat(
                ComplexYtPath.builder()
                        .clusterName("fake")
                        .basePath(basePath)
                        .tableName(tableName)
                        .build(),
                "<>[]",
                -1,
                new IdDeserializer(),
                TypeInformation.of(RowData.class),
                new StubCredentialsProvider(),
                retryStrategy);
    }

    @Override
    protected YTsaurusClient createClient(ComplexYtPath path) {
        return FakeYtCluster.get(path.getFullPath()).client();
    }

    /** Decodes the single {@code id} column the fake cluster serves. */
    private static final class IdDeserializer implements YTreeNodeDeserializationSchema {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData deserialize(YTreeNode node) {
            return GenericRowData.of(node.mapNode().getOrThrow("id").intValue());
        }

        @Override
        public RowData deserialize(byte[] message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    private static final class StubCredentialsProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;

        @Override
        public String getProviderIdentifier() {
            return "stub";
        }

        @Override
        public OAuthCredentialsConfig getCredentials(String clusterName) {
            return null;
        }
    }
}
