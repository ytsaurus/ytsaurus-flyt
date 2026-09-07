package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.time.Duration;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.SerializationUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;

import static org.assertj.core.api.Assertions.assertThat;

class YtRowDataInputFormatSerializationTest {

    /**
     * InputFormatCacheLoader clones the input format once per split on every FULL cache reload.
     * A non-serializable field would surface as a reload failure, which permanently disables the
     * shared LookupFullCache.
     */
    @Test
    void inputFormatSurvivesCloningDoneOnEveryFullCacheReload() throws Exception {
        YtRowDataInputFormat format = YtRowDataInputFormat.builder()
                .path(ComplexYtPath.builder()
                        .clusterName("cluster")
                        .basePath("//home/some/dir")
                        .tableName("table")
                        .build())
                .ysonSchemaString("<>[]")
                .limit(-1)
                .deserializer(new StubDeserializer())
                .rowDataTypeInfo(TypeInformation.of(RowData.class))
                .credentialsProvider(new StubCredentialsProvider())
                .retryStrategy(() -> new FixedRetryStrategy(0, Duration.ZERO))
                .build();

        var bytes = SerializationUtils.serialize(format);
        var restored = (YtRowDataInputFormat) SerializationUtils.deserialize(bytes);

        assertThat(restored).isNotNull();
        assertThat(restored.getProducedType()).isEqualTo(format.getProducedType());
    }

    private static final class StubDeserializer implements DeserializationSchema<RowData> {
        @Override
        public RowData deserialize(byte[] message) {
            return null;
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
