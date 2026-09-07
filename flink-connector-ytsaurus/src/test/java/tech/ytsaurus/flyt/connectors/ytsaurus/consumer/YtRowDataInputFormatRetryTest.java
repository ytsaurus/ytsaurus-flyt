package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the read path that feeds a lookup 'FULL' cache: a transient YT failure must not lose or
 * duplicate rows, because a failed reload permanently disables the shared cache.
 */
class YtRowDataInputFormatRetryTest {

    private static final String BASE_PATH = "//home/test";
    private static final String TABLE = "table";
    private static final String FULL_PATH = BASE_PATH + "/" + TABLE;

    /** No backoff, so the tests do not sleep; the retry count is what matters here. */
    private static final SerializableSupplier<RetryStrategy> IMMEDIATE_RETRIES =
            () -> new FixedRetryStrategy(5, Duration.ZERO);

    private static final SerializableSupplier<RetryStrategy> NO_RETRIES =
            () -> new FixedRetryStrategy(0, Duration.ZERO);

    @AfterEach
    void tearDown() {
        FakeYtCluster.reset();
    }

    @Test
    void transientFailureDuringReloadLosesNoRowsAndDuplicatesNone() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, 10, 4, 1);

        assertThat(reload(IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).as("the failure must actually have been injected").isZero();
    }

    @Test
    void repeatedFailuresStillProduceEveryRowExactlyOnce() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, 20, 7, 3);

        assertThat(reload(IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).isZero();
    }

    @Test
    void readerIsReopenedAtTheRowItFailedOn() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, 10, 4, 1);

        reload(IMMEDIATE_RETRIES);

        assertThat(cluster.requestedStartRows())
                .as("the retry resumes at the first row that was not emitted")
                .containsExactly(0, 4);
        assertThat(cluster.requestedPaths().get(0))
                .as("a fresh read carries no range")
                .isEqualTo(FULL_PATH);
        assertThat(cluster.requestedPaths().get(1))
                .as("the range really is sent to YT, not just tracked locally")
                .contains("row_index")
                .endsWith(FULL_PATH);
    }

    @Test
    void withoutRetriesTheFailurePropagates() {
        FakeYtCluster.register(FULL_PATH, 10, 4, 1);

        assertThatThrownBy(() -> reload(NO_RETRIES))
                .hasMessageContaining(FULL_PATH)
                .hasRootCauseMessage("transient YT failure at row 4");
    }

    @Test
    void healthyReadIssuesASingleRequest() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, 5, 0, 0);

        assertThat(reload(IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.requestedStartRows()).containsExactly(0);
    }

    /**
     * The first load blocks LookupFullCache#open, so it must fail fast and let a restart rebuild
     * the cache rather than stall task startup with retries.
     */
    @Test
    void firstFullCacheLoadDoesNotRetryEvenWhenRetriesAreConfigured() {
        FakeYtCluster.register(FULL_PATH, 10, 4, 1);

        assertThatThrownBy(() -> readAll(IMMEDIATE_RETRIES, true, 1))
                .hasRootCauseMessage("transient YT failure at row 4");
    }

    /** A plain scan has no such blocking phase, so its very first read honours the strategy. */
    @Test
    void plainScanRetriesOnItsFirstRead() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, 10, 4, 1);

        assertThat(readAll(IMMEDIATE_RETRIES, false, 1)).isEqualTo(cluster.expectedIds());
    }

    /** Reads as a FULL cache reload, i.e. not the blocking first load. */
    private List<Integer> reload(SerializableSupplier<RetryStrategy> retryStrategy) throws Exception {
        return readAll(retryStrategy, true, 2);
    }

    private List<Integer> readAll(
            SerializableSupplier<RetryStrategy> retryStrategy, boolean fullCacheLoader, int loadNumber)
            throws Exception {
        TestInputFormat format = newFormat(retryStrategy, fullCacheLoader);
        List<Integer> ids = new ArrayList<>();
        try {
            format.openInputFormat();
            InputSplit split = null;
            for (int load = 0; load < loadNumber; load++) {
                split = format.createInputSplits(1)[0];
            }
            format.open(split);
            while (!format.reachedEnd()) {
                RowData row = format.nextRecord(null);
                if (row == null) {
                    break;
                }
                ids.add(row.getInt(0));
            }
        } finally {
            format.close();
            format.closeInputFormat();
        }
        return ids;
    }

    private static TestInputFormat newFormat(
            SerializableSupplier<RetryStrategy> retryStrategy, boolean fullCacheLoader) {
        return new TestInputFormat(
                ComplexYtPath.builder().clusterName("fake").basePath(BASE_PATH).tableName(TABLE).build(),
                "<>[]",
                -1,
                new IdDeserializer(),
                TypeInformation.of(RowData.class),
                new StubCredentialsProvider(),
                retryStrategy,
                fullCacheLoader);
    }

    /** Resolves the YT client to the in-memory cluster instead of dialling a real proxy. */
    private static final class TestInputFormat extends YtRowDataInputFormat {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("checkstyle:parameternumber")
        private TestInputFormat(
                ComplexYtPath path,
                String ysonSchemaString,
                long limit,
                org.apache.flink.api.common.serialization.DeserializationSchema<RowData> deserializer,
                TypeInformation<RowData> rowDataTypeInfo,
                CredentialsProvider credentialsProvider,
                SerializableSupplier<RetryStrategy> retryStrategy,
                boolean fullCacheLoader) {
            super(path, ysonSchemaString, limit, deserializer, rowDataTypeInfo, credentialsProvider,
                    retryStrategy, fullCacheLoader);
        }

        @Override
        protected YTsaurusClient createClient(ComplexYtPath path) {
            return FakeYtCluster.get(path.getFullPath()).client();
        }
    }

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
