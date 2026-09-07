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

    @Test
    void transientFailureDuringReloadLosesNoRowsAndDuplicatesNone() throws Exception {
        FakeYtCluster cluster = new FakeYtCluster(10, 4, 1);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).as("the failure must actually have been injected").isZero();
    }

    @Test
    void repeatedFailuresStillProduceEveryRowExactlyOnce() throws Exception {
        FakeYtCluster cluster = new FakeYtCluster(20, 7, 3);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).isZero();
    }

    @Test
    void readerIsReopenedAtTheRowItFailedOn() throws Exception {
        FakeYtCluster cluster = new FakeYtCluster(10, 4, 1);

        reload(cluster, IMMEDIATE_RETRIES);

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
        FakeYtCluster cluster = new FakeYtCluster(10, 4, 1);

        assertThatThrownBy(() -> reload(cluster, NO_RETRIES))
                .hasMessageContaining(FULL_PATH)
                .hasRootCauseMessage("transient YT failure at row 4");
    }

    @Test
    void healthyReadIssuesASingleRequest() throws Exception {
        FakeYtCluster cluster = new FakeYtCluster(5, 0, 0);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.requestedStartRows()).containsExactly(0);
    }

    /**
     * The first load blocks LookupFullCache#open, so it must fail fast and let a restart rebuild
     * the cache rather than stall task startup with retries.
     */
    @Test
    void firstLoadDoesNotRetryEvenWhenRetriesAreConfigured() {
        FakeYtCluster cluster = new FakeYtCluster(10, 4, 1);

        assertThatThrownBy(() -> readAll(cluster, IMMEDIATE_RETRIES, 1))
                .hasRootCauseMessage("transient YT failure at row 4");
    }

    /** Reads as a FULL cache reload, i.e. not the blocking first load. */
    private List<Integer> reload(FakeYtCluster cluster, SerializableSupplier<RetryStrategy> retryStrategy)
            throws Exception {
        return readAll(cluster, retryStrategy, 2);
    }

    private List<Integer> readAll(
            FakeYtCluster cluster, SerializableSupplier<RetryStrategy> retryStrategy, int loadNumber)
            throws Exception {
        YtRowDataInputFormat format = newFormat(cluster, retryStrategy);
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


    /**
     * Overrides the {@code createClient} seam so the format talks to the fake cluster. The seam is
     * re-invoked on every deserialized copy, which a field holding the client would not survive.
     */
    private static YtRowDataInputFormat newFormat(
            FakeYtCluster cluster, SerializableSupplier<RetryStrategy> retryStrategy) {
        return new YtRowDataInputFormat(
                ComplexYtPath.builder().clusterName("fake").basePath(BASE_PATH).tableName(TABLE).build(),
                "<>[]",
                -1,
                new IdDeserializer(),
                TypeInformation.of(RowData.class),
                new StubCredentialsProvider(),
                retryStrategy) {
            private static final long serialVersionUID = 1L;

            @Override
            protected YTsaurusClient createClient(ComplexYtPath path) {
                return cluster.client();
            }
        };
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
