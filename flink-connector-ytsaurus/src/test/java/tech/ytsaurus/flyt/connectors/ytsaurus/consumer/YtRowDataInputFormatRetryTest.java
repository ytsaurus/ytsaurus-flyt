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
 * Covers the read path that feeds a lookup 'FULL' cache: a transient YT failure while opening the
 * reader must not fail the reload, because a failed reload permanently disables the shared cache.
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
    void transientFailureWhileOpeningIsRetried() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 1);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).as("the failure must actually have been injected").isZero();
        assertThat(cluster.requestedPaths()).hasSize(2);
    }

    @Test
    void repeatedOpenFailuresAreRetried() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(20, 3);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.failuresLeft()).isZero();
    }

    /** The strategy allows 5 retries, so the 5th failure must still be recovered from. */
    @Test
    void everyAllowedRetryIsUsed() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 5);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.requestedPaths())
                .as("five failed opens plus the successful one")
                .hasSize(6);
    }

    @Test
    void oneFailureBeyondTheBudgetPropagates() {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 6);

        assertThatThrownBy(() -> reload(cluster, IMMEDIATE_RETRIES))
                .hasMessageContaining(FULL_PATH);
        assertThat(cluster.requestedPaths()).hasSize(6);
    }

    /**
     * Resuming mid-stream would need a row-index range, which YT rejects on the sorted dynamic
     * tables a FULL cache is built over, and re-reading from the start would duplicate the rows
     * already put in the cache. So a mid-read failure is surfaced rather than retried.
     */
    @Test
    void failureMidReadIsNotRetried() {
        FakeYtCluster cluster = FakeYtCluster.failingAtRow(10, 4, 1);

        assertThatThrownBy(() -> reload(cluster, IMMEDIATE_RETRIES))
                .hasMessageContaining(FULL_PATH)
                .hasMessageContaining("row 4")
                .hasRootCauseMessage("transient YT failure at row 4");
        assertThat(cluster.requestedPaths())
                .as("the reader is not re-opened, so no row can be duplicated")
                .hasSize(1);
    }

    @Test
    void withoutRetriesTheOpenFailurePropagates() {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 1);

        assertThatThrownBy(() -> reload(cluster, NO_RETRIES))
                .hasMessageContaining(FULL_PATH)
                .hasRootCauseMessage("transient YT failure while opening the reader");
    }

    @Test
    void healthyReadIssuesASingleRequest() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.healthy(5);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.requestedPaths()).containsExactly(FULL_PATH);
    }

    /**
     * Guards against reintroducing a row-index resume: YT rejects those on sorted dynamic tables
     * with "Row index selectors are not supported for sorted dynamic tables".
     */
    @Test
    void readRequestsNeverCarryARowRange() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 2);

        reload(cluster, IMMEDIATE_RETRIES);

        assertThat(cluster.requestedPaths()).isNotEmpty().allSatisfy(
                requested -> assertThat(requested).isEqualTo(FULL_PATH));
    }

    /**
     * An empty batch on a live reader is a client-side race, not a failure: the reader is reused,
     * so retrying it cannot duplicate a row.
     */
    @Test
    void emptyBatchOnALiveReaderIsRetried() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.returningEmptyBatches(10, 2);

        assertThat(reload(cluster, IMMEDIATE_RETRIES)).isEqualTo(cluster.expectedIds());
        assertThat(cluster.requestedPaths())
                .as("the same reader is reused, so no second readTable is issued")
                .hasSize(1);
    }

    /** Empty batches follow the same policy as any other retry, so the first load fails fast. */
    @Test
    void emptyBatchOnTheFirstLoadIsNotRetried() {
        FakeYtCluster cluster = FakeYtCluster.returningEmptyBatches(10, 1);

        assertThatThrownBy(() -> readAll(cluster, IMMEDIATE_RETRIES, 1))
                .rootCause().hasMessageContaining("not at EOF");
    }

    @Test
    void endlessEmptyBatchesFailInsteadOfSpinning() {
        FakeYtCluster cluster = FakeYtCluster.returningEmptyBatches(10, Integer.MAX_VALUE);

        assertThatThrownBy(() -> reload(cluster, IMMEDIATE_RETRIES))
                .hasMessageContaining(FULL_PATH)
                .rootCause().hasMessageContaining("not at EOF");
    }

    /**
     * The first load blocks LookupFullCache#open, so it must fail fast and let a restart rebuild
     * the cache rather than stall task startup with retries.
     */
    @Test
    void firstLoadDoesNotRetryEvenWhenRetriesAreConfigured() {
        FakeYtCluster cluster = FakeYtCluster.failingOnOpen(10, 1);

        assertThatThrownBy(() -> readAll(cluster, IMMEDIATE_RETRIES, 1))
                .hasRootCauseMessage("transient YT failure while opening the reader");
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
