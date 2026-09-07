package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.inputformat.InputFormatCacheLoader;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end cover for the reported failure: a transient YT error during a lookup 'FULL' cache
 * reload used to poison the shared cache for the rest of the TaskManager's life, because
 * {@code CacheLoader} sets {@code isStopped} and {@code LookupFullCache} keeps {@code
 * reloadFailCause} forever. Retrying inside the input format keeps the error from ever getting
 * there.
 */
class YtLookupFullCacheTest {

    private static final String BASE_PATH = "//home/test";
    private static final String TABLE = "table";
    private static final String FULL_PATH = BASE_PATH + "/" + TABLE;
    private static final int ROW_COUNT = 10;

    private static final SerializableSupplier<RetryStrategy> IMMEDIATE_RETRIES =
            () -> new FixedRetryStrategy(5, Duration.ZERO);
    private static final SerializableSupplier<RetryStrategy> NO_RETRIES =
            () -> new FixedRetryStrategy(0, Duration.ZERO);

    private final ManualReloadTrigger trigger = new ManualReloadTrigger();

    @AfterEach
    void tearDown() {
        FakeYtCluster.reset();
    }

    @Test
    void transientFailureDuringReloadKeepsTheCacheUsable() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, ROW_COUNT, 0, 0);
        InputFormatCacheLoader cacheLoader = cacheLoader(IMMEDIATE_RETRIES);

        try (LookupFullCache cache = openCache(cacheLoader)) {
            assertThat(cache.getIfPresent(key(4))).hasSize(1);

            cluster.armFailures(4, 1);
            trigger.trigger();

            assertThat(cluster.failuresLeft()).as("the failure must have been injected").isZero();
            assertThat(cache.getIfPresent(key(4)))
                    .as("the cache still serves after a transient failure")
                    .hasSize(1);
            assertThat(cache.size()).isEqualTo(ROW_COUNT);
        }
    }

    @Test
    void reloadsKeepWorkingAfterATransientFailure() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, ROW_COUNT, 0, 0);
        InputFormatCacheLoader cacheLoader = cacheLoader(IMMEDIATE_RETRIES);

        try (LookupFullCache cache = openCache(cacheLoader)) {
            cluster.armFailures(4, 1);
            trigger.trigger();
            trigger.trigger();

            assertThat(cache.size())
                    .as("later reloads still run, so the cache is not frozen")
                    .isEqualTo(ROW_COUNT);
            assertThat(cache.getIfPresent(key(9))).hasSize(1);
        }
    }

    /**
     * Reproduces the original bug with retries switched off, which is what the connector did before
     * the fix: one transient error and every later lookup throws the stale cause, with no further
     * reload attempted.
     */
    @Test
    void withoutRetriesASingleFailurePoisonsTheCacheForGood() throws Exception {
        FakeYtCluster cluster = FakeYtCluster.register(FULL_PATH, ROW_COUNT, 0, 0);
        InputFormatCacheLoader cacheLoader = cacheLoader(NO_RETRIES);

        try (LookupFullCache cache = openCache(cacheLoader)) {
            cluster.armFailures(4, 1);
            trigger.trigger();

            assertThatThrownBy(() -> cache.getIfPresent(key(4)))
                    .hasRootCauseMessage("transient YT failure at row 4");

            int requestsSoFar = cluster.requestedPaths().size();
            trigger.trigger();
            assertThat(cluster.requestedPaths())
                    .as("the loader never touches YT again")
                    .hasSize(requestsSoFar);
            assertThatThrownBy(() -> cache.getIfPresent(key(4)))
                    .hasRootCauseMessage("transient YT failure at row 4");
        }
    }

    private LookupFullCache openCache(InputFormatCacheLoader cacheLoader) throws Exception {
        LookupFullCache cache = new LookupFullCache(cacheLoader, trigger);
        cache.setUserCodeClassLoader(Thread.currentThread().getContextClassLoader());
        cache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        return cache;
    }

    private static InputFormatCacheLoader cacheLoader(
            SerializableSupplier<RetryStrategy> retryStrategy) throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()))
                .getLogicalType();
        RowDataSerializer rowSerializer = (RowDataSerializer) InternalSerializers.create(rowType);

        @SuppressWarnings("rawtypes")
        GeneratedProjection generatedProjection = new GeneratedProjection("", "", new Object[0]) {
            @Override
            public Projection<RowData, RowData> newInstance(ClassLoader classLoader) {
                return row -> GenericRowData.of(row.getInt(0));
            }
        };
        GenericRowDataKeySelector keySelector = new GenericRowDataKeySelector(
                InternalTypeInfo.of(rowType), InternalSerializers.create(rowType), generatedProjection);

        InputFormatCacheLoader cacheLoader = new InputFormatCacheLoader(
                TestYtInputFormat.create(BASE_PATH, TABLE, retryStrategy),
                keySelector,
                rowSerializer);
        cacheLoader.open(new Configuration(), Thread.currentThread().getContextClassLoader());
        return cacheLoader;
    }

    /** Builds the lookup key exactly as the key selector does, so map lookups match. */
    private static RowData key(int id) {
        return GenericRowData.of(id);
    }

    /** {@link CacheReloadTrigger} driven by the test rather than by a timer. */
    private static final class ManualReloadTrigger implements CacheReloadTrigger {
        private static final long serialVersionUID = 1L;

        private transient Context context;

        void trigger() throws Exception {
            context.triggerReload().get();
        }

        @Override
        public void open(Context context) throws Exception {
            this.context = context;
            trigger();
        }

        @Override
        public void close() {
        }
    }

}
