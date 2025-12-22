package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;
import tech.ytsaurus.core.common.YTsaurusError;

import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster.ClusterPickStrategy;

@Slf4j
public class YtRowDataMulticlusterLookupFunction extends LookupFunction {
    private static final Long REFRESH_TERMINATION_TIMEOUT_MS = 60_000L;
    private static final Long REFRESH_TIMEOUT_MS = 60_000L;
    private static final int MAX_LOOKUP_RETRIES = 3;

    private final SerializableFunction<String, YtRowDataLookupFunction> lookupFunctionFactory;
    private final ClusterPickStrategy clusterStrategy;
    private final ReadableConfig options;

    private transient ScheduledExecutorService clusterRefresher;
    private transient ReentrantReadWriteLock lookupLock;
    private transient AtomicReference<String> currentCluster;
    private transient AtomicReference<YtRowDataLookupFunction> currentLookupFunction;
    private transient FunctionContext savedContext;
    private transient volatile boolean isRefreshing;

    public YtRowDataMulticlusterLookupFunction(
            ClusterPickStrategy clusterStrategy,
            SerializableFunction<String, YtRowDataLookupFunction> lookupFunctionFactory,
            ReadableConfig options) {
        Preconditions.checkNotNull(clusterStrategy);
        Preconditions.checkNotNull(lookupFunctionFactory);
        Preconditions.checkNotNull(options);
        this.clusterStrategy = clusterStrategy;
        this.lookupFunctionFactory = lookupFunctionFactory;
        this.options = options;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.savedContext = context;
        this.currentCluster = new AtomicReference<>();
        this.currentLookupFunction = new AtomicReference<>();
        this.lookupLock = new ReentrantReadWriteLock();
        this.isRefreshing = false;
        this.clusterStrategy.open(options);
        refreshYtCluster();
        this.clusterRefresher = Executors.newScheduledThreadPool(1);
        this.clusterRefresher.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshYtCluster();
                    } catch (Exception e) {
                        log.error("Failed to refresh YT cluster for {}",
                                getPathString(currentLookupFunction.get()), e);
                    }
                },
                clusterStrategy.getPollingPeriod().toMillis(),
                clusterStrategy.getPollingPeriod().toMillis(),
                TimeUnit.MILLISECONDS);
        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                context::getMetricGroup);
    }

    @Override
    public void close() throws Exception {
        try {
            YtRowDataLookupFunction current = currentLookupFunction.get();
            if (current != null) {
                current.close();
            }
        } finally {
            if (clusterRefresher != null) {
                clusterRefresher.shutdown();
                boolean terminated = clusterRefresher.awaitTermination(
                        REFRESH_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (!terminated) {
                    log.warn("Failed to terminate lookup function for cluster {} (table: {})",
                            currentCluster,
                            getPathString(currentLookupFunction.get()));
                    clusterRefresher.shutdownNow();
                } else {
                    log.info("Lookup function closed successfully for cluster {} (table: {})",
                            currentCluster,
                            getPathString(currentLookupFunction.get()));
                }
            }
            clusterStrategy.close();
        }
    }

    @Override
    public Collection<RowData> lookup(RowData rowData) throws IOException {
        boolean locked = true;
        for (int attempt = 0; attempt < MAX_LOOKUP_RETRIES; attempt++) {
            lookupLock.readLock().lock();
            try {
                return currentLookupFunction.get().lookup(rowData);
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof YTsaurusError)) {
                    throw e;
                }
                YTsaurusError cause = (YTsaurusError) e.getCause();
                if (!cause.isUnrecoverable()) {
                    throw e;
                }

                log.warn("Attempt {}/{}: Unrecoverable YT error during lookup at {}",
                        attempt + 1, MAX_LOOKUP_RETRIES,
                        getPathString(currentLookupFunction.get()), e);

                if (!clusterStrategy.isAvailable(currentCluster.get())) {
                    log.error("Cluster {} is unavailable, triggering refresh (current={})",
                            currentCluster.get(), getPathString(currentLookupFunction.get()));
                    locked = false;
                    lookupLock.readLock().unlock();
                    refreshYtCluster();
                    lookupLock.readLock().lock();
                    locked = true;
                }
            } finally {
                if (locked) {
                    lookupLock.readLock().unlock();
                }
            }
        }
        throw new IOException(String.format("Failed after %d lookup attempts at '%s'",
                MAX_LOOKUP_RETRIES, getPathString(currentLookupFunction.get())));
    }

    @SneakyThrows
    private void refreshYtCluster() {
        if (isRefreshing) {
            log.debug("Refresh already in progress");
            return;
        }

        if (!lookupLock.writeLock().tryLock(REFRESH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            log.warn("Could not acquire write lock for refresh");
            return;
        }

        isRefreshing = true;
        try {
            log.info("Refreshing YT Cluster for lookup {}... current={}",
                    getPathString(currentLookupFunction.get()),
                    currentCluster);

            String previous = currentCluster.get();
            String next = clusterStrategy.pickCluster();
            if (Objects.equals(next, previous)) {
                return;
            }

            YtRowDataLookupFunction previousLookupFunction = currentLookupFunction.get();
            YtRowDataLookupFunction nextLookupFunction = null;

            try {
                nextLookupFunction = lookupFunctionFactory.apply(next);
                nextLookupFunction.open(savedContext);

                if (previousLookupFunction != null) {
                    previousLookupFunction.close();
                }
            } catch (Exception e) {
                if (nextLookupFunction != null) {
                    try {
                        nextLookupFunction.close();
                    } catch (Exception ex) {
                        log.error("Failed to close partially initialized lookup function, next={}",
                                getPathString(nextLookupFunction),
                                ex);
                    }
                }
                log.error("Critical error during cluster refresh, current={}, next={}",
                        getPathString(currentLookupFunction.get()),
                        getPathString(nextLookupFunction),
                        e);
                throw e;
            }

            currentCluster.set(next);
            currentLookupFunction.set(nextLookupFunction);

            log.info("Cluster changed from {} to {} (table was: {}, now: {})",
                    previous, next,
                    getPathString(previousLookupFunction),
                    getPathString(nextLookupFunction));
        } finally {
            isRefreshing = false;
            lookupLock.writeLock().unlock();
        }
    }

    private String getPathString(YtRowDataLookupFunction function) {
        return Optional.ofNullable(function)
                .map(YtRowDataLookupFunction::getPath)
                .map(ComplexYtPath::toString)
                .orElse("<none>");
    }
}
