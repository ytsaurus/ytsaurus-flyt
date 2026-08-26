package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

/**
 * A Caffeine-backed writer cache with conditional expiration.
 *
 * <p>Native Caffeine expiration cannot veto eviction when a writer has uncommitted data. This class uses
 * Caffeine for atomic per-key operations and checks the writer state immediately before closing and removing it.
 */
@Slf4j
final class YtDynamicTableWriterCache {
    private final Cache<String, CacheEntry> cache;
    private final long ttlNanos;
    private final long cleanupPeriodNanos;
    private final Ticker ticker;
    private final Object cleanupLock = new Object();

    @Nullable
    private ScheduledExecutorService cleanupExecutor;
    @Nullable
    private ScheduledFuture<?> cleanupFuture;
    private volatile boolean cleanupEnabled;

    YtDynamicTableWriterCache(Duration ttl, Duration cleanupPeriod) {
        this(ttl, cleanupPeriod, Ticker.systemTicker());
    }

    @VisibleForTesting
    YtDynamicTableWriterCache(Duration ttl, Duration cleanupPeriod, Ticker ticker) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(!ttl.isNegative(), "Cache TTL must not be negative");
        Preconditions.checkNotNull(cleanupPeriod);
        Preconditions.checkArgument(!cleanupPeriod.isZero() && !cleanupPeriod.isNegative(),
                "Cache cleanup period must be positive");

        this.cache = Caffeine.newBuilder().build();
        this.ttlNanos = ttl.toNanos();
        this.cleanupPeriodNanos = cleanupPeriod.toNanos();
        this.ticker = Preconditions.checkNotNull(ticker);
    }

    YtDynamicTableWriter getOrAcquire(String tableName, Supplier<YtDynamicTableWriter> writerSupplier) {
        CacheEntry result = cache.asMap().compute(tableName, (ignored, current) -> {
            if (current == null) {
                YtDynamicTableWriter writer = Preconditions.checkNotNull(writerSupplier.get());
                return new CacheEntry(writer, ticker.read(), 0);
            }
            return current.withAccessTime(ticker.read());
        });
        return Preconditions.checkNotNull(result).getWriter();
    }

    Collection<YtDynamicTableWriter> valuesSnapshot() {
        return cache.asMap().values().stream()
                .map(CacheEntry::getWriter)
                .collect(Collectors.toUnmodifiableList());
    }

    synchronized void startCleanup() {
        Preconditions.checkState(cleanupFuture == null, "Cache cleanup is already scheduled");
        cleanupEnabled = true;
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        cleanupFuture = cleanupExecutor.scheduleWithFixedDelay(
                this::runScheduledCleanup,
                cleanupPeriodNanos,
                cleanupPeriodNanos,
                TimeUnit.NANOSECONDS);
    }

    private void runScheduledCleanup() {
        synchronized (cleanupLock) {
            if (cleanupEnabled) {
                cleanupExpiredEntries();
            }
        }
    }

    @VisibleForTesting
    void cleanupExpired() {
        synchronized (cleanupLock) {
            cleanupExpiredEntries();
        }
    }

    private void cleanupExpiredEntries() {
        long now = ticker.read();
        try {
            for (String key : cache.asMap().keySet()) {
                cache.asMap().computeIfPresent(key, (ignored, entry) -> cleanupEntry(key, entry, now));
            }
        } catch (Exception e) {
            log.error("Unable to finish writer cache cleanup", e);
        }
    }

    private CacheEntry cleanupEntry(String key, CacheEntry entry, long now) {
        if (!entry.isExpired(now, ttlNanos)) {
            return entry;
        }

        try {
            if (entry.getWriter().isBusy()) {
                return entry;
            }
            entry.getWriter().close();
            return null;
        } catch (Exception e) {
            CacheEntry failedEntry = entry.withCleanupFailure();
            log.error("Unable to clean up writer cache entry for key: {} "
                            + "(total cleanup failures for the entry: {})",
                    key, failedEntry.getCleanupFailureCount(), e);
            return failedEntry;
        }
    }

    void stopCleanup() {
        ScheduledExecutorService executor;
        ScheduledFuture<?> future;
        synchronized (this) {
            future = Preconditions.checkNotNull(cleanupFuture, "Cache cleanup has not been scheduled");
            executor = Preconditions.checkNotNull(cleanupExecutor, "Cache cleanup executor is not initialized");
            cleanupEnabled = false;
            future.cancel(false);
            executor.shutdown();
        }

        synchronized (cleanupLock) {
            // Wait for an already running cleanup before the pool closes the remaining writers.
        }

        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                log.warn("Writer cache cleanup executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while stopping writer cache cleanup executor", e);
        }
    }

    @VisibleForTesting
    int getSize() {
        return cache.asMap().size();
    }

    @VisibleForTesting
    boolean isCleanupTerminated() {
        ScheduledExecutorService executor = cleanupExecutor;
        return executor != null && executor.isTerminated();
    }

    @Getter
    private static final class CacheEntry {
        private final YtDynamicTableWriter writer;
        private final long accessedAtNanos;
        private final int cleanupFailureCount;

        private CacheEntry(YtDynamicTableWriter writer, long accessedAtNanos, int cleanupFailureCount) {
            this.writer = writer;
            this.accessedAtNanos = accessedAtNanos;
            this.cleanupFailureCount = cleanupFailureCount;
        }

        private CacheEntry withAccessTime(long newAccessedAtNanos) {
            return new CacheEntry(writer, newAccessedAtNanos, cleanupFailureCount);
        }

        private CacheEntry withCleanupFailure() {
            return new CacheEntry(writer, accessedAtNanos, cleanupFailureCount + 1);
        }

        private boolean isExpired(long now, long ttl) {
            return now - accessedAtNanos >= ttl;
        }
    }
}
