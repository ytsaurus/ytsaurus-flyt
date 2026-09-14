package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

/**
 * A Caffeine-backed writer cache with conditional expiration.
 *
 * <p>An entry is pinned while it is being used or its writer has uncommitted data. Writer state changes are
 * published back to Caffeine so that its expiration policy can schedule eviction once the writer becomes idle.
 * The idle transition starts a fresh TTL. An eviction listener closes an expired writer synchronously before another
 * writer can be installed for the same table.
 */
@Slf4j
final class YtDynamicTableWriterCache {
    private static final Duration PINNED_DURATION = Duration.ofNanos(Long.MAX_VALUE);

    private final Cache<String, CacheEntry> cache;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object cleanupLock = new Object();
    private volatile boolean stopped;

    YtDynamicTableWriterCache(Duration ttl) {
        this(ttl, Ticker.systemTicker(), Scheduler.systemScheduler());
    }

    @VisibleForTesting
    YtDynamicTableWriterCache(Duration ttl, Ticker ticker) {
        this(ttl, ticker, Scheduler.disabledScheduler());
    }

    private YtDynamicTableWriterCache(Duration ttl, Ticker ticker, Scheduler scheduler) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(!ttl.isZero() && !ttl.isNegative(), "Cache TTL must be positive");

        this.cache = Caffeine.<String, CacheEntry>newBuilder()
                .expireAfter(Expiry.<String, CacheEntry>accessing(
                        (key, entry) -> entry.isPinned() ? PINNED_DURATION : ttl))
                .ticker(Preconditions.checkNotNull(ticker))
                .scheduler(Preconditions.checkNotNull(scheduler))
                .evictionListener(this::onEviction)
                .build();
    }

    WriterLease acquire(String tableName, Supplier<YtDynamicTableWriter> writerSupplier) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(writerSupplier);

        lifecycleLock.readLock().lock();
        boolean acquired = false;
        try {
            Preconditions.checkState(!stopped, "Writer cache is stopped");
            CacheEntry entry = cache.asMap().compute(tableName, (ignored, current) -> {
                CacheEntry result = current;
                if (result == null) {
                    YtDynamicTableWriter writer = Preconditions.checkNotNull(writerSupplier.get());
                    result = new CacheEntry(writer);
                    CacheEntry created = result;
                    writer.setCacheStateListener(() -> onWriterStateChanged(tableName, created));
                }
                result.activeUses++;
                return result;
            });
            acquired = true;
            return new WriterLease(tableName, Preconditions.checkNotNull(entry));
        } finally {
            if (!acquired) {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    YtDynamicTableWriter getOrAcquire(String tableName, Supplier<YtDynamicTableWriter> writerSupplier) {
        try (WriterLease lease = acquire(tableName, writerSupplier)) {
            return lease.getWriter();
        }
    }

    Collection<YtDynamicTableWriter> valuesSnapshot() {
        return cache.asMap().values().stream()
                .map(CacheEntry::getWriter)
                .collect(Collectors.toUnmodifiableList());
    }

    Collection<YtDynamicTableWriter> stopCleanup() {
        lifecycleLock.writeLock().lock();
        try {
            Preconditions.checkState(!stopped, "Writer cache is already stopped");
            stopped = true;

            List<CacheEntry> entries = List.copyOf(cache.asMap().values());
            entries.forEach(CacheEntry::retire);
            cache.invalidateAll();
            synchronized (cleanupLock) {
                // Wait for an eviction that started before invalidateAll().
            }
            return entries.stream()
                    .filter(entry -> !entry.isClosed())
                    .map(CacheEntry::getWriter)
                    .collect(Collectors.toUnmodifiableList());
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private void release(String tableName, CacheEntry expected) {
        try {
            cache.asMap().computeIfPresent(tableName, (ignored, current) -> {
                if (current == expected) {
                    Preconditions.checkState(current.activeUses > 0, "Writer cache lease is already released");
                    current.activeUses--;
                }
                return current;
            });
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private void onWriterStateChanged(String tableName, CacheEntry expected) {
        if (stopped || expected.retired) {
            return;
        }

        synchronized (expected) {
            try {
                boolean busy = expected.getWriter().isBusy();
                if (busy != expected.lastKnownBusy
                        && cache.asMap().replace(tableName, expected, expected)) {
                    expected.lastKnownBusy = busy;
                }
            } catch (RuntimeException e) {
                if (!stopped) {
                    log.error("Unable to update writer cache state for key: {}", tableName, e);
                }
            }
        }
    }

    private void onEviction(@Nullable String tableName, @Nullable CacheEntry entry, RemovalCause cause) {
        if (entry == null) {
            return;
        }

        synchronized (cleanupLock) {
            entry.retire();
            try {
                entry.closeOnce();
            } catch (Exception e) {
                log.error("Unable to close writer cache entry for key: {}", tableName, e);
            }
        }
    }

    @VisibleForTesting
    void cleanupExpired() {
        if (!stopped) {
            cache.cleanUp();
        }
    }

    @VisibleForTesting
    int getSize() {
        return cache.asMap().size();
    }

    private static final class CacheEntry {
        private final YtDynamicTableWriter writer;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int activeUses;
        private boolean lastKnownBusy;
        private volatile boolean retired;

        private CacheEntry(YtDynamicTableWriter writer) {
            this.writer = writer;
        }

        private YtDynamicTableWriter getWriter() {
            return writer;
        }

        private boolean isPinned() {
            return activeUses > 0 || writer.isBusy();
        }

        private void retire() {
            retired = true;
            writer.clearCacheStateListener();
        }

        private boolean isClosed() {
            return closed.get();
        }

        private void closeOnce() {
            if (closed.compareAndSet(false, true)) {
                writer.close();
            }
        }
    }

    /** A lease must be closed by the thread that acquired it. */
    final class WriterLease implements AutoCloseable {
        private final String tableName;
        private final CacheEntry entry;
        private final AtomicBoolean released = new AtomicBoolean();

        private WriterLease(String tableName, CacheEntry entry) {
            this.tableName = tableName;
            this.entry = entry;
        }

        YtDynamicTableWriter getWriter() {
            return entry.getWriter();
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release(tableName, entry);
            }
        }
    }
}
