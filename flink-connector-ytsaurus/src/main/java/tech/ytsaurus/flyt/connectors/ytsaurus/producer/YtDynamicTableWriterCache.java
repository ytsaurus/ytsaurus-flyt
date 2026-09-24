package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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
 * <p>An entry is pinned while it is being used or its writer has uncommitted data. Writer idle transitions are
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

    void withWriter(
            String tableName,
            Supplier<YtDynamicTableWriter> writerSupplier,
            Consumer<YtDynamicTableWriter> action) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(writerSupplier);
        Preconditions.checkNotNull(action);

        lifecycleLock.readLock().lock();
        CacheEntry entry = null;
        try {
            Preconditions.checkState(!stopped, "Writer cache is stopped");
            entry = pin(tableName, writerSupplier, false);
            action.accept(entry.getWriter());
        } finally {
            try {
                if (entry != null) {
                    unpin(tableName, entry);
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    /** Compatibility bridge for the deprecated public raw-writer API. */
    YtDynamicTableWriter getOrAcquireLegacy(
            String tableName,
            Supplier<YtDynamicTableWriter> writerSupplier) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(writerSupplier);

        lifecycleLock.readLock().lock();
        CacheEntry entry = null;
        try {
            Preconditions.checkState(!stopped, "Writer cache is stopped");
            entry = pin(tableName, writerSupplier, true);
            return entry.getWriter();
        } finally {
            try {
                if (entry != null) {
                    unpin(tableName, entry);
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    /**
     * Returns a snapshot retained for compatibility. Exposed writers stay pinned until cache shutdown.
     */
    Collection<YtDynamicTableWriter> legacyValuesSnapshot() {
        lifecycleLock.readLock().lock();
        try {
            List<YtDynamicTableWriter> writers = new ArrayList<>();
            for (String tableName : List.copyOf(cache.asMap().keySet())) {
                CacheEntry entry = pinIfPresent(tableName, true);
                if (entry == null) {
                    continue;
                }
                try {
                    writers.add(entry.getWriter());
                } finally {
                    unpin(tableName, entry);
                }
            }
            return List.copyOf(writers);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    void forEachWriter(Consumer<YtDynamicTableWriter> action) {
        Preconditions.checkNotNull(action);

        lifecycleLock.writeLock().lock();
        try {
            Preconditions.checkState(!stopped, "Writer cache is stopped");
            for (String tableName : List.copyOf(cache.asMap().keySet())) {
                CacheEntry entry = pinIfPresent(tableName, false);
                if (entry == null) {
                    continue;
                }
                try {
                    action.accept(entry.getWriter());
                } finally {
                    unpin(tableName, entry);
                }
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
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

    private CacheEntry pin(
            String tableName,
            Supplier<YtDynamicTableWriter> writerSupplier,
            boolean legacyPinned) {
        CacheEntry entry = cache.asMap().compute(tableName, (ignored, current) -> {
            CacheEntry result = current;
            if (result == null) {
                YtDynamicTableWriter writer = Preconditions.checkNotNull(writerSupplier.get());
                result = new CacheEntry(writer);
                CacheEntry created = result;
                writer.setCacheIdleListener(() -> onWriterIdle(tableName, created));
            }
            if (legacyPinned) {
                result.legacyPinned = true;
            }
            result.activeUses++;
            return result;
        });
        return Preconditions.checkNotNull(entry);
    }

    @Nullable
    private CacheEntry pinIfPresent(String tableName, boolean legacyPinned) {
        return cache.asMap().computeIfPresent(tableName, (ignored, current) -> {
            if (legacyPinned) {
                current.legacyPinned = true;
            }
            current.activeUses++;
            return current;
        });
    }

    private void unpin(String tableName, CacheEntry expected) {
        cache.asMap().computeIfPresent(tableName, (ignored, current) -> {
            if (current == expected) {
                Preconditions.checkState(current.activeUses > 0, "Writer cache entry is not pinned");
                current.activeUses--;
            }
            return current;
        });
    }

    private void onWriterIdle(String tableName, CacheEntry expected) {
        if (stopped || expected.retired) {
            return;
        }

        try {
            cache.asMap().replace(tableName, expected, expected);
        } catch (RuntimeException e) {
            if (!stopped) {
                log.error("Unable to update writer cache state for key: {}", tableName, e);
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
        private volatile boolean legacyPinned;
        private volatile boolean retired;

        private CacheEntry(YtDynamicTableWriter writer) {
            this.writer = writer;
        }

        private YtDynamicTableWriter getWriter() {
            return writer;
        }

        private boolean isPinned() {
            return activeUses > 0 || legacyPinned || writer.isBusy();
        }

        private void retire() {
            retired = true;
            writer.clearCacheIdleListener();
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

}
