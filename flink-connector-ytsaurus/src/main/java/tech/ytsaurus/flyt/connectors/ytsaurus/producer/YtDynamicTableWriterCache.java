package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
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
 * <p>An entry is pinned while it is being used or its writer has uncommitted data. Successful background commits
 * refresh the entry so that Caffeine can schedule eviction once the writer becomes idle. The transition to idle
 * starts a fresh TTL. An eviction listener closes an expired writer synchronously before another writer can be
 * installed for the same table.
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
        try {
            Preconditions.checkState(!stopped, "Writer cache is stopped");
            CacheEntry entry = pin(tableName, writerSupplier);
            try {
                action.accept(entry.getWriter());
            } finally {
                unpin(tableName, entry);
            }
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
                CacheEntry entry = pinIfPresent(tableName);
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

    private CacheEntry pin(String tableName, Supplier<YtDynamicTableWriter> writerSupplier) {
        return cache.asMap().compute(tableName, (ignored, current) -> {
            if (current == null) {
                YtDynamicTableWriter writer = Preconditions.checkNotNull(writerSupplier.get());
                current = new CacheEntry(writer);
                CacheEntry created = current;
                writer.setCacheStateListener(() -> refreshExpiration(tableName, created));
            }
            current.activeUses++;
            return current;
        });
    }

    @Nullable
    private CacheEntry pinIfPresent(String tableName) {
        return cache.asMap().computeIfPresent(tableName, (ignored, current) -> {
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

    private void refreshExpiration(String tableName, CacheEntry expected) {
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

}
