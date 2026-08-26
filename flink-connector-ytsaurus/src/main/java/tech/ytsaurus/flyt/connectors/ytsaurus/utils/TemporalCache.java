package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

/**
 * @deprecated Use Caffeine directly for general-purpose caches. The writer pool uses a domain-specific
 * Caffeine-backed cache that retains writers with uncommitted data.
 */
@Deprecated
@Slf4j
public class TemporalCache<K, V> {
    private final Cache<K, CacheEntry> cache;
    private final Duration ttl;
    private final long cleanupPeriod;
    private final Consumer<Map.Entry<K, V>> removalListener;
    private final Object cleanupLock = new Object();
    @Nullable
    private final Function<V, Boolean> expirationCondition;
    @Nullable
    private ScheduledExecutorService cleanupExecutor;
    @Nullable
    private ScheduledFuture<?> cleanupFuture;
    private volatile boolean cleanupEnabled;

    public TemporalCache(Duration ttl,
                         long cleanupPeriodMs,
                         Consumer<Map.Entry<K, V>> removalListener,
                         @Nullable Function<V, Boolean> expirationCondition) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(cleanupPeriodMs > 0);
        this.ttl = ttl;
        this.cleanupPeriod = cleanupPeriodMs;
        this.removalListener = removalListener;
        this.expirationCondition = expirationCondition;
        this.cache = Caffeine.newBuilder().build();
    }

    public static <K, V> TemporalCacheBuilder<K, V> builder() {
        return new TemporalCacheBuilder<>();
    }

    public TemporalCacheBuilder<K, V> toBuilder() {
        return new TemporalCacheBuilder<K, V>()
                .removalListener(removalListener)
                .ttl(ttl, expirationCondition)
                .cleanupPeriod(cleanupPeriod);
    }

    public void put(K key, V value) {
        cache.put(key, new CacheEntry(value, LocalDateTime.now(), expirationCondition, 0));
    }

    public boolean containsKey(K key) {
        return cache.asMap().containsKey(key);
    }

    public int getSize() {
        return cache.asMap().size();
    }

    public V get(K key) {
        CacheEntry entry = cache.asMap().computeIfPresent(key, (ignored, current) -> {
            current.prolong();
            return current;
        });
        return entry == null ? null : entry.getValue();
    }

    public Collection<V> values() {
        return cache.asMap().values().stream()
                .map(CacheEntry::getValue)
                .collect(Collectors.toUnmodifiableList());
    }

    public synchronized void schedule() {
        Preconditions.checkState(cleanupFuture == null, "Cache cleanup is already scheduled");
        cleanupEnabled = true;
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        cleanupFuture = cleanupExecutor.scheduleWithFixedDelay(
                this::cleanup,
                cleanupPeriod,
                cleanupPeriod,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void cleanup() {
        synchronized (cleanupLock) {
            if (cleanupFuture == null || cleanupEnabled) {
                cleanupEntries();
            }
        }
    }

    private void cleanupEntries() {
        LocalDateTime now = LocalDateTime.now();
        try {
            for (K key : cache.asMap().keySet()) {
                cache.asMap().computeIfPresent(key, (ignored, entry) -> cleanupEntry(key, entry, now));
            }
        } catch (Exception e) {
            log.error("Unable to finish cleanup operation", e);
        }
    }

    private CacheEntry cleanupEntry(K key, CacheEntry entry, LocalDateTime now) {
        try {
            if (!entry.isExpired(now)) {
                return entry;
            }
        } catch (Exception e) {
            log.error("Unable to check expiration for key: {}", key, e);
            return entry;
        }

        try {
            if (removalListener != null) {
                removalListener.accept(Map.entry(key, entry.getValue()));
            }
            return null;
        } catch (Exception e) {
            entry.increaseFailureCount();
            log.error("Unable to accept removal listener for key: {} "
                            + "(total cleanup failures for the entry: {})",
                    key, entry.getCleanupFailureCount(), e);
            return entry;
        }
    }

    public void cancel() {
        synchronized (this) {
            ScheduledFuture<?> future = Preconditions.checkNotNull(
                    cleanupFuture, "TemporalCache has not been scheduled");
            cleanupEnabled = false;
            future.cancel(false);
            ScheduledExecutorService executor = Preconditions.checkNotNull(
                    cleanupExecutor, "TemporalCache cleanup executor is not initialized");
            executor.shutdown();
        }

        synchronized (cleanupLock) {
            // Wait for a running listener before callers operate on the remaining values.
        }
    }

    @Nullable
    public Function<V, Boolean> getExpirationCondition() {
        return expirationCondition;
    }

    public long getCleanupPeriod() {
        return cleanupPeriod;
    }

    public Duration getTtl() {
        return ttl;
    }

    @Data
    @AllArgsConstructor
    private class CacheEntry {
        private V value;
        private LocalDateTime accessedAt;
        private Function<V, Boolean> expirationCondition;

        @Getter
        private int cleanupFailureCount;

        private boolean isExpired(LocalDateTime relativeTo) {
            return Duration.between(accessedAt, relativeTo).compareTo(ttl) >= 0
                    && Optional.ofNullable(expirationCondition)
                    .map(condition -> condition.apply(value))
                    .orElse(true);
        }

        private void prolong() {
            accessedAt = LocalDateTime.now();
        }

        private void increaseFailureCount() {
            cleanupFailureCount++;
        }
    }

    public static class TemporalCacheBuilder<K, V> {
        private Duration ttl;
        private Long cleanupPeriodMs;
        private Consumer<Map.Entry<K, V>> removalListener;
        private Function<V, Boolean> expirationCondition;

        public TemporalCacheBuilder() {
        }

        public TemporalCacheBuilder<K, V> ttl(Duration ttl, Function<V, Boolean> expirationCondition) {
            this.ttl = ttl;
            this.expirationCondition = expirationCondition;
            return this;
        }

        public TemporalCacheBuilder<K, V> ttl(long ttl, TimeUnit unit) {
            this.ttl = Duration.of(ttl, unit.toChronoUnit());
            return this;
        }

        public TemporalCacheBuilder<K, V> ttl(long ttl, TimeUnit unit, Function<V, Boolean> expirationCondition) {
            this.ttl = Duration.of(ttl, unit.toChronoUnit());
            this.expirationCondition = expirationCondition;
            return this;
        }

        public TemporalCacheBuilder<K, V> cleanupPeriod(long cleanupPeriodMs) {
            this.cleanupPeriodMs = cleanupPeriodMs;
            return this;
        }

        public TemporalCacheBuilder<K, V> cleanupPeriod(long cleanupPeriod, TimeUnit unit) {
            this.cleanupPeriodMs = unit.toMillis(cleanupPeriod);
            return this;
        }

        public TemporalCacheBuilder<K, V> removalListener(Consumer<Map.Entry<K, V>> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        public TemporalCache<K, V> build() {
            Preconditions.checkNotNull(ttl);
            Preconditions.checkNotNull(cleanupPeriodMs);
            return new TemporalCache<>(ttl, cleanupPeriodMs, removalListener, expirationCondition);
        }
    }
}
