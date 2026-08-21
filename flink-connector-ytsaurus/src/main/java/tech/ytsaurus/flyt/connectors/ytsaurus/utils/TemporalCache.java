package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;
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
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

@Slf4j
public class TemporalCache<K, V> {
    private final Cache<K, CacheEntry> cache;
    private final Duration ttl;
    private final long ttlNanos;
    private final long cleanupPeriod;
    private final Consumer<Map.Entry<K, V>> removalListener;
    private final Ticker ticker;
    @Nullable
    private final Function<V, Boolean> expirationCondition;

    private ScheduledExecutorService cleanupExecutor;
    private ScheduledFuture<?> cleanupFuture;

    public TemporalCache(Duration ttl,
                         long cleanupPeriodMs,
                         Consumer<Map.Entry<K, V>> removalListener,
                         @Nullable Function<V, Boolean> expirationCondition) {
        this(ttl, cleanupPeriodMs, removalListener, expirationCondition, Ticker.systemTicker());
    }

    private TemporalCache(Duration ttl,
                          long cleanupPeriodMs,
                          Consumer<Map.Entry<K, V>> removalListener,
                          @Nullable Function<V, Boolean> expirationCondition,
                          Ticker ticker) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(!ttl.isNegative(), "TTL must not be negative");
        Preconditions.checkArgument(cleanupPeriodMs > 0);
        this.ttl = ttl;
        this.ttlNanos = ttl.toNanos();
        this.cleanupPeriod = cleanupPeriodMs;
        this.removalListener = removalListener;
        this.expirationCondition = expirationCondition;
        this.ticker = ticker;
        this.cache = Caffeine.newBuilder().build();
    }

    public static <K, V> TemporalCacheBuilder<K, V> builder() {
        return new TemporalCacheBuilder<>();
    }

    public TemporalCacheBuilder<K, V> toBuilder() {
        return new TemporalCacheBuilder<K, V>()
                .removalListener(removalListener)
                .ttl(ttl, expirationCondition)
                .cleanupPeriod(cleanupPeriod)
                .ticker(ticker);
    }

    public void put(K key, V value) {
        cache.put(key, new CacheEntry(value, ticker.read(), expirationCondition, 0));
    }

    public boolean containsKey(K key) {
        return cache.asMap().containsKey(key);
    }

    public int getSize() {
        return Math.toIntExact(cache.estimatedSize());
    }

    public V get(K key) {
        CacheEntry entry = cache.getIfPresent(key);
        if (entry == null) {
            return null;
        }

        entry.prolong(ticker.read());
        return entry.getValue();
    }

    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        CacheEntry entry = cache.get(key, missingKey -> new CacheEntry(
                mappingFunction.apply(missingKey), ticker.read(), expirationCondition, 0));
        entry.prolong(ticker.read());
        return entry.getValue();
    }

    public Collection<V> values() {
        return cache.asMap().values().stream().map(CacheEntry::getValue).collect(Collectors.toUnmodifiableList());
    }

    public void schedule() {
        Preconditions.checkArgument(cleanupFuture == null, "Cache cleanup is already scheduled");
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "temporal-cache-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        cleanupFuture = cleanupExecutor.scheduleWithFixedDelay(
                this::cleanup,
                cleanupPeriod,
                cleanupPeriod,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void cleanup() {
        try {
            long now = ticker.read();
            cache.asMap().forEach((key, observedEntry) -> cache.asMap().computeIfPresent(key, (ignored, entry) -> {
                if (entry != observedEntry) {
                    return entry;
                }
                return removeIfExpired(key, entry, now);
            }));
        } catch (Exception e) {
            log.error("Unable to finish cleanup operation", e);
        }
    }

    private CacheEntry removeIfExpired(K key, CacheEntry entry, long now) {
        try {
            if (!entry.isExpired(now)) {
                return entry;
            }
            try {
                if (removalListener != null) {
                    removalListener.accept(Map.entry(key, entry.getValue()));
                }
                return null;
            } catch (Exception e) {
                entry.increaseFailureCount();
                log.error("Unable to accept removal listener for key: {} " +
                                "(total cleanup failures for the entry: {})",
                        key, entry.getCleanupFailureCount(), e);
            }
        } catch (Exception e) {
            log.error("Unable to check expiration for key: {}", key, e);
        }
        return entry;
    }

    public void cancel() {
        Preconditions.checkNotNull(cleanupFuture, "TemporalCache has not been scheduled");
        cleanupFuture.cancel(true);
        cleanupExecutor.shutdownNow();
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
        V value;
        private volatile long accessedAt;
        Function<V, Boolean> expirationCondition;

        @Getter
        private int cleanupFailureCount;

        private boolean isExpired(long relativeToNanos) {
            return relativeToNanos - accessedAt >= ttlNanos
                    && Optional.ofNullable(expirationCondition)
                    .map(condition -> condition.apply(value))
                    .orElse(true);
        }

        private void prolong(long accessedAtNanos) {
            accessedAt = accessedAtNanos;
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
        private Ticker ticker = Ticker.systemTicker();

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

        @VisibleForTesting
        TemporalCacheBuilder<K, V> ticker(Ticker ticker) {
            this.ticker = ticker;
            return this;
        }

        public TemporalCache<K, V> build() {
            Preconditions.checkNotNull(ttl);
            Preconditions.checkNotNull(cleanupPeriodMs);
            return new TemporalCache<>(ttl, cleanupPeriodMs, removalListener, expirationCondition, ticker);
        }
    }
}
