package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

@Slf4j
public class TemporalCache<K, V> {
    private final Map<K, CacheEntry> cacheEntryMap;
    private final Duration ttl;
    private final long cleanupPeriod;
    private Consumer<Map.Entry<K, V>> removalListener;
    private ScheduledFuture<?> cleanupFuture;
    @Nullable
    private Function<V, Boolean> expirationCondition;

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
        this.cacheEntryMap = new ConcurrentHashMap<>();
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
        cacheEntryMap.put(key, new CacheEntry(value, LocalDateTime.now(), expirationCondition, 0));
    }

    public boolean containsKey(K key) {
        return cacheEntryMap.containsKey(key);
    }

    public int getSize() {
        return cacheEntryMap.size();
    }

    public V get(K key) {
        CacheEntry entry = cacheEntryMap.get(key);
        if (entry == null) {
            return null;
        }

        entry.prolong();
        return entry.getValue();
    }

    public Collection<V> values() {
        return cacheEntryMap.values().stream().map(CacheEntry::getValue).collect(Collectors.toUnmodifiableList());
    }

    public void schedule() {
        Preconditions.checkArgument(cleanupFuture == null, "Cache cleanup is already scheduled");
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        cleanupFuture = service.scheduleWithFixedDelay(
                this::cleanup,
                cleanupPeriod,
                cleanupPeriod,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void cleanup() {
        try {
            LocalDateTime frozenCurrent = LocalDateTime.now();
            Iterator<Map.Entry<K, CacheEntry>> entryIterator = cacheEntryMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<K, CacheEntry> current = entryIterator.next();
                CacheEntry entry = current.getValue();
                try {
                    if (entry.isExpired(frozenCurrent)) {
                        try {
                            if (removalListener != null) {
                                removalListener.accept(Map.entry(current.getKey(), entry.getValue()));
                            }
                            entryIterator.remove();
                        } catch (Exception e) {
                            entry.increaseFailureCount();
                            log.error("Unable to accept removal listener for key: {} " +
                                            "(total cleanup failures for the entry: {})",
                                    current.getKey(), entry.getCleanupFailureCount(), e);
                        }
                    }
                } catch (Exception e) {
                    log.error("Unable to check expiration for key: {}", current.getKey(), e);
                }
            }
        } catch (Exception e) {
            log.error("Unable to finish cleanup operation", e);
        }
    }

    public void cancel() {
        Preconditions.checkNotNull(cleanupFuture, "TemporalCache has not been scheduled");
        cleanupFuture.cancel(true);
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
        LocalDateTime accessedAt;
        Function<V, Boolean> expirationCondition;

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
