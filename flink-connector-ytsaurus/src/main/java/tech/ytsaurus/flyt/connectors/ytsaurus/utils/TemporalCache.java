package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

/**
 * Compatibility facade for a time-to-idle cache backed by Caffeine.
 *
 * <p>Expiration is intentionally driven by an explicit maintenance pass rather than Caffeine's
 * native expiration policy. Callers may veto removal based on mutable value state, and removal
 * must complete successfully before the mapping becomes unavailable.</p>
 */
@Slf4j
public class TemporalCache<K, V> {
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final Cache<K, CacheEntry> cache;
    private final Duration ttl;
    private final long ttlNanos;
    private final long cleanupPeriod;
    @Nullable
    private final Consumer<Map.Entry<K, V>> removalListener;
    @Nullable
    private final Function<V, Boolean> expirationCondition;
    private final LongSupplier ticker;
    private final AtomicReference<Thread> cleanupThread = new AtomicReference<>();
    private final ReentrantLock cleanupLock = new ReentrantLock();

    @Nullable
    private ScheduledExecutorService cleanupExecutor;
    @Nullable
    private ScheduledFuture<?> cleanupFuture;
    private volatile boolean cleanupCancelled;

    public TemporalCache(Duration ttl,
                         long cleanupPeriodMs,
                         Consumer<Map.Entry<K, V>> removalListener,
                         @Nullable Function<V, Boolean> expirationCondition) {
        this(ttl, cleanupPeriodMs, removalListener, expirationCondition, System::nanoTime);
    }

    @VisibleForTesting
    TemporalCache(Duration ttl,
                  long cleanupPeriodMs,
                  @Nullable Consumer<Map.Entry<K, V>> removalListener,
                  @Nullable Function<V, Boolean> expirationCondition,
                  LongSupplier ticker) {
        Preconditions.checkNotNull(ttl);
        Preconditions.checkArgument(!ttl.isNegative(), "Cache TTL must not be negative");
        Preconditions.checkArgument(cleanupPeriodMs > 0);
        this.ttl = ttl;
        this.ttlNanos = ttl.toNanos();
        this.cleanupPeriod = cleanupPeriodMs;
        this.removalListener = removalListener;
        this.expirationCondition = expirationCondition;
        this.ticker = Objects.requireNonNull(ticker);
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
        cache.asMap().put(key, new CacheEntry(value, ticker.getAsLong()));
    }

    public boolean containsKey(K key) {
        return cache.asMap().containsKey(key);
    }

    public int getSize() {
        return cache.asMap().size();
    }

    @Nullable
    public V get(K key) {
        AtomicReference<V> result = new AtomicReference<>();
        cache.asMap().computeIfPresent(key, (ignored, entry) -> {
            entry.throwIfRemovalFailed();
            entry.prolong(ticker.getAsLong());
            result.set(entry.value);
            return entry;
        });
        return result.get();
    }

    /**
     * Returns the cached value or atomically creates it for the key.
     */
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        AtomicReference<V> result = new AtomicReference<>();
        cache.asMap().compute(key, (currentKey, existingEntry) -> {
            CacheEntry entry = existingEntry;
            if (entry == null) {
                entry = new CacheEntry(mappingFunction.apply(currentKey), ticker.getAsLong());
            }
            entry.throwIfRemovalFailed();
            entry.prolong(ticker.getAsLong());
            result.set(entry.value);
            return entry;
        });
        return result.get();
    }

    /**
     * Atomically obtains a value and applies an operation while removal for the key is excluded.
     */
    public void getAndAccept(K key,
                             Function<? super K, ? extends V> mappingFunction,
                             Consumer<? super V> operation) {
        AtomicReference<RuntimeException> runtimeFailure = new AtomicReference<>();
        AtomicReference<Error> error = new AtomicReference<>();
        cache.asMap().compute(key, (currentKey, existingEntry) -> {
            CacheEntry entry = existingEntry;
            if (entry == null) {
                entry = new CacheEntry(mappingFunction.apply(currentKey), ticker.getAsLong());
            }
            entry.throwIfRemovalFailed();
            entry.prolong(ticker.getAsLong());
            try {
                operation.accept(entry.value);
            } catch (RuntimeException e) {
                runtimeFailure.set(e);
            } catch (Error e) {
                error.set(e);
            }
            return entry;
        });
        if (runtimeFailure.get() != null) {
            throw runtimeFailure.get();
        }
        if (error.get() != null) {
            throw error.get();
        }
    }

    /**
     * Applies an operation to an existing value while removal for the key is excluded.
     *
     * @return whether the mapping was present
     */
    public boolean acceptIfPresent(K key, Consumer<? super V> operation) {
        AtomicBoolean present = new AtomicBoolean();
        cache.asMap().computeIfPresent(key, (ignored, entry) -> {
            entry.throwIfRemovalFailed();
            present.set(true);
            operation.accept(entry.value);
            return entry;
        });
        return present.get();
    }

    /**
     * Atomically applies a final operation and removes the value only when it succeeds.
     *
     * @return whether the mapping was present
     */
    public boolean remove(K key, Consumer<? super V> operation) {
        AtomicBoolean present = new AtomicBoolean();
        AtomicReference<RuntimeException> runtimeFailure = new AtomicReference<>();
        AtomicReference<Error> error = new AtomicReference<>();
        cache.asMap().computeIfPresent(key, (ignored, entry) -> {
            present.set(true);
            entry.throwIfRemovalFailed();
            try {
                operation.accept(entry.value);
                return null;
            } catch (RuntimeException e) {
                runtimeFailure.set(e);
                return entry;
            } catch (Error e) {
                error.set(e);
                return entry;
            }
        });
        if (runtimeFailure.get() != null) {
            throw runtimeFailure.get();
        }
        if (error.get() != null) {
            throw error.get();
        }
        return present.get();
    }

    public Collection<V> values() {
        return cache.asMap().values().stream()
                .map(entry -> entry.value)
                .collect(Collectors.toUnmodifiableList());
    }

    public Map<K, V> entries() {
        return cache.asMap().entrySet().stream().collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> entry.getValue().value));
    }

    public void clear() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    public synchronized void schedule() {
        Preconditions.checkArgument(cleanupFuture == null, "Cache cleanup is already scheduled");
        cleanupCancelled = false;
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "temporal-cache-cleanup");
            thread.setDaemon(true);
            cleanupThread.set(thread);
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
        cleanupLock.lock();
        try {
            if (cleanupCancelled) {
                return;
            }
            long frozenCurrent = ticker.getAsLong();
            for (K key : List.copyOf(cache.asMap().keySet())) {
                if (cleanupCancelled) {
                    return;
                }
                cleanupEntry(key, frozenCurrent);
            }
        } catch (Exception e) {
            log.error("Unable to finish cleanup operation", e);
        } finally {
            cleanupLock.unlock();
        }
    }

    private void cleanupEntry(K key, long frozenCurrent) {
        cache.asMap().computeIfPresent(key, (currentKey, entry) -> {
            if (entry.hasRemovalFailure()) {
                return entry;
            }
            try {
                if (!entry.isExpired(frozenCurrent)) {
                    return entry;
                }
            } catch (Exception e) {
                log.error("Unable to check expiration for key: {}", currentKey, e);
                return entry;
            }
            try {
                if (expirationCondition != null && !expirationCondition.apply(entry.value)) {
                    return entry;
                }
            } catch (Exception e) {
                log.error("Unable to check expiration condition for key: {}", currentKey, e);
                return entry;
            }
            try {
                if (removalListener != null) {
                    removalListener.accept(Map.entry(currentKey, entry.value));
                }
                return null;
            } catch (Exception e) {
                entry.recordRemovalFailure(currentKey, e);
                return entry;
            }
        });
    }

    public void cancel() {
        ScheduledExecutorService executor;
        ScheduledFuture<?> future;
        synchronized (this) {
            Preconditions.checkNotNull(cleanupFuture, "TemporalCache has not been scheduled");
            executor = Preconditions.checkNotNull(cleanupExecutor);
            future = cleanupFuture;
            cleanupCancelled = true;
        }

        future.cancel(false);
        executor.shutdown();
        if (Thread.currentThread() == cleanupThread.get()) {
            return;
        }

        try {
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Temporal cache cleanup executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while stopping temporal cache cleanup", e);
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

    private final class CacheEntry {
        private final V value;
        private volatile long accessedAtNanos;
        @Nullable
        private volatile RuntimeException removalFailure;
        private int cleanupFailureCount;

        private CacheEntry(V value, long accessedAtNanos) {
            this.value = Objects.requireNonNull(value);
            this.accessedAtNanos = accessedAtNanos;
        }

        private boolean isExpired(long relativeToNanos) {
            return relativeToNanos - accessedAtNanos >= ttlNanos;
        }

        private void prolong(long currentNanos) {
            accessedAtNanos = currentNanos;
        }

        private void recordRemovalFailure(K key, Exception failure) {
            cleanupFailureCount++;
            removalFailure = new RuntimeException(
                    String.format(
                            "Unable to remove cache entry for key '%s' after %d attempt(s)",
                            key,
                            cleanupFailureCount),
                    failure);
            log.error(
                    "Unable to remove cache entry for key: {} (total cleanup failures for the entry: {})",
                    key,
                    cleanupFailureCount,
                    failure);
        }

        private void throwIfRemovalFailed() {
            if (removalFailure != null) {
                throw removalFailure;
            }
        }

        private boolean hasRemovalFailure() {
            return removalFailure != null;
        }
    }

    public static class TemporalCacheBuilder<K, V> {
        private Duration ttl;
        private Long cleanupPeriodMs;
        @Nullable
        private Consumer<Map.Entry<K, V>> removalListener;
        @Nullable
        private Function<V, Boolean> expirationCondition;
        private LongSupplier ticker = System::nanoTime;

        public TemporalCacheBuilder() {
        }

        public TemporalCacheBuilder<K, V> ttl(Duration ttl, @Nullable Function<V, Boolean> expirationCondition) {
            this.ttl = ttl;
            this.expirationCondition = expirationCondition;
            return this;
        }

        public TemporalCacheBuilder<K, V> ttl(long ttl, TimeUnit unit) {
            this.ttl = Duration.of(ttl, unit.toChronoUnit());
            return this;
        }

        public TemporalCacheBuilder<K, V> ttl(long ttl,
                                              TimeUnit unit,
                                              @Nullable Function<V, Boolean> expirationCondition) {
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

        public TemporalCacheBuilder<K, V> removalListener(
                @Nullable Consumer<Map.Entry<K, V>> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        @VisibleForTesting
        public TemporalCacheBuilder<K, V> ticker(LongSupplier ticker) {
            this.ticker = Objects.requireNonNull(ticker);
            return this;
        }

        public TemporalCache<K, V> build() {
            Preconditions.checkNotNull(ttl);
            Preconditions.checkNotNull(cleanupPeriodMs);
            return new TemporalCache<>(ttl, cleanupPeriodMs, removalListener, expirationCondition, ticker);
        }
    }
}
