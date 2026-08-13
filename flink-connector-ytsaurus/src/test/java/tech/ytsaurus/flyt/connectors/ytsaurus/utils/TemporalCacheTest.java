package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class TemporalCacheTest {
    @Test
    public void testRemoveAfterExpire() {
        MutableTicker ticker = new MutableTicker();
        Consumer<Map.Entry<String, Integer>> listener = Mockito.mock();
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(10, TimeUnit.MINUTES)
                .cleanupPeriod(1, TimeUnit.MINUTES)
                .removalListener(listener)
                .ticker(ticker)
                .build();

        cache.put("first", 1);
        cache.put("second", 2);
        ticker.advance(Duration.ofMinutes(10));
        cache.cleanup();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(listener).accept(Map.entry("first", 1));
        Mockito.verify(listener).accept(Map.entry("second", 2));
    }

    @Test
    public void testAccessProlongsLifetime() {
        MutableTicker ticker = new MutableTicker();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .ticker(ticker)
                .build();

        cache.put("prolonged", "value");
        cache.put("not-prolonged", "value");
        ticker.advance(Duration.ofHours(12));
        Assertions.assertEquals("value", cache.get("prolonged"));

        ticker.advance(Duration.ofHours(13));
        cache.cleanup();

        Assertions.assertEquals(1, cache.getSize());
        Assertions.assertTrue(cache.containsKey("prolonged"));
        Assertions.assertFalse(cache.containsKey("not-prolonged"));
    }

    @Test
    public void testAccessBeforeCleanupRescuesExpiredEntry() {
        MutableTicker ticker = new MutableTicker();
        var cache = TemporalCache.<String, String>builder()
                .ttl(10, TimeUnit.MINUTES)
                .cleanupPeriod(1, TimeUnit.MINUTES)
                .ticker(ticker)
                .build();

        cache.put("key", "value");
        ticker.advance(Duration.ofMinutes(11));
        Assertions.assertEquals("value", cache.get("key"));
        cache.cleanup();

        Assertions.assertEquals(1, cache.getSize());
    }

    @Test
    public void testExpirationConditionIsRechecked() {
        MutableTicker ticker = new MutableTicker();
        AtomicBoolean removable = new AtomicBoolean();
        Consumer<Map.Entry<String, String>> listener = Mockito.mock();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.NANOSECONDS, ignored -> removable.get())
                .cleanupPeriod(1, TimeUnit.DAYS)
                .removalListener(listener)
                .ticker(ticker)
                .build();

        cache.put("key", "value");
        ticker.advance(Duration.ofNanos(1));
        cache.cleanup();
        Assertions.assertEquals(1, cache.getSize());
        Mockito.verifyNoInteractions(listener);

        removable.set(true);
        cache.cleanup();
        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(listener).accept(Map.entry("key", "value"));
    }

    @Test
    public void testRemovalFailureQuarantinesEntryWithoutUnsafeRetry() {
        MutableTicker ticker = new MutableTicker();
        AtomicInteger attempts = new AtomicInteger();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.NANOSECONDS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .removalListener(ignored -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("close failed");
                    }
                })
                .ticker(ticker)
                .build();

        cache.put("key", "value");
        ticker.advance(Duration.ofNanos(1));
        cache.cleanup();

        Assertions.assertEquals(1, cache.getSize());
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, () -> cache.get("key"));
        Assertions.assertTrue(failure.getMessage().contains("Unable to remove cache entry"));
        Assertions.assertEquals("close failed", failure.getCause().getMessage());

        cache.cleanup();
        Assertions.assertEquals(1, cache.getSize());
        Assertions.assertEquals(1, attempts.get());
        Assertions.assertThrows(RuntimeException.class, () -> cache.remove("key", ignored -> {
        }));
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testConcurrentGetCreatesOneValue() {
        var cache = TemporalCache.<String, Object>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .build();
        AtomicInteger creations = new AtomicInteger();
        Object expected = new Object();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Object>> results = new ArrayList<>();
        try {
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return cache.get("key", ignored -> {
                        creations.incrementAndGet();
                        return expected;
                    });
                }));
            }
            start.countDown();
            for (Future<Object> result : results) {
                Assertions.assertSame(expected, result.get(5, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }

        Assertions.assertEquals(1, creations.get());
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testCleanupWaitsForActiveOperation() {
        MutableTicker ticker = new MutableTicker();
        CountDownLatch operationStarted = new CountDownLatch(1);
        CountDownLatch allowOperationToFinish = new CountDownLatch(1);
        AtomicBoolean removed = new AtomicBoolean();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.NANOSECONDS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .removalListener(ignored -> removed.set(true))
                .ticker(ticker)
                .build();
        cache.put("key", "value");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> operation = executor.submit(() -> cache.getAndAccept(
                    "key",
                    ignored -> {
                        throw new AssertionError("Value must already be cached");
                    },
                    ignored -> {
                        operationStarted.countDown();
                        try {
                            allowOperationToFinish.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }));
            Assertions.assertTrue(operationStarted.await(5, TimeUnit.SECONDS));
            ticker.advance(Duration.ofNanos(1));

            Future<?> cleanup = executor.submit(cache::cleanup);
            Assertions.assertThrows(TimeoutException.class, () -> cleanup.get(100, TimeUnit.MILLISECONDS));
            Assertions.assertFalse(removed.get());

            allowOperationToFinish.countDown();
            operation.get(5, TimeUnit.SECONDS);
            cleanup.get(5, TimeUnit.SECONDS);
        } finally {
            allowOperationToFinish.countDown();
            executor.shutdownNow();
        }

        Assertions.assertTrue(removed.get());
        Assertions.assertEquals(0, cache.getSize());
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testCancelStopsScheduledCleanup() {
        CountDownLatch removed = new CountDownLatch(1);
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(Duration.ZERO, ignored -> true)
                .cleanupPeriod(1, TimeUnit.MILLISECONDS)
                .removalListener(ignored -> removed.countDown())
                .build();

        cache.put("first", 1);
        cache.schedule();
        Assertions.assertTrue(removed.await(5, TimeUnit.SECONDS));
        cache.cancel();

        cache.put("second", 2);
        TimeUnit.MILLISECONDS.sleep(50);
        Assertions.assertEquals(1, cache.getSize());
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testCancelWaitsForCleanupWithoutInterruptingListener() {
        CountDownLatch listenerStarted = new CountDownLatch(1);
        CountDownLatch allowListenerToFinish = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(Duration.ZERO, ignored -> true)
                .cleanupPeriod(1, TimeUnit.MILLISECONDS)
                .removalListener(ignored -> {
                    listenerStarted.countDown();
                    try {
                        allowListenerToFinish.await();
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                })
                .build();
        cache.put("key", 1);
        cache.schedule();
        Assertions.assertTrue(listenerStarted.await(5, TimeUnit.SECONDS));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> cancel = executor.submit(cache::cancel);
            Assertions.assertThrows(TimeoutException.class, () -> cancel.get(100, TimeUnit.MILLISECONDS));
            Assertions.assertFalse(interrupted.get());

            allowListenerToFinish.countDown();
            cancel.get(5, TimeUnit.SECONDS);
        } finally {
            allowListenerToFinish.countDown();
            executor.shutdownNow();
        }

        Assertions.assertFalse(interrupted.get());
        Assertions.assertEquals(0, cache.getSize());
    }

    private static final class MutableTicker implements LongSupplier {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long getAsLong() {
            return nanos.get();
        }

        private void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }
}
