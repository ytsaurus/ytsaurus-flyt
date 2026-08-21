package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.github.benmanes.caffeine.cache.Ticker;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class TemporalCacheTest {
    @Test
    public void testRemoveAfterExpire() {
        TestTicker ticker = new TestTicker();
        Consumer<Map.Entry<String, Integer>> consumer = Mockito.mock();
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(10, TimeUnit.MINUTES)
                .cleanupPeriod(1, TimeUnit.MINUTES)
                .removalListener(consumer)
                .ticker(ticker)
                .build();
        cache.put("first", 1);
        cache.put("second", 2);

        ticker.advance(11, TimeUnit.MINUTES);
        cache.cleanup();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(consumer).accept(Mockito.eq(Map.entry("first", 1)));
        Mockito.verify(consumer).accept(Mockito.eq(Map.entry("second", 2)));
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testScheduleCleanup() {
        TestTicker ticker = new TestTicker();
        CountDownLatch removed = new CountDownLatch(2);
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(100, TimeUnit.MILLISECONDS)
                .cleanupPeriod(10, TimeUnit.MILLISECONDS)
                .removalListener(ignored -> removed.countDown())
                .ticker(ticker)
                .build();
        cache.put("first", 1);
        cache.put("second", 2);
        ticker.advance(100, TimeUnit.MILLISECONDS);

        cache.schedule();
        Assertions.assertTrue(removed.await(1, TimeUnit.SECONDS));
        cache.cancel();

        Assertions.assertEquals(0, cache.getSize());
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testNoCleanupAfterCancel() {
        TestTicker ticker = new TestTicker();
        CountDownLatch firstCleanup = new CountDownLatch(1);
        var cache = Mockito.spy(TemporalCache.<String, Integer>builder()
                .ttl(1, TimeUnit.NANOSECONDS)
                .cleanupPeriod(1, TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .build());
        Mockito.doAnswer(invocation -> {
            invocation.callRealMethod();
            cache.cancel();
            firstCleanup.countDown();
            return null;
        }).when(cache).cleanup();

        cache.put("sample", 1);
        ticker.advance(1, TimeUnit.NANOSECONDS);
        cache.schedule();
        Assertions.assertTrue(firstCleanup.await(1, TimeUnit.SECONDS));
        Assertions.assertEquals(0, cache.getSize());

        cache.put("sample1", 1);
        cache.put("sample2", 2);
        ticker.advance(1, TimeUnit.NANOSECONDS);
        Thread.sleep(20);

        Assertions.assertEquals(2, cache.getSize());
        Mockito.verify(cache, Mockito.times(1)).cleanup();
    }

    @Test
    public void testCachePut() {
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .build();

        cache.put("my_key", "my_value");

        Assertions.assertTrue(cache.containsKey("my_key"));
    }

    @Test
    public void testCacheProlong() {
        TestTicker ticker = new TestTicker();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .ticker(ticker)
                .build();
        cache.put("prolonged", "sample");
        cache.put("not_prolonged", "sample");

        ticker.advance(12, TimeUnit.HOURS);
        Assertions.assertEquals("sample", cache.get("prolonged"));
        ticker.advance(13, TimeUnit.HOURS);
        cache.cleanup();

        Assertions.assertEquals(1, cache.getSize());
        Assertions.assertEquals("sample", cache.get("prolonged"));
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testAtomicValueCreation() {
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .build();
        AtomicInteger creations = new AtomicInteger();
        CountDownLatch creationStarted = new CountDownLatch(1);
        CountDownLatch allowCreation = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);

        try {
            var first = executor.submit(() -> cache.get("key", key -> {
                creations.incrementAndGet();
                creationStarted.countDown();
                try {
                    allowCreation.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return "value";
            }));
            Assertions.assertTrue(creationStarted.await(1, TimeUnit.SECONDS));
            var second = executor.submit(() -> cache.get("key", key -> {
                creations.incrementAndGet();
                return "other";
            }));

            allowCreation.countDown();

            Assertions.assertEquals("value", first.get(1, TimeUnit.SECONDS));
            Assertions.assertEquals("value", second.get(1, TimeUnit.SECONDS));
            Assertions.assertEquals(1, creations.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailedRemovalIsRetried() {
        TestTicker ticker = new TestTicker();
        AtomicInteger attempts = new AtomicInteger();
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.NANOSECONDS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .removalListener(ignored -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("first attempt fails");
                    }
                })
                .ticker(ticker)
                .build();
        cache.put("key", "value");
        ticker.advance(1, TimeUnit.NANOSECONDS);

        cache.cleanup();
        Assertions.assertTrue(cache.containsKey("key"));
        cache.cleanup();

        Assertions.assertFalse(cache.containsKey("key"));
        Assertions.assertEquals(2, attempts.get());
    }

    private static class TestTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        private void advance(long duration, TimeUnit unit) {
            nanos.addAndGet(unit.toNanos(duration));
        }
    }
}
