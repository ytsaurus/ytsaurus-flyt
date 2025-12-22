package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TemporalCacheTest {
    @Test
    public void testRemoveAfterExpire() {
        Consumer<Map.Entry<String, Integer>> consumer = Mockito.mock();
        var cache = TemporalCache.<String, Integer>builder()
                .ttl(10, TimeUnit.MINUTES)
                .cleanupPeriod(1, TimeUnit.MINUTES)
                .removalListener(consumer)
                .build();
        cache.put("first", 1);
        cache.put("second", 2);
        withCurrentDateTime(LocalDateTime.now().plus(11, ChronoUnit.MINUTES), () -> {
            cache.cleanup();
            Assertions.assertEquals(0, cache.getSize());
        });
        Mockito.verify(consumer).accept(Mockito.eq(Map.entry("first", 1)));
        Mockito.verify(consumer).accept(Mockito.eq(Map.entry("second", 2)));
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testScheduleCleanup() {
        var cache = Mockito.spy(TemporalCache.<String, Integer>builder()
                .ttl(100, TimeUnit.MILLISECONDS)
                .cleanupPeriod(10, TimeUnit.MILLISECONDS)
                .removalListener((ignored) -> {
                })
                .build());

        var start = LocalDateTime.now();
        var expire = start.plus(100, ChronoUnit.MILLIS);
        withCurrentDateTime(start, () -> {
            cache.put("first", 1);
            cache.put("second", 2);
        });

        int initialSize = cache.getSize();
        AtomicBoolean success = new AtomicBoolean(true);

        CountDownLatch latch = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            if (!success.get()) {
                return null;
            }
            LocalDateTime current = LocalDateTime.now();
            Object result = withCurrentDateTime(current, () -> {
                try {
                    return invocation.callRealMethod();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
            if (current.isAfter(expire)) {
                latch.countDown();
            } else if (cache.getSize() != initialSize) {
                success.set(false);
                latch.countDown();
            }
            return result;
        }).when(cache).cleanup();

        cache.schedule();
        latch.await();
        cache.cancel();

        Assertions.assertTrue(success.get(), "Cache got cleared before expected");
        Assertions.assertEquals(0, cache.getSize(), "Cache didn't get cleared at expected datetime");
    }

    @SneakyThrows
    @Test
    @Timeout(10)
    public void testNoCleanupAfterClose() {
        var cache = Mockito.spy(TemporalCache.<String, Integer>builder()
                .ttl(1, TimeUnit.NANOSECONDS)
                .cleanupPeriod(1, TimeUnit.MILLISECONDS)
                .build());

        var start = LocalDateTime.now().minus(1, ChronoUnit.YEARS);
        withCurrentDateTime(start, () -> cache.put("sample", 1));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            invocation.callRealMethod();
            cache.cancel();
            countDownLatch.countDown();
            return null;
        }).when(cache).cleanup();

        cache.schedule();
        countDownLatch.await();
        Assertions.assertEquals(0, cache.getSize());

        cache.put("sample1", 1);
        cache.put("sample2", 2);

        Assertions.assertEquals(2, cache.getSize());
        Mockito.verify(cache, Mockito.times(1)).cleanup();
    }

    @Test
    public void testCachePut() {
        var cache = TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.DAYS)
                .removalListener((ignored) -> {
                })
                .build();

        cache.put("my_key", "my_value");
        Assertions.assertTrue(cache.containsKey("my_key"));
    }

    @Test
    public void testCacheProlong() {
        var cache = Mockito.spy(TemporalCache.<String, String>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(1, TimeUnit.MILLISECONDS)
                .removalListener((ignored) -> {
                })
                .build());

        cache.put("prolonged", "sample");
        cache.put("not_prolonged", "sample");

        withCurrentDateTime(
                LocalDateTime.now().plus(12, ChronoUnit.HOURS),
                () -> {
                    cache.get("prolonged");
                    Assertions.assertEquals(2, cache.getSize());
                });

        withCurrentDateTime(
                LocalDateTime.now()
                        .plus(1, ChronoUnit.DAYS)
                        .plus(1, ChronoUnit.HOURS),
                () -> {
                    cache.cleanup();
                    Assertions.assertEquals(1, cache.getSize());
                    Assertions.assertEquals("sample", cache.get("prolonged"));
                });
    }

    private <T> T withCurrentDateTime(LocalDateTime newNow, Supplier<T> supplier) {
        try (MockedStatic<LocalDateTime> localDateTimeMock = Mockito.mockStatic(
                LocalDateTime.class,
                Mockito.CALLS_REAL_METHODS)) {
            localDateTimeMock.when(LocalDateTime::now).thenReturn(newNow);
            return supplier.get();
        }
    }

    private void withCurrentDateTime(LocalDateTime newNow, Runnable runnable) {
        try (MockedStatic<LocalDateTime> localDateTimeMock = Mockito.mockStatic(
                LocalDateTime.class,
                Mockito.CALLS_REAL_METHODS)) {
            localDateTimeMock.when(LocalDateTime::now).thenReturn(newNow);
            runnable.run();
        }
    }
}
