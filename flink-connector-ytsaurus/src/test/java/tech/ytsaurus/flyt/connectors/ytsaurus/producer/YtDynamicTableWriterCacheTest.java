package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class YtDynamicTableWriterCacheTest {
    private static final Duration TTL = Duration.ofMinutes(1);

    private final TestTicker ticker = new TestTicker();

    @Test
    public void testRemoveExpiredIdleWriter() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);

        ticker.advance(TTL);
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).clearCacheStateListener();
        Mockito.verify(writer).close();
    }

    @Test
    public void testKeepBusyWriterAndRestartExpirationWhenItBecomesIdle() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);
        Runnable refreshExpiration = captureStateListener(writer);

        Mockito.when(writer.isBusy()).thenReturn(true);
        cache.withWriter("table", () -> {
            throw new AssertionError("Writer supplier must not be called for a cached writer");
        }, ignored -> {
        });
        refreshExpiration.run();
        ticker.advance(TTL.plusNanos(1));
        cache.cleanupExpired();

        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        Mockito.when(writer.isBusy()).thenReturn(false);
        refreshExpiration.run();

        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        ticker.advance(TTL.minusNanos(1));
        cache.cleanupExpired();
        Assertions.assertEquals(1, cache.getSize());
        // Caffeine schedules variable expiration on a timer wheel, so allow the wheel
        // to advance past its smallest bucket instead of asserting at a 1 ns boundary.
        ticker.advance(Duration.ofSeconds(2));
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    public void testRefreshExpirationOnAccess() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);

        ticker.advance(Duration.ofSeconds(30));
        YtDynamicTableWriter cachedWriter = useWriter(cache, "table", () -> {
            throw new AssertionError("Writer supplier must not be called for a cached writer");
        });
        ticker.advance(Duration.ofSeconds(59));
        cache.cleanupExpired();

        Assertions.assertSame(writer, cachedWriter);
        Assertions.assertEquals(1, cache.getSize());

        ticker.advance(Duration.ofSeconds(1));
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    public void testRemoveEntryAfterCloseFailure() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriter replacement = Mockito.mock(YtDynamicTableWriter.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(writer).close();
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);
        ticker.advance(TTL);

        Assertions.assertDoesNotThrow(cache::cleanupExpired);
        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();

        Assertions.assertSame(replacement, useWriter(cache, "table", () -> replacement));
        Assertions.assertEquals(1, cache.getSize());
    }

    @Test
    @Timeout(10)
    public void testConcurrentAcquireCreatesSingleWriter() throws Exception {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        AtomicInteger supplierCalls = new AtomicInteger();
        CountDownLatch supplierStarted = new CountDownLatch(1);
        CountDownLatch secondAcquireStarted = new CountDownLatch(1);
        CountDownLatch releaseSupplier = new CountDownLatch(1);
        Supplier<YtDynamicTableWriter> writerSupplier = () -> {
            supplierCalls.incrementAndGet();
            supplierStarted.countDown();
            try {
                releaseSupplier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return writer;
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<YtDynamicTableWriter> first = executor.submit(
                    () -> useWriter(cache, "table", writerSupplier));
            Assertions.assertTrue(supplierStarted.await(5, TimeUnit.SECONDS));
            Future<YtDynamicTableWriter> second = executor.submit(() -> {
                secondAcquireStarted.countDown();
                return useWriter(cache, "table", writerSupplier);
            });
            Assertions.assertTrue(secondAcquireStarted.await(5, TimeUnit.SECONDS));
            releaseSupplier.countDown();

            Assertions.assertSame(writer, first.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(writer, second.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, supplierCalls.get());
            Assertions.assertEquals(1, cache.getSize());
            Mockito.verify(writer, Mockito.times(1)).setCacheStateListener(Mockito.any());
        } finally {
            releaseSupplier.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testAcquireWaitsForExpiredWriterCleanup() throws Exception {
        YtDynamicTableWriter expiredWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriter replacementWriter = Mockito.mock(YtDynamicTableWriter.class);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            closeStarted.countDown();
            releaseClose.await();
            return null;
        }).when(expiredWriter).close();
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", expiredWriter);
        ticker.advance(TTL);
        AtomicInteger replacementSupplierCalls = new AtomicInteger();
        CountDownLatch acquireStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> cleanup = executor.submit(cache::cleanupExpired);
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            Future<YtDynamicTableWriter> acquire = executor.submit(() -> {
                acquireStarted.countDown();
                return useWriter(cache, "table", () -> {
                    replacementSupplierCalls.incrementAndGet();
                    return replacementWriter;
                });
            });
            Assertions.assertTrue(acquireStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(acquire.isDone());
            Assertions.assertEquals(0, replacementSupplierCalls.get());

            releaseClose.countDown();
            cleanup.get(5, TimeUnit.SECONDS);

            Assertions.assertSame(replacementWriter, acquire.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, replacementSupplierCalls.get());
            Assertions.assertEquals(1, cache.getSize());
            Mockito.verify(expiredWriter).close();
        } finally {
            releaseClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testStopCleanupWaitsForDirectCleanup() throws Exception {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            closeStarted.countDown();
            releaseClose.await();
            return null;
        }).when(writer).close();
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);
        ticker.advance(TTL);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch stopStarted = new CountDownLatch(1);

        try {
            Future<?> cleanup = executor.submit(cache::cleanupExpired);
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            Future<Collection<YtDynamicTableWriter>> stop = executor.submit(() -> {
                stopStarted.countDown();
                return cache.stopCleanup();
            });

            Assertions.assertTrue(stopStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(stop.isDone());

            releaseClose.countDown();
            cleanup.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(List.of(), stop.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(0, cache.getSize());
            Mockito.verify(writer).close();
        } finally {
            releaseClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testWithWriterPreventsExpirationUntilActionCompletes() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        cache.withWriter("table", () -> writer, cachedWriter -> {
            ticker.advance(TTL.plusNanos(1));
            cache.cleanupExpired();

            Assertions.assertSame(writer, cachedWriter);
            Assertions.assertEquals(1, cache.getSize());
            Mockito.verify(writer, Mockito.never()).close();
        });
        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        ticker.advance(TTL);
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    public void testForEachWriterPinsEntryDuringAction() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);

        cache.forEachWriter(cachedWriter -> {
            ticker.advance(TTL.plusNanos(1));
            cache.cleanupExpired();

            Assertions.assertSame(writer, cachedWriter);
            Assertions.assertEquals(1, cache.getSize());
            Mockito.verify(writer, Mockito.never()).close();
        });

        ticker.advance(TTL);
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    @Timeout(10)
    public void testForEachWriterBlocksConcurrentUse() throws Exception {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "table", writer);
        CountDownLatch bulkActionStarted = new CountDownLatch(1);
        CountDownLatch releaseBulkAction = new CountDownLatch(1);
        CountDownLatch useAttempted = new CountDownLatch(1);
        CountDownLatch useStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> bulkAction = executor.submit(() -> cache.forEachWriter(ignored -> {
                bulkActionStarted.countDown();
                await(releaseBulkAction);
            }));
            Assertions.assertTrue(bulkActionStarted.await(5, TimeUnit.SECONDS));

            Future<?> use = executor.submit(() -> {
                useAttempted.countDown();
                cache.withWriter("table", () -> {
                    throw new AssertionError("Writer supplier must not be called for a cached writer");
                }, ignored -> useStarted.countDown());
            });
            Assertions.assertTrue(useAttempted.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, useStarted.getCount());
            Assertions.assertFalse(use.isDone());

            releaseBulkAction.countDown();
            bulkAction.get(5, TimeUnit.SECONDS);
            use.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(0, useStarted.getCount());
        } finally {
            releaseBulkAction.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testExpirationStartsAfterLastConcurrentUse() throws Exception {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> first = executor.submit(() -> cache.withWriter("table", () -> writer, ignored -> {
                firstStarted.countDown();
                await(releaseFirst);
            }));
            Assertions.assertTrue(firstStarted.await(5, TimeUnit.SECONDS));

            Future<?> second = executor.submit(() -> cache.withWriter("table", () -> {
                throw new AssertionError("Writer supplier must not be called for a cached writer");
            }, ignored -> {
                secondStarted.countDown();
                await(releaseSecond);
            }));
            Assertions.assertTrue(secondStarted.await(5, TimeUnit.SECONDS));

            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            ticker.advance(TTL.plusNanos(1));
            cache.cleanupExpired();

            Assertions.assertEquals(1, cache.getSize());
            Mockito.verify(writer, Mockito.never()).close();

            releaseSecond.countDown();
            second.get(5, TimeUnit.SECONDS);
            ticker.advance(TTL);
            cache.cleanupExpired();

            Assertions.assertEquals(0, cache.getSize());
            Mockito.verify(writer).close();
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testActionFailureStillUnpinsEntry() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        RuntimeException failure = new RuntimeException("action failed");

        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class,
                () -> cache.withWriter("table", () -> writer, ignored -> {
                    throw failure;
                }));
        Assertions.assertSame(failure, thrown);

        ticker.advance(TTL);
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    @Timeout(10)
    public void testStopWaitsForWithWriterAction() throws Exception {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        CountDownLatch stopStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> action = executor.submit(() -> cache.withWriter("table", () -> writer, ignored -> {
                actionStarted.countDown();
                try {
                    releaseAction.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }));
            Assertions.assertTrue(actionStarted.await(5, TimeUnit.SECONDS));

            Future<Collection<YtDynamicTableWriter>> stop = executor.submit(() -> {
                stopStarted.countDown();
                return cache.stopCleanup();
            });
            Assertions.assertTrue(stopStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(stop.isDone());

            releaseAction.countDown();
            action.get(5, TimeUnit.SECONDS);

            Assertions.assertEquals(List.of(writer), stop.get(5, TimeUnit.SECONDS));
            Mockito.verify(writer).clearCacheStateListener();
            Mockito.verify(writer, Mockito.never()).close();
        } finally {
            releaseAction.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testStopRejectsAcquireAndReturnsRemainingWriters() {
        YtDynamicTableWriter firstWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriter secondWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        putWriter(cache, "first", firstWriter);
        putWriter(cache, "second", secondWriter);

        Collection<YtDynamicTableWriter> remaining = cache.stopCleanup();

        Assertions.assertEquals(Set.of(firstWriter, secondWriter), Set.copyOf(remaining));
        Assertions.assertThrows(UnsupportedOperationException.class, remaining::clear);
        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(firstWriter, Mockito.never()).close();
        Mockito.verify(secondWriter, Mockito.never()).close();
        Mockito.verify(firstWriter).clearCacheStateListener();
        Mockito.verify(secondWriter).clearCacheStateListener();

        AtomicInteger supplierCalls = new AtomicInteger();
        AtomicInteger actionCalls = new AtomicInteger();
        Assertions.assertThrows(IllegalStateException.class, () -> cache.withWriter(
                "third",
                () -> {
                    supplierCalls.incrementAndGet();
                    return Mockito.mock(YtDynamicTableWriter.class);
                },
                ignored -> actionCalls.incrementAndGet()));
        Assertions.assertEquals(0, supplierCalls.get());
        Assertions.assertEquals(0, actionCalls.get());
    }

    private YtDynamicTableWriterCache makeCache() {
        return new YtDynamicTableWriterCache(TTL, ticker);
    }

    private void putWriter(
            YtDynamicTableWriterCache cache,
            String tableName,
            YtDynamicTableWriter writer) {
        cache.withWriter(tableName, () -> writer, cachedWriter -> Assertions.assertSame(writer, cachedWriter));
    }

    private YtDynamicTableWriter useWriter(
            YtDynamicTableWriterCache cache,
            String tableName,
            Supplier<YtDynamicTableWriter> writerSupplier) {
        AtomicReference<YtDynamicTableWriter> result = new AtomicReference<>();
        cache.withWriter(tableName, writerSupplier, result::set);
        YtDynamicTableWriter writer = result.get();
        Assertions.assertNotNull(writer);
        return writer;
    }

    private Runnable captureStateListener(YtDynamicTableWriter writer) {
        ArgumentCaptor<Runnable> listenerCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(writer).setCacheStateListener(listenerCaptor.capture());
        return listenerCaptor.getValue();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class TestTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        private void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }
}
