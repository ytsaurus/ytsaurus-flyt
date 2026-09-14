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
        cache.getOrAcquire("table", () -> writer);

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
        cache.getOrAcquire("table", () -> writer);
        Runnable stateChanged = captureStateListener(writer);

        Mockito.when(writer.isBusy()).thenReturn(true);
        stateChanged.run();
        ticker.advance(TTL.plusNanos(1));
        cache.cleanupExpired();

        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        Mockito.when(writer.isBusy()).thenReturn(false);
        stateChanged.run();

        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        ticker.advance(TTL.minusNanos(1));
        stateChanged.run();
        ticker.advance(Duration.ofNanos(1));
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    public void testRefreshExpirationOnAccess() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        cache.getOrAcquire("table", () -> writer);

        ticker.advance(Duration.ofSeconds(30));
        YtDynamicTableWriter cachedWriter = cache.getOrAcquire("table", () -> {
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
        cache.getOrAcquire("table", () -> writer);
        ticker.advance(TTL);

        Assertions.assertDoesNotThrow(cache::cleanupExpired);
        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();

        Assertions.assertSame(replacement, cache.getOrAcquire("table", () -> replacement));
        Assertions.assertEquals(1, cache.getSize());
    }

    @Test
    public void testValuesSnapshotIsDetachedAndUnmodifiable() {
        YtDynamicTableWriter firstWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriter secondWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        cache.getOrAcquire("first", () -> firstWriter);

        Collection<YtDynamicTableWriter> snapshot = cache.valuesSnapshot();
        cache.getOrAcquire("second", () -> secondWriter);

        Assertions.assertEquals(List.of(firstWriter), snapshot);
        Assertions.assertThrows(UnsupportedOperationException.class, snapshot::clear);
        Assertions.assertEquals(2, cache.getSize());
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
                    () -> cache.getOrAcquire("table", writerSupplier));
            Assertions.assertTrue(supplierStarted.await(5, TimeUnit.SECONDS));
            Future<YtDynamicTableWriter> second = executor.submit(() -> {
                secondAcquireStarted.countDown();
                return cache.getOrAcquire("table", writerSupplier);
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
        cache.getOrAcquire("table", () -> expiredWriter);
        ticker.advance(TTL);
        AtomicInteger replacementSupplierCalls = new AtomicInteger();
        CountDownLatch acquireStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> cleanup = executor.submit(cache::cleanupExpired);
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            Future<YtDynamicTableWriter> acquire = executor.submit(() -> {
                acquireStarted.countDown();
                return cache.getOrAcquire("table", () -> {
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
            Assertions.assertEquals(List.of(replacementWriter), cache.valuesSnapshot());
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
        cache.getOrAcquire("table", () -> writer);
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
    public void testLeasePreventsExpirationUntilReleased() {
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        YtDynamicTableWriterCache.WriterLease lease = cache.acquire("table", () -> writer);

        ticker.advance(TTL.plusNanos(1));
        cache.cleanupExpired();

        Assertions.assertSame(writer, lease.getWriter());
        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        lease.close();

        Assertions.assertEquals(1, cache.getSize());
        Mockito.verify(writer, Mockito.never()).close();

        ticker.advance(TTL);
        cache.cleanupExpired();

        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(writer).close();
    }

    @Test
    public void testStopRejectsAcquireAndReturnsRemainingWriters() {
        YtDynamicTableWriter firstWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriter secondWriter = Mockito.mock(YtDynamicTableWriter.class);
        YtDynamicTableWriterCache cache = makeCache();
        cache.getOrAcquire("first", () -> firstWriter);
        cache.getOrAcquire("second", () -> secondWriter);

        Collection<YtDynamicTableWriter> remaining = cache.stopCleanup();

        Assertions.assertEquals(Set.of(firstWriter, secondWriter), Set.copyOf(remaining));
        Assertions.assertThrows(UnsupportedOperationException.class, remaining::clear);
        Assertions.assertEquals(0, cache.getSize());
        Mockito.verify(firstWriter, Mockito.never()).close();
        Mockito.verify(secondWriter, Mockito.never()).close();
        Mockito.verify(firstWriter).clearCacheStateListener();
        Mockito.verify(secondWriter).clearCacheStateListener();

        AtomicInteger supplierCalls = new AtomicInteger();
        Assertions.assertThrows(IllegalStateException.class, () -> cache.getOrAcquire("third", () -> {
            supplierCalls.incrementAndGet();
            return Mockito.mock(YtDynamicTableWriter.class);
        }));
        Assertions.assertEquals(0, supplierCalls.get());
    }

    private YtDynamicTableWriterCache makeCache() {
        return new YtDynamicTableWriterCache(TTL, ticker);
    }

    private Runnable captureStateListener(YtDynamicTableWriter writer) {
        ArgumentCaptor<Runnable> listenerCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(writer).setCacheStateListener(listenerCaptor.capture());
        return listenerCaptor.getValue();
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
