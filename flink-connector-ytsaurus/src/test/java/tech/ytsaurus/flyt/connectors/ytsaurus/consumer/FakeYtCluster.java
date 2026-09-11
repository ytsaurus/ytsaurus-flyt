package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.rpcproxy.TReqReadTable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * In-memory stand-in for a YT cluster that can fail a read a configurable number of times.
 *
 */
final class FakeYtCluster {

    private final List<YTreeNode> rows;
    private final long failAtRow;
    private final List<String> requestedPaths = new ArrayList<>();

    private int openFailuresLeft;
    private int readFailuresLeft;
    private int emptyBatchesLeft;

    private FakeYtCluster(int rowCount, long failAtRow, int openFailures, int readFailures) {
        this.rows = IntStream.range(0, rowCount)
                .mapToObj(i -> YTree.builder().beginMap().key("id").value(i).endMap().build())
                .collect(Collectors.toList());
        this.failAtRow = failAtRow;
        this.openFailuresLeft = openFailures;
        this.readFailuresLeft = readFailures;
    }

    static FakeYtCluster healthy(int rowCount) {
        return new FakeYtCluster(rowCount, 0, 0, 0);
    }

    /** Fails the first {@code failures} readTable calls, i.e. before any row has been emitted. */
    static FakeYtCluster failingOnOpen(int rowCount, int failures) {
        return new FakeYtCluster(rowCount, 0, failures, 0);
    }

    /** Fails {@code failures} reads once the stream reaches {@code failAtRow}. */
    static FakeYtCluster failingAtRow(int rowCount, long failAtRow, int failures) {
        return new FakeYtCluster(rowCount, failAtRow, 0, failures);
    }

    /**
     * Returns {@code emptyBatches} empty batches before any data, without reaching EOF — the race
     * where readyEvent() fires because the request completed but the stash has nothing yet.
     */
    static FakeYtCluster returningEmptyBatches(int rowCount, int emptyBatches) {
        FakeYtCluster cluster = new FakeYtCluster(rowCount, 0, 0, 0);
        cluster.emptyBatchesLeft = emptyBatches;
        return cluster;
    }

    List<Integer> expectedIds() {
        return IntStream.range(0, rows.size()).boxed().collect(Collectors.toList());
    }

    /** Serialized YPath of every readTable request, in order. */
    List<String> requestedPaths() {
        return requestedPaths;
    }

    int failuresLeft() {
        return openFailuresLeft + readFailuresLeft;
    }

    YTsaurusClient client() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.readTable(any(ReadTable.class))).thenAnswer(invocation -> {
            ReadTable<?> request = invocation.getArgument(0);
            synchronized (requestedPaths) {
                requestedPaths.add(serializedPath(request));
            }
            synchronized (this) {
                if (openFailuresLeft > 0) {
                    openFailuresLeft--;
                    throw new IOException("transient YT failure while opening the reader");
                }
            }
            return CompletableFuture.completedFuture(reader());
        });
        return client;
    }

    private static String serializedPath(ReadTable<?> request) {
        TReqReadTable.Builder builder = TReqReadTable.newBuilder();
        request.writeTo(builder);
        return builder.getPath().toStringUtf8();
    }

    /**
     * Emits one row per batch and signals EOF the way the real reader does: the last read returns
     * nothing and flips {@code canRead()} in the same call.
     */
    private TableReader<YTreeNode> reader() throws Exception {
        @SuppressWarnings("unchecked")
        TableReader<YTreeNode> reader = mock(TableReader.class);
        AtomicInteger next = new AtomicInteger();
        AtomicBoolean eof = new AtomicBoolean();

        when(reader.readyEvent()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.canRead()).thenAnswer(invocation -> !eof.get());
        when(reader.read()).thenAnswer(invocation -> {
            int index = next.get();
            failIfArmed(index);
            synchronized (this) {
                if (emptyBatchesLeft > 0) {
                    emptyBatchesLeft--;
                    return List.of();
                }
            }
            if (index >= rows.size()) {
                eof.set(true);
                return null;
            }
            next.incrementAndGet();
            return List.of(rows.get(index));
        });
        return reader;
    }

    private synchronized void failIfArmed(int index) throws IOException {
        if (readFailuresLeft > 0 && index >= failAtRow) {
            readFailuresLeft--;
            throw new IOException("transient YT failure at row " + index);
        }
    }
}
