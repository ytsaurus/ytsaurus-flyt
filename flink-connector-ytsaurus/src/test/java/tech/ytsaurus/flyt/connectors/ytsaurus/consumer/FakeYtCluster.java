package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 * <p>Registered in a static map and resolved by table path, so the input format under test keeps no
 * reference to it and stays serializable, as a Flink input format must be.
 */
final class FakeYtCluster {

    private static final Map<String, FakeYtCluster> CLUSTERS = new ConcurrentHashMap<>();
    private static final Pattern ROW_INDEX = Pattern.compile("\"row_index\"=(\\d+)");

    private final List<YTreeNode> rows;

    private final long failAtRow;

    private int failuresLeft;
    private final List<String> requestedPaths = new ArrayList<>();
    private final List<Integer> requestedStartRows = new ArrayList<>();

    private FakeYtCluster(int rowCount, long failAtRow, int failures) {
        this.rows = IntStream.range(0, rowCount)
                .mapToObj(i -> YTree.builder().beginMap().key("id").value(i).endMap().build())
                .collect(Collectors.toList());
        this.failAtRow = failAtRow;
        this.failuresLeft = failures;
    }

    static FakeYtCluster register(String path, int rowCount, long failAtRow, int failures) {
        FakeYtCluster cluster = new FakeYtCluster(rowCount, failAtRow, failures);
        CLUSTERS.put(path, cluster);
        return cluster;
    }

    static FakeYtCluster get(String path) {
        return CLUSTERS.get(path);
    }

    static void reset() {
        CLUSTERS.clear();
    }

    List<Integer> expectedIds() {
        return IntStream.range(0, rows.size()).boxed().collect(Collectors.toList());
    }

    /** Serialized YPath of every readTable request, in order. */
    List<String> requestedPaths() {
        return requestedPaths;
    }

    /** Lower row limit of every readTable request: 0 for a fresh read, N for a resume at row N. */
    List<Integer> requestedStartRows() {
        return requestedStartRows;
    }

    int failuresLeft() {
        return failuresLeft;
    }

    YTsaurusClient client() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.readTable(any(ReadTable.class))).thenAnswer(invocation -> {
            ReadTable<?> request = invocation.getArgument(0);
            String path = serializedPath(request);
            int startRow = startRow(path);
            synchronized (requestedPaths) {
                requestedPaths.add(path);
                requestedStartRows.add(startRow);
            }
            return CompletableFuture.completedFuture(reader(startRow));
        });
        return client;
    }

    private static String serializedPath(ReadTable<?> request) {
        TReqReadTable.Builder builder = TReqReadTable.newBuilder();
        request.writeTo(builder);
        return builder.getPath().toStringUtf8();
    }

    /**
     * Parses the lower row limit the input format attaches when it resumes. YPath serializes a row
     * range as an attribute, e.g.
     * {@code <"ranges"=[{"lower_limit"={"row_index"=4;};};];>//home/test/table}.
     */
    private static int startRow(String path) {
        Matcher matcher = ROW_INDEX.matcher(path);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }

    /**
     * Emits one row per batch and signals EOF the way the real reader does: the last read returns
     * nothing and flips {@code canRead()} in the same call.
     */
    private TableReader<YTreeNode> reader(int startRow) throws Exception {
        @SuppressWarnings("unchecked")
        TableReader<YTreeNode> reader = mock(TableReader.class);
        AtomicInteger next = new AtomicInteger(startRow);
        AtomicBoolean eof = new AtomicBoolean();

        when(reader.readyEvent()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.canRead()).thenAnswer(invocation -> !eof.get());
        when(reader.read()).thenAnswer(invocation -> {
            int index = next.get();
            failIfArmed(index);
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
        if (failuresLeft > 0 && index >= failAtRow) {
            failuresLeft--;
            throw new IOException("transient YT failure at row " + index);
        }
    }
}
