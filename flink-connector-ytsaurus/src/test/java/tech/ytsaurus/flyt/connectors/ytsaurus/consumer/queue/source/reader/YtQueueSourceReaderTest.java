package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueSourceReaderTest {
    @Test
    void snapshotAdvancesOnlyThroughEmittedPartOfFetchedBatch() throws Exception {
        TableSchema schema = queueSchema();
        List<Long> offsets = List.of(1L, 2L, 3L, 4L, 5L, 6L);
        List<UnversionedRow> rows = offsets.stream()
                .map(ignored -> queueRow())
                .collect(Collectors.toList());
        Map<UnversionedRow, String> values = new IdentityHashMap<>();
        for (int index = 0; index < rows.size(); index++) {
            values.put(rows.get(index), "record-" + offsets.get(index));
        }
        OneBatchPuller puller = new OneBatchPuller(new YtQueueBatch(
                schema,
                1,
                rows));
        RecordingCommitter committer = new RecordingCommitter();
        YtQueueSourceReader<String> reader = new YtQueueSourceReader<>(
                () -> puller,
                (row, ignoredSchema) -> values.get(row),
                readerOptions(),
                new Configuration(),
                new TestingReaderContext(),
                committer
        );
        TestingReaderOutput<String> output = new TestingReaderOutput<>();
        reader.addSplits(List.of(split("queue-id", 0, 1)));

        try {
            reader.isAvailable().get(2, TimeUnit.SECONDS);
            List<Long> expectedNextOffsets = List.of(2L, 3L, 4L, 5L, 6L, 7L);
            for (int index = 0; index < rows.size(); index++) {
                reader.pollNext(output);

                List<YtQueueSplit> snapshot = reader.snapshotState(index + 1L);
                assertThat(snapshot)
                        .extracting(YtQueueSplit::getNextOffset)
                        .containsExactly(expectedNextOffsets.get(index));
                assertThat(committer.lastSnapshot)
                        .extracting(YtQueueSplit::getNextOffset)
                        .containsExactly(expectedNextOffsets.get(index));
            }
            assertThat(output.getEmittedRecords()).containsExactly(
                    "record-1",
                    "record-2",
                    "record-3",
                    "record-4",
                    "record-5",
                    "record-6");
        } finally {
            reader.close();
        }
        assertThat(puller.closed).isTrue();
        assertThat(committer.closed).isTrue();
    }

    @Test
    void closesPullerFactoryWithoutAssignedSplits() throws Exception {
        TrackingPullerFactory pullerFactory = new TrackingPullerFactory();
        YtQueueSourceReader<String> reader = new YtQueueSourceReader<>(
                pullerFactory,
                (row, schema) -> row.toString(),
                readerOptions(),
                new Configuration(),
                new TestingReaderContext());

        reader.close();

        assertThat(pullerFactory.closed).isTrue();
        assertThat(pullerFactory.createdPullers).isZero();
    }

    @Test
    void closesWorkerPullerBeforeSharedFactory() throws Exception {
        OrderTrackingPullerFactory pullerFactory = new OrderTrackingPullerFactory();
        YtQueueSourceReader<String> reader = new YtQueueSourceReader<>(
                pullerFactory,
                (row, schema) -> row.toString(),
                readerOptions(),
                new Configuration(),
                new TestingReaderContext());
        reader.addSplits(List.of(split("queue-id", 0, 0)));
        assertThat(pullerFactory.pullStarted.await(2, TimeUnit.SECONDS)).isTrue();

        reader.close();

        assertThat(pullerFactory.closeOrder).containsExactly("puller", "factory");
    }

    @Test
    void closesPullerFactoryWhenDeserializerOpenFails() {
        TrackingPullerFactory pullerFactory = new TrackingPullerFactory();
        YtQueueRecordDeserializer<String> deserializer = new YtQueueRecordDeserializer<>() {
            @Override
            public void open(SourceReaderContext context) throws Exception {
                throw new IOException("open failed");
            }

            @Override
            public String deserialize(UnversionedRow row, TableSchema schema) {
                return row.toString();
            }
        };

        assertThatThrownBy(() -> new YtQueueSourceReader<>(
                pullerFactory,
                deserializer,
                readerOptions(),
                new Configuration(),
                new TestingReaderContext()))
                .isInstanceOf(IOException.class)
                .hasMessage("open failed");
        assertThat(pullerFactory.closed).isTrue();
        assertThat(pullerFactory.createdPullers).isZero();
    }

    private static final class OneBatchPuller implements YtQueuePuller {
        private final YtQueueBatch batch;
        private CompletableFuture<YtQueueBatch> inFlight;
        private boolean first = true;
        private volatile boolean closed;

        private OneBatchPuller(YtQueueBatch batch) {
            this.batch = batch;
        }

        @Override
        public synchronized CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request) {
            if (first) {
                first = false;
                return CompletableFuture.completedFuture(batch);
            }
            inFlight = new CompletableFuture<>();
            return inFlight;
        }

        @Override
        public synchronized void wakeUp() {
            if (inFlight != null) {
                inFlight.cancel(true);
            }
        }

        @Override
        public void close() {
            closed = true;
            wakeUp();
        }
    }

    private static TableSchema queueSchema() {
        return TableSchema.builder().build();
    }

    private static YtQueueReaderOptions readerOptions() {
        return new YtQueueReaderOptions(100, 1024, Duration.ofSeconds(1), 1, 2);
    }

    private static UnversionedRow queueRow() {
        return new UnversionedRow(List.of());
    }

    private static final class RecordingCommitter implements YtQueueOffsetCommitter {
        private List<YtQueueSplit> lastSnapshot = List.of();
        private boolean closed;

        @Override
        public void snapshotState(long checkpointId, List<YtQueueSplit> splits) {
            lastSnapshot = splits;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TrackingPullerFactory implements YtQueuePullerFactory {
        private int createdPullers;
        private boolean closed;

        @Override
        public YtQueuePuller get() {
            createdPullers++;
            TableSchema schema = queueSchema();
            return new OneBatchPuller(new YtQueueBatch(
                    schema,
                    0,
                    List.of()));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class OrderTrackingPullerFactory implements YtQueuePullerFactory {
        private final CountDownLatch pullStarted = new CountDownLatch(1);
        private final List<String> closeOrder = new CopyOnWriteArrayList<>();

        @Override
        public YtQueuePuller get() {
            return new YtQueuePuller() {
                private final CompletableFuture<YtQueueBatch> inFlight = new CompletableFuture<>();

                @Override
                public CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request) {
                    pullStarted.countDown();
                    return inFlight;
                }

                @Override
                public void wakeUp() {
                    inFlight.cancel(true);
                }

                @Override
                public void close() {
                    closeOrder.add("puller");
                }
            };
        }

        @Override
        public void close() {
            closeOrder.add("factory");
        }
    }
}
