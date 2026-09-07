package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadSerializationContext;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.core.cypress.Range;
import tech.ytsaurus.core.cypress.RangeLimit;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractYtRowDataInputFormat
        extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractYtRowDataInputFormat.class);

    protected final String ysonSchemaString;
    protected final long limit;
    protected final DeserializationSchema<RowData> deserializer;
    protected final TypeInformation<RowData> rowDataTypeInfo;
    protected final CredentialsProvider credentialsProvider;
    protected final SerializableSupplier<RetryStrategy> retryStrategy;

    protected transient YTsaurusClient client;
    protected transient TableReader<YTreeNode> tableReader;
    protected transient boolean hasNext;
    protected transient Queue<YTreeNode> readBuffer;
    protected transient long rowsRead;
    /** Counts FULL cache loads; lives on the instance createInputSplits() is called on. */
    private transient int loadNumber;
    private transient boolean failFast;
    protected transient Function<YTreeNode, RowData> deserializeFunction;

    protected ComplexYtPath path;

    protected AbstractYtRowDataInputFormat(
            String ysonSchemaString,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo,
            CredentialsProvider credentialsProvider,
            SerializableSupplier<RetryStrategy> retryStrategy) {

        this.ysonSchemaString = ysonSchemaString;
        this.limit = limit;
        this.deserializer = deserializer;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.credentialsProvider = credentialsProvider;
        this.retryStrategy = checkNotNull(retryStrategy, "No retry strategy supplied.");
    }

    @Override
    public void openInputFormat() {
        path = resolvePath();
        client = createClient(path);
        readBuffer = new LinkedBlockingQueue<>();
    }

    @Override
    public void open(InputSplit inputSplit) {
        initDeserializer();

        if (path.isPartitioned()) {
            throw new UnsupportedOperationException("Partition table unsupported");
        }

        LOG.info("Yson schema: {}", ysonSchemaString);
        LOG.info("Row Data Type: {}", rowDataTypeInfo);

        failFast = inputSplit instanceof YtInputSplit && ((YtInputSplit) inputSplit).isFailFast();

        // open(split) may be called once per split on the same instance, and rowsRead is the
        // resume offset, so it must start at 0 for each split
        rowsRead = 0;
        openReaderWithRetry();
        // the reader is left null only when the thread was interrupted while opening it: report an
        // empty split so the caller stops cooperatively instead of failing the whole reload
        hasNext = tableReader != null && tableReader.canRead();

        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                () -> getRuntimeContext().getMetricGroup());
    }


    @Override
    public RowData nextRecord(RowData reuse) {
        if (!hasNext) {
            return null;
        }

        YTreeNode row = pollRowWithRetry();
        if (row == null) {
            LOG.info("Finished reading {} rows from {}", rowsRead, path.getFullPath());
            hasNext = false;
            return null;
        }

        if (rowsRead % 100000 == 0) {
            LOG.info("Total read {} rows from {}", rowsRead, path.getFullPath());
        }

        RowData rowData = deserializeFunction.apply(row);

        rowsRead++;
        updateHasNext();

        return rowData;
    }

    /**
     * Returns the next row, retrying transient YT failures with backoff.
     *
     * <p>On retry the reader is re-opened at row {@link #rowsRead}, so no row is emitted twice and
     * none is skipped. Returns {@code null} only when the table is really exhausted or the thread
     * was interrupted.
     */
    @Nullable
    private YTreeNode pollRowWithRetry() {
        return doWithRetry("reading", () -> {
            if (tableReader == null) {
                openReaderAt(rowsRead);
            }
            YTreeNode buffered = readBuffer.poll();
            if (buffered != null) {
                return buffered;
            }
            if (!tableReader.canRead()) {
                return null;
            }
            tableReader.readyEvent().get();
            List<YTreeNode> rows = tableReader.read();
            if (rows != null) {
                readBuffer.addAll(rows);
            }
            YTreeNode row = readBuffer.poll();
            if (row == null && tableReader.canRead()) {
                // EOF arrives as an empty batch that flips canRead() as it is read, so an empty
                // batch with canRead() still true is not the end of the table: readyEvent() also
                // fires when the request future completes, which can happen before EOF reaches
                // the stash. Retry instead of truncating the read.
                throw new IOException(String.format(
                        "Empty batch from %s at row %d while the reader is not at EOF",
                        path.getFullPath(), rowsRead));
            }
            return row;
        });
    }

    /** Opens the reader at {@link #rowsRead}, retrying transient YT failures with backoff. */
    private void openReaderWithRetry() {
        doWithRetry("opening reader for", () -> {
            openReaderAt(rowsRead);
            return tableReader;
        });
    }

    /**
     * Runs {@code body} — one attempt at a YT read, throwing to ask for another one — retrying
     * transient failures with backoff. Between attempts the broken reader is dropped, so the next
     * one re-opens at {@link #rowsRead}: no row is emitted twice and none is skipped. Returns
     * {@code null} if the thread was interrupted, which lets the caller stop cooperatively instead
     * of failing the whole reload; throws once retries run out.
     */
    @Nullable
    private <T> T doWithRetry(String action, SupplierWithException<T, Exception> body) {
        RetryStrategy retry = newRetryStrategy();
        while (true) {
            try {
                return body.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Interrupted while {} {} at row {}, stopping.",
                        action, path.getFullPath(), rowsRead);
                return null;
            } catch (Exception e) {
                retry = nextRetryOrThrow(retry, e, action);
                if (!backoff(retry)) {
                    return null;
                }
            }
        }
    }

    /** Drops the broken reader and returns the next strategy, or throws once retries run out. */
    private RetryStrategy nextRetryOrThrow(RetryStrategy retry, Exception cause, String action) {
        closeReaderQuietly();
        if (retry.getNumRemainingRetries() <= 1) {
            throw new FlinkRuntimeException(String.format(
                    "Unable to read table %s, failed at row %d", path.getFullPath(), rowsRead), cause);
        }
        RetryStrategy next = retry.getNextRetryStrategy();
        LOG.warn("Transient YT failure while {} {} at row {}, retrying in {} ({} attempts left)",
                action, path.getFullPath(), rowsRead, next.getRetryDelay(),
                next.getNumRemainingRetries(), cause);
        return next;
    }

    /** @return {@code false} if the thread was interrupted and reading must stop. */
    private boolean backoff(RetryStrategy retry) {
        try {
            Thread.sleep(retry.getRetryDelay().toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void openReaderAt(long startRow) throws Exception {
        YPath ypath = YPath.simple(path.getFullPath());
        if (startRow > 0) {
            ypath = ypath.plusRange(Range.lower(RangeLimit.row(startRow)));
            LOG.info("Resuming read of {} from row {}", path.getFullPath(), startRow);
        }
        readBuffer.clear();
        tableReader = client.readTable(
                new ReadTable<>(ypath, ReadSerializationContext.ysonBinary())
        ).get();
    }

    private void closeReaderQuietly() {
        TableReader<YTreeNode> reader = tableReader;
        tableReader = null;
        if (reader == null) {
            return;
        }
        try {
            reader.close().orTimeout(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("Unable to close table reader for {}", path.getFullPath(), e);
        }
    }


    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }


    /**
     * Closes all resources used.
     */
    @Override
    public void close() {
        closeReaderQuietly();

        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOG.error("Unable to close YT client");
        }
    }

    @Override
    public void closeInputFormat() {
        close();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        // InputFormatCacheLoader calls this on the long-lived instance once per reload and only
        // then clones it for the actual read, so the clone learns which load it belongs to.
        loadNumber++;
        return new YtInputSplit[]{new YtInputSplit(0, 1, loadNumber == 1)};
    }

    /**
     * The first load of a FULL cache runs inside {@code LookupFullCache#open}, which blocks the
     * task in {@code awaitFirstLoad()}: retrying there stalls startup for minutes, and a plain
     * restart rebuilds the cache anyway. Later reloads run on a background thread and a failure
     * there permanently disables the cache, so those honour the configured strategy. A plain scan
     * only ever performs load #1 and is handed a no-retry strategy, so this costs it nothing.
     */
    private RetryStrategy newRetryStrategy() {
        return failFast ? new FixedRetryStrategy(0, Duration.ZERO) : retryStrategy.get();
    }

    /** Carries to the reader whether it serves the blocking first load of a FULL cache. */
    public static final class YtInputSplit extends GenericInputSplit {
        private static final long serialVersionUID = 1L;

        private final boolean failFast;

        YtInputSplit(int partitionNumber, int totalNumberOfPartitions, boolean failFast) {
            super(partitionNumber, totalNumberOfPartitions);
            this.failFast = failFast;
        }

        boolean isFailFast() {
            return failFast;
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    protected abstract ComplexYtPath resolvePath();

    protected YTsaurusClient createClient(ComplexYtPath path) {
        return YtUtils.makeYtClient(path, credentialsProvider.getCredentials(path.getClusterName()));
    }

    protected void updateHasNext() {
        hasNext = (!readBuffer.isEmpty() || tableReader.canRead())
                && (limit == -1 || rowsRead < limit);
    }

    private void initDeserializer() {
        if (deserializer instanceof YTreeNodeDeserializationSchema) {
            // Fast convertor
            deserializeFunction = ((YTreeNodeDeserializationSchema) deserializer)::deserialize;
        } else {
            LOG.warn("You use slow YT converter {}. Please implement YTreeNodeDeserializationSchema!",
                    deserializer.getClass().getSimpleName());
            deserializeFunction = node -> {
                try {
                    // Slow convertor
                    return deserializer.deserialize(node.toBinary());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
