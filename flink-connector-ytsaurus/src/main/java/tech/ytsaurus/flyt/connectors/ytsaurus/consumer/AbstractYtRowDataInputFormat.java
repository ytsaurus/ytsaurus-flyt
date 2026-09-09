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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadSerializationContext;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.RetryUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTreeNode;

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
        this.retryStrategy = retryStrategy;
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

        failFast = ((YtInputSplit) inputSplit).isFailFast();

        // open(split) may be called once per split on the same instance, start at 0 for each split
        rowsRead = 0;
        openReaderWithRetry();
        // table reader could be null in case of interruption
        hasNext = tableReader != null && tableReader.canRead();

        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                () -> getRuntimeContext().getMetricGroup());
    }


    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        YTreeNode row;
        try {
            row = pollRow();
        } catch (InterruptedException e) {
            // cooperative cancellation: stop quietly so close() is not held up and the reload is
            // reported as interrupted rather than failed
            Thread.currentThread().interrupt();
            LOG.info("Interrupted while reading {} after {} rows, stopping.",
                    path.getFullPath(), rowsRead);
            hasNext = false;
            return null;
        } catch (Exception e) {
            throw new IOException(String.format(
                    "Unable to read table %s, failed at row %d", path.getFullPath(), rowsRead), e);
        }
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
     * Returns the next row, or {@code null} when the table is exhausted.
     *
     * <p>A read that fails is never retried, because row index selectors are not supported for sorted dynamic tables,
     * while re-reading everything before failed row looks like an overkill. An empty batch is different and is retried.
     */
    @Nullable
    private YTreeNode pollRow() throws Exception {
        YTreeNode buffered = readBuffer.poll();
        if (buffered != null) {
            return buffered;
        }
        if (!tableReader.canRead()) {
            return null;
        }

        RetryStrategy retry = null;
        while (true) {
            tableReader.readyEvent().get();
            List<YTreeNode> rows = tableReader.read();
            if (rows != null) {
                readBuffer.addAll(rows);
            }
            YTreeNode row = readBuffer.poll();
            if (row != null) {
                return row;
            }
            if (!tableReader.canRead()) {
                return null;
            }
            // EOF arrives as an empty batch that flips canRead() as it is read, so an empty batch
            // with canRead() still true is not the end of the table: readyEvent() also fires when
            // the request future completes, which can happen just before EOF reaches the stash.
            if (retry == null) {
                retry = newRetryStrategy();
            }
            if (retry.getNumRemainingRetries() < 1) {
                throw new IOException(String.format(
                        "Empty batch from %s at row %d while the reader is not at EOF",
                        path.getFullPath(), rowsRead));
            }
            retry = RetryUtils.awaitNextAttempt(retry);
            if (retry == null) {
                return null;
            }
        }
    }

    /**
     * Opens the reader, retrying transient YT failures with backoff.
     */
    private void openReaderWithRetry() {
        RetryStrategy retry = newRetryStrategy();
        while (true) {
            try {
                openReader();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Interrupted while opening reader for {}, stopping.", path.getFullPath());
                return;
            } catch (Exception e) {
                closeReaderQuietly();
                if (retry.getNumRemainingRetries() < 1) {
                    throw new FlinkRuntimeException(String.format(
                            "Unable to open reader for table %s", path.getFullPath()), e);
                }
                LOG.warn("YT failure while opening reader for {}, retrying in {} ({} attempts left)",
                        path.getFullPath(), retry.getRetryDelay(), retry.getNumRemainingRetries(), e);
                retry = RetryUtils.awaitNextAttempt(retry);
                if (retry == null) {
                    return;
                }
            }
        }
    }

    private void openReader() throws Exception {
        readBuffer.clear();
        tableReader = client.readTable(
                new ReadTable<>(YPath.simple(path.getFullPath()), ReadSerializationContext.ysonBinary())
        ).get();
    }

    private void closeReaderQuietly() {
        if (tableReader == null) {
            return;
        }
        try {
            tableReader.close().orTimeout(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("Unable to close table reader for {}", path.getFullPath(), e);
        } finally {
            tableReader = null;
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
        loadNumber++;
        return new YtInputSplit[]{new YtInputSplit(0, 1, loadNumber == 1)};
    }

    /**
     * The first load of a FULL cache should fail-fast, subsequent reload should be retried.
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
