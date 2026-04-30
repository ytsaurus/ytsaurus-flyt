package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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


    protected transient YTsaurusClient client;
    protected transient TableReader<YTreeNode> tableReader;
    protected transient boolean hasNext;
    protected transient Queue<YTreeNode> readBuffer;
    protected transient long rowsRead;
    protected transient Function<YTreeNode, RowData> deserializeFunction;

    protected ComplexYtPath path;

    protected AbstractYtRowDataInputFormat(
            String ysonSchemaString,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo, CredentialsProvider credentialsProvider) {

        this.ysonSchemaString = ysonSchemaString;
        this.limit = limit;
        this.deserializer = deserializer;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.credentialsProvider = credentialsProvider;
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

        this.tableReader = client.readTable(
                new ReadTable<>(
                        YPath.simple(path.getFullPath()),
                        ReadSerializationContext.ysonBinary()
                )
        ).join();

        hasNext = tableReader.canRead();
        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                () -> getRuntimeContext().getMetricGroup());
    }


    @Override
    public RowData nextRecord(RowData reuse) {
        if (!hasNext) {
            LOG.warn("No next records");
            return null;
        }

        try {
            if (readBuffer.isEmpty()) {
                tableReader.readyEvent().join();
                List<YTreeNode> rows = tableReader.read();
                if (rows == null) {
                    rows = new ArrayList<>();
                }
                readBuffer.addAll(rows);
            }

            YTreeNode row = readBuffer.poll();
            if (row == null) {
                LOG.warn("No next records {} in {}", rowsRead, path.getFullPath());
                updateHasNext();
                return null;
            }
            if (rowsRead % 100000 == 0) {
                LOG.info("Total read {} rows from {}", rowsRead, path.getFullPath());
            }

            RowData rowData = deserializeFunction.apply(row);

            rowsRead++;
            updateHasNext();

            return rowData;
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        try {
            if (tableReader != null) {
                tableReader.close().orTimeout(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            LOG.error("Unable to close table reader");
        }

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
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
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
