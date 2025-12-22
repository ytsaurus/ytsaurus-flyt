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
import org.apache.flink.configuration.Configuration;
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
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTreeNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

public class YtRowDataInputFormat extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(YtRowDataInputFormat.class);

    private static final long serialVersionUID = 1L;

    private final ComplexYtPath path;

    private final String ysonSchemaString;

    private final OAuthCredentialsConfig credentialsConfig;

    private final long limit;

    private Function<YTreeNode, RowData> deserializeFunction;

    private TypeInformation<RowData> rowDataTypeInfo;

    private DeserializationSchema<RowData> deserializer;

    private transient YTsaurusClient client;

    private transient TableReader<YTreeNode> tableReader;

    private transient boolean hasNext;

    private transient Queue<YTreeNode> readBuffer;

    private transient long rowsRead;

    private YtRowDataInputFormat(
            ComplexYtPath path,
            String ysonSchemaString,
            CredentialsProvider credentialsProvider,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo) {
        this.path = path;
        this.ysonSchemaString = ysonSchemaString;
        this.credentialsConfig = credentialsProvider.getCredentials(path.getClusterName());
        this.limit = limit;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing here
    }

    @Override
    public void openInputFormat() {
        client = YtUtils.makeYtClient(path, credentialsConfig);
        readBuffer = new LinkedBlockingQueue<>();
    }

    @Override
    public void closeInputFormat() {
        close();
    }

    @Override
    public void open(InputSplit inputSplit) {
        if (deserializer instanceof YTreeNodeDeserializationSchema) {
            // Fast convertor
            final YTreeNodeDeserializationSchema deserializationSchema = (YTreeNodeDeserializationSchema) deserializer;
            deserializeFunction = deserializationSchema::deserialize;
        } else {
            LOG.warn("You use slow YT converter {}. Please implement YTreeNodeDeserializationSchema!",
                    deserializer.getClass().getSimpleName());
            deserializeFunction = yTreeNode -> {
                try {
                    // Slow convertor
                    return deserializer.deserialize(yTreeNode.toBinary());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }
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
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
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
            final RowData rowData = deserializeFunction.apply(row);

            rowsRead++;
            updateHasNext();
            return rowData;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateHasNext() {
        hasNext = (!readBuffer.isEmpty() || tableReader.canRead()) && (limit == -1 || rowsRead < limit);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ComplexYtPath path;

        private String ysonSchemaString;

        private CredentialsProvider credentialsProvider;

        private long limit;

        private DeserializationSchema<RowData> deserializer;

        private TypeInformation<RowData> rowDataTypeInfo;

        public Builder() {

        }

        public Builder setPath(ComplexYtPath path) {
            this.path = path;
            return this;
        }

        public Builder setYsonSchemaString(String ysonSchemaString) {
            this.ysonSchemaString = ysonSchemaString;
            return this;
        }

        public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public Builder setLimit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder setDeserializer(DeserializationSchema<RowData> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public YtRowDataInputFormat build() {
            return new YtRowDataInputFormat(
                    this.path,
                    this.ysonSchemaString,
                    this.credentialsProvider,
                    this.limit,
                    this.deserializer,
                    this.rowDataTypeInfo);
        }
    }
}
