package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.function.SerializableFunction;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtClientConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor.ManualCredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.RowTypeUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

@UtilityClass
public final class YtProducers {
    private static final String YT_SINK_NAME_TEMPLATE = "YT<%s::%s>";
    private static final String YT_SINK_UID_TEMPLATE = "yt-sink-%s--%s";

    /**
     * Write stream to YT dynamic table
     *
     * @param dataStream   to write
     * @param path         of the catalog to write to
     * @param dataType     physical data type (flink table schema)
     * @param ysonSchema   string representation of YSON schema for the table
     * @param clientConfig YT client config
     * @param converter    to convert user type to RowData
     * @param <T>          type of data in data stream
     */
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            String path,
                                                            DataType dataType,
                                                            String ysonSchema,
                                                            PartitionConfig partitionConfig,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter) {
        return writeToDynamicTable(dataStream,
                path,
                dataType,
                ysonSchema,
                partitionConfig,
                clientConfig,
                converter,
                TimestampFormat.ISO_8601,
                ReshardingConfig.none());
    }

    /**
     * Write stream to YT dynamic table
     *
     * @param dataStream   to write
     * @param path         of the catalog to write to
     * @param dataType     physical data type (flink table schema)
     * @param ysonSchema   string representation of YSON schema for the table
     * @param clientConfig YT client config
     * @param converter    to convert user type to RowData
     * @param <T>          type of data in data stream
     * @return sink object
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            String path,
                                                            DataType dataType,
                                                            String ysonSchema,
                                                            PartitionConfig partitionConfig,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter,
                                                            TimestampFormat format,
                                                            ReshardingConfig reshardingConfig) {
        return writeToDynamicTable(dataStream,
                path,
                dataType,
                ysonSchema,
                partitionConfig,
                clientConfig,
                converter,
                format,
                reshardingConfig,
                YtTableAttributes.empty());
    }

    /**
     * Write stream to YT dynamic table
     *
     * @param dataStream   to write
     * @param path         of the catalog to write to
     * @param dataType     physical data type (flink table schema)
     * @param ysonSchema   string representation of YSON schema for the table
     * @param clientConfig YT client config
     * @param converter    to convert user type to RowData
     * @param <T>          type of data in data stream
     * @return sink object
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            String path,
                                                            DataType dataType,
                                                            String ysonSchema,
                                                            PartitionConfig partitionConfig,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter,
                                                            TimestampFormat format,
                                                            ReshardingConfig reshardingConfig,
                                                            YtTableAttributes tableAttributes) {
        return writeToDynamicTable(dataStream,
                path,
                dataType,
                ysonSchema,
                partitionConfig,
                clientConfig,
                converter,
                format,
                reshardingConfig,
                tableAttributes,
                YtWriterOptions.builder().build());
    }

    /**
     * Write stream to YT dynamic table
     *
     * @param dataStream   to write
     * @param path         of the catalog to write to
     * @param dataType     physical data type (flink table schema)
     * @param ysonSchema   string representation of YSON schema for the table
     * @param clientConfig YT client config
     * @param converter    to convert user type to RowData
     * @param <T>          type of data in data stream
     * @return sink object
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            String path,
                                                            DataType dataType,
                                                            String ysonSchema,
                                                            PartitionConfig partitionConfig,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter,
                                                            TimestampFormat format,
                                                            ReshardingConfig reshardingConfig,
                                                            YtTableAttributes tableAttributes,
                                                            YtWriterOptions ytWriterOptions) {
        if (reshardingConfig == null) {
            reshardingConfig = ReshardingConfig.none();
        }
        var rowDataSinkFunction = (RichSinkFunction<RowData>)
                ((SinkFunctionProvider) YtDynamicTableSink.builder()
                        .type(dataType)
                        .ytConverters(new RowDataToYtListConverters(format))
                        .path(ComplexYtPath.builder()
                                .clusterName(clientConfig.getProxy())
                                .basePath(path)
                                .build())
                        .retryStrategy(() -> new ExponentialBackoffRetryStrategy(
                                5,
                                Duration.of(10, ChronoUnit.SECONDS),
                                Duration.of(2, ChronoUnit.MINUTES)))
                        .ysonSchemaString(ysonSchema)
                        .credentialsProvider(
                                new ManualCredentialsProvider(clientConfig.getUser(), clientConfig.getToken()))
                        .partitionConfig(partitionConfig)
                        .reshardingConfig(reshardingConfig)
                        .tableAttributes(tableAttributes)
                        .ytWriterOptions(ytWriterOptions)
                        .build()
                        .getSinkRuntimeProvider(null))
                        .createSinkFunction();

        var sink = new RichSinkFunction<T>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                rowDataSinkFunction.setRuntimeContext(getRuntimeContext());
                rowDataSinkFunction.open(parameters);
            }

            @Override
            public void invoke(T value, Context context) throws Exception {
                rowDataSinkFunction.invoke(converter.apply(value), context);
            }

            @Override
            public void close() throws Exception {
                rowDataSinkFunction.close();
            }
        };

        return dataStream.addSink(sink)
                .uid(String.format(YT_SINK_UID_TEMPLATE, clientConfig.getProxy(), path))
                .name(String.format(YT_SINK_NAME_TEMPLATE, clientConfig.getProxy(), path));
    }

    /**
     * Write stream to YT dynamic table automatically extracting flink table schema (data type)
     *
     * @param dataStream      to write
     * @param tClass          class of the data
     * @param path            of the catalog to write to
     * @param ysonSchema      string representation of YSON schema for the table
     * @param partitionConfig config containing settings for partitioning
     * @param clientConfig    YT client config
     * @param converter       to convert user type to RowData
     * @param <T>             type of data in data stream
     * @return sink object
     */
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            Class<T> tClass,
                                                            String path,
                                                            String ysonSchema,
                                                            PartitionConfig partitionConfig,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter) {
        DataType rowType = RowTypeUtils.getRowTypeFromClass(tClass);
        return writeToDynamicTable(dataStream, path, rowType, ysonSchema, partitionConfig, clientConfig, converter);
    }

    /**
     * Write stream to YT dynamic table automatically extracting flink table schema (data type)
     *
     * @param dataStream   to write
     * @param tClass       class of the data
     * @param path         of the catalog to write to
     * @param ysonSchema   string representation of YSON schema for the table
     * @param clientConfig YT client config
     * @param converter    to convert user type to RowData
     * @param <T>          type of data in data stream
     * @return sink object
     */
    public static <T> DataStreamSink<T> writeToDynamicTable(DataStream<T> dataStream,
                                                            Class<T> tClass,
                                                            String path,
                                                            String ysonSchema,
                                                            YtClientConfig clientConfig,
                                                            SerializableFunction<T, RowData> converter) {
        return writeToDynamicTable(dataStream, tClass, path, ysonSchema, null, clientConfig, converter);
    }
}
