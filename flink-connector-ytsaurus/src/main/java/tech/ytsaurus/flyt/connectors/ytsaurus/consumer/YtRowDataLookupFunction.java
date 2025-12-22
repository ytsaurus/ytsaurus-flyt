package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.lang.model.SourceVersion;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.LookupMethod;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtClusterUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class YtRowDataLookupFunction extends LookupFunction {

    private static final RetryStrategy DEFAULT_RETRY_STRATEGY = new ExponentialBackoffRetryStrategy(
            5,
            Duration.of(10, ChronoUnit.SECONDS),
            Duration.of(2, ChronoUnit.MINUTES)
    );

    private final OAuthCredentialsConfig credentialsConfig;

    private final String ysonSchemaString;

    @Getter
    private final ComplexYtPath path;

    private final LookupMethod lookupMethod;

    private final PartitionConfig partitionConfig;

    private final DeserializationSchema<RowData> deserializer;

    private Function<YTreeNode, RowData> deserializeFunction;

    private DataType[] keyTypes;

    private String[] keyNames;

    private String[] fieldNames;

    @Setter
    private boolean failOnUnavailable;

    private transient YTsaurusClient client;

    private transient TableSchema schema;

    private transient RowDataToYtListConverters.RowDataToYtMapConverter externalConverter;

    private transient ExecutorService executors;

    private transient List<ComplexYtPath> partitions;

    private transient String selectQuery;


    @SuppressWarnings("checkstyle:parameternumber")
    public YtRowDataLookupFunction(
            CredentialsProvider credentialsProvider,
            String ysonSchemaString,
            ComplexYtPath path,
            LookupMethod lookupMethod,
            @Nullable PartitionConfig partitionConfig,
            DeserializationSchema<RowData> deserializer,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            DataType[] keyTypes,
            RowType rowType,
            boolean failOnUnavailable) {
        checkNotNull(ysonSchemaString, "No YT schema supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        checkNotNull(keyTypes, "No keyTypes supplied.");
        log.info("Field names {}", List.of(fieldNames));
        log.info("Field types {}", List.of(fieldTypes));
        log.info("Field keyNames {}", List.of(keyNames));
        log.info("Field keyTypes {}", List.of(keyTypes));
        log.info("Lookup method {}", lookupMethod);
        log.info("RowType: {}", rowType);
        log.info("Fail on unavailable {}", failOnUnavailable);
        this.ysonSchemaString = ysonSchemaString;
        this.credentialsConfig = credentialsProvider.getCredentials(path.getClusterName());
        this.path = path;
        this.partitionConfig = partitionConfig;
        this.deserializer = deserializer;
        this.keyTypes = keyTypes;
        this.keyNames = keyNames;
        this.lookupMethod = lookupMethod;
        this.fieldNames = fieldNames;
        this.failOnUnavailable = failOnUnavailable;
    }

    @Override
    public void open(FunctionContext context) {
        log.info("Open lookup connector with schema: {}", ysonSchemaString);
        if (deserializer instanceof YTreeNodeDeserializationSchema) {
            // Fast convertor
            final YTreeNodeDeserializationSchema deserializationSchema = (YTreeNodeDeserializationSchema) deserializer;
            deserializeFunction = deserializationSchema::deserialize;
        } else {
            log.warn("You use slow YT converter {}. Please implement YTreeNodeDeserializationSchema!",
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
        schema = TableSchema.fromYTree(YTreeTextSerializer.deserialize(ysonSchemaString)).toLookup();
        externalConverter = createKeyExternalConverter();
        client = YtUtils.makeYtClient(path, credentialsConfig);
        if (path.isPartitioned()) {
            executors = Executors.newCachedThreadPool();
            partitions = scanPartitions();
        }

        validateKeyNames();

        StringBuilder query = new StringBuilder()
                .append("SELECT ")
                .append(String.join(", ", fieldNames))
                .append(" FROM [%s] ")
                .append("WHERE ");
        String filters = Arrays.stream(keyNames)
                .map(keyName -> String.format("%s = {%s}", keyName, keyName))
                .collect(Collectors.joining(" AND "));
        query.append(filters);
        this.selectQuery = query.toString();
        log.info("Select Query: {}", selectQuery);
        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                context::getMetricGroup);
    }

    private void validateKeyNames() {
        Optional<String> invalidName = Arrays.stream(keyNames)
                .filter(name -> !SourceVersion.isName(name))
                .findAny();

        if (invalidName.isPresent()) {
            throw new IllegalArgumentException(
                    "Key is not valid key name. You can use only literals, digits or '_' : " + invalidName.get());
        }
    }

    private RowDataToYtListConverters.RowDataToYtMapConverter createKeyExternalConverter() {
        // TODO add property
        RowDataToYtListConverters converters = new RowDataToYtListConverters(TimestampFormat.SQL);
        List<DataTypes.Field> keyFields = new ArrayList<>();
        for (int i = 0; i < keyNames.length; i++) {
            keyFields.add(DataTypes.FIELD(keyNames[i], keyTypes[i]));
        }
        return converters.createConverter(DataTypes.ROW(keyFields).getLogicalType(), null);
    }

    @SneakyThrows
    private List<ComplexYtPath> scanPartitions() {
        final YTreeNode list = client.listNode(path.getBasePath()).get(10, TimeUnit.SECONDS);
        log.info("Table {} has partitions {}", path.getBasePath(), list);
        return list.asList().stream()
                .map(partition -> path.copy().setTableName(partition.stringValue()))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        if (path.isPartitioned()) {
            return lookupFromPartitions(keyRow);
        }
        return lookupFromTable(keyRow, path.getFullPath());
    }

    private List<RowData> lookupFromPartitions(RowData keyRow) throws Exception {
        final AtomicBoolean hasError = new AtomicBoolean();
        final AtomicReference<Exception> error = null;

//            fork join
        final CompletableFuture<Collection<RowData>>[] futures = new CompletableFuture[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            final ComplexYtPath partition = partitions.get(i);
            final CompletableFuture future = CompletableFuture.supplyAsync(() -> {
                try {
                    return lookupFromTable(keyRow, partition.getFullPath());
                } catch (Exception e) {
                    hasError.set(true);
                    error.set(e);
                }
                return Collections.emptyList();
            });
            futures[i] = future;
        }

        CompletableFuture.allOf(futures).orTimeout(5, TimeUnit.MINUTES).join();
        if (hasError.get()) {
            throw error.get();
        }

        int resultPartitionIndex = -1;
        List<RowData> result = new ArrayList<>();
        for (int i = 0; i < futures.length; i++) {
            Collection<RowData> res = futures[i].get();
            if (!res.isEmpty()) {
                if (resultPartitionIndex != -1) {
                    throw new RuntimeException(String.format("Duplicate key in different partitions: %s and %s",
                            partitions.get(resultPartitionIndex).getTableName(), partitions.get(i).getTableName()));
                }
                resultPartitionIndex = i;
                result.addAll(res);
            }
        }
        return result;
    }

    private Collection<RowData> lookupFromTable(RowData keyRow, String fullTablePath) throws InterruptedException {
        RetryStrategy backoffRetryStrategy = DEFAULT_RETRY_STRATEGY;
        while (backoffRetryStrategy.getNumRemainingRetries() > 0) {
            try {
                List<YTreeMapNode> rows;
                switch (lookupMethod) {
                    case LOOKUP:
                        final LookupRowsRequest.Builder lookupRequest = makeLookupRequest(keyRow, fullTablePath);
                        rows = client.lookupRows(lookupRequest.build()).join().getYTreeRows();
                        break;
                    case SELECT:
                        SelectRowsRequest selectRequest = makeSelectRowsRequest(keyRow, fullTablePath);
                        rows = client.selectRows(selectRequest).join().getYTreeRows();
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown lookup method: " + lookupMethod);
                }

                final Collection<RowData> res = new ArrayList<>();
                for (YTreeMapNode row : rows) {
                    final RowData rowData = deserializeFunction.apply(row);
                    res.add(rowData);
                }
                return res;
            } catch (Exception e) {
                log.error("Unable to lookup data from {}.", fullTablePath, e);
                if (failOnUnavailable
                        && e.getCause() instanceof YTsaurusError
                        && ((YTsaurusError) e.getCause()).isUnrecoverable()
                        && !YtClusterUtils.isAvailable(
                        path.getClusterName().toUpperCase(Locale.ROOT))) {
                    throw e;
                }
                backoffRetryStrategy = backoffRetryStrategy.getNextRetryStrategy();
                if (backoffRetryStrategy.getNumRemainingRetries() == 0) {
                    log.error("Unable to retry lookup '{}' for table {}", keyRow, fullTablePath, e);
                    throw e;
                }
                Thread.sleep(backoffRetryStrategy.getRetryDelay().toMillis());
                log.info("Start retry lookup {} for table {}", keyRow, fullTablePath);
            }
        }
        return Collections.emptyList();
    }

    private LookupRowsRequest.Builder makeLookupRequest(RowData keyRow, String fullTablePath) {
        LookupRowsRequest.Builder requestBuilder = LookupRowsRequest.builder()
                .setPath(fullTablePath)
                .setSchema(schema);
        requestBuilder.addLookupColumns(schema.toValues().getColumnNames());
        List<Object> filter = new ArrayList<>();
        Map<String, Object> ytKeys = (Map<String, Object>) externalConverter.convert(null, keyRow);
        for (int i = 0; i < keyRow.getArity(); i++) {
            filter.add(ytKeys.get(keyNames[i]));
        }
        requestBuilder.addFilter(filter);
        return requestBuilder;
    }

    private SelectRowsRequest makeSelectRowsRequest(RowData keyRow, String fullTablePath) {
        SelectRowsRequest.Builder request = SelectRowsRequest.builder()
                .setQuery(String.format(selectQuery, fullTablePath));
        Map<String, Object> ytKeys = (Map<String, Object>) externalConverter.convert(null, keyRow);

        YTreeBuilder yTreeBuilder = YTree.mapBuilder();
        ytKeys.forEach((k, v) -> {
            yTreeBuilder.key(k);
            yTreeBuilder.value(v);
        });

        request.setPlaceholderValues(yTreeBuilder.buildMap());

        return request.build();
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Unable to close YT client");
            }
        }
        if (executors != null) {
            executors.shutdown();
            try {
                executors.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //ignore
            }
        }
    }
}
