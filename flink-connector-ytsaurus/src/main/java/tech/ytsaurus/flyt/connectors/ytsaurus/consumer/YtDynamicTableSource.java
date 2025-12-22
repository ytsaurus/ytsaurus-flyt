package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.LookupMethod;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.cache.LookupCacheBuilder;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster.ClusterPickStrategy;

@Builder
@Slf4j
public class YtDynamicTableSource
        implements
        ScanTableSource,
        LookupTableSource,
        SupportsProjectionPushDown,
        SupportsLimitPushDown {

    private final DataType type;

    private LookupOptions.LookupCacheType cacheType;

    @Nullable
    private final LookupCacheBuilder cacheBuilder;

    private PartitionConfig partitionConfig;

    private CredentialsProvider credentialsProvider;

    private final String clusterName;

    private final Map<String, ComplexYtPath> pathMap;

    private final ClusterPickStrategy clusterPickStrategy;

    private final String ysonSchemaString;

    private DataType physicalRowDataType;

    private DecodingFormat<DeserializationSchema<RowData>> converter;

    private long limit;

    private boolean asyncLookup;

    private LookupMethod lookupMethod;

    private ReadableConfig options;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        DataType[] keyTypes = new DataType[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "YT only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
            keyTypes[i] = DataType.getFieldDataTypes(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        for (ComplexYtPath path : pathMap.values()) {
            if (!path.isPartitioned()) {
                path.setTableName(path.getBaseTableName());
            }
        }
        Preconditions.checkNotNull(
                converter, "Value decoding format must not be null.");
        final CredentialsProvider finalCredentialsProvider = credentialsProvider;
        final DeserializationSchema<RowData> runtimeDecoder = converter.createRuntimeDecoder(context,
                physicalRowDataType);
        final String finalYsonSchema = ysonSchemaString;
        final LookupMethod finalLookupMethod = lookupMethod;
        final PartitionConfig finalPartitionConfig = partitionConfig;
        final String[] fieldNames = DataType.getFieldNames(physicalRowDataType).toArray(new String[0]);
        final DataType[] fieldTypes = DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]);
        final Map<String, ComplexYtPath> finalPathMap = pathMap;
        // TODO: maybe bring to options later, rn needed just to pass
        //       unavailability exception to multicluster lookup function
        final boolean failOnUnavailable = pathMap.size() > 1;
        final SerializableFunction<ComplexYtPath, YtRowDataLookupFunction> function =
                path -> new YtRowDataLookupFunction(
                        finalCredentialsProvider,
                        finalYsonSchema,
                        path,
                        finalLookupMethod,
                        finalPartitionConfig,
                        runtimeDecoder,
                        fieldNames,
                        fieldTypes,
                        keyNames,
                        keyTypes,
                        rowType,
                        failOnUnavailable);
        LookupFunction lookupFunction;
        if (pathMap.size() == 1) {
            lookupFunction = function.apply(pathMap.values().iterator().next());
        } else {
            Preconditions.checkNotNull(clusterPickStrategy,
                    "Strategy must be present when given multiple paths");
            log.info("Using {} strategy for lookup: {}", clusterPickStrategy.getName(), compilePathName());
            lookupFunction = new YtRowDataMulticlusterLookupFunction(
                    clusterPickStrategy,
                    cluster -> function.apply(finalPathMap.get(cluster)),
                    options);
        }
        if (asyncLookup) {
            if (cacheType == LookupOptions.LookupCacheType.NONE) {
                log.info("No async lookup cache for table {}", compilePathName());
                return AsyncLookupFunctionProvider.of(new YtRowDataAsyncLookupFunction(lookupFunction));
            } else if (cacheType == LookupOptions.LookupCacheType.FULL) {
                throw new IllegalStateException("Async cache must not be FULL");
            } else if (cacheType == LookupOptions.LookupCacheType.PARTIAL) {
                log.info("Enable PARTIAL async lookup cache for table {}", compilePathName());
                return cacheBuilder.apply(new YtRowDataAsyncLookupFunction(lookupFunction), null);
            }
        } else {
            if (cacheType == LookupOptions.LookupCacheType.NONE) {
                log.info("No lookup cache for table {}", compilePathName());
                return LookupFunctionProvider.of(lookupFunction);
            } else if (cacheType == LookupOptions.LookupCacheType.FULL) {
                log.info("Enable FULL lookup cache for table {}", compilePathName());
                return cacheBuilder.apply(lookupFunction, getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE));
            } else if (cacheType == LookupOptions.LookupCacheType.PARTIAL) {
                log.info("Enable PARTIAL lookup cache for table {}", compilePathName());
                return cacheBuilder.apply(lookupFunction, null);
            }
        }
        throw new IllegalStateException("Unknown cache type: " + cacheType);
    }

    @Override
    public DynamicTableSource copy() {
        return YtDynamicTableSource.builder()
                .type(type)
                .credentialsProvider(credentialsProvider)
                .clusterName(clusterName)
                .pathMap(pathMap)
                .ysonSchemaString(ysonSchemaString)
                .physicalRowDataType(physicalRowDataType)
                .converter(converter)
                .limit(limit)
                .cacheType(cacheType)
                .cacheBuilder(cacheBuilder)
                .asyncLookup(asyncLookup)
                .lookupMethod(lookupMethod)
                .options(options)
                .clusterPickStrategy(clusterPickStrategy)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "YT";
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final YtRowDataInputFormat.Builder builder =
                YtRowDataInputFormat.builder()
                        .setYsonSchemaString(ysonSchemaString)
                        .setCredentialsProvider(credentialsProvider);
        ComplexYtPath path;
        if (pathMap.size() == 1) {
            path = pathMap.values().iterator().next();
        } else {
            Preconditions.checkNotNull(clusterPickStrategy,
                    "Strategy must be present when given multiple paths");
            clusterPickStrategy.open(options);
            log.info("Using {} strategy for scan: {}", clusterPickStrategy.getName(), compilePathName());
            String ytCluster = clusterPickStrategy.pickCluster();
            path = pathMap.get(ytCluster);
            log.info("Strategy {} for {} scan picked cluster {}, so path is: {}",
                    clusterPickStrategy.getName(),
                    compilePathName(),
                    ytCluster,
                    path);
        }
        if (path.isPartitioned()) {
//            It will throw in runtime to fix lookup initialization
//            throw new UnsupportedOperationException("Partition table unsupported");
            log.warn("Ignore partition error for scan table");
        } else {
            path.setTableName(path.getBaseTableName());
        }
        builder.setPath(path);

        Preconditions.checkNotNull(
                converter, "Value decoding format must not be null.");

        builder.setDeserializer(converter.createRuntimeDecoder(runtimeProviderContext, physicalRowDataType));
        builder.setLimit(limit);
        log.info("Limit = {}", limit);
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalRowDataType));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YtDynamicTableSource)) {
            return false;
        }
        YtDynamicTableSource that = (YtDynamicTableSource) o;
        return Objects.equals(clusterName, that.clusterName)
                && Objects.equals(pathMap, that.pathMap)
                && Objects.equals(cacheBuilder, that.cacheBuilder)
                && Objects.equals(cacheType, that.cacheType)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(ysonSchemaString, that.ysonSchemaString)
                && Objects.equals(type, that.type)
                && Objects.equals(asyncLookup, that.asyncLookup)
                && Objects.equals(lookupMethod, that.lookupMethod)
                && Objects.equals(limit, that.limit)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                clusterName,
                pathMap,
                cacheType,
                cacheBuilder,
                physicalRowDataType,
                ysonSchemaString,
                type,
                asyncLookup,
                lookupMethod,
                limit,
                options);
    }

    @Override
    public void applyLimit(long limit) {
        log.info("Apply limit: {}", limit);
        this.limit = limit;
    }

    private String compilePathName() {
        return pathMap.entrySet().stream()
                .map(entry ->
                        entry.getKey() + ":" + entry.getValue().getBasePath())
                .collect(Collectors.joining(", "));
    }
}
