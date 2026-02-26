package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.locks.api.LockConfigOptions;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.cache.LookupCacheBuilder;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.locks.LocksConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.RowTypeUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.TableFactoryPropertyHelper;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.YtDynamicTableSource;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster.ClusterPickStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtDynamicTableSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtWriterOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.TrackableFieldDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.YtPartitioningInstantRowDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CLUSTER_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CLUSTER_PICK_STRATEGY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.COMMIT_TRANSACTION_PERIOD;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.EAGER_INITIALIZATION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.ENABLE_DYNAMIC_STORE_READ;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.FLUSH_MODIFICATION_PERIOD;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.LOOKUP_ASYNC;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.LOOKUP_METHOD;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.MIN_PARTITION_TTL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.MOUNT_MODE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.OPTIMIZE_FOR;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_KEY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_KEY_ALLOWED_LOGIC_TYPE_ROOTS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_SCALE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_TTL_DAY_CNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_TTL_IN_DAYS_FROM_CREATION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH_MAP;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PRIMARY_MEDIUM;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PROXY_ROLE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RESHARD_LAST_PARTITIONS_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RESHARD_STRATEGY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RESHARD_TABLET_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RESHARD_UNIFORM;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RETRY_STRATEGY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.ROWS_IN_MODIFICATION_LIMIT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.ROWS_IN_TRANSACTION_LIMIT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TABLET_CELL_BUNDLE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TIMESTAMP_FORMAT_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TRACKABLE_FIELD;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TRANSACTION_ATOMICITY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TRANSACTION_TIMEOUT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YSON_SCHEMA;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_CUSTOM_ATTRIBUTES;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_TOKEN_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_USERNAME_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes.CUSTOM_ATTRIBUTES_PREFIX;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils.getPathMap;
import static tech.ytsaurus.flyt.locks.api.LockConfigOptions.LOCKS_OPTIONS_PREFIX;

@Slf4j
public class YTsaurusDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "ytsaurus";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryPropertyHelper helper = new TableFactoryPropertyHelper(this, context);
        Configuration options = helper.getOptionsWithoutPropertiesPrefix();

        RowDataToYtListConverters ytConverter = new RowDataToYtListConverters(options.get(TIMESTAMP_FORMAT_OPTION));
        validateOptions(helper);

        TableSchema tableSchema = getAndValidateTableSchema(options);
        DataType type = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        PartitionConfig partitionConfig = getAndValidatePartitioning(tableSchema, context, options);
        CredentialsProvider credentialsProvider = getAndValidateCredentialsProvider(options);
        TrackableField trackableFiled = getAndValidateTrackableField(tableSchema, type, options);
        Integer parallelism = options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);
        SerializableSupplier<RetryStrategy> retryStrategy = getRetryStrategy(options);
        ReshardingConfig reshardingConfig = getReshardingConfig(options);
        ComplexYtPath path = ComplexYtPath.builder()
                .clusterName(options.get(CLUSTER_NAME))
                .basePath(options.get(PATH))
                .isPartitioned(partitionConfig != null)
                .enableDynamicStoreRead(options.get(ENABLE_DYNAMIC_STORE_READ))
                .build();
        log.info("Created YT Resharding config for table '{}' - {}", path.getBasePath(), reshardingConfig);

        YtTableAttributes ytTableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(options.getOptional(OPTIMIZE_FOR).orElse(null))
                .setPrimaryMedium(options.getOptional(PRIMARY_MEDIUM).orElse(null))
                .setTabletCellBundle(options.getOptional(TABLET_CELL_BUNDLE).orElse(null))
                .setCustomAttributes(helper.getOptionValuesWithAdditionalPrefix(CUSTOM_ATTRIBUTES_PREFIX));

        // for compatibility
        options.getOptional(YT_CUSTOM_ATTRIBUTES).ifPresent(ytTableAttributes::setCustomAttributesYson);

        log.info("Created YtTableAttributes for table '{}' - {}", path.getBasePath(), ytTableAttributes);

        YtWriterOptions ytWriterOptions = initYtWriterOptions(options);

        DataMetricsConfig dataMetricsConfig = DataMetricsConfig.fromProperties(helper.getProperties());
        if (dataMetricsConfig != null && !dataMetricsConfig.isEmpty()) {
            log.info("Configuring data metrics with {} metrics for alias: {}",
                    dataMetricsConfig.getMetrics().size(), dataMetricsConfig.getMetricTablePathAlias());
        }

        return YtDynamicTableSink.builder()
                .type(type)
                .ytConverters(ytConverter)
                .path(path)
                .trackableField(trackableFiled)
                .ysonSchemaString(options.get(YSON_SCHEMA))
                .tableAttributes(ytTableAttributes)
                .credentialsProvider(credentialsProvider)
                .partitionConfig(partitionConfig)
                .parallelism(parallelism)
                .retryStrategy(retryStrategy)
                .eagerInitialization(options.get(EAGER_INITIALIZATION))
                .reshardingConfig(reshardingConfig)
                .ytWriterOptions(ytWriterOptions)
                .dataMetricsConfig(dataMetricsConfig)
                .build();
    }

    private YtWriterOptions initYtWriterOptions(ReadableConfig options) {
        YtWriterOptions.YtWriterOptionsBuilder builder = YtWriterOptions.builder();

        options.getOptional(COMMIT_TRANSACTION_PERIOD).ifPresent(builder::commitTransactionPeriod);
        options.getOptional(FLUSH_MODIFICATION_PERIOD).ifPresent(builder::flushModificationPeriod);
        options.getOptional(TRANSACTION_TIMEOUT).ifPresent(builder::transactionTimeout);
        options.getOptional(ROWS_IN_TRANSACTION_LIMIT).ifPresent(builder::rowsInTransactionLimit);
        options.getOptional(ROWS_IN_MODIFICATION_LIMIT).ifPresent(builder::rowsInModificationLimit);
        options.getOptional(MOUNT_MODE).ifPresent(builder::mountMode);
        options.getOptional(PROXY_ROLE).ifPresent(builder::proxyRole);
        options.getOptional(TRANSACTION_ATOMICITY).ifPresent(builder::atomicity);

        //locks
        String locksProvider = options.get(LockConfigOptions.LOCKS_PROVIDER);
        LocksConfig locksConfig = new LocksConfig(locksProvider, options);
        builder.lockConfig(locksConfig);

        return builder.build();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryPropertyHelper helper = new TableFactoryPropertyHelper(this, context);
        Configuration options = helper.getOptionsWithoutPropertiesPrefix();

        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        validateOptions(helper);

        TableSchema tableSchema = getAndValidateTableSchema(options);
        PartitionConfig partitionConfig = getAndValidatePartitioning(tableSchema, context, options);
        CredentialsProvider credentialsProvider = getAndValidateCredentialsProvider(options);
        LookupCacheBuilder cacheBuilder = getLookupCacheBuilder(options);
        ClusterPickStrategy clusterPickStrategy = getClusterPickStrategy(options);
        Map<String, ComplexYtPath> pathMap = getPathMap(options);

        return YtDynamicTableSource.builder()
                .type(context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType())
                .converter(valueDecodingFormat)
                .pathMap(pathMap)
                .clusterPickStrategy(clusterPickStrategy)
                .ysonSchemaString(options.get(YSON_SCHEMA))
                .clusterName(options.get(CLUSTER_NAME))
                .credentialsProvider(credentialsProvider)
                .physicalRowDataType(context.getPhysicalRowDataType())
                .limit(-1)
                .cacheType(options.get(LookupOptions.CACHE_TYPE))
                .cacheBuilder(cacheBuilder)
                .partitionConfig(partitionConfig)
                .asyncLookup(options.get(LOOKUP_ASYNC))
                .lookupMethod(options.get(LOOKUP_METHOD))
                .options(options)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(YSON_SCHEMA,
                CREDENTIALS_SOURCE,
                CLUSTER_NAME);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                PARTITION_KEY,
                PARTITION_SCALE,
                YT_USERNAME_OPTION,
                YT_TOKEN_OPTION,
                ENABLE_DYNAMIC_STORE_READ,
                TIMESTAMP_FORMAT_OPTION,
                TRACKABLE_FIELD,
                EAGER_INITIALIZATION,
                LOOKUP_ASYNC,
                PARTITION_TTL_DAY_CNT,
                PARTITION_TTL_IN_DAYS_FROM_CREATION,
                MIN_PARTITION_TTL,
                OPTIMIZE_FOR,
                PRIMARY_MEDIUM,
                RETRY_STRATEGY,
                RESHARD_STRATEGY,
                RESHARD_LAST_PARTITIONS_COUNT,
                RESHARD_TABLET_COUNT,
                TABLET_CELL_BUNDLE,
                RESHARD_UNIFORM,
                LOOKUP_METHOD,
                PATH,
                PATH_MAP,
                CLUSTER_PICK_STRATEGY,
                COMMIT_TRANSACTION_PERIOD,
                FLUSH_MODIFICATION_PERIOD,
                TRANSACTION_TIMEOUT,
                ROWS_IN_MODIFICATION_LIMIT,
                ROWS_IN_TRANSACTION_LIMIT,
                MOUNT_MODE,
                PROXY_ROLE,
                TRANSACTION_ATOMICITY,
                YT_CUSTOM_ATTRIBUTES,
                FactoryUtil.SINK_PARALLELISM,
                LookupOptions.CACHE_TYPE,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                LookupOptions.FULL_CACHE_RELOAD_STRATEGY,
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL,
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE,
                LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME,
                LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS
        );
    }

    protected void validateOptions(FactoryUtil.FactoryHelper<DynamicTableFactory> helper) {
        helper.validateExcept(LOCKS_OPTIONS_PREFIX, CLUSTER_PICK_STRATEGY.key());
    }

    protected CredentialsProvider getAndValidateCredentialsProvider(Configuration options) {
        return YtConfigUtils.getAndValidateCredentialsProvider(options);
    }

    private TableSchema getAndValidateTableSchema(ReadableConfig options) {
        // Additional transformation and validation for the raw YSON string
        YTreeNode ysonSchema = YTreeTextSerializer.deserialize(options.get(YSON_SCHEMA));
        if (ysonSchema == null || !ysonSchema.isListNode()) {
            throw new ValidationException("'schema' option is required and must be a YSON list: " +
                    "see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema");
        }
        try {
            return TableSchema.fromYTree(ysonSchema);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(
                    "Unable to construct table schema from YSON: option 'schema': " + e.getMessage(), e);
        }
    }

    private PartitionConfig getAndValidatePartitioning(TableSchema tableSchema,
                                                       Context context,
                                                       ReadableConfig options) {
        // Additional transformation and validation for partitioning
        String partitionKey = options.get(PARTITION_KEY);
        PartitionScale partitionScale = options.get(PARTITION_SCALE);
        if ((partitionKey != null) ^ (partitionScale != null)) {
            throw new IllegalArgumentException("Both 'partitionKey' and 'partitionScale' options must be specified");
        }
        if (partitionKey == null) {
            return null;
        }

        int columnIndex = tableSchema.findColumn(partitionKey);
        if (columnIndex == -1) {
            throw new IllegalArgumentException(
                    String.format("Partition key '%s' is not found in provided YSON schema", partitionKey));
        }

        Column partitionColumn = context.getCatalogTable()
                .getResolvedSchema()
                .getColumn(partitionKey)
                .orElseThrow(() -> new ValidationException(
                        String.format("Partition key '%s' is not found in provided Flink schema", partitionKey)));

        LogicalType partitionKeyLogicalType = partitionColumn.getDataType().getLogicalType();
        if (!PARTITION_KEY_ALLOWED_LOGIC_TYPE_ROOTS.contains(partitionKeyLogicalType.getTypeRoot())) {
            throw new IllegalArgumentException(String.format(
                    "Partition key %s '%s' must be one of the following logical types: %s",
                    partitionKeyLogicalType.getTypeRoot(),
                    partitionKey,
                    PARTITION_KEY_ALLOWED_LOGIC_TYPE_ROOTS.stream()
                            .map(Enum::name)
                            .collect(Collectors.joining(", "))
            ));
        }

        Integer partitionTtlDayCnt = options.get(PARTITION_TTL_DAY_CNT);
        Integer partitionTtlInDaysFromCreation = options.get(PARTITION_TTL_IN_DAYS_FROM_CREATION);
        Integer minPartitionTtl = options.get(MIN_PARTITION_TTL);

        // If we have partitioning configured correctly, fill in the config
        return new PartitionConfig(
                partitionKey,
                partitionScale,
                partitionTtlDayCnt,
                partitionTtlInDaysFromCreation,
                minPartitionTtl,
                new YtPartitioningInstantRowDataConverter(partitionKeyLogicalType));
    }


    private TrackableField getAndValidateTrackableField(TableSchema tableSchema,
                                                        DataType type, ReadableConfig options) {
        String trackableField = options.get(TRACKABLE_FIELD);
        if (trackableField != null) {
            int columnIndex = tableSchema.findColumn(trackableField);
            if (columnIndex == -1) {
                throw new IllegalArgumentException(String.format(
                        "Unable to find trackable field '%s' in YT table schema %s",
                        trackableField,
                        tableSchema.getColumnNames()
                ));
            }
            final RowType logicalType = (RowType) type.getLogicalType();
            final String[] fieldNames = RowTypeUtils.getFieldNames(logicalType);
            final LogicalType[] fieldTypes = RowTypeUtils.getFieldTypes(logicalType);
            LogicalType trackableFieldType = null;
            for (int i = 0; i < fieldNames.length; i++) {
                if (trackableField.equals(fieldNames[i])) {
                    trackableFieldType = fieldTypes[i];
                    break;
                }
            }
            if (trackableFieldType == null) {
                throw new IllegalArgumentException(String.format(
                        "Unable to find trackable field '%s' in Flink table schema %s",
                        trackableField,
                        Arrays.toString(fieldNames)
                ));
            }
            return new TrackableField(
                    trackableField,
                    RowTypeUtils.findColumnByName(logicalType, trackableField),
                    trackableFieldType.getTypeRoot(),
                    new TrackableFieldDataConverter(trackableFieldType)
            );
        }
        return null;
    }

    private ClusterPickStrategy getClusterPickStrategy(ReadableConfig options) {
        return ClusterPickStrategy.loadOrThrow(options.get(CLUSTER_PICK_STRATEGY));
    }

    private LookupCacheBuilder getLookupCacheBuilder(ReadableConfig options) {
        LookupCacheBuilder cacheBuilder = null;
        if (options.get(LOOKUP_ASYNC)) {
            if (options
                    .get(LookupOptions.CACHE_TYPE)
                    .equals(LookupOptions.LookupCacheType.PARTIAL)) {
                cacheBuilder = (lookupFunction, ignore) -> PartialCachingAsyncLookupProvider.of(
                        (AsyncLookupFunction) lookupFunction,
                        DefaultLookupCache.fromConfig(options)
                );
            } else if (options
                    .get(LookupOptions.CACHE_TYPE)
                    .equals(LookupOptions.LookupCacheType.FULL)) {
                throw new IllegalStateException("Async cache must not be FULL");
            }
        } else {
            if (options
                    .get(LookupOptions.CACHE_TYPE)
                    .equals(LookupOptions.LookupCacheType.PARTIAL)) {
                cacheBuilder = (lookupFunction, ignore) -> PartialCachingLookupProvider.of(
                        (LookupFunction) lookupFunction,
                        DefaultLookupCache.fromConfig(options)
                );
            } else if (options
                    .get(LookupOptions.CACHE_TYPE)
                    .equals(LookupOptions.LookupCacheType.FULL)) {
                final LookupOptions.ReloadStrategy reloadStrategyType
                        = options.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY);
                CacheReloadTrigger trigger;
                if (reloadStrategyType == null) {
                    throw new IllegalArgumentException("lookup.full-cache.reload-strategy is null");
                } else if (reloadStrategyType.equals(LookupOptions.ReloadStrategy.PERIODIC)) {
                    trigger = PeriodicCacheReloadTrigger.fromConfig(options);
                } else if (reloadStrategyType.equals(LookupOptions.ReloadStrategy.TIMED)) {
                    trigger = TimedCacheReloadTrigger.fromConfig(options);
                } else {
                    throw new IllegalArgumentException("Unknown full cache strategy: " + reloadStrategyType);
                }

                cacheBuilder = (lookupFunction, scanRuntimeProvider) -> new FullCachingLookupProvider() {
                    @Override
                    public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider() {
                        return scanRuntimeProvider;
                    }

                    @Override
                    public CacheReloadTrigger getCacheReloadTrigger() {
                        return trigger;
                    }

                    @Override
                    public LookupFunction createLookupFunction() {
                        return (LookupFunction) lookupFunction;
                    }
                };
            }
        }
        return cacheBuilder;
    }

    private SerializableSupplier<RetryStrategy> getRetryStrategy(ReadableConfig options) {
        tech.ytsaurus.flyt.connectors.ytsaurus.common.RetryStrategy retryStrategy = options.get(RETRY_STRATEGY);
        switch (retryStrategy) {
            case EXPONENTIAL:
                return () -> new ExponentialBackoffRetryStrategy(
                        5,
                        Duration.of(10, ChronoUnit.SECONDS),
                        Duration.of(2, ChronoUnit.MINUTES)
                );
            case NO_RETRY:
                return () -> new FixedRetryStrategy(0, Duration.ZERO);
            default:
                throw new IllegalArgumentException("Unknown retry strategy type: " + retryStrategy);
        }
    }

    private ReshardingConfig getReshardingConfig(ReadableConfig options) {
        ReshardStrategy reshardStrategy = options.get(RESHARD_STRATEGY);
        if (reshardStrategy == ReshardStrategy.NONE) {
            return ReshardingConfig.none();
        }
        int tabletCount = options.getOptional(RESHARD_TABLET_COUNT)
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Option '%s' required for reshard strategy '%s'.", RESHARD_TABLET_COUNT.key(),
                        reshardStrategy.name())));
        int lastPartitionsCount = options.get(RESHARD_LAST_PARTITIONS_COUNT);
        if (!options.get(RESHARD_UNIFORM)) {
            throw new UnsupportedOperationException("Only uniform partitioning is available");
        }
        return ReshardingConfig.builder()
                .reshardStrategy(reshardStrategy)
                .tabletCount(tabletCount)
                .lastPartitionsCount(lastPartitionsCount)
                .uniform(true)
                .build();
    }
}

