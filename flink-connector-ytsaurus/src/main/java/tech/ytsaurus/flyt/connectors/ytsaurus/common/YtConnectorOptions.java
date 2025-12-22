package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import tech.ytsaurus.client.request.Atomicity;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster.FirstAvailableClusterPickStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.MountMode;

@UtilityClass
public class YtConnectorOptions {

    public static final List<LogicalTypeRoot> PARTITION_KEY_ALLOWED_LOGIC_TYPE_ROOTS = List.of(
            LogicalTypeRoot.DATE,
            LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    public static final ConfigOption<String> PATH = ConfigOptions.key("path")
            .stringType()
            .noDefaultValue();

    /**
     * Schema for the YT dynamic table which comes in the form of a serialized YSON list
     *
     * @see <a href="https://ytsaurus.tech/docs/en/user-guide/storage/static-schema>YT Documentation</a>
     */
    public static final ConfigOption<String> YSON_SCHEMA = ConfigOptions.key("schema")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PARTITION_KEY = ConfigOptions.key("partitionKey")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<PartitionScale> PARTITION_SCALE = ConfigOptions.key("partitionScale")
            .enumType(PartitionScale.class)
            .noDefaultValue();

    public static final ConfigOption<String> CLUSTER_NAME = ConfigOptions.key("clusterName")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Map<String, String>> PATH_MAP = ConfigOptions.key("pathMap")
            .mapType()
            .noDefaultValue()
            .withDescription("Map of cluster-to-table-path. Useful when querying multiple clusters");

    public static final ConfigOption<String> CREDENTIALS_SOURCE = ConfigOptions.key("credentialsSource")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> YT_USERNAME_OPTION = ConfigOptions.key("ytUsername")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> YT_TOKEN_OPTION = ConfigOptions.key("ytToken")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> TRACKABLE_FIELD = ConfigOptions.key("trackableField")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<TimestampFormat> TIMESTAMP_FORMAT_OPTION = ConfigOptions.key("timestampFormat")
            .enumType(TimestampFormat.class)
            .defaultValue(TimestampFormat.ISO_8601);

    public static final ConfigOption<Boolean> EAGER_INITIALIZATION = ConfigOptions.key("eagerInitialization")
            .booleanType()
            .defaultValue(true);

    /**
     * Set enable_dynamic_store_read attribute for dynamic table after mount
     *
     * @see <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/mapreduce#running_operations_over_dyn_tables">YT Documentation</a>
     */
    public static final ConfigOption<Boolean> ENABLE_DYNAMIC_STORE_READ = ConfigOptions.key("enableDynamicStoreRead")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Integer> PARTITION_TTL_DAY_CNT = ConfigOptions.key("partitionTtlDayCnt")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PARTITION_TTL_IN_DAYS_FROM_CREATION = ConfigOptions
            .key("partitionTtlInDaysFromCreation")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> MIN_PARTITION_TTL = ConfigOptions
            .key("minPartitionTtl")
            .intType()
            .defaultValue(20);

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup.");

    public static final ConfigOption<OptimizeFor> OPTIMIZE_FOR = ConfigOptions.key("optimizeFor")
            .enumType(OptimizeFor.class)
            .noDefaultValue();

    public static final ConfigOption<PrimaryMedium> PRIMARY_MEDIUM = ConfigOptions.key("primaryMedium")
            .enumType(PrimaryMedium.class)
            .noDefaultValue();

    public static final ConfigOption<RetryStrategy> RETRY_STRATEGY = ConfigOptions.key("retryStrategy")
            .enumType(RetryStrategy.class)
            .defaultValue(RetryStrategy.EXPONENTIAL);

    public static final ConfigOption<String> TABLET_CELL_BUNDLE = ConfigOptions.key("tabletCellBundle")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> RESHARD_TABLET_COUNT = ConfigOptions.key("reshard.tabletCount")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Boolean> RESHARD_UNIFORM = ConfigOptions.key("reshard.uniform")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<ReshardStrategy> RESHARD_STRATEGY = ConfigOptions.key("reshard.strategy")
            .enumType(ReshardStrategy.class)
            .defaultValue(ReshardStrategy.NONE);

    public static final ConfigOption<Integer> RESHARD_LAST_PARTITIONS_COUNT =
            ConfigOptions.key("reshard.lastPartitionsCount")
                    .intType()
                    .defaultValue(7) // days in a week (daily partitions are the most typical)
                    .withDescription("Number of partitions to consider in LAST_PARTITIONS reshard strategy.");

    public static final ConfigOption<LookupMethod> LOOKUP_METHOD = ConfigOptions.key("lookupMethod")
            .enumType(LookupMethod.class)
            .defaultValue(LookupMethod.LOOKUP);

    public static final ConfigOption<String> CLUSTER_PICK_STRATEGY = ConfigOptions.key("clusterPickStrategy")
            .stringType()
            .defaultValue(FirstAvailableClusterPickStrategy.NAME);

    public static final ConfigOption<Duration> COMMIT_TRANSACTION_PERIOD = ConfigOptions.key("commitTransactionPeriod")
            .durationType()
            .noDefaultValue();

    public static final ConfigOption<Duration> FLUSH_MODIFICATION_PERIOD = ConfigOptions.key("flushModificationPeriod")
            .durationType()
            .noDefaultValue();

    public static final ConfigOption<Duration> TRANSACTION_TIMEOUT = ConfigOptions.key("transactionTimeout")
            .durationType()
            .noDefaultValue();

    public static final ConfigOption<Integer> ROWS_IN_MODIFICATION_LIMIT = ConfigOptions.key("rowsInModificationLimit")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> ROWS_IN_TRANSACTION_LIMIT = ConfigOptions.key("rowsInTransactionLimit")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<MountMode> MOUNT_MODE = ConfigOptions.key("mountMode")
            .enumType(MountMode.class)
            .defaultValue(MountMode.ALWAYS);

    public static final ConfigOption<Atomicity> TRANSACTION_ATOMICITY = ConfigOptions.key("transactionAtomicity")
            .enumType(Atomicity.class)
            .noDefaultValue();

    public static final ConfigOption<String> YT_CUSTOM_ATTRIBUTES =
            ConfigOptions
                    .key("customAttributes")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> PROXY_ROLE = ConfigOptions.key("proxyRole")
            .stringType()
            .noDefaultValue();
}
