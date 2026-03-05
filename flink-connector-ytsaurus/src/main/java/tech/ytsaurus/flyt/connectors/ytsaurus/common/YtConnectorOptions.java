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

    public static final ConfigOption<String> PARTITION_KEY = ConfigOptions.key("partition-key")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("partitionKey");

    public static final ConfigOption<PartitionScale> PARTITION_SCALE = ConfigOptions.key("partition-scale")
            .enumType(PartitionScale.class)
            .noDefaultValue()
            .withDeprecatedKeys("partitionScale");

    public static final ConfigOption<String> PROXY = ConfigOptions.key("proxy")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("clusterName");

    public static final ConfigOption<Map<String, String>> PATH_MAP = ConfigOptions.key("path-map")
            .mapType()
            .noDefaultValue()
            .withDescription("Map of cluster-to-table-path. Useful when querying multiple clusters")
            .withDeprecatedKeys("pathMap");

    public static final ConfigOption<String> CREDENTIALS_SOURCE = ConfigOptions.key("credentials-source")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("credentialsSource");

    public static final ConfigOption<String> YT_USERNAME_OPTION = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("ytUsername");

    public static final ConfigOption<String> YT_TOKEN_OPTION = ConfigOptions.key("token")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("ytToken");

    public static final ConfigOption<String> TRACKABLE_FIELD = ConfigOptions.key("trackable-field")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("trackableField");

    public static final ConfigOption<TimestampFormat> TIMESTAMP_FORMAT_OPTION = ConfigOptions.key("timestamp-format")
            .enumType(TimestampFormat.class)
            .defaultValue(TimestampFormat.ISO_8601)
            .withDeprecatedKeys("timestampFormat");

    public static final ConfigOption<Boolean> EAGER_INITIALIZATION = ConfigOptions.key("eager-initialization")
            .booleanType()
            .defaultValue(true)
            .withDeprecatedKeys("eagerInitialization");

    /**
     * Set enable_dynamic_store_read attribute for dynamic table after mount
     *
     * @see <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/mapreduce#running_operations_over_dyn_tables">YT Documentation</a>
     */
    public static final ConfigOption<Boolean> ENABLE_DYNAMIC_STORE_READ = ConfigOptions.key("enable-dynamic-store-read")
            .booleanType()
            .defaultValue(true)
            .withDeprecatedKeys("enableDynamicStoreRead");

    public static final ConfigOption<Integer> PARTITION_TTL_DAY_CNT = ConfigOptions.key("partition-ttl-day-cnt")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("partitionTtlDayCnt");

    public static final ConfigOption<Integer> PARTITION_TTL_IN_DAYS_FROM_CREATION = ConfigOptions
            .key("partition-ttl-in-days-from-creation")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("partitionTtlInDaysFromCreation");

    public static final ConfigOption<Integer> MIN_PARTITION_TTL = ConfigOptions
            .key("min-partition-ttl")
            .intType()
            .defaultValue(20)
            .withDeprecatedKeys("minPartitionTtl");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup.");

    public static final ConfigOption<OptimizeFor> OPTIMIZE_FOR = ConfigOptions.key("optimize-for")
            .enumType(OptimizeFor.class)
            .noDefaultValue()
            .withDeprecatedKeys("optimizeFor");

    public static final ConfigOption<PrimaryMedium> PRIMARY_MEDIUM = ConfigOptions.key("primary-medium")
            .enumType(PrimaryMedium.class)
            .noDefaultValue()
            .withDeprecatedKeys("primaryMedium");

    public static final ConfigOption<RetryStrategy> RETRY_STRATEGY = ConfigOptions.key("retry-strategy")
            .enumType(RetryStrategy.class)
            .defaultValue(RetryStrategy.EXPONENTIAL)
            .withDeprecatedKeys("retryStrategy");

    public static final ConfigOption<String> TABLET_CELL_BUNDLE = ConfigOptions.key("tablet-cell-bundle")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("tabletCellBundle");

    public static final ConfigOption<Integer> RESHARD_TABLET_COUNT = ConfigOptions.key("reshard.tablet-count")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("reshard.tabletCount");

    public static final ConfigOption<Boolean> RESHARD_UNIFORM = ConfigOptions.key("reshard.uniform")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<ReshardStrategy> RESHARD_STRATEGY = ConfigOptions.key("reshard.strategy")
            .enumType(ReshardStrategy.class)
            .defaultValue(ReshardStrategy.NONE);

    public static final ConfigOption<Integer> RESHARD_LAST_PARTITIONS_COUNT =
            ConfigOptions.key("reshard.last-partitions-count")
                    .intType()
                    .defaultValue(7) // days in a week (daily partitions are the most typical)
                    .withDescription("Number of partitions to consider in LAST_PARTITIONS reshard strategy.")
                    .withDeprecatedKeys("reshard.lastPartitionsCount");

    public static final ConfigOption<LookupMethod> LOOKUP_METHOD = ConfigOptions.key("lookup-method")
            .enumType(LookupMethod.class)
            .defaultValue(LookupMethod.LOOKUP)
            .withDeprecatedKeys("lookupMethod");

    public static final ConfigOption<String> CLUSTER_PICK_STRATEGY = ConfigOptions.key("cluster-pick-strategy")
            .stringType()
            .defaultValue(FirstAvailableClusterPickStrategy.NAME)
            .withDeprecatedKeys("clusterPickStrategy");

    public static final ConfigOption<Duration> COMMIT_TRANSACTION_PERIOD = ConfigOptions.key("commit-transaction-period")
            .durationType()
            .noDefaultValue()
            .withDeprecatedKeys("commitTransactionPeriod");

    public static final ConfigOption<Duration> FLUSH_MODIFICATION_PERIOD = ConfigOptions.key("flush-modification-period")
            .durationType()
            .noDefaultValue()
            .withDeprecatedKeys("flushModificationPeriod");

    public static final ConfigOption<Duration> TRANSACTION_TIMEOUT = ConfigOptions.key("transaction-timeout")
            .durationType()
            .noDefaultValue()
            .withDeprecatedKeys("transactionTimeout");

    public static final ConfigOption<Integer> ROWS_IN_MODIFICATION_LIMIT = ConfigOptions.key("rows-in-modification-limit")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("rowsInModificationLimit");

    public static final ConfigOption<Integer> ROWS_IN_TRANSACTION_LIMIT = ConfigOptions.key("rows-in-transaction-limit")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("rowsInTransactionLimit");

    public static final ConfigOption<MountMode> MOUNT_MODE = ConfigOptions.key("mount-mode")
            .enumType(MountMode.class)
            .defaultValue(MountMode.ALWAYS)
            .withDeprecatedKeys("mountMode");

    public static final ConfigOption<Atomicity> TRANSACTION_ATOMICITY = ConfigOptions.key("transaction-atomicity")
            .enumType(Atomicity.class)
            .noDefaultValue()
            .withDeprecatedKeys("transactionAtomicity");

    public static final ConfigOption<String> YT_CUSTOM_ATTRIBUTES =
            ConfigOptions
                    .key("custom-attributes")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("customAttributes");

    public static final ConfigOption<String> PROXY_ROLE = ConfigOptions.key("proxy-role")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("proxyRole");
}
