package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.TableFactoryPropertyHelper;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtStaticDynamicTableSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.YtStaticSink;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.TrackableFieldDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PROXY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PROXY_ROLE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TIMESTAMP_FORMAT_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.TRACKABLE_FIELD;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.USE_TLS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YSON_SCHEMA;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_TOKEN_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_USERNAME_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtStaticConnectorOptions.ROLL_INTERVAL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtStaticConnectorOptions.WRITE_TIMEOUT;

/** SQL/Flow adapter; subclasses may provide application-specific credential providers. */
public class YTsaurusStaticTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "ytsaurus-static";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryPropertyHelper helper = new TableFactoryPropertyHelper(this, context);
        helper.validateExcept(validationExcludedPrefixes().toArray(new String[0]));
        Configuration options = helper.getOptionsWithoutPropertiesPrefix();
        RowType rowType = (RowType) context.getCatalogTable().getResolvedSchema()
                .toPhysicalRowDataType().getLogicalType();
        Integer parallelism = options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);
        if (parallelism != null && parallelism < 1) {
            throw new ValidationException("sink.parallelism must be positive");
        }
        if (!context.getCatalogTable().getPartitionKeys().isEmpty()) {
            throw new ValidationException("Static YT sink writes exactly one table and does not support partitions");
        }
        Map<String, String> attributes = helper.getEffectiveOptionValuesWithPrefix(
                YtTableAttributes.CUSTOM_ATTRIBUTES_PREFIX);
        try {
            YtStaticSink.Builder<org.apache.flink.table.data.RowData> builder = YtStaticSink
                    .forRowData(rowType, options.get(YSON_SCHEMA))
                    .setProxy(options.get(PROXY))
                    .setPath(options.get(PATH))
                    .setCredentialsProvider(getAndValidateCredentialsProvider(options))
                    .setTimestampFormat(options.get(TIMESTAMP_FORMAT_OPTION))
                    .setProxyRole(options.getOptional(PROXY_ROLE).orElse(null))
                    .setUseTls(options.get(USE_TLS))
                    .setRollInterval(options.get(ROLL_INTERVAL))
                    .setWriteTimeout(options.get(WRITE_TIMEOUT))
                    .setCustomAttributes(attributes)
                    .setDataMetricsConfig(DataMetricsConfig.fromConfiguration(options));
            options.getOptional(TRACKABLE_FIELD).ifPresent(name -> {
                int index = rowType.getFieldIndex(name);
                if (index < 0) {
                    throw new ValidationException("Trackable field not found in Flink schema: " + name);
                }
                switch (rowType.getTypeAt(index).getTypeRoot()) {
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        break;
                    default:
                        throw new ValidationException("trackable-field must be a TIMESTAMP column");
                }
                builder.setTrackableField(new TrackableField(name, index, rowType.getTypeAt(index).getTypeRoot(),
                        new TrackableFieldDataConverter(rowType.getTypeAt(index))));
            });
            return new YtStaticDynamicTableSink(builder.build(), parallelism);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new ValidationException("Invalid static YT sink configuration: " + e.getMessage(), e);
        }
    }

    protected CredentialsProvider getAndValidateCredentialsProvider(Configuration options) {
        return YtConfigUtils.getAndValidateCredentialsProvider(options);
    }

    protected List<String> validationExcludedPrefixes() {
        return new ArrayList<>(List.of(YtTableAttributes.CUSTOM_ATTRIBUTES_PREFIX, "data_metrics."));
    }

    @Override
    public String factoryIdentifier() { return IDENTIFIER; }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(PROXY, PATH, YSON_SCHEMA, CREDENTIALS_SOURCE);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(ROLL_INTERVAL, WRITE_TIMEOUT, FactoryUtil.SINK_PARALLELISM,
                PROXY_ROLE, USE_TLS, TIMESTAMP_FORMAT_OPTION, TRACKABLE_FIELD,
                YT_USERNAME_OPTION, YT_TOKEN_OPTION);
    }
}
