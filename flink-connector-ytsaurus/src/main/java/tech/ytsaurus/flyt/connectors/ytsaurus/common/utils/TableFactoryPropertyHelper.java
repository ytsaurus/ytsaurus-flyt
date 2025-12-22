package tech.ytsaurus.flyt.connectors.ytsaurus.common.utils;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/**
 * Helper utility for discovering formats and validating all options for a {@link DynamicTableFactory}.
 * </p>
 * In addition to {@link FactoryUtil.TableFactoryHelper} it provides functionality
 * to skip validation for options with certain prefix and remove this prefix from all table options.
 * </p>
 * It is helpful when client and connector updates in different time. If client updates faster than
 * connector and passes unrecognized options with prefix, connector doesn't fail on validating. After connector
 * update it can operate with these new options normally.
 */
public class TableFactoryPropertyHelper extends FactoryUtil.FactoryHelper<DynamicTableFactory> {
    public static final String PROPERTIES_OPTION_PREFIX = "properties.";

    private final FactoryUtil.TableFactoryHelper tableFactoryHelper;

    public TableFactoryPropertyHelper(DynamicTableFactory tableFactory, DynamicTableFactory.Context context) {
        super(
                tableFactory,
                context.getCatalogTable().getOptions(),
                PROPERTY_VERSION,
                CONNECTOR);
        this.tableFactoryHelper = FactoryUtil.createTableFactoryHelper(factory, context);
    }

    public Configuration getOptionsWithoutPropertiesPrefix() {
        Configuration propertyOptions = new Configuration();
        Configuration basicOptions = new Configuration();
        allOptions.toMap().forEach((key, value) -> {
            if (key.startsWith(PROPERTIES_OPTION_PREFIX)) {
                propertyOptions.setString(key.substring(PROPERTIES_OPTION_PREFIX.length()), value);
            } else {
                basicOptions.setString(key, value);
            }
        });
        basicOptions.addAll(propertyOptions); // prioritize options with prefix on collisions
        return basicOptions;
    }

    public Properties getProperties() {
        Map<String, String> optionValuesWithPrefix = getOptionValuesWithPrefix(PROPERTIES_OPTION_PREFIX);
        Properties properties = new Properties();
        properties.putAll(optionValuesWithPrefix);
        return properties;
    }

    public Map<String, String> getOptionValuesWithAdditionalPrefix(String prefix) {
        return getOptionValuesWithPrefix(PROPERTIES_OPTION_PREFIX + prefix);
    }

    private Map<String, String> getOptionValuesWithPrefix(String prefix) {
        return allOptions.toMap().entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()), Map.Entry::getValue));
    }

    @Override
    public ReadableConfig getOptions() {
        return tableFactoryHelper.getOptions();
    }

    @Override
    public void validate() {
        tableFactoryHelper.validateExcept(PROPERTIES_OPTION_PREFIX);
    }

    @Override
    public void validateExcept(String... prefixesToSkip) {
        String[] prefixes = new String[prefixesToSkip.length + 1];
        System.arraycopy(prefixesToSkip, 0, prefixes, 0, prefixesToSkip.length);
        prefixes[prefixes.length - 1] = PROPERTIES_OPTION_PREFIX;
        tableFactoryHelper.validateExcept(prefixes);
    }

    public <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
            Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
        return tableFactoryHelper.discoverDecodingFormat(formatFactoryClass, formatOption);
    }

    public <I, F extends DecodingFormatFactory<I>> Optional<DecodingFormat<I>> discoverOptionalDecodingFormat(
            Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
        return tableFactoryHelper.discoverOptionalDecodingFormat(formatFactoryClass, formatOption);
    }

    public <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
            Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
        return tableFactoryHelper.discoverEncodingFormat(formatFactoryClass, formatOption);
    }

    public <I, F extends EncodingFormatFactory<I>> Optional<EncodingFormat<I>> discoverOptionalEncodingFormat(
            Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
        return tableFactoryHelper.discoverOptionalEncodingFormat(formatFactoryClass, formatOption);
    }
}
