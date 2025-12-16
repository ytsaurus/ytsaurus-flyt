package tech.ytsaurus.flyt.formats.yson;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.flyt.formats.yson.common.TimestampFormat;

import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.IGNORE_PARSE_ERRORS;
import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.TIMESTAMP_FORMAT;
import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptionsUtil.validateDecodingFormatOptions;

public class YsonFormatFactory implements SerializationFormatFactory, DeserializationFormatFactory {

    public static final String IDENTIFIER = "yson";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateDecodingFormatOptions(formatOptions);

        return new DecodingFormat<>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
                final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
                TimestampFormat timestampOption = YsonFormatOptionsUtil.getTimestampFormat(formatOptions);
                return new YsonRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampOption);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        YsonFormatOptionsUtil.validateEncodingFormatOptions(formatOptions);
        TimestampFormat timestampOption = YsonFormatOptionsUtil.getTimestampFormat(formatOptions);

        return new EncodingFormat<>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new YsonRowDataSerializationSchema(rowType, timestampOption);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }
}
