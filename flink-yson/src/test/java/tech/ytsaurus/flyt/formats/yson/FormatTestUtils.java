package tech.ytsaurus.flyt.formats.yson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

@UtilityClass
public class FormatTestUtils {
    public static void assertFormatExists(String identifier, Class<? extends Factory> factoryClass) {
        Assertions.assertNotNull(
                FactoryUtil.discoverFactory(
                        FormatTestUtils.class.getClassLoader(),
                        factoryClass,
                        identifier));
    }

    public static DynamicTableSource.Context makeDummyTableSourceContext() {
        return new DynamicTableSource.Context() {
            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                return (TypeInformation<T>) TypeInformation.of(String.class);
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                return (TypeInformation<T>) TypeInformation.of(String.class);
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
                return null;
            }
        };
    }

    public static DynamicTableSink.Context makeDummyTableSinkContext() {
        return new DynamicTableSink.Context() {
            @Override
            public boolean isBounded() {
                return false;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                return (TypeInformation<T>) TypeInformation.of(String.class);
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                return (TypeInformation<T>) TypeInformation.of(String.class);
            }

            @Override
            public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
                return null;
            }

            @Override
            public Optional<int[][]> getTargetColumns() {
                return Optional.empty();
            }
        };
    }

    @SneakyThrows
    public static DeserializationSchema<RowData> makeTestDeserializationSchema(DeserializationFormatFactory factory,
                                                                               DataType dataType,
                                                                               Map<String, String> options) {
        var format = factory.createDecodingFormat(null, Configuration.fromMap(options));
        var schema = format.createRuntimeDecoder(makeDummyTableSourceContext(), dataType);
        var context = Mockito.mock(DeserializationSchema.InitializationContext.class);
        Mockito.when(context.getMetricGroup()).thenReturn(Mockito.mock(MetricGroup.class));
        schema.open(context);
        return schema;
    }

    @SneakyThrows
    public static DeserializationSchema<RowData> makeTestDeserializationSchema(DeserializationFormatFactory factory,
                                                                               DataType dataType) {
        return makeTestDeserializationSchema(factory, dataType, Map.of());
    }

    @SneakyThrows
    public static SerializationSchema<RowData> makeTestSerializationSchema(SerializationFormatFactory factory,
                                                                           DataType dataType,
                                                                           Map<String, String> options) {
        var format = factory.createEncodingFormat(null, Configuration.fromMap(options));
        var schema = format.createRuntimeEncoder(makeDummyTableSinkContext(), dataType);
        schema.open(null);
        return schema;
    }

    @SneakyThrows
    public static SerializationSchema<RowData> makeTestSerializationSchema(SerializationFormatFactory factory,
                                                                           DataType dataType) {
        return makeTestSerializationSchema(factory, dataType, Map.of());
    }

    @SneakyThrows
    public static List<RowData> deserializeWithSchema(DeserializationSchema<RowData> schema, byte[] message) {
        List<RowData> results = new ArrayList<>();
        Collector<RowData> collector = new Collector<>() {
            @Override
            public void collect(RowData record) {
                results.add(record);
            }

            @Override
            public void close() {
            }
        };
        schema.deserialize(message, collector);
        return results;
    }

    @SneakyThrows
    public static RowData deserializeWithSchemaSingle(DeserializationSchema<RowData> schema, byte[] message) {
        List<RowData> result = deserializeWithSchema(schema, message);
        if (result.size() != 1) {
            throw new AssertionFailedError(
                    String.format("Expected single record, but got: %s",
                            result.stream()
                                    .map(Objects::toString)
                                    .collect(Collectors.joining(", "))));
        }
        return result.get(0);
    }
}
