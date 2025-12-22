package tech.ytsaurus.flyt.connectors.ytsaurus.common.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;

@UtilityClass
@Slf4j
public class RowTypeUtils {
    public static String[] getFieldNames(RowType type) {
        return type.getFieldNames().toArray(new String[0]);
    }

    public static int findColumnByName(LogicalType type, String columnName) {
        RowType root = (RowType) type;
        return root.getFieldIndex(columnName);
    }

    public static LogicalType[] getFieldTypes(RowType type) {
        return type.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
    }

    /**
     * Make flink row data type out of class
     *
     * @param tClass the class to make row data out of
     * @param <T>    class type
     * @return data type - row with fields out of the original class if possible
     */
    public static <T> DataType getRowTypeFromClass(Class<T> tClass) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataTypeFactory dataTypeFactory = ((TableEnvironmentInternal) tableEnv)
                .getCatalogManager()
                .getDataTypeFactory();
        return unrollClassToRow(DataTypes.of(tClass).toDataType(dataTypeFactory));
    }

    public static DataType unrollClassToRow(DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case ARRAY:
            case MULTISET:
                return unrollCollection((CollectionDataType) dataType);

            case MAP:
                return unrollKeyValue((KeyValueDataType) dataType);

            case STRUCTURED_TYPE:
                // Essentially, the target transformation: class -> Row
                return classTypeToRow(dataType);

            default:
                return dataType;
        }
    }

    private static DataType classTypeToRow(DataType dataType) {
        List<DataTypes.Field> fieldList = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>(dataType.getChildren());
        StructuredType structuredType = (StructuredType) dataType.getLogicalType();

        Class<?> implementation = structuredType.getImplementationClass().orElseThrow();
        List<Field> classFieldList = Arrays.stream(implementation.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers()))
                .collect(Collectors.toList());

        List<String> attributeNames = structuredType.getAttributes().stream()
                .map(StructuredType.StructuredAttribute::getName)
                .collect(Collectors.toList());

        // Reorder resulting fields to be aligned with the order of fields
        // because order (during serialization, in flink data types) matters writing into YT
        for (Field field : classFieldList) {
            int reorderedIndex = attributeNames.indexOf(field.getName());
            if (reorderedIndex == -1) {
                throw new IllegalStateException(String.format(
                        "Unsupported conversion. " +
                                "Field '%s' found in class '%s' field list but not in flink converted type. " +
                                "Field list: %s, flink structured type attributes: %s",
                        field.getName(),
                        implementation,
                        classFieldList,
                        attributeNames));
            }
            String replacedFieldName = getAnnotatedFieldName(field);
            DataType original = dataTypes.get(reorderedIndex);
            DataType physicalType = unrollClassToRow(original);
            fieldList.add(DataTypes.FIELD(replacedFieldName, physicalType));
        }

        return DataTypes.ROW(fieldList.toArray(DataTypes.Field[]::new));
    }

    private static String getAnnotatedFieldName(Field field) {
        JsonProperty annotation = field.getAnnotation(JsonProperty.class);
        if (annotation == null) {
            return field.getName();
        }
        return annotation.value();
    }

    private static DataType unrollCollection(CollectionDataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        DataType transformedElement = unrollClassToRow(dataType.getElementDataType());

        LogicalType transformedLogicalType;
        if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            transformedLogicalType = new ArrayType(
                    logicalType.isNullable(),
                    transformedElement.getLogicalType());
        } else {
            transformedLogicalType = new MultisetType(
                    logicalType.isNullable(),
                    transformedElement.getLogicalType());
        }

        return new CollectionDataType(
                transformedLogicalType,
                dataType.getConversionClass(),
                transformedElement);
    }

    private static DataType unrollKeyValue(KeyValueDataType dataType) {
        DataType transformedKey = unrollClassToRow(dataType.getKeyDataType());
        DataType transformedValue = unrollClassToRow(dataType.getValueDataType());
        return new KeyValueDataType(
                new MapType(
                        dataType.getLogicalType().isNullable(),
                        transformedKey.getLogicalType(),
                        transformedValue.getLogicalType()),
                dataType.getConversionClass(),
                transformedKey,
                transformedValue);
    }
}
