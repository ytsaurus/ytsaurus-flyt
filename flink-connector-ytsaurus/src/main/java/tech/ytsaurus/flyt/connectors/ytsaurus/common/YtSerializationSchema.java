package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import tech.ytsaurus.core.tables.TableSchema;


public class YtSerializationSchema<T> implements Serializable, ResultTypeQueryable<T> {
    @Getter
    private final YtRowDeserializer<T> deserializer;

    @Getter
    private final YtRowFilter<T> initFilter;

    private final SerializableTableSchema serializableTableSchema;

    private final TypeInformation<T> typeInformation;

    @Getter
    private final String[] columns;

    public YtSerializationSchema(YtRowDeserializer<T> deserializer,
                                 String[] columns,
                                 TableSchema tableSchema,
                                 TypeInformation<T> typeInformation) {
        this(deserializer, columns, tableSchema, typeInformation, row -> true);
    }

    public YtSerializationSchema(YtRowDeserializer<T> deserializer,
                                 String[] columns,
                                 TableSchema tableSchema,
                                 TypeInformation<T> typeInformation,
                                 YtRowFilter<T> initFilter) {
        this.deserializer = deserializer;
        this.columns = columns;
        this.serializableTableSchema = new SerializableTableSchema(tableSchema);
        this.typeInformation = typeInformation;
        this.initFilter = initFilter;
    }

    public YtSerializationSchema(YtRowDeserializer<T> deserializer,
                                 String[] columns,
                                 TableSchema tableSchema,
                                 Class<T> tClass) {
        this(deserializer, columns, tableSchema, TypeInformation.of(tClass));
    }

    public YtSerializationSchema(YtRowDeserializer<T> deserializer,
                                 String[] columns,
                                 TableSchema tableSchema,
                                 Class<T> tClass,
                                 YtRowFilter<T> initFilter) {
        this(deserializer, columns, tableSchema, TypeInformation.of(tClass), initFilter);
    }

    public TableSchema getTableSchema() {
        return serializableTableSchema.toTableSchema();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }
}
