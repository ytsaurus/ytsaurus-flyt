package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.LookupMethod;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

class YtRowDataLookupFunctionTest {
    @Test
    void doesNotResolveCredentialsBeforeRuntimeOpen() throws Exception {
        CredentialsProvider credentialsProvider = Mockito.mock(
                CredentialsProvider.class,
                Mockito.withSettings().serializable());
        @SuppressWarnings("unchecked")
        DeserializationSchema<RowData> deserializer = Mockito.mock(
                DeserializationSchema.class,
                Mockito.withSettings().serializable());
        DataType idType = DataTypes.BIGINT().notNull();
        DataType payloadType = DataTypes.STRING();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", idType),
                DataTypes.FIELD("payload", payloadType))
                .notNull()
                .getLogicalType();

        YtRowDataLookupFunction function = new YtRowDataLookupFunction(
                credentialsProvider,
                "[{name=id;type=int64;sort_order=ascending;};{name=payload;type=string;}]",
                ComplexYtPath.builder()
                        .clusterName("test-cluster")
                        .basePath("//tmp/lookup-test")
                        .isPartitioned(false)
                        .enableDynamicStoreRead(true)
                        .build(),
                LookupMethod.LOOKUP,
                null,
                deserializer,
                new String[]{"id", "payload"},
                new DataType[]{idType, payloadType},
                new String[]{"id"},
                new DataType[]{idType},
                rowType,
                false);

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(function);
        }

        Mockito.verifyNoInteractions(credentialsProvider);
    }
}
