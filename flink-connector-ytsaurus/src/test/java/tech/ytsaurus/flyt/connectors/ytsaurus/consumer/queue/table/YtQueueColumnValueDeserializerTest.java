package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.github.luben.zstd.Zstd;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YtQueueColumnValueDeserializerTest {
    private static final byte[] PAYLOAD = "{message=\"hello\"}".getBytes(StandardCharsets.UTF_8);

    private static final TableSchema SCHEMA = TableSchema.builder()
            .addValue("value", ColumnValueType.STRING)
            .addValue("codec", ColumnValueType.STRING)
            .addValue("$timestamp", ColumnValueType.UINT64)
            .build();

    private static final TableSchema SCHEMA_WITHOUT_CODEC = TableSchema.builder()
            .addValue("value", ColumnValueType.STRING)
            .addValue("$timestamp", ColumnValueType.UINT64)
            .build();

    @Test
    void decompressesValueBeforeDelegatingToTheFormat() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, compressZstd(PAYLOAD)),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)),
                value(2, ColumnValueType.UINT64, 42L));

        RowData deserialized = new YtQueueColumnValueDeserializer<>(format, "value", "codec")
                .deserialize(row, SCHEMA);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
        assertThat(deserialized).isNotNull();
    }

    @Test
    void readsValueAsIsWhenCodecColumnIsNotConfigured() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, PAYLOAD),
                value(1, ColumnValueType.UINT64, 42L));

        new YtQueueColumnValueDeserializer<>(format).deserialize(row, SCHEMA_WITHOUT_CODEC);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
    }

    @Test
    void ignoresCodecColumnPresentInSchemaWhenItIsNotConfigured() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, PAYLOAD),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));

        new YtQueueColumnValueDeserializer<>(format).deserialize(row, SCHEMA);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
    }

    @Test
    void readsUncompressedValueWhenCodecValueIsNull() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, PAYLOAD),
                value(1, ColumnValueType.NULL, null));

        new YtQueueColumnValueDeserializer<>(format, "value", "codec").deserialize(row, SCHEMA);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
    }

    @Test
    void skipsRowWithNullValue() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.NULL, null),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));

        assertThat(new YtQueueColumnValueDeserializer<>(format, "value", "codec").deserialize(row, SCHEMA))
                .isNull();
        assertThat(format.lastMessage).isNull();
    }

    @Test
    void readsValueAndCodecFromCustomColumns() throws Exception {
        TableSchema schema = TableSchema.builder()
                .addValue("payload", ColumnValueType.STRING)
                .addValue("compression", ColumnValueType.STRING)
                .build();
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, compressZstd(PAYLOAD)),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));

        new YtQueueColumnValueDeserializer<>(format, "payload", "compression")
                .deserialize(row, schema);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
    }

    @Test
    void failsWhenValueColumnIsAbsentFromTheQueueSchema() {
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(new RecordingDeserializationSchema(), "payload");
        UnversionedRow row = row(value(0, ColumnValueType.STRING, PAYLOAD));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void failsWhenConfiguredCodecColumnIsAbsentFromTheQueueSchema() {
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(
                new RecordingDeserializationSchema(),
                "value",
                "compression");
        UnversionedRow row = row(value(0, ColumnValueType.STRING, PAYLOAD));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("compression");
    }

    @Test
    void failsWhenValueColumnIsNotStringLike() {
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(new RecordingDeserializationSchema());
        UnversionedRow row = row(value(0, ColumnValueType.INT64, 1L));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("string-like");
    }

    @Test
    void rejectsBlankAndEqualColumnNames() {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();

        assertThatThrownBy(() -> new YtQueueColumnValueDeserializer<>(format, " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new YtQueueColumnValueDeserializer<>(format, "value", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new YtQueueColumnValueDeserializer<>(format, "value", "value"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void opensFlinkDeserializerWithSourceReaderContext() throws Exception {
        @SuppressWarnings("unchecked")
        DeserializationSchema<RowData> format = mock(DeserializationSchema.class);
        SourceReaderContext context = mock(SourceReaderContext.class);
        SourceReaderMetricGroup metricGroup = mock(SourceReaderMetricGroup.class);
        UserCodeClassLoader classLoader = mock(UserCodeClassLoader.class);
        when(context.metricGroup()).thenReturn(metricGroup);
        when(context.getUserCodeClassLoader()).thenReturn(classLoader);

        new YtQueueColumnValueDeserializer<>(format).open(context);

        ArgumentCaptor<DeserializationSchema.InitializationContext> initializationContext =
                ArgumentCaptor.forClass(DeserializationSchema.InitializationContext.class);
        verify(format).open(initializationContext.capture());
        assertThat(initializationContext.getValue().getMetricGroup()).isSameAs(metricGroup);
        assertThat(initializationContext.getValue().getUserCodeClassLoader()).isSameAs(classLoader);
    }

    private static UnversionedRow row(UnversionedValue... values) {
        return new UnversionedRow(List.of(values));
    }

    private static UnversionedValue value(int id, ColumnValueType type, Object value) {
        return new UnversionedValue(id, type, false, value);
    }

    private static byte[] compressZstd(byte[] payload) {
        byte[] frame = Zstd.compress(payload, 6);
        byte[] compressed = new byte[Long.BYTES + frame.length];
        ByteBuffer.wrap(compressed, 0, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(payload.length);
        System.arraycopy(frame, 0, compressed, Long.BYTES, frame.length);
        return compressed;
    }

    private static final class RecordingDeserializationSchema implements DeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;

        private byte[] lastMessage;

        @Override
        public RowData deserialize(byte[] message) {
            lastMessage = message;
            return GenericRowData.of(message.length);
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<RowData> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class);
        }
    }
}
