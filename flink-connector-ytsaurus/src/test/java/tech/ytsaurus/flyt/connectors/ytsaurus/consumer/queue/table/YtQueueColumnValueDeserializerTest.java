package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.github.luben.zstd.Zstd;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resolvesColumnIndicesOnlyOnFirstRecord(boolean withCodec) throws Exception {
        TableSchema initialSchema = mock(TableSchema.class);
        when(initialSchema.findColumn("value")).thenReturn(2);
        if (withCodec) {
            when(initialSchema.findColumn("codec")).thenReturn(0);
        }
        TableSchema laterSchema = mock(TableSchema.class);
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(
                format, "value", withCodec ? "codec" : null);
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)),
                value(2, ColumnValueType.STRING, withCodec ? compressZstd(PAYLOAD) : PAYLOAD));

        assertThat(deserializer.deserialize(row, initialSchema).getInt(0)).isEqualTo(PAYLOAD.length);
        assertThat(deserializer.deserialize(row, initialSchema).getInt(0)).isEqualTo(PAYLOAD.length);
        assertThat(deserializer.deserialize(row, laterSchema).getInt(0)).isEqualTo(PAYLOAD.length);

        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
        verify(initialSchema).findColumn("value");
        if (withCodec) {
            verify(initialSchema).findColumn("codec");
        }
        verifyNoMoreInteractions(initialSchema);
        verifyNoInteractions(laterSchema);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reinitializesColumnIndicesAfterSerialization(boolean withCodec) throws Exception {
        TableSchema initialSchema = TableSchema.builder()
                .addValue("codec", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(
                new RecordingDeserializationSchema(), "value", withCodec ? "codec" : null);
        byte[] payload = withCodec ? compressZstd(PAYLOAD) : PAYLOAD;
        byte[] codec = "zstd_6".getBytes(StandardCharsets.UTF_8);
        deserializer.deserialize(row(
                value(0, ColumnValueType.STRING, codec),
                value(1, ColumnValueType.STRING, payload)), initialSchema);

        YtQueueColumnValueDeserializer<RowData> restored =
                InstantiationUtil.clone(deserializer, getClass().getClassLoader());
        openWithCounters(restored);
        RowData result = restored.deserialize(row(
                value(0, ColumnValueType.STRING, payload),
                value(1, ColumnValueType.STRING, codec)), SCHEMA);

        assertThat(result.getInt(0)).isEqualTo(PAYLOAD.length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown_codec", "zstd_6"})
    void failsOnUnknownCodecOrCorruptedCompressionByDefault(String codecName) throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(format, "value", "codec");
        SourceReaderMetricGroup metrics = openWithCounters(deserializer);
        UnversionedRow row = invalidCompressedRow(codecName);

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(format.lastMessage).isNull();
        assertThat(metrics.counter("numDecompressionErrors").getCount()).isEqualTo(1);
        assertThat(metrics.counter("numNullPayloads").getCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown_codec", "zstd_6"})
    void skipsAndCountsDecompressionErrorsThenContinuesReading(String codecName) throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(format, "value", "codec", true);
        SourceReaderMetricGroup metrics = openWithCounters(deserializer);

        assertThat(deserializer.deserialize(invalidCompressedRow(codecName), SCHEMA)).isNull();
        assertThat(format.lastMessage).isNull();
        assertThat(metrics.counter("numDecompressionErrors").getCount()).isEqualTo(1);
        assertThat(metrics.counter("numNullPayloads").getCount()).isZero();

        UnversionedRow validRow = row(
                value(0, ColumnValueType.STRING, compressZstd(PAYLOAD)),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));
        assertThat(deserializer.deserialize(validRow, SCHEMA)).isNotNull();
        assertThat(format.lastMessage).isEqualTo(PAYLOAD);
        assertThat(metrics.counter("numDecompressionErrors").getCount()).isEqualTo(1);
    }

    @Test
    void propagatesFormatErrorsWhenDecompressionErrorsAreIgnored() throws Exception {
        @SuppressWarnings("unchecked")
        DeserializationSchema<RowData> format = mock(DeserializationSchema.class);
        IllegalArgumentException failure = new IllegalArgumentException("Invalid payload format");
        when(format.deserialize(PAYLOAD)).thenThrow(failure);
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(format, "value", "codec", true);
        SourceReaderMetricGroup metrics = openWithCounters(deserializer);
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, compressZstd(PAYLOAD)),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA)).isSameAs(failure);
        assertThat(metrics.counter("numDecompressionErrors").getCount()).isZero();
        assertThat(metrics.counter("numNullPayloads").getCount()).isZero();
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
    void countsRowsSkippedBecauseThePayloadIsNullOrAbsent() throws Exception {
        RecordingDeserializationSchema format = new RecordingDeserializationSchema();
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(format, "value", "codec");
        SourceReaderMetricGroup metrics = openWithCounters(deserializer);
        Counter counter = metrics.counter("numNullPayloads");
        UnversionedRow row = row(
                value(0, ColumnValueType.NULL, null),
                value(1, ColumnValueType.STRING, "zstd_6".getBytes(StandardCharsets.UTF_8)));

        assertThat(deserializer.deserialize(row, SCHEMA)).isNull();
        assertThat(counter.getCount()).isEqualTo(1);
        assertThat(deserializer.deserialize(row(value(1, ColumnValueType.STRING, new byte[0])), SCHEMA))
                .isNull();
        assertThat(format.lastMessage).isNull();
        assertThat(counter.getCount()).isEqualTo(2);

        assertThat(deserializer.deserialize(row(value(0, ColumnValueType.STRING, PAYLOAD)), SCHEMA))
                .isNotNull();
        assertThat(counter.getCount()).isEqualTo(2);
        assertThat(metrics.counter("numDecompressionErrors").getCount()).isZero();
    }

    @Test
    void doesNotCountNullFormatResultsAsNullPayloads() throws Exception {
        @SuppressWarnings("unchecked")
        DeserializationSchema<RowData> format = mock(DeserializationSchema.class);
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(format);
        Counter counter = openWithCounters(deserializer).counter("numNullPayloads");

        assertThat(deserializer.deserialize(row(value(0, ColumnValueType.STRING, PAYLOAD)), SCHEMA))
                .isNull();
        assertThat(counter.getCount()).isZero();
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failsWhenValueColumnIsAbsentFromTheQueueSchema(boolean ignoreDecompressionErrors) {
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(
                        new RecordingDeserializationSchema(), "payload", null, ignoreDecompressionErrors);
        UnversionedRow row = row(value(0, ColumnValueType.STRING, PAYLOAD));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payload");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failsWhenConfiguredCodecColumnIsAbsentFromTheQueueSchema(boolean ignoreDecompressionErrors) {
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(
                new RecordingDeserializationSchema(),
                "value",
                "compression",
                ignoreDecompressionErrors);
        UnversionedRow row = row(value(0, ColumnValueType.STRING, PAYLOAD));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("compression");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failsWhenValueColumnIsNotStringLike(boolean ignoreDecompressionErrors) {
        YtQueueColumnValueDeserializer<RowData> deserializer =
                new YtQueueColumnValueDeserializer<>(
                        new RecordingDeserializationSchema(), "value", null, ignoreDecompressionErrors);
        UnversionedRow row = row(value(0, ColumnValueType.INT64, 1L));

        assertThatThrownBy(() -> deserializer.deserialize(row, SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("string-like");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failsWhenCodecColumnIsNotStringLike(boolean ignoreDecompressionErrors) {
        YtQueueColumnValueDeserializer<RowData> deserializer = new YtQueueColumnValueDeserializer<>(
                new RecordingDeserializationSchema(), "value", "codec", ignoreDecompressionErrors);
        UnversionedRow row = row(
                value(0, ColumnValueType.STRING, PAYLOAD),
                value(1, ColumnValueType.INT64, 1L));

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
        verify(metricGroup).counter("numNullPayloads");
        verify(metricGroup).counter("numDecompressionErrors");
        assertThat(initializationContext.getValue().getMetricGroup()).isSameAs(metricGroup);
        assertThat(initializationContext.getValue().getUserCodeClassLoader()).isSameAs(classLoader);
    }

    private static SourceReaderMetricGroup openWithCounters(YtQueueColumnValueDeserializer<RowData> deserializer)
            throws Exception {
        SourceReaderContext context = mock(SourceReaderContext.class);
        SourceReaderMetricGroup metricGroup = mock(SourceReaderMetricGroup.class);
        when(context.metricGroup()).thenReturn(metricGroup);
        when(metricGroup.counter("numNullPayloads")).thenReturn(new SimpleCounter());
        when(metricGroup.counter("numDecompressionErrors")).thenReturn(new SimpleCounter());
        deserializer.open(context);
        return metricGroup;
    }

    private static UnversionedRow invalidCompressedRow(String codecName) {
        byte[] truncatedPayload = Arrays.copyOf(compressZstd(PAYLOAD), Long.BYTES + 1);
        return row(
                value(0, ColumnValueType.STRING, truncatedPayload),
                value(1, ColumnValueType.STRING, codecName.getBytes(StandardCharsets.UTF_8)));
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
