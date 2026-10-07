package tech.ytsaurus.flyt.formats.yson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class YsonRowDataDeserializationSchemaTest {
    private static final RowType ROW_TYPE = RowType.of(
            new LogicalType[]{new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH),
                    new VarCharType(VarCharType.MAX_LENGTH)},
            new String[]{"id", "message", "optional"});
    private static final String MESSAGE = "Привет; 世界 🌍";
    private static final String PLAIN_ROW = "{\"id\"=42;\"message\"=\"" + MESSAGE + "\";\"optional\"=#}";

    private final YsonRowDataDeserializationSchema decoder = new YsonRowDataDeserializationSchema(
            ROW_TYPE, TypeInformation.of(RowData.class), false, false, TimestampFormat.SQL);

    @Test
    void roundTripsActualEncoderWithNullAndUnicode() throws IOException {
        YsonRowDataSerializationSchema encoder = new YsonRowDataSerializationSchema(ROW_TYPE, TimestampFormat.SQL);
        RowData input = GenericRowData.of(42L, StringData.fromString(MESSAGE), null);

        byte[] encoded = encoder.serialize(input);

        assertTrue(new String(encoded, StandardCharsets.UTF_8).endsWith(";"));
        assertRow(decoder.deserialize(encoded));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ";", " \n\t", "; \n\t", " \n;\t"})
    void acceptsPlainNodeAndOptionalTerminator(String suffix) throws IOException {
        assertRow(decoder.deserialize((" \n" + PLAIN_ROW + suffix).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void preservesBinaryNodeSupport() throws IOException {
        byte[] encoded = YTree.mapBuilder()
                .key("id").value(42L)
                .key("message").value(MESSAGE)
                .key("optional").entity()
                .buildMap().toBinary();

        assertRow(decoder.deserialize(encoded));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " \n\t", ";"})
    void rejectsEmptyRecords(String payload) {
        assertThrows(IOException.class, () -> decoder.deserialize(payload.getBytes(StandardCharsets.UTF_8)));
    }

    @ParameterizedTest
    @ValueSource(strings = {";{}", ";{};", ";#", ";garbage", "garbage", ";;", ";]"})
    void rejectsMultipleRecordsAndTrailingGarbage(String suffix) {
        assertThrows(IOException.class,
                () -> decoder.deserialize((PLAIN_ROW + suffix).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void preservesNullMessageHandling() throws IOException {
        assertNull(decoder.deserialize((byte[]) null));
    }

    private static void assertRow(RowData row) {
        assertEquals(3, row.getArity());
        assertEquals(42L, row.getLong(0));
        assertEquals(MESSAGE, row.getString(1).toString());
        assertTrue(row.isNullAt(2));
    }
}
