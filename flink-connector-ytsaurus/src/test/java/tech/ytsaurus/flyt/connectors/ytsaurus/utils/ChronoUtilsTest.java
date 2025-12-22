package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChronoUtilsTest {

    @Test
    void getTimestampFromUtcDateTime() {
        long timestampFromDateTime = ChronoUtils.getTimestampFromUtcDateTime("2023-11-08T12:23:58.808");
        assertEquals(1699446238808L, timestampFromDateTime);
    }
}
