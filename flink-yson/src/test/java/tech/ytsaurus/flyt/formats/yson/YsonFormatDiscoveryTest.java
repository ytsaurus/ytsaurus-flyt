package tech.ytsaurus.flyt.formats.yson;

import org.junit.jupiter.api.Test;

public class YsonFormatDiscoveryTest {
    @Test
    public void checkFormatDiscoverable() {
        FormatTestUtils.assertFormatExists(YsonFormatFactory.IDENTIFIER, YsonFormatFactory.class);
    }
}
