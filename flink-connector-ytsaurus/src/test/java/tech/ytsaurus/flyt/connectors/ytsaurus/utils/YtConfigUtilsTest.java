package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class YtConfigUtilsTest {

    @Test
    public void splitsNonPartitionedTablePath() {
        ComplexYtPath path = YtConfigUtils.makeComplexPath(
                "//home/project/tables/orders", "hahn", false, new Configuration());

        assertEquals("//home/project/tables", path.getBasePath());
        assertEquals("orders", path.getTableName());
        assertEquals("//home/project/tables/orders", path.getFullPath());
        assertFalse(path.isPartitioned());
    }

    @Test
    public void splitsTablePathDirectlyUnderRoot() {
        ComplexYtPath path = YtConfigUtils.makeComplexPath(
                "//orders", "hahn", false, new Configuration());

        assertEquals("/", path.getBasePath());
        assertEquals("orders", path.getTableName());
        assertEquals("//orders", path.getFullPath());
    }

    @Test
    public void keepsPartitionedBasePathWithoutTableName() {
        ComplexYtPath path = YtConfigUtils.makeComplexPath(
                "//home/project/tables/orders", "hahn", true, new Configuration());

        assertEquals("//home/project/tables/orders", path.getBasePath());
        assertNull(path.getTableName());
    }
}
