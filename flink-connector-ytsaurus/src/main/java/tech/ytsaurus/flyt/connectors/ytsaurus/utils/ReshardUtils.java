package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ReshardUtils {

    /**
     * Generate uniform partitioning
     * See <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/resharding#hash">docs</a>
     */
    public static List<List<?>> getUniformPartitioning(int tabletCount) {
        List<List<?>> pivotKeys = new ArrayList<>();
        for (int i = 0; i < tabletCount; i++) {
            List<Long> pivotKey = new ArrayList<>();
            if (i != 0) {
                pivotKey.add(BigInteger.TWO.pow(64)
                        .multiply(BigInteger.valueOf(i))
                        .divide(BigInteger.valueOf(tabletCount))
                        .longValue()
                );
            }
            pivotKeys.add(pivotKey);
        }
        return pivotKeys;
    }
}
