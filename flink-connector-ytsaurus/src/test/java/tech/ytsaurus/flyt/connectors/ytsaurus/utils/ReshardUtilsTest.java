package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ReshardUtilsTest {

    @Test
    void getUniformPartitioningOne() {
        List<List<?>> result = ReshardUtils.getUniformPartitioning(1);
        List<List<?>> expected = List.of(
                List.of()
        );
        assertListOfListEquals(expected, result);
    }

    @Test
    void getUniformPartitioningTwo() {
        List<List<?>> result = ReshardUtils.getUniformPartitioning(2);
        List<List<?>> expected = List.of(
                List.of(),
                List.of(Long.parseUnsignedLong("9223372036854775808"))
        );
        assertListOfListEquals(expected, result);
    }

    @Test
    void getUniformPartitioningThree() {
        List<List<?>> result = ReshardUtils.getUniformPartitioning(3);
        List<List<?>> expected = List.of(
                List.of(),
                List.of(Long.parseUnsignedLong("6148914691236517205")),
                List.of(Long.parseUnsignedLong("12297829382473034410"))
        );
        assertListOfListEquals(expected, result);
    }

    @Test
    void getUniformPartitioningEight() {
        List<List<?>> result = ReshardUtils.getUniformPartitioning(8);
        List<List<?>> expected = List.of(
                List.of(),
                List.of(Long.parseUnsignedLong("2305843009213693952")),
                List.of(Long.parseUnsignedLong("4611686018427387904")),
                List.of(Long.parseUnsignedLong("6917529027641081856")),
                List.of(Long.parseUnsignedLong("9223372036854775808")),
                List.of(Long.parseUnsignedLong("11529215046068469760")),
                List.of(Long.parseUnsignedLong("13835058055282163712")),
                List.of(Long.parseUnsignedLong("16140901064495857664"))
        );
        assertListOfListEquals(expected, result);
    }

    private void assertListOfListEquals(List<List<?>> expected, List<List<?>> result) {
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i).toArray(), result.get(i).toArray());
        }
    }
}
