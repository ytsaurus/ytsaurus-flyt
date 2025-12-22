package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.api.ValidationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtWriterOptionsTest {

    @Test
    void testDefaultOptions() {
        YtWriterOptions ytWriterOptions = YtWriterOptions.builder()
                .build();

        Assertions.assertThat(ytWriterOptions.getCommitTransactionPeriod()).isEqualTo(Duration.of(10,
                ChronoUnit.SECONDS));
        Assertions.assertThat(ytWriterOptions.getFlushModificationPeriod()).isEqualTo(Duration.of(1,
                ChronoUnit.SECONDS));
        Assertions.assertThat(ytWriterOptions.getTransactionTimeout()).isEqualTo(Duration.of(5, ChronoUnit.MINUTES));
        Assertions.assertThat(ytWriterOptions.getRowsInModificationLimit()).isEqualTo(1_000);
        Assertions.assertThat(ytWriterOptions.getRowsInTransactionLimit()).isEqualTo(99_000);
    }

    @Test
    void testValidateFailsWhenSumGreaterThanLimit() {
        YtWriterOptions.YtWriterOptionsBuilder builder = YtWriterOptions.builder()
                .rowsInTransactionLimit(50_000)
                .rowsInModificationLimit(51_000);

        assertThrows(ValidationException.class, builder::build);
    }

    @Test
    void testValidateSucceedsExactlyOn100_000Sum() {
        YtWriterOptions.YtWriterOptionsBuilder builder = YtWriterOptions.builder()
                .rowsInTransactionLimit(10_000)
                .rowsInModificationLimit(90_000);

        assertDoesNotThrow(builder::build);

        builder.rowsInTransactionLimit(1)
                .rowsInModificationLimit(99_999);

        assertDoesNotThrow(builder::build);
    }

    @Test
    void testValidateFailsExactlyOn100_001Sum() {
        YtWriterOptions.YtWriterOptionsBuilder builder = YtWriterOptions.builder()
                .rowsInTransactionLimit(10_001)
                .rowsInModificationLimit(90_000);

        assertThrows(ValidationException.class, builder::build);
    }
}
