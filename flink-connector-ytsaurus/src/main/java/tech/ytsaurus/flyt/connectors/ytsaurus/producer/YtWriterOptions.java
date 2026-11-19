package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Serializable;
import java.time.Duration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import tech.ytsaurus.client.request.Atomicity;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.locks.LocksConfig;


@Data
public class YtWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int ROWS_LIMIT = 100_000;

    public static final Atomicity DEFAULT_ATOMICITY = Atomicity.Full;

    public static final Duration DEFAULT_FLUSH_MODIFICATION_PERIOD = Duration.ofSeconds(1);
    public static final Duration DEFAULT_COMMIT_TRANSACTION_PERIOD = Duration.ofSeconds(10);
    public static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofMinutes(5);

    private Duration commitTransactionPeriod;
    private Duration flushModificationPeriod;
    private Duration transactionTimeout;
    private int rowsInTransactionLimit;
    private int rowsInModificationLimit;
    private MountMode mountMode;
    private Atomicity atomicity;
    private LocksConfig locksConfig;
    private String proxyRole;

    @SuppressWarnings("checkstyle:parameternumber")
    YtWriterOptions(Duration commitTransactionPeriod,
                    Duration flushModificationPeriod,
                    Duration transactionTimeout,
                    int rowsInTransactionLimit,
                    int rowsInModificationLimit,
                    MountMode mountMode,
                    Atomicity atomicity,
                    LocksConfig locksConfig,
                    String proxyRole) {
        this.commitTransactionPeriod = commitTransactionPeriod;
        this.flushModificationPeriod = flushModificationPeriod;
        this.transactionTimeout = transactionTimeout;
        this.rowsInTransactionLimit = rowsInTransactionLimit;
        this.rowsInModificationLimit = rowsInModificationLimit;
        this.mountMode = mountMode;
        this.atomicity = atomicity;
        this.locksConfig = locksConfig;
        this.proxyRole = proxyRole;
    }

    public static YtWriterOptionsBuilder builder() {
        return new YtWriterOptionsBuilder();
    }

    @Slf4j
    public static class YtWriterOptionsBuilder {
        private Duration commitTransactionPeriod = DEFAULT_COMMIT_TRANSACTION_PERIOD;
        private Duration flushModificationPeriod = DEFAULT_FLUSH_MODIFICATION_PERIOD;
        private Duration transactionTimeout = DEFAULT_TRANSACTION_TIMEOUT;
        private int rowsInTransactionLimit = 99_000;
        private int rowsInModificationLimit = 1_000;
        private MountMode mountMode = MountMode.ALWAYS;
        private Atomicity atomicity = DEFAULT_ATOMICITY;
        private LocksConfig locksConfig = new LocksConfig("noop", new Configuration());
        private String proxyRole;

        YtWriterOptionsBuilder() {
        }

        public YtWriterOptionsBuilder commitTransactionPeriod(Duration commitTransactionPeriod) {
            this.commitTransactionPeriod = commitTransactionPeriod;
            return this;
        }

        public YtWriterOptionsBuilder flushModificationPeriod(Duration flushModificationPeriod) {
            this.flushModificationPeriod = flushModificationPeriod;
            return this;
        }

        public YtWriterOptionsBuilder transactionTimeout(Duration transactionTimeout) {
            this.transactionTimeout = transactionTimeout;
            return this;
        }

        public YtWriterOptionsBuilder rowsInTransactionLimit(int rowsInTransactionLimit) {
            this.rowsInTransactionLimit = rowsInTransactionLimit;
            return this;
        }

        public YtWriterOptionsBuilder rowsInModificationLimit(int rowsInModificationLimit) {
            this.rowsInModificationLimit = rowsInModificationLimit;
            return this;
        }

        public YtWriterOptionsBuilder mountMode(MountMode mountMode) {
            this.mountMode = mountMode;
            return this;
        }

        public YtWriterOptionsBuilder atomicity(Atomicity atomicity) {
            this.atomicity = atomicity;
            return this;
        }

        public YtWriterOptionsBuilder lockConfig(LocksConfig locksConfig) {
            this.locksConfig = locksConfig;
            return this;
        }

        public YtWriterOptionsBuilder proxyRole(String proxyRole) {
            this.proxyRole = proxyRole;
            return this;
        }

        public YtWriterOptions build() {
            validate();
            YtWriterOptions ytWriterOptions = new YtWriterOptions(this.commitTransactionPeriod,
                    this.flushModificationPeriod, this.transactionTimeout, this.rowsInTransactionLimit,
                    this.rowsInModificationLimit, this.mountMode, this.atomicity, this.locksConfig, this.proxyRole);
            log.info("Successfully initialized  YtWriterOptions: {}", ytWriterOptions);
            return ytWriterOptions;
        }

        private void validate() {
            if (this.rowsInTransactionLimit + this.rowsInModificationLimit > ROWS_LIMIT) {
                throw new ValidationException(
                        String.format("Rows in transaction and modification limit must be lower than %d", ROWS_LIMIT)
                );
            }
        }
    }
}
