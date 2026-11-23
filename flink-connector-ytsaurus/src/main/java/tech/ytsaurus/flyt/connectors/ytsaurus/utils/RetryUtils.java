package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Duration;

import javax.annotation.Nullable;

import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.RETRY_STRATEGY;

@UtilityClass
public class RetryUtils {

    public static SerializableSupplier<RetryStrategy> getRetryStrategy(ReadableConfig options) {
        return getRetryStrategy(options.get(RETRY_STRATEGY));
    }

    public static SerializableSupplier<RetryStrategy> getRetryStrategy(
            tech.ytsaurus.flyt.connectors.ytsaurus.common.RetryStrategy retryStrategy) {
        switch (retryStrategy) {
            case EXPONENTIAL:
                return () -> new ExponentialBackoffRetryStrategy(5, Duration.ofSeconds(10), Duration.ofMinutes(2));
            case NO_RETRY:
                return () -> new FixedRetryStrategy(0, Duration.ZERO);
            default:
                throw new IllegalArgumentException("Unknown retry strategy type: " + retryStrategy);
        }
    }

    /**
     * Waits out the delay of the current strategy and only then advances it.
     *
     * @return strategy to use for the next attempt, or {@code null} if the thread was interrupted
     */
    @Nullable
    public static RetryStrategy awaitNextAttempt(RetryStrategy retry) {
        try {
            Thread.sleep(retry.getRetryDelay().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        return retry.getNextRetryStrategy();
    }
}
