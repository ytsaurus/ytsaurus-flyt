package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import javax.annotation.Nullable;

import lombok.experimental.UtilityClass;
import org.apache.flink.util.concurrent.RetryStrategy;

@UtilityClass
public class RetryUtils {

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
