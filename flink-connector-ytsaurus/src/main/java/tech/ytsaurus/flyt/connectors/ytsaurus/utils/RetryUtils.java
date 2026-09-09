package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import lombok.experimental.UtilityClass;
import org.apache.flink.util.concurrent.RetryStrategy;

@UtilityClass
public class RetryUtils {

    /**
     * Waits out the delay of the current strategy and only then advances it. The order matters:
     * {@link RetryStrategy#getRetryDelay()} reports the delay of the strategy it is called on, so
     * sleeping after advancing skips the configured initial delay and waits the doubled one.
     *
     * <p>Interruption is propagated rather than swallowed — whether a half-finished operation may
     * stop quietly is the caller's decision, not this helper's.
     *
     * @param retry strategy of the attempt that has just failed; must have retries remaining
     * @return strategy to use for the next attempt
     */
    public static RetryStrategy awaitNextAttempt(RetryStrategy retry) throws InterruptedException {
        Thread.sleep(retry.getRetryDelay().toMillis());
        return retry.getNextRetryStrategy();
    }
}
