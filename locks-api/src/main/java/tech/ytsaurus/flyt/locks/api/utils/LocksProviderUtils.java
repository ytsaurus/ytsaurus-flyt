package tech.ytsaurus.flyt.locks.api.utils;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.flyt.locks.api.LockMode;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.exceptions.LockException;

@Slf4j
@UtilityClass
public class LocksProviderUtils {

    /**
     * Tries to perform the action under a distributed lock if predicate is false.
     * Double-checks predicate after acquiring the lock to avoid races.
     * Retries on lock exceptions, as per retryStrategy. All other exceptions abort immediately.
     *
     * @param resourcePath  resource identifier to lock
     * @param mode          lock mode
     * @param predicate     when false, action is performed; when true, action is skipped
     * @param action        action performed under acquired lock
     * @param retryStrategy Flink-style retry strategy
     * @param locksProvider LocksProvider (yt, noop, etc)
     * @param <T>           result type
     * @return result of action, or null if predicate true before/during lock
     */
    public static <T> T doWithLockAndPredicate(
            String resourcePath,
            LockMode mode,
            BooleanSupplier predicate,
            Callable<T> action,
            RetryStrategy retryStrategy,
            LocksProvider locksProvider
    ) throws Exception {
        RetryStrategy currentStrategy = retryStrategy;

        while (!predicate.getAsBoolean()) {
            String lockId = null;
            try {
                log.info("Acquiring lock for resource: {}", resourcePath);
                lockId = locksProvider.acquireLock(resourcePath, mode, Duration.ZERO);

                // Double-check predicate
                if (predicate.getAsBoolean()) {
                    return null;
                }

                return action.call();

            } catch (LockException e) {
                if (currentStrategy.getNumRemainingRetries() <= 0) {
                    throw new RuntimeException("No more retries left for resource: " + resourcePath, e);
                }
                Duration delay = currentStrategy.getRetryDelay();
                currentStrategy = currentStrategy.getNextRetryStrategy();

                log.warn("LockException ({}), will retry for resource: {} after {} ms",
                        e.getClass().getSimpleName(),
                        resourcePath,
                        delay.toMillis());

                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedException("Interrupted during sleep between retries");
                }

            } catch (Exception e) {
                log.error("Unexpected exception during action under lock for resource: {}", resourcePath, e);
                throw new RuntimeException(e);
            } finally {
                if (lockId != null) {
                    log.info("Releasing lock for resource: {}", resourcePath);
                    locksProvider.releaseLock(lockId);
                }
            }
        }
        return null;
    }

}
