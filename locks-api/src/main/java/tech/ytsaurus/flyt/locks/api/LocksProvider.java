package tech.ytsaurus.flyt.locks.api;

import java.io.Serializable;
import java.time.Duration;

import tech.ytsaurus.flyt.locks.api.exceptions.LockConflictException;
import tech.ytsaurus.flyt.locks.api.exceptions.LockException;

/**
 * Universal interface for distributed locks with shared/exclusive modes.
 * Configured from Flink via configure() call.
 */
public interface LocksProvider extends Serializable {

    /**
     * Configure lock strategy from Flink Configuration.
     * Called by Flink framework once after instance creation.
     */
    void configure(org.apache.flink.configuration.ReadableConfig config);

    /**
     * Acquire resource lock with retries.
     *
     * @param resourcePath resource path/name (e.g., tablePath)
     * @param mode         lock mode: SHARED or EXCLUSIVE
     * @return lockId (lock identifier — use for release/renew)
     */
    String acquireLock(String resourcePath, LockMode mode) throws LockException;

    /**
     * Acquire resource lock with retries.
     *
     * @param resourcePath resource path/name (e.g., tablePath)
     * @param mode         lock mode: SHARED or EXCLUSIVE
     * @param timeout      timeout: if 0 means one try to acquire lock
     * @return lockId (lock identifier — use for release/renew)
     * @throws LockException         if lock cannot be acquired due to an error
     * @throws LockConflictException if the lock cannot be acquired because of a lock conflict
     */
    String acquireLock(String resourcePath, LockMode mode, Duration timeout)
            throws LockException, LockConflictException;

    /**
     * Release previously acquired lock by lockId.
     */
    void releaseLock(String lockId);

    /**
     * Renew lock lease/TTL by lockId (optional).
     * For implementations where renewal is not required — default empty implementation.
     */
    default void renewLock(String lockId) {
    }

    /**
     * Check if resource is currently locked (optional, not always implementable).
     */
    default boolean isLocked(String resourcePath) {
        return false;
    }

    /**
     * Returns lock strategy type.
     *
     * @return lock strategy type (e.g., "yt", "noop", etc.)
     */
    String getStrategyName();
}
