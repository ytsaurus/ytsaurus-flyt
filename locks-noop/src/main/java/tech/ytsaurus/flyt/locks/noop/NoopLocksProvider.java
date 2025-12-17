package tech.ytsaurus.flyt.locks.noop;

import java.time.Duration;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;
import tech.ytsaurus.flyt.locks.api.LockMode;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.exceptions.LockException;

@Slf4j
public class NoopLocksProvider implements LocksProvider {

    private static final long serialVersionUID = 1L;

    private static final String LOCK_STRATEGY_NAME = "noop";

    @Override
    public void configure(ReadableConfig config) {
        log.info("Configure NoopLocksProvider. Do nothing.");
    }

    @Override
    public String acquireLock(String resourcePath, LockMode mode) throws LockException {
        log.info("Acquire lock NoopLocksProvider. Do nothing. [{}; {}]", resourcePath, mode);
        return UUID.randomUUID().toString();
    }

    @Override
    public String acquireLock(String resourcePath, LockMode mode, Duration timeout) throws LockException {
        log.info("Acquire lock NoopLocksProvider. Do nothing. [{}; {}; {}]", resourcePath, mode, timeout);
        return UUID.randomUUID().toString();
    }

    @Override
    public void releaseLock(String lockId) {
        log.info("Release lock NoopLocksProvider. Do nothing. {}}", lockId);
    }

    @Override
    public String getStrategyName() {
        return LOCK_STRATEGY_NAME;
    }
}
