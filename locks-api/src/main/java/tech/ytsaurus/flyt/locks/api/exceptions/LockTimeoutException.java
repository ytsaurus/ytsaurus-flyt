package tech.ytsaurus.flyt.locks.api.exceptions;

public class LockTimeoutException extends LockException {
    public LockTimeoutException(String message) {
        super(message);
    }

    public LockTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
