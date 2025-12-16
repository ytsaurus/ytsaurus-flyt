package tech.ytsaurus.flyt.locks.api.exceptions;

public class LockConflictException extends LockException {

    public LockConflictException(String message) {
        super(message);
    }

    public LockConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
