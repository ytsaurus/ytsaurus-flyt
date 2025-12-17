package tech.ytsaurus.flyt.locks.api.exceptions;

public class LockUnknownException extends LockException {
    public LockUnknownException(String message) {
        super(message);
    }

    public LockUnknownException(String message, Throwable cause) {
        super(message, cause);
    }
}
