package me.predatorray.candybox.common.exception;

/**
 * Thrown when an underlying storage operation (ledger create/append/read/recover) fails. Wraps the
 * backend-specific cause so callers never see raw BookKeeper or ZooKeeper exceptions.
 */
public class StorageException extends CandyboxException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
