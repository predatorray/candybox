package me.predatorray.candybox.common.exception;

/**
 * Thrown when a persisted or wire record cannot be encoded or decoded (unknown format version,
 * truncated buffer, corrupt field).
 */
public class SerializationException extends CandyboxException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
