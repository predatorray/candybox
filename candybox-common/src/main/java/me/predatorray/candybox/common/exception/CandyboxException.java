package me.predatorray.candybox.common.exception;

/**
 * Base of the Candybox exception hierarchy. Unchecked, so the public API stays clean; callers that
 * care about a specific failure catch the relevant subtype.
 */
public class CandyboxException extends RuntimeException {

    public CandyboxException(String message) {
        super(message);
    }

    public CandyboxException(String message, Throwable cause) {
        super(message, cause);
    }
}
