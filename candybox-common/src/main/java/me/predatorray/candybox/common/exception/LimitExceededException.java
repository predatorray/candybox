package me.predatorray.candybox.common.exception;

/**
 * Thrown when a request violates a configured size limit (key length, metadata size, locator size,
 * Candy size, frame size, ...). A specialization of {@link ValidationException}.
 */
public class LimitExceededException extends ValidationException {

    public LimitExceededException(String message) {
        super(message);
    }
}
