package me.predatorray.candybox.common.exception;

/**
 * Thrown when a request is structurally invalid (bad name, malformed key, etc.). Validated at the
 * client (fail fast) and re-validated at the node (authoritative).
 */
public class ValidationException extends CandyboxException {

    public ValidationException(String message) {
        super(message);
    }
}
